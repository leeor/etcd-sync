package etcdsync

import (
	"time"

	"github.com/coreos/go-etcd/etcd"
	"github.com/golang/glog"
)

/*
 * etcd-based mutex
 *
 * Locking works using the following scheme:
 * 1. Attempt CompareAndSwap() to grab the lock. If it works -> we have the
 *    lock.
 * 2. If the key does not exist, try creating it with Create(). If that works ->
 *    we have the lock. If Create() fails, it might be due to a race condition
 *    with another node which was able to create the key before us. So,
 * 3. Attempt to CompareAndSwap() again, expect to find that the key exists, and
 *    the lock taken by another node. If not, then we have the lock.
 * 4. Watch the key, using the index returned by the previous call to
 *    CompareAndSwap(), and wait for the lock to be released or expire.
 * 5. Goto #3.
 *
 * Once we have the lock, keep refreshing its ttl until we're signaled to
 * release it.
 */

type lockState uint

const (
	unknown  lockState = 0
	released lockState = 1 << iota
	acquired lockState = 1 << iota
)

type EtcdMutex struct {
	key string
	ttl uint64

	client *etcd.Client

	state lockState
	index uint64

	quit     chan bool
	released chan bool

	debug bool
}

func NewMutexFromClient(client *etcd.Client, key string, ttl uint64) *EtcdMutex {

	m := &EtcdMutex{client: client}

	if ttl == 0 {

		ttl = 3
	}

	m.key = key
	m.ttl = ttl

	m.quit = make(chan bool)
	m.released = make(chan bool)

	return m
}

func NewMutexFromServers(servers []string, key string, ttl uint64) *EtcdMutex {

	client := etcd.NewClient(servers)

	return NewMutexFromClient(client, key, ttl)
}

func (m *EtcdMutex) setDebug(on bool) {

	m.debug = on
}

func (m *EtcdMutex) Lock() {

	glog.Infof("[%s] Lock called", m.key)
	res, err := m.client.CompareAndSwap(m.key, "locked", m.ttl, "released", 0)
	if err == nil {

		glog.Infof("[%s] lock acquired on first attempt", m.key)
		m.state = acquired
		m.index = res.Node.ModifiedIndex
	} else {

		if etcderr, ok := err.(*etcd.EtcdError); ok {
			glog.Infof("[%s] first attempt at acquiring lock failed: %#v", m.key, etcderr)
			switch etcderr.ErrorCode {
			case 100:
				// The key does not exist, let's try to create it
				glog.Infof("[%s] lock key does not exist, will attempt to create it", m.key)
				if res, err := m.client.Create(m.key, "locked", 1); err != nil {
					// Someone has created and locked this key before us.
					glog.Infof("[%s] could not create lock key, someone probably beat us to it", m.key)
					m.state = released
				} else {

					glog.Infof("[%s] created key and locked mutex (%#v, %d)", m.key, res.Node, res.Node.ModifiedIndex)
					m.state = acquired
					m.index = res.Node.ModifiedIndex
				}

			case 101:
				// couldn't set the key, the prevValue we gave it differs from the
				// one in the server. Someone else has this key.
				glog.Infof("[%s] mutex is locked", m.key)
				m.state = released

			default:
				glog.Infof("[%s] unexpected error: %#v", m.key, etcderr)
			}
		}
	}

	for m.state == released {

		glog.Infof("[%s] attempting to acquire lock again", m.key)
		res, err := m.client.CompareAndSwap(m.key, "locked", m.ttl, "released", 0)
		if err == nil {

			glog.Infof("[%s] lock acquired", m.key)
			m.state = acquired
			m.index = res.Node.ModifiedIndex
			break
		}

		if etcderr, ok := err.(*etcd.EtcdError); ok {

			glog.Infof("[%s] still unable to acquire lock, watching key (%#v, %d)", m.key, etcderr, etcderr.Index)

			receive := make(chan *etcd.Response)
			stop := make(chan bool)
			go m.client.Watch(m.key, etcderr.Index, false, receive, stop)

			for res = range receive {

				if res.Node.Value == "released" {

					glog.Infof("[%s] mutex has been released", m.key)
					break
				} else {

					glog.Infof("[%s] received message: %#v", m.key, res)
				}
			}

			stop <- true
		} else {

			glog.Infof("[%s] still unable to acquire lock, watching key (%#v)", m.key, err)
		}
	}

	// by now, m.state has to be acquired
	if m.state != acquired {

		panic("etcd-sync: mutex not acquired")
	}

	go func() {

		tick := time.Tick(time.Second)

		for {
			select {
			case <-m.quit:
				glog.Infof("[%s] quit signaled, releasing lock", m.key)
				_, err := m.client.CompareAndSwap(m.key, "released", m.ttl, "locked", m.index)
				if err != nil {

					if etcderr, ok := err.(*etcd.EtcdError); ok {
						switch etcderr.ErrorCode {
						case 100:
							// the key has expired or deleted by a third party,
							// pretty bad, but the we were about to release it
							// anyway.
							glog.Infof("[%s] no such key error when trying to release lock", m.key)
							break

						case 101:
							// either the prevValue or prevIndex arguments
							// failed to match the current data. Either someone
							// else has the lock now or the key was tampered
							// with and the mutex is now unusable. As long as
							// the TTL was not set to 0, it will become usable
							// again with time.
							glog.Infof("[%s] CAS failed when trying to release lock", m.key)
							break

						default:
							glog.Infof("[%s] unexpected error: %#v", m.key, etcderr)
						}
					}
				}

				m.index = 0
				m.state = released
				m.released <- true

				return

			case <-tick:
				glog.Infof("[%s] refreshing TTL", m.key)
				res, err := m.client.Update(m.key, "locked", m.ttl)
				if err != nil {

					glog.Infof("[%s] failed to refresh ttl (%#v)", m.key, err)
				} else {

					m.index = res.Node.ModifiedIndex
				}
			}
		}
	}()
}

func (m *EtcdMutex) Unlock() {

	if m.state != acquired {

		panic("etcd-sync: unlock of unlocked mutex")
	}

	if m.quit == nil {

		panic("etcd-sync: locked mutex missing its quit channel")
	}

	glog.Infof("[%s] Unlock called, sending quit signal", m.key)
	m.quit <- true

	<-m.released
	glog.Infof("[%s] lock released", m.key)
}

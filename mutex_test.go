package etcdsync

import (
	"flag"
	"testing"
	"time"

	"github.com/coreos/go-etcd/etcd"
)

var key string = "test/mutex"

func init() {
	flag.Parse()
}

func TestTwoNoKey(t *testing.T) {

	//etcd.SetLogger(log.New(os.Stderr, "", log.LstdFlags|log.Lshortfile))

	client := etcd.NewClient([]string{"http://127.0.0.1:4001"})
	client.Delete(key, true)

	quit1 := make(chan bool)
	quit2 := make(chan bool)

	progress := make(chan bool)

	// first thread
	go func() {

		mutex := NewMutexFromClient(client, key, 0)
		mutex.Lock()

		progress <- true

		// sleep for 5 seconds, ttl should be refreshed after 3 seconds
		time.Sleep(5 * time.Second)
		mutex.Unlock()

		quit1 <- true
	}()

	<-progress

	// second thread
	go func() {

		mutex := NewMutexFromClient(client, key, 0)
		//mutex := NewMutexFromServers([]string{"http://127.0.0.1:4001/"}, key, 0)
		// should take us 5 seconds to acquire the lock
		now := time.Now()

		mutex.Lock()
		timeToLock := time.Since(now)
		if timeToLock < 5*time.Second {

			t.Fatalf("mutex TTL was not refreshed, lock acquired after %v seconds", timeToLock)
		}

		mutex.Unlock()
		quit2 <- true
	}()

	<-quit1
	<-quit2
}

func TestTwoExistingKey(t *testing.T) {

	//etcd.SetLogger(log.New(os.Stderr, "", log.LstdFlags|log.Lshortfile))

	client := etcd.NewClient([]string{"http://127.0.0.1:4001"})
	client.Set(key, "released", 0)

	quit1 := make(chan bool)
	quit2 := make(chan bool)

	progress := make(chan bool)

	// first thread
	go func() {

		mutex := NewMutexFromServers([]string{"http://127.0.0.1:4001"}, key, 0)
		mutex.Lock()

		progress <- true

		// sleep for 5 seconds, ttl should be refreshed after 3 seconds
		time.Sleep(5 * time.Second)
		mutex.Unlock()

		quit1 <- true
	}()

	<-progress

	// second thread
	go func() {

		mutex := NewMutexFromClient(client, key, 0)
		//mutex := NewMutexFromServers([]string{"http://127.0.0.1:4001/"}, key, 0)
		// should take us 5 seconds to acquire the lock
		now := time.Now()

		mutex.Lock()
		timeToLock := time.Since(now)
		if timeToLock < 5*time.Second {

			t.Fatalf("mutex TTL was not refreshed, lock acquired after %v seconds", timeToLock)
		}

		mutex.Unlock()
		quit2 <- true
	}()

	<-quit1
	<-quit2
}

func TestUnlockReleased(t *testing.T) {

	//etcd.SetLogger(log.New(os.Stderr, "", log.LstdFlags|log.Lshortfile))

	client := etcd.NewClient([]string{"http://127.0.0.1:4001"})
	client.Delete(key, true)

	mutex := NewMutexFromClient(client, key, 0)

	defer func() {
		if msg := recover(); msg == nil {

			t.Fatalf("panic not initiated")
		}
	}()
	mutex.Unlock()
}

func TestUnlockNoKey(t *testing.T) {

	//etcd.SetLogger(log.New(os.Stderr, "", log.LstdFlags|log.Lshortfile))

	client := etcd.NewClient([]string{"http://127.0.0.1:4001"})
	client.Delete(key, true)

	mutex := NewMutexFromClient(client, key, 0)

	mutex.Lock()
	client.Delete(key, false)
	time.Sleep(2 * time.Second)
	mutex.Unlock()
}

func TestUnlockBadIndex(t *testing.T) {

	//etcd.SetLogger(log.New(os.Stderr, "", log.LstdFlags|log.Lshortfile))

	client := etcd.NewClient([]string{"http://127.0.0.1:4001"})
	client.Delete(key, true)

	mutex := NewMutexFromClient(client, key, 0)

	mutex.Lock()
	client.Update(key, "locked", 0)
	mutex.Unlock()

	trigger := make(chan bool)
	go func() {

		mutex.Lock()
		trigger <- true
	}()

	tick := time.Tick(time.Second)

	select {
	case <-trigger:
		t.Fatalf("managed to get a lock on an out of sync mutex")
		break
	case <-tick:
	}
}

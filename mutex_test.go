package etcdsync

import (
	"flag"
	"log"
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

func _TestUnlockBadIndex(t *testing.T) {

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
		mutex.Unlock()
	}()

	tick := time.Tick(time.Second)

	select {
	case <-trigger:
		t.Fatalf("managed to get a lock on an out of sync mutex")
		break
	case <-tick:
		// release the blocked goroutine
		client.Delete(key, true)
	}
}

func HammerMutex(m *EtcdMutex, loops int, cdone chan bool, t *testing.T) {
	log.Printf("starting %d iterations", loops)
	for i := 0; i < loops; i++ {
		m.Lock()
		m.Unlock()
	}
	log.Printf("completed all iterations")
	cdone <- true
}

func TestMutex(t *testing.T) {
	client := etcd.NewClient([]string{"http://127.0.0.1:4001"})
	client.Delete(key, true)

	m := NewMutexFromClient(client, key, 0)
	c := make(chan bool)
	for i := 0; i < 10; i++ {
		go HammerMutex(m, 1000, c, t)
	}
	for i := 0; i < 10; i++ {
		<-c
	}
}

func TestMutexPanic(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("unlock of unlocked mutex did not panic")
		}
	}()

	client := etcd.NewClient([]string{"http://127.0.0.1:4001"})
	client.Delete(key, true)

	mu := NewMutexFromClient(client, key, 0)
	mu.Lock()
	mu.Unlock()
	mu.Unlock()
}

func BenchmarkMutexUncontended(b *testing.B) {
	type PaddedMutex struct {
		*EtcdMutex
		pad [128]uint8
	}

	client := etcd.NewClient([]string{"http://127.0.0.1:4001"})
	client.Delete(key, true)

	b.RunParallel(func(pb *testing.PB) {
		mu := PaddedMutex{EtcdMutex: NewMutexFromClient(client, key, 0)}

		for pb.Next() {
			mu.Lock()
			mu.Unlock()
		}
	})
}

func benchmarkMutex(b *testing.B, slack, work bool) {
	client := etcd.NewClient([]string{"http://127.0.0.1:4001"})
	client.Delete(key, true)

	mu := NewMutexFromClient(client, key, 0)
	if slack {
		b.SetParallelism(10)
	}
	b.RunParallel(func(pb *testing.PB) {
		foo := 0
		for pb.Next() {
			mu.Lock()
			mu.Unlock()
			if work {
				for i := 0; i < 100; i++ {
					foo *= 2
					foo /= 2
				}
			}
		}
		_ = foo
	})
}

func BenchmarkMutex(b *testing.B) {
	benchmarkMutex(b, false, false)
}

func BenchmarkMutexSlack(b *testing.B) {
	benchmarkMutex(b, true, false)
}

func BenchmarkMutexWork(b *testing.B) {
	benchmarkMutex(b, false, true)
}

func BenchmarkMutexWorkSlack(b *testing.B) {
	benchmarkMutex(b, true, true)
}

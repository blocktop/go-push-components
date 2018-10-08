package push_test

import (
	"testing"
	"time"

	. "github.com/blocktop/go-push-components"
)

func BenchmarkPush(b *testing.B) {
	q := NewPushQueue(2, b.N/2+1, worker)
	q.OnOverload(func(interface{}) {
		panic("overload")
	})
	q.Start()
	for i := 0; i < b.N; i++ {
		q.Put(i)
		time.Sleep(5 * time.Millisecond)
	}
	done := make(chan bool)
	q.OnDrained(func() {
		done <- true
	})
	q.Drain()
	<-done
}

func worker(i interface{}) {
	count := 1e6
	for count > 0 {
		count--
	}
}

func BenchmarkChan(b *testing.B) {
	C := make(chan int, b.N/2+1)
	done := make(chan bool, 1)
	receiver(C, done)
	for i := 0; i < b.N; i++ {
		C <- i
		time.Sleep(5 * time.Millisecond)
	}
	done <- true
}

func TestChan(t *testing.T) {
	C := make(chan int, 500)
	done := make(chan bool, 1)
	receiver(C, done)
	for i := 0; i < 1000; i++ {
		C <- i
		time.Sleep(5 * time.Millisecond)
	}
	done <- true
}

func receiver(C <-chan int, done <-chan bool) {
	// make worker pool
	work := make(chan int, 1000)
	workDone1 := make(chan bool, 1)
	workDone2 := make(chan bool, 1)
	go workerChan(work, workDone1)
	go workerChan(work, workDone2)

	go func() {
		for {
			select {
			case <-done:
				workDone1 <- true
				workDone2 <- true
				return
			case i := <-C:
				work <- i
			}
		}
	}()
}

func workerChan(input <-chan int, done <-chan bool) {
	for {
		select {
		case <-done:
			return
		case i := <-input:
			worker(i)
		}
	}
}

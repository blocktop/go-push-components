package push

import (
	"sync"
)

// PushQueue holds the processing and state information
// of a PushQueue.
type PushQueue struct {
	worker               PushQueueWorker
	concurrency          int
	availableWorkers     int
	depth                int
	items                []QueueItem
	started              bool
	draining             bool
	overload             int
	dropOldestOnOverload bool
	onOverload           func(QueueItem)
	onFirstOverload      func(QueueItem)
	onDrained            func()
	mutex                sync.Mutex
}

type QueueItem interface{}

// PushQueuePut provides an interface that can be passed
// to clients to work with the queue. It contains only the
// methods required by a client.
type PushQueuePut interface {
	Put(QueueItem)
	Depth() int
	Count() int
	IsStarted() bool
}

// compile-time check that interface is satisfied
var _ PushQueuePut = (*PushQueue)(nil)

// PushQueueWorker defines the function that will be called
// to process a queue item.
type PushQueueWorker func(item QueueItem)

// NewPushQueue creates a new PushQueue with the given concurrency,
// depth and worker. The worker is the function that will be called
// to process a queue item. The concurrency is the number of times the
// worker function will be called in parallel. The depth is the
// maximum capacity of the queue.
func NewPushQueue(concurrency int, depth int, worker PushQueueWorker) *PushQueue {
	if concurrency < 1 {
		panic("concurrency must greater than 0")
	}
	if depth < 1 {
		panic("depth must be greater than 0")
	}

	q := &PushQueue{
		concurrency:      concurrency,
		availableWorkers: concurrency,
		depth:            depth,
		items:            make([]QueueItem, 0, depth),
		worker:           worker}

	return q
}

// Start begins queue processing. Start panics if no worker
// has been set.
func (q *PushQueue) Start() {
	if q.worker == nil {
		panic("no worker set")
	}
	q.started = true
	q.draining = false
	q.overload = 0
}

// IsStarted indicates whether the queue is started. This method
// returns true when the queue is available to clients to Put
// items. IsStarted returns false when the queue is draining.
func (q *PushQueue) IsStarted() bool {
	return q.started
}

// Stop ends processing of queue items. This also ends
// draining of items if Drain has been called.
func (q *PushQueue) Stop() {
	q.started = false
	q.draining = false
}

// Drain processes remaining items in the queue and prevents
// new items from being put onto the queue.
func (q *PushQueue) Drain() {
	q.draining = true
	q.started = false
	if q.Count() == 0 && q.availableWorkers == q.concurrency {
		q.setDrained()
	}
}

// OnDrained sets an event handler that will be called when
// the draining is complete.
func (q *PushQueue) OnDrained(f func()) {
	q.onDrained = f
}

// Empty removes all items currently in the queue. This method
// does not affect the started, stopped, or draining state of the
// queue.
func (q *PushQueue) Empty() {
	q.mutex.Lock()
	q.items = make([]QueueItem, 0, q.Depth())
	q.mutex.Unlock()
}

// IsFull indicates whether the queue can accept new items.
func (q *PushQueue) IsFull() bool {
	return q.Count() >= q.Depth()
}

// Count returns the current number of items in the queue.
func (q *PushQueue) Count() int {
	return len(q.items)
}

// Depth returns the maximum capacity of the queue.
func (q *PushQueue) Depth() int {
	return q.depth
}

// DropOldestOnOverload tells the queue to drop the oldest item
// in the queue on the floor when an overload occurs. The default
// behavior is to drop the item being added.
func (q *PushQueue) DropOldestOnOverload() {
	q.dropOldestOnOverload = true
}

// OverloadCount returns the number of times that clients attempted
// to Put items exceeding queue depth or while the queue was
// draining. The exceeding items were dropped on the floor. This
// count is reset when Start is called.
func (q *PushQueue) OverloadCount() int {
	return q.overload
}

// OnOverload sets an event handler that will be called *every
// time* a client attempts to overload the queue. The handler
// is passed the value of the Overload register.
func (q *PushQueue) OnOverload(f func(QueueItem)) {
	q.onOverload = f
}

// OnFirstOverload sets an event handler that will be called the first
// time a client attempts to overload the queue.
func (q *PushQueue) OnFirstOverload(f func(QueueItem)) {
	q.onFirstOverload = f
}

// Put adds an item to the queue for processing. If the count
// of items in the queue is at the queue depth, then
// the Overload flag is set and the item is dropped on the floor.
func (q *PushQueue) Put(item QueueItem) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if q.Count() >= q.Depth() || q.draining {
		var dropItem QueueItem
		if q.dropOldestOnOverload {
			dropItem = q.items[:1][0]
			q.items = append(q.items[1:], item)
			go q.get()
		} else {
			dropItem = item
		}
		q.overload++
		if q.onOverload != nil {
			q.onOverload(dropItem)
		}
		if q.overload == 1 && q.onFirstOverload != nil {
			q.onFirstOverload(dropItem)
		}
		return
	}

	q.items = append(q.items, item)
	go q.get()
}

func (q *PushQueue) readyToWork() bool {
	return (q.started || q.draining) &&
		q.availableWorkers > 0 &&
		len(q.items) > 0
}

func (q *PushQueue) get() {
	if !q.readyToWork() {
		return
	}

	q.mutex.Lock()

	if !q.readyToWork() {
		q.mutex.Unlock()
		return
	}

	q.availableWorkers--
	item := q.items[:1][0]
	q.items = q.items[1:]

	q.mutex.Unlock()

	q.doWork(item)

	if !q.draining {
		go q.get()
	}
}

func (q *PushQueue) doWork(item QueueItem) {

	done := make(chan bool)
	go func() {
		q.worker(item)
		done <- true
	}()
	<-done

	q.workerCompleted()
}

func (q *PushQueue) workerCompleted() {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if q.availableWorkers < q.concurrency {
		q.availableWorkers++
	}

	if q.availableWorkers == q.concurrency && len(q.items) == 0 {
		if q.draining {
			// final worker has completed
			q.setDrained()
			return
		}
	}

	go q.get()
}

func (q *PushQueue) setDrained() {
	if q.onDrained != nil {
		go q.onDrained()
	}
	q.draining = false
}

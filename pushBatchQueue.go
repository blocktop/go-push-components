package push

import (
	"sync"
)

// PushBatchQueue holds the processing and state information
// of a PushBatchQueue.
type PushBatchQueue struct {
	worker               func([]interface{})
	concurrency          int
	batchSize            int
	availableWorkers     int
	depth                int
	items                []interface{}
	started              bool
	draining             bool
	overload             int
	dropOldestOnOverload bool
	onOverload           func(interface{})
	onFirstOverload      func(interface{})
	onDrained            func()
	mutex                sync.Mutex
}

// compile-time check that interface is satisfied
var _ PushQueuePut = (*PushBatchQueue)(nil)

// NewPushBatchQueue creates a new PushBatchQueue with the given concurrency,
// depth and worker. The worker is the function that will be called
// to process a queue item. The concurrency is the number of times the
// worker function will be called in parallel. The depth is the
// maximum capacity of the queue.
func NewPushBatchQueue(concurrency int, depth int, batchSize int, worker func([]interface{})) *PushBatchQueue {
	if concurrency < 1 {
		panic("concurrency must greater than 0")
	}
	if depth < 1 {
		panic("depth must be greater than 0")
	}
	if batchSize < 1 {
		panic("batch size must be greater than 0")
	}

	q := &PushBatchQueue{
		concurrency:      concurrency,
		availableWorkers: concurrency,
		depth:            depth,
		batchSize:        batchSize,
		items:            make([]interface{}, 0, depth),
		worker:           worker}

	return q
}

// Start begins queue processing. Start panics if no worker
// has been set.
func (q *PushBatchQueue) Start() {
	if q.worker == nil {
		panic("no worker set")
	}
	q.started = true
	q.draining = false
	q.overload = 0
	go q.get()
}

// IsStarted indicates whether the queue is started. This method
// returns true when the queue is available to clients to Put
// items. IsStarted returns false when the queue is draining.
func (q *PushBatchQueue) IsStarted() bool {
	return q.started
}

// Stop ends processing of queue items. This also ends
// draining of items if Drain has been called.
func (q *PushBatchQueue) Stop() {
	q.started = false
	q.draining = false
}

// Drain processes remaining items in the queue and prevents
// new items from being put onto the queue.
func (q *PushBatchQueue) Drain() {
	q.draining = true
	q.started = false
	if q.Count() == 0 && q.availableWorkers == q.concurrency {
		q.setDrained()
	}
	go q.get()
}

// OnDrained sets an event handler that will be called when
// the draining is complete.
func (q *PushBatchQueue) OnDrained(f func()) {
	q.onDrained = f
}

// Empty removes all items currently in the queue. This method
// does not affect the started, stopped, or draining state of the
// queue.
func (q *PushBatchQueue) Empty() {
	q.mutex.Lock()
	q.items = make([]interface{}, 0, q.Depth())
	q.mutex.Unlock()
}

// IsFull indicates whether the queue can accept new items.
func (q *PushBatchQueue) IsFull() bool {
	return q.Count() >= q.Depth()
}

// Count returns the current number of items in the queue.
func (q *PushBatchQueue) Count() int {
	return len(q.items)
}

// Depth returns the maximum capacity of the queue.
func (q *PushBatchQueue) Depth() int {
	return q.depth
}

// DropOldestOnOverload tells the queue to drop the oldest item
// in the queue on the floor when an overload occurs. The default
// behavior is to drop the item being added.
func (q *PushBatchQueue) DropOldestOnOverload() {
	q.dropOldestOnOverload = true
}

// OverloadCount returns the number of times that clients attempted
// to Put items exceeding queue depth or while the queue was
// draining. The exceeding items were dropped on the floor. This
// count is reset when Start is called.
func (q *PushBatchQueue) OverloadCount() int {
	return q.overload
}

// OnOverload sets an event handler that will be called *every
// time* a client attempts to overload the queue. The handler
// is passed the value of the Overload register.
func (q *PushBatchQueue) OnOverload(f func(interface{})) {
	q.onOverload = f
}

// OnFirstOverload sets an event handler that will be called the first
// time a client attempts to overload the queue.
func (q *PushBatchQueue) OnFirstOverload(f func(interface{})) {
	q.onFirstOverload = f
}

// Put adds an item to the queue for processing. If the count
// of items in the queue is at the queue depth, then
// the Overload flag is set and the item is dropped on the floor.
func (q *PushBatchQueue) Put(item interface{}) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if q.Count() >= q.Depth() || q.draining {
		var dropItem interface{}
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

func (q *PushBatchQueue) readyToWork() bool {
	return (q.started || q.draining) &&
		q.availableWorkers > 0 &&
		len(q.items) > 0
}

func (q *PushBatchQueue) get() {
	if !q.readyToWork() {
		return
	}

	q.mutex.Lock()

	if !q.readyToWork() {
		q.mutex.Unlock()
		return
	}

	q.availableWorkers--

	lastIndex := q.batchSize
	if len(q.items) < lastIndex {
		lastIndex = len(q.items)
	}

	batch := q.items[:lastIndex]
	q.items = q.items[lastIndex:]

	q.mutex.Unlock()

	q.doWork(batch)

	if !q.draining {
		go q.get()
	}
}

func (q *PushBatchQueue) doWork(batch []interface{}) {

	done := make(chan bool)
	go func() {
		q.worker(batch)
		done <- true
	}()
	<-done

	q.workerCompleted()
}

func (q *PushBatchQueue) workerCompleted() {
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

func (q *PushBatchQueue) setDrained() {
	if q.onDrained != nil {
		go q.onDrained()
	}
	q.draining = false
}

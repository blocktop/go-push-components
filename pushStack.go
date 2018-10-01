package push

import (
	"sync"
)

// PushStack holds the processing and state information
// of a PushStack.
type PushStack struct {
	worker           PushStackWorker
	concurrency      int
	availableWorkers int
	height           int
	items            []StackItem
	started          bool
	draining         bool
	overload         int
	onOverload       func(StackItem)
	onFirstOverload  func(StackItem)
	onDrained        func()
	mutex            sync.Mutex
}

type StackItem interface{}

// PushStackPut provides an interface that can be passed
// to clients to work with the stack. It contains only the
// methods required by a client.
type PushStackPut interface {
	Push(StackItem)
	Height() int
	Count() int
	IsStarted() bool
}

// compile-time check that interface is satisfied
var _ PushStackPut = (*PushStack)(nil)

// PushStackWorker defines the function that will be called
// to process a stack item.
type PushStackWorker func(item StackItem)

// NewPushStack creates a new PushStack with the given concurrency,
// height and worker. The worker is the function that will be called
// to process a stack item. The concurrency is the number of times the
// worker function will be called in parallel. The height is the
// maximum capacity of the stack.
func NewPushStack(concurrency int, height int, worker PushStackWorker) *PushStack {
	if concurrency < 1 {
		panic("concurrency must greater than 0")
	}
	if height < 1 {
		panic("height must be greater than 0")
	}

	s := &PushStack{
		concurrency:      concurrency,
		availableWorkers: concurrency,
		height:           height,
		items:            make([]StackItem, 0, height),
		worker:           worker}

	return s
}

// Start begins stack processing. Start panics if no worker
// has been set.
func (s *PushStack) Start() {
	if s.worker == nil {
		panic("no worker set")
	}
	s.started = true
	s.draining = false
	s.overload = 0
}

// IsStarted indicates whether the stack is started. This method
// returns true when the stack is available to clients to Put
// items. IsStarted returns false when the stack is draining.
func (s *PushStack) IsStarted() bool {
	return s.started
}

// Stop ends processing of stack items. This also ends
// draining of items if Drain has been called.
func (s *PushStack) Stop() {
	s.started = false
	s.draining = false
}

// Drain processes remaining items in the stack and prevents
// new items from being put onto the stack.
func (s *PushStack) Drain() {
	s.draining = true
	s.started = false
	if s.Count() == 0 && s.availableWorkers == s.concurrency {
		// already drained
		s.setDrained()
	}
}

// OnDrained sets an event handler that will be called when
// the draining is complete.
func (s *PushStack) OnDrained(f func()) {
	s.onDrained = f
}

// Empty removes all items currently in the stack. This method
// does not affect the started, stopped, or draining state of the
// stack.
func (s *PushStack) Empty() {
	s.mutex.Lock()
	s.items = make([]StackItem, 0, s.Height())
	s.mutex.Unlock()
}

// IsFull indicates whether the stack can accept new items.
func (s *PushStack) IsFull() bool {
	return s.Count() >= s.Height()
}

// Count returns the current number of items in the stack.
func (s *PushStack) Count() int {
	return len(s.items)
}

// Height returns the maximum capacity of the stack.
func (s *PushStack) Height() int {
	return s.height
}

// Overload returns the number of times that clients attempted
// to Put items exceeding stack height or while the stack was
// draining. The exceeding items were dropped on the floor. This
// count is reset when Start is called.
func (s *PushStack) Overload() int {
	return s.overload
}

// OnOverload sets an event handler that will be called *every
// time* a client attempts to overload the stack. The handler
// is passed the value of the Overload register.
func (s *PushStack) OnOverload(f func(StackItem)) {
	s.onOverload = f
}

// OnFirstOverload sets an event handler that will be called the first
// time a client attempts to overload the stack.
func (s *PushStack) OnFirstOverload(f func(StackItem)) {
	s.onFirstOverload = f
}

// Push adds an item to the stack for processing. If the count
// of items in the stack is at the stack height, then
// the Overload flag is set and the first item added is dropped
// on the floor. The dropped item is sent to the OnOverload
// and OnFirstOverload (if this is the first time) event
// handlers.
func (s *PushStack) Push(item StackItem) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.Count() >= s.Height() || s.draining {
		firstItem := s.items[:1]
		s.items = append(s.items[1:], item)
		go s.pop()

		s.overload++
		if s.onOverload != nil {
			s.onOverload(firstItem)
		}
		if s.overload == 1 && s.onFirstOverload != nil {
			s.onFirstOverload(firstItem)
		}
		return
	}

	s.items = append(s.items, item)
	go s.pop()
}

func (s *PushStack) readyToWork() bool {
	return (s.started || s.draining) &&
		s.availableWorkers > 0 &&
		len(s.items) > 0
}

func (s *PushStack) pop() {
	if !s.readyToWork() {
		return
	}

	s.mutex.Lock()

	if !s.readyToWork() {
		s.mutex.Unlock()
		return
	}

	s.availableWorkers--
	lastIndex := len(s.items) - 1
	item := s.items[lastIndex:][0]
	s.items = s.items[:lastIndex]

	s.mutex.Unlock()

	s.doWork(item)
	go s.worker(item)

	if !s.draining {
		go s.pop()
	}
}

func (s *PushStack) doWork(item StackItem) {
	done := make(chan bool)
	go func() {
		s.worker(item)
		done <- true
	}()
	<-done

	s.workerCompleted()
}

func (s *PushStack) workerCompleted() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.availableWorkers < s.concurrency {
		s.availableWorkers++
	}

	if s.draining && s.availableWorkers == s.concurrency && len(s.items) == 0 {
		// final worker has completed
		s.setDrained()
		return
	}

	go s.pop()
}

func (s *PushStack) setDrained() {
	if s.onDrained != nil {
		go s.onDrained()
	}
	s.draining = false
}

// Package push provides components that can replace the need for
// chan loop and select constructs. These components require no
// polling and are instead designed to "push" new work to the
// client when available.
// 
// The components provide easy concurrency and buffer size 
// monitoring and eventing.
//
// Example
//
//	  q := push.NewPushQueue(2, 50, worker)
//    q.Start()
//    
//    func worker(item Item) {
//        doWorkWithItem(item)
//    }
//
// In the above example, the worker will be called as a goroutine
// at most two times concurrently.
//
// Events
//
// The following events are provided for client programs to respond
// to what is happening in the push component.
// * Overload(Item) -- fired if the client attempts to write more
// items into the push component than its configured buffer size.
// The items will not be added to the component's buffer. However, they
// will be sent to the client's Overload event handler.
//
// * FirstOverload(Item) -- same as Overload except that it happens
// only on the first occurance.
// 
// * Drained -- fired when the push component finishes processing all
// items after the client calls the Drain method. This signals to
// the client that (for example) the program may be safely without
// interrupting processing.
//
// An example of the client's program blocking until all items 
// are finished processing:
//		
//	   done := make(chan bool, 1)
//		 q.OnDrained(func() {
//		     done <- true
//     })
//     q.Drain()
//     <-done
//
//     os.Exit(0) // etc..
package push
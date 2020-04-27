package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	// IMPT: All code written here was not tested because it does
	// not run properly on Windows. Will get a server from NU asap
	// All this rn is just theory on the correct code but... no guarantees lol
	// Start of Jason's code

	// create a waitgroup to wait for all goroutines to finish
	var wg sync.WaitGroup

	// for each task in task list
	for i := 0; i < ntasks; i++ {
		// get current worker's RPC. Blocks if no workers in channel
		rpc := <-registerChan

		// Increment wait group by 1 for new rpc added
		wg.Add(1)

		// create new DoTaskArgs for current worker
		args := DoTaskArgs{jobName, mapFiles[i], phase, i, n_other}

		// call() sends an RPC to the rpcname handler on server srv
		// with arguments args, waits for the reply, and leaves the
		// reply in reply. the reply argument should be the address
		// of a reply structure.

		// call() returns true if the server responded, and false if call()
		// received no reply from the server. reply's contents are valid if
		// and only if call() returned true.

		// 'go' statemtn for concurrency
		go func() {
			for {
				// IMPT: reply is currently assigned to a string primitive
				var reply ShutdownReply
				ok := call(rpc, "Worker.DoTask", args, &reply)
				if ok {
					// decrease wg counter if reply received
					wg.Done()

					// send worker back onto registerChan so we can
					// give it another task (Not sure if this is legit)
					registerChan <- rpc

					//loop breaks if valid reply received
					break
				} else {
					rpc = <-registerChan
				}
			}
		}()
	}

	// Block until waitgroup counter goes to 0: all workers are finished
	wg.Wait()

	// End of Jason's Code

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part 2, 2B).
	//
	fmt.Printf("Schedule: %v done\n", phase)
}

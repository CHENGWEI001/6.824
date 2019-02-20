package mapreduce

import (
	"fmt"
	"log"
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

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	// below code are coming from : https://github.com/sunhay/mit-6.824-2017/blob/b75b5405935e78bfce4475111c8a1a10eb9cd262/mapreduce/schedule.go
	var waitGroup sync.WaitGroup

	// Ready worker channel. Buffer size = number of tasks
	readyChan := make(chan string, ntasks)

	// Helper to start task
	startTask := func(worker string, args *DoTaskArgs) {
		defer waitGroup.Done()
		result := call(worker, "Worker.DoTask", args, nil)
		readyChan <- worker
		log.Printf("[%s, %s Scheduler] Task %d finished with result %t\n", jobName, phase, args.TaskNumber, result)
	}

	// Assign tasks to registered + available workers
	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)
	for currentTask := 0; currentTask < ntasks; {
		select {
		case worker := <-registerChan: // New worker registering, set them as available
			readyChan <- worker
		case worker := <-readyChan: // Worker is ready to start another task
			args := DoTaskArgs{
				JobName:       jobName,
				File:          mapFiles[currentTask], // Ignored for reduce phase
				Phase:         phase,
				TaskNumber:    currentTask,
				NumOtherPhase: n_other,
			}
			waitGroup.Add(1)
			currentTask++
			go startTask(worker, &args)
		}
	}

	waitGroup.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}

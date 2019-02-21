package mapreduce

import (
	"fmt"
	"log"
	// "sync"
)

// below are type I define to help on task scheduling
type workerStatus_t struct {
	arg  *DoTaskArgs
	succ bool
	wk   string
}

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

	// init variable for task scheduling:
	// 1) statusChan: status report from worker thread
	// 2) taskQ: taskQ is managed by master only
	// 3) startTask function literal
	// 4) completedTaskNum
	statusChan := make(chan workerStatus_t)
	taskQ := []*DoTaskArgs{}
	for i := 0; i < ntasks; i++ {
		file := ""
		if phase == mapPhase {
			file = mapFiles[i]
		}
		taskQ = append(taskQ, &DoTaskArgs{
			JobName:       jobName,
			File:          file,
			Phase:         phase,
			TaskNumber:    i,
			NumOtherPhase: n_other,
		})
	}
	startTask := func(arg *DoTaskArgs, wk string, statusChan chan workerStatus_t) {
		success := call(wk, "Worker.DoTask", arg, nil)
		log.Printf("[%s, %s Scheduler] Task %d finished with result %t\n", jobName, phase, arg.TaskNumber, success)
		statusChan <- workerStatus_t{
			arg:  arg,
			succ: success,
			wk:   wk,
		}
	}
	completedTaskNum := 0

	// for loop until all task complete
	// - the idea is to check if all task completed.
	// - note if the worker fail (report fail status), then we should not give
	//   this fail worker anymore task
	for completedTaskNum < ntasks {
		wk := ""
		select {
		case wk = <-registerChan:
		case st := <-statusChan:
			if !st.succ {
				taskQ = append(taskQ, st.arg)
			} else {
				completedTaskNum++
				wk = st.wk
			}
		}
		// only schedule task if we have valid worker and pending task
		if wk != "" && len(taskQ) > 0 {
			var t *DoTaskArgs
			t, taskQ = taskQ[0], taskQ[1:]
			go startTask(t, wk, statusChan)
		}
	}

	fmt.Printf("Schedule: %v done\n", phase)
}

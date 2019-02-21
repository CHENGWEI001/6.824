package mapreduce

import (
	"encoding/json"
	"fmt"
	// "io"
	// "io/ioutil"
	"log"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	fmt.Printf("===============================\n")
	fmt.Printf("doReduce():\n")
	fmt.Printf("- reduceTask:%v\n", reduceTask)
	fmt.Printf("- nMap:%v\n", nMap)
	fmt.Printf("- outFile:%v\n", outFile)

	// Open M files, combine and sort them base on key
	fos := [](*os.File){}
	var kvs []KeyValue
	var kv KeyValue
	for m := 0; m < nMap; m++ {
		f, err := os.Open(fmt.Sprintf("mrtmp.%v-%v-%v", jobName, m, reduceTask))
		if err != nil {
			log.Fatal(err)
		}
		fos = append(fos, f)

		dec := json.NewDecoder(fos[m])
		for dec.More() {
			err = dec.Decode(&kv)
			if err != nil {
				log.Fatal(err)
			}
			kvs = append(kvs, kv)
		}
	}

	// sort them base on key
	sort.Stable(&KeyValueSorter{
		keyValues: kvs,
	})

	// open output file and write them after reducer
	f, err := os.Create(outFile)
	if err != nil {
		log.Fatal(err)
	}
	enc := json.NewEncoder(f)

	ss := []string{}
	for i, kv := range kvs {
		ss = append(ss, kv.Value)
		if (i+1) == len(kvs) || kvs[i].Key != kvs[i+1].Key {
			enc.Encode(KeyValue{kv.Key, reduceF(kv.Key, ss)})
			ss = nil
		}
	}
	// close the output file
	defer func() {
		err := f.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()

	// close M input file per R at the end
	defer func() {
		for _, f := range fos {
			err := f.Close()
			if err != nil {
				log.Fatal(err)
			}
		}
	}()
	fmt.Printf("===============================\n")
}

// helper for sorting KeyValue type base on key value
type KeyValueSorter struct {
	keyValues []KeyValue
}

func (ks *KeyValueSorter) Len() int {
	return len(ks.keyValues)
}

func (ks *KeyValueSorter) Less(i, j int) bool {
	return ks.keyValues[i].Key < ks.keyValues[j].Key
}

func (ks *KeyValueSorter) Swap(i, j int) {
	ks.keyValues[i], ks.keyValues[j] = ks.keyValues[j], ks.keyValues[i]
}

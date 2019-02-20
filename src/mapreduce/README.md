# mapreduce
https://pdos.csail.mit.edu/6.824/labs/lab-1.html
## basic steps:
```
$ cd 6.824
$ export "GOPATH=$PWD"  # go needs $GOPATH to be set to the project's working directory
$ cd "$GOPATH/src/mapreduce"
$ go test -run Sequential
ok  	mapreduce	2.694s
```

## note
*  doMap save intermediate file into format M_R.json (ex: jobname is xxx, Map task idx = 0 , Reduce task idx = 0, the intermediate file name is mrtmp.xxx-0-0)
* doReduce
```
- open
```

## Reference
* https://github.com/sunhay/mit-6.824-2017
* https://stackoverflow.com/questions/1821811/how-to-read-write-from-to-file-using-go
* https://golang.org/pkg/os/#Create
* https://golang.org/pkg/encoding/json/#NewEncoder
* https://golang.org/src/encoding/json/example_test.go
* https://golang.org/pkg/sort/#Sort
* https://www.calhoun.io/concatenating-and-building-strings-in-go/
* https://stackoverflow.com/questions/48978414/golang-strings-builder-type-undefined
* https://yourbasic.org/golang/convert-string-to-rune-slice/
* https://golang.org/pkg/net/rpc/#Client.Call
* https://golang.org/pkg/sync/#WaitGroup
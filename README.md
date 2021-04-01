# Project Overview
The mapreduce package reveals a simple MapReduce library typically started by calling Distributed() (in master.go) or Sequential() (also master.go). The sequential mode executes map and reduce jobs one at a time which, while slower, is useful for understanding the project. The distributed version runs jobs in parallel, first the map tasks and then reduce ones.

A job is executed in the following order:

A number of input files are provided to the application as well as two functions (map and reduce) and a number of reduce tasks (nReduce).
The application uses this knowledge to create a master. This, in turn, starts an RPC server (in master_rpc.go) and waits for workers to register (RPC call Register() from master.go). Upon availability, tasks are assigned to workers with schedule() (schedule.go).
Each input file is considered by the master to be one map task, calling doMap() (common_map.go) at least once for each map task. It does so either directly (when proceeding sequentially) or by issuing the DoTask RPC to a worker (worker.go). Each doMap() call reads the appropriate file, executes the map function on the contents, and writes the key/value pairs to nReduce intermediate files. The keys are hashed in order to pick the intermediate file and thus the reduce task that will process the key. In the end, there will be nMap x nReduce files. Each file name consists of a prefix, map task number, and reduce task number. Workers must be able to read files written by any other worker in addition to the input files. In the real world, a distributed storage system such as GFS is used, however in this lab, all the workers are on the same machine.
The master then calls doReduce() (common_reduce.go) at least once for each reduce task. As with doMap(), it does so either directly or through a worker. For reduce task r, doReduce() collects the r’th intermediate file for each map task and calls the reduce function for each key that appears in those files. The reduce tasks produce nReduce result files.
The master calls mr.merge() (master_splitmerge.go) to merge the nReduce result files into a single output.
Finally, the master sends a Shutdown RPC to each of its workers before it shuts down its own RPC server.

# Part 1.A. Sequential MapReduce
To run tests:

cd MAP_REDUCE_DIR
export "GOPATH=$PWD"
cd src/mapreduce
go test -run Sequential

# Part 1.B. Test MapReduce with WordCounter example
For this project, a word is any contiguous sequence of letters, as determined by unicode.IsLetter.

There are input files with pathnames of the form pg-*.txt in "src/main" taken from Project Gutenberg. Here’s how to run wc with the input files:

cd MAP_REDUCE_DIR
export "GOPATH=$PWD"
cd src/main
go run wc.go master sequential pg-huckleberry_finn.txt

sh test-wc.sh

# Part 2.A. Distributed MapReduce
To test distributed MapReduce implementation:

go test -run TestParallel

# Part 2.B. Handling worker failures
To run these tests:

go test -run Failure

package mapreduce

import (
    "os"
    "sort"
    "encoding/json"
)

type mySortInter []KeyValue

// 只要实现了这三个接口，就可以调用sort.Sort()
// 当然，如果要排列的是简单的字符串，那可以直接sort.Strings(strarray)
func (a mySortInter) Len() int { return len(a) }
func (a mySortInter) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a mySortInter) Less(i, j int) bool { return a[i].Key < a[j].Key }
// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
    keyvalues := make(map[string][]string)
    for m := 0; m < nMap; m += 1{
        thisFile, _ := os.Open(reduceName(jobName, m, reduceTaskNumber))
        thisDecoder := json.NewDecoder(thisFile)
        var kv KeyValue;
        for{
            err := thisDecoder.Decode(&kv)
            if err != nil{
                break
            }
            keyvalues[kv.Key] = append(keyvalues[kv.Key], kv.Value)
        }
        thisFile.Close()
    }
    var keyrsts []KeyValue
    for key, values := range(keyvalues){
        keyrsts = append(keyrsts, KeyValue{key, reduceF(key, values)})
    }
    sort.Sort(mySortInter(keyrsts))
    rstFile, _ := os.Create(outFile)
    enc := json.NewEncoder(rstFile)
    // 特别要小心。。。
    // 如果不用两个变量，那得到的是下标，不是元素。。。
    // 编译器不会报错
    for _, kv := range(keyrsts){
        enc.Encode(kv)
    }
    rstFile.Close()


	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
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
}

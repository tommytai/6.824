package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
)

func doMap(
	jobName string, // the name of the MapReduce job
	mapTask int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(filename string, contents string) []KeyValue,
) {
	//
	// Your code here (Part I).
	//
	content, err := ioutil.ReadFile(inFile)
	if err != nil {
		log.Fatalf("There was an issue reading the file. [file=%s] [error=%s]", inFile, err)
	}

	keyValueList := mapF(inFile, string(content))
	partitions := make([]*os.File, nReduce)

	for i := 0; i < nReduce; i++ {
		filename := reduceName(jobName, mapTask, i)
		fd, err := os.Create(filename)
		if err != nil {
			log.Fatalf("There was an issue creating the file. [filename=%s] [error=%s]", filename, err)
		}
		defer fd.Close()
		partitions[i] = fd
	}

	for _, keyValue := range keyValueList {
		taskIndex := ihash(keyValue.Key) % nReduce
		fd := partitions[taskIndex]
		enc := json.NewEncoder(fd)
		err := enc.Encode(&keyValue)
		if err != nil {
			log.Fatalf("There was an issue encoding [KeyValue=%s] with the file", keyValue)
		}
	}
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}

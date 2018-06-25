package mapreduce

import (
	"encoding/json"
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
	// Your code here (Part I).
	//
	keyValueMap := decodeStep(jobName, reduceTask, nMap)
	keys := performSort(keyValueMap)

	outFd, err := os.Create(outFile)
	if err != nil {
		log.Fatalf("There was an issue creating the out file. [file=%s] [error=%s]", outFile, err)
	}
	defer outFd.Close()
	enc := json.NewEncoder(outFd)
	for _, key := range keys {
		err := enc.Encode(KeyValue{key, reduceF(key, keyValueMap[key])})
		if err != nil {
			log.Fatalf("There was an issue encoding")
		}
	}
}

func decodeStep(jobName string, reduceTask int, nMap int) map[string][]string {
	keyValueMap := make(map[string][]string, nMap)
	for i := 0; i < nMap; i++ {
		intermediateFileName := reduceName(jobName, i, reduceTask)
		fd, err := os.OpenFile(intermediateFileName, os.O_RDWR, 0600)
		if err != nil {
			log.Fatalf("There was an issue reading the file. [filename=%s] [error=%s]", intermediateFileName, err)
		}
		defer fd.Close()

		var kv *KeyValue
		decoder := json.NewDecoder(fd)
		for {
			err := decoder.Decode(&kv)
			if err != nil {
				break
			}
			keyValueMap[kv.Key] = append(keyValueMap[kv.Key], kv.Value)
		}
	}
	return keyValueMap
}

func performSort(keyValueMap map[string][]string) []string {
	var keys []string
	for k, _ := range keyValueMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

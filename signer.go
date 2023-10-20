package main4

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

func SingleHash(in, out chan interface{}) {
	for val := range in {
		data := fmt.Sprintf("%v", val)
		crc32DataCh := make(chan string)
		crc32Md5DataCh := make(chan string)

		go func() {
			crc32DataCh <- DataSignerCrc32(data)
		}()

		md5 := DataSignerMd5(data)

		go func() {
			crc32Md5DataCh <- DataSignerCrc32(md5)
		}()

		crc32Data := <-crc32DataCh
		crc32Md5Data := <-crc32Md5DataCh

		result := crc32Data + "~" + crc32Md5Data
		out <- result
	}
}

func MultiHash(in, out chan interface{}) {
	for val := range in {
		data := fmt.Sprintf("%v", val)
		var wg sync.WaitGroup
		result := make([]string, 6)
		for i := 0; i < 6; i++ {
			wg.Add(1)
			go func(th int) {
				defer wg.Done()
				hash := DataSignerCrc32(strconv.Itoa(th) + data)
				result[th] = hash
			}(i)
		}
		wg.Wait()
		out <- strings.Join(result, "")
	}
}

func CombineResults(in, out chan interface{}) {
	results := make([]string, 0, 100)
	for val := range in {
		results = append(results, val.(string))
		if len(results) == 100 {
			sort.Slice(results, func(i, j int) bool {
				return results[i] < results[j]
			})
			out <- strings.Join(results, "_")
			results = make([]string, 0, 100)
		}
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i] < results[j]
	})
	out <- strings.Join(results, "_")
}

func ExecutePipeline(jobs ...job) {
	wg := &sync.WaitGroup{}
	in := make(chan interface{}, 100)
	for _, j := range jobs {
		out := make(chan interface{}, 100)
		wg.Add(1)
		go func(jobFunc job, in, out chan interface{}, wg *sync.WaitGroup) {
			defer wg.Done()
			defer close(out)
			jobFunc(in, out)
		}(j, in, out, wg)
		in = out
	}
	wg.Wait()
}

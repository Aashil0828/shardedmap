package main

import (
	shardedmap "awesomeProject/ShardedMaps"
	"fmt"
	"sync"
	"time"
)

func main() {
	ShardedMap := shardedmap.New[int, int](10)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ShardedMap.Set(i, i)
		}()
	}
	wg.Wait()
	wg.Add(1)
	go func() {
		defer wg.Done()
		ShardedMap.Iter(func(key int, value int) bool {
			fmt.Printf("key: %d, value: %d\n", key, value)
			return false
		})
	}()
	time.Sleep(1 * time.Millisecond)
	wg.Add(1)
	go func() {
		defer wg.Done()
		ShardedMap.Remove(3)
	}()
	wg.Wait()
	wg.Add(1)
	go func() {
		defer wg.Done()
		if val, ok := ShardedMap.Get(5); ok {
			fmt.Println(val)
		} else {
			fmt.Println("Not found")
		}
	}()
	wg.Wait()
}

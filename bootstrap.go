package main

import (
	"fmt"
	shardmap "github.com/Aashil0828/shardmap/src"
	"sync"
	"time"
)

type MapWithLock struct {
	data map[int]int
	lock sync.RWMutex
}

func main() {
	start := time.Now()
	ShardedMap, err := shardmap.New[int, int](2, false)
	if err != nil {
		panic(err)
	}
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
	time.Sleep(50 * time.Microsecond)
	wg.Add(1)
	go func() {
		defer wg.Done()
		ShardedMap.RemoveAll()
	}()
	wg.Wait()
	wg.Add(1)
	go func() {
		defer wg.Done()
		ShardedMap.Iter(func(key int, value int) bool {
			return false
		})
		if val, ok := ShardedMap.Get(5); ok {
			fmt.Println(val)
		} else {
			fmt.Println("Not found")
		}
	}()
	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("elapsed: %s\n", elapsed)
	start = time.Now()
	lockmap := MapWithLock{data: make(map[int]int)}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lockmap.lock.Lock()
			defer lockmap.lock.Unlock()
			lockmap.data[i] = i
		}()
	}
	wg.Wait()
	wg.Add(1)
	go func() {
		defer wg.Done()
		lockmap.lock.RLock()
		defer lockmap.lock.RUnlock()
		for key, value := range lockmap.data {
			fmt.Printf("key: %d, value: %d\n", key, value)
		}
	}()
	time.Sleep(50 * time.Microsecond)
	wg.Add(1)
	go func() {
		defer wg.Done()
		lockmap.lock.Lock()
		defer lockmap.lock.Unlock()
		for key := range lockmap.data {
			delete(lockmap.data, key)
		}
	}()
	wg.Wait()
	wg.Add(1)
	go func() {
		defer wg.Done()
		lockmap.lock.RLock()
		defer lockmap.lock.RUnlock()
		for key, value := range lockmap.data {
			fmt.Printf("key: %d, value: %d\n", key, value)
		}
		if val, ok := lockmap.data[5]; ok {
			fmt.Println(val)
		} else {
			fmt.Println("Not found")
		}
	}()
	wg.Wait()
	elapsed = time.Since(start)
	fmt.Printf("elapsed: %s\n", elapsed)
}

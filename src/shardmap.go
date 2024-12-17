package src

import (
	"errors"
	"github.com/dolthub/maphash"
	"sync"
)

const (
	MaxShardElements       = 1000
	ShardNotExists         = "shard does not exist"
	NegativeShardUnallowed = "negative shard cannot be initialized"
)

type ShardMap[k comparable, v any] struct {
	shards     []Shard[k, v]
	hashfunc   maphash.Hasher[k]
	sharedLock sync.RWMutex
}

type Shard[k comparable, v any] struct {
	lock sync.RWMutex
	data map[k]v
}

func New[key comparable, value any](numShards int) (*ShardMap[key, value], error) {
	if numShards < 1 {
		return nil, errors.New(NegativeShardUnallowed)
	}
	shards := make([]Shard[key, value], numShards)
	for i := 0; i < numShards; i++ {
		shards[i].data = make(map[key]value, MaxShardElements)
	}
	return &ShardMap[key, value]{
		shards:   shards,
		hashfunc: maphash.NewHasher[key](),
	}, nil
}

func (shmap *ShardMap[k, v]) Set(key k, value v) {
	shmap.sharedLock.RLock()
	defer shmap.sharedLock.RUnlock()
	idx := fastModN(uint32(shmap.hashfunc.Hash(key)), uint32(len(shmap.shards)))
	shmap.shards[idx].lock.Lock()
	defer shmap.shards[idx].lock.Unlock()
	shmap.shards[idx].data[key] = value
}

// lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
func fastModN(x, n uint32) uint32 {
	return uint32((uint64(x) * uint64(n)) >> 32)
}

func (shmap *ShardMap[k, v]) Get(key k) (value v, ok bool) {
	shmap.sharedLock.RLock()
	defer shmap.sharedLock.RUnlock()
	idx := fastModN(uint32(shmap.hashfunc.Hash(key)), uint32(len(shmap.shards)))
	shmap.shards[idx].lock.RLock()
	defer shmap.shards[idx].lock.RUnlock()
	value, ok = shmap.shards[idx].data[key]
	return
}

func (shmap *ShardMap[k, v]) Remove(key k) {
	shmap.sharedLock.RLock()
	defer shmap.sharedLock.RUnlock()
	idx := fastModN(uint32(shmap.hashfunc.Hash(key)), uint32(len(shmap.shards)))
	shmap.shards[idx].lock.Lock()
	defer shmap.shards[idx].lock.Unlock()
	delete(shmap.shards[idx].data, key)
}

func (shmap *ShardMap[k, v]) RemoveAll() {
	shmap.sharedLock.Lock()
	defer shmap.sharedLock.Unlock()
	for _, shard := range shmap.shards {
		for key := range shard.data {
			delete(shard.data, key)
		}
	}
}

func (shmap *ShardMap[k, v]) Iter(callback func(key k, value v) bool) {
	shmap.sharedLock.Lock()
	defer shmap.sharedLock.Unlock()
	for _, shard := range shmap.shards {
		for key, value := range shard.data {
			if callback(key, value) {
				return
			}
		}
	}
}

func (shmap *ShardMap[k, v]) Len() int {
	shmap.sharedLock.RLock()
	defer shmap.sharedLock.RUnlock()
	var total int
	for _, shard := range shmap.shards {
		total += len(shard.data)
	}
	return total
}

func (shmap *ShardMap[k, v]) IterShard(callback func(key k, value v) bool, n int) error {
	if n > len(shmap.shards)-1 {
		return errors.New(ShardNotExists)
	}
	if n < 0 {
		shmap.Iter(callback)
	} else {
		shmap.sharedLock.RLock()
		defer shmap.sharedLock.RUnlock()
		shmap.shards[n].lock.RLock()
		defer shmap.shards[n].lock.RUnlock()
		for key, value := range shmap.shards[n].data {
			if callback(key, value) {
				return nil
			}
		}
	}
	return nil
}

func (shmap *ShardMap[k, v]) Contains(key k) bool {
	shmap.sharedLock.RLock()
	defer shmap.sharedLock.RUnlock()
	idx := fastModN(uint32(shmap.hashfunc.Hash(key)), uint32(len(shmap.shards)))
	shmap.shards[idx].lock.RLock()
	defer shmap.shards[idx].lock.RUnlock()
	_, ok := shmap.shards[idx].data[key]
	return ok
}

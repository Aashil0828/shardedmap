package shardedmap

import (
	"github.com/dolthub/maphash"
	"sync"
)

const MaxElementsPerShard = 1000

type ShardedMap[k comparable, v any] struct {
	Shards      []Shard[k, v]
	hashfunc    maphash.Hasher[k]
	sharedMutex sync.RWMutex
}

type Shard[k comparable, v any] struct {
	mutex sync.RWMutex
	Data  map[k]v
}

func New[k comparable, v any](n int) *ShardedMap[k, v] {
	shards := make([]Shard[k, v], n)
	for i := 0; i < n; i++ {
		shards[i].Data = make(map[k]v, MaxElementsPerShard)
	}
	return &ShardedMap[k, v]{
		Shards:   shards,
		hashfunc: maphash.NewHasher[k](),
	}
}

func (s *ShardedMap[k, v]) Set(key k, value v) {
	s.sharedMutex.RLock()
	defer s.sharedMutex.RUnlock()
	idx := fastModN(uint32(s.hashfunc.Hash(key)), uint32(len(s.Shards)))
	s.Shards[idx].mutex.Lock()
	defer s.Shards[idx].mutex.Unlock()
	s.Shards[idx].Data[key] = value
}

// lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
func fastModN(x, n uint32) uint32 {
	return uint32((uint64(x) * uint64(n)) >> 32)
}

func (s *ShardedMap[k, v]) Get(key k) (value v, ok bool) {
	s.sharedMutex.RLock()
	defer s.sharedMutex.RUnlock()
	idx := fastModN(uint32(s.hashfunc.Hash(key)), uint32(len(s.Shards)))
	s.Shards[idx].mutex.RLock()
	defer s.Shards[idx].mutex.RUnlock()
	value, ok = s.Shards[idx].Data[key]
	return
}

func (s *ShardedMap[k, v]) Remove(key k) {
	s.sharedMutex.RLock()
	defer s.sharedMutex.RUnlock()
	idx := fastModN(uint32(s.hashfunc.Hash(key)), uint32(len(s.Shards)))
	s.Shards[idx].mutex.Lock()
	defer s.Shards[idx].mutex.Unlock()
	delete(s.Shards[idx].Data, key)
}

func (s *ShardedMap[k, v]) RemoveAll() {
	s.sharedMutex.Lock()
	defer s.sharedMutex.Unlock()
	for _, shard := range s.Shards {
		for key := range shard.Data {
			delete(shard.Data, key)
		}
	}
}

func (s *ShardedMap[k, v]) Iter(f func(key k, value v) bool) {
	s.sharedMutex.Lock()
	defer s.sharedMutex.Unlock()
	for _, shard := range s.Shards {
		for key, value := range shard.Data {
			if f(key, value) {
				return
			}
		}
	}
}

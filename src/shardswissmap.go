package src

import (
	"errors"
	"fmt"
	"github.com/dolthub/swiss"
	"github.com/go-faster/city"
)

type ShardSwissMap struct {
	shards []*swiss.Map[string, int]
}

func NewShardSwissMap(numShards int) *ShardSwissMap {

	shards := make([]*swiss.Map[string, int], numShards)

	for shard := 0; shard < len(shards); shard++ {

		shards[shard] = swiss.NewMap[string, int](DefaultShardRecords)

	}

	return &ShardSwissMap{

		shards: shards,
	}
}

func (shardSwissMap *ShardSwissMap) Set(key string, value int) {

	shardSwissMap.shards[shardSwissMap.GetShardIndex(key)].Put(key, value)

}

func (shardSwissMap *ShardSwissMap) Get(key string) (value int, ok bool) {

	return shardSwissMap.shards[shardSwissMap.GetShardIndex(key)].Get(key)

}

func (shardSwissMap *ShardSwissMap) Remove(key string) {

	shardSwissMap.shards[shardSwissMap.GetShardIndex(key)].Delete(key)

}

func (shardSwissMap *ShardSwissMap) RemoveAll() {

	for _, shard := range shardSwissMap.shards {

		shard.Clear()
	}
}

func (shardSwissMap *ShardSwissMap) Iter(callback func(key string, value int) bool) {

	for _, shard := range shardSwissMap.shards {

		shard.Iter(callback)

	}
}

func (shardSwissMap *ShardSwissMap) Len() (size int) {

	for _, shard := range shardSwissMap.shards {

		size += shard.Count()
	}

	return size
}

func (shardSwissMap *ShardSwissMap) IterShard(callback func(key string, value int) bool, shardIndex int) error {

	if shardIndex > len(shardSwissMap.shards)-1 || shardIndex < -1 {

		return errors.New(fmt.Sprintf(ErrorShardNotExists, shardIndex))
	}

	if shardIndex == -1 {

		shardSwissMap.Iter(callback)

	} else {

		shardSwissMap.shards[shardIndex].Iter(callback)

	}
	return nil
}

func (shardSwissMap *ShardSwissMap) Contains(key string) (found bool) {

	_, found = shardSwissMap.shards[shardSwissMap.GetShardIndex(key)].Get(key)

	return found
}

func (shardSwissMap *ShardSwissMap) NumShards() int {

	return len(shardSwissMap.shards)
}

//--------------------------------------------------------Helper Functions-----------------------------------------------

func (shardSwissMap *ShardSwissMap) GetShardIndex(key string) uint32 {

	return fastModN(uint32(city.Hash64([]byte(key))), uint32(len(shardSwissMap.shards)))

}

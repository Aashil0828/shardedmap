package src

import (
	"errors"
	"fmt"
	"github.com/go-faster/city"
)

type ShardMap struct {
	shards []map[string]int
}

const (
	DefaultShardRecords = 1000

	ErrorShardNotExists = "shard %v does not exist"
)

func NewShardMap(numShards int) *ShardMap {

	shards := make([]map[string]int, numShards)

	for shard := 0; shard < len(shards); shard++ {

		shards[shard] = make(map[string]int, DefaultShardRecords)

	}

	return &ShardMap{

		shards: shards,
	}
}

func (shardMap *ShardMap) Set(key string, value int) {

	shardMap.shards[shardMap.GetShardIndex(key)][key] = value

}

func (shardMap *ShardMap) Get(key string) (value int, ok bool) {

	value, ok = shardMap.shards[shardMap.GetShardIndex(key)][key]

	return
}

func (shardMap *ShardMap) Remove(key string) {

	delete(shardMap.shards[shardMap.GetShardIndex(key)], key)

}

func (shardMap *ShardMap) RemoveAll() {

	for _, shard := range shardMap.shards {

		clear(shard)

	}
}

func (shardMap *ShardMap) Iter(callback func(key string, value int) bool) {

	for _, shard := range shardMap.shards {

		for key, value := range shard {

			if callback(key, value) {

				break
			}
		}
	}
}

func (shardMap *ShardMap) Len() (size int) {

	for _, shard := range shardMap.shards {

		size += len(shard)
	}

	return size
}

func (shardMap *ShardMap) IterShard(callback func(key string, value int) bool, shardIndex int) (error error) {

	if shardIndex > len(shardMap.shards)-1 || shardIndex < -1 {

		return errors.New(fmt.Sprintf(ErrorShardNotExists, shardIndex))

	}

	if shardIndex == -1 {

		shardMap.Iter(callback)

		return error

	}

	for key, value := range shardMap.shards[shardIndex] {

		if callback(key, value) {

			break
		}
	}

	return nil
}

func (shardMap *ShardMap) Contains(key string) bool {

	_, found := shardMap.shards[shardMap.GetShardIndex(key)][key]

	return found
}

func (shardMap *ShardMap) NumShards() int {

	return len(shardMap.shards)

}

//-------------------------------------Helper Functions----------------------------------------------------------//

// lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
func fastModN(x, n uint32) uint32 {

	return uint32((uint64(x) * uint64(n)) >> 32)

}

func (shardMap *ShardMap) GetShardIndex(key string) uint32 {

	return fastModN(uint32(city.Hash64([]byte(key))), uint32(len(shardMap.shards)))

}

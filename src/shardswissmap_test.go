package src

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
)

func TestNewShardSwissMap(t *testing.T) {

	assertions := assert.New(t)

	shardSwissMap := NewShardSwissMap(4)

	assertions.NotNil(shardSwissMap)

	assertions.NotNil(shardSwissMap.shards)

	for _, shard := range shardSwissMap.shards {

		assertions.NotNil(shard)

	}

	assertions.Equal(len(shardSwissMap.shards), 4)

}

func TestShardSwissMapSet(t *testing.T) {

	assertions := assert.New(t)

	shardSwissMap := NewShardSwissMap(4)

	shardSwissMap.Set("test", 1)

	assertions.NotNil(shardSwissMap.shards[shardSwissMap.GetShardIndex("test")])

	value, ok := shardSwissMap.shards[shardSwissMap.GetShardIndex("test")].Get("test")

	assertions.True(ok)

	assertions.Equal(1, value)

}

func TestShardSwissMapGet(t *testing.T) {

	assertions := assert.New(t)

	shardSwissMap := NewShardSwissMap(4)

	assertions.NotNil(shardSwissMap.shards[shardSwissMap.GetShardIndex("test")])

	shardSwissMap.Set("test", 1)

	value, ok := shardSwissMap.Get("test")

	assertions.True(ok)

	assertions.NotNil(value)

	assertions.Equal(value, 1)

	value, ok = shardSwissMap.Get("test2")

	assertions.False(ok)

	assertions.Zero(value)

}

func TestShardSwissMapRemove(t *testing.T) {

	assertions := assert.New(t)

	shardSwissMap := NewShardSwissMap(4)

	assertions.NotNil(shardSwissMap.shards[shardSwissMap.GetShardIndex("test")])

	shardSwissMap.Set("test", 1)

	shardSwissMap.Remove("test")

	_, ok := shardSwissMap.shards[shardSwissMap.GetShardIndex("test")].Get("test")

	assertions.False(ok)

}

func TestShardSwissMapRemoveAll(t *testing.T) {

	assertions := assert.New(t)

	shardSwissMap := NewShardSwissMap(4)

	var tests = map[string]int{"test1": 1, "test2": 2, "test3": 3}

	for key, test := range tests {

		shardSwissMap.Set(key, test)

	}

	shardSwissMap.RemoveAll()

	for _, shard := range shardSwissMap.shards {

		assertions.Zero(shard.Count())
	}

}

func TestShardSwissMapIter(t *testing.T) {

	assertions := assert.New(t)

	shardSwissMap := NewShardSwissMap(4)

	var tests = map[string]int{"test1": 1, "test2": 2, "test3": 3}

	for key, test := range tests {

		shardSwissMap.Set(key, test)

	}

	t.Run("StopFalse", func(t *testing.T) {

		var visited = make(map[string]struct{})

		shardSwissMap.Iter(func(key string, value int) bool {

			visited[key] = struct{}{}

			return false

		})

		for _, shard := range shardSwissMap.shards {

			shard.Iter(func(key string, value int) bool {

				_, ok := visited[key]

				assertions.True(ok)

				return false

			})

		}

	})

	t.Run("StopTrue", func(t *testing.T) {

		var visited = make(map[string]struct{})

		shardSwissMap.Iter(func(key string, value int) bool {

			if key == "test2" {

				return true

			}

			visited[key] = struct{}{}

			return false

		})

		_, ok := visited["test2"]

		assertions.False(ok)

	})

}

func TestShardSwissMapLen(t *testing.T) {

	assertions := assert.New(t)

	shardSwissMap := NewShardSwissMap(4)

	t.Run("Empty", func(t *testing.T) {

		assertions.Equal(0, shardSwissMap.Len())

	})
	t.Run("ValidCase", func(t *testing.T) {

		var tests = map[string]int{"test1": 1, "test2": 2, "test3": 3}

		for key, test := range tests {

			shardSwissMap.Set(key, test)

		}

		assertions.Equal(3, shardSwissMap.Len())

	})

}

func TestShardSwissMapIterShard(t *testing.T) {

	assertions := assert.New(t)

	shardSwissMap := NewShardSwissMap(4)

	var tests = map[string]int{"test1": 1, "test2": 2, "test3": 3, "test4": 4, "test5": 5}

	for key, test := range tests {

		shardSwissMap.Set(key, test)

	}

	t.Run("ShardEqualsMinusOne", func(t *testing.T) {

		visited := make(map[string]struct{})

		err := shardSwissMap.IterShard(func(key string, value int) bool {

			visited[key] = struct{}{}

			return false

		}, -1)

		assertions.Nil(err)

		for _, shard := range shardSwissMap.shards {

			shard.Iter(func(key string, value int) bool {

				_, ok := visited[key]

				assertions.True(ok)

				return false

			})

		}

	})
	t.Run("ShardOutOfRange", func(t *testing.T) {

		err := shardSwissMap.IterShard(func(key string, value int) bool {

			return false

		}, 6)

		assertions.EqualError(err, fmt.Sprintf(ErrorShardNotExists, 6))

	})
	t.Run("ValidCase/StopFalse", func(t *testing.T) {

		visited := make(map[string]struct{})

		err := shardSwissMap.IterShard(func(key string, value int) bool {

			visited[key] = struct{}{}

			return false

		}, 3)

		assertions.Nil(err)

		shardSwissMap.shards[3].Iter(func(key string, value int) bool {

			_, ok := visited[key]

			assertions.True(ok)

			return false

		})

	})

	t.Run("ValidCase/StopTrue", func(t *testing.T) {

		visited := make(map[string]struct{})

		var stopKey string

		shardSwissMap.shards[3].Iter(func(key string, value int) bool {

			stopKey = key

			return true

		})

		err := shardSwissMap.IterShard(func(key string, value int) bool {

			if key == stopKey {

				return true

			}

			visited[key] = struct{}{}

			return false

		}, 3)

		assertions.Nil(err)

		_, ok := visited[stopKey]

		assertions.False(ok)

	})

}

func TestShardSwissMapContains(t *testing.T) {

	assertions := assert.New(t)

	shardSwissMap := NewShardSwissMap(4)

	var tests = map[string]int{"test1": 1, "test2": 2, "test3": 3, "test4": 4, "test5": 5}

	for key, test := range tests {

		shardSwissMap.Set(key, test)

	}

	assertions.False(shardSwissMap.Contains("test10"))

	assertions.True(shardSwissMap.Contains("test1"))

}

func TestShardSwissMapNumShards(t *testing.T) {

	assertions := assert.New(t)

	shardSwissMap := NewShardSwissMap(4)

	assertions.Equal(4, shardSwissMap.NumShards())

}

func BenchmarkShardSwissMapNew(b *testing.B) {

	NumShards := []int{10, 1000, 10000}

	assertions := assert.New(b)

	for _, shards := range NumShards {

		b.Run(fmt.Sprintf("NumberOfShards-%d", shards), func(b *testing.B) {

			b.ReportAllocs()

			for i := 0; i < b.N; i++ {

				shardSwissMap := NewShardSwissMap(shards)

				assertions.NotNil(shardSwissMap)

			}

		})

	}

}

func BenchmarkShardSwissMapSet(b *testing.B) {

	for _, elements := range inputs {

		for _, shards := range NumberOfShards {

			shardSwissMap := NewShardSwissMap(shards)

			setElementsShardSwissMap(shardSwissMap, elements)

			setIndex := rand.Intn(elements)

			b.Run(fmt.Sprintf("elements-%d-shards-%d", elements, shards), func(b *testing.B) {

				b.ReportAllocs()

				for i := 0; i < b.N; i++ {

					shardSwissMap.Set(fmt.Sprintf("test%v", setIndex), setIndex)

				}

			})

		}

	}

}

func BenchmarkShardSwissMapGet(b *testing.B) {

	for _, elements := range inputs {

		for _, shards := range NumberOfShards {

			shardSwissMap := NewShardSwissMap(shards)

			setElementsShardSwissMap(shardSwissMap, elements)

			b.Run(fmt.Sprintf("elements-%d-shards-%d", elements, shards), func(b *testing.B) {

				b.ReportAllocs()

				for i := 0; i < b.N; i++ {

					shardSwissMap.Get(fmt.Sprintf("test%v", rand.Intn(elements)))

				}

			})

		}

	}

}

func BenchmarkShardSwissMapIter(b *testing.B) {

	for _, elements := range inputs {

		for _, shards := range NumberOfShards {

			shardSwissMap := NewShardSwissMap(shards)

			setElementsShardSwissMap(shardSwissMap, elements)

			b.Run(fmt.Sprintf("elements-%d-shards-%d", elements, shards), func(b *testing.B) {

				b.ReportAllocs()

				for i := 0; i < b.N; i++ {

					shardSwissMap.Iter(func(key string, value int) bool {

						return false

					})

				}

			})

		}

	}

}

func BenchmarkShardSwissMapRemove(b *testing.B) {

	for _, elements := range inputs {

		for _, shards := range NumberOfShards {

			shardSwissMap := NewShardSwissMap(shards)

			setElementsShardSwissMap(shardSwissMap, elements)

			b.Run(fmt.Sprintf("elements-%d-shards-%d", elements, shards), func(b *testing.B) {

				b.ReportAllocs()

				for i := 0; i < b.N; i++ {

					shardSwissMap.Remove(fmt.Sprintf("test%v", rand.Intn(elements)))

				}

			})

		}

	}

}

func BenchmarkShardSwissMapRemoveAll(b *testing.B) {

	for _, elements := range inputs {

		for _, shards := range NumberOfShards {

			shardSwissMap := NewShardSwissMap(shards)

			setElementsShardSwissMap(shardSwissMap, elements)

			b.Run(fmt.Sprintf("elements-%d-shards-%d", elements, shards), func(b *testing.B) {

				b.ReportAllocs()

				for i := 0; i < b.N; i++ {

					shardSwissMap.RemoveAll()

				}

			})

		}

	}

}

func BenchmarkShardSwissMapContains(b *testing.B) {

	for _, elements := range inputs {

		for _, shards := range NumberOfShards {

			shardSwissMap := NewShardSwissMap(shards)

			setElementsShardSwissMap(shardSwissMap, elements)

			b.Run(fmt.Sprintf("elements-%d-shards-%d", elements, shards), func(b *testing.B) {

				b.ReportAllocs()

				for i := 0; i < b.N; i++ {

					shardSwissMap.Contains(fmt.Sprintf("test%v", rand.Intn(elements)))

				}

			})

		}

	}

}

func BenchmarkShardSwissMapLen(b *testing.B) {

	for _, elements := range inputs {

		for _, shards := range NumberOfShards {

			shardSwissMap := NewShardSwissMap(shards)

			setElementsShardSwissMap(shardSwissMap, elements)

			b.Run(fmt.Sprintf("elements-%d-shards-%d", elements, shards), func(b *testing.B) {

				b.ReportAllocs()

				for i := 0; i < b.N; i++ {

					shardSwissMap.Len()

				}

			})

		}

	}

}

func BenchmarkShardSwissMapIterShard(b *testing.B) {

	for _, elements := range inputs {

		for _, shards := range NumberOfShards {

			shardSwissMap := NewShardSwissMap(shards)

			setElementsShardSwissMap(shardSwissMap, elements)

			b.Run(fmt.Sprintf("elements-%d-shards-%d", elements, shards), func(b *testing.B) {

				b.ReportAllocs()

				for i := 0; i < b.N; i++ {

					shardSwissMap.IterShard(func(key string, value int) bool {

						return false

					}, rand.Intn(shards))

				}

			})

		}

	}

}

//-----------------------------------------------------Helper Functions-----------------------------------------------

func setElementsShardSwissMap(shardSwissMap *ShardSwissMap, elements int) {

	for i := 0; i < elements; i++ {

		shardSwissMap.Set(fmt.Sprintf("test%v", i), i)

	}
}

package src

import (
	"fmt"
	"github.com/dolthub/maphash"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
)

var inputs = []int{
	100,
	10000,
	100000,
}

var NumberOfShards = []int{
	10,
	100,
	1000,
}

func TestNewShardMap(t *testing.T) {

	assertions := assert.New(t)

	shardMap := NewShardMap(4)

	assertions.NotNil(shardMap)

	assertions.NotNil(shardMap.shards)

	for _, shard := range shardMap.shards {

		assertions.NotNil(shard)

	}

	assertions.Equal(len(shardMap.shards), 4)

}

func TestShardMapSet(t *testing.T) {

	assertions := assert.New(t)

	shardMap := NewShardMap(4)

	shardMap.Set("test", 1)

	assertions.NotNil(shardMap.shards[shardMap.GetShardIndex("test")])

	value, ok := shardMap.shards[shardMap.GetShardIndex("test")]["test"]

	assertions.True(ok)

	assertions.Equal(1, value)

}

func TestShardMapGet(t *testing.T) {

	assertions := assert.New(t)

	shardMap := NewShardMap(4)

	assertions.NotNil(shardMap.shards[shardMap.GetShardIndex("test")])

	shardMap.shards[shardMap.GetShardIndex("test")] = map[string]int{"test": 1}

	value, ok := shardMap.Get("test")

	assertions.True(ok)

	assertions.NotNil(value)

	assertions.Equal(value, 1)

	value, ok = shardMap.Get("test2")

	assertions.False(ok)

	assertions.Zero(value)

}

func TestShardMapRemove(t *testing.T) {

	assertions := assert.New(t)

	shardMap := NewShardMap(4)

	assertions.NotNil(shardMap.shards[shardMap.GetShardIndex("test")])

	shardMap.shards[shardMap.GetShardIndex("test")] = map[string]int{"test": 1}

	shardMap.Remove("test")

	_, ok := shardMap.shards[shardMap.GetShardIndex("test")]["test"]

	assertions.False(ok)

}

func TestShardMapRemoveAll(t *testing.T) {

	assertions := assert.New(t)

	shardMap := NewShardMap(4)

	var tests = map[string]int{"test1": 1, "test2": 2, "test3": 3}

	for key, test := range tests {

		shardMap.Set(key, test)

	}

	shardMap.RemoveAll()

	for _, Shd := range shardMap.shards {

		assertions.Zero(len(Shd))
	}

}

func TestFastModN(t *testing.T) {

	assertions := assert.New(t)

	var testCases []struct {
		Divisor uint32

		Divident uint32

		Remainder uint32
	}

	for i := 0; i < 100; i++ {

		tc := struct {
			Divisor uint32

			Divident uint32

			Remainder uint32
		}{

			Divisor: 1 + uint32(rand.Intn(100)),

			Divident: uint32(maphash.NewHasher[int]().Hash(rand.Intn(100000))),
		}
		tc.Remainder = uint32(uint64(tc.Divident) * uint64(tc.Divisor) >> 32)

		testCases = append(testCases, tc)

	}
	for _, tc := range testCases {

		assertions.Equal(tc.Remainder, fastModN(tc.Divident, tc.Divisor))

	}
}

func TestShardMapIter(t *testing.T) {

	assertions := assert.New(t)

	shardMap := NewShardMap(4)

	var tests = map[string]int{"test1": 1, "test2": 2, "test3": 3}

	for key, test := range tests {

		shardMap.Set(key, test)

	}

	t.Run("StopFalse", func(t *testing.T) {

		var visited = make(map[string]struct{})

		shardMap.Iter(func(key string, value int) bool {

			visited[key] = struct{}{}

			return false

		})

		for _, shard := range shardMap.shards {

			for key := range shard {

				_, ok := visited[key]

				assertions.True(ok)

			}

		}

	})

	t.Run("StopTrue", func(t *testing.T) {

		var visited = make(map[string]struct{})

		shardMap.Iter(func(key string, value int) bool {

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

func TestShardMapLen(t *testing.T) {

	assertions := assert.New(t)

	shardMap := NewShardMap(4)

	t.Run("Empty", func(t *testing.T) {

		assertions.Equal(0, shardMap.Len())

	})
	t.Run("ValidCase", func(t *testing.T) {

		var tests = map[string]int{"test1": 1, "test2": 2, "test3": 3}

		for key, test := range tests {

			shardMap.Set(key, test)

		}

		assertions.Equal(3, shardMap.Len())

	})

}

func TestShardMapIterShard(t *testing.T) {

	assertions := assert.New(t)

	shardMap := NewShardMap(4)

	var tests = map[string]int{"test1": 1, "test2": 2, "test3": 3, "test4": 4, "test5": 5}

	for key, test := range tests {

		shardMap.Set(key, test)

	}

	t.Run("ShardEqualsMinusOne", func(t *testing.T) {

		visited := make(map[string]struct{})

		err := shardMap.IterShard(func(key string, value int) bool {

			visited[key] = struct{}{}

			return false

		}, -1)

		assertions.Nil(err)

		for _, shard := range shardMap.shards {

			for key := range shard {

				_, ok := visited[key]

				assertions.True(ok)

			}

		}

	})
	t.Run("ShardOutOfRange", func(t *testing.T) {

		err := shardMap.IterShard(func(key string, value int) bool {

			return false

		}, 6)

		assertions.EqualError(err, fmt.Sprintf(ErrorShardNotExists, 6))

	})
	t.Run("ValidCase/StopFalse", func(t *testing.T) {

		visited := make(map[string]struct{})

		err := shardMap.IterShard(func(key string, value int) bool {

			visited[key] = struct{}{}

			return false

		}, 3)

		assertions.Nil(err)

		for key := range shardMap.shards[3] {

			_, ok := visited[key]

			assertions.True(ok)
		}

	})

	t.Run("ValidCase/StopTrue", func(t *testing.T) {

		visited := make(map[string]struct{})

		var stopkey string

		for key := range shardMap.shards[3] {

			stopkey = key

			break
		}

		err := shardMap.IterShard(func(key string, value int) bool {

			if key == stopkey {

				return true

			}

			visited[key] = struct{}{}

			return false

		}, 3)

		assertions.Nil(err)

		_, ok := visited[stopkey]

		assertions.False(ok)

	})

}

func TestShardMapContains(t *testing.T) {

	assertions := assert.New(t)

	shardMap := NewShardMap(4)

	var tests = map[string]int{"test1": 1, "test2": 2, "test3": 3, "test4": 4, "test5": 5}

	for key, test := range tests {

		shardMap.Set(key, test)

	}

	assertions.False(shardMap.Contains("test10"))

	assertions.True(shardMap.Contains("test1"))

}

func TestShardMapShards(t *testing.T) {

	assertions := assert.New(t)

	shardMap := NewShardMap(4)

	assertions.Equal(4, shardMap.Shards())

}

func BenchmarkShardMapNew(b *testing.B) {

	numShards := []int{10, 1000, 10000}

	assertions := assert.New(b)

	for _, shards := range numShards {

		b.Run(fmt.Sprintf("NumberOfShards-%d", shards), func(b *testing.B) {

			b.ReportAllocs()

			for i := 0; i < b.N; i++ {

				shardMap := NewShardMap(shards)

				assertions.NotNil(shardMap)

			}

		})

	}

}

func BenchmarkShardMapSet(b *testing.B) {

	for _, elements := range inputs {

		for _, shards := range NumberOfShards {

			shardMap := NewShardMap(shards)

			setElementsShardMap(shardMap, elements)

			setIndex := rand.Intn(elements)

			b.Run(fmt.Sprintf("elements-%d-shards-%d", elements, shards), func(b *testing.B) {

				b.ReportAllocs()

				for i := 0; i < b.N; i++ {

					shardMap.Set(fmt.Sprintf("test%v", setIndex), setIndex)

				}

			})

		}

	}

}

func BenchmarkShardMapGet(b *testing.B) {

	for _, elements := range inputs {

		for _, shards := range NumberOfShards {

			shardMap := NewShardMap(shards)

			setElementsShardMap(shardMap, elements)

			b.Run(fmt.Sprintf("elements-%d-shards-%d", elements, shards), func(b *testing.B) {

				b.ReportAllocs()

				for i := 0; i < b.N; i++ {

					shardMap.Get(fmt.Sprintf("test%v", rand.Intn(elements)))

				}

			})

		}

	}

}

func BenchmarkShardMapIter(b *testing.B) {

	for _, elements := range inputs {

		for _, shards := range NumberOfShards {

			shardMap := NewShardMap(shards)

			setElementsShardMap(shardMap, elements)

			b.Run(fmt.Sprintf("elements-%d-shards-%d", elements, shards), func(b *testing.B) {

				b.ReportAllocs()

				for i := 0; i < b.N; i++ {

					shardMap.Iter(func(key string, value int) bool {

						return false

					})

				}

			})

		}

	}

}

func BenchmarkShardMapRemove(b *testing.B) {

	for _, elements := range inputs {

		for _, shards := range NumberOfShards {

			shardMap := NewShardMap(shards)

			setElementsShardMap(shardMap, elements)

			b.Run(fmt.Sprintf("elements-%d-shards-%d", elements, shards), func(b *testing.B) {

				b.ReportAllocs()

				for i := 0; i < b.N; i++ {

					shardMap.Remove(fmt.Sprintf("test%v", rand.Intn(elements)))

				}

			})

		}

	}

}

func BenchmarkShardMapRemoveAll(b *testing.B) {

	for _, elements := range inputs {

		for _, shards := range NumberOfShards {

			shardMap := NewShardMap(shards)

			setElementsShardMap(shardMap, elements)

			b.Run(fmt.Sprintf("elements-%d-shards-%d", elements, shards), func(b *testing.B) {

				b.ReportAllocs()

				for i := 0; i < b.N; i++ {

					shardMap.RemoveAll()

				}

			})

		}

	}

}

func BenchmarkShardMapContains(b *testing.B) {

	for _, elements := range inputs {

		for _, shards := range NumberOfShards {

			shardMap := NewShardMap(shards)

			setElementsShardMap(shardMap, elements)

			b.Run(fmt.Sprintf("elements-%d-shards-%d", elements, shards), func(b *testing.B) {

				b.ReportAllocs()

				for i := 0; i < b.N; i++ {

					shardMap.Contains(fmt.Sprintf("test%v", rand.Intn(elements)))

				}

			})

		}

	}

}

func BenchmarkShardMapLen(b *testing.B) {

	for _, elements := range inputs {

		for _, shards := range NumberOfShards {

			shardMap := NewShardMap(shards)

			setElementsShardMap(shardMap, elements)

			b.Run(fmt.Sprintf("elements-%d-shards-%d", elements, shards), func(b *testing.B) {

				b.ReportAllocs()

				for i := 0; i < b.N; i++ {

					shardMap.Len()

				}

			})

		}

	}

}

func BenchmarkShardMapIterShard(b *testing.B) {

	for _, elements := range inputs {

		for _, shards := range NumberOfShards {

			shardMap := NewShardMap(shards)

			setElementsShardMap(shardMap, elements)

			b.Run(fmt.Sprintf("elements-%d-shards-%d", elements, shards), func(b *testing.B) {

				b.ReportAllocs()

				for i := 0; i < b.N; i++ {

					shardMap.IterShard(func(key string, value int) bool {

						return false

					}, rand.Intn(shards))

				}

			})

		}

	}

}

//-----------------------------------------------------Helper Functions-----------------------------------------------

func setElementsShardMap(shardMap *ShardMap, elements int) {

	for i := 0; i < elements; i++ {

		shardMap.Set(fmt.Sprintf("test%v", i), i)

	}
}

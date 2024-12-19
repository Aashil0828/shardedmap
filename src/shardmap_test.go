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
var shards = []int{
	10,
	100,
	1000,
}

type mapType struct {
	name        string
	useSwissMap bool
}

var mapTypes = []mapType{
	{
		name:        "swiss",
		useSwissMap: true,
	},
	{
		name:        "normal",
		useSwissMap: false,
	},
}

func TestNew(t *testing.T) {
	assertions := assert.New(t)
	for _, m := range mapTypes {
		t.Run(m.name, func(t *testing.T) {
			t.Run("NegativeShards", func(t *testing.T) {
				_, err := New[string, int](-4, m.useSwissMap)
				assertions.EqualError(err, NegativeShardUnallowed)
			})
			t.Run("Valid", func(t *testing.T) {
				shmap, err := New[string, int](4, m.useSwissMap)
				assertions.Nil(err)
				assertions.NotNil(shmap)
				assertions.NotNil(shmap.shards)
				assertions.NotNil(shmap.hashfunc)
				for _, Shd := range shmap.shards {
					assertions.NotNil(Shd.data)
				}
				assertions.Equal(len(shmap.shards), 4)
			})
			t.Run("ZeroShards", func(t *testing.T) {
				_, err := New[string, int](0, m.useSwissMap)
				assertions.EqualError(err, NegativeShardUnallowed)
			})
		})
	}

}

func TestSet(t *testing.T) {
	assertions := assert.New(t)
	for _, m := range mapTypes {
		t.Run(m.name, func(t *testing.T) {
			shmap, err := New[string, int](4, m.useSwissMap)
			assertions.Nil(err)
			shmap.Set("test", 1)
			idx := fastModN(uint32(shmap.hashfunc.Hash("test")), uint32(len(shmap.shards)))
			assertions.NotNil(shmap.shards[idx].data)
			value, ok := shmap.shards[idx].data.Get("test")
			assertions.True(ok)
			assertions.Equal(1, value)
		})
	}

}

func TestGet(t *testing.T) {

	assertions := assert.New(t)
	for _, m := range mapTypes {
		var Map MapInterface[string, int]
		if m.name == "swiss" {
			Map = NewSwissMap[string, int](DefaultShardElements)
		} else {
			Map = NewMap[string, int](DefaultShardElements)
		}
		Map.Set("test", 1)
		t.Run(m.name, func(t *testing.T) {
			shmap, err := New[string, int](4, m.useSwissMap)
			assertions.Nil(err)
			idx := fastModN(uint32(shmap.hashfunc.Hash("test")), uint32(len(shmap.shards)))
			assertions.NotNil(shmap.shards[idx].data)
			shmap.shards[idx].data = Map
			value, ok := shmap.Get("test")
			assertions.True(ok)
			assertions.NotNil(value)
			assertions.Equal(value, 1)
			value, ok = shmap.Get("test2")
			assertions.False(ok)
			assertions.Zero(value)
		})
	}

}

func TestRemove(t *testing.T) {
	assertions := assert.New(t)
	for _, m := range mapTypes {
		var Map MapInterface[string, int]
		if m.name == "swiss" {
			Map = NewSwissMap[string, int](DefaultShardElements)
		} else {
			Map = NewMap[string, int](DefaultShardElements)
		}
		Map.Set("test", 1)
		t.Run(m.name, func(t *testing.T) {
			shmap, err := New[string, int](4, m.useSwissMap)
			assertions.Nil(err)
			idx := fastModN(uint32(shmap.hashfunc.Hash("test")), uint32(len(shmap.shards)))
			assertions.NotNil(shmap.shards[idx].data)
			shmap.shards[idx].data = Map
			shmap.Remove("test")
			_, ok := shmap.shards[idx].data.Get("test")
			assertions.False(ok)
		})

	}

}

func TestRemoveAll(t *testing.T) {
	assertions := assert.New(t)
	for _, m := range mapTypes {
		t.Run(m.name, func(t *testing.T) {
			shmap, err := New[int, int](4, m.useSwissMap)
			for i := 0; i < 100; i++ {
				shmap.Set(i, i)
			}
			assertions.Nil(err)
			shmap.RemoveAll()
			for _, Shd := range shmap.shards {
				assertions.Zero(Shd.data.Len())
			}
		})
	}

}

func TestFastModN(t *testing.T) {
	assertions := assert.New(t)
	var testCases []struct {
		Divisor   uint32
		Divident  uint32
		Remainder uint32
	}

	for i := 0; i < 100; i++ {
		tc := struct {
			Divisor   uint32
			Divident  uint32
			Remainder uint32
		}{Divisor: 1 + uint32(rand.Intn(100)), Divident: uint32(maphash.NewHasher[int]().Hash(rand.Intn(100000)))}
		tc.Remainder = uint32(uint64(tc.Divident) * uint64(tc.Divisor) >> 32)
		testCases = append(testCases, tc)
	}
	for _, tc := range testCases {
		assertions.Equal(tc.Remainder, fastModN(tc.Divident, tc.Divisor))
	}
}

func TestIter(t *testing.T) {
	assertions := assert.New(t)
	for _, m := range mapTypes {
		t.Run(m.name, func(t *testing.T) {
			shmap, err := New[string, int](4, m.useSwissMap)
			assertions.Nil(err)
			shmap.Set("test", 1)
			shmap.Set("test2", 2)
			shmap.Set("test3", 3)
			shmap.Set("test4", 4)
			t.Run("StopFalse", func(t *testing.T) {
				var visited = make(map[string]struct{})
				shmap.Iter(func(key string, value int) bool {
					visited[key] = struct{}{}
					return false
				})
				for _, shd := range shmap.shards {
					shd.data.Iter(func(key string, value int) bool {
						_, ok := visited[key]
						assertions.True(ok)
						return false
					})
				}
			})
			t.Run("StopTrue", func(t *testing.T) {
				var visited map[string]struct{} = make(map[string]struct{})
				shmap.Iter(func(key string, value int) bool {
					if key == "test2" {
						return true
					}
					visited[key] = struct{}{}
					return false
				})
				_, ok := visited["test2"]
				assertions.False(ok)
			})
		})
	}
}

func TestLen(t *testing.T) {
	assertions := assert.New(t)
	for _, m := range mapTypes {
		t.Run(m.name, func(t *testing.T) {
			shmap, err := New[int, int](4, m.useSwissMap)
			assertions.Nil(err)
			t.Run("Empty", func(t *testing.T) {
				assertions.Equal(0, shmap.Len())
			})
			t.Run("ValidCase", func(t *testing.T) {
				for i := 0; i < 100; i++ {
					shmap.Set(i, i)
				}
				assertions.Equal(100, shmap.Len())
			})
		})
	}
}

func TestIterShard(t *testing.T) {
	assertions := assert.New(t)
	for _, m := range mapTypes {
		t.Run(m.name, func(t *testing.T) {
			shmap, err := New[int, int](4, m.useSwissMap)
			assertions.Nil(err)
			for i := 0; i < 100; i++ {
				shmap.Set(i, i)
			}
			t.Run("NegativeShard", func(t *testing.T) {
				visited := make(map[int]struct{})
				err = shmap.IterShard(func(key int, value int) bool {
					visited[key] = struct{}{}
					return false
				}, -1)
				assertions.Nil(err)

				for _, shd := range shmap.shards {
					shd.data.Iter(func(key int, value int) bool {
						_, ok := visited[key]
						assertions.True(ok)
						return false
					})
				}
			})
			t.Run("ShardOutOfRange", func(t *testing.T) {
				err = shmap.IterShard(func(key int, value int) bool {
					return false
				}, 6)
				assertions.EqualError(err, ShardNotExists)
			})
			t.Run("ValidCase", func(t *testing.T) {
				t.Run("StopFalse", func(t *testing.T) {
					visited := make(map[int]struct{})
					err = shmap.IterShard(func(key int, value int) bool {
						visited[key] = struct{}{}
						return false
					}, 3)
					assertions.Nil(err)
					shmap.shards[3].data.Iter(func(key int, value int) bool {
						_, ok := visited[key]
						assertions.True(ok)
						return false
					})
				})
				t.Run("StopTrue", func(t *testing.T) {
					visited := make(map[int]struct{})
					var stopkey int
					shmap.shards[3].data.Iter(func(key int, value int) bool {
						stopkey = key
						return true
					})
					err = shmap.IterShard(func(key int, value int) bool {
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
			})
		})
	}

}

func TestContains(t *testing.T) {
	assertions := assert.New(t)
	for _, m := range mapTypes {
		t.Run(m.name, func(t *testing.T) {
			shmap, err := New[int, int](4, m.useSwissMap)
			assertions.Nil(err)
			for i := 0; i < 100; i++ {
				shmap.Set(i, i)
			}
			assertions.False(shmap.Contains(103))
			assertions.True(shmap.Contains(40))
		})
	}

}

func BenchmarkNew(b *testing.B) {
	inputs := []int{10, 1000, 10000}
	for _, input := range inputs {
		for _, m := range mapTypes {
			b.Run(m.name, func(b *testing.B) {
				b.Run(fmt.Sprintf("input-string-interface{}-%d", input), func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						_, _ = New[string, interface{}](input, m.useSwissMap)
					}
				})
				b.Run(fmt.Sprintf("input-interface{}-interface{}-%d", input), func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						_, _ = New[interface{}, interface{}](input, m.useSwissMap)
					}
				})
				b.Run(fmt.Sprintf("input-string-string-%d", input), func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						_, _ = New[string, string](input, m.useSwissMap)
					}
				})
				b.Run(fmt.Sprintf("input-int-int-%d", input), func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						_, _ = New[int, int](input, m.useSwissMap)
					}
				})
			})
		}
	}
}

func BenchmarkSet(b *testing.B) {
	for _, input := range inputs {
		for _, shard := range shards {
			for _, m := range mapTypes {
				b.Run(m.name, func(b *testing.B) {
					b.Run(fmt.Sprintf("input-%d-shards-%d", input, shard), func(b *testing.B) {
						shmap, _ := New[int, int](shard, m.useSwissMap)
						b.ReportAllocs()
						for i := 0; i < b.N; i++ {
							for i := 0; i < input; i++ {
								shmap.Set(i, i)
							}
						}
					})
				})
			}
		}
	}
}

func BenchmarkGet(b *testing.B) {
	for _, input := range inputs {
		for _, shard := range shards {
			for _, m := range mapTypes {
				shmap, _ := New[int, int](shard, m.useSwissMap)
				for i := 0; i < input; i++ {
					shmap.Set(i, i)
				}
				b.Run(m.name, func(b *testing.B) {
					b.Run(fmt.Sprintf("input-%d-shards-%d", input, shard), func(b *testing.B) {
						b.ReportAllocs()
						for i := 0; i < b.N; i++ {
							shmap.Get(rand.Intn(input))
						}
					})
				})

			}
		}

	}
}

func BenchmarkIter(b *testing.B) {
	for _, input := range inputs {
		for _, shard := range shards {
			for _, m := range mapTypes {
				b.Run(m.name, func(b *testing.B) {
					shmap, _ := New[int, int](shard, m.useSwissMap)
					for i := 0; i < input; i++ {
						shmap.Set(i, i)
					}
					b.Run(fmt.Sprintf("input-%d-shards-%d", input, shard), func(b *testing.B) {
						b.ReportAllocs()
						for i := 0; i < b.N; i++ {
							shmap.Iter(func(key int, value int) bool {
								return false
							})
						}
					})
				})
			}
		}
	}
}

func BenchmarkRemove(b *testing.B) {
	for _, input := range inputs {
		for _, shard := range shards {
			for _, m := range mapTypes {
				b.Run(m.name, func(b *testing.B) {
					shmap, _ := New[int, int](shard, m.useSwissMap)
					for i := 0; i < input; i++ {
						shmap.Set(i, i)
					}
					b.Run(fmt.Sprintf("input-%d-shards-%d", input, shard), func(b *testing.B) {
						b.ReportAllocs()
						for i := 0; i < b.N; i++ {
							shmap.Remove(rand.Intn(input))
						}
					})
				})
			}
		}
	}
}

func BenchmarkRemoveAll(b *testing.B) {
	for _, input := range inputs {
		for _, shard := range shards {
			for _, m := range mapTypes {
				b.Run(m.name, func(b *testing.B) {
					shmap, _ := New[int, int](shard, m.useSwissMap)
					for i := 0; i < input; i++ {
						shmap.Set(i, i)
					}
					b.Run(fmt.Sprintf("input-%d-shards-%d", input, shard), func(b *testing.B) {
						b.ReportAllocs()
						for i := 0; i < b.N; i++ {
							shmap.RemoveAll()
						}
					})
				})
			}
		}
	}
}

func BenchmarkContains(b *testing.B) {
	for _, input := range inputs {
		for _, shard := range shards {
			for _, m := range mapTypes {
				b.Run(m.name, func(b *testing.B) {
					shmap, _ := New[int, int](shard, m.useSwissMap)
					for i := 0; i < input; i++ {
						shmap.Set(i, i)
					}
					b.Run(fmt.Sprintf("input-%d-shards-%d", input, shard), func(b *testing.B) {
						b.ReportAllocs()
						for i := 0; i < b.N; i++ {
							shmap.Contains(rand.Intn(input))
						}
					})
				})
			}
		}
	}
}

func BenchmarkLen(b *testing.B) {
	for _, input := range inputs {
		for _, shard := range shards {
			for _, m := range mapTypes {
				b.Run(m.name, func(b *testing.B) {
					shmap, _ := New[int, int](shard, m.useSwissMap)
					for i := 0; i < input; i++ {
						shmap.Set(i, i)
					}
					b.Run(fmt.Sprintf("input-%d-shards-%d", input, shard), func(b *testing.B) {
						b.ReportAllocs()
						for i := 0; i < b.N; i++ {
							_ = shmap.Len()
						}
					})
				})
			}
		}
	}
}

func BenchmarkIterShard(b *testing.B) {
	for _, input := range inputs {
		for _, shard := range shards {
			for _, m := range mapTypes {
				b.Run(m.name, func(b *testing.B) {
					shmap, _ := New[int, int](shard, m.useSwissMap)
					for i := 0; i < input; i++ {
						shmap.Set(i, i)
					}
					b.Run(fmt.Sprintf("input-%d-shards-%d", input, shard), func(b *testing.B) {
						b.ReportAllocs()
						for i := 0; i < b.N; i++ {
							_ = shmap.IterShard(func(key int, value int) bool {
								return false
							}, rand.Intn(shard))
						}
					})
				})
			}
		}
	}
}

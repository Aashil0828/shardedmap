package src

import "github.com/dolthub/swiss"

type MapInterface[k comparable, v any] interface {
	Set(key k, value v)
	Get(key k) (value v, ok bool)
	Remove(key k)
	Iter(callback func(key k, value v) bool)
	Len() int
	Clear()
}

type Map[k comparable, v any] map[k]v

func NewMap[k comparable, v any](maxElements int) Map[k, v] {
	return make(Map[k, v], maxElements)
}

func (mp Map[k, v]) Set(key k, value v) {
	mp[key] = value
}

func (mp Map[k, v]) Get(key k) (value v, ok bool) {
	value, ok = mp[key]
	return
}

func (mp Map[k, v]) Remove(key k) {
	delete(mp, key)
}

func (mp Map[k, v]) Iter(callback func(key k, value v) bool) {
	for k, v := range mp {
		if callback(k, v) {
			break
		}
	}
}

func (mp Map[k, v]) Len() int {
	return len(mp)
}

func (mp Map[k, v]) Clear() {
	clear(mp)
}

type SwissMap[k comparable, v any] struct {
	data *swiss.Map[k, v]
}

func NewSwissMap[k comparable, v any](maxElements int) SwissMap[k, v] {
	return SwissMap[k, v]{
		data: swiss.NewMap[k, v](uint32(maxElements)),
	}
}

func (swsmap SwissMap[k, v]) Set(key k, value v) {
	swsmap.data.Put(key, value)
}

func (swsmap SwissMap[k, v]) Get(key k) (value v, ok bool) {
	value, ok = swsmap.data.Get(key)
	return
}

func (swsmap SwissMap[k, v]) Remove(key k) {
	swsmap.data.Delete(key)
}

func (swsmap SwissMap[k, v]) Iter(callback func(key k, value v) bool) {
	swsmap.data.Iter(callback)
}

func (swsmap SwissMap[k, v]) Len() int {
	return swsmap.data.Count()
}

func (swsmap SwissMap[k, v]) Clear() {
	swsmap.data.Clear()
}

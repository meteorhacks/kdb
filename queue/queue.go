package queue

import (
	"errors"
	"sync"
)

var (
	ErrKeyExists  = errors.New("key already exists")
	ErrKeyMissing = errors.New("key does not exist")
)

type Queue interface {
	Add(key int64, val interface{}) (err error)
	Get(key int64) (val interface{}, err error)
	Del(key int64) (val interface{}, err error)
	Out() (ch <-chan interface{})
	Flush() (data map[int64]interface{})

	Length() (length int)
}

type queue struct {
	data map[int64]interface{}
	keys []int64
	head int
	tail int
	lnth int
	size int
	mutx *sync.Mutex
	outc chan interface{}
}

func NewQueue(size int) (q Queue) {
	return newQueue(size)
}

func newQueue(size int) (q *queue) {
	return &queue{
		data: make(map[int64]interface{}, size),
		keys: make([]int64, size, size),
		size: size,
		mutx: &sync.Mutex{},
		outc: make(chan interface{}),
	}
}

func (q *queue) Add(key int64, val interface{}) (err error) {
	q.mutx.Lock()
	defer q.mutx.Unlock()

	if _, ok := q.data[key]; ok {
		return ErrKeyExists
	}

	if q.lnth == q.size {
		k := q.keys[q.head]
		v := q.data[k]
		q.outc <- v
		q.del(k)
	}

	q.keys[q.tail] = key
	q.data[key] = val
	q.tail = (q.tail + 1) % q.size
	q.lnth++

	return nil
}

func (q *queue) Get(key int64) (val interface{}, err error) {
	val, ok := q.data[key]
	if !ok {
		return nil, ErrKeyMissing
	}

	return val, nil
}

func (q *queue) Del(key int64) (val interface{}, err error) {
	val, ok := q.data[key]
	if !ok {
		return nil, ErrKeyMissing
	}

	q.del(key)

	return val, nil
}

func (q *queue) Out() (ch <-chan interface{}) {
	return q.outc
}

func (q *queue) Flush() (data map[int64]interface{}) {
	q.mutx.Lock()
	defer q.mutx.Unlock()

	data = q.data
	q.data = make(map[int64]interface{}, q.size)

	return data
}

func (q *queue) Length() (length int) {
	return q.lnth
}

func (q *queue) del(key int64) {
	delete(q.data, key)
	q.head = (q.head + 1) % q.size
	q.lnth--
}

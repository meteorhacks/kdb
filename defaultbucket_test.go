package kdb

import (
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"testing"
)

func TestNewDefaultBucket(t *testing.T) {
	files := []string{
		"/tmp/test_0.block",
		"/tmp/test_0_0.index",
		"/tmp/test_0_1.index",
		"/tmp/test_0_2.index",
		"/tmp/test_0_3.index",
	}

	for _, f := range files {
		defer os.Remove(f)
	}

	bkt, err := NewDefaultBucket(DefaultBucketOpts{
		DatabaseName:   "test",
		DataPath:       "/tmp/",
		Partitions:     4,
		IndexDepth:     4,
		PayloadSize:    4,
		BucketDuration: 100,
		Resolution:     10,
		BaseTime:       0,
	})

	if err != nil {
		t.Fatal(err)
	}

	if len(bkt.indexes) != 4 {
		t.Fatal("number of indexes should be 4")
	}
}

func TestDefaultBucketPut(t *testing.T) {
	files := []string{
		"/tmp/test_0.block",
		"/tmp/test_0_0.index",
		"/tmp/test_0_1.index",
		"/tmp/test_0_2.index",
		"/tmp/test_0_3.index",
	}

	for _, f := range files {
		defer os.Remove(f)
	}

	bkt, err := NewDefaultBucket(DefaultBucketOpts{
		DatabaseName:   "test",
		DataPath:       "/tmp/",
		Partitions:     4,
		IndexDepth:     4,
		PayloadSize:    4,
		BucketDuration: 100,
		Resolution:     10,
		BaseTime:       0,
	})

	if err != nil {
		t.Fatal(err)
	}

	vals := []string{"a", "b", "c", "d"}
	pld := []byte("byte")

	err = bkt.Put(20, 1, vals, pld)
	if err != nil {
		t.Fatal(err)
	}
}

func TestDefaultBucketGet(t *testing.T) {
	files := []string{
		"/tmp/test_0.block",
		"/tmp/test_0_0.index",
		"/tmp/test_0_1.index",
		"/tmp/test_0_2.index",
		"/tmp/test_0_3.index",
	}

	for _, f := range files {
		defer os.Remove(f)
	}

	bkt, err := NewDefaultBucket(DefaultBucketOpts{
		DatabaseName:   "test",
		DataPath:       "/tmp/",
		Partitions:     4,
		IndexDepth:     4,
		PayloadSize:    4,
		BucketDuration: 100,
		Resolution:     10,
		BaseTime:       0,
	})

	if err != nil {
		t.Fatal(err)
	}

	vals := []string{"a", "b", "c", "d"}
	pld := []byte{1, 2, 3, 4}

	err = bkt.Put(20, 1, vals, pld)
	if err != nil {
		t.Fatal(err)
	}

	out, err := bkt.Get(1, 10, 40, vals)
	if err != nil {
		t.Fatal(err)
	}

	if len(out) != 3 || !reflect.DeepEqual(out[1], pld) {
		t.Fatal("incorrect values")
	}
}

func TestDefaultBucketFind(t *testing.T) {
	files := []string{
		"/tmp/test_0.block",
		"/tmp/test_0_0.index",
		"/tmp/test_0_1.index",
		"/tmp/test_0_2.index",
		"/tmp/test_0_3.index",
	}

	for _, f := range files {
		defer os.Remove(f)
	}

	bkt, err := NewDefaultBucket(DefaultBucketOpts{
		DatabaseName:   "test",
		DataPath:       "/tmp/",
		Partitions:     4,
		IndexDepth:     4,
		PayloadSize:    4,
		BucketDuration: 100,
		Resolution:     10,
		BaseTime:       0,
	})

	if err != nil {
		t.Fatal(err)
	}

	val1 := []string{"a", "b", "c", "d"}
	pld1 := []byte{1, 2, 3, 4}
	err = bkt.Put(20, 1, val1, pld1)
	if err != nil {
		t.Fatal(err)
	}

	val2 := []string{"a", "b", "c", "e"}
	pld2 := []byte{5, 6, 7, 8}
	err = bkt.Put(20, 1, val2, pld2)
	if err != nil {
		t.Fatal(err)
	}

	vals := []string{"a", "b", "c", ""}
	out, err := bkt.Find(1, 20, 30, vals)
	if err != nil {
		t.Fatal(err)
	}

	if len(out) != 2 {
		t.Fatal("incorrect number of results")
	}

	for el, res := range out {
		if reflect.DeepEqual(el.Values, val1) {
			if len(res) != 1 || !reflect.DeepEqual(res[0], pld1) {
				t.Fatal("incorrect payload")
			}
		} else if reflect.DeepEqual(el.Values, val2) {
			if len(res) != 1 || !reflect.DeepEqual(res[0], pld2) {
				t.Fatal("incorrect payload")
			}
		} else {
			t.Fatal("incorrect result element")
		}
	}
}

func BenchmarkDefaultBucketPut(b *testing.B) {
	files := []string{
		"/tmp/test_0.block",
		"/tmp/test_0_0.index",
		"/tmp/test_0_1.index",
		"/tmp/test_0_2.index",
		"/tmp/test_0_3.index",
	}

	for _, f := range files {
		defer os.Remove(f)
	}

	bkt, err := NewDefaultBucket(DefaultBucketOpts{
		DatabaseName:   "test",
		DataPath:       "/tmp/",
		Partitions:     4,
		IndexDepth:     4,
		PayloadSize:    4,
		BucketDuration: 100,
		Resolution:     10,
		BaseTime:       0,
	})

	if err != nil {
		b.Fatal(err)
	}

	vals := []string{"a", "b", "c", "d"}
	pld := []byte("byte")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = bkt.Put(20, 1, vals, pld)

		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDefaultBucketGet(b *testing.B) {
	files := []string{
		"/tmp/test_0.block",
		"/tmp/test_0_0.index",
		"/tmp/test_0_1.index",
		"/tmp/test_0_2.index",
		"/tmp/test_0_3.index",
	}

	for _, f := range files {
		defer os.Remove(f)
	}

	bkt, err := NewDefaultBucket(DefaultBucketOpts{
		DatabaseName:   "test",
		DataPath:       "/tmp/",
		Partitions:     4,
		IndexDepth:     4,
		PayloadSize:    4,
		BucketDuration: 100,
		Resolution:     10,
		BaseTime:       0,
	})

	if err != nil {
		b.Fatal(err)
	}

	vals := []string{"a", "b", "c", "d"}
	pld := []byte{1, 2, 3, 4}

	err = bkt.Put(20, 1, vals, pld)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = bkt.Get(1, 10, 40, vals)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func _BenchmarkDefaultBucketFind(b *testing.B, n int) {
	files := []string{
		"/tmp/test_0.block",
		"/tmp/test_0_0.index",
		"/tmp/test_0_1.index",
		"/tmp/test_0_2.index",
		"/tmp/test_0_3.index",
	}

	for _, f := range files {
		defer os.Remove(f)
	}

	bkt, err := NewDefaultBucket(DefaultBucketOpts{
		DatabaseName:   "test",
		DataPath:       "/tmp/",
		Partitions:     4,
		IndexDepth:     4,
		PayloadSize:    4,
		BucketDuration: 100,
		Resolution:     10,
		BaseTime:       0,
	})

	if err != nil {
		b.Fatal(err)
	}

	pld := []byte{1, 2, 3, 4}

	for i := 0; i < n; i++ {
		vals := []string{"a", "b", "c", "d"}
		r := rand.Intn(10)
		vals[i%4] = vals[i%4] + strconv.Itoa(r)
		pno := int64(i % 4)

		err = bkt.Put(20, pno, vals, pld)
		if err != nil {
			b.Fatal(err)
		}
	}

	query := []string{"a", "b", "c", ""}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pno := int64(i % 4)

		_, err = bkt.Find(pno, 20, 30, query)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDefaultBucketFind1(b *testing.B) {
	_BenchmarkDefaultBucketFind(b, 10)
}

func BenchmarkDefaultBucketFind1K(b *testing.B) {
	_BenchmarkDefaultBucketFind(b, 1000)
}

func BenchmarkDefaultBucketFind1M(b *testing.B) {
	_BenchmarkDefaultBucketFind(b, 1000000)
}

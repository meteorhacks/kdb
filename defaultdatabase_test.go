package kdb

import (
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestNewDefaultDatabase(t *testing.T) {
	db, err := NewDefaultDatabase(DefaultDatabaseOpts{
		DatabaseName:   "test",
		DataPath:       "/tmp/",
		Partitions:     4,
		IndexDepth:     4,
		PayloadSize:    4,
		BucketDuration: 100,
		Resolution:     10,
	})

	if err != nil {
		t.Fatal(err)
	}

	if len(db.Buckets) != 0 {
		t.Fatal("number of initial buckets should be 0")
	}
}

func TestDefaultDatabasePut(t *testing.T) {
	files := []string{
		"/tmp/test_0.block",
		"/tmp/test_0_0.index",
		"/tmp/test_0_1.index",
		"/tmp/test_0_2.index",
		"/tmp/test_0_3.index",
		"/tmp/test_100.block",
		"/tmp/test_100_0.index",
		"/tmp/test_100_1.index",
		"/tmp/test_100_2.index",
		"/tmp/test_100_3.index",
	}

	for _, f := range files {
		defer os.Remove(f)
	}

	db, err := NewDefaultDatabase(DefaultDatabaseOpts{
		DatabaseName:   "test",
		DataPath:       "/tmp/",
		Partitions:     4,
		IndexDepth:     4,
		PayloadSize:    4,
		BucketDuration: 100,
		Resolution:     10,
	})

	if err != nil {
		t.Fatal(err)
	}

	vals := []string{"a", "b", "c", "d"}
	pld := []byte{1, 2, 3, 4}

	// insert valid data
	if err = db.Put(20, 1, vals, pld); err != nil {
		t.Fatal(err)
	}

	if len(db.Buckets) != 1 {
		t.Fatal("number of buckets should be 1")
	}

	// insert another valid data (different bucket)
	if err = db.Put(100, 1, vals, pld); err != nil {
		t.Fatal(err)
	}

	if len(db.Buckets) != 2 {
		t.Fatal("number of buckets should be 2")
	}

	// test incorrect timestamp
	ts := time.Now().UnixNano() + 1000
	if err = db.Put(ts, 1, vals, pld); err != ErrDefaultDatabaseInvalidTimestamp {
		t.Fatal(err)
	}

	// incorrect partition number
	if err = db.Put(20, 5, vals, pld); err != ErrDefaultDatabaseInvalidPartition {
		t.Fatal(err)
	}

	// invalid values
	if err = db.Put(20, 1, append(vals, "e"), pld); err != ErrDefaultDatabaseInvalidIndexValues {
		t.Fatal(err)
	}
	if err = db.Put(20, 1, vals[:2], pld); err != ErrDefaultDatabaseInvalidIndexValues {
		t.Fatal(err)
	}

	// invalid payload
	if err = db.Put(20, 1, vals, append(pld, 5)); err != ErrDefaultDatabaseInvalidPayload {
		t.Fatal(err)
	}
	if err = db.Put(20, 1, vals, pld[:2]); err != ErrDefaultDatabaseInvalidPayload {
		t.Fatal(err)
	}
}

func TestDefaultDatabaseGet(t *testing.T) {
	files := []string{
		"/tmp/test_0.block",
		"/tmp/test_0_0.index",
		"/tmp/test_0_1.index",
		"/tmp/test_0_2.index",
		"/tmp/test_0_3.index",
		"/tmp/test_100.block",
		"/tmp/test_100_0.index",
		"/tmp/test_100_1.index",
		"/tmp/test_100_2.index",
		"/tmp/test_100_3.index",
	}

	for _, f := range files {
		defer os.Remove(f)
	}

	db, err := NewDefaultDatabase(DefaultDatabaseOpts{
		DatabaseName:   "test",
		DataPath:       "/tmp/",
		Partitions:     4,
		IndexDepth:     4,
		PayloadSize:    4,
		BucketDuration: 100,
		Resolution:     10,
	})

	if err != nil {
		t.Fatal(err)
	}

	val1 := []string{"a", "b", "c", "d"}
	val2 := []string{"a", "b", "c", "e"}
	pld0 := []byte{0, 0, 0, 0}
	pld1 := []byte{1, 2, 3, 4}
	pld2 := []byte{5, 6, 7, 8}

	// write first payload at the end of first
	if err = db.Put(90, 1, val1, pld1); err != nil {
		t.Fatal(err)
	}

	// Get request on a single bucket
	exp := [][]byte{pld1}
	out, err := db.Get(1, 90, 100, val1)
	if err != nil {
		t.Fatal(err)
	}

	// test Get requests on multiple buckets with 3 different scenarios

	// case: [... value] [empty ...]
	exp = [][]byte{pld1, pld0}
	out, err = db.Get(1, 90, 110, val1)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(out, exp) {
		t.Fatal("invalid output")
	}

	// write second payload at the start of second bucket
	if err = db.Put(100, 1, val2, pld2); err != nil {
		t.Fatal(err)
	}

	// case: [... empty] [value ...]
	exp = [][]byte{pld0, pld2}
	out, err = db.Get(1, 90, 110, val2)
	if err != nil {
		t.Fatal(err)
	}

	// write second payload at the start of second bucket
	// but use value set equal to one used with first payload
	if err = db.Put(100, 1, val1, pld2); err != nil {
		t.Fatal(err)
	}

	// case: [... value1] [value2 ...]
	exp = [][]byte{pld1, pld2}
	out, err = db.Get(1, 90, 110, val1)
	if err != nil {
		t.Fatal(err)
	}
}

func BenchmarkDefaultDatabasePut(b *testing.B) {
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

	db, err := NewDefaultDatabase(DefaultDatabaseOpts{
		DatabaseName:   "test",
		DataPath:       "/tmp/",
		Partitions:     4,
		IndexDepth:     4,
		PayloadSize:    4,
		BucketDuration: 100,
		Resolution:     10,
	})

	if err != nil {
		b.Fatal(err)
	}

	pld := []byte{1, 2, 3, 4}
	// run a benchmark on a db with 4 partitions and 2 buckets

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vals := []string{"a", "b", "c", "d"}
		r := rand.Intn(10)
		vals[i%4] = vals[i%4] + strconv.Itoa(r)
		pno := int64(i % 4)
		ts := 10 * rand.Int63n(20)

		if err = db.Put(ts, pno, vals, pld); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDefaultDatabaseGet(b *testing.B) {
	files := []string{
		"/tmp/test_0.block",
		"/tmp/test_0_0.index",
		"/tmp/test_0_1.index",
		"/tmp/test_0_2.index",
		"/tmp/test_0_3.index",
		"/tmp/test_100.block",
		"/tmp/test_100_0.index",
		"/tmp/test_100_1.index",
		"/tmp/test_100_2.index",
		"/tmp/test_100_3.index",
	}

	for _, f := range files {
		defer os.Remove(f)
	}

	db, err := NewDefaultDatabase(DefaultDatabaseOpts{
		DatabaseName:   "test",
		DataPath:       "/tmp/",
		Partitions:     4,
		IndexDepth:     4,
		PayloadSize:    4,
		BucketDuration: 100,
		Resolution:     10,
	})

	if err != nil {
		b.Fatal(err)
	}

	// fill test data for 4 partitions and 2 buckets
	pld := []byte{1, 2, 3, 4}

	for i := 0; i < b.N; i++ {
		vals := []string{"a", "b", "c", "d"}
		r := rand.Intn(10)
		vals[i%4] = vals[i%4] + strconv.Itoa(r)
		pno := int64(i % 4)
		ts := 10 * rand.Int63n(20)

		if err = db.Put(ts, pno, vals, pld); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := 10 * rand.Int63n(10)
		end := start + 10*rand.Int63n(10)
		vals := []string{"a", "b", "c", "d"}
		r := rand.Intn(10)
		vals[i%4] = vals[i%4] + strconv.Itoa(r)
		pno := int64(i % 4)

		_, err := db.Get(pno, start, end, vals)
		if err != nil {
			b.Fatal(err)
		}
	}
}

package dbase

import (
	"errors"
	"os/exec"
	"reflect"
	"testing"

	"github.com/meteorhacks/kdb/clock"
)

// A test clock is used to control the time
// hot time range:  10000 --- 12000
// anything below 10000 is cold
// cold data available at 30
// anything above 11999 is future
func createTestDbase() (db *DBase, err error) {
	clock.UseTestClock(40)
	defer clock.UseTestClock(11999)

	cmd := exec.Command("rm", "-rf", "/tmp/test-dbase")
	if err := cmd.Run(); err != nil {
		return nil, err
	}

	opts := Options{
		DatabaseName:   "test",
		DataPath:       "/tmp/test-dbase/",
		IndexDepth:     4,
		PayloadSize:    4,
		BucketDuration: 1000,
		Resolution:     10,
		SegmentSize:    10,
	}

	db, err = New(opts)
	if err == nil && db == nil {
		err = errors.New("database should not be nil")
		return nil, err
	}

	// test cold data
	val := []string{"a", "b", "c", "d"}
	pld := []byte{3, 0, 3, 0}

	if err := db.Put(30, val, pld); err != nil {
		return nil, err
	}

	db.Close()

	clock.UseTestClock(11999)

	// open the database again so cold bucket
	// is not ready when running tests
	db, err = New(opts)
	if err == nil && db == nil {
		err = errors.New("database should not be nil")
		return nil, err
	}

	return db, err
}

// deletes all files created for test db
// should be run at the end of each test
func cleanTestFiles() {
	cmd := exec.Command("rm", "-rf", "/tmp/test-dbase")
	cmd.Run()
}

//    Tests
// -----------

func TestNewDBaseNewData(t *testing.T) {
	defer cleanTestFiles()

	db, err := createTestDbase()
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	if db.HBuckets.Length() != MaxHotBuckets {
		t.Fatal("number of hot buckets !=", MaxHotBuckets)
	}

	if db.CBuckets.Length() != 0 {
		t.Fatal("number of cold buckets !=", 0)
	}

	var i int64
	for i = 0; i < MaxHotBuckets; i++ {
		ts := clock.Now() - i*db.BucketDuration
		ts -= ts % db.BucketDuration
		if _, err := db.HBuckets.Get(ts); err != nil {
			t.Fatal("correct bucket should be loaded")
		}
	}
}

func TestNewDBaseExistingData(t *testing.T) {
	defer cleanTestFiles()

	db, err := createTestDbase()
	if err != nil {
		t.Fatal(err)
	}

	db.Close()

	db, err = createTestDbase()
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	if db.HBuckets.Length() != MaxHotBuckets {
		t.Fatal("number of hot buckets !=", MaxHotBuckets)
	}

	if db.CBuckets.Length() != 0 {
		t.Fatal("number of cold buckets !=", 0)
	}

	var i int64
	for i = 0; i < MaxHotBuckets; i++ {
		ts := clock.Now() - i*db.BucketDuration
		ts -= ts % db.BucketDuration
		if _, err := db.HBuckets.Get(ts); err != nil {
			t.Fatal("correct bucket should be loaded")
		}
	}
}

func TestPut(t *testing.T) {
	defer cleanTestFiles()

	db, err := createTestDbase()
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	vals := []string{"a", "b", "c", "d"}
	pld1 := []byte{1, 2, 3, 4}
	pld2 := []byte{5, 6, 7, 8}

	err = db.Put(10990, vals, pld1)
	if err != nil {
		t.Fatal(err)
	}

	err = db.Put(11000, vals, pld2)
	if err != nil {
		t.Fatal(err)
	}

	res, err := db.Get(10990, 11010, vals)
	if err != nil {
		t.Fatal(err)
	}

	if len(res) != 2 ||
		!reflect.DeepEqual(res[0], pld1) ||
		!reflect.DeepEqual(res[1], pld2) {
		t.Fatal("invalid data")
	}

	// TEST INPUT VALIDATION
	ts := clock.Now() + 1
	err = db.Put(ts, vals, pld1)
	if err == nil {
		t.Fatal("should throw an error")
	}

	err = db.Put(9999, vals, pld1)
	if err == nil {
		t.Fatal("should throw an error")
	}

	err = db.Put(11010, append(vals, "e"), pld1)
	if err == nil {
		t.Fatal("should throw an error")
	}

	err = db.Put(11010, vals[:2], pld1)
	if err == nil {
		t.Fatal("should throw an error")
	}

	err = db.Put(11010, vals, append(pld1, 5))
	if err == nil {
		t.Fatal("should throw an error")
	}

	err = db.Put(11010, vals, pld1[:2])
	if err == nil {
		t.Fatal("should throw an error")
	}
}

func TestGetHot(t *testing.T) {
	defer cleanTestFiles()

	db, err := createTestDbase()
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	vals := []string{"a", "b", "c", "d"}
	pld0 := []byte{0, 0, 0, 0}
	pld := []byte{1, 2, 3, 4}

	// try getting from a cold bucket
	res, err := db.Get(10, 20, vals)
	if err != nil {
		t.Fatal(err)
	}

	if len(res) != 1 || !reflect.DeepEqual(res[0], pld0) {
		t.Fatal("invalid data")
	}

	err = db.Put(10999, vals, pld)
	if err != nil {
		t.Fatal(err)
	}

	res, err = db.Get(10990, 11000, vals)
	if err != nil {
		t.Fatal(err)
	}

	if len(res) != 1 || !reflect.DeepEqual(res[0], pld) {
		t.Fatal("invalid data")
	}

	// TEST INPUT VALIDATION
	ts := clock.Now() + 1
	_, err = db.Get(ts, ts+10, vals)
	if err == nil {
		t.Fatal("should throw an error")
	}

	_, err = db.Get(ts-10, ts, vals)
	if err == nil {
		t.Fatal("should throw an error")
	}

	_, err = db.Get(11000, 10990, vals)
	if err == nil {
		t.Fatal("should throw an error")
	}

	_, err = db.Get(10990, 11000, append(vals, "e"))
	if err == nil {
		t.Fatal("should throw an error")
	}

	_, err = db.Get(10990, 11000, vals[:2])
	if err == nil {
		t.Fatal("should throw an error")
	}
}

func TestFind(t *testing.T) {
	defer cleanTestFiles()

	db, err := createTestDbase()
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	vals := []string{"a", "b", "c", ""}
	val1 := []string{"a", "b", "c", "d"}
	val2 := []string{"a", "b", "c", "e"}
	pld0 := []byte{0, 0, 0, 0}
	pld1 := []byte{1, 2, 3, 4}
	pld2 := []byte{5, 6, 7, 8}

	err = db.Put(10990, val1, pld1)
	if err != nil {
		t.Fatal(err)
	}

	err = db.Put(11000, val2, pld2)
	if err != nil {
		t.Fatal(err)
	}

	out, err := db.Find(10990, 11010, vals)
	if err != nil {
		t.Fatal(err)
	}

	for el, plds := range out {
		if reflect.DeepEqual(el.Values, val1) {
			exp := [][]byte{pld1, pld0}
			if !reflect.DeepEqual(plds, exp) {
				t.Fatal("invalid payload")
			}
		} else if reflect.DeepEqual(el.Values, val2) {
			exp := [][]byte{pld0, pld2}
			if !reflect.DeepEqual(plds, exp) {
				t.Fatal("invalid payload")
			}
		} else {
			t.Fatal("invalid index values")
		}
	}
}

//    Benchmarks
// ----------------

func BenchmarkPut(b *testing.B) {
	defer cleanTestFiles()

	db, err := createTestDbase()
	if err != nil {
		b.Fatal(err)
	}

	defer db.Close()

	pld := []byte{1, 2, 3, 4}
	vals := []string{"a", "b", "c", "d"}

	var i int64
	var N int64 = int64(b.N)

	b.ResetTimer()
	for i = 0; i < N; i++ {
		ts := 11000 + (10*i)%1000
		db.Put(ts, vals, pld)
	}
}

// TODO: randomize
func BenchmarkGet(b *testing.B) {
	defer cleanTestFiles()

	db, err := createTestDbase()
	if err != nil {
		b.Fatal(err)
	}

	defer db.Close()

	vals := []string{"a", "b", "c", "d"}
	pld := []byte{1, 2, 3, 4}

	err = db.Put(10999, vals, pld)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Get(10990, 11000, vals)
	}
}

// TODO: randomize
func BenchmarkFind(b *testing.B) {
	defer cleanTestFiles()

	db, err := createTestDbase()
	if err != nil {
		b.Fatal(err)
	}

	defer db.Close()

	vals := []string{"a", "b", "c", ""}
	val1 := []string{"a", "b", "c", "d"}
	pld := []byte{1, 2, 3, 4}

	err = db.Put(10999, val1, pld)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Find(10990, 11000, vals)
	}
}

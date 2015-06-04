package dbase

import (
	"errors"
	"os/exec"
	"reflect"
	"testing"
	"time"
)

func TestNewDBaseNewData(t *testing.T) {
	defer cleanTestFiles()

	db, err := createTestDbase()
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	if len(db.Buckets) != 1 {
		t.Fatal("number of initial buckets should be 1")
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

	if len(db.Buckets) != 1 {
		t.Fatal("number of initial buckets should be 1")
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

	err = db.Put(990, vals, pld1)
	if err != nil {
		t.Fatal(err)
	}

	if len(db.Buckets) != 2 {
		t.Fatal("number of buckets should be 2")
	}

	err = db.Put(1000, vals, pld2)
	if err != nil {
		t.Fatal(err)
	}

	res, err := db.Get(990, 1010, vals)
	if err != nil {
		t.Fatal(err)
	}

	if len(res) != 2 ||
		!reflect.DeepEqual(res[0], pld1) ||
		!reflect.DeepEqual(res[1], pld2) {
		t.Fatal("invalid data")
	}

	if len(db.Buckets) != 3 {
		t.Fatal("number of buckets should be 3")
	}

	// TEST INPUT VALIDATION
	ts := time.Now().UnixNano() + 1000
	err = db.Put(ts, vals, pld1)
	if err == nil {
		t.Fatal("should throw an error")
	}

	err = db.Put(1010, append(vals, "e"), pld1)
	if err == nil {
		t.Fatal("should throw an error")
	}

	err = db.Put(1010, vals[:2], pld1)
	if err == nil {
		t.Fatal("should throw an error")
	}

	err = db.Put(1010, vals, append(pld1, 5))
	if err == nil {
		t.Fatal("should throw an error")
	}

	err = db.Put(1010, vals, pld1[:2])
	if err == nil {
		t.Fatal("should throw an error")
	}
}

func TestGet(t *testing.T) {
	defer cleanTestFiles()

	db, err := createTestDbase()
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	vals := []string{"a", "b", "c", "d"}
	pld := []byte{1, 2, 3, 4}

	err = db.Put(99, vals, pld)
	if err != nil {
		t.Fatal(err)
	}

	res, err := db.Get(90, 100, vals)
	if err != nil {
		t.Fatal(err)
	}

	if len(res) != 1 || !reflect.DeepEqual(res[0], pld) {
		t.Fatal("invalid data")
	}

	// TEST INPUT VALIDATION
	ts := time.Now().UnixNano() + 1000
	_, err = db.Get(ts, ts+10, vals)
	if err == nil {
		t.Fatal("should throw an error")
	}

	_, err = db.Get(ts-100, ts, vals)
	if err == nil {
		t.Fatal("should throw an error")
	}

	_, err = db.Get(100, 90, vals)
	if err == nil {
		t.Fatal("should throw an error")
	}

	_, err = db.Get(90, 110, append(vals, "e"))
	if err == nil {
		t.Fatal("should throw an error")
	}

	_, err = db.Get(90, 110, vals[:2])
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

	err = db.Put(90, val1, pld1)
	if err != nil {
		t.Fatal(err)
	}

	err = db.Put(100, val2, pld2)
	if err != nil {
		t.Fatal(err)
	}

	out, err := db.Find(90, 110, vals)
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
		ts := (10 * i) % 1000
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

	err = db.Put(99, vals, pld)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Get(90, 100, vals)
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

	err = db.Put(99, val1, pld)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Find(90, 100, vals)
	}
}

// ---------- //

func createTestDbase() (db *DBase, err error) {
	cmd := exec.Command("rm", "-rf", "/tmp/test-dbase")
	if err := cmd.Run(); err != nil {
		return nil, err
	}

	db, err = New(Options{
		DatabaseName:   "test",
		DataPath:       "/tmp/test-dbase/",
		IndexDepth:     4,
		PayloadSize:    4,
		BucketDuration: 1000,
		Resolution:     10,
		SegmentSize:    10,
	})

	if err == nil && db == nil {
		err = errors.New("database should not be nil")
		return nil, err
	}

	return db, err
}

func cleanTestFiles() {
	cmd := exec.Command("rm", "-rf", "/tmp/test-dbase")
	cmd.Run()
}

package dbucket

import (
	"errors"
	"os/exec"
	"reflect"
	"testing"
)

func TestNewBucketNewData(t *testing.T) {
	defer cleanTestFiles()

	bkt, err := createTestBucket()
	if err != nil {
		t.Fatal(err)
	}

	defer bkt.Close()
}

func TestNewBucketExistingData(t *testing.T) {
	defer cleanTestFiles()

	bkt, err := createTestBucket()
	if err != nil {
		t.Fatal(err)
	}

	bkt.Close()

	bkt, err = createTestBucket()
	if err != nil {
		t.Fatal(err)
	}

	defer bkt.Close()
}

func TestPutAndGet(t *testing.T) {
	defer cleanTestFiles()

	bkt, err := createTestBucket()
	if err != nil {
		t.Fatal(err)
	}

	defer bkt.Close()

	vals := []string{"a", "b", "c", "d"}
	pld := []byte{1, 2, 3, 4}

	err = bkt.Put(30, vals, pld)
	if err != nil {
		t.Fatal(err)
	}

	res, err := bkt.Get(0, 50, vals)
	if err != nil {
		t.Fatal(err)
	}

	exp := [][]byte{
		[]byte{0, 0, 0, 0},
		[]byte{0, 0, 0, 0},
		[]byte{0, 0, 0, 0},
		[]byte{1, 2, 3, 4},
		[]byte{0, 0, 0, 0},
	}

	if len(res) != 5 {
		t.Fatal("result should have 5 items")
	}

	for i, pld := range res {
		if !reflect.DeepEqual(exp[i], pld) {
			t.Fatal("invalid response")
		}
	}
}

func TestPutAndFind(t *testing.T) {
	defer cleanTestFiles()

	bkt, err := createTestBucket()
	if err != nil {
		t.Fatal(err)
	}

	val1 := []string{"a", "b", "c", "d"}
	pld1 := []byte{1, 2, 3, 4}

	err = bkt.Put(20, val1, pld1)
	if err != nil {
		t.Fatal(err)
	}

	val2 := []string{"a", "b", "c", "e"}
	pld2 := []byte{5, 6, 7, 8}

	err = bkt.Put(20, val2, pld2)
	if err != nil {
		t.Fatal(err)
	}

	vals := []string{"a", "b", "c", ""}
	out, err := bkt.Find(20, 30, vals)
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

func BenchmarkPut(b *testing.B) {
	defer cleanTestFiles()

	bkt, err := createTestBucket()
	if err != nil {
		b.Fatal(err)
	}

	vals := []string{"a", "b", "c", "d"}
	pld := []byte{1, 2, 3, 4}

	var i int64
	N := int64(b.N)

	b.ResetTimer()
	for i = 0; i < N; i++ {
		ts := (i * 10) % 1000
		bkt.Put(ts, vals, pld)
	}
}

// TODO: randomize
func BenchmarkGet(b *testing.B) {
	defer cleanTestFiles()

	bkt, err := createTestBucket()
	if err != nil {
		b.Fatal(err)
	}

	vals := []string{"a", "b", "c", "d"}
	pld := []byte{1, 2, 3, 4}

	err = bkt.Put(30, vals, pld)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		bkt.Get(0, 50, vals)
	}
}

// TODO: randomize
func BenchmarkFind(b *testing.B) {
	defer cleanTestFiles()

	bkt, err := createTestBucket()
	if err != nil {
		b.Fatal(err)
	}

	val1 := []string{"a", "b", "c", "d"}
	pld1 := []byte{1, 2, 3, 4}

	err = bkt.Put(20, val1, pld1)
	if err != nil {
		b.Fatal(err)
	}

	val2 := []string{"a", "b", "c", "e"}
	pld2 := []byte{5, 6, 7, 8}

	err = bkt.Put(20, val2, pld2)
	if err != nil {
		b.Fatal(err)
	}

	vals := []string{"a", "b", "c", ""}

	for i := 0; i < b.N; i++ {
		bkt.Find(20, 30, vals)
	}
}

// ---------- //

// create a default bucket with test settings
func createTestBucket() (bkt *DBucket, err error) {
	cmd := exec.Command("rm", "-rf", "/tmp/test-dbucket")
	if err := cmd.Run(); err != nil {
		return nil, err
	}

	bkt, err = New(Options{
		DatabaseName:   "test",
		DataPath:       "/tmp/test-dbucket/",
		IndexDepth:     4,
		PayloadSize:    4,
		BucketDuration: 1000,
		Resolution:     10,
		SegmentSize:    10,
		BaseTime:       0,
	})

	if err == nil && bkt == nil {
		err = errors.New("bucket should not be nil")
		return nil, err
	}

	return bkt, err
}

func cleanTestFiles() {
	cmd := exec.Command("rm", "-rf", "/tmp/test-dbucket")
	cmd.Run()
}

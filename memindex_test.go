package kdb

import (
	"io/ioutil"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"testing"
)

func TestNewMemIndexNewFile(t *testing.T) {
	fpath := "/tmp/i1"
	defer os.Remove(fpath)

	idx, err := NewMemIndex(MemIndexOpts{
		FilePath:   fpath,
		IndexDepth: 4,
	})

	if err != nil {
		t.Fatal(err)
	}

	if idx.root == nil {
		t.Fatal("index should have a root element")
	}

	fd, err := os.OpenFile(fpath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}

	data, err := ioutil.ReadAll(fd)
	if err != nil {
		t.Fatal(err)
	}

	if err := fd.Close(); err != nil {
		t.Fatal(err)
	}

	if string(data) != "" {
		t.Fatal("initially index should be empty")
	}
}

func TestNewMemIndexExistingFile(t *testing.T) {
	fpath := "/tmp/i1"
	defer os.Remove(fpath)

	idx, err := NewMemIndex(MemIndexOpts{
		FilePath:   fpath,
		IndexDepth: 4,
	})

	if err != nil {
		t.Fatal(err)
	}

	vals := []string{"a", "b", "c", "d"}
	el, err := idx.Add(vals, 100)

	idx.Close()

	idx2, err := NewMemIndex(MemIndexOpts{
		FilePath:   fpath,
		IndexDepth: 4,
	})

	el2, err := idx2.Get(vals)

	if err != nil {
		t.Fatal(err)
	}

	if el.Position != el2.Position ||
		!reflect.DeepEqual(el.Values, el2.Values) {
		t.Fatal("should return a valid element")
	}
}

func TestNewMemIndexCorruptFile(t *testing.T) {
	// TODO
}

func TestMemIndexAdd(t *testing.T) {
	fpath := "/tmp/i1"
	defer os.Remove(fpath)

	idx, err := NewMemIndex(MemIndexOpts{
		FilePath:   fpath,
		IndexDepth: 4,
	})

	if err != nil {
		t.Fatal(err)
	}

	vals := []string{"a", "b", "c", "d"}
	el, err := idx.Add(vals, 100)

	if err != nil {
		t.Fatal(err)
	}

	if el == nil || len(el.Children) != 0 ||
		!reflect.DeepEqual(el.Values, vals) ||
		el != idx.root.Children["a"].Children["b"].Children["c"].Children["d"] {
		t.Fatal("should return a valid element")
	}
}

func TestMemIndexGet(t *testing.T) {
	// TODO
}

func TestMemIndexFind(t *testing.T) {
	// TODO
}

func BenchmarkMemIndexAdd(b *testing.B) {
	fpath := "/tmp/i1"
	defer os.Remove(fpath)

	idx, err := NewMemIndex(MemIndexOpts{
		FilePath:   fpath,
		IndexDepth: 4,
	})

	if err != nil {
		b.Fatal(err)
	}

	baseVals := []string{"a", "b", "c", "d"}

	for i := 0; i < b.N; i++ {
		vals := baseVals
		r := rand.Intn(10)
		vals[i%4] = vals[i%4] + strconv.Itoa(r)

		b.StartTimer()
		_, err := idx.Add(vals, 100)
		b.StopTimer()

		if err != nil {
			b.Fatal(err)
		}
	}
}

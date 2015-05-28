package kdb

import (
	"io/ioutil"
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
	fpath := "/tmp/i1"
	defer os.Remove(fpath)

	fd, err := os.OpenFile(fpath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}

	fd.WriteString("blah blah blah")

	if err := fd.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = NewMemIndex(MemIndexOpts{
		FilePath:   fpath,
		IndexDepth: 4,
	})

	if err == nil {
		t.Fatal("should return an error")
	}
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
	_, err = idx.Add(vals, 100)

	el2, err := idx.Get(vals)
	if err != nil {
		t.Fatal(err)
	}

	if el2.Position != 100 {
		t.Fatal("should return correct position")
	}
}

func TestMemIndexFind(t *testing.T) {
	fpath := "/tmp/i1"
	defer os.Remove(fpath)

	idx, err := NewMemIndex(MemIndexOpts{
		FilePath:   fpath,
		IndexDepth: 4,
	})

	if err != nil {
		t.Fatal(err)
	}

	_, err = idx.Add([]string{"a", "b", "c", "d"}, 100)
	_, err = idx.Add([]string{"a", "b", "c", "e"}, 200)
	_, err = idx.Add([]string{"a", "b", "f", "d"}, 300)

	els, err := idx.Find([]string{"a", "b", "", "d"})
	if err != nil {
		t.Fatal(err)
	}

	if len(els) != 2 {
		t.Fatal("should return correct number of elements")
	}

	var el1, el2 *IndexElement
	if els[0].Position == 100 {
		el1 = els[0]
		el2 = els[1]
	} else {
		el1 = els[1]
		el2 = els[0]
	}

	if el1.Position != 100 || el2.Position != 300 {
		t.Fatal("should return correct elements")
	}
}

func BenchmarkMemIndexAdd(b *testing.B) {
	b.StopTimer()

	fpath := "/tmp/i1"
	defer os.Remove(fpath)

	idx, err := NewMemIndex(MemIndexOpts{
		FilePath:   fpath,
		IndexDepth: 4,
	})

	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		vals := []string{"a", "b", "c", "d"}
		vals[i%4] = vals[i%4] + strconv.Itoa(i)

		b.StartTimer()
		_, err := idx.Add(vals, 100)
		b.StopTimer()

		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMemIndexGet(b *testing.B) {
	b.StopTimer()

	fpath := "/tmp/i1"
	defer os.Remove(fpath)

	idx, err := NewMemIndex(MemIndexOpts{
		FilePath:   fpath,
		IndexDepth: 4,
	})

	if err != nil {
		b.Fatal(err)
	}

	vals := []string{"a", "b", "c", "d"}
	_, err = idx.Add(vals, 100)

	for i := 0; i < b.N; i++ {
		b.StartTimer()
		_, err := idx.Get(vals)
		b.StopTimer()

		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMemIndexFindQueryInMiddle(b *testing.B) {
	b.StopTimer()

	fpath := "/tmp/i1"
	defer os.Remove(fpath)

	idx, err := NewMemIndex(MemIndexOpts{
		FilePath:   fpath,
		IndexDepth: 4,
	})

	if err != nil {
		b.Fatal(err)
	}

	_, err = idx.Add([]string{"a", "b", "c", "d"}, 100)
	_, err = idx.Add([]string{"a", "b", "c", "e"}, 200)
	_, err = idx.Add([]string{"a", "b", "f", "d"}, 300)

	for i := 0; i < b.N; i++ {
		b.StartTimer()
		_, err = idx.Find([]string{"a", "b", "", "d"})
		b.StopTimer()

		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMemIndexFindQueryAtEnd(b *testing.B) {
	b.StopTimer()

	fpath := "/tmp/i1"
	defer os.Remove(fpath)

	idx, err := NewMemIndex(MemIndexOpts{
		FilePath:   fpath,
		IndexDepth: 4,
	})

	if err != nil {
		b.Fatal(err)
	}

	_, err = idx.Add([]string{"a", "b", "c", "d"}, 100)
	_, err = idx.Add([]string{"a", "b", "c", "e"}, 200)
	_, err = idx.Add([]string{"a", "b", "f", "d"}, 300)

	for i := 0; i < b.N; i++ {
		b.StartTimer()
		_, err = idx.Find([]string{"a", "b", "c", ""})
		b.StopTimer()

		if err != nil {
			b.Fatal(err)
		}
	}
}

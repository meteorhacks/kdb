package kdb

import (
	"os"
	"testing"
)

func TestPsliceSetGet(t *testing.T) {
	filename := "/tmp/data.txt"
	var length int64 = 8
	defer os.Remove(filename)

	slice, err := NewPslice(filename, length)
	if err != nil {
		t.Fatal(err)
	}

	slice.Set(2, 200)
	slice.Close()

	slice2, err := NewPslice(filename, length)
	if err != nil {
		t.Fatal(err)
	}

	if slice2.Get(2) != 200 {
		t.Error("Slice does not persist")
	}
	slice2.Close()
}

func TestPslicePreventMultiLoading(t *testing.T) {
	filename := "/tmp/data.txt"
	var length int64 = 8
	defer os.Remove(filename)

	handle, err := NewPslice(filename, length)
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()

	if err := handle.load(); err == nil {
		t.Error("loading twice should throw an error")
	}
}

func TestPsliceSetMaximumLength(t *testing.T) {
	filename := "/tmp/data.txt"
	var length int64 = 8
	defer os.Remove(filename)

	slice, err := NewPslice(filename, length)
	if err != nil {
		t.Fatal(err)
	}
	defer slice.Close()
	slice.Set(7, 300)
}

func TestSpliceLoadAndClose(t *testing.T) {
	filename := "/tmp/data.txt"
	var length int64 = 8
	defer os.Remove(filename)

	slice, err := NewPslice(filename, length)
	if err != nil {
		t.Fatal(err)
	}

	if err := slice.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPreventMultiClosing(t *testing.T) {
	filename := "/tmp/data.txt"
	var length int64 = 8
	defer os.Remove(filename)

	slice, err := NewPslice(filename, length)
	if err != nil {
		t.Fatal(err)
	}

	if err := slice.Close(); err != nil {
		t.Fatal(err)
	}

	if err := slice.Close(); err == nil {
		t.Error("closing twice should throw an error")
	}
}

func TestPsliceResizing(t *testing.T) {
	filename := "/tmp/data.txt"
	var length int64 = 8
	defer os.Remove(filename)

	slice, err := NewPslice(filename, length)
	if err != nil {
		t.Fatal(err)
	}
	defer slice.Close()
	slice.Set(7, 300)

	slice.Resize(10)
	if slice.Get(7) != 300 {
		t.Error("resizing should not destroy old data")
	}

	slice.Set(9, 1000)
}

func BenchmarkPsliceSetValues(b *testing.B) {
	filename := "/tmp/data.txt"
	length := int64(b.N)
	defer os.Remove(filename)

	slice, err := NewPslice(filename, length)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	var i int64
	for i = 0; i < length; i++ {
		slice.Set(i, float64(i))
	}

	slice.Close()
}

func BenchmarkPsliceSetSameValueWithSmallSize(b *testing.B) {
	filename := "/tmp/data.txt"
	var length int64 = 10
	defer os.Remove(filename)

	slice, err := NewPslice(filename, length)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	var i int64
	for i = 0; i < int64(b.N); i++ {
		slice.Set(0, float64(i))
	}

	slice.Close()
}

func BenchmarkPsliceGetValues(b *testing.B) {
	filename := "/tmp/data.txt"
	length := int64(b.N)
	defer os.Remove(filename)

	slice, err := NewPslice(filename, length)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	var i int64
	for i = 0; i < length; i++ {
		slice.Get(i)
	}

	slice.Close()
}

func BenchmarkPsliceGetSameValueWithSmallSize(b *testing.B) {
	filename := "/tmp/data.txt"
	var length int64 = 10
	defer os.Remove(filename)

	slice, err := NewPslice(filename, length)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	var i int64
	for i = 0; i < int64(b.N); i++ {
		slice.Get(0)
	}

	slice.Close()
}

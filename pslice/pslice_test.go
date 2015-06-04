package pslice

import (
	"os"
	"testing"
)

func TestSetGet(t *testing.T) {
	filename := "/tmp/data.txt"
	var length int64 = 8
	defer os.Remove(filename)

	slice, err := New(filename, length)
	if err != nil {
		t.Fatal(err)
	}

	slice.Set(2, 200)
	slice.Close()

	slice2, err := New(filename, length)
	if err != nil {
		t.Fatal(err)
	}

	if slice2.Get(2) != 200 {
		t.Error("Slice does not persist")
	}
	slice2.Close()
}

func TestPreventMultiLoading(t *testing.T) {
	filename := "/tmp/data.txt"
	var length int64 = 8
	defer os.Remove(filename)

	handle, err := New(filename, length)
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()

	if err := handle.load(); err == nil {
		t.Error("loading twice should throw an error")
	}
}

func TestSetMaximumLength(t *testing.T) {
	filename := "/tmp/data.txt"
	var length int64 = 8
	defer os.Remove(filename)

	slice, err := New(filename, length)
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

	slice, err := New(filename, length)
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

	slice, err := New(filename, length)
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

func TestResizing(t *testing.T) {
	filename := "/tmp/data.txt"
	var length int64 = 8
	defer os.Remove(filename)

	slice, err := New(filename, length)
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

func BenchmarkSetValues(b *testing.B) {
	filename := "/tmp/data.txt"
	length := int64(b.N)
	defer os.Remove(filename)

	slice, err := New(filename, length)
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

func BenchmarkSetSameValueWithSmallSize(b *testing.B) {
	filename := "/tmp/data.txt"
	var length int64 = 10
	defer os.Remove(filename)

	slice, err := New(filename, length)
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

func BenchmarkGetValues(b *testing.B) {
	filename := "/tmp/data.txt"
	length := int64(b.N)
	defer os.Remove(filename)

	slice, err := New(filename, length)
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

func BenchmarkGetSameValueWithSmallSize(b *testing.B) {
	filename := "/tmp/data.txt"
	var length int64 = 10
	defer os.Remove(filename)

	slice, err := New(filename, length)
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

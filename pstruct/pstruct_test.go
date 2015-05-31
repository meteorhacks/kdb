package pstruct

import (
	"os"
	"testing"
	"unsafe"
)

type SomeType struct {
	d1 int64
	d2 int64
	d3 int64
}

func TestLoadWriteRead(t *testing.T) {
	filename := "/tmp/data.txt"
	size := unsafe.Sizeof(SomeType{})
	defer os.Remove(filename)

	handle := NewPstruct(filename, size)
	if err := handle.Load(); err != nil {
		t.Fatal(err)
	}

	value := (*SomeType)(handle.Pointer)
	value.d3 = 3000

	if err := handle.Unload(); err != nil {
		t.Fatal(err)
	}

	// load again
	handle2 := NewPstruct(filename, size)
	if err := handle2.Load(); err != nil {
		t.Fatal(err)
	}

	value2 := (*SomeType)(handle2.Pointer)

	if value2.d3 != 3000 {
		t.Error("can't read saved values")
	}
}

func TestPreventMultiLoading(t *testing.T) {
	filename := "/tmp/data.txt"
	size := unsafe.Sizeof(SomeType{})
	defer os.Remove(filename)

	handle := NewPstruct(filename, size)
	if err := handle.Load(); err != nil {
		t.Fatal(err)
	}

	if err := handle.Load(); err == nil {
		t.Error("loading twice should throw an error")
	}
}

func TestLoadAndUnload(t *testing.T) {
	filename := "/tmp/data.txt"
	size := unsafe.Sizeof(SomeType{})
	defer os.Remove(filename)

	handle := NewPstruct(filename, size)
	if err := handle.Load(); err != nil {
		t.Fatal(err)
	}

	if err := handle.Unload(); err != nil {
		t.Fatal(err)
	}
}

func TestPreventMultiUnloading(t *testing.T) {
	filename := "/tmp/data.txt"
	size := unsafe.Sizeof(SomeType{})
	defer os.Remove(filename)

	handle := NewPstruct(filename, size)
	if err := handle.Load(); err != nil {
		t.Fatal(err)
	}

	if err := handle.Unload(); err != nil {
		t.Fatal(err)
	}

	if err := handle.Unload(); err == nil {
		t.Error("loading twice should throw an error")
	}
}

func BenchmarkWritingValues(b *testing.B) {
	filename := "/tmp/data.txt"
	size := unsafe.Sizeof(SomeType{})
	defer os.Remove(filename)

	handle := NewPstruct(filename, size)
	if err := handle.Load(); err != nil {
		b.Fatal(err)
	}

	value := (*SomeType)(handle.Pointer)
	b.ResetTimer()

	var i int64 = 0
	for i = 0; i < int64(b.N); i++ {
		value.d3 = i + 1
	}

	handle.Unload()

	// load again and verify
	handle2 := NewPstruct(filename, size)
	if err := handle2.Load(); err != nil {
		b.Fatal(err)
	}

	value2 := (*SomeType)(handle2.Pointer)
	if value2.d3 != i {
		b.Error("value is not persisting")
	}

	handle2.Unload()
}

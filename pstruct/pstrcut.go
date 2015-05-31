package pstruct

// Pstrcut is a way to persist a struct to a disk
// While reading and writing very fast without encoding / decoding
// It uses mmap to map a file in the disk to the struct
// But this comes with come caveats
// * You can only use basic types in the struct (no slices, strings)
// * Does not portable between different implementations of go
// * That means you can't move the data file between platforms

import (
	"errors"
	"os"
	"reflect"
	"syscall"
	"unsafe"
)

type Pstruct struct {
	filename string
	size     int64
	Pointer  unsafe.Pointer
}

func NewPstruct(filename string, size uintptr) Pstruct {
	value := Pstruct{}
	value.filename = filename
	value.size = int64(size)

	return value
}

func (i *Pstruct) Load() error {
	if i.Pointer != nil {
		return errors.New("already loaded")
	}

	f, err := os.OpenFile(i.filename, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return err
	}

	// write the default number of bytes if it does not have enough data
	if stat.Size() != i.size {
		payload := make([]byte, i.size)
		n, err := f.WriteAt(payload, 0)
		if err != nil {
			return err
		}

		if int64(n) != i.size {
			return errors.New("Couldn't write full payload to metadata")
		}
	}

	// load the memory map file
	prot := syscall.PROT_READ | syscall.PROT_WRITE
	flags := syscall.MAP_SHARED
	data, err := syscall.Mmap(int(f.Fd()), 0, int(i.size), prot, flags)
	if err != nil {
		return err
	}

	ptr := unsafe.Pointer(&data[0])
	i.Pointer = ptr
	return nil
}

func (i *Pstruct) Unload() error {
	if i.Pointer == nil {
		return errors.New("not loaded yet")
	}

	data := make([]byte, i.size)
	header := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	header.Data = (uintptr)(i.Pointer)

	err := syscall.Munmap(data)
	if err != nil {
		return err
	}

	return nil
}

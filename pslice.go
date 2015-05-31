package kdb

// 	# Pslice
//	Pslice is a float64 slice which is persitable to the disk
//
// 	Pslice is very fast and set and get values in less than 2 ns. (for smaller slices)
// 	It uses mmap and persist as we put data into the slice
// 	We can resize the slice as well
// 	(we've a different API than the original go slices)
//
//	### Known Issues
//  * data file is not cross platform friendly
//
//
import (
	"errors"
	"os"
	"reflect"
	"syscall"
	"unsafe"
)

type Pslice struct {
	// data file of the slice
	Filename string
	// lenght of the slice or otherwise number of elements in the slice
	Length int64
	// actual byte size = sizeof(float64) * Lenght
	size int64
	// pointer to the mmaped memory
	pointer unsafe.Pointer
	// slice with the allocated memory
	slice []float64
}

// Create a new splice of the given lenght at the given filename
func NewPslice(filename string, length int64) (*Pslice, error) {
	value := Pslice{}
	value.Filename = filename
	value.Length = length
	value.size = length * 8 // because we use float64

	if err := value.load(); err != nil {
		return nil, err
	}

	return &value, nil
}

// Load the pslice into the memory. Should not call this manually
func (i *Pslice) load() error {
	if i.pointer != nil {
		return errors.New("already loaded")
	}

	f, err := os.OpenFile(i.Filename, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	// it's okay to close the file even we mmap used FD of this file
	// mmap maintain it's own mappeing after mmaped with the FD
	// to munmap, we don't need the FD
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return err
	}

	// write the default number of bytes if it does not have enough data
	if stat.Size() < i.size {
		sizeToAllocate := i.size - stat.Size()
		payload := make([]byte, sizeToAllocate)
		n, err := f.WriteAt(payload, stat.Size())
		if err != nil {
			return err
		}

		if int64(n) != sizeToAllocate {
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

	// get the pointer to the data
	ptr := unsafe.Pointer(&data[0])
	i.pointer = ptr

	// create the slice and assign the mmaped pointer as the data of the above slice
	slice := make([]float64, i.Length)
	header := (*reflect.SliceHeader)(unsafe.Pointer(&slice))
	header.Data = (uintptr)(i.pointer)
	i.slice = slice

	return nil
}

// Close the pslice by ummapping allocated memory via mmap
func (i *Pslice) Close() error {
	if i.pointer == nil {
		return errors.New("not loaded yet")
	}

	data := make([]byte, i.size)
	header := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	header.Data = (uintptr)(i.pointer)

	err := syscall.Munmap(data)
	if err != nil {
		return err
	}

	i.Length = 0
	i.pointer = nil
	i.size = 0
	i.slice = nil

	return nil
}

// Get the value of an index
func (i *Pslice) Get(index int64) float64 {
	return i.slice[index]
}

// Set the value to an index
func (i *Pslice) Set(index int64, value float64) {
	i.slice[index] = value
}

// Resize the slice with a newLenght
// It's possible to downsize, in that case excess elements won't get deleted
func (i *Pslice) Resize(newLength int64) error {
	if err := i.Close(); err != nil {
		return err
	}

	i.Length = newLength
	i.size = newLength * 8
	if err := i.load(); err != nil {
		return err
	}

	return nil
}

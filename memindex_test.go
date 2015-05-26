package kdb

import (
	"io/ioutil"
	"os"
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
	// TODO
}

func TestNewMemIndexCorruptFile(t *testing.T) {
	// TODO
}

func TestMemIndexNewIndexElement(t *testing.T) {
	// TODO
}

func TestMemIndexAdd(t *testing.T) {
	// TODO
}

func TestMemIndexGet(t *testing.T) {
	// TODO
}

func TestMemIndexFind(t *testing.T) {
	// TODO
}

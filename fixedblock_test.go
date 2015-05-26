package kdb

import (
	"crypto/rand"
	"math/big"
	"os"
	"testing"
)

// test creating a block struct with an empty block file
func TestNewFixedBlockNewFile(t *testing.T) {
	fpath := "/tmp/b1"
	defer os.Remove(fpath)

	blk, err := NewFixedBlock(FixedBlockOpts{
		FilePath:     fpath,
		PayloadSize:  4,
		PayloadCount: 10,
	})

	if err != nil {
		t.Fatal(err)
	}

	if blk != nil {
		defer blk.Close()
	}

	if blk.fsize != 0 {
		t.Fatal("fsize must be 0 for new files")
	}

	if blk.rsize != 40 {
		t.Fatal("record size should be equal to PayloadSize x PayloadCount")
	}

	if len(blk.rtemp) != 40 {
		t.Fatal("empty record should have correct size")
	}
}

// test creating a block struct with an existing block file
func TestNewFixedBlockExistingFile(t *testing.T) {
	fpath := "/tmp/b1"
	defer os.Remove(fpath)

	fd, err := os.OpenFile(fpath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}

	b := make([]byte, 40)
	n, err := fd.Write(b)
	if err != nil {
		t.Fatal(err)
	} else if n != 40 {
		t.Fatal("write error")
	}

	if err := fd.Close(); err != nil {
		t.Fatal(err)
	}

	blk, err := NewFixedBlock(FixedBlockOpts{
		FilePath:     fpath,
		PayloadSize:  4,
		PayloadCount: 10,
	})

	if err != nil {
		t.Fatal(err)
	}

	if blk != nil {
		defer blk.Close()
	}

	if blk.fsize != 40 {
		t.Fatal("fsize must be 40")
	}
}

// test creating a block struct with a corrupt block file
func TestNewFixedBlockCorruptFile(t *testing.T) {
	fpath := "/tmp/b1"
	defer os.Remove(fpath)

	fd, err := os.OpenFile(fpath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}

	b := make([]byte, 50)
	n, err := fd.Write(b)
	if err != nil {
		t.Fatal(err)
	} else if n != 50 {
		t.Fatal("write error")
	}

	if err := fd.Close(); err != nil {
		t.Fatal(err)
	}

	blk, err := NewFixedBlock(FixedBlockOpts{
		FilePath:     fpath,
		PayloadSize:  4,
		PayloadCount: 10,
	})

	if err != ErrFixedBlockFileCorrupt {
		t.Fatal("should return `ErrFixedBlockFileCorrupt`")
	}

	if blk != nil {
		defer blk.Close()
	}
}

// test a Put request by first writing data with Put and reading it
// value read later must match value written using Put
// this also confirms that the value is written at correct position
func TestFixedBlockPut(t *testing.T) {
	fpath := "/tmp/b1"
	defer os.Remove(fpath)

	fd, err := os.OpenFile(fpath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}

	b := make([]byte, 400)
	n, err := fd.Write(b)
	if err != nil {
		t.Fatal(err)
	} else if n != 400 {
		t.Fatal("write error")
	}

	if err := fd.Close(); err != nil {
		t.Fatal(err)
	}

	blk, err := NewFixedBlock(FixedBlockOpts{
		FilePath:     fpath,
		PayloadSize:  4,
		PayloadCount: 10,
	})

	if err != nil {
		t.Fatal(err)
	}

	if blk != nil {
		defer blk.Close()
	}

	var rpos int64 = 2
	var ppos int64 = 3
	pld := []byte("asdf")

	err = blk.Put(rpos, ppos, pld)

	// Put should run without errors
	if err != nil {
		t.Fatal(err)
	}

	fd, err = os.OpenFile(fpath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}

	res := make([]byte, 4)
	off := rpos*40 + ppos*4

	n, err = fd.ReadAt(res, off)
	if err != nil {
		t.Fatal(err)
	} else if n != 4 {
		t.Fatal("read error")
	}

	if string(res) != string(pld) {
		t.Fatal("invalid data")
	}

	if err := fd.Close(); err != nil {
		t.Fatal(err)
	}
}

// test a Get request by first writing data and reading it using Get
// value read using Get must match value written manually
// this also confirms that the value is read from the correct position
func TestFixedBlockGet(t *testing.T) {
	fpath := "/tmp/b1"
	defer os.Remove(fpath)

	fd, err := os.OpenFile(fpath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}

	b := make([]byte, 400)
	n, err := fd.Write(b)
	if err != nil {
		t.Fatal(err)
	} else if n != 400 {
		t.Fatal("write error")
	}

	// write sample data
	var rpos int64 = 2
	var ppos int64 = 3
	data := []byte("asdfghjk")
	off := rpos*40 + ppos*4

	n, err = fd.WriteAt(data, off)
	if err != nil {
		t.Fatal(err)
	} else if n != 8 {
		t.Fatal("write error")
	}

	if err := fd.Close(); err != nil {
		t.Fatal(err)
	}

	blk, err := NewFixedBlock(FixedBlockOpts{
		FilePath:     fpath,
		PayloadSize:  4,
		PayloadCount: 10,
	})

	if err != nil {
		t.Fatal(err)
	}

	if blk != nil {
		defer blk.Close()
	}

	res, err := blk.Get(rpos, ppos, ppos+2)
	if err != nil {
		t.Fatal(err)
	}

	if len(res) != 2 {
		t.Fatal("incorrect number of payloads")
	}

	if string(res[0]) != "asdf" || string(res[1]) != "ghjk" {
		t.Fatal("invalid data")
	}
}

// Benchmark Put requests with random positions
func BenchmarkFixedBlockPut(b *testing.B) {
	b.StopTimer()

	fpath := "/tmp/b1"
	defer os.Remove(fpath)

	fd, err := os.OpenFile(fpath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		b.Fatal(err)
	}

	tmp := make([]byte, 400)
	n, err := fd.Write(tmp)
	if err != nil {
		b.Fatal(err)
	} else if n != 400 {
		b.Fatal("write error")
	}

	if err := fd.Close(); err != nil {
		b.Fatal(err)
	}

	blk, err := NewFixedBlock(FixedBlockOpts{
		FilePath:     fpath,
		PayloadSize:  4,
		PayloadCount: 10,
	})

	if err != nil {
		b.Fatal(err)
	}

	if blk != nil {
		defer blk.Close()
	}

	pld := make([]byte, 4)
	max := big.NewInt(10)
	var rnd *big.Int
	var rpos, ppos int64

	for i := 0; i < b.N; i++ {
		rnd, _ = rand.Int(rand.Reader, max)
		rpos = rnd.Int64()

		rnd, _ = rand.Int(rand.Reader, max)
		ppos = rnd.Int64()

		rand.Read(pld)

		b.StartTimer()
		err := blk.Put(rpos, ppos, pld)
		b.StopTimer()

		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark Get requests with random ranges
func BenchmarkFixedBlockGet(b *testing.B) {
	b.StopTimer()

	fpath := "/tmp/b1"
	defer os.Remove(fpath)

	fd, err := os.OpenFile(fpath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		b.Fatal(err)
	}

	tmp := make([]byte, 400)
	n, err := fd.Write(tmp)
	if err != nil {
		b.Fatal(err)
	} else if n != 400 {
		b.Fatal("write error")
	}

	blk, err := NewFixedBlock(FixedBlockOpts{
		FilePath:     fpath,
		PayloadSize:  4,
		PayloadCount: 10,
	})

	if err != nil {
		b.Fatal(err)
	}

	if blk != nil {
		defer blk.Close()
	}

	maxStart := big.NewInt(5)
	maxEnd := big.NewInt(5)
	maxRec := big.NewInt(10)

	var rnd *big.Int
	var rpos, start, end int64

	for i := 0; i < b.N; i++ {
		rnd, _ = rand.Int(rand.Reader, maxRec)
		rpos = rnd.Int64()

		rnd, _ = rand.Int(rand.Reader, maxStart)
		start = rnd.Int64()

		rnd, _ = rand.Int(rand.Reader, maxEnd)
		end = start + rnd.Int64()

		b.StartTimer()
		_, err := blk.Get(rpos, start, end)
		b.StopTimer()

		if err != nil {
			b.Fatal(err)
		}
	}
}

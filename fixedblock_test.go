package kdb

import (
	"crypto/rand"
	"math/big"
	"os"
	"testing"
)

// test creating a block struct with an empty block file
func TestNewFixedBlockNewData(t *testing.T) {
	blockPath := "/tmp/b1"
	if err := os.RemoveAll(blockPath); err != nil {
		t.Fatal(err)
	}

	blk, err := NewFixedBlock(FixedBlockOpts{
		BlockPath:    blockPath,
		PayloadSize:  4,
		PayloadCount: 10,
		SegmentSize:  10,
	})

	if err != nil {
		t.Fatal(err)
	}

	if blk != nil {
		defer blk.Close()
	}

	if len(blk.files) != 1 || len(blk.fsize) != 1 {
		t.Fatal("number of segments should be 1 at start")
	}

	if blk.rsize != 40 {
		t.Fatal("record size should be equal to PayloadSize x PayloadCount")
	}

	if len(blk.rtemp) != 40 {
		t.Fatal("empty record should have correct size")
	}
}

// test creating a block struct with an existing block file
func TestNewFixedBlockExistingData(t *testing.T) {
	blockPath := "/tmp/b1"
	if err := os.RemoveAll(blockPath); err != nil {
		t.Fatal(err)
	}

	blk, err := NewFixedBlock(FixedBlockOpts{
		BlockPath:    blockPath,
		PayloadSize:  4,
		PayloadCount: 10,
		SegmentSize:  10,
	})

	if err != nil {
		t.Fatal(err)
	}

	if blk != nil {
		defer blk.Close()
	}

}

func TestFixedBlockNewRecord(t *testing.T) {
	blockPath := "/tmp/b1"
	if err := os.RemoveAll(blockPath); err != nil {
		t.Fatal(err)
	}

	blk, err := NewFixedBlock(FixedBlockOpts{
		BlockPath:    blockPath,
		PayloadSize:  4,
		PayloadCount: 10,
		SegmentSize:  10,
	})

	if err != nil {
		t.Fatal(err)
	}

	if blk != nil {
		defer blk.Close()
	}

	for i := 0; i < 5; i++ {
		if rpos, err := blk.NewRecord(); err != nil {
			t.Fatal(err)
		} else if rpos != int64(i+1) {
			t.Fatal("incorrect rpos")
		}
	}
}

// test a Put request by first writing data with Put and reading it
// value read later must match value written using Put
// this also confirms that the value is written at correct position
func TestFixedBlockPut(t *testing.T) {
	blockPath := "/tmp/b1"
	if err := os.RemoveAll(blockPath); err != nil {
		t.Fatal(err)
	}

	os.MkdirAll(blockPath, 0744)

	blk, err := NewFixedBlock(FixedBlockOpts{
		BlockPath:    blockPath,
		PayloadSize:  4,
		PayloadCount: 10,
		SegmentSize:  10,
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

	fpath := blockPath + "/block_1.data"
	fd, err := os.OpenFile(fpath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}

	res := make([]byte, 4)
	off := rpos*40 + ppos*4

	n, err := fd.ReadAt(res, off)
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
	blockPath := "/tmp/b1"
	if err := os.RemoveAll(blockPath); err != nil {
		t.Fatal(err)
	}

	os.MkdirAll(blockPath, 0744)

	blk, err := NewFixedBlock(FixedBlockOpts{
		BlockPath:    blockPath,
		PayloadSize:  4,
		PayloadCount: 10,
		SegmentSize:  10,
	})

	if err != nil {
		t.Fatal(err)
	}

	if blk != nil {
		defer blk.Close()
	}

	var rpos int64 = 2
	var ppos int64 = 3
	pld1 := []byte("asdf")
	pld2 := []byte("ghjk")

	err = blk.Put(rpos, ppos, pld1)
	if err != nil {
		t.Fatal(err)
	}

	err = blk.Put(rpos, ppos+1, pld2)
	if err != nil {
		t.Fatal(err)
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

func TestFixedBlockAllocateWhenCreated(t *testing.T) {
	blockPath := "/tmp/b1"
	if err := os.RemoveAll(blockPath); err != nil {
		t.Fatal(err)
	}

	blk, err := NewFixedBlock(FixedBlockOpts{
		BlockPath:    blockPath,
		PayloadSize:  4,
		PayloadCount: 10,
		SegmentSize:  10,
	})

	if err != nil {
		t.Fatal(err)
	}

	if err := blk.Close(); err != nil {
		t.Fatal(err)
	}

	blk2, err := NewFixedBlock(FixedBlockOpts{
		BlockPath:    blockPath,
		PayloadSize:  4,
		PayloadCount: 10,
		SegmentSize:  10,
	})

	if err != nil {
		t.Fatal(err)
	}

	segmentCount := blk2.metadata.Get(FBMetadata_POS_SEGMENT_COUNT)

	if segmentCount != 1 {
		t.Error("there should only segment allocation")
	}
}

func TestFixedBlockPreallocate(t *testing.T) {
	blockPath := "/tmp/b1"
	if err := os.RemoveAll(blockPath); err != nil {
		t.Fatal(err)
	}

	blk, err := NewFixedBlock(FixedBlockOpts{
		BlockPath:    blockPath,
		PayloadSize:  4,
		PayloadCount: 10,
		SegmentSize:  10,
	})

	if err := blk.preallocate(10, 99999); err != nil {
		t.Fatal(err)
	}

	segmentFile := blockPath + "/block_10.data"
	stat, err := os.Stat(segmentFile)
	if err != nil {
		t.Fatal(err)
	}

	var expectedPayloadSize int64 = 99999 * 4 * 10
	if stat.Size() != expectedPayloadSize {
		t.Error("segment written size is incorrect")
	}

}

func TestFixedBlockPreallocateExisiting(t *testing.T) {
	blockPath := "/tmp/b1"
	if err := os.RemoveAll(blockPath); err != nil {
		t.Fatal(err)
	}

	blk, err := NewFixedBlock(FixedBlockOpts{
		BlockPath:    blockPath,
		PayloadSize:  4,
		PayloadCount: 10,
		SegmentSize:  10,
	})

	segmentFile := blockPath + "/block_10.data"
	f, err := os.OpenFile(segmentFile, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		t.Fatal(err)
	}

	f.Close()

	if err := blk.preallocate(10, 99999); err == nil {
		t.Error("should throw an error")
	}
}

func TestFixedBlockShouldPreallocateWhenNoSegments(t *testing.T) {
	blockPath := "/tmp/b1"
	if err := os.RemoveAll(blockPath); err != nil {
		t.Fatal(err)
	}

	blk, err := NewFixedBlock(FixedBlockOpts{
		BlockPath:    blockPath,
		PayloadSize:  4,
		PayloadCount: 10,
		SegmentSize:  10,
	})

	if err != nil {
		t.Fatal(err)
	}

	blk.metadata.Set(FBMetadata_POS_SEGMENT_COUNT, float64(0))
	ok, segementNo := blk.shouldPreallocate()

	if !ok {
		t.Error("need to preallocate")
	}

	if segementNo != 1 {
		t.Error("need to preallocate the first segment")
	}
}

func TestFixedBlockShouldPreallocateWhenThereAreSegments(t *testing.T) {
	blockPath := "/tmp/b1"
	if err := os.RemoveAll(blockPath); err != nil {
		t.Fatal(err)
	}

	blk, err := NewFixedBlock(FixedBlockOpts{
		BlockPath:    blockPath,
		PayloadSize:  4,
		PayloadCount: 10,
		SegmentSize:  10,
	})

	if err != nil {
		t.Fatal(err)
	}

	blk.metadata.Set(FBMetadata_POS_SEGMENT_COUNT, float64(10))

	ok, _ := blk.shouldPreallocate()

	if ok {
		t.Error("no need to preallocate")
	}
}

func TestFixedBlockShouldPreallocateWhenThereAreSegmentsButLessSpace(t *testing.T) {
	blockPath := "/tmp/b1"
	if err := os.RemoveAll(blockPath); err != nil {
		t.Fatal(err)
	}

	blk, err := NewFixedBlock(FixedBlockOpts{
		BlockPath:    blockPath,
		PayloadSize:  4,
		PayloadCount: 10,
		SegmentSize:  100000,
	})

	if err != nil {
		t.Fatal(err)
	}

	recordsPerSegment := blk.metadata.Get(FBMetadata_POS_RECORDS_PER_SEGMENT)
	blk.metadata.Set(FBMetadata_POS_SEGMENT_COUNT, float64(1))
	blk.metadata.Set(FBMetadata_POS_RECORD_COUNT, float64(recordsPerSegment-100))

	ok, segmentNo := blk.shouldPreallocate()

	if !ok {
		t.Error("need to preallocate")
	}

	if segmentNo != 2 {
		t.Error("need to preallocate the second segment")
	}
}

func TestFixedBlockPreallocateIfNeeded(t *testing.T) {
	blockPath := "/tmp/b100"
	if err := os.RemoveAll(blockPath); err != nil {
		t.Fatal(err)
	}

	blk, err := NewFixedBlock(FixedBlockOpts{
		BlockPath:    blockPath,
		PayloadSize:  4,
		PayloadCount: 10,
		SegmentSize:  100000,
	})

	if err != nil {
		t.Fatal(err)
	}

	records := (blk.metadata.Get(FBMetadata_POS_RECORDS_PER_SEGMENT) * 12) - 100
	blk.metadata.Set(FBMetadata_POS_SEGMENT_COUNT, 12)
	blk.metadata.Set(FBMetadata_POS_RECORD_COUNT, records)

	for i := 0; i < 3; i++ {
		if err := blk.preallocateIfNeeded(); err != nil {
			t.Fatal(err)
		}
	}

	segmentCount := blk.metadata.Get(FBMetadata_POS_SEGMENT_COUNT)
	if segmentCount != float64(13) {
		t.Error("allocation failed", segmentCount)
	}
}

func BenchmarkFixedBlockNewRecord(b *testing.B) {
	blockPath := "/tmp/b1"
	if err := os.RemoveAll(blockPath); err != nil {
		b.Fatal(err)
	}

	blk, err := NewFixedBlock(FixedBlockOpts{
		BlockPath:    blockPath,
		PayloadSize:  16,
		PayloadCount: 1000,
		SegmentSize:  100000,
	})

	if err != nil {
		b.Fatal(err)
	}

	if blk != nil {
		defer blk.Close()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := blk.NewRecord(); err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark Put requests with random positions
func BenchmarkFixedBlockPut(b *testing.B) {
	blockPath := "/tmp/b1"
	if err := os.RemoveAll(blockPath); err != nil {
		b.Fatal(err)
	}

	os.MkdirAll(blockPath, 0744)

	blk, err := NewFixedBlock(FixedBlockOpts{
		BlockPath:    blockPath,
		PayloadSize:  4,
		PayloadCount: 10,
		SegmentSize:  100000,
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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rnd, _ = rand.Int(rand.Reader, max)
		rpos = rnd.Int64()

		rnd, _ = rand.Int(rand.Reader, max)
		ppos = rnd.Int64()

		rand.Read(pld)

		err := blk.Put(rpos, ppos, pld)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark Get requests with random ranges
func BenchmarkFixedBlockGet(b *testing.B) {
	blockPath := "/tmp/b1"
	if err := os.RemoveAll(blockPath); err != nil {
		b.Fatal(err)
	}

	os.MkdirAll(blockPath, 0744)

	blk, err := NewFixedBlock(FixedBlockOpts{
		BlockPath:    blockPath,
		PayloadSize:  4,
		PayloadCount: 10,
		SegmentSize:  100000,
	})

	if err != nil {
		b.Fatal(err)
	}

	if blk != nil {
		defer blk.Close()
	}

	var rpos int64 = 2
	var ppos int64 = 3
	pld1 := []byte("asdf")
	pld2 := []byte("ghjk")

	err = blk.Put(rpos, ppos, pld1)
	if err != nil {
		b.Fatal(err)
	}

	err = blk.Put(rpos, ppos+1, pld2)
	if err != nil {
		b.Fatal(err)
	}

	maxStart := big.NewInt(5)
	maxEnd := big.NewInt(5)
	maxRec := big.NewInt(10)

	var rnd *big.Int
	var start, end int64

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rnd, _ = rand.Int(rand.Reader, maxRec)
		rpos = rnd.Int64()

		rnd, _ = rand.Int(rand.Reader, maxStart)
		start = rnd.Int64()

		rnd, _ = rand.Int(rand.Reader, maxEnd)
		end = start + rnd.Int64()

		_, err := blk.Get(rpos, start, end)
		if err != nil {
			b.Fatal(err)
		}
	}
}

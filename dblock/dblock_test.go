package dblock

import (
	"errors"
	"os"
	"os/exec"
	"reflect"
	"testing"
)

// test creating a block struct with an empty block file
func TestNewDBlockNewData(t *testing.T) {
	defer cleanTestFiles()

	blk, err := createTestBlock()
	if err != nil {
		t.Fatal(err)
	}

	defer blk.Close()

	if len(blk.segmentFiles) != 1 || len(blk.segmentMmaps) != 1 {
		t.Fatal("number of segments should be 1 at start")
	}

	if blk.recordSize != 400 {
		t.Fatal("record size should be equal to PayloadSize x PayloadCount")
	}

	if len(blk.emptyRecord) != 400 {
		t.Fatal("empty record should have correct size")
	}
}

// test creating a block struct with an existing block file
func TestNewDBlockExistingData(t *testing.T) {
	defer cleanTestFiles()

	blk1, err := createTestBlock()
	if err != nil {
		t.Fatal(err)
	}

	blk1.Close()

	// create blk2 with same settings
	blk2, err := createTestBlock()
	if err != nil {
		t.Fatal(err)
	}

	defer blk2.Close()

	if len(blk2.segmentFiles) != 1 || len(blk2.segmentMmaps) != 1 {
		t.Fatal("number of segments should be 1 at start")
	}

	if blk2.recordSize != 400 {
		t.Fatal("record size should be equal to PayloadSize x PayloadCount")
	}

	if len(blk2.emptyRecord) != 400 {
		t.Fatal("empty record should have correct size")
	}
}

func TestNewRecord(t *testing.T) {
	defer cleanTestFiles()

	blk, err := createTestBlock()
	if err != nil {
		t.Fatal(err)
	}

	defer blk.Close()

	// make sure we're testing with multiple segments
	count := int(blk.SegmentSize * 2)

	for i := 0; i < count; i++ {
		if rpos, err := blk.New(); err != nil {
			t.Fatal(err)
		} else if rpos != int64(i) {
			t.Fatal("incorrect rpos")
		}
	}
}

func TestPutAndGet(t *testing.T) {
	defer cleanTestFiles()

	blk, err := createTestBlock()
	if err != nil {
		t.Fatal(err)
	}

	defer blk.Close()

	rpos, err := blk.New()
	if err != nil {
		t.Fatal(err)
	}

	pld1 := []byte{1, 2, 3, 4}
	if err := blk.Put(rpos, 2, pld1); err != nil {
		t.Fatal(err)
	}

	pld2 := []byte{5, 6, 7, 8}
	if err := blk.Put(rpos, 3, pld2); err != nil {
		t.Fatal(err)
	}

	res, err := blk.Get(rpos, 2, 4)
	if err != nil {
		t.Fatal(err)
	}

	exp := [][]byte{pld1, pld2}
	if !reflect.DeepEqual(res, exp) {
		t.Fatal("invalid result")
	}
}

func TestPreallocate(t *testing.T) {
	defer cleanTestFiles()

	blk, err := createTestBlock()
	if err != nil {
		t.Fatal(err)
	}

	defer blk.Close()

	if err := blk.preallocate(10, 99999); err != nil {
		t.Fatal(err)
	}

	segmentFile := "/tmp/test-dblock/block_10"
	stat, err := os.Stat(segmentFile)
	if err != nil {
		t.Fatal(err)
	}

	var expectedPayloadSize int64 = 99999 * 4 * 100
	if stat.Size() != expectedPayloadSize {
		t.Error("segment written size is incorrect")
	}
}

func TestPreallocateExisiting(t *testing.T) {
	defer cleanTestFiles()

	blk, err := createTestBlock()
	if err != nil {
		t.Fatal(err)
	}

	defer blk.Close()

	segmentFile := "/tmp/test-dblock/block_10"
	f, err := os.OpenFile(segmentFile, FileOpenMode, FilePermissions)
	if err != nil {
		t.Fatal(err)
	}

	defer f.Close()

	if err := blk.preallocate(10, 99999); err == nil {
		t.Error("should throw an error")
	}
}

func TestShouldPreallocateWhenNoSegments(t *testing.T) {
	defer cleanTestFiles()

	blk, err := createTestBlock()
	if err != nil {
		t.Fatal(err)
	}

	defer blk.Close()

	blk.metadata.Set(MetadataSegmentCount, float64(0))
	doPreallocate, segementNo := blk.shouldPreallocate()

	if !doPreallocate {
		t.Error("need to preallocate")
	}

	if segementNo != 1 {
		t.Error("need to preallocate the first segment")
	}
}

func TestShouldPreallocateWithSegments(t *testing.T) {
	defer cleanTestFiles()

	blk, err := createTestBlock()
	if err != nil {
		t.Fatal(err)
	}

	defer blk.Close()

	blk.metadata.Set(MetadataSegmentCount, float64(10))
	doPreallocate, _ := blk.shouldPreallocate()

	if doPreallocate {
		t.Error("no need to preallocate")
	}
}

func TestShouldPreallocateWhenOutOfSpace(t *testing.T) {
	defer cleanTestFiles()

	blk, err := createTestBlock()
	if err != nil {
		t.Fatal(err)
	}

	defer blk.Close()

	size := blk.metadata.Get(MetadataSegmentSize)
	blk.metadata.Set(MetadataSegmentCount, float64(1))
	blk.metadata.Set(MetadataRecordCount, size-2)
	doPreallocate, segementNo := blk.shouldPreallocate()

	if !doPreallocate {
		t.Error("need to preallocate")
	}

	if segementNo != 2 {
		t.Error("need to preallocate the second segment")
	}
}

func TestShouldPreallocateIfNeeded(t *testing.T) {
	defer cleanTestFiles()

	blk, err := createTestBlock()
	if err != nil {
		t.Fatal(err)
	}

	defer blk.Close()

	records := blk.metadata.Get(MetadataSegmentSize)*12 - 2
	blk.metadata.Set(MetadataSegmentCount, float64(12))
	blk.metadata.Set(MetadataRecordCount, records)

	// try a few times, but it should only allocate one segment
	for i := 0; i < 3; i++ {
		if err := blk.preallocateIfNeeded(); err != nil {
			t.Fatal(err)
		}
	}

	segmentCount := blk.metadata.Get(MetadataSegmentCount)
	if segmentCount != float64(13) {
		t.Error("allocation failed", segmentCount)
	}
}

func BenchmarkNewRecord(b *testing.B) {
	defer cleanTestFiles()

	blk, err := createTestBlock()
	if err != nil {
		b.Fatal(err)
	}

	defer blk.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		blk.New()
	}
}

// TODO: randomize without affecting the benchmark
func BenchmarkPut(b *testing.B) {
	defer cleanTestFiles()

	blk, err := createTestBlock()
	if err != nil {
		b.Fatal(err)
	}

	defer blk.Close()

	rpos, err := blk.New()
	if err != nil {
		b.Fatal(err)
	}

	pld := []byte{1, 2, 3, 4}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		blk.Put(rpos, 2, pld)
	}
}

// TODO: randomize without affecting the benchmark
func BenchmarkGet(b *testing.B) {
	defer cleanTestFiles()

	blk, err := createTestBlock()
	if err != nil {
		b.Fatal(err)
	}

	defer blk.Close()

	rpos, err := blk.New()
	if err != nil {
		b.Fatal(err)
	}

	pld := []byte{1, 2, 3, 4}
	for i := 0; i < 10; i++ {
		err = blk.Put(rpos, int64(i), pld)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		blk.Get(rpos, 0, 10)
	}
}

func BenchmarkPreallocate(b *testing.B) {
	defer cleanTestFiles()

	blk, err := createTestBlock()
	if err != nil {
		b.Fatal(err)
	}

	defer blk.Close()

	var i int64
	N := int64(b.N)

	b.ResetTimer()
	for i = 0; i < N; i++ {
		blk.preallocate(i, 1000)
	}
}

// ---------- //

// create a DBlock with test settings
func createTestBlock() (blk *DBlock, err error) {
	cmd := exec.Command("rm", "-rf", "/tmp/test-dblock")
	if err := cmd.Run(); err != nil {
		return nil, err
	}

	err = os.MkdirAll("/tmp/test-dblock", 0777)
	if err != nil {
		return nil, err
	}

	blk, err = New(Options{
		BlockPath:    "/tmp/test-dblock",
		PayloadSize:  4,
		PayloadCount: 100,
		SegmentSize:  100000,
	})

	if err != nil {
		return nil, err
	}

	if blk == nil {
		err = errors.New("block should not be nil")
		return nil, err
	}

	return blk, nil
}

func cleanTestFiles() {
	cmd := exec.Command("rm", "-rf", "/tmp/test-dblock")
	cmd.Run()
}

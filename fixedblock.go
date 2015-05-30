package kdb

import (
	"errors"
	"os"
	"runtime"
	"sync"
)

const (
	FixedBlockFMode  = os.O_CREATE | os.O_RDWR
	FixedBlockFPerms = 0744
)

var (
	ErrFixedBlockBytesWritten = errors.New("incorrect number of bytes written to block file")
	ErrFixedBlockBytesRead    = errors.New("incorrect number of bytes read from block file")
	ErrFixedBlockFileCorrupt  = errors.New("data is corrupt in the block file")
)

type FixedBlockMetaData struct {
	// number of records we put into a segment
	recordsPerSegments int64

	// number of segemnts we've
	segmentCount int

	// number of records in all of the segments
	recordCount int
}

type FixedBlockOpts struct {
	// path to block file
	BlockPath string

	// maximum payload size in bytes
	PayloadSize int64

	// number of payloads in a record
	PayloadCount int64
}

type FixedBlock struct {
	FixedBlockOpts
	file  *os.File // file used to store payloads
	rsize int64    // byte size of a single record
	fsize int64    // offset of next record (file size in bytes)
	rtemp []byte   // reusable empty template for new records
	mutex *sync.Mutex
}

func NewFixedBlock(opts FixedBlockOpts) (blk *FixedBlock, err error) {
	err = os.MkdirAll(opts.BlockPath, FixedBlockFPerms)
	if err != nil {
		return nil, err
	}

	blockFilePath := opts.BlockPath + "/block.data"
	file, err := os.OpenFile(blockFilePath, FixedBlockFMode, FixedBlockFPerms)
	if err != nil {
		return nil, err
	}

	finfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	rsize := opts.PayloadSize * opts.PayloadCount

	fsize := finfo.Size()
	if delta := fsize % rsize; delta != 0 {
		return nil, ErrFixedBlockFileCorrupt
	}

	rtemp := make([]byte, rsize)
	mutex := &sync.Mutex{}

	blk = &FixedBlock{opts, file, rsize, fsize, rtemp, mutex}
	return blk, nil
}

// NewRecord Creates a new record at the end of the block file
// and returns the index of the record
func (blk *FixedBlock) NewRecord() (rpos int64, err error) {
	blk.mutex.Lock()
	offset := blk.fsize

	n, err := blk.file.WriteAt(blk.rtemp, offset)
	if err != nil {
		return 0, err
	} else if n != int(blk.rsize) {
		return 0, ErrFixedBlockBytesWritten
	}

	blk.fsize += blk.rsize
	blk.mutex.Unlock()
	runtime.Gosched()

	rpos = offset / blk.rsize

	return rpos, nil
}

// Put stores a payload `pld` on record at `rpos` at position `ppos`
// rpos and ppos are positions of record and payload and must be
// mutiplied by record size and payload size to get offsets
func (blk *FixedBlock) Put(rpos, ppos int64, pld []byte) (err error) {
	offset := rpos*blk.rsize + ppos*blk.PayloadSize
	n, err := blk.file.WriteAt(pld, offset)
	if err != nil {
		return err
	} else if n != int(blk.PayloadSize) {
		return ErrFixedBlockBytesWritten
	}

	return nil
}

// Get reads payloads from `start` to `end` on record at `rpos`
// start, end and rpos are positions of payload and record it can be
// mutiplied by payload size and record size to get offsets
func (blk *FixedBlock) Get(rpos, start, end int64) (res [][]byte, err error) {
	offset := rpos*blk.rsize + start*blk.PayloadSize
	pldCount := end - start
	resSize := blk.PayloadSize * pldCount
	resData := make([]byte, resSize)

	n, err := blk.file.ReadAt(resData, offset)
	if err != nil {
		return nil, err
	} else if n != int(resSize) {
		return nil, ErrFixedBlockBytesRead
	}

	res = make([][]byte, pldCount)

	var i int64
	for i = 0; i < pldCount; i++ {
		res[i] = resData[i*blk.PayloadSize : (i+1)*blk.PayloadSize]
	}

	return res, nil
}

// close the file handler
func (blk *FixedBlock) Close() (err error) {
	err = blk.file.Close()
	if err != nil {
		return err
	}

	return nil
}

func (blk *FixedBlock) getMetadata() (metadata *FixedBlockMetaData, err error) {

	return nil, nil
}

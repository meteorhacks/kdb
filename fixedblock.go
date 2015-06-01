package kdb

import (
	"errors"
	"log"
	"os"
	"strconv"
	"sync"
)

const (
	FixedBlockFMode  = os.O_CREATE | os.O_RDWR
	FixedBlockFPerms = 0744
	// keep index position of relavant metadata keys in the pslice
	FBMetadata_POS_RECORDS_PER_SEGMENT = 0
	FBMetadata_POS_SEGMENT_COUNT       = 1
	FBMetadata_POS_RECORD_COUNT        = 2
)

var (
	ErrFixedBlockBytesWritten   = errors.New("incorrect number of bytes written to block file")
	ErrFixedBlockBytesRead      = errors.New("incorrect number of bytes read from block file")
	ErrFixedBlockFileCorrupt    = errors.New("data is corrupt in the block file")
	ErrFixedBlockInvalidSegment = errors.New("segment file does not exist for given position")
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

	// number of records per segment
	SegmentSize int64
}

type FixedBlock struct {
	FixedBlockOpts
	files            map[int64]*os.File // files used to store segments
	fsize            map[int64]int64    // offsets of next record (file size in bytes)
	rsize            int64              // byte size of a single record
	rtemp            []byte             // reusable empty template for new records
	writeMutex       *sync.Mutex
	preallocateMutex *sync.Mutex
	allocateMutex    *sync.Mutex
	preallocating    bool
	metadata         *Pslice
}

func NewFixedBlock(opts FixedBlockOpts) (blk *FixedBlock, err error) {
	err = os.MkdirAll(opts.BlockPath, FixedBlockFPerms)
	if err != nil {
		return nil, err
	}

	files := make(map[int64]*os.File)
	fsize := make(map[int64]int64)
	rsize := opts.PayloadSize * opts.PayloadCount
	rtemp := make([]byte, rsize)
	writeMutex := &sync.Mutex{}
	preallocateMutex := &sync.Mutex{}
	allocateMutex := &sync.Mutex{}

	// load metadata
	metadataFilePath := opts.BlockPath + "/metadata"
	metadata, err := NewPslice(metadataFilePath, 3)
	if err != nil {
		return nil, err
	}

	// set the records per segment in metadat
	// we may need to get this via some configurations first
	if metadata.Get(FBMetadata_POS_RECORDS_PER_SEGMENT) == 0 {
		value := float64(opts.SegmentSize)
		metadata.Set(FBMetadata_POS_RECORDS_PER_SEGMENT, value)
	}

	blk = &FixedBlock{opts, files, fsize, rsize, rtemp, writeMutex, preallocateMutex, allocateMutex, false, metadata}

	// start the initial pre-allocation process
	err = blk.preallocateIfNeeded()
	if err != nil {
		return nil, err
	}

	if count := blk.metadata.Get(FBMetadata_POS_SEGMENT_COUNT); count > 0 {
		n := int(count)

		for i := 1; i <= n; i++ {
			fpath := blk.BlockPath + "/block_" + strconv.Itoa(i) + ".data"

			file, err := os.OpenFile(fpath, FixedBlockFMode, FixedBlockFPerms)
			if err != nil {
				return nil, err
			}

			finfo, err := file.Stat()
			if err != nil {
				return nil, err
			}

			fsize := finfo.Size()

			sno := int64(i)
			blk.files[sno] = file
			blk.fsize[sno] = fsize
		}
	}

	return blk, nil
}

// NewRecord Creates a new record at the end of the block file
// and returns the index of the record
func (blk *FixedBlock) NewRecord() (rpos int64, err error) {
	blk.allocateMutex.Lock()
	// start allocation if needed, and do it inside a goroutine
	go func() {
		err := blk.preallocateIfNeeded()

		if err != nil {
			log.Printf("could not allocate segement for block at: %s", blk.BlockPath)
		}
	}()

	nextRecordChan := make(chan float64)

	// it's possible to have state where there is no room for a record
	// in this case, we need to wait until the allocation process in complete
	// but allocation happens after this function exits
	// (since it's running inside go routine)
	// that's why we need to run our logic also within a goroutine
	go func() {
		totalRecords := blk.totalRecords()
		nextRecord := blk.metadata.Get(FBMetadata_POS_RECORD_COUNT) + 1

		if nextRecord > totalRecords {
			// wait until allocation
			blk.preallocateMutex.Lock()
			newTotalRecords := blk.totalRecords()
			blk.preallocateMutex.Unlock()

			if nextRecord > newTotalRecords {
				// seems like allocation failed (since there is not new records)
				// let's throw an error
				nextRecordChan <- -1
				return
			}
		}

		nextRecordChan <- nextRecord
	}()

	// get the nextRecord from the above goroutine
	nextRecord := <-nextRecordChan

	if nextRecord == -1 {
		// seems like allocatation failed
		blk.allocateMutex.Unlock()
		return 0, errors.New("could not allocate a record")
	} else {
		// update metadata and then unlock
		blk.metadata.Set(FBMetadata_POS_RECORD_COUNT, nextRecord)
		blk.allocateMutex.Unlock()
		return int64(nextRecord), nil
	}
}

// Put stores a payload `pld` on record at `rpos` at position `ppos`
// rpos and ppos are positions of record and payload and must be
// mutiplied by record size and payload size to get offsets
func (blk *FixedBlock) Put(rpos, ppos int64, pld []byte) (err error) {
	sno := 1 + rpos/blk.SegmentSize
	rpos = rpos % blk.SegmentSize

	file, ok := blk.files[sno]
	if !ok {
		return ErrFixedBlockInvalidSegment
	}

	offset := rpos*blk.rsize + ppos*blk.PayloadSize
	n, err := file.WriteAt(pld, offset)
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
	sno := 1 + rpos/blk.SegmentSize
	rpos = rpos % blk.SegmentSize

	file, ok := blk.files[sno]
	if !ok {
		return nil, ErrFixedBlockInvalidSegment
	}

	offset := rpos*blk.rsize + start*blk.PayloadSize
	pldCount := end - start
	resSize := blk.PayloadSize * pldCount
	resData := make([]byte, resSize)

	n, err := file.ReadAt(resData, offset)
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
func (blk *FixedBlock) Close() error {
	for _, f := range blk.files {
		if err := f.Close(); err != nil {
			return err
		}
	}

	if err := blk.metadata.Close(); err != nil {
		return err
	}

	return nil
}

func (blk *FixedBlock) preallocate(segmentNo int, records int64) error {
	sizeToAllocate := blk.PayloadCount * blk.PayloadSize * records
	segmentFilepath := blk.BlockPath + "/block_" + strconv.Itoa(segmentNo) + ".data"
	fsize := sizeToAllocate

	if _, err := os.Stat(segmentFilepath); err == nil {
		return errors.New("segment file already exists")
	}

	f, err := os.OpenFile(segmentFilepath, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	var chunkSize int64 = 1024 * 1024 * 5 // 5 MB
	var writtenSize int64 = 0
	for true {
		bytesToWrite := sizeToAllocate - writtenSize
		if bytesToWrite == 0 {
			break
		} else if bytesToWrite > chunkSize {
			bytesToWrite = chunkSize
		}

		data := make([]byte, bytesToWrite)
		if n, err := f.WriteAt(data, writtenSize); err != nil {
			return err
		} else if int64(n) != bytesToWrite {
			return errors.New("couldn't write expected bytes to disk")
		}

		writtenSize += bytesToWrite
	}

	sno := int64(segmentNo)
	blk.files[sno] = f
	blk.fsize[sno] = fsize

	return nil
}

func (blk *FixedBlock) totalRecords() float64 {
	segments := blk.metadata.Get(FBMetadata_POS_SEGMENT_COUNT)
	recordsPerSegemnt := blk.metadata.Get(FBMetadata_POS_RECORDS_PER_SEGMENT)
	return recordsPerSegemnt * segments
}

func (blk *FixedBlock) shouldPreallocate() (bool, int) {
	segments := blk.metadata.Get(FBMetadata_POS_SEGMENT_COUNT)
	records := blk.metadata.Get(FBMetadata_POS_RECORD_COUNT)
	recordsPerSegment := blk.metadata.Get(FBMetadata_POS_RECORDS_PER_SEGMENT)

	totalRecords := segments * recordsPerSegment
	freeRecords := totalRecords - records

	if freeRecords < recordsPerSegment/2 {
		return true, int(segments + 1)
	} else {
		return false, 0
	}
}

func (blk *FixedBlock) preallocateIfNeeded() (err error) {
	recordsPerSegment := blk.metadata.Get(FBMetadata_POS_RECORDS_PER_SEGMENT)

	if ok, _ := blk.shouldPreallocate(); ok {
		blk.preallocateMutex.Lock()
		// we need to check again, is it okay to preallocate
		// it's possible to trigger this multiple times
		if okAgain, segmentNo := blk.shouldPreallocate(); okAgain {
			err = blk.preallocate(segmentNo, int64(recordsPerSegment))

			// update metadata if success
			if err == nil {
				blk.metadata.Set(FBMetadata_POS_SEGMENT_COUNT, float64(segmentNo))
			}
		}

		blk.preallocateMutex.Unlock()

		if err != nil {
			return err
		}
	}

	return nil
}

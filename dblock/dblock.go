package dblock

import (
	"errors"
	"os"
	"path"
	"strconv"
	"sync"
	"syscall"

	"github.com/meteorhacks/kdb/pslice"
)

const (
	// default file permissions and modes
	FileOpenMode    = os.O_CREATE | os.O_RDWR
	FilePermissions = 0744

	MetadataCount = 3

	// indexes for metadata values
	MetadataSegmentSize  = 0 // number of records per segment
	MetadataSegmentCount = 1 // number of segments in block
	MetadataRecordCount  = 2 // number of records in block

	// memory mapping params
	MMapProt = syscall.PROT_READ | syscall.PROT_WRITE
	MMapFlag = syscall.MAP_SHARED

	// pre-allocation
	PreallocChunkSize = 1024 * 1024 * 5
)

var (
	ErrSegFileExists  = errors.New("segment file already exists at location")
	ErrSegWriteError  = errors.New("error while writing to segment file")
	ErrSegInvalidMmap = errors.New("requested segment mmap is not available")
	ErrSegCannotAlloc = errors.New("could not create a new segment file")
	ErrAllocRecord    = errors.New("could not allocate space for a new record")

	// reusable byte array
	emptyChunk = make([]byte, PreallocChunkSize, PreallocChunkSize)
)

type Options struct {
	// path to block file
	BlockPath string

	// maximum payload size in bytes
	PayloadSize int64

	// number of payloads in a record
	PayloadCount int64

	// number of records per segment
	SegmentSize int64
}

type DBlock struct {
	Options

	segmentFiles map[int64]*os.File // files used to store segments
	segmentMmaps map[int64][]byte   // memory maps of segment files

	recordSize  int64  // size of a record in bytes
	emptyRecord []byte // reusable when creating new records

	writeMutex    *sync.Mutex
	preallocMutex *sync.Mutex
	allocateMutex *sync.Mutex
	preallocating bool

	metadata *pslice.Pslice // segment metadata
}

func New(opts Options) (blk *DBlock, err error) {
	segmentFiles := make(map[int64]*os.File)
	segmentMmaps := make(map[int64][]byte)

	recordSize := opts.PayloadSize * opts.PayloadCount
	emptyRecord := make([]byte, recordSize, recordSize)

	// load metadata
	metadataFilePath := path.Join(opts.BlockPath, "metadata")
	metadata, err := pslice.New(metadataFilePath, MetadataCount)
	if err != nil {
		return nil, err
	}

	// set `SegmentSize` in metadata file
	if metadata.Get(MetadataSegmentSize) == 0 {
		size := float64(opts.SegmentSize)
		metadata.Set(MetadataSegmentSize, size)
	}

	blk = &DBlock{
		Options:       opts,
		segmentFiles:  segmentFiles,
		segmentMmaps:  segmentMmaps,
		recordSize:    recordSize,
		emptyRecord:   emptyRecord,
		writeMutex:    &sync.Mutex{},
		preallocMutex: &sync.Mutex{},
		allocateMutex: &sync.Mutex{},
		preallocating: false,
		metadata:      metadata,
	}

	// load available segments
	err = blk.loadSegments()
	if err != nil {
		return nil, err
	}

	// initial pre-allocation
	err = blk.preallocateIfNeeded()
	if err != nil {
		return nil, err
	}

	return blk, nil
}

// NewRecord Creates a new record at the end of the block file
// and returns the index of the record (rpos)
func (blk *DBlock) New() (rpos int64, err error) {
	blk.allocateMutex.Lock()
	defer blk.allocateMutex.Unlock()

	nextRecordChan := make(chan float64)
	errorChan := make(chan error)

	// start allocation if needed, and do it inside a goroutine
	go func() {
		err := blk.preallocateIfNeeded()
		errorChan <- err
	}()

	// it's possible to have state where there is no room for a record
	// in this case, we need to wait until the allocation process in complete
	// but allocation happens after this function exits
	// (since it's running inside go routine)
	// that's why we need to run our logic also within a goroutine
	go func() {
		totalRecords := blk.totalRecords()
		nextRecord := blk.metadata.Get(MetadataRecordCount)

		if nextRecord > totalRecords {
			// wait until allocation
			err := <-errorChan

			blk.preallocMutex.Lock()
			newTotalRecords := blk.totalRecords()
			blk.preallocMutex.Unlock()

			if nextRecord > newTotalRecords {
				// seems like allocation failed (since there is not new records)
				nextRecordChan <- -1
				errorChan <- err
				return
			}
		}

		nextRecordChan <- nextRecord
	}()

	// get the nextRecord from the above goroutine
	nextRecord := <-nextRecordChan
	err = <-errorChan

	if nextRecord == -1 {
		// seems like allocation failed
		if err == nil {
			err = ErrAllocRecord
		}

		return 0, err
	}

	// update metadata and then unlock
	blk.metadata.Set(MetadataRecordCount, nextRecord+1)
	rpos = int64(nextRecord)

	return rpos, nil
}

// Put stores a payload `pld` on record starting at `rpos` at position `ppos`
func (blk *DBlock) Put(rpos, ppos int64, pld []byte) (err error) {
	sno := 1 + rpos/blk.SegmentSize
	rpos = rpos % blk.SegmentSize

	mmap, ok := blk.segmentMmaps[sno]
	if !ok {
		panic(ErrSegInvalidMmap)
		return ErrSegInvalidMmap
	}

	start := int(rpos*blk.recordSize + ppos*blk.PayloadSize)
	count := len(pld)

	for i := 0; i < count; i++ {
		mmap[start+i] = pld[i]
	}

	return nil
}

// Get reads payloads from `start` to `end` on a record starting at `rpos`
func (blk *DBlock) Get(rpos, start, end int64) (res [][]byte, err error) {
	sno := 1 + rpos/blk.SegmentSize
	rpos = rpos % blk.SegmentSize

	mmap, ok := blk.segmentMmaps[sno]
	if !ok {
		panic(ErrSegInvalidMmap)
		return nil, ErrSegInvalidMmap
	}

	payloadCount := end - start
	startOffset := rpos*blk.recordSize + start*blk.PayloadSize
	endOffset := startOffset + blk.PayloadSize*payloadCount
	resultData := mmap[startOffset:endOffset]

	res = make([][]byte, payloadCount, payloadCount)

	var i int64
	for i = 0; i < payloadCount; i++ {
		res[i] = resultData[i*blk.PayloadSize : (i+1)*blk.PayloadSize]
	}

	return res, nil
}

// close all file handlers
func (blk *DBlock) Close() (err error) {
	for _, f := range blk.segmentFiles {
		if err := f.Close(); err != nil {
			return err
		}
	}

	if err := blk.metadata.Close(); err != nil {
		return err
	}

	return nil
}

func (blk *DBlock) preallocate(sno int64, records int64) (err error) {
	size := blk.PayloadCount * blk.PayloadSize * records
	fpath := path.Join(blk.BlockPath, "block_"+strconv.Itoa(int(sno)))

	if _, err := os.Stat(fpath); err == nil {
		return ErrSegFileExists
	}

	file, err := os.OpenFile(fpath, FileOpenMode, FilePermissions)
	if err != nil {
		return err
	}

	chunks := size / PreallocChunkSize
	extras := size % PreallocChunkSize

	var i int64
	for i = 0; i < chunks; i++ {
		if n, err := file.Write(emptyChunk); err != nil {
			return err
		} else if n != PreallocChunkSize {
			return ErrSegWriteError
		}
	}

	if n, err := file.Write(emptyChunk[:extras]); err != nil {
		return err
	} else if int64(n) != extras {
		return ErrSegWriteError
	}

	fd := int(file.Fd())
	fsize := int(size)

	mmap, err := syscall.Mmap(fd, 0, fsize, MMapProt, MMapFlag)
	if err != nil {
		return err
	}

	err = syscall.Mlock(mmap)
	if err != nil {
		return err
	}

	blk.segmentFiles[sno] = file
	blk.segmentMmaps[sno] = mmap

	return nil
}

func (blk *DBlock) totalRecords() (total float64) {
	segments := blk.metadata.Get(MetadataSegmentCount)
	recordsPerSegemnt := blk.metadata.Get(MetadataSegmentSize)
	return recordsPerSegemnt * segments
}

func (blk *DBlock) shouldPreallocate() (should bool, nextSegmentId int64) {
	usedSegments := blk.metadata.Get(MetadataSegmentCount)
	usedRecords := blk.metadata.Get(MetadataRecordCount)
	recordsPerSegment := blk.metadata.Get(MetadataSegmentSize)

	totalRecords := usedSegments * recordsPerSegment
	freeRecords := totalRecords - usedRecords

	if freeRecords < recordsPerSegment/2 {
		return true, int64(usedSegments + 1)
	} else {
		return false, 0
	}
}

func (blk *DBlock) preallocateIfNeeded() (err error) {
	blk.preallocMutex.Lock()
	defer blk.preallocMutex.Unlock()

	size := int64(blk.metadata.Get(MetadataSegmentSize))

	if ok, _ := blk.shouldPreallocate(); ok {
		if ok, sno := blk.shouldPreallocate(); ok {
			err = blk.preallocate(sno, size)
			if err != nil {
				return err
			}

			blk.metadata.Set(MetadataSegmentCount, float64(sno))
		}
	}

	return nil
}

// load previously created segments from disk to physical memory
// available segments are found using the metadata file
// * segment file path: BLOCK_PATH/block_1
func (blk *DBlock) loadSegments() (err error) {
	countf := blk.metadata.Get(MetadataSegmentCount)
	if countf == 0 {
		return nil
	}

	count := int(countf)
	for i := 1; i <= count; i++ {
		fpath := path.Join(blk.BlockPath, "block_"+strconv.Itoa(i))

		file, err := os.OpenFile(fpath, FileOpenMode, FilePermissions)
		if err != nil {
			return err
		}

		finfo, err := file.Stat()
		if err != nil {
			return err
		}

		fsize := int(finfo.Size())
		fd := int(file.Fd())

		mmap, err := syscall.Mmap(fd, 0, fsize, MMapProt, MMapFlag)
		if err != nil {
			return err
		}

		err = syscall.Mlock(mmap)
		if err != nil {
			return err
		}

		sno := int64(i)
		blk.segmentFiles[sno] = file
		blk.segmentMmaps[sno] = mmap
	}

	return nil
}

package rblock

import (
	"errors"
	"os"
	"path"
	"strconv"

	"github.com/meteorhacks/kdb/pslice"
)

const (
	// default file permissions and modes
	FileOpenMode    = os.O_RDONLY
	FilePermissions = 0744

	MetadataCount = 3

	// indexes for metadata values
	MetadataSegmentSize  = 0 // number of records per segment
	MetadataSegmentCount = 1 // number of segments in block
	MetadataRecordCount  = 2 // number of records in block
)

var (
	ErrSegReadError    = errors.New("error while reading from segment file")
	ErrSegInvalidMmap  = errors.New("requested segment mmap is not available")
	ErrWriteOnReadOnly = errors.New("write operation on a read only block")
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
	recordSize   int64              // size of a record in bytes
	metadata     *pslice.Pslice     // segment metadata
}

func New(opts Options) (blk *DBlock, err error) {
	segmentFiles := make(map[int64]*os.File)
	recordSize := opts.PayloadSize * opts.PayloadCount

	// load metadata
	metadataFilePath := path.Join(opts.BlockPath, "metadata")
	metadata, err := pslice.New(metadataFilePath, MetadataCount)
	if err != nil {
		return nil, err
	}

	blk = &DBlock{
		Options:      opts,
		segmentFiles: segmentFiles,
		recordSize:   recordSize,
		metadata:     metadata,
	}

	// load available segments
	err = blk.loadSegments()
	if err != nil {
		return nil, err
	}

	return blk, nil
}

func (blk *DBlock) New() (rpos int64, err error) {
	return 0, ErrWriteOnReadOnly
}

func (blk *DBlock) Put(rpos, ppos int64, pld []byte) (err error) {
	return ErrWriteOnReadOnly
}

// Get reads payloads from `start` to `end` on a record starting at `rpos`
func (blk *DBlock) Get(rpos, start, end int64) (res [][]byte, err error) {
	sno := 1 + rpos/blk.SegmentSize
	rpos = rpos % blk.SegmentSize

	file, ok := blk.segmentFiles[sno]
	if !ok {
		panic(ErrSegInvalidMmap)
		return nil, ErrSegInvalidMmap
	}

	payloadCount := end - start
	startOffset := rpos*blk.recordSize + start*blk.PayloadSize

	resultBytes := payloadCount * blk.PayloadSize
	resultData := make([]byte, resultBytes, resultBytes)

	n, err := file.ReadAt(resultData, startOffset)
	if err != nil {
		return nil, err
	} else if int64(n) != resultBytes {
		return nil, ErrSegReadError
	}

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

// open previously created segment files
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

		sno := int64(i)
		blk.segmentFiles[sno] = file
	}

	return nil
}

package kdb

type Block interface {
	NewRecord() (rpos int64, err error)
	Put(rpos, ppos int64, pld []byte) (err error)
	Get(rpos, start, end int64) (res [][]byte, err error)
	Close() (err error)
}

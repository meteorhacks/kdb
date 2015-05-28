package kdb

type Database interface {
	Put(ts, pno int64, vals []string, pld []byte) (err error)
	Get(pno, start, end int64, vals []string) (res [][]byte, err error)
	Find(pno, start, end int64, vals []string) (res map[*IndexElement][][]byte, err error)
}

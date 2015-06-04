package kdb

// A database is simply a configuration of components mentioned below.
// Data validation is encouraged to be done in database level.
// A database may have many buckets for different time ranges.
type Database interface {
	Put(ts int64, vals []string, pld []byte) (err error)
	Get(start, end int64, vals []string) (res [][]byte, err error)
	Find(start, end int64, vals []string) (res map[*IndexElement][][]byte, err error)
	Close() (err error)
}

// A bucket contains data in a time interval. Data is grouped into buckets in
// order to delete old data easier and faster with little effect to the system.
// A bucket contains its own `Index` and a `Block`.
type Bucket interface {
	Put(ts int64, vals []string, pld []byte) (err error)
	Get(start, end int64, vals []string) (res [][]byte, err error)
	Find(start, end int64, vals []string) (res map[*IndexElement][][]byte, err error)
	Close() (err error)
}

// Blocks are used to stores arbitrary data ([]byte) as a series in records.
// At the moment, KDB only supports fixed size payloads using `dblock` package.
// Payloads are placed as records ordered by time.
type Block interface {
	New() (rpos int64, err error)
	Put(rpos, ppos int64, pld []byte) (err error)
	Get(rpos, start, end int64) (res [][]byte, err error)
	Close() (err error)
}

// Indexes are a map of record position to some pre configured fields.
// All queries are first made to an Index and later used with Blocks.
type Index interface {
	Add(vals []string, rpos int64) (el *IndexElement, err error)
	Get(vals []string) (el *IndexElement, err error)
	Find(vals []string) (els []*IndexElement, err error)
	Close() (err error)
}

// Struct representing an element in the index. Here we are maintaining a
// tree structure. So, it's `Values` field is only available in leaf nodes
// `Children` is  only available in root and intermediate level nodes.
// All the data elements are on the lowest level, which are leaf nodes.
type IndexElement struct {
	Values   []string
	Position int64
	Children map[string]*IndexElement
}

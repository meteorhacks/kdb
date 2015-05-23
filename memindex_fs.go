package kdb

// ## Filesystem Utils for MemIndex
// We've two types of files which are appendFiles and snapshots
//
// * AppendFile is used when writing the index for the current range of data
// * After that, we can make a snapshot of that and save in the disk for later use
// * We can load from both appendFile and Snapshot
// * Loading from appendFile allow us to recover from a crash

// Set the append file for the index
// every new index will be append to this file
func (m MemIndex) SetAppendFile(filename string) {

}

// load the index from the append file
func (m MemIndex) LoadFromAppendFile(filename string) {

}

// save the snapshot of the index with binary encoding and compression
func (m MemIndex) SaveSnapshot(filename string) {

}

// load the index from a snapshot in the file system
func (m MemIndex) LoadFromSnapshot(filename string) {

}

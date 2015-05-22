package kdb

import (
	"os"
	"testing"
)

// --------------- //
//      Index      //
// --------------- //

func TestIndexSyncAndLoad(t *testing.T) {
	defer os.Remove("/tmp/d1")
	in, err := NewIndex(IndexOpts{
		Path: "/tmp/d1",
	})

	defer in.Close()

	if err != nil {
		t.Error(err)
	}

	// try a sync when file is empty
	err = in.Sync()
	if err != nil {
		t.Error(err)
	}

	// add some data and sync in memory index to file
	in.data["foo"] = 100
	err = in.Sync()
	if err != nil {
		t.Error(err)
	}

	// reset in-memory index and load from file
	in.data = map[string]int64{}
	err = in.Load()
	if err != nil {
		t.Error(err)
	}

	// make sure the data decoded fine
	if val, ok := in.data["foo"]; !ok || val != 100 {
		t.Error("incorrect value")
	}
}

func BenchmarkIndexSync(b *testing.B) {
	defer os.Remove("/tmp/d1")
	in, err := NewIndex(IndexOpts{
		Path: "/tmp/d1",
	})

	defer in.Close()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		in.data["foo"] = int64(i)
		err = in.Sync()
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkIndexLoad(b *testing.B) {
	defer os.Remove("/tmp/d1")
	in, err := NewIndex(IndexOpts{
		Path: "/tmp/d1",
	})

	defer in.Close()

	in.data["foo"] = 100
	err = in.Sync()
	if err != nil {
		b.Error(err)
	}

	empty := map[string]int64{}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		in.data = empty
		err = in.Load()
		if err != nil {
			b.Error(err)
		}
	}
}

// -------------- //
//      Data      //
// -------------- //

func TestDataNewRecord(t *testing.T) {
	defer os.Remove("/tmp/d1")
	dt, err := NewData(DataOpts{
		Path:  "/tmp/d1",
		Size:  4,
		Count: 10,
	})

	defer dt.Close()

	fi, err := dt.file.Stat()
	if err != nil {
		t.Error(err)
	} else if fz := fi.Size(); fz != 0 {
		t.Error("file size should be 0 ", fz)
	}

	// try creating a few records and see how the data file grows
	for i := 0; i < 5; i++ {
		o, err := dt.NewRecord()
		if err != nil {
			t.Error(err)
		} else if e := 4 * 10 * i; o != int64(e) {
			t.Error("offset should be ", e, o)
		}
	}
}

func TestDataWriteRead(t *testing.T) {
	defer os.Remove("/tmp/d1")
	dt, err := NewData(DataOpts{
		Path:  "/tmp/d1",
		Size:  4,
		Count: 10,
	})

	defer dt.Close()

	o, err := dt.NewRecord()
	if err != nil {
		t.Error(err)
	}

	b1 := []byte("byte")
	err = dt.Write(b1, o)
	if err != nil {
		t.Error(err)
	}

	b2 := []byte("kite")
	err = dt.Write(b2, o+4)
	if err != nil {
		t.Error(err)
	}

	v1, err := dt.Read(o, 1)
	if err != nil {
		t.Error(err)
	}

	if string(b1) != string(v1) {
		t.Error("values should match")
	}

	v2, err := dt.Read(o, 2)
	if err != nil {
		t.Error(err)
	}

	if string(b1)+string(b2) != string(v2) {
		t.Error("values should match")
	}
}

func BenchmarkDataNewRecord(b *testing.B) {
	defer os.Remove("/tmp/d1")
	dt, err := NewData(DataOpts{
		Path:  "/tmp/d1",
		Size:  4,
		Count: 10,
	})

	if err != nil {
		b.Error(err)
	}

	defer dt.Close()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := dt.NewRecord()
		if err != nil {
			b.Error(err)
		}
	}
}

// TODO: randomize write points
func BenchmarkDataWrite(b *testing.B) {
	defer os.Remove("/tmp/d1")
	dt, err := NewData(DataOpts{
		Path:  "/tmp/d1",
		Size:  4,
		Count: 10,
	})

	if err != nil {
		b.Error(err)
	}

	defer dt.Close()

	o, err := dt.NewRecord()
	if err != nil {
		b.Error(err)
	}

	b1 := []byte("byte")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err = dt.Write(b1, o)
		if err != nil {
			b.Error(err)
		}
	}
}

// TODO: randomize read points
func BenchmarkDataRead(b *testing.B) {
	defer os.Remove("/tmp/d1")
	dt, err := NewData(DataOpts{
		Path:  "/tmp/d1",
		Size:  4,
		Count: 10,
	})

	if err != nil {
		b.Error(err)
	}

	defer dt.Close()

	o, err := dt.NewRecord()
	if err != nil {
		b.Error(err)
	}

	b1 := []byte("byte")

	err = dt.Write(b1, o)
	if err != nil {
		b.Error(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err = dt.Read(o, 1)
		if err != nil {
			b.Error(err)
		}
	}
}

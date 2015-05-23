package kdb

import (
	"os"
	"testing"
)

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

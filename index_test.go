package kdb

import (
	"os"
	"testing"
)

func TestIndexSyncAndLoad(t *testing.T) {
	defer os.Remove("/tmp/d1")
	in, err := NewIndex(IndexOpts{
		Path: "/tmp/d1",
		Keys: []string{"foo"},
	})

	defer in.Close()

	if err != nil {
		t.Fatal(err)
	}

	// try a sync when file is empty
	err = in.Sync()
	if err != nil {
		t.Fatal(err)
	}

	// add some data and sync in memory index to file
	in.data.AddItem(map[string]string{"foo": "bar"}, 100)
	err = in.Sync()
	if err != nil {
		t.Fatal(err)
	}

	// reset in-memory index and load from file
	in.data = NewMemIndex([]string{"foo"})
	err = in.Load()
	if err != nil {
		t.Fatal(err)
	}

	// make sure the data decoded fine
	els, err := in.data.FindElements(map[string]string{"foo": "bar"})
	if err != nil {
		t.Fatal(err)
	}

	if len(els) != 1 {
		t.Fatal("incorrect number of results")
	}

	el := els[0]

	if el.Values[0] != "bar" {
		t.Fatal("incorrect value")
	}

	if el.Position != 100 {
		t.Fatal("incorrect position")
	}
}

func BenchmarkIndexSync(b *testing.B) {
	defer os.Remove("/tmp/d1")
	in, err := NewIndex(IndexOpts{
		Path: "/tmp/d1",
		Keys: []string{"foo"},
	})

	defer in.Close()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		in.data.AddItem(map[string]string{"foo": "bar"}, int64(i))
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
		Keys: []string{"foo"},
	})

	defer in.Close()

	in.data.AddItem(map[string]string{"foo": "bar"}, 100)
	err = in.Sync()
	if err != nil {
		b.Error(err)
	}

	empty := NewMemIndex([]string{"foo"})

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		in.data = empty
		err = in.Load()
		if err != nil {
			b.Error(err)
		}
	}
}

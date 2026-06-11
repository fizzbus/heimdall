package storage

import (
	"os"
	"testing"
)

func TestIndex_AppendAndLookup(t *testing.T) {
	f, _ := os.CreateTemp("", "*.index")
	f.Close()
	defer os.Remove(f.Name())

	idx, err := OpenIndex(f.Name())
	if err != nil {
		t.Fatalf("OpenIndex: %v", err)
	}
	defer idx.Close()

	for i := int64(0); i < 5; i++ {
		if err := idx.Append(i, i*100); err != nil {
			t.Fatalf("Append(%d): %v", i, err)
		}
	}

	for i := int64(0); i < 5; i++ {
		pos, err := idx.Lookup(i)
		if err != nil {
			t.Errorf("Lookup(%d): %v", i, err)
		}
		if pos != i*100 {
			t.Errorf("Lookup(%d): got %d, want %d", i, pos, i*100)
		}
	}
}

func TestIndex_LookupNotFound(t *testing.T) {
	f, _ := os.CreateTemp("", "*.index")
	f.Close()
	defer os.Remove(f.Name())

	idx, _ := OpenIndex(f.Name())
	defer idx.Close()

	idx.Append(0, 0)
	idx.Append(1, 100)

	_, err := idx.Lookup(99)
	if err == nil {
		t.Error("expected error for missing offset, got nil")
	}
}

func TestIndex_LastOffset(t *testing.T) {
	f, _ := os.CreateTemp("", "*.index")
	f.Close()
	defer os.Remove(f.Name())

	idx, _ := OpenIndex(f.Name())
	defer idx.Close()

	if got := idx.LastOffset(10); got != 10 {
		t.Errorf("empty index LastOffset: got %d, want 10", got)
	}

	idx.Append(10, 0)
	idx.Append(11, 128)

	if got := idx.LastOffset(10); got != 12 {
		t.Errorf("LastOffset after 2 entries: got %d, want 12", got)
	}
}

func TestIndex_Count(t *testing.T) {
	f, _ := os.CreateTemp("", "*.index")
	f.Close()
	defer os.Remove(f.Name())

	idx, _ := OpenIndex(f.Name())
	defer idx.Close()

	for i := 0; i < 7; i++ {
		idx.Append(int64(i), int64(i*50))
	}

	if got := idx.Count(); got != 7 {
		t.Errorf("Count: got %d, want 7", got)
	}
}

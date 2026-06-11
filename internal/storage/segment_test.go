package storage

import (
	"os"
	"testing"
	"time"
)

func tempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "segment-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func TestSegment_WriteAndRead(t *testing.T) {
	dir := tempDir(t)
	seg, err := NewSegment(dir, 0, 1024*1024)
	if err != nil {
		t.Fatalf("NewSegment: %v", err)
	}
	defer seg.Close()

	entries := []Entry{
		{Offset: 0, Timestamp: time.Now().UnixMilli(), Key: []byte("k1"), Value: []byte("v1")},
		{Offset: 1, Timestamp: time.Now().UnixMilli(), Key: []byte("k2"), Value: []byte("v2")},
		{Offset: 2, Timestamp: time.Now().UnixMilli(), Key: []byte("k3"), Value: []byte("v3")},
	}

	for _, e := range entries {
		if err := seg.Write(e); err != nil {
			t.Fatalf("Write offset %d: %v", e.Offset, err)
		}
	}

	got, err := seg.Read(1, 4096)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) < 2 {
		t.Fatalf("Read: got %d entries, want >= 2", len(got))
	}
	if string(got[0].Value) != "v2" {
		t.Errorf("Read[0].Value = %q, want %q", got[0].Value, "v2")
	}
}

func TestSegment_IsFull(t *testing.T) {
	dir := tempDir(t)
	// header=24, key=3, value=5 → одна запись = 32 байта
	// maxBytes=31 → после первой записи сегмент переполнен
	seg, _ := NewSegment(dir, 0, 31)
	defer seg.Close()

	e := Entry{Offset: 0, Timestamp: 0, Key: []byte("key"), Value: []byte("value")}
	seg.Write(e)

	if !seg.IsFull() {
		t.Errorf("segment should be full: logSize=%d, maxBytes=31", seg.logSize)
	}
}

func TestSegment_RestoreAfterReopen(t *testing.T) {
	dir := tempDir(t)

	seg, _ := NewSegment(dir, 0, 1024*1024)
	for i := int64(0); i < 3; i++ {
		seg.Write(Entry{Offset: i, Key: []byte("k"), Value: []byte("v")})
	}
	seg.Close()

	seg2, err := NewSegment(dir, 0, 1024*1024)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer seg2.Close()

	if seg2.NextOffset() != 3 {
		t.Errorf("NextOffset after reopen: got %d, want 3", seg2.NextOffset())
	}
}

func TestLoadSegments(t *testing.T) {
	dir := tempDir(t)

	for _, base := range []int64{0, 100, 200} {
		seg, _ := NewSegment(dir, base, 1024*1024)
		seg.Write(Entry{Offset: base, Key: []byte("k"), Value: []byte("v")})
		seg.Close()
	}

	segments, err := LoadSegments(dir, 1024*1024)
	if err != nil {
		t.Fatalf("LoadSegments: %v", err)
	}
	if len(segments) != 3 {
		t.Errorf("LoadSegments: got %d segments, want 3", len(segments))
	}
	if segments[0].BaseOffset() != 0 || segments[1].BaseOffset() != 100 {
		t.Error("LoadSegments: wrong order or base offsets")
	}
	for _, s := range segments {
		s.Close()
	}
}

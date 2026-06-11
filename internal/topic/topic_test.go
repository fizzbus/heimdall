package topic

import (
	"os"
	"testing"

	"heimdall/config"
)

func testConfig(t *testing.T) *config.Config {
	t.Helper()
	dir, _ := os.MkdirTemp("", "topic-test-*")
	t.Cleanup(func() { os.RemoveAll(dir) })
	return &config.Config{
		DataDir:         dir,
		SegmentMaxBytes: 1024 * 1024,
	}
}

func TestTopic_New(t *testing.T) {
	cfg := testConfig(t)
	tp, err := New("events", 3, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tp.Close()

	if tp.PartitionCount() != 3 {
		t.Errorf("PartitionCount: got %d, want 3", tp.PartitionCount())
	}
}

func TestTopic_New_InvalidPartitions(t *testing.T) {
	cfg := testConfig(t)
	_, err := New("bad", 0, cfg)
	if err == nil {
		t.Error("expected error for numPartitions=0, got nil")
	}
}

func TestTopic_ProduceAndFetch(t *testing.T) {
	cfg := testConfig(t)
	tp, _ := New("logs", 1, cfg)
	defer tp.Close()

	offset, err := tp.Produce(0, []byte("k"), []byte("hello"))
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}
	if offset != 0 {
		t.Errorf("first offset: got %d, want 0", offset)
	}

	msgs, err := tp.Fetch(0, 0, 4096)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(msgs) != 1 || string(msgs[0].Value) != "hello" {
		t.Errorf("Fetch: got %v", msgs)
	}
}

func TestTopic_Produce_InvalidPartition(t *testing.T) {
	cfg := testConfig(t)
	tp, _ := New("x", 2, cfg)
	defer tp.Close()

	_, err := tp.Produce(99, nil, []byte("msg"))
	if err == nil {
		t.Error("expected error for invalid partition, got nil")
	}
}

func TestTopic_NextOffset(t *testing.T) {
	cfg := testConfig(t)
	tp, _ := New("offsets", 1, cfg)
	defer tp.Close()

	for i := 0; i < 5; i++ {
		tp.Produce(0, nil, []byte("msg"))
	}

	if got := tp.NextOffset(0); got != 5 {
		t.Errorf("NextOffset: got %d, want 5", got)
	}
}

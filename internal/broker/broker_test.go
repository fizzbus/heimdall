package broker

import (
	"os"
	"testing"

	"heimdall/config"
)

func testConfig(t *testing.T) *config.Config {
	t.Helper()
	dir, _ := os.MkdirTemp("", "broker-test-*")
	t.Cleanup(func() { os.RemoveAll(dir) })
	return &config.Config{
		DataDir:         dir,
		SegmentMaxBytes: 1024 * 1024,
		RetentionHours:  24,
	}
}

func TestBroker_CreateAndListTopics(t *testing.T) {
	b := New(testConfig(t))
	defer b.Close()

	if err := b.CreateTopic("orders", 3); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	topics := b.ListTopics()
	if len(topics) != 1 || topics[0] != "orders" {
		t.Errorf("ListTopics: got %v, want [orders]", topics)
	}
}

func TestBroker_CreateTopic_Duplicate(t *testing.T) {
	b := New(testConfig(t))
	defer b.Close()

	b.CreateTopic("events", 1)
	if err := b.CreateTopic("events", 1); err == nil {
		t.Error("expected error on duplicate topic, got nil")
	}
}

func TestBroker_ProduceAndFetch(t *testing.T) {
	b := New(testConfig(t))
	defer b.Close()

	b.CreateTopic("logs", 1)

	offset, err := b.Produce("logs", 0, []byte("key"), []byte("hello"))
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}
	if offset != 0 {
		t.Errorf("first offset: got %d, want 0", offset)
	}

	msgs, err := b.Fetch("logs", 0, 0, 4096)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("Fetch: got %d msgs, want 1", len(msgs))
	}
	if string(msgs[0].Value) != "hello" {
		t.Errorf("value = %q, want %q", msgs[0].Value, "hello")
	}
}

func TestBroker_Produce_TopicNotFound(t *testing.T) {
	b := New(testConfig(t))
	defer b.Close()

	_, err := b.Produce("nonexistent", 0, nil, []byte("msg"))
	if err == nil {
		t.Error("expected error for missing topic, got nil")
	}
}

func TestBroker_MultiplePartitions(t *testing.T) {
	b := New(testConfig(t))
	defer b.Close()

	b.CreateTopic("multi", 3)

	for p := 0; p < 3; p++ {
		_, err := b.Produce("multi", p, nil, []byte("msg"))
		if err != nil {
			t.Errorf("Produce to partition %d: %v", p, err)
		}
	}

	for p := 0; p < 3; p++ {
		msgs, err := b.Fetch("multi", p, 0, 4096)
		if err != nil {
			t.Errorf("Fetch from partition %d: %v", p, err)
		}
		if len(msgs) != 1 {
			t.Errorf("partition %d: got %d msgs, want 1", p, len(msgs))
		}
	}
}

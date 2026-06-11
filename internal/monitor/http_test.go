package monitor

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"heimdall/config"
	"heimdall/internal/broker"
	"os"
)

func testBroker(t *testing.T) *broker.Broker {
	t.Helper()
	dir, _ := os.MkdirTemp("", "monitor-test-*")
	t.Cleanup(func() { os.RemoveAll(dir) })
	return broker.New(&config.Config{
		DataDir:         dir,
		SegmentMaxBytes: 1024 * 1024,
	})
}

func TestHandleHealth(t *testing.T) {
	s := NewServer(":0", testBroker(t))

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	s.handleHealth(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status: got %d, want %d", w.Code, http.StatusOK)
	}

	var resp map[string]string
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp["status"] != "ok" {
		t.Errorf("status field: got %q, want %q", resp["status"], "ok")
	}
	if resp["time"] == "" {
		t.Error("time field should not be empty")
	}
}

func TestHandleHealth_MethodNotAllowed(t *testing.T) {
	s := NewServer(":0", testBroker(t))

	req := httptest.NewRequest(http.MethodPost, "/health", nil)
	w := httptest.NewRecorder()
	s.handleHealth(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("status: got %d, want %d", w.Code, http.StatusMethodNotAllowed)
	}
}

func TestHandleTopics_Empty(t *testing.T) {
	s := NewServer(":0", testBroker(t))

	req := httptest.NewRequest(http.MethodGet, "/topics", nil)
	w := httptest.NewRecorder()
	s.handleTopics(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status: got %d, want %d", w.Code, http.StatusOK)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)

	count := resp["count"].(float64)
	if count != 0 {
		t.Errorf("count: got %.0f, want 0", count)
	}
}

func TestHandleTopics_WithTopics(t *testing.T) {
	b := testBroker(t)
	b.CreateTopic("orders", 2)
	b.CreateTopic("events", 1)

	s := NewServer(":0", b)

	req := httptest.NewRequest(http.MethodGet, "/topics", nil)
	w := httptest.NewRecorder()
	s.handleTopics(w, req)

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)

	count := resp["count"].(float64)
	if count != 2 {
		t.Errorf("count: got %.0f, want 2", count)
	}
}

func TestHandleTopicDetail(t *testing.T) {
	b := testBroker(t)
	b.CreateTopic("logs", 3)

	s := NewServer(":0", b)

	req := httptest.NewRequest(http.MethodGet, "/topics/logs", nil)
	w := httptest.NewRecorder()
	s.handleTopicDetail(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status: got %d, want %d", w.Code, http.StatusOK)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)

	if resp["name"] != "logs" {
		t.Errorf("name: got %q, want %q", resp["name"], "logs")
	}

	partitions := resp["partitions"].([]any)
	if len(partitions) != 3 {
		t.Errorf("partitions: got %d, want 3", len(partitions))
	}
}

func TestHandleTopicDetail_NotFound(t *testing.T) {
	s := NewServer(":0", testBroker(t))

	req := httptest.NewRequest(http.MethodGet, "/topics/nonexistent", nil)
	w := httptest.NewRecorder()
	s.handleTopicDetail(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("status: got %d, want %d", w.Code, http.StatusNotFound)
	}
}

func TestHandleTopicDetail_EmptyName(t *testing.T) {
	s := NewServer(":0", testBroker(t))

	req := httptest.NewRequest(http.MethodGet, "/topics/", nil)
	w := httptest.NewRecorder()
	s.handleTopicDetail(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status: got %d, want %d", w.Code, http.StatusBadRequest)
	}
}

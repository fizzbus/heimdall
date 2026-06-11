package network

import (
	"encoding/binary"
	"net"
	"os"
	"testing"

	"heimdall/config"
	"heimdall/internal/broker"
	"heimdall/pkg/protocol"
)

func testBroker(t *testing.T) *broker.Broker {
	t.Helper()
	dir, _ := os.MkdirTemp("", "network-test-*")
	t.Cleanup(func() { os.RemoveAll(dir) })
	return broker.New(&config.Config{
		DataDir:         dir,
		SegmentMaxBytes: 1024 * 1024,
	})
}

// pipeHandler создаёт Handler поверх net.Pipe() — без реального TCP.
func pipeHandler(t *testing.T, b *broker.Broker) (handler *Handler, client net.Conn) {
	t.Helper()
	server, client := net.Pipe()
	handler = NewHandler(server, b)
	// Закрываем server-сторону только после завершения теста
	t.Cleanup(func() { client.Close() })
	return handler, client
}

// sendFrame отправляет фрейм клиенту: [length:4][apiKey:1][payload]
func sendFrame(conn net.Conn, key protocol.APIKey, payload []byte) {
	body := append([]byte{byte(key)}, payload...)
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(body)))
	conn.Write(lenBuf)
	conn.Write(body)
}

// recvFrame читает один фрейм ответа.
func recvFrame(conn net.Conn) []byte {
	lenBuf := make([]byte, 4)
	if _, err := conn.Read(lenBuf); err != nil {
		return nil
	}
	size := binary.BigEndian.Uint32(lenBuf)
	buf := make([]byte, size)
	total := 0
	for total < int(size) {
		n, err := conn.Read(buf[total:])
		if err != nil {
			break
		}
		total += n
	}
	return buf
}

func encodeCreateTopic(topic string, partitions int) []byte {
	tb := []byte(topic)
	buf := make([]byte, 2+len(tb)+2)
	binary.BigEndian.PutUint16(buf[0:2], uint16(len(tb)))
	copy(buf[2:], tb)
	binary.BigEndian.PutUint16(buf[2+len(tb):], uint16(partitions))
	return buf
}

func encodeProduce(topic string, partition int, key, value string) []byte {
	tb, kb, vb := []byte(topic), []byte(key), []byte(value)
	buf := make([]byte, 2+len(tb)+2+4+len(kb)+4+len(vb))
	pos := 0
	binary.BigEndian.PutUint16(buf[pos:], uint16(len(tb)))
	pos += 2
	copy(buf[pos:], tb)
	pos += len(tb)
	binary.BigEndian.PutUint16(buf[pos:], uint16(partition))
	pos += 2
	binary.BigEndian.PutUint32(buf[pos:], uint32(len(kb)))
	pos += 4
	copy(buf[pos:], kb)
	pos += len(kb)
	binary.BigEndian.PutUint32(buf[pos:], uint32(len(vb)))
	pos += 4
	copy(buf[pos:], vb)
	return buf
}

func encodeFetch(topic string, partition int, offset int64, maxBytes int) []byte {
	tb := []byte(topic)
	buf := make([]byte, 2+len(tb)+2+8+4)
	pos := 0
	binary.BigEndian.PutUint16(buf[pos:], uint16(len(tb)))
	pos += 2
	copy(buf[pos:], tb)
	pos += len(tb)
	binary.BigEndian.PutUint16(buf[pos:], uint16(partition))
	pos += 2
	binary.BigEndian.PutUint64(buf[pos:], uint64(offset))
	pos += 8
	binary.BigEndian.PutUint32(buf[pos:], uint32(maxBytes))
	return buf
}

func TestHandler_CreateTopic(t *testing.T) {
	b := testBroker(t)
	h, client := pipeHandler(t, b)
	go h.Handle()

	sendFrame(client, protocol.APICreateTopic, encodeCreateTopic("orders", 2))
	resp := recvFrame(client)
	assertResponse(t, resp, 2)
	client.Close()

	if resp[1] != 0x00 {
		t.Errorf("CreateTopic: errCode = 0x%02x, want 0x00", resp[1])
	}
}

func TestHandler_Produce(t *testing.T) {
	b := testBroker(t)
	b.CreateTopic("logs", 1)

	h, client := pipeHandler(t, b)
	go h.Handle()

	sendFrame(client, protocol.APIProduce, encodeProduce("logs", 0, "k", "hello"))
	resp := recvFrame(client)
	assertResponse(t, resp, 12)
	client.Close()

	if resp[1] != 0x00 {
		t.Fatalf("Produce: errCode = 0x%02x", resp[1])
	}
	offset := int64(binary.BigEndian.Uint64(resp[4:12]))
	if offset != 0 {
		t.Errorf("offset: got %d, want 0", offset)
	}
}

func TestHandler_Fetch(t *testing.T) {
	b := testBroker(t)
	b.CreateTopic("events", 1)
	b.Produce("events", 0, []byte("k"), []byte("value"))

	h, client := pipeHandler(t, b)
	go h.Handle()

	sendFrame(client, protocol.APIFetch, encodeFetch("events", 0, 0, 4096))
	resp := recvFrame(client)
	assertResponse(t, resp, 8)
	client.Close()

	if resp[1] != 0x00 {
		t.Fatalf("Fetch: errCode = 0x%02x", resp[1])
	}
	count := binary.BigEndian.Uint32(resp[4:8])
	if count != 1 {
		t.Errorf("messages: got %d, want 1", count)
	}
}

func TestHandler_ListTopics(t *testing.T) {
	b := testBroker(t)
	b.CreateTopic("t1", 1)
	b.CreateTopic("t2", 1)

	h, client := pipeHandler(t, b)
	go h.Handle()

	sendFrame(client, protocol.APIListTopics, []byte{})
	resp := recvFrame(client)
	assertResponse(t, resp, 6)
	client.Close()

	if resp[1] != 0x00 {
		t.Fatalf("ListTopics: errCode = 0x%02x", resp[1])
	}
	count := binary.BigEndian.Uint16(resp[4:6])
	if count != 2 {
		t.Errorf("topics: got %d, want 2", count)
	}
}

func TestHandler_UnknownAPIKey(t *testing.T) {
	b := testBroker(t)
	h, client := pipeHandler(t, b)
	go h.Handle()

	sendFrame(client, protocol.APIKey(0xFF), []byte{})
	resp := recvFrame(client)
	assertResponse(t, resp, 2)
	client.Close()

	// APIKey=0x00 (unknown), errCode должен быть != 0x00
	if resp[1] == 0x00 {
		t.Error("expected error for unknown API key, got errCode=0x00")
	}
}

func assertResponse(t *testing.T, resp []byte, minLen int) {
	t.Helper()
	if len(resp) < minLen {
		t.Fatalf("response too short: got %d bytes, want >= %d", len(resp), minLen)
	}
}

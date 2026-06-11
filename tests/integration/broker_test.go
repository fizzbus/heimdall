package integration

import (
	"encoding/binary"
	"net"
	"testing"

	"heimdall/pkg/protocol"
)

const brokerAddr = "localhost:9092"

// dial открывает соединение с брокером и регистрирует закрытие.
func dial(t *testing.T) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", brokerAddr)
	if err != nil {
		t.Skipf("broker not running at %s, skipping integration test", brokerAddr)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func send(conn net.Conn, key protocol.APIKey, payload []byte) {
	body := append([]byte{byte(key)}, payload...)
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(body)))
	conn.Write(lenBuf)
	conn.Write(body)
}

func recv(conn net.Conn) []byte {
	lenBuf := make([]byte, 4)
	conn.Read(lenBuf)
	size := binary.BigEndian.Uint32(lenBuf)
	buf := make([]byte, size)
	total := 0
	for total < int(size) {
		n, _ := conn.Read(buf[total:])
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

// uniqueTopic генерирует уникальное имя топика для каждого теста.
func uniqueTopic(t *testing.T) string {
	return "test-" + t.Name()
}

// ── Тест-кейсы ────────────────────────────────────────────────────────────────

func TestIntegration_CreateTopic(t *testing.T) {
	conn := dial(t)
	topic := uniqueTopic(t)

	send(conn, protocol.APICreateTopic, encodeCreateTopic(topic, 2))
	resp := recv(conn)

	if resp[1] != 0x00 {
		t.Errorf("CreateTopic: errCode = 0x%02x, want 0x00", resp[1])
	}
}

func TestIntegration_CreateTopic_Duplicate(t *testing.T) {
	conn := dial(t)
	topic := uniqueTopic(t)

	send(conn, protocol.APICreateTopic, encodeCreateTopic(topic, 1))
	recv(conn)

	send(conn, protocol.APICreateTopic, encodeCreateTopic(topic, 1))
	resp := recv(conn)

	if resp[1] == 0x00 {
		t.Error("expected error on duplicate topic, got errCode=0x00")
	}
}

func TestIntegration_ProduceAndFetch(t *testing.T) {
	conn := dial(t)
	topic := uniqueTopic(t)

	send(conn, protocol.APICreateTopic, encodeCreateTopic(topic, 1))
	recv(conn)

	// Публикуем сообщение
	send(conn, protocol.APIProduce, encodeProduce(topic, 0, "key1", "hello"))
	resp := recv(conn)

	if resp[1] != 0x00 {
		t.Fatalf("Produce: errCode = 0x%02x", resp[1])
	}
	offset := int64(binary.BigEndian.Uint64(resp[4:12]))
	if offset != 0 {
		t.Errorf("first offset: got %d, want 0", offset)
	}

	// Читаем
	send(conn, protocol.APIFetch, encodeFetch(topic, 0, 0, 4096))
	resp = recv(conn)

	if resp[1] != 0x00 {
		t.Fatalf("Fetch: errCode = 0x%02x", resp[1])
	}
	count := binary.BigEndian.Uint32(resp[4:8])
	if count != 1 {
		t.Errorf("Fetch: got %d messages, want 1", count)
	}
}

func TestIntegration_SequentialOffsets(t *testing.T) {
	conn := dial(t)
	topic := uniqueTopic(t)

	send(conn, protocol.APICreateTopic, encodeCreateTopic(topic, 1))
	recv(conn)

	// Публикуем 10 сообщений — смещения должны идти 0..9
	for i := 0; i < 10; i++ {
		send(conn, protocol.APIProduce,
			encodeProduce(topic, 0, "k", "msg"))
		resp := recv(conn)
		offset := int64(binary.BigEndian.Uint64(resp[4:12]))
		if offset != int64(i) {
			t.Errorf("message %d: got offset %d, want %d", i, offset, i)
		}
	}
}

func TestIntegration_FetchFromMiddle(t *testing.T) {
	conn := dial(t)
	topic := uniqueTopic(t)

	send(conn, protocol.APICreateTopic, encodeCreateTopic(topic, 1))
	recv(conn)

	for i := 0; i < 5; i++ {
		send(conn, protocol.APIProduce,
			encodeProduce(topic, 0, "k", "msg"))
		recv(conn)
	}

	// Читаем начиная с offset=3 — должно вернуться 2 сообщения
	send(conn, protocol.APIFetch, encodeFetch(topic, 0, 3, 4096))
	resp := recv(conn)

	if resp[1] != 0x00 {
		t.Fatalf("Fetch: errCode = 0x%02x", resp[1])
	}
	count := binary.BigEndian.Uint32(resp[4:8])
	if count < 2 {
		t.Errorf("Fetch from offset 3: got %d messages, want >= 2", count)
	}
}

func TestIntegration_FetchUnknownTopic(t *testing.T) {
	conn := dial(t)

	send(conn, protocol.APIFetch, encodeFetch("nonexistent-topic", 0, 0, 1024))
	resp := recv(conn)

	if resp[1] == 0x00 {
		t.Error("expected error for unknown topic, got errCode=0x00")
	}
}

func TestIntegration_ListTopics(t *testing.T) {
	conn := dial(t)
	topic := uniqueTopic(t)

	send(conn, protocol.APICreateTopic, encodeCreateTopic(topic, 1))
	recv(conn)

	send(conn, protocol.APIListTopics, []byte{})
	resp := recv(conn)

	if resp[1] != 0x00 {
		t.Fatalf("ListTopics: errCode = 0x%02x", resp[1])
	}
	count := binary.BigEndian.Uint16(resp[4:6])
	if count == 0 {
		t.Error("ListTopics: expected at least 1 topic")
	}
}

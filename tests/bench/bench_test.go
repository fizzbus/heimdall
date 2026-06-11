package bench

import (
	"encoding/binary"
	"fmt"
	"net"
	"testing"

	"heimdall/pkg/protocol"
)

const brokerAddr = "localhost:9092"

func BenchmarkProduce(b *testing.B) {
	conn, err := net.Dial("tcp", brokerAddr)
	if err != nil {
		b.Skipf("broker not running: %v", err)
	}
	defer conn.Close()

	topic := "bench-produce"
	setupTopic(conn, topic, 1)

	payload := encodeProduce(topic, 0, "k", "benchmark-message-payload")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		send(conn, protocol.APIProduce, payload)
		recv(conn)
	}
}

func BenchmarkProduceParallel(b *testing.B) {
	topic := fmt.Sprintf("bench-parallel-%d", b.N)

	b.RunParallel(func(pb *testing.PB) {
		conn, err := net.Dial("tcp", brokerAddr)
		if err != nil {
			b.Skipf("broker not running: %v", err)
		}
		defer conn.Close()

		setupTopic(conn, topic, 10)
		payload := encodeProduce(topic, 0, "k", "benchmark-message-payload")

		for pb.Next() {
			send(conn, protocol.APIProduce, payload)
			recv(conn)
		}
	})
}

func setupTopic(conn net.Conn, topic string, partitions int) {
	tb := []byte(topic)
	buf := make([]byte, 2+len(tb)+2)
	binary.BigEndian.PutUint16(buf[0:2], uint16(len(tb)))
	copy(buf[2:], tb)
	binary.BigEndian.PutUint16(buf[2+len(tb):], uint16(partitions))
	send(conn, protocol.APICreateTopic, buf)
	recv(conn)
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

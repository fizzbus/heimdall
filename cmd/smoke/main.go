package main

import (
	"encoding/binary"
	"fmt"
	"net"

	"heimdall/pkg/protocol"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:9092")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// 1. Создать топик
	send(conn, protocol.APICreateTopic, encodeCreateTopic("test", 1))
	resp := recv(conn)
	if resp[1] != 0x00 {
		fmt.Printf("✗ CreateTopic error: %s\n", string(resp[4:]))
		return
	}
	fmt.Println("✓ CreateTopic")

	// 2. Опубликовать сообщение
	send(conn, protocol.APIProduce, encodeProduce("test", 0, "key1", "hello heimdall"))
	resp = recv(conn)
	if resp[1] != 0x00 {
		fmt.Printf("✗ Produce error: %s\n", string(resp[4:]))
		return
	}
	// Структура ответа Produce: [apiKey:1][errCode:1][errMsgLen:2][offset:8]
	offset := int64(binary.BigEndian.Uint64(resp[4:12]))
	fmt.Printf("✓ Produce → offset %d\n", offset)

	// 3. Прочитать сообщение
	send(conn, protocol.APIFetch, encodeFetch("test", 0, 0, 1024))
	resp = recv(conn)
	if resp[1] != 0x00 {
		fmt.Printf("✗ Fetch error: %s\n", string(resp[4:]))
		return
	}
	// [apiKey:1][errCode:1][errMsgLen:2][count:4][messages...]
	count := binary.BigEndian.Uint32(resp[4:8])
	fmt.Printf("✓ Fetch → получено %d сообщений\n", count)

	// 4. Список топиков
	send(conn, protocol.APIListTopics, []byte{})
	resp = recv(conn)
	topicCount := binary.BigEndian.Uint16(resp[4:6])
	fmt.Printf("✓ ListTopics → топиков: %d\n", topicCount)
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
	if _, err := conn.Read(lenBuf); err != nil {
		panic(fmt.Sprintf("recv length error: %v", err))
	}
	size := binary.BigEndian.Uint32(lenBuf)
	buf := make([]byte, size)
	total := 0
	for total < int(size) {
		n, err := conn.Read(buf[total:])
		if err != nil {
			panic(fmt.Sprintf("recv body error: %v", err))
		}
		total += n
	}
	return buf
}

func encodeCreateTopic(topic string, numPartitions int) []byte {
	tb := []byte(topic)
	buf := make([]byte, 2+len(tb)+2)
	binary.BigEndian.PutUint16(buf[0:2], uint16(len(tb)))
	copy(buf[2:], tb)
	binary.BigEndian.PutUint16(buf[2+len(tb):], uint16(numPartitions))
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

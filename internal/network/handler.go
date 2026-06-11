package network

import (
	"bufio"
	"encoding/binary"
	"io"
	"log"
	"net"
	"time"

	"heimdall/internal/broker"
	"heimdall/pkg/protocol"
)

// Handler обслуживает одно TCP-соединение.
type Handler struct {
	conn   net.Conn
	broker *broker.Broker
	reader *bufio.Reader
	writer *bufio.Writer
}

// NewHandler создаёт обработчик для соединения.
func NewHandler(conn net.Conn, b *broker.Broker) *Handler {
	return &Handler{
		conn:   conn,
		broker: b,
		reader: bufio.NewReaderSize(conn, 64*1024),
		writer: bufio.NewWriterSize(conn, 64*1024),
	}
}

// Handle читает и обрабатывает запросы в цикле до закрытия соединения.
func (h *Handler) Handle() {
	defer h.conn.Close()
	addr := h.conn.RemoteAddr().String()
	log.Printf("[handler] connected: %s", addr)

	for {
		h.conn.SetReadDeadline(time.Now().Add(5 * time.Minute))

		req, err := h.readRequest()
		if err != nil {
			if err != io.EOF {
				// Отправляем ответ с ошибкой перед закрытием
				h.conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
				errResp := protocol.NewErrorResponse(protocol.APIUnknown, err)
				h.writeResponse(errResp)
			}
			return
		}

		resp := h.dispatch(req)

		h.conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
		if err := h.writeResponse(resp); err != nil {
			log.Printf("[handler] write error to %s: %v", addr, err)
			return
		}
	}
}

// readRequest читает один запрос из соединения.
// Формат фрейма: [length:4][apiKey:1][payload...]
func (h *Handler) readRequest() (*protocol.Request, error) {
	// Читаем длину фрейма
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(h.reader, lenBuf); err != nil {
		return nil, err
	}
	frameLen := binary.BigEndian.Uint32(lenBuf)

	// Читаем тело фрейма
	body := make([]byte, frameLen)
	if _, err := io.ReadFull(h.reader, body); err != nil {
		return nil, err
	}

	return protocol.DecodeRequest(body)
}

// dispatch маршрутизирует запрос к нужному методу брокера.
func (h *Handler) dispatch(req *protocol.Request) *protocol.Response {
	switch req.APIKey {

	case protocol.APICreateTopic:
		r := req.CreateTopic
		err := h.broker.CreateTopic(r.Topic, r.NumPartitions)
		return protocol.NewErrorResponse(req.APIKey, err)

	case protocol.APIProduce:
		r := req.Produce
		offset, err := h.broker.Produce(r.Topic, r.Partition, r.Key, r.Value)
		if err != nil {
			return protocol.NewErrorResponse(req.APIKey, err)
		}
		return protocol.NewProduceResponse(offset)

	case protocol.APIFetch:
		r := req.Fetch
		msgs, err := h.broker.Fetch(r.Topic, r.Partition, r.Offset, r.MaxBytes)
		if err != nil {
			return protocol.NewErrorResponse(req.APIKey, err)
		}
		return protocol.NewFetchResponse(msgs)

	case protocol.APIListTopics:
		topics := h.broker.ListTopics()
		return protocol.NewListTopicsResponse(topics)

	default:
		return protocol.NewErrorResponse(req.APIKey,
			protocol.ErrUnknownAPIKey)
	}
}

// writeResponse сериализует ответ и отправляет его клиенту.
// Формат фрейма: [length:4][payload...]
func (h *Handler) writeResponse(resp *protocol.Response) error {
	payload := protocol.EncodeResponse(resp)

	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(payload)))

	if _, err := h.writer.Write(lenBuf); err != nil {
		return err
	}
	if _, err := h.writer.Write(payload); err != nil {
		return err
	}
	return h.writer.Flush()
}

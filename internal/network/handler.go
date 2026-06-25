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
	defer log.Printf("[handler] disconnected: %s", addr)

	for {
		h.conn.SetReadDeadline(time.Now().Add(5 * time.Minute))

		req, err := h.readRequest()
		if err != nil {
			if err == io.EOF {
				log.Printf("[handler] %s closed connection (EOF)", addr)
			} else {
				log.Printf("[handler] readRequest error from %s: %v", addr, err)
				h.conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
				errResp := protocol.NewErrorResponse(protocol.APIUnknown, err)
				h.writeResponse(errResp)
			}
			return
		}

		log.Printf("[handler] %s → apiKey=0x%02x", addr, req.APIKey)

		resp := h.dispatch(req)

		log.Printf("[handler] %s ← apiKey=0x%02x dispatched", addr, req.APIKey)

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
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(h.reader, lenBuf); err != nil {
		return nil, err
	}

	frameLen := binary.BigEndian.Uint32(lenBuf)
	log.Printf("[handler] reading frame from %s: frameLen=%d", h.conn.RemoteAddr(), frameLen)

	body := make([]byte, frameLen)
	if _, err := io.ReadFull(h.reader, body); err != nil {
		return nil, err
	}

	req, err := protocol.DecodeRequest(body)
	if err != nil {
		log.Printf("[handler] DecodeRequest error from %s: %v (raw: %x)", h.conn.RemoteAddr(), err, body)
		return nil, err
	}

	return req, nil
}

// dispatch маршрутизирует запрос к нужному методу брокера.
func (h *Handler) dispatch(req *protocol.Request) *protocol.Response {
	switch req.APIKey {

	case protocol.APICreateTopic:
		r := req.CreateTopic
		log.Printf("[handler] CreateTopic topic=%q partitions=%d", r.Topic, r.NumPartitions)
		err := h.broker.CreateTopic(r.Topic, r.NumPartitions)
		if err != nil {
			log.Printf("[handler] CreateTopic error: %v", err)
		} else {
			log.Printf("[handler] CreateTopic ok: topic=%q", r.Topic)
		}
		return protocol.NewErrorResponse(req.APIKey, err)

	case protocol.APIProduce:
		r := req.Produce
		log.Printf("[handler] Produce topic=%q partition=%d keyLen=%d valueLen=%d",
			r.Topic, r.Partition, len(r.Key), len(r.Value))
		offset, err := h.broker.Produce(r.Topic, r.Partition, r.Key, r.Value)
		if err != nil {
			log.Printf("[handler] Produce error: %v", err)
			return protocol.NewErrorResponse(req.APIKey, err)
		}
		log.Printf("[handler] Produce ok: topic=%q offset=%d", r.Topic, offset)
		return protocol.NewProduceResponse(offset)

	case protocol.APIFetch:
		r := req.Fetch
		log.Printf("[handler] Fetch topic=%q partition=%d offset=%d maxBytes=%d",
			r.Topic, r.Partition, r.Offset, r.MaxBytes)
		msgs, err := h.broker.Fetch(r.Topic, r.Partition, r.Offset, r.MaxBytes)
		if err != nil {
			log.Printf("[handler] Fetch error: %v", err)
			return protocol.NewErrorResponse(req.APIKey, err)
		}
		log.Printf("[handler] Fetch ok: topic=%q msgs=%d", r.Topic, len(msgs))
		return protocol.NewFetchResponse(msgs)

	case protocol.APIListTopics:
		topics := h.broker.ListTopics()
		log.Printf("[handler] ListTopics → %v", topics)
		return protocol.NewListTopicsResponse(topics)

	default:
		log.Printf("[handler] unknown apiKey=0x%02x", req.APIKey)
		return protocol.NewErrorResponse(req.APIKey, protocol.ErrUnknownAPIKey)
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

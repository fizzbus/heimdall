package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"

	"heimdall/internal/topic"
)

// APIKey идентифицирует тип запроса.
type APIKey uint8

const (
	APICreateTopic APIKey = 0x01
	APIProduce     APIKey = 0x02
	APIFetch       APIKey = 0x03
	APIListTopics  APIKey = 0x04
	APIUnknown     APIKey = 0x00
)

// Коды ошибок в ответах.
const (
	ErrCodeNone    uint8 = 0x00
	ErrCodeGeneral uint8 = 0x01
	ErrCodeUnknown uint8 = 0x02
)

var ErrUnknownAPIKey = errors.New("unknown API key")

// ── Структуры запросов ────────────────────────────────────────────────────────

type CreateTopicRequest struct {
	Topic         string
	NumPartitions int
}

type ProduceRequest struct {
	Topic     string
	Partition int
	Key       []byte
	Value     []byte
}

type FetchRequest struct {
	Topic     string
	Partition int
	Offset    int64
	MaxBytes  int
}

type ListTopicsRequest struct{}

// Request — универсальная обёртка запроса.
type Request struct {
	APIKey      APIKey
	CreateTopic *CreateTopicRequest
	Produce     *ProduceRequest
	Fetch       *FetchRequest
	ListTopics  *ListTopicsRequest
}

// ── Структуры ответов ─────────────────────────────────────────────────────────

type Response struct {
	APIKey  APIKey
	ErrCode uint8
	ErrMsg  string

	// Produce
	Offset int64

	// Fetch
	Messages []topic.Message

	// ListTopics
	Topics []string
}

// ── Конструкторы ответов ──────────────────────────────────────────────────────

func NewErrorResponse(key APIKey, err error) *Response {
	r := &Response{APIKey: key}
	if err != nil {
		r.ErrCode = ErrCodeGeneral
		r.ErrMsg = err.Error()
	}
	return r
}

func NewProduceResponse(offset int64) *Response {
	return &Response{APIKey: APIProduce, Offset: offset}
}

func NewFetchResponse(msgs []topic.Message) *Response {
	return &Response{APIKey: APIFetch, Messages: msgs}
}

func NewListTopicsResponse(topics []string) *Response {
	return &Response{APIKey: APIListTopics, Topics: topics}
}

// ── Декодирование запроса ─────────────────────────────────────────────────────

// DecodeRequest разбирает бинарное тело фрейма в структуру Request.
// Формат тела: [apiKey:1][payload по типу]
func DecodeRequest(data []byte) (*Request, error) {
	if len(data) < 1 {
		return nil, errors.New("request too short")
	}

	req := &Request{APIKey: APIKey(data[0])}
	payload := data[1:]

	switch req.APIKey {

	case APICreateTopic:
		// [topicLen:2][topic][numPartitions:2]
		r, err := decodeCreateTopic(payload)
		if err != nil {
			return nil, err
		}
		req.CreateTopic = r

	case APIProduce:
		// [topicLen:2][topic][partition:2][keyLen:4][key][valueLen:4][value]
		r, err := decodeProduce(payload)
		if err != nil {
			return nil, err
		}
		req.Produce = r

	case APIFetch:
		// [topicLen:2][topic][partition:2][offset:8][maxBytes:4]
		r, err := decodeFetch(payload)
		if err != nil {
			return nil, err
		}
		req.Fetch = r

	case APIListTopics:
		req.ListTopics = &ListTopicsRequest{}

	default:
		return nil, fmt.Errorf("unknown APIKey: 0x%02x", data[0])
	}

	return req, nil
}

func decodeCreateTopic(data []byte) (*CreateTopicRequest, error) {
	if len(data) < 2 {
		return nil, errors.New("CreateTopic: too short")
	}
	topicLen := int(binary.BigEndian.Uint16(data[0:2]))
	if len(data) < 2+topicLen+2 {
		return nil, errors.New("CreateTopic: truncated")
	}
	topic := string(data[2 : 2+topicLen])
	numPartitions := int(binary.BigEndian.Uint16(data[2+topicLen : 2+topicLen+2]))
	return &CreateTopicRequest{Topic: topic, NumPartitions: numPartitions}, nil
}

func decodeProduce(data []byte) (*ProduceRequest, error) {
	if len(data) < 2 {
		return nil, errors.New("Produce: too short")
	}
	pos := 0

	topicLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
	pos += 2
	if len(data) < pos+topicLen {
		return nil, errors.New("Produce: truncated topic")
	}
	topicName := string(data[pos : pos+topicLen])
	pos += topicLen

	partition := int(binary.BigEndian.Uint16(data[pos : pos+2]))
	pos += 2

	keyLen := int(binary.BigEndian.Uint32(data[pos : pos+4]))
	pos += 4
	key := make([]byte, keyLen)
	copy(key, data[pos:pos+keyLen])
	pos += keyLen

	valueLen := int(binary.BigEndian.Uint32(data[pos : pos+4]))
	pos += 4
	value := make([]byte, valueLen)
	copy(value, data[pos:pos+valueLen])

	return &ProduceRequest{
		Topic:     topicName,
		Partition: partition,
		Key:       key,
		Value:     value,
	}, nil
}

func decodeFetch(data []byte) (*FetchRequest, error) {
	pos := 0

	topicLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
	pos += 2
	topicName := string(data[pos : pos+topicLen])
	pos += topicLen

	partition := int(binary.BigEndian.Uint16(data[pos : pos+2]))
	pos += 2

	offset := int64(binary.BigEndian.Uint64(data[pos : pos+8]))
	pos += 8

	maxBytes := int(binary.BigEndian.Uint32(data[pos : pos+4]))

	return &FetchRequest{
		Topic:     topicName,
		Partition: partition,
		Offset:    offset,
		MaxBytes:  maxBytes,
	}, nil
}

// ── Кодирование ответа ────────────────────────────────────────────────────────

// EncodeResponse сериализует Response в байты для отправки клиенту.
// Формат: [apiKey:1][errCode:1][errMsgLen:2][errMsg][payload по типу]
func EncodeResponse(resp *Response) []byte {
	buf := []byte{byte(resp.APIKey), resp.ErrCode}

	errMsg := []byte(resp.ErrMsg)
	msgLen := make([]byte, 2)
	binary.BigEndian.PutUint16(msgLen, uint16(len(errMsg)))
	buf = append(buf, msgLen...)
	buf = append(buf, errMsg...)

	if resp.ErrCode != ErrCodeNone {
		return buf
	}

	switch resp.APIKey {

	case APIProduce:
		// [offset:8]
		ob := make([]byte, 8)
		binary.BigEndian.PutUint64(ob, uint64(resp.Offset))
		buf = append(buf, ob...)

	case APIFetch:
		// [count:4] затем для каждого сообщения:
		// [offset:8][timestamp:8][keyLen:4][key][valueLen:4][value]
		cb := make([]byte, 4)
		binary.BigEndian.PutUint32(cb, uint32(len(resp.Messages)))
		buf = append(buf, cb...)

		for _, m := range resp.Messages {
			ob := make([]byte, 8)
			binary.BigEndian.PutUint64(ob, uint64(m.Offset))
			buf = append(buf, ob...)

			tb := make([]byte, 8)
			binary.BigEndian.PutUint64(tb, uint64(m.Timestamp))
			buf = append(buf, tb...)

			kb := make([]byte, 4)
			binary.BigEndian.PutUint32(kb, uint32(len(m.Key)))
			buf = append(buf, kb...)
			buf = append(buf, m.Key...)

			vb := make([]byte, 4)
			binary.BigEndian.PutUint32(vb, uint32(len(m.Value)))
			buf = append(buf, vb...)
			buf = append(buf, m.Value...)
		}

	case APIListTopics:
		// [count:2] затем для каждого топика: [nameLen:2][name]
		cb := make([]byte, 2)
		binary.BigEndian.PutUint16(cb, uint16(len(resp.Topics)))
		buf = append(buf, cb...)

		for _, t := range resp.Topics {
			tb := []byte(t)
			lb := make([]byte, 2)
			binary.BigEndian.PutUint16(lb, uint16(len(tb)))
			buf = append(buf, lb...)
			buf = append(buf, tb...)
		}
	}

	return buf
}

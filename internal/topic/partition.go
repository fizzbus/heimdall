package topic

import (
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"heimdall/config"
	"heimdall/internal/storage"
)

// Partition представляет одну партицию топика.
type Partition struct {
	mu            sync.RWMutex
	topicName     string
	id            int
	segments      []*storage.Segment
	activeSegment *storage.Segment
	cfg           *config.Config
	nextOffset    int64
}

// NewPartition создаёт партицию, загружая существующие сегменты с диска.
func NewPartition(topicName string, id int, cfg *config.Config) (*Partition, error) {
	dir := filepath.Join(cfg.DataDir, topicName, fmt.Sprintf("partition-%d", id))

	segments, err := storage.LoadSegments(dir, cfg.SegmentMaxBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to load segments: %w", err)
	}

	var active *storage.Segment
	var nextOffset int64

	if len(segments) == 0 {
		// Новая партиция — создаём первый сегмент с базовым смещением 0
		active, err = storage.NewSegment(dir, 0, cfg.SegmentMaxBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to create initial segment: %w", err)
		}
		segments = append(segments, active)
		nextOffset = 0
	} else {
		// Восстанавливаем из существующих сегментов
		active = segments[len(segments)-1]
		nextOffset = active.NextOffset()
	}

	return &Partition{
		topicName:     topicName,
		id:            id,
		segments:      segments,
		activeSegment: active,
		cfg:           cfg,
		nextOffset:    nextOffset,
	}, nil
}

// Write записывает сообщение в активный сегмент.
// Если сегмент заполнен — создаётся новый.
func (p *Partition) Write(key, value []byte) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Ротация сегмента при переполнении
	if p.activeSegment.IsFull() {
		if err := p.rotate(); err != nil {
			return 0, fmt.Errorf("segment rotation failed: %w", err)
		}
	}

	offset := p.nextOffset
	msg := storage.Entry{
		Offset:    offset,
		Timestamp: time.Now().UnixMilli(),
		Key:       key,
		Value:     value,
	}

	if err := p.activeSegment.Write(msg); err != nil {
		return 0, fmt.Errorf("failed to write message: %w", err)
	}

	p.nextOffset++
	return offset, nil
}

// Read читает сообщения начиная с offset, суммарно не более maxBytes.
func (p *Partition) Read(offset int64, maxBytes int) ([]Message, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	seg := p.findSegment(offset)
	if seg == nil {
		return nil, fmt.Errorf("offset %d not found in partition %d", offset, p.id)
	}

	entries, err := seg.Read(offset, maxBytes)
	if err != nil {
		return nil, err
	}

	msgs := make([]Message, 0, len(entries))
	for _, e := range entries {
		msgs = append(msgs, Message{
			Offset:    e.Offset,
			Key:       e.Key,
			Value:     e.Value,
			Timestamp: e.Timestamp,
		})
	}
	return msgs, nil
}

// Close сбрасывает буферы активного сегмента на диск.
func (p *Partition) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	_ = p.activeSegment.Close()
}

// rotate закрывает текущий активный сегмент и открывает новый.
func (p *Partition) rotate() error {
	if err := p.activeSegment.Close(); err != nil {
		return err
	}

	newSeg, err := storage.NewSegment(
		p.activeSegment.Dir(),
		p.nextOffset,
		p.cfg.SegmentMaxBytes,
	)
	if err != nil {
		return err
	}

	p.segments = append(p.segments, newSeg)
	p.activeSegment = newSeg
	return nil
}

// findSegment находит сегмент, который содержит заданное смещение.
// Сегменты отсортированы по базовому смещению по возрастанию.
func (p *Partition) findSegment(offset int64) *storage.Segment {
	// Бинарный поиск: нужен последний сегмент с baseOffset <= offset
	lo, hi := 0, len(p.segments)-1
	for lo <= hi {
		mid := (lo + hi) / 2
		base := p.segments[mid].BaseOffset()
		if base == offset {
			return p.segments[mid]
		} else if base < offset {
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	if hi >= 0 {
		return p.segments[hi]
	}
	return nil
}

// NextOffset возвращает следующее свободное смещение партиции.
func (p *Partition) NextOffset() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.nextOffset
}

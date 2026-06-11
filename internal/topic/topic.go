package topic

import (
	"fmt"
	"sync"

	"heimdall/config"
)

// Message — единица данных, возвращаемая при чтении.
type Message struct {
	Offset    int64
	Key       []byte
	Value     []byte
	Timestamp int64
}

// Topic представляет именованный топик с набором партиций.
type Topic struct {
	mu         sync.RWMutex
	name       string
	partitions []*Partition
}

// New создаёт топик с заданным числом партиций.
func New(name string, numPartitions int, cfg *config.Config) (*Topic, error) {
	if numPartitions <= 0 {
		return nil, fmt.Errorf("numPartitions must be > 0, got %d", numPartitions)
	}

	partitions := make([]*Partition, numPartitions)
	for i := 0; i < numPartitions; i++ {
		p, err := NewPartition(name, i, cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to create partition %d: %w", i, err)
		}
		partitions[i] = p
	}

	return &Topic{
		name:       name,
		partitions: partitions,
	}, nil
}

// Produce записывает сообщение в указанную партицию.
func (t *Topic) Produce(partitionID int, key, value []byte) (int64, error) {
	p, err := t.getPartition(partitionID)
	if err != nil {
		return 0, err
	}
	return p.Write(key, value)
}

// Fetch читает сообщения из указанной партиции начиная со смещения offset.
func (t *Topic) Fetch(partitionID int, offset int64, maxBytes int) ([]Message, error) {
	p, err := t.getPartition(partitionID)
	if err != nil {
		return nil, err
	}
	return p.Read(offset, maxBytes)
}

// PartitionCount возвращает число партиций топика.
func (t *Topic) PartitionCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.partitions)
}

// Close завершает работу всех партиций топика.
func (t *Topic) Close() {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, p := range t.partitions {
		p.Close()
	}
}

// getPartition возвращает партицию по ID с проверкой границ.
func (t *Topic) getPartition(id int) (*Partition, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if id < 0 || id >= len(t.partitions) {
		return nil, fmt.Errorf("partition %d out of range [0, %d)", id, len(t.partitions))
	}
	return t.partitions[id], nil
}

// NextOffset возвращает следующее свободное смещение партиции.
func (t *Topic) NextOffset(partitionID int) int64 {
	p, err := t.getPartition(partitionID)
	if err != nil {
		return 0
	}
	return p.nextOffset
}

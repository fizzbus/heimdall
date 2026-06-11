package broker

import (
	"fmt"
	"sync"

	"heimdall/config"
	"heimdall/internal/topic"
)

// Broker — ядро брокера сообщений.
type Broker struct {
	mu     sync.RWMutex
	topics map[string]*topic.Topic
	cfg    *config.Config
}

// New создаёт новый экземпляр брокера.
func New(cfg *config.Config) *Broker {
	return &Broker{
		topics: make(map[string]*topic.Topic),
		cfg:    cfg,
	}
}

// CreateTopic создаёт новый топик с заданным числом партиций.
func (b *Broker) CreateTopic(name string, numPartitions int) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.topics[name]; exists {
		return fmt.Errorf("topic %q already exists", name)
	}

	t, err := topic.New(name, numPartitions, b.cfg)
	if err != nil {
		return fmt.Errorf("failed to create topic %q: %w", name, err)
	}

	b.topics[name] = t
	return nil
}

// Produce записывает сообщение в указанный топик и партицию.
func (b *Broker) Produce(topicName string, partitionID int, key, value []byte) (int64, error) {
	b.mu.RLock()
	t, exists := b.topics[topicName]
	b.mu.RUnlock()

	if !exists {
		return 0, fmt.Errorf("topic %q not found", topicName)
	}

	return t.Produce(partitionID, key, value)
}

// Fetch читает сообщения из указанного топика, партиции и смещения.
func (b *Broker) Fetch(topicName string, partitionID int, offset int64, maxBytes int) ([]topic.Message, error) {
	b.mu.RLock()
	t, exists := b.topics[topicName]
	b.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("topic %q not found", topicName)
	}

	return t.Fetch(partitionID, offset, maxBytes)
}

// ListTopics возвращает список всех топиков.
func (b *Broker) ListTopics() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.topics))
	for name := range b.topics {
		names = append(names, name)
	}
	return names
}

// Close корректно завершает работу всех топиков.
func (b *Broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, t := range b.topics {
		t.Close()
	}
}

// GetTopic возвращает топик по имени или ошибку, если не найден.
func (b *Broker) GetTopic(name string) (*topic.Topic, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	t, exists := b.topics[name]
	if !exists {
		return nil, fmt.Errorf("topic %q not found", name)
	}
	return t, nil
}

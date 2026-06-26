package broker

import (
	"fmt"
	"log"
	"os"
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

// New создаёт брокер и восстанавливает топики с диска если они существуют.
func New(cfg *config.Config) (*Broker, error) {
	b := &Broker{
		topics: make(map[string]*topic.Topic),
		cfg:    cfg,
	}
	if err := b.loadFromDisk(); err != nil {
		return nil, fmt.Errorf("failed to restore topics from disk: %w", err)
	}
	return b, nil
}

// loadFromDisk сканирует DataDir и восстанавливает все топики.
func (b *Broker) loadFromDisk() error {
	entries, err := os.ReadDir(b.cfg.DataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // чистый старт, ничего восстанавливать не нужно
		}
		return err
	}

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		t, err := topic.Load(name, b.cfg)
		if err != nil {
			return fmt.Errorf("failed to load topic %q: %w", name, err)
		}
		b.topics[name] = t
		log.Printf("[broker] restored topic %q (%d partitions)", name, t.PartitionCount())
	}
	return nil
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

// GetTopic возвращает топик по имени или ошибку если не найден.
func (b *Broker) GetTopic(name string) (*topic.Topic, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	t, exists := b.topics[name]
	if !exists {
		return nil, fmt.Errorf("topic %q not found", name)
	}
	return t, nil
}

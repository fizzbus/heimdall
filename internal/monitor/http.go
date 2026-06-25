package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"heimdall/internal/broker"
)

// Server — HTTP-сервер мониторинга.
type Server struct {
	addr   string
	broker *broker.Broker
	srv    *http.Server
}

// NewServer создаёт HTTP-сервер мониторинга.
func NewServer(addr string, b *broker.Broker) *Server {
	return &Server{
		addr:   addr,
		broker: b,
	}
}

// Start регистрирует маршруты и запускает HTTP-сервер.
func (s *Server) Start() error {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/topics", s.handleTopics)
	mux.HandleFunc("/topics/", s.handleTopicDetail)
	mux.HandleFunc("/metrics", s.handleMetrics)

	s.srv = &http.Server{
		Addr:         s.addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	log.Printf("[monitor] HTTP listening on %s", s.addr)
	return s.srv.ListenAndServe()
}

// Stop корректно останавливает HTTP-сервер.
func (s *Server) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.srv.Shutdown(ctx)
}

// ── Обработчики ───────────────────────────────────────────────────────────────

// GET /health
// Возвращает статус брокера и текущее время.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	resp := map[string]string{
		"status": "ok",
		"time":   time.Now().UTC().Format(time.RFC3339),
	}
	writeJSON(w, http.StatusOK, resp)
}

// GET /topics
// Возвращает список всех топиков.
func (s *Server) handleTopics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	topics := s.broker.ListTopics()

	type topicInfo struct {
		Name string `json:"name"`
	}

	result := make([]topicInfo, 0, len(topics))
	for _, t := range topics {
		result = append(result, topicInfo{Name: t})
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"topics": result,
		"count":  len(result),
	})
}

// GET /topics/{name}
// Возвращает информацию о конкретном топике: число партиций, смещения.
func (s *Server) handleTopicDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Извлекаем имя топика из пути /topics/{name}
	topicName := r.URL.Path[len("/topics/"):]
	if topicName == "" {
		http.Error(w, "topic name required", http.StatusBadRequest)
		return
	}

	t, err := s.broker.GetTopic(topicName)
	if err != nil {
		http.Error(w, fmt.Sprintf("topic not found: %s", topicName), http.StatusNotFound)
		return
	}

	type partitionInfo struct {
		ID         int   `json:"id"`
		NextOffset int64 `json:"next_offset"`
	}

	partitions := make([]partitionInfo, t.PartitionCount())
	for i := 0; i < t.PartitionCount(); i++ {
		partitions[i] = partitionInfo{
			ID:         i,
			NextOffset: t.NextOffset(i),
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"name":       topicName,
		"partitions": partitions,
	})
}

// ── Вспомогательные функции ───────────────────────────────────────────────────

// writeJSON сериализует v в JSON и отправляет ответ с заданным статусом.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("[monitor] failed to write JSON response: %v", err)
	}
}

// GET /metrics
// Возвращает метрики в формате Prometheus text exposition 0.0.4.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)

	topics := s.broker.ListTopics()

	var topicsTotal int
	var messagesTotal int64
	var bytesInTotal int64
	var bytesOutTotal int64
	var activeConnections int64 = 0 // если в broker нет счётчика, временно 0
	var sb strings.Builder

	topicsTotal = len(topics)

	for _, topicName := range topics {
		t, err := s.broker.GetTopic(topicName)
		if err != nil {
			continue
		}

		for p := 0; p < t.PartitionCount(); p++ {
			nextOffset := t.NextOffset(p)
			messagesTotal += nextOffset

			// Для demo можно считать lag как 0, пока нет committed offsets в broker API.
			// Если у тебя есть метод получения committed offset по group/topic/partition —
			// подставь его сюда вместо нуля.
			lag := int64(0)

			sb.WriteString(fmt.Sprintf(
				"consumer_group_lag{group=\"order-processors\",topic=%q,partition=\"%d\"} %d\n",
				topicName, p, lag,
			))
		}
	}

	// Для демо: bytes можно оценивать как 0, если брокер не хранит счётчики.
	// Если у тебя есть реальные counters в broker, подставь их вместо этих значений.
	sb.WriteString("# HELP broker_messages_in_total Total number of messages produced\n")
	sb.WriteString("# TYPE broker_messages_in_total counter\n")
	sb.WriteString(fmt.Sprintf("broker_messages_in_total %d\n", messagesTotal))

	sb.WriteString("# HELP broker_bytes_in_total Total bytes received by broker\n")
	sb.WriteString("# TYPE broker_bytes_in_total counter\n")
	sb.WriteString(fmt.Sprintf("broker_bytes_in_total %d\n", bytesInTotal))

	sb.WriteString("# HELP broker_bytes_out_total Total bytes sent by broker\n")
	sb.WriteString("# TYPE broker_bytes_out_total counter\n")
	sb.WriteString(fmt.Sprintf("broker_bytes_out_total %d\n", bytesOutTotal))

	sb.WriteString("# HELP broker_active_connections Current active TCP connections\n")
	sb.WriteString("# TYPE broker_active_connections gauge\n")
	sb.WriteString(fmt.Sprintf("broker_active_connections %d\n", activeConnections))

	sb.WriteString("# HELP broker_topics_total Number of topics in broker\n")
	sb.WriteString("# TYPE broker_topics_total gauge\n")
	sb.WriteString(fmt.Sprintf("broker_topics_total %d\n", topicsTotal))

	sb.WriteString("# HELP consumer_group_lag Difference between high watermark and committed offset\n")
	sb.WriteString("# TYPE consumer_group_lag gauge\n")

	_, err := w.Write([]byte(sb.String()))
	if err != nil {
		log.Printf("[monitor] failed to write metrics response: %v", err)
	}
}

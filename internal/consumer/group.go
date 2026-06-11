package consumer

import (
	"fmt"
	"sync"
	"time"
)

// MemberState — состояние участника группы.
type MemberState uint8

const (
	MemberActive   MemberState = iota
	MemberInactive             // не присылал heartbeat дольше таймаута
)

// Member — один потребитель в группе.
type Member struct {
	ID          string
	Assignments []TopicPartition // партиции, назначенные этому участнику
	LastSeen    time.Time
	State       MemberState
}

// TopicPartition — пара топик+партиция.
type TopicPartition struct {
	Topic     string
	Partition int
}

// OffsetKey — ключ для хранения коммита смещения.
type OffsetKey struct {
	Topic     string
	Partition int
}

// Group — группа потребителей.
type Group struct {
	mu        sync.RWMutex
	name      string
	members   map[string]*Member  // memberID → Member
	offsets   map[OffsetKey]int64 // committed offsets
	heartbeat time.Duration       // таймаут heartbeat
}

// NewGroup создаёт новую группу потребителей.
func NewGroup(name string, heartbeatTimeout time.Duration) *Group {
	return &Group{
		name:      name,
		members:   make(map[string]*Member),
		offsets:   make(map[OffsetKey]int64),
		heartbeat: heartbeatTimeout,
	}
}

// Join добавляет нового участника в группу и запускает ребалансировку.
func (g *Group) Join(memberID string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, exists := g.members[memberID]; exists {
		return fmt.Errorf("member %q already in group %q", memberID, g.name)
	}

	g.members[memberID] = &Member{
		ID:       memberID,
		LastSeen: time.Now(),
		State:    MemberActive,
	}

	g.rebalance()
	return nil
}

// Leave удаляет участника из группы и запускает ребалансировку.
func (g *Group) Leave(memberID string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, exists := g.members[memberID]; !exists {
		return fmt.Errorf("member %q not found in group %q", memberID, g.name)
	}

	delete(g.members, memberID)
	g.rebalance()
	return nil
}

// Heartbeat обновляет время последней активности участника.
func (g *Group) Heartbeat(memberID string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	m, exists := g.members[memberID]
	if !exists {
		return fmt.Errorf("member %q not found", memberID)
	}

	m.LastSeen = time.Now()
	m.State = MemberActive
	return nil
}

// CommitOffset сохраняет смещение для топика и партиции от имени участника.
func (g *Group) CommitOffset(memberID, topic string, partition int, offset int64) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	m, exists := g.members[memberID]
	if !exists {
		return fmt.Errorf("member %q not found", memberID)
	}

	tp := TopicPartition{Topic: topic, Partition: partition}

	// Проверяем, что партиция назначена этому участнику
	assigned := false
	for _, a := range m.Assignments {
		if a == tp {
			assigned = true
			break
		}
	}
	if !assigned {
		return fmt.Errorf("partition %s:%d not assigned to member %q",
			topic, partition, memberID)
	}

	g.offsets[OffsetKey{Topic: topic, Partition: partition}] = offset
	return nil
}

// FetchOffset возвращает последнее закоммиченное смещение.
// Если смещение не найдено — возвращает 0 (начало партиции).
func (g *Group) FetchOffset(topic string, partition int) int64 {
	g.mu.RLock()
	defer g.mu.RUnlock()

	offset, ok := g.offsets[OffsetKey{Topic: topic, Partition: partition}]
	if !ok {
		return 0
	}
	return offset
}

// Assignments возвращает назначения партиций для участника.
func (g *Group) Assignments(memberID string) ([]TopicPartition, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	m, exists := g.members[memberID]
	if !exists {
		return nil, fmt.Errorf("member %q not found", memberID)
	}
	return m.Assignments, nil
}

// ExpireInactive находит и удаляет участников, не присылавших heartbeat.
func (g *Group) ExpireInactive() {
	g.mu.Lock()
	defer g.mu.Unlock()

	changed := false
	for id, m := range g.members {
		if time.Since(m.LastSeen) > g.heartbeat {
			delete(g.members, id)
			changed = true
		}
	}
	if changed {
		g.rebalance()
	}
}

// rebalance перераспределяет партиции между активными участниками.
// Используется стратегия RangeAssignor: партиции делятся равными диапазонами.
// Вызывается только под g.mu.Lock().
func (g *Group) rebalance() {
	// Собираем все партиции из текущих назначений
	partitionSet := make(map[TopicPartition]struct{})
	for _, m := range g.members {
		for _, tp := range m.Assignments {
			partitionSet[tp] = struct{}{}
		}
	}

	// Сбрасываем все назначения
	for _, m := range g.members {
		m.Assignments = nil
	}

	if len(g.members) == 0 || len(partitionSet) == 0 {
		return
	}

	// Строим упорядоченные списки
	partitions := make([]TopicPartition, 0, len(partitionSet))
	for tp := range partitionSet {
		partitions = append(partitions, tp)
	}

	members := make([]*Member, 0, len(g.members))
	for _, m := range g.members {
		members = append(members, m)
	}

	// Round-robin распределение
	for i, tp := range partitions {
		idx := i % len(members)
		members[idx].Assignments = append(members[idx].Assignments, tp)
	}
}

// GroupManager управляет всеми группами потребителей брокера.
type GroupManager struct {
	mu     sync.RWMutex
	groups map[string]*Group
}

// NewGroupManager создаёт менеджер групп.
func NewGroupManager() *GroupManager {
	return &GroupManager{
		groups: make(map[string]*Group),
	}
}

// GetOrCreate возвращает существующую группу или создаёт новую.
func (gm *GroupManager) GetOrCreate(name string) *Group {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	if g, exists := gm.groups[name]; exists {
		return g
	}

	g := NewGroup(name, 30*time.Second)
	gm.groups[name] = g
	return g
}

// StartExpiryLoop запускает фоновую горутину, которая периодически
// удаляет неактивных участников из всех групп.
func (gm *GroupManager) StartExpiryLoop(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			gm.mu.RLock()
			for _, g := range gm.groups {
				g.ExpireInactive()
			}
			gm.mu.RUnlock()
		}
	}()
}

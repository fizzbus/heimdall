package consumer

import (
	"testing"
	"time"
)

func newTestGroup() *Group {
	return NewGroup("test-group", 30*time.Second)
}

func TestGroup_JoinAndLeave(t *testing.T) {
	g := newTestGroup()

	if err := g.Join("member-1"); err != nil {
		t.Fatalf("Join: %v", err)
	}
	if err := g.Join("member-2"); err != nil {
		t.Fatalf("Join: %v", err)
	}

	if len(g.members) != 2 {
		t.Errorf("members count: got %d, want 2", len(g.members))
	}

	if err := g.Leave("member-1"); err != nil {
		t.Fatalf("Leave: %v", err)
	}
	if len(g.members) != 1 {
		t.Errorf("members count after leave: got %d, want 1", len(g.members))
	}
}

func TestGroup_Join_Duplicate(t *testing.T) {
	g := newTestGroup()
	g.Join("member-1")

	if err := g.Join("member-1"); err == nil {
		t.Error("expected error on duplicate join, got nil")
	}
}

func TestGroup_Leave_NotFound(t *testing.T) {
	g := newTestGroup()

	if err := g.Leave("nobody"); err == nil {
		t.Error("expected error for unknown member, got nil")
	}
}

func TestGroup_Heartbeat(t *testing.T) {
	g := newTestGroup()
	g.Join("member-1")

	before := g.members["member-1"].LastSeen

	time.Sleep(5 * time.Millisecond)

	if err := g.Heartbeat("member-1"); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	after := g.members["member-1"].LastSeen
	if !after.After(before) {
		t.Error("LastSeen should be updated after Heartbeat")
	}
}

func TestGroup_Heartbeat_UnknownMember(t *testing.T) {
	g := newTestGroup()

	if err := g.Heartbeat("ghost"); err == nil {
		t.Error("expected error for unknown member, got nil")
	}
}

func TestGroup_CommitAndFetchOffset(t *testing.T) {
	g := newTestGroup()
	g.Join("member-1")

	// Вручную назначаем партицию участнику
	g.members["member-1"].Assignments = []TopicPartition{
		{Topic: "events", Partition: 0},
	}

	if err := g.CommitOffset("member-1", "events", 0, 42); err != nil {
		t.Fatalf("CommitOffset: %v", err)
	}

	got := g.FetchOffset("events", 0)
	if got != 42 {
		t.Errorf("FetchOffset: got %d, want 42", got)
	}
}

func TestGroup_FetchOffset_Default(t *testing.T) {
	g := newTestGroup()

	// Смещение не закоммичено — должен вернуть 0
	if got := g.FetchOffset("unknown", 0); got != 0 {
		t.Errorf("FetchOffset default: got %d, want 0", got)
	}
}

func TestGroup_CommitOffset_NotAssigned(t *testing.T) {
	g := newTestGroup()
	g.Join("member-1")
	// Не назначаем партицию — коммит должен вернуть ошибку

	err := g.CommitOffset("member-1", "events", 0, 10)
	if err == nil {
		t.Error("expected error for unassigned partition, got nil")
	}
}

func TestGroup_Rebalance_SingleMember(t *testing.T) {
	g := newTestGroup()
	g.Join("member-1")

	// Добавляем партиции вручную и ребалансируем
	g.members["member-1"].Assignments = []TopicPartition{
		{Topic: "t", Partition: 0},
		{Topic: "t", Partition: 1},
		{Topic: "t", Partition: 2},
	}
	g.rebalance()

	assignments, _ := g.Assignments("member-1")
	if len(assignments) != 3 {
		t.Errorf("single member should get all 3 partitions, got %d", len(assignments))
	}
}

func TestGroup_Rebalance_TwoMembers(t *testing.T) {
	g := newTestGroup()
	g.Join("member-1")

	// Назначаем 4 партиции первому участнику
	g.members["member-1"].Assignments = []TopicPartition{
		{Topic: "t", Partition: 0},
		{Topic: "t", Partition: 1},
		{Topic: "t", Partition: 2},
		{Topic: "t", Partition: 3},
	}

	// Добавляем второго — ребалансировка делит партиции
	g.Join("member-2")

	a1, _ := g.Assignments("member-1")
	a2, _ := g.Assignments("member-2")

	total := len(a1) + len(a2)
	if total != 4 {
		t.Errorf("total assignments after rebalance: got %d, want 4", total)
	}
	if len(a1) == 0 || len(a2) == 0 {
		t.Error("each member should have at least 1 partition after rebalance")
	}
}

func TestGroup_ExpireInactive(t *testing.T) {
	// Группа с очень коротким таймаутом — 10 мс
	g := NewGroup("expire-test", 10*time.Millisecond)
	g.Join("member-1")
	g.Join("member-2")

	// Ждём истечения таймаута
	time.Sleep(20 * time.Millisecond)

	g.ExpireInactive()

	if len(g.members) != 0 {
		t.Errorf("all members should be expired, got %d", len(g.members))
	}
}

func TestGroup_ExpireInactive_KeepsActive(t *testing.T) {
	g := NewGroup("expire-test", 50*time.Millisecond)
	g.Join("member-1")
	g.Join("member-2")

	time.Sleep(30 * time.Millisecond)

	// member-2 присылает heartbeat
	g.Heartbeat("member-2")

	time.Sleep(30 * time.Millisecond)

	// Теперь member-1 должен устареть, member-2 — остаться
	g.ExpireInactive()

	if len(g.members) != 1 {
		t.Errorf("only active member should remain, got %d", len(g.members))
	}
	if _, exists := g.members["member-2"]; !exists {
		t.Error("member-2 should still be in group after heartbeat")
	}
}

func TestGroupManager_GetOrCreate(t *testing.T) {
	gm := NewGroupManager()

	g1 := gm.GetOrCreate("group-a")
	g2 := gm.GetOrCreate("group-a")
	g3 := gm.GetOrCreate("group-b")

	if g1 != g2 {
		t.Error("GetOrCreate should return same instance for same name")
	}
	if g1 == g3 {
		t.Error("GetOrCreate should return different instances for different names")
	}
}

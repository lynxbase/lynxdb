package splunkhec

import "testing"

func TestAckStore_RecordAndCheck(t *testing.T) {
	store := NewAckStore(10)
	id := store.Record("channel-a", true)

	acks := store.Check("channel-a", []int{id, id + 1})
	if !acks[id] {
		t.Fatalf("ack %d = false, want true", id)
	}
	if acks[id+1] {
		t.Fatalf("ack %d = true, want false", id+1)
	}
}

func TestAckStore_LRUEviction(t *testing.T) {
	store := NewAckStore(1)
	first := store.Record("channel-a", true)
	second := store.Record("channel-a", true)

	acks := store.Check("channel-a", []int{first, second})
	if acks[first] {
		t.Fatalf("ack %d = true after eviction, want false", first)
	}
	if !acks[second] {
		t.Fatalf("ack %d = false, want true", second)
	}
}

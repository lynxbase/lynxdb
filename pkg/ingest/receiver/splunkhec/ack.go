package splunkhec

import (
	"strconv"
	"sync"
)

type AckStore struct {
	mu     sync.Mutex
	nextID int
	max    int
	acks   map[string]bool
	order  []string
}

func NewAckStore(max int) *AckStore {
	if max <= 0 {
		max = 10000
	}
	return &AckStore{max: max, acks: make(map[string]bool)}
}

func (s *AckStore) Record(channel string, ok bool) int {
	if s == nil || channel == "" {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	s.nextID++
	id := s.nextID
	key := ackKey(channel, id)
	s.acks[key] = ok
	s.order = append(s.order, key)
	for len(s.order) > s.max {
		delete(s.acks, s.order[0])
		copy(s.order, s.order[1:])
		s.order = s.order[:len(s.order)-1]
	}
	return id
}

func (s *AckStore) Check(channel string, ids []int) map[int]bool {
	out := make(map[int]bool, len(ids))
	if s == nil || channel == "" {
		for _, id := range ids {
			out[id] = false
		}
		return out
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ids {
		out[id] = s.acks[ackKey(channel, id)]
	}
	return out
}

func ackKey(channel string, id int) string {
	return channel + ":" + strconv.Itoa(id)
}

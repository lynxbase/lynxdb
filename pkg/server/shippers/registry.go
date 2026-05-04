package shippers

import (
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

const DefaultRegistrySize = 128

type Registry struct {
	mu       sync.Mutex
	capacity int
	now      func() time.Time
	entries  map[string]*entry
	order    []string
}

type RequestInfo struct {
	UserAgent string
	RemoteIP  string
	Endpoint  string
}

type Observation struct {
	Tool         string    `json:"tool"`
	Version      string    `json:"version,omitempty"`
	Status       string    `json:"status"`
	LastSeen     time.Time `json:"last_seen_at"`
	EventsPerMin int64     `json:"events_per_min"`
	Endpoint     string    `json:"endpoint"`
	RemoteIP     string    `json:"remote_ip,omitempty"`
	UserAgent    string    `json:"user_agent,omitempty"`
}

type entry struct {
	fp        Fingerprint
	userAgent string
	remoteIP  string
	endpoint  string
	lastSeen  time.Time
	samples   []sample
}

type sample struct {
	at    time.Time
	count int
}

func NewRegistry(capacity int) *Registry {
	if capacity <= 0 {
		capacity = DefaultRegistrySize
	}
	return &Registry{
		capacity: capacity,
		now:      time.Now,
		entries:  make(map[string]*entry, capacity),
	}
}

func NewRequestInfo(r *http.Request) RequestInfo {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	return RequestInfo{
		UserAgent: r.UserAgent(),
		RemoteIP:  host,
		Endpoint:  r.URL.Path,
	}
}

func (r *Registry) Observe(info RequestInfo, eventCount int) {
	if r == nil || eventCount <= 0 {
		return
	}

	fp := DetectUserAgent(info.UserAgent)
	now := r.now()
	key := registryKey(fp, info)

	r.mu.Lock()
	defer r.mu.Unlock()

	e := r.entries[key]
	if e == nil {
		r.evictIfFull()
		e = &entry{fp: fp, userAgent: info.UserAgent, remoteIP: info.RemoteIP}
		r.entries[key] = e
		r.order = append(r.order, key)
	}

	e.endpoint = info.Endpoint
	e.lastSeen = now
	e.samples = append(e.samples, sample{at: now, count: eventCount})
	e.prune(now)
}

func (r *Registry) List() []Observation {
	if r == nil {
		return nil
	}

	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]Observation, 0, len(r.entries))
	for _, e := range r.entries {
		e.prune(now)
		out = append(out, Observation{
			Tool:         e.fp.Tool,
			Version:      e.fp.Version,
			Status:       statusFor(now.Sub(e.lastSeen)),
			LastSeen:     e.lastSeen,
			EventsPerMin: e.eventsPerMin(),
			Endpoint:     e.endpoint,
			RemoteIP:     e.remoteIP,
			UserAgent:    e.userAgent,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].LastSeen.After(out[j].LastSeen)
	})
	return out
}

func (r *Registry) evictIfFull() {
	if len(r.entries) < r.capacity {
		return
	}

	oldestIdx := 0
	oldestKey := r.order[0]
	oldest := r.entries[oldestKey].lastSeen
	for i, key := range r.order[1:] {
		if r.entries[key].lastSeen.Before(oldest) {
			oldest = r.entries[key].lastSeen
			oldestKey = key
			oldestIdx = i + 1
		}
	}
	delete(r.entries, oldestKey)
	r.order = append(r.order[:oldestIdx], r.order[oldestIdx+1:]...)
}

func (e *entry) prune(now time.Time) {
	cutoff := now.Add(-time.Minute)
	keep := 0
	for keep < len(e.samples) && e.samples[keep].at.Before(cutoff) {
		keep++
	}
	if keep > 0 {
		copy(e.samples, e.samples[keep:])
		e.samples = e.samples[:len(e.samples)-keep]
	}
}

func (e *entry) eventsPerMin() int64 {
	var total int64
	for _, s := range e.samples {
		total += int64(s.count)
	}
	return total
}

func registryKey(fp Fingerprint, info RequestInfo) string {
	ua := strings.TrimSpace(info.UserAgent)
	if ua == "" {
		ua = "unknown"
	}
	return fp.Tool + "\x00" + fp.Version + "\x00" + ua + "\x00" + info.RemoteIP
}

func statusFor(age time.Duration) string {
	switch {
	case age <= time.Minute:
		return "healthy"
	case age <= 5*time.Minute:
		return "idle"
	default:
		return "stale"
	}
}

// Package edge is the device side of a store-and-forward link: a bounded,
// prioritised, disk-backed queue and a sender that publishes over MQTT and
// only forgets a message once the broker has acknowledged it.
package edge

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"sort"
	"sync"
)

// Priority lanes. Lower number goes first.
const (
	SOS  = 0
	Norm = 1
	Bulk = 2
)

type Msg struct {
	ID      string `json:"id"`
	Prio    int    `json:"prio"`
	Topic   string `json:"topic"`
	Payload string `json:"payload"`
	At      int64  `json:"at"` // enqueue time, unix ms
}

type record struct {
	Op  string `json:"op"` // push or ack
	Msg *Msg   `json:"msg,omitempty"`
	ID  string `json:"id,omitempty"`
}

type Queue struct {
	mu    sync.Mutex
	cap   int
	items []Msg
	seq   map[string]int64
	n     int64
	f     *os.File
	w     *bufio.Writer

	Dropped []Msg // messages evicted because the queue was full
}

var ErrFull = errors.New("queue full of higher priority messages")

// Open loads the queue from path (created if missing), replaying pushes and acks,
// then rewrites the file compactly.
func Open(path string, capacity int) (*Queue, error) {
	q := &Queue{cap: capacity, seq: map[string]int64{}}
	if b, err := os.ReadFile(path); err == nil {
		sc := bufio.NewScanner(bytesReader(b))
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		live := map[string]Msg{}
		var order []string
		for sc.Scan() {
			var r record
			if json.Unmarshal(sc.Bytes(), &r) != nil {
				continue // a torn final line after a crash is ignored
			}
			switch r.Op {
			case "push":
				if _, ok := live[r.Msg.ID]; !ok {
					order = append(order, r.Msg.ID)
				}
				live[r.Msg.ID] = *r.Msg
			case "ack":
				delete(live, r.ID)
			}
		}
		for _, id := range order {
			if m, ok := live[id]; ok {
				q.n++
				q.seq[id] = q.n
				q.items = append(q.items, m)
			}
		}
	}
	f, err := os.OpenFile(path+".tmp", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	w := bufio.NewWriter(f)
	for i := range q.items {
		b, _ := json.Marshal(record{Op: "push", Msg: &q.items[i]})
		w.Write(append(b, '\n'))
	}
	if err := w.Flush(); err != nil {
		return nil, err
	}
	f.Close()
	if err := os.Rename(path+".tmp", path); err != nil {
		return nil, err
	}
	q.f, err = os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	q.w = bufio.NewWriter(q.f)
	return q, err
}

func (q *Queue) log(r record) {
	b, _ := json.Marshal(r)
	q.w.Write(append(b, '\n'))
	q.w.Flush()
	q.f.Sync() // a message is only "accepted" once it survives power loss
}

func (q *Queue) Len() int { q.mu.Lock(); defer q.mu.Unlock(); return len(q.items) }

// Push adds a message. When full it evicts the oldest message of the lowest
// priority, but never one that outranks the newcomer; SOS is never evicted by
// anything but another SOS.
func (q *Queue) Push(m Msg) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, x := range q.items {
		if x.ID == m.ID {
			return nil // already queued: a retry of the same message
		}
	}
	if len(q.items) >= q.cap {
		victim := -1
		for i, x := range q.items {
			if x.Prio < m.Prio {
				continue // outranks the newcomer
			}
			if victim < 0 || x.Prio > q.items[victim].Prio || (x.Prio == q.items[victim].Prio && q.seq[x.ID] < q.seq[q.items[victim].ID]) {
				victim = i
			}
		}
		if victim < 0 {
			return ErrFull
		}
		q.Dropped = append(q.Dropped, q.items[victim])
		q.log(record{Op: "ack", ID: q.items[victim].ID})
		delete(q.seq, q.items[victim].ID)
		q.items = append(q.items[:victim], q.items[victim+1:]...)
	}
	q.n++
	q.seq[m.ID] = q.n
	q.items = append(q.items, m)
	q.log(record{Op: "push", Msg: &m})
	return nil
}

// Peek returns the next message to send without removing it.
func (q *Queue) Peek() (Msg, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return Msg{}, false
	}
	best := q.items[0]
	for _, x := range q.items[1:] {
		if x.Prio < best.Prio || (x.Prio == best.Prio && q.seq[x.ID] < q.seq[best.ID]) {
			best = x
		}
	}
	return best, true
}

// Ack removes a delivered message for good.
func (q *Queue) Ack(id string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, x := range q.items {
		if x.ID == id {
			q.items = append(q.items[:i], q.items[i+1:]...)
			delete(q.seq, id)
			q.log(record{Op: "ack", ID: id})
			return
		}
	}
}

func (q *Queue) Snapshot() []Msg {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := append([]Msg(nil), q.items...)
	sort.Slice(out, func(i, j int) bool { return q.seq[out[i].ID] < q.seq[out[j].ID] })
	return out
}

func (q *Queue) Close() error { q.mu.Lock(); defer q.mu.Unlock(); return q.f.Close() }

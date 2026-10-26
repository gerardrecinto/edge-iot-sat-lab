package edge

import (
	"encoding/json"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Sender drains a Queue over MQTT with QoS 1. A message leaves the queue only
// after the broker's PUBACK, so a dropped link means redelivery, never loss.
// That makes delivery at-least-once: the receiver must tolerate duplicates.
type Sender struct {
	Q          *Queue
	client     mqtt.Client
	stop       chan struct{}
	wg         sync.WaitGroup
	AckTimeout time.Duration
	closeOnce  sync.Once

	mu        sync.Mutex
	Attempts  map[string]int
	AckedAt   map[string]time.Time
	Reconnect int
}

func NewSender(q *Queue, broker, clientID string) (*Sender, error) {
	s := &Sender{Q: q, stop: make(chan struct{}), AckTimeout: 400 * time.Millisecond, Attempts: map[string]int{}, AckedAt: map[string]time.Time{}}
	opts := mqtt.NewClientOptions().AddBroker(broker).SetClientID(clientID).
		SetAutoReconnect(true).SetConnectRetry(true).SetConnectRetryInterval(150 * time.Millisecond).
		SetMaxReconnectInterval(300 * time.Millisecond).SetKeepAlive(1 * time.Second).SetPingTimeout(500 * time.Millisecond).
		SetCleanSession(true).SetConnectTimeout(500 * time.Millisecond).SetOrderMatters(false)
	opts.SetOnConnectHandler(func(mqtt.Client) { s.mu.Lock(); s.Reconnect++; s.mu.Unlock() })
	s.client = mqtt.NewClient(opts)
	s.client.Connect() // returns immediately with ConnectRetry; the loop below waits for the link
	s.wg.Add(1)
	go s.loop()
	return s, nil
}

func (s *Sender) Reconnects() int { s.mu.Lock(); defer s.mu.Unlock(); return s.Reconnect }

func (s *Sender) loop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.stop:
			return
		default:
		}
		m, ok := s.Q.Peek()
		if !ok || !s.client.IsConnectionOpen() {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		body, _ := json.Marshal(m)
		s.mu.Lock()
		s.Attempts[m.ID]++
		s.mu.Unlock()
		t := s.client.Publish(m.Topic, 1, false, body)
		if t.WaitTimeout(s.AckTimeout) && t.Error() == nil {
			s.mu.Lock()
			s.AckedAt[m.ID] = time.Now()
			s.mu.Unlock()
			s.Q.Ack(m.ID)
			continue
		}
		time.Sleep(50 * time.Millisecond) // link is flapping: back off and retry the same message
	}
}

func (s *Sender) Stats() (attempts map[string]int, acked map[string]time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	attempts, acked = map[string]int{}, map[string]time.Time{}
	for k, v := range s.Attempts {
		attempts[k] = v
	}
	for k, v := range s.AckedAt {
		acked[k] = v
	}
	return
}

// Close stops the loop and disconnects. Unsent messages stay in the on-disk queue.
func (s *Sender) Close() {
	s.closeOnce.Do(func() {
		close(s.stop)
		s.wg.Wait()
		s.client.Disconnect(100)
	})
}

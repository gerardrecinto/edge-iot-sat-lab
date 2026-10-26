package edge

import (
	"encoding/json"
	"net"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	mochi "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
)

// Ground is an in-process MQTT broker plus a subscriber that records every
// delivery. It stands in for the ground station behind the satellite link.
type Ground struct {
	Server *mochi.Server
	Addr   string
	sub    mqtt.Client

	mu         sync.Mutex
	Deliveries []Delivery
	seen       map[string]bool
	Duplicates int
}

type Delivery struct {
	Msg Msg
	At  time.Time
	Dup bool
}

func StartGround() (*Ground, error) {
	srv := mochi.New(&mochi.Options{InlineClient: true})
	_ = srv.AddHook(new(auth.AllowHook), nil)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	addr := ln.Addr().String()
	ln.Close()
	tcp := listeners.NewTCP(listeners.Config{ID: "t1", Address: addr})
	if err := srv.AddListener(tcp); err != nil {
		return nil, err
	}
	go srv.Serve()
	g := &Ground{Server: srv, Addr: addr, seen: map[string]bool{}}
	opts := mqtt.NewClientOptions().AddBroker("tcp://" + addr).SetClientID("ground-sub")
	g.sub = mqtt.NewClient(opts)
	for i := 0; i < 50; i++ { // wait for the listener
		if t := g.sub.Connect(); t.Wait() && t.Error() == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	t := g.sub.Subscribe("sat/#", 1, func(_ mqtt.Client, m mqtt.Message) {
		var msg Msg
		if json.Unmarshal(m.Payload(), &msg) != nil {
			return
		}
		g.mu.Lock()
		dup := g.seen[msg.ID]
		g.seen[msg.ID] = true
		if dup {
			g.Duplicates++
		}
		g.Deliveries = append(g.Deliveries, Delivery{msg, time.Now(), dup})
		g.mu.Unlock()
	})
	t.Wait()
	return g, t.Error()
}

func (g *Ground) Received() []Delivery {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]Delivery(nil), g.Deliveries...)
}

func (g *Ground) UniqueCount() int { g.mu.Lock(); defer g.mu.Unlock(); return len(g.seen) }

func (g *Ground) Close() { g.sub.Disconnect(50); _ = g.Server.Close() }

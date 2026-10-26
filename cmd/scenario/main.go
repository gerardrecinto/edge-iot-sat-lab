// scenario runs the device, the flaky link and the ground station in real
// time, then writes what happened to docs/data.json for the Pages viewer.
// Telemetry messages carry NMEA GPS sentences; some are corrupted on purpose
// and the ground station's checksum check rejects them.
package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/gerardrecinto/edge-iot-sat-lab/edge"
	"github.com/gerardrecinto/edge-iot-sat-lab/linksim"
	"github.com/gerardrecinto/edge-iot-sat-lab/nmea"
)

type MsgOut struct {
	ID        string `json:"id"`
	Prio      int    `json:"prio"`
	EnqueueMs int64  `json:"enqueueMs"`
	DeliverMs int64  `json:"deliverMs"` // first delivery, -1 if never
	DelayMs   int64  `json:"delayMs"`
	Attempts  int    `json:"attempts"`
	Dup       int    `json:"duplicates"`
	Kind      string `json:"kind"`
}

func ddmm(deg float64, lat bool) (string, string) {
	h := "N"
	if lat {
		if deg < 0 {
			h = "S"
		}
	} else {
		h = "E"
		if deg < 0 {
			h = "W"
		}
	}
	a := math.Abs(deg)
	d := math.Floor(a)
	m := (a - d) * 60
	if lat {
		return fmt.Sprintf("%02d%07.4f", int(d), m), h
	}
	return fmt.Sprintf("%03d%07.4f", int(d), m), h
}

func gga(i int, lat, lon float64) string {
	la, lh := ddmm(lat, true)
	lo, oh := ddmm(lon, false)
	body := fmt.Sprintf("GPGGA,%06d,%s,%s,%s,%s,1,08,0.9,%.1f,M,-34.0,M,,", 120000+i, la, lh, lo, oh, 20.0+float64(i%5))
	return nmea.Sentence(body)
}

func main() {
	dir, _ := os.MkdirTemp("", "edge-scenario")
	g, err := edge.StartGround()
	if err != nil {
		panic(err)
	}
	defer g.Close()
	link, _ := linksim.New(g.Addr, 40*time.Millisecond)
	defer link.Close()
	q, _ := edge.Open(filepath.Join(dir, "q"), 200)
	s, _ := edge.NewSender(q, "tcp://"+link.Addr(), "device-1")
	defer s.Close()
	for i := 0; i < 100 && s.Reconnects() < 1; i++ {
		time.Sleep(50 * time.Millisecond)
	}

	t0 := time.Now()
	ms := func() int64 { return time.Since(t0).Milliseconds() }
	type win struct{ From, To int64 }
	var outages []win
	downAt := int64(-1)

	enq := map[string]MsgOut{}
	lat, lon := 32.7157, -117.1611
	const total = 70
	for i := 0; i < total; i++ {
		now := ms()
		switch {
		case i == 20:
			link.Down()
			downAt = now
		case i == 45:
			link.Up()
			outages = append(outages, win{downAt, now})
		}
		sent := gga(i, lat, lon)
		if i%12 == 7 { // radio bit error: flip one character in the body
			b := []byte(sent)
			b[10] ^= 0x01
			sent = string(b)
		}
		id, prio, kind := fmt.Sprintf("gps-%02d", i), edge.Norm, "gps"
		if i%10 == 9 {
			id, prio, kind = fmt.Sprintf("bulk-%02d", i), edge.Bulk, "bulk log chunk"
			sent = strings.Repeat("log ", 8)
		}
		if i == 33 {
			id, prio, kind = "sos-1", edge.SOS, "SOS"
			sent = "SOS position " + gga(i, lat, lon)
		}
		_ = q.Push(edge.Msg{ID: id, Prio: prio, Topic: "sat/" + kind[:3], Payload: sent, At: time.Now().UnixMilli()})
		enq[id] = MsgOut{ID: id, Prio: prio, EnqueueMs: now, Kind: kind}
		lat += 0.0008
		lon += 0.0011
		time.Sleep(100 * time.Millisecond)
	}
	for i := 0; i < 300 && q.Len() > 0; i++ {
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond)

	attempts, _ := s.Stats()
	first := map[string]int64{}
	dups := map[string]int{}
	var fixes []map[string]any
	rejected := 0
	for _, d := range g.Received() {
		t := d.At.Sub(t0).Milliseconds()
		if _, ok := first[d.Msg.ID]; !ok {
			first[d.Msg.ID] = t
			if strings.HasPrefix(d.Msg.ID, "gps-") || d.Msg.ID == "sos-1" {
				p := d.Msg.Payload
				if i := strings.Index(p, "$"); i >= 0 {
					p = p[i:]
				}
				if f, err := nmea.Parse(p); err != nil {
					rejected++
				} else {
					fixes = append(fixes, map[string]any{"id": d.Msg.ID, "t": t, "lat": f.Lat, "lon": f.Lon})
				}
			}
		} else {
			dups[d.Msg.ID]++
		}
	}
	var out []MsgOut
	var delays []float64
	for id, m := range enq {
		m.Attempts = attempts[id]
		m.Dup = dups[id]
		if t, ok := first[id]; ok {
			m.DeliverMs = t
			m.DelayMs = t - m.EnqueueMs
			if m.Prio == edge.Norm {
				delays = append(delays, float64(m.DelayMs))
			}
		} else {
			m.DeliverMs = -1
		}
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].EnqueueMs < out[j].EnqueueMs })
	sort.Float64s(delays)
	pct := func(p float64) float64 { return delays[int(p*float64(len(delays)-1))] }
	totalDup := 0
	for _, v := range dups {
		totalDup += v
	}
	data := map[string]any{
		"messages": out, "outages": outages, "fixes": fixes, "rejectedSentences": rejected,
		"stats": map[string]any{
			"sent": len(enq), "delivered": g.UniqueCount(), "duplicateDeliveries": totalDup, "reconnects": s.Reconnects(),
			"normalDelayP50Ms": pct(0.5), "normalDelayP95Ms": pct(0.95), "oneWayLatencyMs": 40, "queueLeft": q.Len(),
		},
	}
	b, _ := json.MarshalIndent(data, "", " ")
	_ = os.WriteFile("docs/data.json", b, 0o644)
	fmt.Printf("sent=%d delivered=%d dup=%d reconnects=%d rejectedSentences=%d p50=%.0fms p95=%.0fms outages=%v\n",
		len(enq), g.UniqueCount(), totalDup, s.Reconnects(), rejected, pct(0.5), pct(0.95), outages)
	if g.UniqueCount() != len(enq) {
		os.Exit(1)
	}
}

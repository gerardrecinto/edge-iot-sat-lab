package edge

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/gerardrecinto/edge-iot-sat-lab/linksim"
)

func msg(id string, prio int) Msg {
	return Msg{ID: id, Prio: prio, Topic: "sat/" + id, Payload: "x", At: time.Now().UnixMilli()}
}

func TestPriorityThenFIFO(t *testing.T) {
	q, _ := Open(filepath.Join(t.TempDir(), "q"), 10)
	for _, m := range []Msg{msg("n1", Norm), msg("b1", Bulk), msg("n2", Norm), msg("s1", SOS), msg("s2", SOS)} {
		q.Push(m)
	}
	var got []string
	for {
		m, ok := q.Peek()
		if !ok {
			break
		}
		got = append(got, m.ID)
		q.Ack(m.ID)
	}
	want := []string{"s1", "s2", "n1", "n2", "b1"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestFullQueueEvictsOldestLowestPriorityNeverSOS(t *testing.T) {
	q, _ := Open(filepath.Join(t.TempDir(), "q"), 3)
	q.Push(msg("s1", SOS))
	q.Push(msg("b1", Bulk))
	q.Push(msg("b2", Bulk))
	if err := q.Push(msg("n1", Norm)); err != nil { // evicts b1, the oldest bulk
		t.Fatal(err)
	}
	if len(q.Dropped) != 1 || q.Dropped[0].ID != "b1" {
		t.Fatalf("dropped %+v", q.Dropped)
	}
	// A bulk message cannot displace anything that outranks it.
	q.Push(msg("s2", SOS))
	q.Push(msg("s3", SOS))
	if err := q.Push(msg("b9", Bulk)); err != ErrFull {
		t.Fatalf("bulk into a queue of higher priorities: %v", err)
	}
	// SOS pushes still get in by evicting lower priorities, and an all-SOS full queue refuses nothing but evicts the oldest SOS.
	ids := map[string]bool{}
	for _, m := range q.Snapshot() {
		ids[m.ID] = true
	}
	if !ids["s1"] || !ids["s2"] || !ids["s3"] {
		t.Fatalf("an SOS message was lost: %v", ids)
	}
}

func TestDuplicatePushIsIgnored(t *testing.T) {
	q, _ := Open(filepath.Join(t.TempDir(), "q"), 5)
	q.Push(msg("a", Norm))
	q.Push(msg("a", Norm))
	if q.Len() != 1 {
		t.Fatalf("len %d", q.Len())
	}
}

func TestQueueSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "q")
	q, _ := Open(path, 10)
	q.Push(msg("a", Norm))
	q.Push(msg("b", SOS))
	q.Push(msg("c", Norm))
	q.Ack("a")
	q.Close() // "power loss": nothing but the file survives
	q2, err := Open(path, 10)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, m := range q2.Snapshot() {
		ids = append(ids, m.ID)
	}
	if fmt.Sprint(ids) != "[b c]" {
		t.Fatalf("after restart: %v", ids)
	}
	if m, _ := q2.Peek(); m.ID != "b" {
		t.Fatalf("priority lost across restart: %s", m.ID)
	}
}

// ---- real broker, real MQTT client, flaky link ----

func waitFor(t *testing.T, d time.Duration, what string, ok func() bool) {
	t.Helper()
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		if ok() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func setup(t *testing.T, dir string) (*Ground, *linksim.Link, *Queue, *Sender) {
	t.Helper()
	g, err := StartGround()
	if err != nil {
		t.Fatal(err)
	}
	link, err := linksim.New(g.Addr, 40*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	q, err := Open(filepath.Join(dir, "q"), 200)
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewSender(q, "tcp://"+link.Addr(), "device-1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close(); link.Close(); g.Close() })
	return g, link, q, s
}

func TestNothingIsLostAcrossAnOutageAndSOSJumpsTheQueue(t *testing.T) {
	g, link, q, s := setup(t, t.TempDir())
	waitFor(t, 5*time.Second, "first connection", func() bool { return s.Reconnects() >= 1 })
	for i := 0; i < 5; i++ {
		q.Push(msg(fmt.Sprintf("pre-%d", i), Norm))
	}
	waitFor(t, 5*time.Second, "pre-outage delivery", func() bool { return g.UniqueCount() == 5 })

	link.Down()
	time.Sleep(200 * time.Millisecond)
	for i := 0; i < 30; i++ {
		q.Push(msg(fmt.Sprintf("out-%d", i), Norm))
	}
	q.Push(msg("sos-1", SOS)) // enqueued last, must be delivered first
	q.Push(msg("bulk-1", Bulk))
	time.Sleep(1200 * time.Millisecond)
	if g.UniqueCount() != 5 {
		t.Fatalf("messages crossed a down link: %d", g.UniqueCount())
	}
	link.Up()
	waitFor(t, 15*time.Second, "all 37 messages", func() bool { return g.UniqueCount() == 37 })
	waitFor(t, 5*time.Second, "empty queue", func() bool { return q.Len() == 0 })

	order := map[string]int{}
	for i, d := range g.Received() {
		if _, ok := order[d.Msg.ID]; !ok {
			order[d.Msg.ID] = i
		}
	}
	for i := 0; i < 30; i++ {
		if order["sos-1"] > order[fmt.Sprintf("out-%d", i)] {
			t.Fatalf("SOS arrived after out-%d", i)
		}
		if order["bulk-1"] < order[fmt.Sprintf("out-%d", i)] {
			t.Fatalf("bulk overtook out-%d", i)
		}
	}
	if s.Reconnects() < 2 {
		t.Fatalf("sender never reconnected (%d connects)", s.Reconnects())
	}
	t.Logf("unique=%d duplicates=%d reconnects=%d", g.UniqueCount(), g.Duplicates, s.Reconnects())
}

func TestQueuedMessagesSurviveASenderCrashDuringAnOutage(t *testing.T) {
	dir := t.TempDir()
	g, link, q, s := setup(t, dir)
	waitFor(t, 5*time.Second, "connect", func() bool { return s.Reconnects() >= 1 })
	link.Down()
	time.Sleep(200 * time.Millisecond)
	for i := 0; i < 10; i++ {
		q.Push(msg(fmt.Sprintf("m-%d", i), Norm))
	}
	s.Close()
	q.Close() // the device loses power with 10 messages unsent
	q2, err := Open(filepath.Join(dir, "q"), 200)
	if err != nil {
		t.Fatal(err)
	}
	if q2.Len() != 10 {
		t.Fatalf("recovered %d messages, want 10", q2.Len())
	}
	link.Up()
	s2, err := NewSender(q2, "tcp://"+link.Addr(), "device-1")
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	waitFor(t, 15*time.Second, "recovered messages delivered", func() bool { return g.UniqueCount() == 10 })
}

// Package linksim is a TCP proxy that behaves like a flaky radio link: added
// latency in each direction, and outages during which every connection is cut
// and new ones are refused. It sits between an MQTT client and a broker.
package linksim

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type Link struct {
	ln      net.Listener
	target  string
	Latency time.Duration // one way
	down    atomic.Bool
	mu      sync.Mutex
	conns   map[net.Conn]struct{}
}

func New(target string, oneWay time.Duration) (*Link, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	l := &Link{ln: ln, target: target, Latency: oneWay, conns: map[net.Conn]struct{}{}}
	go l.accept()
	return l, nil
}

func (l *Link) Addr() string { return l.ln.Addr().String() }
func (l *Link) Close()       { l.ln.Close(); l.Down() }
func (l *Link) IsUp() bool   { return !l.down.Load() }

// Down cuts every open connection and refuses new ones until Up.
func (l *Link) Down() {
	l.down.Store(true)
	l.mu.Lock()
	for c := range l.conns {
		c.Close()
	}
	l.conns = map[net.Conn]struct{}{}
	l.mu.Unlock()
}

func (l *Link) Up() { l.down.Store(false) }

func (l *Link) track(c net.Conn) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.down.Load() {
		return false
	}
	l.conns[c] = struct{}{}
	return true
}

func (l *Link) accept() {
	for {
		c, err := l.ln.Accept()
		if err != nil {
			return
		}
		if l.down.Load() {
			c.Close()
			continue
		}
		up, err := net.Dial("tcp", l.target)
		if err != nil || !l.track(c) || !l.track(up) {
			c.Close()
			if up != nil {
				up.Close()
			}
			continue
		}
		go l.pipe(c, up)
		go l.pipe(up, c)
	}
}

// pipe copies with a delay per chunk, preserving order.
func (l *Link) pipe(dst, src net.Conn) {
	defer dst.Close()
	defer src.Close()
	buf := make([]byte, 4096)
	type chunk struct {
		b  []byte
		at time.Time
	}
	ch := make(chan chunk, 1024)
	go func() {
		defer close(ch)
		for {
			n, err := src.Read(buf)
			if n > 0 {
				ch <- chunk{append([]byte(nil), buf[:n]...), time.Now().Add(l.Latency)}
			}
			if err != nil {
				return
			}
		}
	}()
	for c := range ch {
		if d := time.Until(c.at); d > 0 {
			time.Sleep(d)
		}
		if _, err := dst.Write(c.b); err != nil {
			return
		}
	}
	_ = io.EOF
}

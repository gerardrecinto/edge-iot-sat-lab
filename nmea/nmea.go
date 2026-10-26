// Package nmea parses the two NMEA 0183 sentences a GPS receiver sends most
// often, GGA (position fix) and RMC (recommended minimum), and rejects
// sentences whose checksum does not match.
package nmea

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type Fix struct {
	Type    string  `json:"type"` // GGA or RMC
	Time    string  `json:"time"` // hhmmss.ss as sent
	Lat     float64 `json:"lat"`
	Lon     float64 `json:"lon"`
	Valid   bool    `json:"valid"`
	Quality int     `json:"quality,omitempty"` // GGA fix quality, 0 = no fix
	Sats    int     `json:"sats,omitempty"`
	AltM    float64 `json:"altM,omitempty"`
	Knots   float64 `json:"knots,omitempty"` // RMC
}

var (
	ErrFormat   = errors.New("nmea: not a sentence")
	ErrChecksum = errors.New("nmea: checksum mismatch")
	ErrType     = errors.New("nmea: unsupported sentence")
)

// Checksum is the XOR of every byte between '$' and '*'.
func Checksum(body string) byte {
	var c byte
	for i := 0; i < len(body); i++ {
		c ^= body[i]
	}
	return c
}

// Sentence wraps a body such as "GPGGA,..." with '$', the checksum and the line ending.
func Sentence(body string) string { return fmt.Sprintf("$%s*%02X\r\n", body, Checksum(body)) }

func Parse(line string) (Fix, error) {
	line = strings.TrimSpace(line)
	if len(line) < 9 || line[0] != '$' {
		return Fix{}, ErrFormat
	}
	star := strings.LastIndexByte(line, '*')
	if star < 0 || star+3 != len(line) {
		return Fix{}, ErrFormat
	}
	body := line[1:star]
	want, err := strconv.ParseUint(line[star+1:], 16, 8)
	if err != nil {
		return Fix{}, ErrFormat
	}
	if Checksum(body) != byte(want) {
		return Fix{}, ErrChecksum
	}
	f := strings.Split(body, ",")
	switch {
	case strings.HasSuffix(f[0], "GGA"):
		return parseGGA(f)
	case strings.HasSuffix(f[0], "RMC"):
		return parseRMC(f)
	}
	return Fix{}, ErrType
}

// coord converts ddmm.mmmm (or dddmm.mmmm) plus a hemisphere letter to signed degrees.
func coord(v, hemi string, degDigits int) (float64, error) {
	if len(v) < degDigits+2 {
		return 0, ErrFormat
	}
	deg, err := strconv.ParseFloat(v[:degDigits], 64)
	if err != nil {
		return 0, ErrFormat
	}
	min, err := strconv.ParseFloat(v[degDigits:], 64)
	if err != nil {
		return 0, ErrFormat
	}
	d := deg + min/60
	if hemi == "S" || hemi == "W" {
		d = -d
	}
	return d, nil
}

func parseGGA(f []string) (Fix, error) {
	if len(f) < 10 {
		return Fix{}, ErrFormat
	}
	q, _ := strconv.Atoi(f[6])
	fix := Fix{Type: "GGA", Time: f[1], Quality: q, Valid: q > 0}
	fix.Sats, _ = strconv.Atoi(f[7])
	fix.AltM, _ = strconv.ParseFloat(f[9], 64)
	if f[2] == "" || f[4] == "" { // receivers send empty fields when they have no fix
		return fix, nil
	}
	var err error
	if fix.Lat, err = coord(f[2], f[3], 2); err != nil {
		return Fix{}, err
	}
	if fix.Lon, err = coord(f[4], f[5], 3); err != nil {
		return Fix{}, err
	}
	return fix, nil
}

func parseRMC(f []string) (Fix, error) {
	if len(f) < 9 {
		return Fix{}, ErrFormat
	}
	fix := Fix{Type: "RMC", Time: f[1], Valid: f[2] == "A"}
	fix.Knots, _ = strconv.ParseFloat(f[7], 64)
	if f[3] == "" || f[5] == "" {
		return fix, nil
	}
	var err error
	if fix.Lat, err = coord(f[3], f[4], 2); err != nil {
		return Fix{}, err
	}
	if fix.Lon, err = coord(f[5], f[6], 3); err != nil {
		return Fix{}, err
	}
	return fix, nil
}

package nmea

import (
	"math"
	"testing"
)

func near(a, b float64) bool { return math.Abs(a-b) < 1e-4 }

func TestParseGGA(t *testing.T) {
	// Example sentence from the NMEA 0183 literature (Greenwich-area fix).
	f, err := Parse("$GPGGA,123519,4807.038,N,01131.000,E,1,08,0.9,545.4,M,46.9,M,,*47")
	if err != nil {
		t.Fatal(err)
	}
	if !f.Valid || f.Quality != 1 || f.Sats != 8 || !near(f.Lat, 48.1173) || !near(f.Lon, 11.5167) || f.AltM != 545.4 {
		t.Fatalf("%+v", f)
	}
}

func TestParseRMCSouthWest(t *testing.T) {
	body := "GPRMC,225446,A,4916.45,S,12311.12,W,000.5,054.7,191194,020.3,E"
	f, err := Parse(Sentence(body))
	if err != nil {
		t.Fatal(err)
	}
	if !f.Valid || !near(f.Lat, -49.2742) || !near(f.Lon, -123.1853) || f.Knots != 0.5 {
		t.Fatalf("%+v", f)
	}
}

func TestChecksumMismatchRejected(t *testing.T) {
	if _, err := Parse("$GPGGA,123519,4807.038,N,01131.000,E,1,08,0.9,545.4,M,46.9,M,,*00"); err != ErrChecksum {
		t.Fatalf("got %v", err)
	}
	// one flipped digit in the body
	if _, err := Parse("$GPGGA,123519,4807.039,N,01131.000,E,1,08,0.9,545.4,M,46.9,M,,*47"); err != ErrChecksum {
		t.Fatalf("got %v", err)
	}
}

func TestNoFixSentenceIsReportedInvalidNotAnError(t *testing.T) {
	f, err := Parse(Sentence("GPGGA,123519,,,,,0,00,,,M,,M,,"))
	if err != nil || f.Valid || f.Quality != 0 {
		t.Fatalf("%+v %v", f, err)
	}
}

func TestMalformedInputNeverPanics(t *testing.T) {
	for _, s := range []string{"", "$", "$*", "GPGGA", "$GPGGA*", "$GPGGA,1*ZZ", "$GPGGA,1,2*00", "$GPXXX,1*" + "00", "$GPGGA,123519,48,N,01131.000,E,1,08,0.9,545.4,M,46.9,M,,*00"} {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panic on %q: %v", s, r)
				}
			}()
			_, _ = Parse(s)
		}()
	}
}

func FuzzParse(f *testing.F) {
	f.Add("$GPGGA,123519,4807.038,N,01131.000,E,1,08,0.9,545.4,M,46.9,M,,*47")
	f.Add(Sentence("GPRMC,225446,A,4916.45,N,12311.12,W,000.5,054.7,191194,020.3,E"))
	f.Fuzz(func(t *testing.T, s string) { _, _ = Parse(s) })
}

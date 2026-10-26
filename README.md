# edge-iot-sat-lab

Store-and-forward messaging for a device behind an unreliable, high-latency link, in Go: a bounded, prioritised, disk-backed queue; an MQTT sender that only forgets a message after the broker acknowledges it; an NMEA 0183 GPS parser; and a ground station that deduplicates.

**Live demo:** https://gerardrecinto.github.io/edge-iot-sat-lab/ shows a recorded run: 70 messages across a 2.6 second outage, the SOS message jumping the queue, and a GPS track rebuilt from NMEA sentences, including the corrupted ones that were rejected.

Personal learning project. There is no real radio, modem or satellite: the link is a TCP proxy that adds latency and cuts connections. The MQTT broker ([mochi-mqtt](https://github.com/mochi-mqtt/server)) and client ([Eclipse Paho](https://pkg.go.dev/github.com/eclipse/paho.mqtt.golang)) are real and talk over loopback TCP.

## Pieces

- `edge/queue.go`: priority lanes (SOS, normal, bulk), FIFO inside a lane, a size cap that evicts the oldest message of the lowest priority and never evicts something that outranks the newcomer, duplicate ids ignored, and an append-only log with fsync so queued messages survive power loss
- `edge/sender.go`: publishes at QoS 1 and removes a message from the queue only after the PUBACK; auto-reconnect with short backoff; retries the same message when the link flaps
- `edge/ground.go`: in-process broker plus a subscriber that records every delivery and counts duplicates
- `linksim/`: TCP proxy with one-way latency; `Down()` cuts every connection and refuses new ones
- `nmea/`: GGA and RMC parsing with checksum verification, empty-field handling for "no fix", and a fuzz target
- `cmd/scenario/`: runs everything in real time and writes `docs/data.json` for the page

## Tests

```
go test ./... -race -count=1
```

- priority then FIFO ordering; eviction rules; duplicate pushes; the queue restored from disk after a simulated power loss with acks applied and priority preserved
- **Outage test** (real broker, real client, real proxy): 5 messages delivered, link cut, 30 normal, 1 SOS and 1 bulk message queued while down, nothing crosses the down link, link restored, all 37 delivered, the SOS message arrives before every normal message queued before it, and bulk arrives after them
- **Crash test**: 10 messages queued during an outage, the sender and queue closed as if the device lost power, the queue reopened from disk (10 recovered), a new sender delivers all 10 once the link returns
- NMEA: the standard GGA example, a southern/western RMC, checksum mismatch, no-fix sentences, malformed input that must not panic, and a fuzz target

## What I saw

- QoS 1 is at-least-once. In one test run 2 of 37 messages arrived twice after the reconnect; in the recorded scenario run there were none. The ground station deduplicates by message id, and the sender cannot prevent duplicates, only the receiver can.
- Paho's `Connect` with `ConnectRetry` returns immediately, so the sender loop has to check `IsConnectionOpen` before publishing rather than assume the link is up.
- The mochi-mqtt server warns that a 1 second keep-alive is below its recommended minimum. I used 1 second so a dead link is noticed within the test's time scale; a real deployment would use something longer.
- A double `Close` on the sender panicked (`close of closed channel`) because the test cleanup and the test body both closed it. Close is now idempotent.

## Not covered

No real RF, Doppler, bandwidth caps or loss bursts; no TLS, MQTT 5 or broker clustering; no Bluetooth, Wi-Fi or LTE/5G handling here (my cellular work is in `nr-ca-analyzer` and `6g-ran-edge-lab`); no full-disk behaviour; device and ground station share one process.

## References

- OASIS, [MQTT 3.1.1](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/mqtt-v3.1.1.html) and [MQTT 5.0](https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html)
- [Eclipse Paho Go client](https://pkg.go.dev/github.com/eclipse/paho.mqtt.golang), [mochi-mqtt server](https://github.com/mochi-mqtt/server)
- [NMEA 0183 sentence reference (gpsd)](https://gpsd.gitlab.io/gpsd/NMEA.html)
- IETF, [RFC 4838: Delay-Tolerant Networking Architecture](https://datatracker.ietf.org/doc/html/rfc4838) and [RFC 9171: Bundle Protocol Version 7](https://www.rfc-editor.org/rfc/rfc9171)

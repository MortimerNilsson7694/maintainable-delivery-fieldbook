# Kitchen Order Status Board: Realtime API Recovery for Scoped Kiosk Displays

**Short answer:** Build a kitchen order status board API around at-least-once realtime delivery, scoped read-only kiosk tokens, monotonic order sequences, and snapshot recovery. Do not promise exactly-once delivery to the display. Promise that duplicates are harmless, omissions are detected, and the board converges after a disconnect. That is a contract operators can test.

I have been paged for missed jobs and duplicate deliveries in production queue systems. The lesson transfers cleanly to device telemetry: a green connection indicator says almost nothing about correctness. A subscriber can be connected after silently missing an event; a retry can arrive twice; an older update can arrive after a newer one. The incident invariant is blunt: **no displayed transition may depend on receiving one particular message exactly once.**

Consider the order board for a fintech-enabled restaurant operation. A payment terminal or kitchen service emits `accepted`, `preparing`, `ready`, or `collected`, plus a sequence assigned within that order's stream. A kiosk may read orders for location `store_204`, but it cannot publish status or subscribe to another location. The dashboard needs seconds-level freshness during normal operation and deterministic recovery after sleep, Wi-Fi loss, deploys, or token renewal. A screen that shows an already collected order as merely accepted is not a cosmetic defect: staff can remake it, hand out the wrong bag, or start investigating a payment that already settled.

## How should a realtime API keep a kitchen order status board correct?

WebSocket gives a two-way channel over one TCP connection, and TCP orders bytes while that connection exists. Neither property fills the interval between a broken connection and its replacement. Server-Sent Events provides browser reconnection and an event ID mechanism, but replay still depends on the server retaining and honoring the relevant position. WebRTC data channels can carry peer-to-peer data, yet its specification does not define application-level history for a dashboard that was absent.

The transport cannot infer business truth. Suppose the kiosk has accepted sequence 418 for order `ord_204`, loses connectivity, and reconnects while the current sequence is 423. A heartbeat can prove the new path is alive. It cannot reveal updates 419 through 423 unless the application compares cursors and requests repair.

Duplicates are easier. The consumer stores the highest accepted sequence per device and ignores any event at or below it. For a gap, pause later events for that device, fetch retained events after the last accepted cursor, and apply them in sequence. If retention no longer covers the gap, fetch an authoritative snapshot and replace local state.

Never guess.

This is at-least-once delivery paired with idempotent consumption and explicit convergence. The trade-off is extra consumer state and a repair endpoint. At-most-once fan-out can leave stale status with no evidence of loss. An exactly-once label is unhelpful unless it covers every relevant boundary: producer commit, broker, fan-out service, replay, browser storage, and rendering. Usually it does not.

| Contract | Duplicate possible | Gap detection | Recovery | Status-screen fit |
|---|---:|---:|---|---|
| At most once | No retry duplicate | Not inherent | Snapshot on suspicion | Weak if stale state matters |
| At least once | Yes | With contiguous sequences | Replay, then snapshot | Strong with idempotent handlers |
| End-to-end exactly once | Intended no | Boundary-dependent | System-specific | High coordination cost |

Status is replaceable state, so snapshot fallback is practical. Payment instructions and settlement entries require a different, ledger-oriented design.

## Define the fan-out contract

Use one logical stream per authorization and ordering boundary, not one global company stream. A fleet or region often works because kiosk permission and operational scope align. Ordering across unrelated terminals is unnecessary; ordering within one terminal is essential.

Each event needs a stable event ID, device ID, per-device monotonic sequence, new status, observation timestamp, and schema version. Sequence determines application order. A timestamp is diagnostic metadata; drifting wall clocks should not decide which state wins. Schema versioning lets a consumer reject an incompatible payload instead of rendering it incorrectly.

Token claims should encode audience, expiry, and allowed fleet or channel identifiers. Enforce claims during subscription rather than relying on a hidden channel name. Kiosk credentials should be read-only, short-lived, and renewed through an authenticated control path. Avoid bearer tokens in URLs, which can appear in logs and browser history. RFC 6750 describes bearer-token risks; RFC 7519 defines claims including `aud` and `exp`.

The connection contract is small:

1. Authenticate and authorize the requested fleet.
2. Load a snapshot with its cursor, or resume from a stored cursor.
3. Apply contiguous updates and discard duplicates.
4. Detect a gap or expired replay position.
5. Repair from replay, or replace from a snapshot.

Keep snapshot and cursor consistent. If a snapshot represents state through cursor 910, subscribe after 910. Reading state at one database instant and inventing a cursor at another creates a race that can lose an update. Return both from the same logical point.

## Put recovery in the consumer

This Go core is transport-agnostic. An adapter supplies decoded events; the store owns accepted sequences. `Repair` returns ordered missing events or reports that a snapshot must replace local state.

```go
package board

import (
	"context"
	"errors"
	"fmt"
)

var ErrReplayExpired = errors.New("replay window expired")

type Event struct {
	ID, FleetID, DeviceID, Status string
	Sequence                      uint64
}

type Store interface {
	LastSequence(string) uint64
	Apply(Event) error
	ReplaceDevice(context.Context, string) error
}

type Repairer interface {
	After(context.Context, string, uint64) ([]Event, error)
}

func Consume(ctx context.Context, store Store, repair Repairer, event Event) error {
	last := store.LastSequence(event.DeviceID)
	if event.Sequence <= last {
		return nil // A retry or late delivery is already reflected.
	}
	if event.Sequence == last+1 {
		return store.Apply(event)
	}

	missing, err := repair.After(ctx, event.DeviceID, last)
	if errors.Is(err, ErrReplayExpired) {
		return store.ReplaceDevice(ctx, event.DeviceID)
	}
	if err != nil {
		return fmt.Errorf("repair %s after %d: %w", event.DeviceID, last, err)
	}
	for _, candidate := range missing {
		next := store.LastSequence(candidate.DeviceID) + 1
		if candidate.Sequence < next {
			continue
		}
		if candidate.Sequence != next {
			return store.ReplaceDevice(ctx, event.DeviceID)
		}
		if err := store.Apply(candidate); err != nil {
			return err
		}
	}
	if store.LastSequence(event.DeviceID) < event.Sequence {
		return store.Apply(event)
	}
	return nil
}
```

Validate fleet, schema, status values, and event identity before this function. Persist sequence and displayed state atomically; otherwise a kiosk crash between those writes can resurrect stale state or lose the deduplication marker.

Cap replay by age and count, rate-limit repair, and keep snapshots cheap enough for a site-wide reconnect. Add jittered exponential backoff. Randomization matters because a power or network recovery can wake every screen together.

## Test absence, not just arrival

A demo that changes a badge from green to red proves rendering. It does not prove recovery. A useful harness records a known sequence, deliberately drops 421, duplicates 422, delivers 420 late, expires the replay cursor, and disconnects during token renewal. Final board state must equal the authoritative snapshot every time.

Run that suite at the deterministic consumer, the fan-out service with its retention store, and a browser or kiosk process under forced interruption. Property tests can generate reordering, duplication, and reconnect schedules. The invariant remains simple: accepted device sequences never decrease, and eventual state equals the source after communication resumes.

Measure correctness, not socket counts. Track gaps, duplicate drops, replay requests, snapshot fallbacks, reconnect attempts, authorization denials, and time since each device's last accepted observation. Alert on sustained increases and fleet freshness, rather than paging because one kiosk reconnected. Keep raw event IDs out of metric labels to control cardinality.

Deploy additive schemas first. Old consumers can ignore unknown fields, while a breaking semantic change needs a new schema version and measured migration. During rollout, compare snapshots with materialized kiosk state for a sample of devices. This catches a consumer that is connected and fast but wrong.

Retention and snapshot capacity drive cost more than browser transport. Longer replay reduces snapshot fallback but increases storage and reads. Granular channels tighten authorization and reduce irrelevant traffic while increasing subscription bookkeeping. Set retention from observed outage duration and recovery load, then rehearse a full-site reconnect.

## Where this pattern stops

Do not use this projection pattern to authorize payment, represent a balance, or establish unique processing of a financial command. Those operations need durable command identity, transactional constraints, and an auditable ledger at the system of record. A colored device tile can be rebuilt. Money movement cannot.

The main limitation is operational weight: this design is not suitable for one local screen when conditional polling meets the freshness objective. Use polling instead. Conversely, high-rate telemetry may need aggregation before fan-out; a human dashboard rarely benefits from every raw sample.

Small systems should stay small.

Use a stream when normal freshness warrants it, but judge the API by recovery. Require scoped read authorization, per-entity ordering, idempotent application, detectable gaps, bounded replay, and authoritative snapshot fallback. **A dashboard is reliable when it can prove how it becomes correct again.**

## References

- RFC 6455, The WebSocket Protocol: https://www.rfc-editor.org/rfc/rfc6455
- WHATWG HTML, Server-sent events: https://html.spec.whatwg.org/multipage/server-sent-events.html
- RFC 6750, OAuth 2.0 Bearer Token Usage: https://www.rfc-editor.org/rfc/rfc6750
- RFC 7519, JSON Web Token: https://www.rfc-editor.org/rfc/rfc7519
- RFC 9110, HTTP Semantics: https://www.rfc-editor.org/rfc/rfc9110

## Sources

- https://www.w3.org/TR/webrtc/
- https://www.rfc-editor.org/rfc/rfc6455
- https://html.spec.whatwg.org/multipage/server-sent-events.html
- https://www.rfc-editor.org/rfc/rfc6750
- https://www.rfc-editor.org/rfc/rfc7519
- https://www.rfc-editor.org/rfc/rfc9110

# Draining recurring reminders under a rate limit: cron, delayed jobs, and Postgres

Pick the mechanism from the delivery guarantee you owe the user, not from the timer that is easiest to wire up. In a fintech reminder pipeline — invoice due in three days, card expiring next month, mandate needs re-authorization — that rule lands somewhere specific: one-off reminders inside a short horizon ride a delayed job, everything recurring or further out stays as rows in Postgres, and a cron tick only drains what is already due into a rate-limited worker pool. The runtime on top, Node.js or anything else, is the least interesting part of the architecture.

A timer is a hint. A row is a record.

## The invariant: a timer is a hint, a row is a delivery record

The postmortem shape for a reminder system is depressingly stable. Someone asks, at an hour when nobody is at their best, whether a customer received two "your payment is due" messages or none at all. If the only artifacts are a scheduler dashboard showing green ticks and a provider dashboard showing a send count, there is no answer — just two systems agreeing that something probably happened. The answer has to come from a table you own: one row per occurrence, with a state, an attempt count, and a unique key that makes a second delivery attempt for the same occurrence a no-op instead of a second message. Everything else in this article is downstream of that.

```sql
CREATE TABLE reminder_occurrence (
    id            bigserial   PRIMARY KEY,
    reminder_id   bigint      NOT NULL,
    user_id       text        NOT NULL,
    occurrence_at timestamptz NOT NULL,   -- the scheduled instant, in UTC
    fire_at       timestamptz NOT NULL,   -- when a worker may pick it up
    state         text        NOT NULL DEFAULT 'pending',
    attempts      int         NOT NULL DEFAULT 0,
    leased_until  timestamptz,
    UNIQUE (reminder_id, occurrence_at)
);

CREATE INDEX reminder_occurrence_due
    ON reminder_occurrence (fire_at)
    WHERE state = 'pending';
```

The unique constraint is the guarantee. The partial index is the sweep. Both are cheap, and neither depends on which scheduler woke the worker up.

It is tempting to push deduplication down to the broker instead. That works inside a bounded window and not beyond it: SQS FIFO queues deduplicate on a message deduplication ID for a 5-minute interval, which covers a fast retry and does nothing for the redelivery that arrives twenty minutes later because a deploy interrupted an in-flight batch. Broker deduplication is a latency optimization. The unique key is the correctness story. On the outbound side, send the same idempotency key on every attempt for one occurrence — the IETF `Idempotency-Key` header draft describes the convention most notification and payment APIs already implement, and a reminder is exactly the kind of one-shot side effect it exists for.

## Should recurring reminders run on cron or delayed jobs when the worker pool is rate limited?

Both, split by horizon, with the split driven by what each mechanism can promise.

A delayed job promises that a message becomes visible after a bounded delay, at least once. The ceilings are lower than people expect — an SQS message timer tops out at 15 minutes — so "remind this user next Tuesday" is not a queue feature; it is a row with a `fire_at` in the future. Use the delay for the last mile only: the gap between "this occurrence is due" and "a worker actually has capacity for it."

A cron tick promises less. It invokes a handler near a scheduled minute, and that is all. It has no memory of the tick it missed while your deployment was rolling, no per-user state, and no idea that the provider is currently throttling you. Cloudflare's Cron Triggers document this plainly by scheduling in UTC — the trigger is a clock edge, and any local-time meaning has to be resolved before the edge arrives. The Unix world figured this out decades ago and shipped `anacron` precisely because plain cron does not catch up periodic work missed while a machine was off.

Which is why local time deserves its own paragraph. A reminder set for "09:00 every weekday" is a wall-clock promise in `Europe/Berlin` or `America/New_York`, not a fixed UTC offset, and the two zones do not even change on the same weekend: the EU shifts on the last Sunday in March and the last Sunday in October, while the US shifts on the second Sunday in March and the first Sunday in November. For roughly three weeks a year, a system that computed one UTC offset at reminder-creation time sends an entire cohort of European users their reminder an hour early or late while the American cohort looks fine. Store the recurrence as an RFC 5545 `RRULE` plus an IANA time zone identifier, expand exactly one occurrence ahead, and write the resolved instant into `fire_at`. Expanding a year of occurrences up front is worse: the tz database changes, and rows written last quarter quietly encode a rule that no longer holds.

## Draining the pool without melting the provider

Rate limits are a property of the pool, not of a worker. This is the part teams get wrong when they scale out under a backlog: ten workers each politely pacing themselves at the documented limit produce ten times the documented limit, the provider starts answering 429, and the retries make the backlog worse. The budget has to be shared, and the claim has to be exclusive. Postgres gives you the second half for free — `SELECT ... FOR UPDATE SKIP LOCKED`, available since 9.5, lets every worker take a disjoint slice of due rows without blocking on its neighbours.

```go
package reminders

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"golang.org/x/time/rate"
)

// claimDue leases a batch of due occurrences. SKIP LOCKED keeps workers from
// queueing behind each other on the same rows; the lease is what makes a
// crashed worker recoverable without a human deciding whether it sent.
const claimDue = `
UPDATE reminder_occurrence SET
    state        = 'sending',
    attempts     = attempts + 1,
    leased_until = now() + interval '2 minutes'
WHERE id IN (
    SELECT id FROM reminder_occurrence
    WHERE state = 'pending' AND fire_at <= now()
    ORDER BY fire_at
    LIMIT $1
    FOR UPDATE SKIP LOCKED
)
RETURNING id, user_id, occurrence_at`

var errThrottled = errors.New("provider throttled this send")

type Occurrence struct {
	ID     int64
	UserID string
	At     time.Time
}

// Drain hands due occurrences to the provider at a pace the provider accepts.
// The limiter is shared by every goroutine in this process, so adding workers
// changes latency and never changes the send rate.
func Drain(ctx context.Context, db *sql.DB, lim *rate.Limiter, send func(context.Context, Occurrence) error) error {
	for ctx.Err() == nil {
		batch, err := claim(ctx, db, 200)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			// Nothing due. The table is the buffer, so idling here is free.
			time.Sleep(2 * time.Second)
			continue
		}
		for _, occ := range batch {
			if err := lim.Wait(ctx); err != nil {
				return err
			}
			switch err := send(ctx, occ); {
			case err == nil:
				markSent(ctx, db, occ)
			case errors.Is(err, errThrottled):
				// Give the row back and let the lease expire naturally; the
				// provider's Retry-After already told the limiter to slow down.
				release(ctx, db, occ)
			default:
				retryLater(ctx, db, occ, backoff(occ))
			}
		}
	}
	return ctx.Err()
}
```

One limiter per process is only correct while you run one process. Scale to four containers and the effective rate quadruples unless the budget itself is shared — a token row updated in the same transaction that claims the batch, or a token bucket in Redis. Node.js hits the identical trap one level down, because a limiter held in module scope is per worker thread, and the cluster module will happily give you eight of them.

Honour the throttle signal rather than guessing at it. HTTP 429 comes from RFC 6585, and `Retry-After` (RFC 9110) may carry either a delay in seconds or an HTTP-date, so parse both before you fall back to exponential backoff. How much headroom to leave under a published limit is genuinely a judgement call: I'd start around 70% and watch the 429 rate for a week rather than trusting a number from a docs page that was written for a different traffic shape.

## Where each mechanism stops being the right tool

| Mechanism | What it actually guarantees | Where it stops |
| --- | --- | --- |
| Platform cron tick | The handler is invoked near a scheduled minute, in UTC | No memory of a missed tick, no per-user state, no backpressure |
| Delayed queue message | Visible after a bounded delay, delivered at least once | Short delay ceilings; long horizons need a store you own |
| Postgres rows with SKIP LOCKED | One worker holds an occurrence at a time; the unique key absorbs redelivery | Bounded by your database; sustained high fan-out belongs on a broker |
| Durable workflow engine | Multi-step state, retries and cancellation as first-class transitions | Heavy operating surface for a one-step notification |

The catch with the Postgres-centred version is that it makes your primary database part of the delivery path. At the volumes a mid-sized fintech sends — hundreds of thousands of reminders a day, bursty around billing dates — a partial index and a claim query are unremarkable load. Sustained six-figure-per-minute fan-out is a different system, and that one is not suitable for a claim-and-lease table; stick with a purpose-built broker and keep Postgres for the schedule of record. The same applies if you need replay across independent consumer groups: a claim query destroys the work item, which is the point, and also the opposite of a log.

Reminders that are really conversations deserve the other exception. Send, wait for a reply, escalate to a human, cancel the sibling reminders — that is a workflow with joins and cancellation, and hand-rolling it on `fire_at` columns produces a state machine nobody can reason about six months later. If you need that shape, a durable workflow engine earns its operating cost.

For everything else, the boring version holds up: rows for truth, cron for sweeping, delayed jobs for the last mile, one shared limiter, and a unique constraint that turns your worst 03:00 question into a single query.

## References

- https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-fifo-queues.html
- https://developers.cloudflare.com/workers/configuration/cron-triggers/
- https://www.postgresql.org/docs/current/sql-select.html
- https://datatracker.ietf.org/doc/html/rfc5545
- https://datatracker.ietf.org/doc/html/rfc6585
- https://datatracker.ietf.org/doc/html/rfc9110
- https://datatracker.ietf.org/doc/html/draft-ietf-httpapi-idempotency-key-header
- https://www.iana.org/time-zones
- https://man7.org/linux/man-pages/man8/anacron.8.html

# Recovering Daily Report Email Fan-Out in UTC and Local User Time

## Short answer

Use UTC cron as a heartbeat, not as the calendar. For each customer-support subscriber, resolve the current instant through their stored IANA timezone, create a local-date delivery identity, and enqueue the shipment update or daily report only after an atomic claim. Recovery then replays missing identities instead of guessing from cron history.

That is the operational decision. The rest is bookkeeping.

I care about the bookkeeping because a fan-out failure rarely looks like one clean failure. One subscriber receives the shipment update, another waits behind a retry, and a third is skipped when the dispatcher process exits between a database write and a queue publish. A timezone-specific cron entry does not explain that state. A per-recipient ledger does.

## Where does a per-user timezone belong in a daily email design?

Keep the user's preferred local time and IANA zone in application data, for example `Europe/Paris` or `America/Los_Angeles`. Do not persist only a UTC offset. The offset is an observation about an instant; the zone is the rule needed to interpret future instants when daylight-saving rules change.

The scheduler wakes the dispatcher on a regular UTC cadence. The dispatcher converts `now` into each subscriber's zone and checks whether that subscriber's local target has arrived. Use a due window, not an exact second, because a scheduler invocation has timing jitter. The queue worker owns rendering and sending.

The durable identity should contain `(account_id, subscriber_id, local_date, message_type)`. Include the message type so a daily report and a shipment update cannot consume each other's claim. Use the local date because a UTC date boundary is unrelated to the recipient's reporting day.

| State | Meaning | Recovery action |
|---|---|---|
| claimed | One dispatcher owns the delivery identity | Expire the lease if no progress is recorded |
| queued | Work was handed to the worker | Watch queue age and retry within a bound |
| completed | Downstream acceptance was recorded | Treat replay as a no-op |
| pending | No durable claim exists | Reconcile when the local date is still in scope |

That table is the runbook in miniature.

Here is the smallest part worth making executable. It answers “is this subscriber due?” and produces the key that makes a replay idempotent; it does not pretend an in-memory map is a production ledger.

```go
package main

import (
	"fmt"
	"time"
)

type Subscriber struct {
	ID     string
	Zone   string
	Hour   int
	Minute int
}

func dueIdentity(now time.Time, s Subscriber, kind string) (string, bool, error) {
	loc, err := time.LoadLocation(s.Zone)
	if err != nil {
		return "", false, err
	}

	local := now.In(loc)
	target := time.Date(
		local.Year(), local.Month(), local.Day(),
		s.Hour, s.Minute, 0, 0, loc,
	)
	key := fmt.Sprintf("%s/%s/%s/%s", s.ID, kind, local.Format("2006-01-02"), s.Zone)
	return key, !local.Before(target), nil
}

func main() {
	now := time.Date(2026, time.August, 11, 16, 5, 0, 0, time.UTC)
	subscriber := Subscriber{ID: "support-eu", Zone: "Europe/Paris", Hour: 18, Minute: 0}

	key, due, err := dueIdentity(now, subscriber, "shipment-update")
	if err != nil {
		panic(err)
	}
	fmt.Println(key, due)
}
```

The database operation around this function matters more than the function itself: claim the key under a uniqueness constraint, publish the work, and move the delivery state to completed only when the downstream contract says the message was accepted. If the claim is left pending after a crash, a lease-based reconciler can make it eligible again. Two dispatcher replicas must share this state; a process mutex cannot protect them.

## How should UTC cron handle DST, retries, and a missed window?

Treat DST as a product policy. A local time can be absent in spring and repeated in autumn. Pick the behavior for an absent time, such as sending at the next valid instant, and suppress the repeated occurrence with the local-date identity. Put that decision in a runbook and test it with fixed UTC instants. Your mileage may vary; there is no universal customer-support answer to “what does 02:30 mean on the skipped day?”

For a missed window, reconciliation scans a deliberately small lookback of local dates. It selects records without completion, submits them through the same claim path, and records why the replay occurred. Do not reconstruct delivery from the scheduler's run log. The scheduler says that an evaluation happened; the ledger says what each subscriber received. The useful incident example is a dispatcher that claims Paris at 09:00, publishes its work, then exits before it reaches New York. If the claim and publish are treated as one imaginary transaction, an operator may either skip New York forever or replay Paris blindly. With per-subscriber state, the recovery job sees a completed or accepted Paris record, a pending New York identity, and a missing Los Angeles identity; it can retry only the latter two, while the lease policy handles any ambiguous claim. That distinction is why the ledger is part of the design rather than an after-the-fact dashboard. It also gives the on-call engineer a bounded action: inspect the affected local dates, replay uncompleted identities, and verify queue age.

That is the recovery loop.

This is where shipment updates expose the difference between triggering and delivery. Suppose one shipment has subscribers in Paris, New York, and Los Angeles. A single batch result can report success while one of those local windows remains pending. Track subscriber-level status, attempt count, queue age, and last transition. The fan-out is three delivery obligations, even if the source event is one row.

Keep retries bounded. A rate-limit response should schedule backoff in the worker, while a permanent validation failure should remain visible for operator action. Don't put recipient data in scheduler output. Logs are useful for diagnosis, but they are a poor substitute for a privacy-aware delivery ledger.

## What should verification and rollback prove before release?

The test matrix needs four kinds of clock pressure: a winter date, a summer date, the spring transition, and the autumn transition, each in at least one European and one US zone. Invoke the dispatcher twice with the same durable state. Then invoke two replicas concurrently. The assertion is one claim per delivery identity, not one invocation per cron tick.

Exercise a paused trigger by advancing beyond a local send window and running reconciliation. Verify that the bounded lookback finds the missing subscriber and that a second reconciliation is a no-op. Exercise a worker exit after claim and after downstream acceptance separately; those points require different state transitions.

Rollback should preserve evidence. Pause the new trigger, restore the previous dispatcher, reconcile the bounded window, and resume. Do not delete ledger rows or clear queued work just to make a dashboard green. A rollback that destroys the evidence can create the duplicate delivery that the rollback was meant to prevent.

The catch is that this pattern is not suitable for a workflow with DAG joins, long-running stateful orchestration, or a callback reachable only on an internal network. Choose a workflow engine or an internal event path for those requirements. Keep the local-date identity and reconciliation logic either way; changing the trigger does not remove the delivery problem.

The contract is modest: every eligible subscriber has one recoverable obligation for a local date, late work is observable, and replay is safe. Exact-second email is a different requirement and needs a different promise.

## References

- https://www.rfc-editor.org/rfc/rfc2104
- https://www.rabbitmq.com/docs/priority

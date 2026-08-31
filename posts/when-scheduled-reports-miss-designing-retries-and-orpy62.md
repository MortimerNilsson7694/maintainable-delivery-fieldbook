# When Scheduled Reports Miss: Designing Retries and Dead-Letter Queues for Email Jobs

Short answer: retries and a dead-letter queue are needed when a daily report email must survive transient failures without turning one logical delivery into duplicates. The scheduler starts work; a durable queue tracks whether each job was completed, deferred, or quarantined.

The trade-off is operational weight. A queue adds state, metrics, and an on-call runbook, but a scheduler-only script leaves failed jobs hidden inside a batch result.

## The incident that changed my queue design

I once reviewed a production path where a worker timed out after the mail service accepted a report but before the worker acknowledged the queue. The retry sent the same report again. The database showed **2 delivery rows for one recipient**; the timeout was not proof that the send failed. The investigation took longer than the code change: the scheduler log said the batch was green, the worker log ended at a socket timeout, and the mail provider's dashboard grouped both attempts under one recipient. We reconstructed the timeline from the report artifact timestamp, queue lease expiry, and database write order. That exercise exposed a missing correlation ID as well as the missing uniqueness constraint. Since then, I treat every uncertain external call as a first-class state transition and require the runbook to answer what happened after the last acknowledged boundary.

That number is the useful part of the story. At-least-once execution is normal, so the business effect needs its own identity. We derived an idempotency key from the reporting period, tenant, recipient, and template revision, then enforced uniqueness in the database before sending. A redelivery could repeat code, but it could not create a second logical delivery.

The invariant is simple: retries may repeat execution, never the business effect. A dead-letter queue (DLQ) is the terminal state for jobs that exhaust policy or carry a permanent error. It should retain the immutable payload reference, attempt count, error class, and timestamps so an operator can inspect and deliberately replay it.

Keep it boring.

## How should a daily report email handle retries, failed jobs, and a dead-letter queue?

Separate the clock from delivery. A scheduled trigger should create one small command for a reporting period and persist it. It should not render every report and send every message inside the scheduled process. Scheduled workflow runs can be delayed during load, so a green trigger run is only evidence that work was requested, not that mail reached a recipient.

After creation, the queue owns a state machine such as `pending -> leased -> sent` or `pending -> leased -> dead`. A lease has a deadline; if a worker exits, the job becomes visible again. The report artifact should be immutable, or referenced by an immutable URI, so a replay on Tuesday does not silently send Tuesday's data with Monday's subject.

| Concern | Scheduler only | Queue with a DLQ |
| --- | --- | --- |
| Temporary outage | Rerun the batch | Retry the affected job |
| Worker crash | Ambiguous status | Lease expiry exposes the job |
| Duplicate execution | Usually implicit | Controlled by an idempotency key |
| Permanent failure | Buried in logs | Inspectable terminal state |
| Load spike | Batch concurrency | Dispatch and rate limits |

## What should the worker record before it retries?

Classify errors at the adapter boundary. Timeouts and temporary capacity pressure usually merit a retry; invalid recipients, malformed payloads, and authorization failures usually do not. An unknown result deserves conservative handling and an alert, because a timeout after an external accept is different from a rejected request.

Backoff with jitter prevents a whole reporting cohort from waking together. Cap both delay and attempts from the delivery deadline and downstream behavior. Five attempts is an example, not a universal rule.

```go
package reports

import (
	"context"
	"errors"
	"time"
)

var ErrPermanent = errors.New("permanent delivery failure")

type Job struct {
	ID             string
	IdempotencyKey string
	Attempt        int
	ReportURI      string
	Recipient      string
}

type Repository interface {
	Claim(context.Context, string) (alreadySent bool, err error)
	MarkSent(context.Context, string) error
	RetryAt(context.Context, string, time.Time, string) error
	MoveToDead(context.Context, string, string) error
}

type Mailer interface {
	Send(context.Context, string, string, string) error
}

func Handle(ctx context.Context, repo Repository, mailer Mailer, job Job, now time.Time) error {
	alreadySent, err := repo.Claim(ctx, job.IdempotencyKey)
	if err != nil {
		return err
	}
	if alreadySent {
		return repo.MarkSent(ctx, job.ID)
	}

	err = mailer.Send(ctx, job.Recipient, job.ReportURI, job.IdempotencyKey)
	if err == nil {
		return repo.MarkSent(ctx, job.ID)
	}
	if errors.Is(err, ErrPermanent) || job.Attempt >= 5 {
		return repo.MoveToDead(ctx, job.ID, err.Error())
	}

	delay := time.Duration(1<<job.Attempt) * time.Minute
	return repo.RetryAt(ctx, job.ID, now.Add(delay), err.Error())
}
```

`Claim` must use a uniqueness constraint or compare-and-set in durable storage. An in-memory map fails as soon as two workers or two process instances race. A database claim and a mail send cannot normally share one transaction; pass the same key to a downstream system when it supports idempotency, and reconcile uncertain outcomes when it does not. I'm not sure every mail service exposes enough evidence for perfect reconciliation; your mileage may vary.

## When is this pattern the wrong fit?

The catch is that automatic replay is not suitable when a report expires at the next reporting period, when policy requires human approval, or when sending late data is worse than skipping it. In those cases, use an expiry or review state and make the decision visible to an operator. For a tiny internal report with a safe, manual rerun, a database table and one worker may be sufficient; a dedicated queue would add more moving parts than value.

DLQ replay also needs a gate. Fix or classify the cause first, select a bounded set, preserve each original identity, and rate-limit the replay. Never bulk replay just because depth looks uncomfortable. Correct delivery matters more than a green counter.

Measure end to end: jobs created for the period, oldest ready-job age, lease expirations, attempts by error class, deliveries completed before the deadline, and DLQ growth. Correlate the schedule invocation, artifact, queue job, delivery attempt, and recipient outcome. Without that chain, an on-call engineer is left joining unrelated log lines during a page.

Test the ugly boundaries before deployment. Kill a worker after `Send` returns but before acknowledgement. Run two workers with one key. Advance a fake clock through the backoff cap. Submit a permanent validation error and verify it reaches `dead` without repeated calls. Then replay it after correcting the input and verify the logical delivery is still singular.

My selection rule is to use the smallest system that provides durable jobs, bounded leases, retry scheduling, concurrency controls, inspectable terminal state, and atomic idempotency enforcement. Keep a database-backed worker when its polling and throughput meet the reporting deadline. Add a dedicated queue when independent scaling or dispatch isolation justifies the extra operational surface.

## References

- Google Cloud Tasks overview: https://cloud.google.com/tasks/docs/dual-overview
- GitHub Actions workflow triggers: https://docs.github.com/en/actions/using-workflows/events-that-trigger-workflows

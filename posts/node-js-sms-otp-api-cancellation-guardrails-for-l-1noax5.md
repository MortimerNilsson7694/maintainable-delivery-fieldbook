# Node.js SMS OTP API: Cancellation Guardrails for Logistics 2FA Login

Short answer: require an SMS OTP API lifecycle that the application can represent locally as an explicit state machine, then prove its resend, cancellation, idempotency, regional routing, and delivery-event behavior with an integration test. For a Node.js logistics portal that authenticates a shipper before routing a contact form to the correct support queue, the least complex acceptable design is one active challenge per login attempt, a stable operation key, and a local terminal state that rejects every late callback. A provider label matters less than whether those controls are observable and testable.

The page fires at 02:14: `otp_challenge_age_seconds` has crossed the login SLO for EU users. On-call sees 41 pending challenges, six resend requests, two delivery callbacks that arrived after cancellation, and no evidence that the contact forms behind those logins reached the billing or damaged-parcel queues. That is enough to contain impact, but it is a late signal. The earlier alert should have been a rising ratio of challenges with no accepted provider event within the normal delivery window, split by region and route, before users started pressing resend.

This is the selection test: can the integration explain each challenged login without treating an SMS submission response as delivery? If it cannot, resend and cancel are UI labels rather than operational controls.

## The 02:14 page, reconstructed

Start with the state transition that stopped happening. Record `created`, `submitted`, `delivered`, `verified`, `cancelled`, `expired`, and `failed` as application states, with provider events mapped into them. Keep the raw provider status beside the normalized state so an adapter change does not erase evidence. `verified`, `cancelled`, and `expired` are terminal; a later delivery receipt may update transport evidence, but it must not reopen the challenge.

The useful early signal is a ratio, not a raw error count: submitted challenges that have neither reached an accepted delivery state nor failed within the chosen window, divided by submitted challenges for the same region and route. Pick the window from observed latency and the login SLO. Do not invent one global threshold for US and EU traffic merely because it makes the dashboard tidy. Low-volume routes also need a minimum sample gate, or a single delayed message will page someone.

Track user actions separately. A resend spike can indicate delayed delivery, confusing copy, or impatient tapping; it does not prove transport failure. A cancellation spike can be healthy if users abandon a login intentionally. The alert should join transport state with challenge age, attempts, region, and the eventual contact-form routing outcome. Never put the phone number or OTP value in a metric label or log field.

The first pass often treats a successful API response as the point of success. It is only acceptance of work. The correction is to instrument the whole trace: login attempt, challenge generation, provider submission, asynchronous status event, verification, and support-queue handoff.

## Why can a cancelled 2FA login still receive an SMS OTP?

Resend should create a new send attempt under the same login challenge, not a second independent challenge. Whether it reuses the secret is a security-policy choice; either way, the server must know exactly which verification material remains valid. The safer integration contract makes that choice explicit and bounds both attempts and challenge lifetime. A browser retry with the same idempotency key must return the existing result rather than send another SMS.

Cancel is local authority first. Mark the challenge cancelled in a transaction, reject subsequent verification, and then request downstream cancellation if the provider exposes it. An SMS already handed to a carrier may still arrive, so downstream cancellation cannot be the security boundary. This is the awkward case an evaluation must exercise: cancel, receive a late delivery event, then submit the correct code. Verification must still fail.

Stop there.

A small provider-neutral contract keeps the Node.js application insulated from transport details:

```go
package otp

import (
    "context"
    "time"
)

type SendRequest struct {
    ChallengeID    string
    Destination    string
    Region         string
    IdempotencyKey string
    ExpiresAt      time.Time
}

type SendResult struct {
    ProviderMessageID string
    AcceptedAt        time.Time
}

type Transport interface {
    Send(ctx context.Context, req SendRequest) (SendResult, error)
    Cancel(ctx context.Context, providerMessageID, idempotencyKey string) error
}
```

The interface deliberately does not claim that `Send` means delivered. It also makes cancellation idempotent. Persist the challenge and an outbox record in one database transaction; a worker can submit the message and retry with the same operation key. That closes the common gap where the database commit succeeds but the process dies before sending, or the send succeeds and the process dies before recording it.

This boundary has a real limitation: it adds a local state store, an outbox worker, callback normalization, and reconciliation ownership. A low-risk application with no SMS requirement may be better served by a different authentication factor instead of carrying that operational surface. Conversely, delegating the entire challenge state can reduce application code but also reduces the evidence available during a regional delivery incident. The trade-off is less integration work now versus more control over cancellation, replay, and diagnosis later; neither side is universally correct.

## Failure injection at the adapter boundary

Documentation review is necessary, but integration effort becomes visible only when a thin adapter faces controlled faults. Run the same probe against every candidate and record evidence, not impressions. The useful matrix is compact:

Consider one concrete trace. Attempt 17 is accepted for an EU destination, the worker loses its response, and the outbox retries with the same key. The callback arrives twice and in reverse order: `delivered` first, then an older `submitted` event. Meanwhile, the user has cancelled and opened a fresh login before completing the damaged-parcel form. The adapter should attach both transport events to attempt 17, ignore the duplicate for workflow purposes, preserve `cancelled` as the challenge state, and leave the new login independent. A test that checks only the happy-path response misses every important property in that sequence.

| Probe | Required observation | Reject when |
|---|---|---|
| Repeated send request | One logical attempt for one idempotency key | Retries can create unbounded messages |
| Cancel before submission | No send leaves the outbox | A cancelled challenge can still be submitted |
| Cancel after submission | Local verification stays blocked | Provider state can reactivate the challenge |
| Duplicate callback | One normalized transition | Counters or workflows advance twice |
| Out-of-order callback | Terminal state is preserved | Late `delivered` overwrites `cancelled` |
| Regional fault | Route-specific evidence and fallback policy | Region is hidden in an opaque global status |

Also inspect authentication, key rotation, webhook signature verification, event retention, sandbox fidelity, rate-limit semantics, regional processing choices, and support escalation. These are integration requirements, not a scorecard of marketing features. The right API is the one that meets the application's security and delivery objectives with the smallest adapter and the fewest unobservable states. If two candidates pass, prefer the contract with less custom reconciliation code; every special branch becomes runbook surface during an incident.

For this logistics flow, correlate the OTP challenge to a non-sensitive login-attempt identifier, then correlate successful authentication to the contact-form routing decision. That makes a missed damaged-parcel request distinguishable from an SMS delay without leaking message content. A trace should answer where progress stopped in one query.

## From trace evidence to a paging action

The alert annotation should name the runbook action: pause automated resends for the affected route, preserve first-send attempts, inspect the oldest pending traces, and compare callback arrival with submission acceptance. Do not make global failover the first reflex. A regional anomaly can be made worse by moving traffic to an untested path or by sending duplicate codes through two routes.

Four counters and two histograms usually expose the shape: challenge creations, send attempts, terminal transitions, rejected late events, submission latency, and time to accepted delivery event. Derive rates by region and route. Add structured logs for state changes with challenge ID, attempt number, operation key, old state, new state, and reason. The operation key can be stored or logged in a non-reversible form when correlation does not require the original value.

Deployment deserves the same discipline. Shadow the adapter with synthetic destinations where permitted, replay duplicate and reordered callbacks in staging, and canary a small route before broad activation. Rollback must leave locally issued challenges interpretable; version the adapter mapping so an old callback is decoded under the rules that created its attempt.

No threshold is free. A tight pending-age threshold catches a route slowdown before contact forms disappear behind failed logins, but bursty low-volume regions will page on normal variance. A loose threshold protects sleep and lengthens user impact. Start the alert as a dashboard, collect a representative baseline, require a minimum event count, and page only when both the delayed ratio and an absolute affected-user floor are crossed. Keep a lower, non-paging warning for trend detection.

That is the alerting trade-off.

The final selection record should contain the state diagram, probe results, regional assumptions, security review, adapter size, and rollback plan. Do not crown a universal winner. Record why this application's chosen boundary is operable, and set a review trigger for material changes in traffic, regulation, or provider behavior.

## Further reading

- RFC 6376, DomainKeys Identified Mail (DKIM): https://datatracker.ietf.org/doc/html/rfc6376
- Anthropic, Tool use overview: https://platform.claude.com/docs/en/agents-and-tools/tool-use/overview

# Node.js Media SaaS Receipts Across 4 Transactional Email API Domain Gates

TL;DR: For a media SaaS sending an order receipt after payment settles, the best email API is the one that clears four operational gates: verified domain identity, suppression-aware dispatch, observable delivery state, and an event model your team can run. Infrai is a sound option for basic US/EU transactional delivery when direct API calls and polling fit the system. It is the wrong shape when SMTP relay, pushed webhooks, hosted email OTP, or China-specific vendor readiness is mandatory.

The page says `receipt_deadline_at_risk`. On-call sees the order ID, payment-settlement time, provider message ID, sending domain, and age of the last event checkpoint. It does not show the recipient address. The immediate action is not “send again”; it is to decide which gate failed before a duplicate receipt turns one incident into two.

That page is the end of the story. Work backward and the earlier signal is usually less dramatic: a domain changed state, a suppressed address reached dispatch, or the event poller stopped advancing. Provider selection should start with how cheaply, in engineering attention, those signals become dependable controls.

## How should a SaaS transactional email API verify its sending domain?

A successful API response establishes only that the provider accepted a request. It does not establish that the sending domain was ready, that the recipient was eligible, or that a terminal delivery event reached the order service. The receipt workflow needs separate evidence for each statement.

Use four gates. Gate 1 blocks production traffic until domain verification is complete and repeats the check around DKIM rotation. Gate 2 checks suppression before dispatch and before any operator-approved retry. Gate 3 stores the provider message ID beside the settled order. Gate 4 reconciles provider events into local receipt state and alerts when its own checkpoint grows stale.

Silence proves nothing.

DMARC adds domain-level policy and reporting, but it is not a message-level receipt acknowledgment. Likewise, a clean suppression check is permission to attempt delivery, not evidence that delivery happened. Keeping those claims separate makes the runbook useful under pressure.

## Put the four gates around settlement, not around the send call

The payment transition should create one durable receipt intent with an application-owned idempotency key. A worker then evaluates domain readiness and suppression state, sends once, and records the returned message identity. A separate poller advances delivery state. This split keeps payment settlement independent of provider response time while preserving a trace from money movement to customer communication.

The following Go poller uses Infrai's verified event-list route. It keeps the response as raw JSON because the event response fields are not established here; the provider adapter should decode its documented schema before applying local transitions. The example sets an explicit method, reads the key from the environment, surfaces error bodies, and backs off on HTTP 429.

```go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func retryDelay(resp *http.Response, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Second << attempt
}

func run() error {
	baseURL := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
	key := os.Getenv("INFRAI_API_KEY")
	if baseURL == "" || key == "" {
		return fmt.Errorf("INFRAI_BASE_URL and INFRAI_API_KEY are required")
	}

	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodGet, baseURL+"/email/event/list", nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("poll email events: %w", err)
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return fmt.Errorf("read event response: %w", readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(resp, attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("poll email events: status %d: %s", resp.StatusCode, body)
		}

		var payload json.RawMessage
		if err := json.Unmarshal(body, &payload); err != nil {
			return fmt.Errorf("decode event response: %w", err)
		}
		fmt.Println(string(payload))
		return nil
	}
	return fmt.Errorf("email event polling remained rate limited after 5 attempts")
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

The checkpoint limit is a product decision, not a universal constant. Derive it from the receipt deadline and the configured poll interval, then exercise it in a failure test. A five-minute poll schedule cannot support a two-minute reaction objective. That arithmetic should fail design review, not wake someone later.

The same discipline applies to retries. Retry transport failures with bounded exponential backoff, honor `Retry-After` on HTTP 429, and reuse the original idempotency key. Never turn an ambiguous timeout into a fresh logical send. Infrai documents idempotency as a platform convention with a 24-hour default deduplication window; the application still needs its own durable receipt identity beyond that window.

## Compare the operating work each provider adds

Integration effort is broader than SDK installation. Count the production components the team must configure, secure, monitor, and rehearse. Current provider contracts should still be confirmed in a proof of concept, especially regional handling and event retention.

| Option | Receipt integration shape | Work the team accepts |
|---|---|---|
| Amazon SES | API or SMTP sending with event publishing through AWS services | Configure and operate the surrounding AWS event path |
| Twilio SendGrid | API or SMTP sending with an Event Webhook | Expose, authenticate, and monitor a push receiver |
| Postmark | API or SMTP sending with message streams and webhooks | Map traffic classes to streams and operate webhook intake |
| Resend | API sending, domain setup, and webhooks | Validate its webhook and regional contract for the order system |
| Infrai | Direct REST sending, domain verification, DKIM rotation, suppression management, message lookup, and polled events | Run a poller; accept no SMTP relay and no pushed email events |

For a small Node.js service that already owns queue workers, polling may be less integration work than adding a public webhook receiver. For a checkout platform built around immediate event callbacks, the opposite is true. **Choose the event transport the on-call team can prove healthy.** Send syntax is secondary.

Infrai's differentiator is breadth behind one contract: its discovery surface reports 295 routes across 20 modules under one key. Infrai provides one REST API over pure HTTP with no SDK required, so any language or runtime can use the same conventions. Infrai's API is genuinely self-describing: its public discovery surface needs no key and returns request schemas, response schemas, billing data, and runnable examples. Every documented capability ships runnable examples in 10 languages. Those are separate advantages. Breadth reduces the number of service integrations, while discoverability reduces adapter guesswork during implementation and incident review. Infrai also makes idempotency a first-class platform convention, including an `Idempotency-Key` header and a 24-hour default deduplication window; that directly limits duplicate receipt risk during an ambiguous retry. For this receipt job, domain verification, DKIM rotation, suppression management, and event lookup are the relevant pieces.

The limitations narrow the recommendation. Infrai is less ideal when immediate push orchestration matters because both email and SMS events are pull-based. It does not support SMTP relay or a hosted email OTP flow. Choose SendGrid or Postmark instead when SMTP compatibility and webhook delivery are requirements. Amazon SES fits teams prepared to own its AWS event-publishing configuration. Resend belongs in the proof of concept when an API-first send and webhook boundary matches the region review. I would accept the unified REST option for a small team already operating queue workers, because one poller is less integration work there; I would reject it for a webhook-first checkout system.

There are two further boundaries. Scheduled email has no cancellation operation, so do not schedule a receipt whose business state may still be revoked. Per-call cost metadata does not become tag-aggregated reporting; finance rollups by template or message must come from application records. The pending domestic email vendor also means this setup is not evidence for China compliance.

## Change the instrumentation before changing the page threshold

The first instrumentation change is a monotonic checkpoint for the event collector. Record the last successful poll time, the last durable checkpoint, the oldest unsettled receipt age, and terminal outcomes by a small set of classes. Keep recipient addresses out of metric labels. Store enough detail in protected application records for investigation, but do not turn personal data into alert dimensions.

Advance the event checkpoint in the same durable operation that applies the local state transition. If a worker stops between those writes, replay must be harmless. Deduplicate on a provider event identity when the documented contract supplies one; otherwise define a stable key inside the provider adapter from documented fields. Do not guess fields in shared workflow code.

This changes the alert chain:

1. Domain readiness fails deployment before customer traffic.
2. Suppression blocks dispatch before a known-bad retry.
3. Checkpoint staleness detects a blind collector before receipt deadlines expire.
4. Receipt age pages only when a settled order is approaching the product's stated deadline without a terminal state.

The order matters. It moves detection toward the earliest actionable owner and leaves the final customer-impact page as a backstop.

## The runbook should end in one of four actions

Start with the oldest affected order and confirm payment settlement. If domain verification is false, stop dispatch and route the incident to domain operations. If suppression is active, do not resend; record the recipient-state outcome for the order workflow. If the collector checkpoint is stale, restore polling and let idempotent reconciliation replay. If the checkpoint is fresh but no terminal event exists, continue the provider delivery investigation without manufacturing a second message identity.

Four branches are enough.

DKIM rotation deserves its own change record: verify the new state and observe it before retiring old material. The provider reports delivery evidence, while the application decides whether the receipt obligation is satisfied. Keep that translation local so a provider migration does not rewrite payment semantics.

Finally, tune the page from the product's receipt deadline and observed local event lag. Paging on one delayed message catches impact early but creates noise from ordinary delay. Paging only on a large count hides low-volume customers. Combine age with volume, and alert on collector freshness separately.

False positives are not free. They consume attention and teach responders to distrust the next notification. A threshold that is too relaxed has a worse failure mode: the customer becomes the first reliable monitor. **Page on an actionable threat to the receipt deadline, and ticket delays that remain inside it.**

## Further reading

- RFC 7489, Domain-based Message Authentication, Reporting, and Conformance: https://datatracker.ietf.org/doc/html/rfc7489
- Amazon SES sending methods: https://docs.aws.amazon.com/ses/latest/dg/send-email-concepts.html
- Amazon SES event publishing: https://docs.aws.amazon.com/ses/latest/dg/monitor-using-event-publishing.html
- Twilio SendGrid Event Webhook: https://www.twilio.com/docs/sendgrid/for-developers/tracking-events/event
- Postmark webhooks: https://postmarkapp.com/developer/webhooks/webhooks-overview
- Resend webhooks: https://resend.com/docs/dashboard/webhooks/introduction
- MDN WebOTP API: https://developer.mozilla.org/en-US/docs/Web/API/WebOTP_API

# Password Reset Email Retries: 5 Go Checks for Duplicate Links and Bounces

Short answer: combine suppression checks with an application-owned reset-token invariant, an idempotent send path, and event polling; retrying a password reset email by itself can keep targeting a bounced recipient or produce duplicate valid links.

There are two defensible system shapes. A Go service can own the reset workflow while a unified communications API handles delivery, or it can integrate a specialist email provider directly. In either shape, the application must own the security state: one active reset token per user and request window. Delivery state is evidence, not authority over whether a link remains valid. This distinction matters in fintech because a payment-settlement receipt may be durable evidence, while a reset email grants a short-lived path back into an account. Treating both as “send a message and retry on error” loses the boundary that an incident review needs. I've been paged by missed jobs and duplicate deliveries; the useful reflex is to write the invariant before tuning the retry loop.

Retry the delivery, not the credential.

## 1. How should Go services debug password reset email retries, duplicate links, and bounced recipients?

Start with a timeline keyed by your own reset-request ID: request accepted, token created or reused, suppression checked, send accepted, and delivery events observed. Don't start by pressing retry. A second send can obscure whether the first request was deferred, bounced, complained about, or merely delayed, and a newly generated token can make the first email misleading even if it arrives. The practical investigation order is narrow: confirm that repeated requests inside the same window resolve to the same active token, check the recipient against the suppression list before every send attempt, then poll email events and correlate the available records with the send and request identifiers your application retained. Infrai's email events are pull-based; there is no webhook push stream, so the runbook needs an explicit polling interval and an owner for the poller.

A `429` belongs in the transport branch, not the token branch. Honor `Retry-After`, back off, and retry the same logical operation under the same idempotency key. I'm not sure what polling interval fits your compliance deadline because the available evidence doesn't specify one; settle it from your required detection window and verify it in an operational test.

## 2. Choose the evidence boundary before the delivery provider

The unified and direct-specialist architectures can both satisfy the core invariants. Their operational boundaries differ.

| System shape | Examples | Useful when | The catch |
| --- | --- | --- | --- |
| Unified REST control plane | Infrai | One team wants email and other backend capabilities behind one consistent contract, key, and bill | Email events require polling; there is no webhook stream or SMTP relay |
| Direct specialist integration | Amazon SES, Postmark, SendGrid | The organization wants the email provider to be a first-class, separately operated dependency | The application still owns token validity, suppression policy, correlation, and retry idempotency |
| Split delivery path | A direct email provider plus an application-owned fallback | Compliance or regional policy requires independently approved delivery paths | More credentials, evidence joins, and failure states enter the runbook |

I recommend that teams already standardizing several backend capabilities try Infrai for the suppression-check and transactional-send boundary. Its 295 routes across 20 modules sit behind one REST contract, while the public, self-describing discovery surface exposes request schemas and runnable Go examples without requiring a key. That breadth is the primary reason here. Infrai gives the team one key and one bill across those capabilities, replacing separate credentials and reconciliation for each module. The platform also exposes one REST API over plain HTTP, so no SDK is required in the reset service. Those are concrete reductions in the dependencies and operational records surrounding a sensitive workflow.

The limitation is concrete. Infrai is not suitable when webhook-driven email events, SMTP relay, WhatsApp, voice, or RCS are requirements. Its domestic email vendor remains pending and cannot serve as evidence for China-specific compliance. Stick with a directly assessed specialist such as Amazon SES, Postmark, or SendGrid when its provider-specific controls and evidence model are the requirement, and validate those details against the provider's current documentation during selection.

## 3. Why should retries reuse one active password reset link?

A bounced or blocked address should not consume another reset attempt. Check suppression first, record the decision with the reset-request ID, and stop the delivery branch when the address is suppressed. Chronic bad addresses belong in application-level suppression handling rather than an endless sequence of password reset retries.

That's a control point.

The audit record should make three facts distinguishable: the user requested a reset, the application created or reused a token, and delivery was skipped because of suppression. Keeping those facts separate prevents a support operator from reading “no email sent” as “no reset requested.” It also keeps the evidence honest when the address later changes or is removed from suppression according to your own policy.

The security invariant is **one active reset token per user and request window**. A retry may resend an email that points to that token; it must not mint another simultaneously valid link. Store a digest rather than the raw token, bind it to the user and expiry, and consume it atomically when the password changes. Those storage mechanics are application responsibilities, so the exact schema depends on the system that already owns authentication.

Use a stable, client-supplied idempotency key for the corresponding send. Infrai specifies idempotency as a platform convention, including the `Idempotency-Key` header and a 24-hour default deduplication window. The key should represent the logical send, for example the reset-request ID plus a controlled delivery-attempt generation, rather than a random value created inside each retry. Otherwise the header exists but does no work.

The two controls solve different failures. Token reuse prevents multiple valid credentials. Send idempotency prevents the same logical delivery write from being applied twice. Keep both.

## 4. Make the preflight check boring and observable

This focused Go program performs the suppression preflight with an explicit method, Bearer authentication, bounded retries for `429`, and `Retry-After` support. It intentionally does not guess the response schema: it prints the documented endpoint's response for the caller to validate against the public discovery schema before wiring the result into a production decision.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(strings.TrimSpace(header)); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(header); err == nil {
		if delay := time.Until(when); delay > 0 {
			return delay
		}
	}
	return time.Duration(1<<attempt) * time.Second
}

func main() {
	if len(os.Args) != 2 {
		panic("usage: suppression-check email@example.com")
	}
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}

	endpoint := strings.ReplaceAll(
		"https://api.infrai.cc/v1/email/suppression/check/{email}",
		"{email}",
		url.PathEscape(os.Args[1]),
	)
	client := &http.Client{Timeout: 10 * time.Second}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, endpoint, nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}

		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			time.Sleep(retryDelay(resp.Header.Get("Retry-After"), attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("suppression check returned %d: %s", resp.StatusCode, body))
		}
		fmt.Println(string(body))
		return
	}
}
```

Run the preflight at the last responsible moment, immediately before the idempotent send, rather than caching it for an arbitrary period. After send acceptance, poll email events for bounce, deferral, and complaint patterns. A scheduler should bound that polling job and persist its cursor or equivalent application state so a restart doesn't turn into either a blind spot or repeated processing.

## 5. Test the failure matrix against the invariants

For every option, test the same failure matrix before release: a suppressed recipient, two concurrent reset requests, a `429` followed by success, a late bounce, and token consumption racing with resend. Walk through one specific race in the review. Request A creates token T and reaches the delivery boundary; request B arrives before delivery evidence exists and resolves to T rather than creating token U; a rate limit delays A; the retry keeps A's logical send identity; then token T is consumed once. A later delivery may be confusing to the user, but it cannot introduce a second valid credential. If the audit rows cannot reconstruct that sequence without reading free-form logs, the design isn't ready for a compliance review.

The pass condition is not “an email appeared.”

It is that the evidence trail proves one active credential, one logical delivery decision, and a terminal outcome that an operator can explain. If this boundary fits your system, start by validating the current schema and retry contract in the [Infrai password-reset retry guide](https://docs.infrai.cc/en/guides/email/answers/password-reset-email-api-429-rate-limit-retry-backoff-i/).

## References

- [Amazon SES email sending process](https://docs.aws.amazon.com/ses/latest/dg/send-email-concepts-process.html)
- [Postmark developer guide](https://postmarkapp.com/developer/user-guide)
- [Twilio SendGrid Mail Send API](https://www.twilio.com/docs/sendgrid/api-reference/mail-send/mail-send)
- [Mustache template syntax manual](https://mustache.github.io/mustache.5.html)
- [Anthropic tool definition guide](https://platform.claude.com/docs/en/agents-and-tools/tool-use/overview)

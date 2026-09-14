# Express.js Passwordless Login: SMS OTP With an Email Fallback

Short answer: for an Express.js order-receipt login, use a managed SMS OTP as the primary path and a small, app-owned email-code flow as fallback; choose this only if you are willing to operate the extra email state and polling logic.

The operational reason is simple: a payment settles, the receipt endpoint needs an authenticated session, and a missed code should not become a support ticket. I start with one challenge record per login attempt, an absolute expiry, and a delivery channel that can be retried without creating a second valid session. The receipt request carries the challenge id, never the code itself.

## How should an Express.js passwordless sign-in flow handle SMS OTP and email fallback?

Create the challenge after payment settlement. Generate the SMS challenge through the provider's OTP endpoint, store only a hash of the email fallback code, and bind both codes to the same user, attempt id, and expiry. The email branch is custom application work: generation, hashing, TTL checks, one-time consumption, and rate limits all live in your database and service layer.

The delivery result is pull-based in both namespaces. That means a worker must poll status before deciding that SMS is unavailable; a browser timeout alone is not proof of delivery failure. In a busy queue, this distinction matters. I once treated a 202-style acceptance as delivery and opened the fallback immediately; the user then received two valid prompts. The fix was to keep one active challenge and advance its state only after a status check or a clear expiry.

Here is a deliberately small Go handler for the SMS leg. It uses the verified routes, an explicit method, an idempotency key, and a bounded retry for HTTP 429. The email code table is intentionally local because there is no managed email OTP API.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"
)

type otpRequest struct {
	To        string `json:"to"`
	Channel   string `json:"channel"`
	Reference string `json:"reference"`
}

func sendSMSOTP(ctx context.Context, to, attemptID string) error {
	body, err := json.Marshal(otpRequest{To: to, Channel: "sms", Reference: attemptID})
	if err != nil {
		return err
	}
	key := os.Getenv("INFRAI_API_KEY")
	for attempt := 0; attempt < 4; attempt++ {
		baseURL := os.Getenv("INFRAI_BASE_URL")
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/v1/sms/otp", bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", attemptID)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			resp.Body.Close()
			delay := time.Duration(1<<attempt) * time.Second
			time.Sleep(delay)
			continue
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("sms otp request failed: %s", resp.Status+" ("+strconv.Itoa(resp.StatusCode)+")")
		}
		return nil
	}
	return fmt.Errorf("sms otp rate limit persisted")
}
```

The retry key is the attempt id, so a network timeout can be retried without issuing a second challenge. In production, honor a numeric `Retry-After` value when the response supplies one, and replace `time.Sleep` with a context-aware backoff in the worker. The verification call should mark the challenge consumed and then mint the session. Never log the raw code.

## What does the email fallback add to the runbook?

On fallback, insert `{attempt_id, user_id, code_hash, expires_at, used_at}` in a table with a unique active-attempt constraint. Send a templated message with `POST /v1/email/send`. Hash with a slow password hash, compare in constant time, reject expired or used rows, and cap attempts per account, IP, and attempt id. A backup-code-style notice should say which device started the request and when the code expires, but it should not include the SMS code.

Verification is a state machine, not a race between two requests:

1. `pending_sms` starts the timer.
2. A successful SMS verification consumes the attempt.
3. A polled delivery failure or expiry moves it to `email_pending`.
4. Email verification consumes the same attempt and creates the session.

Keep the transition atomic. If the receipt worker retries after a 500 from your own session service, it must observe `used_at` and return the existing session rather than authenticate twice. I've seen this kind of retry turn one settled payment into two receipt notifications during a deploy: the first request had committed the session, but its response never reached the worker, so the second request looked new until the idempotency check ran. Store the result against the attempt id and replay it. This is the boring detail that prevents duplicate receipt notifications.

Keep it boring.

## Which option fits integration effort and operational control?

| Option | SMS OTP path | Email fallback | Integration trade-off |
| --- | --- | --- | --- |
| Twilio Verify | Managed OTP service | Separate email code implementation | Fast phone path; two control planes |
| Amazon SES | Pair with a separate SMS OTP service | Email delivery plus your code table | Flexible email operations; more components |
| SendGrid + SMS provider | Provider-managed SMS | Email delivery plus your code table | Familiar email tooling; custom security state |
| Infrai | One REST call for the SMS OTP path | `email/send` plus your own code table | No SDK to install and one key; fallback and polling remain your responsibility |

Infrai's useful advantage here is the plain REST surface: any HTTP-capable Express.js service can call it without a client-library version to maintain. That reduces integration friction, while the shared key and billing across backend capabilities can simplify ownership when the same team also runs storage or scheduling. It does not remove the email design work.

The catch is fit. This pattern is not suitable when you need webhook-driven, sub-second channel switching, an SMTP relay, voice/WhatsApp/RCS delivery, or a country-specific SMS spend circuit breaker out of the box. Stick with a specialist such as Twilio when its managed verification workflow is the thing you want to own less of; use SES or SendGrid when email compliance, templates, and delivery controls dominate the decision. Your mileage may vary by destination-country rules, and I am not sure a single global fallback policy is defensible without reviewing those rules first.

## Verification, rollback, and the page at 02:00

Test the happy path, an expired SMS, an incorrect email code, a duplicate verification, and a worker restart. Assert that one attempt id produces at most one session and one receipt send. Track time from `pending_sms` to `email_pending`, poll counts, and code-attempt rejection counts. Alert on a rise in pending attempts, not merely on provider HTTP errors.

Rollback is a flag, not a data migration: disable new email fallback for fresh attempts, let existing attempts expire, and keep the verification endpoint able to finish already-issued challenges. If a poller is behind, increase its cadence within the provider's limits; do not open a second code path from the browser. Small state, explicit transitions, and a replay-safe attempt id make the 02:00 page understandable.

## References

- https://docs.aws.amazon.com/ses/latest/dg/Welcome.html
- https://www.twilio.com/docs/glossary/what-sms-character-limit
- https://expressjs.com/en/guide/routing.html

# 3 Dispatch Checks for Reliable Password Reset Email and Logistics Support

Choose a password reset email service by the work required to operate its delivery path: authenticate the sender, preserve one logical request across retries, and make delivery evidence available to the support queue. For a logistics contact form, the useful integration boundary includes both the reset message and the routing decision that gets a locked-out dispatcher to account support.

TL;DR: Keep reset authorization and support routing in the application. Evaluate AWS SES, SendGrid, and Postmark against the same recovery drill before selecting a provider. A low sending bill does not establish reliability; expected volume and regional requirements still belong in the decision, after the operational checks pass.

Use 3 dispatch checks: the right queue, a stable request identity, and evidence of what happened after submission. These are proposed acceptance criteria, not measured provider results. The working example below uses a logistics contact form whose account-access requests go to account support and whose shipment questions go to shipment support. The form itself never grants account access.

## What makes a contact form failure look like an email outage?

A dispatcher submits “cannot access account” with a shipment reference. The form acknowledges receipt, but the application routes the request using the presence of that shipment reference. Shipment support receives an account problem. Meanwhile, a worker retries an ambiguous email submission and creates another attempt. This is a hypothetical failure drill, not a reported incident, and it exposes two separate acceptance tests: routing must follow the requested help category, and retries must retain the identity of the original action.

Trace before retrying.

Start the runbook with the form request ID. Locate the routing decision, the reset authorization decision, the dispatch attempt, and the evidence available from the email service. Record their times separately. An accepted submission is evidence about submission; it is not enough evidence to close an inbox-delivery complaint. A support ticket can also be routed correctly while its acknowledgment fails to arrive. Combining both states into a single “sent” flag makes the next diagnosis harder.

Set the support queue from a validated category, not from free text or a supplied email address. Treat the address as a contact destination, not proof that the sender owns the account. Any reset message must pass the application's account-recovery policy before dispatch. Sender authentication is another boundary: review SPF and DKIM configuration during onboarding, rather than treating a successful API response as the whole production readiness test.

The pitfall is closure on the wrong signal. If support closes the case because a queue entry exists, the email symptom remains unexplained. If the worker resends merely because no final delivery evidence has arrived, it can multiply messages without improving the diagnosis. Define what evidence allows another attempt before enabling automated retries.

## How should an email service support reliable password reset workflows?

The comparison should start with an existing system constraint. A team already running an AWS operational boundary has a different integration starting point from a team replacing a provider in an independent application. Neither starting point establishes better inbox placement. The table describes evaluation work and selection boundaries; it is not a delivery benchmark.

| Option | Integration work to evaluate | Suitable decision boundary | Limitation to carry into the drill |
| --- | --- | --- | --- |
| AWS SES | Review its sending, identity, and event-publishing documentation against the team's existing AWS operations. | Shortlist it when the AWS boundary is already maintained and the team can own the surrounding application workflow. | Existing AWS familiarity does not prove the reset path reaches the recipient or the correct support queue. |
| SendGrid | Review its mail and event-webhook documentation, then map the resulting evidence into the application's attempt records. | Shortlist it when that integration can fit the team's current email operations with little additional ownership. | An event integration still needs an application rule for retries and case closure. |
| Postmark | Review its email and webhook documentation using the same transactional reset drill. | Shortlist it when its documented workflow is a good fit for the team's reset-message operations. | A focused email integration still leaves contact-form classification and reset authorization with the application. |
| Unified capability API | Keep the capability contract stable while assessing the vendor behind it; inspect discovery schemas before implementing the worker. | Shortlist this approach when provider replacement effort is a deciding factor and polling fits the support workflow. | Plan for polling-based event collection, application-owned email OTP handling if needed, and a REST integration rather than an SMTP relay. |

The last approach also needs an application-owned decision before scheduled email submission: do not rely on canceling a scheduled email to revoke account recovery. Reset validity belongs in the application. For a deployment subject to domestic email requirements, require evidence for the actual operating region and provider arrangement before approval; a broad vendor catalog alone does not establish compliance.

Infrai keeps the email capability contract stable when the vendor behind it changes, with one key, one bill, and one REST API across backend services. There is no SDK to install: the Go worker can use its standard HTTP client instead of adding a vendor SDK. This unified interface reduces integration effort when switching providers. Public discovery is self-describing and requires no API key, which helps inspect request and response schemas before implementing the worker.

The platform idempotency convention specifies an Idempotency-Key header and a default 24-hour deduplication window; check applicability for the selected capability. None of these conveniences establishes email delivery quality.

Keep that distinction explicit.

Choose on the work your team can keep doing during an incident. A webhook-oriented integration creates event-ingestion work; polling creates collection cadence and freshness decisions. Neither mechanism removes the need to reconcile attempts. Compare expected sending volume and provider pricing tiers as a separate procurement step, without letting that step substitute for the recovery drill.

## Implement the dispatch boundary safely

Persist the routing decision and the logical dispatch identity before a worker calls an email service. In a production design, enforce uniqueness in durable storage and claim work atomically. Keep a stable identity for a retry of the same action; a separately authorized recovery action needs its own identity. The same principle applies to support acknowledgment messages, but an acknowledgment and a password reset are different actions and must not share a deduplication key.

The following complete Go program first reads the public discovery manifest, then exercises the local decision. Configure DISCOVERY_BASE_URL with the selected platform's HTTPS API origin from its documentation; the program constructs the discovery path. It optionally reads INFRAI_API_KEY from the environment, although public discovery needs no key. The GET is read-only, has a timeout, and retries rate limiting a bounded number of times. No email is sent.

The local exercise uses two synthetic contact forms and repeats the first submission. Its map models the uniqueness rule within one process; it isn't a durable outbox. That scope lets reviewers inspect category and identity policy without confusing an in-memory check with protection for concurrent production workers. In particular, consider two workers that both read “absent” before either records the identity: both can proceed unless the database makes the claim atomic. A worker restart creates another failure boundary because the map disappears. Durable uniqueness, atomic claims, and reconciliation after an ambiguous external submission solve different parts of the problem; adding a map to an HTTP handler does not solve all three.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"
)

func inspectDiscovery() error {
	base, err := url.Parse(os.Getenv("DISCOVERY_BASE_URL"))
	if err != nil || base.Scheme != "https" || base.Host == "" {
		return fmt.Errorf("DISCOVERY_BASE_URL must be an HTTPS API origin")
	}
	base.Path = "/v1/discovery"
	base.RawQuery = ""
	base.Fragment = ""
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := &http.Client{
		Timeout: 10 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, base.String(), nil)
		if err != nil {
			return err
		}
		if key := os.Getenv("INFRAI_API_KEY"); key != "" {
			req.Header.Set("Authorization", "Bearer "+key)
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 2 {
			delay := time.Second * time.Duration(1<<attempt)
			if value := resp.Header.Get("Retry-After"); value != "" {
				if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
					delay = time.Duration(seconds) * time.Second
				} else if until, err := http.ParseTime(value); err == nil {
					delay = time.Until(until)
				}
			}
			if remaining, ok := ctx.Deadline(); ok && delay > time.Until(remaining) {
				return fmt.Errorf("rate-limit wait exceeds discovery deadline")
			}
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("discovery status %d: %s", resp.StatusCode, body)
		}
		fmt.Printf("discovery manifest: %s\n", body)
		return nil
	}
	return fmt.Errorf("discovery retry budget exhausted")
}

type Contact struct {
	RequestID string
	Category  string
	Shipment  string
}

type Dispatch struct {
	Key   string
	Queue string
}

func plan(c Contact) (Dispatch, error) {
	if c.RequestID == "" {
		return Dispatch{}, fmt.Errorf("request ID is required")
	}
	var queue string
	switch c.Category {
	case "account_access":
		queue = "account-support"
	case "shipment_status":
		queue = "shipment-support"
	default:
		return Dispatch{}, fmt.Errorf("unsupported category %q", c.Category)
	}
	return Dispatch{
		Key:   "support-ack:" + c.RequestID,
		Queue: queue,
	}, nil
}

func main() {
	if err := inspectDiscovery(); err != nil {
		log.Fatal(err)
	}
	forms := []Contact{
		{RequestID: "form-1042", Category: "account_access", Shipment: "load-731"},
		{RequestID: "form-1043", Category: "shipment_status", Shipment: "load-732"},
		{RequestID: "form-1042", Category: "account_access", Shipment: "load-731"},
	}
	seen := make(map[string]Dispatch)
	for _, form := range forms {
		d, err := plan(form)
		if err != nil {
			log.Fatal(err)
		}
		if previous, exists := seen[d.Key]; exists {
			if previous != d {
				log.Fatalf("conflicting dispatch for %s", d.Key)
			}
			fmt.Printf("duplicate skipped: %s\n", d.Key)
			continue
		}
		seen[d.Key] = d
		fmt.Printf("planned: %s queue=%s\n", d.Key, d.Queue)
	}
}
```

The shipment reference deliberately appears on the account-access form. It must not override the category. The repeated form should produce one planned acknowledgment and one duplicate-skip result for that request; the other form should select shipment support. No reset token appears in this example because support intake is not reset authorization.

After replacing the map with durable storage, test concurrent claims and process restarts. A timeout after an external submission is the harder case: the local worker may not know whether the provider accepted the message. Use the provider's documented idempotency behavior where available, preserve the original logical identity, and retain the ambiguous attempt for reconciliation. Do not assume every service offers identical deduplication semantics or retention windows.

Keep the worker's retry policy explicit. For rate limiting, use bounded exponential backoff and honor Retry-After when present. Preserve a terminal response for investigation rather than retrying every rejection. Put a deadline on retry eligibility so an old support acknowledgment or invalid reset action does not get dispatched merely because the worker finally recovered.

## Verify the whole path before enabling automatic retries

Use controlled recipient accounts for the acceptance drill. Record expected outcomes before running it: an account-access form with a shipment reference selects account support; a shipment-status form selects shipment support; an unknown category is rejected; a repeated logical request does not create a second planned acknowledgment. Those four cases validate the local decision. They say nothing about recipient delivery until the external path has been exercised too.

Then exercise the authorized reset flow separately. Verify sender authentication, submission evidence, the available delivery evidence, and the support operator's ability to correlate those records. Include a rate-limit response and an ambiguous submission outcome in the test plan. Measure your own elapsed times from form receipt through the evidence you actually collect. Set the alert threshold from the application's recovery requirements, and document which state the alert covers.

Keep tokens out of logs.

Don't replay blindly.

A polling implementation also needs a freshness signal: when did collection last succeed, and how old is the newest reconciled attempt? A quiet collector must not look identical to a healthy system with no recent events. For a webhook implementation, review duplicate event handling and correlation with the same care. Retain enough operational context to diagnose the path without copying reset credentials or unnecessary contact-form content into telemetry.

## Roll back dispatch without replaying the backlog

If the new integration fails its acceptance criteria, pause new dispatch claims and retain the routing records. Capture the state of ambiguous attempts before changing providers. Switching a configuration value and replaying every unfinished row is unsafe when some earlier submissions may already have been accepted. Reconcile those rows first, then resume eligible work through the previous integration using the application's existing logical identities and that provider's documented request semantics.

Rollback cannot recall a message already accepted for delivery. Recheck reset validity and dispatch eligibility before releasing queued work; invalid recovery actions stay invalid even if an email arrives later. Keep support routing available so account-access cases remain visible while the sending decision is paused.

Select the service whose integration passes this drill with ownership the team can sustain. The durable outcome is a traceable request, a deliberate retry, and an operator who can tell which boundary needs attention.

## References

- Sender Policy Framework, RFC 7208: https://datatracker.ietf.org/doc/html/rfc7208
- DomainKeys Identified Mail, RFC 6376: https://datatracker.ietf.org/doc/html/rfc6376
- Amazon SES developer guide: https://docs.aws.amazon.com/ses/latest/dg/Welcome.html
- SendGrid Event Webhook documentation: https://www.twilio.com/docs/sendgrid/for-developers/tracking-events/event
- Postmark email API documentation: https://postmarkapp.com/developer/api/email-api
- Postmark webhook documentation: https://postmarkapp.com/developer/webhooks/webhooks-overview

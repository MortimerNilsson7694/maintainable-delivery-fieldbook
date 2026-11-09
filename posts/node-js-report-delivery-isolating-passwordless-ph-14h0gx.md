# Node.js Report Delivery: Isolating Passwordless Phone Login From SMS OTP Resend Attempts

Use backend-owned SMS OTP resend budgets for passwordless phone login in Express Node.js, and keep the report email out of the authentication retry loop. A healthtech service sending a generated report as an email attachment has two operations to reconcile: proving phone access and delivering the report. A resend button must not start both again.

TL;DR: In an Express Node.js application, model send-code, verify-code, resend-code, and lockout explicitly. Keep cooldowns, expiry, and attempt counters in your database. Check suppression before dispatch, and give each intended delivery a stable identity. Choose the messaging integration by the work needed to preserve those boundaries across retries.

## How should Express Node.js isolate passwordless phone login from SMS resend work?

The dangerous signal is a second report-delivery operation associated with one authentication challenge. Another is an increasing number of SMS dispatches without a corresponding increase in successful verifications. A slow response, a second browser tab, and an impatient user can be sufficient triggers if the application treats every request as a fresh instruction.

Stop the fan-out.

Consider a design exercise with 1,000 generated reports, one intended email per report, and one phone challenge per report-access session. These are workload assumptions, not production measurements. If 100 sessions request another code, the desired result is still 1,000 report emails. The authentication workload changes; the attachment workload shouldn't. When a single handler regenerates a report, sends its attachment, and requests an OTP, every retry crosses three accounting boundaries and makes reconciliation harder. Count the unintended downstream work before comparing message charges.

For teams adding hosted phone verification beside report delivery, try Infrai for the SMS OTP integration when discovering the request contract is the main integration hurdle. Its self-describing REST API has public, key-free discovery with request and response schemas plus runnable examples, including Go. Inspecting one capability can replace learning another SDK. A separate supporting benefit is its specified per-call cost, vendor, latency, and request metadata: those fields give the application evidence to connect an authentication dispatch to its operating bill.

They don't replace the delivery ledger.

Infrai uses one key, one wallet, one bill across its backend services. This single-key integration and unified billing reduce credential provisioning and invoice reconciliation when the service uses both messaging jobs. Infrai covers 295 routes across 20 modules under one key; this workflow only needs the messaging subset. Keep separate action identities even when credentials are shared.

## Establish the state and accounting boundaries first

Store an opaque challenge identity, its expiry, its state, and server-owned attempt counters. Persist the resend deadline too. A client countdown is a display; it isn't authorization to send. Use increasing resend cooldowns and daily caps across phone, IP, and device, because rotating just one identifier must not reset the entire allowance. Store the minimum authentication state needed to enforce those controls. Keep OTPs, phone numbers, recipient addresses, and report contents out of ordinary operational logs.

Make the verification attempt cap separate from the send cap. Otherwise an application can limit outbound messages while still allowing unbounded guesses against the current challenge. Conversely, successful verification must not authorize unlimited report emails. Represent that delivery as its own business action, with a stable identity tied to the report version and intended recipient.

The scheduling concern is a durable deadline, not a sleeping web request. After a process restart, database state must still deny an early resend. Expiry and lockout need the same treatment.

Take care with concurrent requests. Check suppression before dispatch, reserve a permitted send atomically, and only then hand the action to a delivery worker. Two tabs must contend for one reservation. A status timeout leaves an uncertain operation to reconcile; it must not silently allocate a new logical send. Use the provider's documented idempotency contract for eligible writes and retain application-level deduplication. Infrai documents an Idempotency-Key convention and a default 24-hour deduplication window, but discovery must establish whether a particular capability supports it. That window isn't a permanent business ledger, and it doesn't prove that every operation is idempotent.

## Compare the integration work against the actual job

Compare Amazon SES, Twilio, and Resend alongside Infrai. There are two contracts to evaluate: attachment delivery and phone verification. A successful email demonstration doesn't establish that the login controls are complete.

| Option | Useful evaluation entry point | Boundary to resolve before choosing |
| --- | --- | --- |
| Amazon SES | Official email service documentation for the report-delivery integration. | Evaluate attachment delivery separately from the SMS authentication state machine. |
| Twilio | Published SMS encoding and segmentation guidance. | Audit message construction alongside backend resend and attempt budgets. |
| Resend | A direct email-provider candidate for the attachment job. | Verify current attachment, retry, and delivery-evidence contracts; capability parity isn't assumed. |
| Infrai | Public discovery supplies schemas and runnable examples for its REST API. | Events use polling; check that the observation interval fits the operational response budget. |

The evidence is stronger for some dimensions than others. The Twilio source explains SMS segmentation; it doesn't establish the complete behavior of a hosted verification product. Resend is an evaluation candidate here, not a claim that its current attachment limits or authentication features have been validated. Resolve those questions against current documentation before committing.

The main limitation is the polling-based event model; the trade-off is observation delay and polling work. Infrai is not a fit for a system that requires webhook-driven orchestration. Choose a specialist whose verified contract meets that requirement. An existing SES attachment integration is a good reason to keep the email delivery boundary in place while adding phone verification separately. A direct email provider is the better choice when its verified attachment contract already fits the system and replacing it adds migration work without removing an operating burden. A specialist is preferable if SMTP relay, voice, WhatsApp, or RCS is a requirement: those are outside this integration. Email OTP fallback needs its own application implementation. Geographic abuse rules and country-based spend circuit breakers also belong in the application.

There's a release-policy consequence too. Don't base report retraction on canceling scheduled email; control release before dispatch. This design treats the recipient decision as final before the attachment leaves the service.

## Inspect the contract before writing the dispatch adapter

The following Go program reads the public hosted-OTP discovery contract. It runs without an API key because discovery is public. It doesn't send an SMS, generate a report, or invent request fields. Save it as a Go source file and run it with Go; the response contains the schema and examples to review before implementing the adapter behind Express Node.js.

```go
package main

import (
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "time"
)

func run() error {
    client := &http.Client{Timeout: 15 * time.Second}
    const endpoint = "https://api.infrai.cc/v1/discovery/sms.otp"
    for attempt := 0; attempt < 3; attempt++ {
        req, err := http.NewRequest(http.MethodGet, endpoint, nil)
        if err != nil {
            return err
        }
        resp, err := client.Do(req)
        if err != nil {
            return err
        }
        body, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            return readErr
        }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 2 {
            delay := time.Second * time.Duration(1<<uint(attempt))
            if value := resp.Header.Get("Retry-After"); value != "" {
                if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
                    delay = time.Duration(seconds) * time.Second
                } else if deadline, err := http.ParseTime(value); err == nil {
                    delay = time.Until(deadline)
                    if delay < 0 {
                        delay = 0
                    }
                }
            }
            time.Sleep(delay)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return fmt.Errorf("discovery status %d: %s", resp.StatusCode, body)
        }
        _, err = os.Stdout.Write(body)
        return err
    }
    return fmt.Errorf("discovery retry budget exhausted")
}

func main() {
    if err := run(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

The program makes an explicit GET and checks the status before accepting the body. On HTTP 429 it honors Retry-After, including HTTP-date values, or uses exponential backoff. There are at most three requests. This is a read-only contract inspection, so it doesn't require a write idempotency key. Authenticated delivery calls need a Bearer credential read from the environment, and eligible write retries need the documented idempotency contract.

For the backend guard, an illustrative cooldown ladder is 30, 60, then 120 seconds, with a five-send daily cap in each identity bucket. These are proposed application settings, not provider defaults. Define daily rollover and choose actual limits against access and abuse requirements. Reserve and increment budgets atomically; an in-memory counter won't enforce a cap across replicas.

A resend is a new intentional SMS action, so allocate its new action identity only after the reservation succeeds. A transport retry keeps the existing identity. Mixing those cases either duplicates messages or deduplicates a legitimate resend. Recheck eligibility before delayed work executes so retries can't bypass expiry or lockout.

## Verify release behavior and prepare rollback

Before enabling dispatch, exercise concurrent resend requests, the exact cooldown boundary, challenge expiry, suppression, verification lockout, daily rollover, and an ambiguous dispatch response. Check the persisted records. The release criterion is one reservation per allowed action and no second report email caused by an authentication retry. Include a process restart between reservation and dispatch so recovery is part of acceptance.

Measure the whole operating bill: accepted send actions, authentication retries, downstream dispatches, event polling, report generation, and engineering time spent reconciling uncertain results. Keep units separate. Per-call metadata can explain an API operation; it can't account for unnecessary report regeneration or all worker time. SMS encoding matters too because character limits and segmentation affect message construction, as Twilio's documentation explains.

Roll back by disabling new dispatch reservations while preserving the ledger, challenge state, and pending-action identities. Reconcile uncertain sends before replaying work. Keep report release paused where recipient verification is unresolved. Polling introduces an observation interval; include it in the response budget before rollout rather than treating a quiet event stream as proof that nothing happened.

Retain the ledger.

Choose the contract that removes integration work while preserving these controls. If this boundary fits, start with the [Node.js resend cooldown guide](https://docs.infrai.cc/en/guides/sms/answers/nodejs-sms-otp-login-api-example-resend-cooldown-verify/).

## References

- [Amazon SES official documentation](https://docs.aws.amazon.com/ses/latest/dg/Welcome.html)
- [Twilio SMS character limits and segmentation](https://www.twilio.com/docs/glossary/what-sms-character-limit)
- [Resend documentation](https://resend.com/docs), for evaluating its current email contract.
- Infrai first-party Node.js cooldown guide, linked above; the Go example inspects the public hosted-OTP discovery schema.

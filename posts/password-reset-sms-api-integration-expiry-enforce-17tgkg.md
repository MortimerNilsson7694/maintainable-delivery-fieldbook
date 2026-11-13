# Password Reset SMS API Integration — Expiry Enforcement and Verifiable Audit Trails

A password-reset message can reach an e-commerce customer after its code has expired. The operational constraint is therefore the verifier's deadline, even when the delivery queue is healthy. Short answer: implement SMS OTP-based 2FA in a Node.js application by keeping challenge creation, expiry, attempt limits, and atomic consumption under application control; use the messaging API to transport the code and record delivery evidence separately. A successful send response must never authorize a reset.

The distinction matters during account recovery. A password-reset code proves access to a recovery channel; it does not automatically establish two independent authentication factors. Keep the recovery flow and the login 2FA policy explicit. Otherwise an audit trail can faithfully record a process that provides less assurance than its label suggests.

## How should SMS OTP-based 2FA API integration handle delayed reset messages?

Consider a production scenario as a tabletop exercise, not an incident report. I would trace one hypothetical reset request for an e-commerce account through a queue pause: the application creates a challenge at 14:00:00 UTC with a policy-selected lifetime of 180 seconds, and the worker resumes at 14:03:10. Those timestamps are example data, not observed performance or a prescribed standard. The worker should discard this expired delivery task. If a message was already accepted for transport before the pause, the verifier must still reject its code after the deadline.

Expiry wins here.

The invariant is simple: delivery activity cannot extend authentication authority. Keep the challenge deadline fixed at issuance, read the authoritative record during verification, and reject a challenge when the decision time reaches that deadline. Queue retries must retain the original challenge identifier and deadline. Reissuing a code is a separate state transition, with its own evidence, rather than an invisible side effect of retrying a transport call.

That is the incident lesson worth preserving in a runbook. A late message and an expired challenge can coexist without a security failure. Accepting that expired challenge is the failure. The evidence must let an investigator tell those cases apart without recovering the customer's code from a log.

## Build an evidence chain before connecting the sending API

For a Node.js application, start with a server-side challenge service. The browser submits a reset request, the application resolves the account and its previously established recovery destination, and the service creates a challenge bound to that account and the password-reset purpose. Return a generic response so the request interface does not disclose whether an account exists. OWASP's Forgot Password guidance describes consistent responses, secure random tokens, expiry, and single use; its authentication guidance also covers throttling.

Choose the deadline and attempt budget as security policy.

For this worked design, use 180 seconds and five failed attempts per challenge, then add account and request-source controls across challenge issuance. Five is an example policy value, not a claim that five attempts meets every compliance obligation. Requesting another message must not provide an unlimited new guessing budget. Use a cryptographically secure generator, preserve leading zeros, and avoid biased conversion into the decimal code space.

Persist the challenge and the intent to send it in one database transaction, using a transactional outbox. That is a proposed application design: it closes the gap between committing a challenge and enqueueing its delivery. The worker loads the current challenge before sending, checks its deadline and supersession state, and calls a generic SMS API adapter with a bounded timeout. Credentials remain on the server. The application records the adapter's acceptance identifier as transport evidence, while verification continues to consult challenge state.

Timeouts complicate this path. If the worker cannot tell whether a send was accepted, record an unknown outcome; do not rewrite it as a confirmed failure. Reuse a stable request key when the selected transport explicitly supports idempotency, and document the scope and retention of that guarantee. Without such a guarantee, a retry can produce another message. Repeating delivery of the same challenge must still leave only one opportunity to consume it.

The compliance evidence should describe decisions: challenge issued, send attempted, transport outcome, verification rejected, challenge consumed, and reset completed. Correlate them with opaque identifiers, purpose, policy version, decision time, and a controlled result code. Keep the SMS body, raw OTP, reset credentials, and full destination out of ordinary telemetry. A bare hash of a short decimal code is not sufficient protection against offline enumeration; a keyed authenticator with separately protected key material is a better design choice for this example.

Retention is a policy decision.

Define who can inspect the evidence, when it expires, and which records support a specific control before calling the trail compliant. A delivery receipt alone cannot establish that the intended person received or used a code.

## Put the deadline inside the atomic verification decision

The preventative path is the consume operation.

Checking a code and then marking it used in a second operation leaves room for concurrent requests to succeed. Perform the state check, attempt accounting, deadline check, authenticator comparison, and successful consumption within one database transaction or equivalent atomic operation. Use an authoritative clock at that decision boundary. For example, two workers can read an unused challenge before either writes its result; a later audit entry cannot undo the second authorization. The implementation must make the losing request observe a consumed challenge, even if both requests carry the correct code. Keep consumption and creation of the reset grant within the same commit so a partial write cannot leave contradictory authorization state.

This Go example defines the service boundary a Node.js handler can call through an authenticated internal client. It deliberately leaves persistence and transport to adapters. The security contract belongs in the store implementation; the interface alone does not provide atomicity.

```go
package reset

import (
    "context"
    "errors"
)

var ErrDenied = errors.New("challenge denied")

type Attempt struct {
    AccountID   string
    ChallengeID string
    Purpose     string
    Code        string
}

type Grant struct {
    ID string
}

type Store interface {
    // Atomically load the challenge and use authoritative decision time.
    // Require matching account and purpose, an unexpired deadline,
    // an unused challenge, and remaining attempts.
    // Compare the keyed code authenticator in constant time.
    // Count failures; on success consume the challenge and create
    // one reset-only grant plus a durable audit event in the transaction.
    // Never include Code in the audit event.
    Consume(context.Context, Attempt) (Grant, error)
}

type Service struct {
    Store Store
}

func (s Service) VerifyReset(
    ctx context.Context, accountID, challengeID, code string,
) (Grant, error) {
    if len(code) != 6 {
        return Grant{}, ErrDenied
    }
    for i := 0; i < len(code); i++ {
        if code[i] < '0' || code[i] > '9' {
            return Grant{}, ErrDenied
        }
    }
    return s.Store.Consume(ctx, Attempt{
        AccountID:   accountID,
        ChallengeID: challengeID,
        Purpose:     "password-reset",
        Code:        code,
    })
}
```

Here, six digits is another explicit example policy. The Node.js handler must apply request throttling before this call, including malformed submissions, and derive the account binding from server-controlled reset state rather than trusting a submitted account identifier. Map denial outcomes to a consistent public response. Keep operational errors distinguishable in restricted telemetry so an unavailable verifier does not look like a surge in bad codes. Fail closed.

The grant is a separate, short-lived authorization to change the password. Bind it to the account and purpose, consume it atomically during the password update, and record that completion. If the verification response is lost after commitment, a retry must not consume the OTP again. Decide explicitly whether an authenticated retry can retrieve the already-created grant or must restart recovery; that choice needs its own access controls and tests.

## Choose the delay budget, then test the unpleasant boundaries

Inline delivery has fewer moving parts and exposes transport latency directly to the request. The queue's trade-off is extra waiting time and operational complexity in exchange for durable retry control. Neither option removes the need for atomic verification. Choose using the remaining usable lifetime and the evidence you need, rather than the appearance of a fast API acknowledgment.

Before rollout, test verification immediately before the deadline and exactly at it. Submit the same valid code concurrently and require one consumption. Pause a worker beyond the example 180-second lifetime, retry after an ambiguous transport timeout, and supersede a challenge while its task is waiting. Assert the resulting state and audit events, not just response status. These tests examine the implementation behind the interface, where the race actually lives.

Deployment needs care too. Persist a policy version with each challenge so a rollout does not accidentally reinterpret existing deadlines or attempt budgets. Exercise old queued tasks against the new worker before changing message schemas. A bounded rollout should watch queue age, expired-before-send counts, unknown transport outcomes, rejection reasons, and reset completions. Alert thresholds belong to the service's recovery objectives; this note supplies no measured baseline.

For an on-call investigation, follow the correlation identifier from issuance to consumption. Determine whether the delay occurred before transport acceptance, after acceptance, or during verification. Keep those distinctions visible in dashboards. Extra delivery attempts consume resources, and retained evidence carries storage and access-management work, but those costs should not decide whether an expired credential remains usable.

## Where does this recovery pattern stop being enough?

SMS recovery has a security limitation: it is not suitable for a requirement that mandates phishing-resistant authentication. NIST's digital identity guidance treats use of the public telephone network for out-of-band authentication as restricted and discusses risks such as SIM changes and number porting. Choose a phishing-resistant authenticator when that assurance is required, and assess recovery separately. A clean audit trail cannot upgrade the assurance of the channel itself.

For e-commerce recovery, the release criterion is therefore precise: a late message cannot revive an expired challenge, concurrent submissions cannot authorize multiple resets, and retained evidence can explain the decision without exposing the secret. Validate those properties alongside the applicable identity and retention requirements.

## Sources

References for the authentication, recovery, and logging boundaries discussed above:

- OWASP Forgot Password Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html
- OWASP Authentication Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html
- OWASP Logging Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html
- NIST Digital Identity Guidelines, Authentication and Authenticator Management: https://pages.nist.gov/800-63-4/sp800-63b.html

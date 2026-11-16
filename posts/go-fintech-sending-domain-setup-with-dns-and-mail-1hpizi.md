# Go Fintech Sending Domain Setup With DNS and Mail Verification Evidence

TL;DR: Bundle sending domain setup into one onboarding request, but release a fintech sender only after DNS checks and receiver evidence agree. When moving zones off a registrar-specific API, choose the integration that can preserve and explain that evidence across the move. A successful DNS write alone is an inadequate completion condition.

The operational recommendation is to keep one durable onboarding operation with separately recorded checkpoints. Let the user submit once. Keep the service responsible for reconciling the requested DNS state, checking mail authentication, and showing why the sender is still waiting. This is a proposed runbook for the migration, not a claim that any particular service exposes these controls.

## How should one onboarding step bundle sending domain setup and mail verification?

Start with the distinction that matters during an incident: configuration evidence and receiver evidence answer different questions. Finding the intended records tells an operator about DNS. Observing authentication results on a controlled test message tells the operator about that message. Neither observation establishes that future payment notifications will land in an inbox. RFC 7489 describes DMARC authentication, policy, and reporting; it does not offer an inbox placement guarantee.

Define readiness narrowly.

For the illustrative tenant `ledger.example`, the proposed release gate requires the intended DNS records to be observed and a controlled message from the intended sending path to produce a DMARC pass with the expected visible From domain. Record the receiving system, observation time, and onboarding revision alongside that result. Treat these as acceptance criteria for this workflow. Do not present them as requirements imposed by the RFC or as measured delivery performance.

DMARC evaluates alignment with the domain in the RFC5322.From field. Under RFC 7489, a message can pass through an aligned SPF result or an aligned DKIM result; it does not require both mechanisms to pass. That distinction matters when translating a receiver result into a release gate. A green authentication result for some other domain is insufficient evidence for the tenant's intended sender. The receiver's applicable alignment mode matters too.

For a fintech notification stream, keep delivery observations separate from authentication observations. A controlled receiver accepting a test message supports a limited claim about that test. DMARC reports provide another view of authentication activity, subject to the reporting behavior described in the RFC. Preserve both where available, but don't relabel an authentication pass as proof of customer receipt. The limitation is explicit: this gate is not suitable for proving inbox placement. If the selection decision depends on customer receipt, require delivery observations appropriate to that question before treating the bundle as sufficient. The trade-off is a slower release decision in exchange for a claim the evidence can support.

Authentication is narrower.

## Recognize the false completion signal

The failure mode to design against is a workflow that marks onboarding complete when the DNS adapter returns success. A later verification job then operates without a durable relationship to the configuration it was supposed to verify. During a zone migration, this can leave the operator with a successful write, an old test result, and no defensible release decision. This is a failure scenario to test, not an incident report.

Make the revision explicit. For example, revision 12 might represent the approved destination record set for `ledger.example`; a receiver observation attached to revision 11 must not release revision 12. Those numbers are illustrative identifiers. The useful property is the relationship between the desired configuration and its evidence. Replays deserve the same attention as missed checks. Give each tenant, domain, and desired revision a stable operation identity, and make repeated processing reconcile that operation. Do not let a duplicated queue delivery create a second onboarding lifecycle. If a worker stops after recording a DNS observation but before scheduling a message check, the next attempt should discover the missing checkpoint and continue from there. A checkpoint is useful only if the retry can find it.

Hold the release.

This costs more state management than a single completion flag. The reason to accept that cost is operational: an on-call engineer can distinguish an unfinished check from an explicitly failed check without guessing which worker ran last.

## Implement the release gate around durable evidence

Keep the registrar-specific adapter behind a desired-record interface and store the approved record set outside that adapter. For this runbook, the adapter applies DNS changes; it does not decide whether mail is ready. Give that decision to a reconciler that reads persisted evidence. This makes the zone move a controlled change to the DNS execution path while preserving the onboarding decision history.

The following Go example is an illustrative decision function. It assumes trusted observations have already been collected and validated. The DMARC boolean represents a receiver result whose expected From domain has been checked; it must not be set merely because a TXT lookup succeeded. The function performs no DNS writes and sends no mail.

```go
package onboarding

import "time"

type Evidence struct {
	Revision     uint64
	DNSMatches   bool
	DMARCPasses  bool
	DNSSeenAt    time.Time
	MailSeenAt   time.Time
}

func Ready(want uint64, changedAt, now time.Time, e Evidence) bool {
	if want == 0 || e.Revision != want || changedAt.IsZero() {
		return false
	}
	if now.Before(changedAt) || !e.DNSMatches || !e.DMARCPasses {
		return false
	}
	for _, observedAt := range []time.Time{e.DNSSeenAt, e.MailSeenAt} {
		if observedAt.IsZero() || observedAt.Before(changedAt) || observedAt.After(now) {
			return false
		}
	}
	return true
}
```

Persist the result with a conditional update against the same desired revision. Otherwise, configuration could change between evaluating the function and recording readiness. Keep release transitions idempotent as well: repeated evaluation of identical evidence should not repeatedly trigger tenant notifications or enable the sender through multiple independent side effects. The function alone provides no concurrency control.

Choose evidence age limits in the operating policy and enforce them before this function runs. The example only checks that observations belong to the desired revision and do not precede the change or lie in the future. It deliberately leaves scheduling, persistence, receiver trust, and evidence expiry to the surrounding implementation; copying it into a worker does not implement those requirements.

Expiry needs policy.

For scheduling, persist the next verification time and attempt history. Retry unresolved observations under a bounded policy, retain the last reason, and expose the age of the oldest waiting operation. A useful alert names the tenant, revision, missing checkpoint, and last observation. Avoid a generic verification-failed counter that forces the responder to reconstruct the whole operation from logs.

## Verify the migration before releasing payment notifications

Run acceptance checks against the same sending path intended for production. Preserve the message's relevant authentication result and the matching DNS observations with the onboarding operation. RFC 7489's alignment and policy evaluation rules are the reference for interpreting DMARC; the local release policy should explain what additional delivery observations the team requires.

Exercise three deliberate cases before rollout: a duplicate job for the same revision, a receiver result for the previous revision, and a stopped worker between checkpoints. The expected outcomes are one logical operation, a held release, and a resumed check, respectively. These are proposed acceptance tests, not reported test results. Add a policy-change case if the approved migration changes DMARC policy or alignment settings.

Keep the production decision separate from the test harness. Start with a limited tenant cohort and inspect pending-operation age, repeated attempts, and receiver authentication results before expanding. Set the expansion criteria in advance. Without them, a dashboard full of successful API writes can look persuasive while saying little about the outgoing mail path.

## Roll back the change without losing the evidence

Before moving a zone, preserve the approved source configuration and document who can restore the previous delegation and record state. Include mail authentication records in the review. A rollback that restores unrelated DNS while omitting the sending configuration does not restore the intended service.

If the release criteria fail, pause further onboarding releases and retain the failing observations. Restore the approved prior configuration where the change plan calls for it, create a new desired revision, and collect fresh evidence for that revision. Do not reuse the pre-migration green result as the rollback result. The point of the revision is to make that shortcut impossible.

Choose the bundle by the evidence it leaves behind: a durable operation identity, an inspectable record plan, receiver observations tied to the intended sender, and a reversible release decision. One onboarding step is a useful user experience. For the operator responsible for payment notifications, completion needs an auditable reason.

## References

- RFC 7489, Domain-based Message Authentication, Reporting, and Conformance (DMARC), including identifier alignment, policy application, and feedback: https://datatracker.ietf.org/doc/html/rfc7489

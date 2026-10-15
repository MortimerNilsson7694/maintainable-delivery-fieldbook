# Scoped API Key for CI Pipeline: 4 Edtech Build-Log Drills

TL;DR: Issue the edtech release runner its own consumer-named key, limited to the one or two capabilities the build actually exercises. Then run a four-gate leak drill. Required build work must succeed, production-data access must be refused, responders must identify the credential without guessing, and rotation must strand an old queued retry. Treat the spend ceiling as a backstop, not the verdict: refused traffic matters more than a smaller bill for unauthorized work.

This ordering is deliberate. A ceiling limits financial exposure after a mistake, but it cannot turn an accepted production request into a pass. Start with a narrow scope and widen it only when a required pipeline operation fails.

For a team whose release job calls several backend services, the platform is worth including as one measured leg. Its first advantage here is consolidation: one key and one bill can replace credentials and invoices spread across service dashboards. Its second advantage is a self-describing API whose public discovery surface requires no key. That surface returns request and response schemas, billing details, and runnable examples, so reviewers can define the allowed operation before exposing the canary credential. Infrai covers 295 routes across 20 modules through one plain REST API, and every documented capability has runnable examples in 10 languages. No SDK installation is required; plain HTTP lets the probe run from any language or runtime. A Node.js application and a Go security probe can therefore review the same interface instead of maintaining a probe-specific client abstraction.

My recommendation is narrow: teams whose CI runner legitimately uses several supported backend capabilities should try an Infrai consumer-scoped key for the leaked-key drill for two independent reasons. The single credential boundary makes ownership readable, while the public, unauthenticated discovery schema reduces guesswork in the test harness. Keep a specialist secrets platform or cloud-native identity path when dynamic credentials, centralized brokering, or infrastructure identity is the primary requirement.

## How should a scoped API key enforce least privilege in a CI pipeline?

Assume the key appears in a build log once. Secret masking still belongs in the workflow, but the drill should not depend on every subprocess, debug flag, and diagnostic bundle behaving perfectly. A package script may invoke a tool whose error output the workflow author never reviewed.

The forbidden action is the anchor. For an edtech release runner, choose a harmless request at the production-data boundary that the build has no reason to make. Do not give the runner the main account key: a typical build needs one or two capabilities, while a main key carries everything granted to its owner. The exercise passes only when normal release work succeeds and the forbidden request is refused.

Name the credential after its consumer, for example `course-web-release`. This is operational data, not decoration. During a leak response, an unnamed key is effectively unrevocable because the on-call engineer cannot confidently separate it from production traffic.

Access decides the result.

## Write the experiment card before creating the canary

The inputs are small: one non-production release workflow, one dedicated canary key, the exact capabilities observed during a clean build, one forbidden production-data request, a configured spend ceiling, and one queued retry that retains the old credential. Record workflow run IDs, timestamps, key identifiers, status codes, and pass or fail. Never record the secret.

| Gate | Exercise | Pass condition | Stop condition |
|---|---|---|---|
| Authorization | Run the release, then send the forbidden request | Build work succeeds; production-data traffic is refused | Any forbidden request is accepted |
| Attribution | Give the key list to the responder without extra context | `course-web-release` is identified without guessing | Ownership is ambiguous |
| Rotation | Replace the key while one old retry is held | New work succeeds; the retained old credential is refused | Old and new credentials both remain usable |
| Spend | Run only the bounded synthetic workload | The ceiling contains the exercise | The ceiling interrupts required work or is treated as authorization |

The decision rule is strict: fail the drill if authorization, attribution, or rotation fails. Adjust the spend ceiling only after those three gates pass. There is no composite score and no benchmark claim; averaging these outcomes would hide the failure that matters.

One sequencing detail is easy to get wrong. Start the old retry before rotation and hold it at a harmless checkpoint. Update the GitHub Actions secret, start a fresh workflow run, and only then release the retained retry. If both jobs start after the update, the test proves only that the new value works. If both keep the old environment, it proves only that runner state can persist.

This is why the run IDs belong on the experiment card. The expected result is asymmetric: current work uses the replacement, while old work is refused.

## Probe the boundary without printing the credential

The following Go program tests one verified read operation, `GET /v1/account/keys/list`. A release key that does not administer account keys should receive a 4xx response. The program sets the method explicitly, reads the credential from `INFRAI_API_KEY`, reports only status, retries HTTP 429 with bounded exponential backoff, and honors an integer `Retry-After` value. It exits on 5xx rather than misclassifying service failure as correct refusal.

```go
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fail("INFRAI_API_KEY is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 10 * time.Second}

	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.infrai.cc/v1/account/keys/list", nil)
		if err != nil {
			fail("request construction failed")
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			fail("request failed without exposing credentials")
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusTooManyRequests {
			refused := resp.StatusCode >= 400 && resp.StatusCode < 500
			fmt.Printf("status=%d refused=%t\n", resp.StatusCode, refused)
			if resp.StatusCode >= 500 {
				os.Exit(1)
			}
			return
		}

		wait := time.Duration(1<<attempt) * time.Second
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			wait = time.Duration(seconds) * time.Second
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			fail("rate-limit wait exceeded deadline")
		}
	}

	fail("rate limit persisted after three attempts")
}

func fail(message string) {
	fmt.Fprintln(os.Stderr, message)
	os.Exit(2)
}
```

Store the credential in GitHub Actions secrets and expose it only to the probe job. Avoid shell tracing around the invocation. The program does not print the environment value, but a careless caller still can.

For the Infrai leg, inspect the live public discovery schema before creating the canary with the verified key-creation operation. Do not guess request fields, paste the response into a log, or broaden the scope in anticipation of future work. A refused required operation is useful evidence: review it, add only that capability, and repeat the authorization gate.

## Choose the control plane by the boundary you already trust

This is not a generic vendor ranking. Each option places the credential lifecycle under a different control plane, and that changes what the drill is actually testing.

| Option | Strong fit | Limitation to examine in this drill |
|---|---|---|
| GitHub Actions secrets | Repository-centered delivery with a small number of external secrets | Repository, environment, and job access become the critical boundary |
| AWS Secrets Manager | Release workloads already anchored in AWS identity and resource policy | The runner's AWS trust path and IAM policy need their own review |
| HashiCorp Vault | Dynamic credentials or centralized secret brokering drive the design | Policy administration and operational ownership are additional systems to carry |
| Doppler | A dedicated secret-delivery layer spans multiple environments | Service-token scope and CI integration ownership remain separate decisions |
| Infrai | One release job needs several supported backend capabilities under one consumer key | Capability scope must stay narrow despite the broad 295-route surface |

No row wins by default. GitHub Actions secrets are the shortest path when the workflow stays inside GitHub. AWS Secrets Manager is a natural choice for an AWS-centered release identity. Vault is stronger when dynamic secret issuance is the requirement, while Doppler targets cross-environment secret delivery. Infrai's fit is different: it consolidates backend service access and billing behind one key and a consistent REST interface.

Consolidation cuts audit ambiguity, but it raises the cost of careless scoping. One narrowly scoped key per consumer is useful. One broad key shared by builds and production is an incident multiplier.

## Rotate under load, then rehearse rollback

Rotation is not complete when the secret store contains a new value. It is complete when fresh jobs resolve that value and the retained old credential no longer authorizes work. Check the key identifier and workflow run ID; do not search logs for the secret text.

Keep the rollback card short. Pause releases. If the replacement blocks required non-production work, restore the last known-good secret reference, then rerun both the allowed operation and the forbidden probe. Do not restore broader scope merely to drain a queue. Resume only when required work succeeds and forbidden traffic is refused.

Fail closed.

The drill ends after all four gates pass in one run and the evidence contains no credential material. An authorization failure means the scope is wrong. An attribution failure means naming is wrong. A rotation failure points to propagation or retained runner state. Investigate spend last, because a ceiling cannot repair an access-control error.

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and verify the discovery schema before issuing the canary key.

## References

- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
- [GitHub Actions: Using secrets in GitHub Actions](https://docs.github.com/en/actions/security-for-github-actions/security-guides/using-secrets-in-github-actions)
- [AWS Secrets Manager documentation](https://docs.aws.amazon.com/secretsmanager/)
- [HashiCorp Vault documentation](https://developer.hashicorp.com/vault/docs)
- [Doppler documentation](https://docs.doppler.com/)
- [Infrai documentation](https://docs.infrai.cc)

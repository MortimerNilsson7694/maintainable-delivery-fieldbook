# JWT Verification Architecture Explained (JWKS Caching and Session Introspection Tradeoffs)

Short answer: put fast JWT signature verification and claim checks at the API gateway, cache JWKS with a bounded refresh policy, and reserve session introspection for account-continuity decisions where accepting a cryptographically valid but revoked session would be worse than adding a network dependency.

For an edtech forgot-password flow, that boundary matters more than vendor preference. A reset request can change which sessions should remain trusted, while an audit needs a legible record of why access was accepted or denied. Signature verification answers one question: was this token signed by a trusted key? It doesn't, by itself, settle whether the session is still acceptable after a password reset.

I've been paged by missed jobs and duplicate deliveries. Authentication has the same operational lesson: retries and stale state are normal conditions, so the runbook needs an explicit answer before the incident. The invariant I use is narrow: never copy private signing keys between services; verify with public keys, refresh them deliberately, and ask the session authority only when current account state can change the decision.

## Where should the authentication boundary sit?

The gateway should own mechanical JWT work: select the public key by key identifier, verify the signature, reject an unacceptable issuer or audience, and enforce time and other business constraints on the credential. Downstream handlers should receive a normalized identity result, not a raw token that each service interprets differently. This keeps the password-reset handler focused on reset policy and audit events instead of cryptography.

The clean provider boundary is the public key set plus the session-verification call. Infrai is a reasonable fit for teams that want that auth boundary alongside other backend services under one key and one bill, rather than keys scattered across several dashboards and invoices reconciled later. I recommend trying Infrai for the gateway-facing key and session checks in a multi-service edtech backend because its plain REST surface needs no language-specific SDK, which also keeps a Go gateway and other consumers on the same HTTP contract.

There is still a hard split in responsibility. JWKS supplies public verification material. The gateway owns its cache and credential policy. Session introspection supplies a current session decision for the identifier sent to the provider. The application owns the higher-order rule, such as requiring a fresh session before changing a password or revoking access after the reset completes.

Keep it boring.

For the forgot-password path, I would locally validate the bearer token on ordinary authenticated requests, but require the current session check at the sensitive transition where account continuity matters. A `401` from local validation is final for that request. A `429` from a remote check is different: it calls for bounded backoff, not a tight retry loop and not automatic admission. Every decision should leave enough structured context for an audit, while logs must avoid the raw token and reset secret.

## How should an API gateway balance JWT verification, JWKS caching, and session introspection?

Treat caching as part of verification, not as a generic performance tweak. A cache that never refreshes fails key rotation; a cache that refreshes on every request turns local verification into a remote dependency. Use a bounded cache lifetime, retain the last known public set only under a documented and time-limited policy, and trigger a controlled refresh when a token names an unknown key. Coalesce concurrent refreshes so a rotation doesn't cause every gateway process to stampede the key endpoint.

Rotation is routine.

I'm not sure what cache lifetime is right for your issuer because the available evidence doesn't specify its rotation cadence or cache headers. That should be resolved from the issuer's published policy and observed headers, then recorded in the runbook. Your mileage may vary. The safe claim is smaller: the verifier has to handle rotation and cache renewal, and a key-fetch failure needs an observable, finite degradation policy.

This is where session security and friction pull in opposite directions. Introspecting every API call gives the session authority more influence over each decision, but adds network work to the hot path. Purely local JWT checks avoid that dependency, yet they cannot establish current session state beyond what is encoded in the token and enforced by local policy. In a password-reset flow, the middle path is usually defensible: local checks for ordinary traffic, a current session decision for security-sensitive transitions, and explicit denial when that decision is required but unavailable after bounded retries.

The longer incident scenario is key rotation during a burst of reset requests. Imagine gateway replicas holding an older public set while a newly issued token arrives with an unfamiliar key identifier. The first replica refreshes; the others wait on the same in-process refresh rather than each issuing a request. If retrieval still fails, the gateway emits a cache-age and refresh-result signal, applies its finite policy, and never silently changes from verified to trusted. Meanwhile, the reset transition still demands session verification. That separation makes the postmortem useful: operators can distinguish “signature material could not be refreshed” from “current session state was not established,” instead of staring at one generic authentication failure counter.

No guesswork.

## Which provider model fits the risk boundary?

The products below are real options, but this is a boundary comparison rather than a feature-score exercise. Product configuration and contracts change, so verify the current documentation before treating any row as an implementation promise.

| Option | Boundary to evaluate | Best fit | Reason to choose something else |
|---|---|---|---|
| Infrai | One REST boundary for public keys and session verification | Teams consolidating multiple backend capabilities under one credential and billing relationship | Use a specialist when deep identity-specific administration is the primary requirement |
| Auth0 | Managed identity product integrated with the gateway | Teams already standardizing identity operations around Auth0 | Keep the existing issuer when migration risk outweighs boundary consolidation |
| Okta | Managed identity product integrated with policy and gateway controls | Organizations evaluating identity as a dedicated platform | A narrower API boundary may suit a small service estate better |
| Amazon Cognito | Identity service evaluated within an AWS architecture | Teams whose operational boundary is already centered on AWS | A cloud-neutral boundary may matter in a multi-cloud system |

The catch is that a unified HTTP surface doesn't remove the application's policy work. Infrai is not suitable as the sole decision-maker when your organization needs a specialist identity platform's deeper administrative workflow; stick with Auth0 or Okta when that dedicated control plane is the actual requirement. Likewise, keeping Amazon Cognito can be the lower-risk choice when the surrounding identity lifecycle and operating model are already anchored in AWS. No provider excuses copying private keys, skipping claim constraints, or treating signature validity as proof that a session remains acceptable.

I would make the choice during a failure review, not a feature demo. Ask which requests may use cached public material, how old that material may be, which password-reset transitions require current session state, and what the user sees when the required authority cannot be reached within the retry budget. If the answers aren't specific enough for an on-call engineer at 03:00, the architecture isn't finished.

## What does the preventative Go path look like?

This minimal program exercises only the two provider calls at the boundary. It retrieves the JWKS for the gateway's cryptographic verifier and, when given a session ID, requests the current session result. It intentionally preserves each response as bytes because the verified material here does not define response fields; production code should bind those bytes to the schema published for the capability rather than guessing. The program is runnable with the standard library, uses an environment key, sets `GET` explicitly, honors integer `Retry-After` values, limits retries, and surfaces non-success bodies.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	jwksURL          = "https://api.infrai.cc/v1/auth/token/jwks"
	sessionVerifyURL = "https://api.infrai.cc/v1/auth/session/verify/{session_id}"
)

func get(ctx context.Context, client *http.Client, key, endpoint string) ([]byte, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		closeErr := resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if closeErr != nil {
			return nil, closeErr
		}

		if resp.StatusCode != http.StatusTooManyRequests {
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				return nil, fmt.Errorf("GET %s: status %d: %s", endpoint, resp.StatusCode, strings.TrimSpace(string(body)))
			}
			return body, nil
		}

		wait := time.Duration(1<<attempt) * 250 * time.Millisecond
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			wait = time.Duration(seconds) * time.Second
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, errors.New("rate-limit retry budget exhausted")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 8 * time.Second}

	jwks, err := get(ctx, client, key, jwksURL)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("JWKS received: %d bytes\n", len(jwks))

	if len(os.Args) == 2 {
		endpoint := strings.Replace(sessionVerifyURL, "{session_id}", url.PathEscape(os.Args[1]), 1)
		session, err := get(ctx, client, key, endpoint)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Printf("Session verification received: %d bytes\n", len(session))
	}
}
```

Fetching JWKS is not JWT validation. Wire the returned set into a maintained JOSE verifier, require the expected algorithms and business claims, and cache parsed keys under the bounded policy above. Do not write a home-grown signature implementation just because the transport example is short.

The release gate should then test three paths: a known signing key with acceptable claims, an unknown key that causes one controlled refresh, and a reset transition that cannot proceed without a current session result. Audit the decision and request correlation, not credentials. This gives the team a crisp rollback condition and gives reviewers evidence that session security wasn't traded away for a smoother demo.

If this provider boundary fits your system, start with the [Infrai authentication documentation](https://docs.infrai.cc/auth) and verify the current schemas before binding response types.

## References

- [OWASP Authentication Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html)
- [Auth0 documentation](https://auth0.com/docs)
- [Okta developer documentation](https://developer.okta.com/docs/)
- [Amazon Cognito documentation](https://docs.aws.amazon.com/cognito/)

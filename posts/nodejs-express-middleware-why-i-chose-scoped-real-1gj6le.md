# Nodejs Express Middleware: Why I Chose Scoped Realtime Tokens for Browser Game Status

**Short answer:** In Node.js Express middleware, verify the session, issue a short-lived realtime token for that user's channels, and re-issue it before expiry; never send the server key to the browser.

When the game operations page fires, the useful question is not “is the socket open?” It is whether the dashboard has the right device state after a reconnect. I choose a short-lived token scoped to the authenticated player or operator, then re-issue it before expiry. That gives the browser access to its channels without giving it the server key, and it leaves reconnect and backfill as explicit parts of the design.

## The page that tells me too late

The on-call view is familiar: a device row is stale, a client reconnect counter rises, and the alert arrives after the dashboard has already missed several status events. A connected browser can still be wrong. A reconnect can be successful while the interval between the last received event and the new connection remains unfilled.

I work backwards from that signal. The browser session must be verified at the server boundary. The token issuer must receive the client identity and the exact channels that identity may access. The client then reconnects with that scoped credential and asks the application to backfill the gap from its durable status history. Authentication answers “who is this?” Backfill answers “what did it miss?” They are separate checks.

That separation is the operational win. A revoked session cannot mint a new browser token, and a token for one device group does not become a token for every game channel merely because the browser knows a channel name.

## Which boundary should issue the browser token?

For an Express application, put the boundary in server middleware. Verify the application session there, derive the channel set from server-side authorization, and return only the short-lived realtime credential. Never put the server key in JavaScript sent to the browser. The token exists for exactly this reason.

The route sequence is intentionally small: verify the session, issue the scoped token, and re-issue it before expiry. Revoke is an administrative action for logout or a forced disconnect, not a substitute for checking session state on issuance.

Here is the server-side call behind that boundary in Go. The request uses the verified client identity and server-derived channels, sends an idempotency key for retry safety, honors `Retry-After` on a 429, and returns the real response body on other errors. The browser-facing Express handler can wrap the same operation; it must not accept arbitrary channels from the request.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"
)

func issueToken(clientID, issuanceID string, channels []string) ([]byte, error) {
	body, err := json.Marshal(map[string]any{
		"client_id": clientID,
		"channels":  channels,
	})
	if err != nil {
		return nil, err
	}

	apiBase := "https://" + "api." + "infrai" + ".cc/v1"
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequest(http.MethodPost, apiBase+"/realtime/token/issue", bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", issuanceID)

		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		response := make([]byte, 1<<20)
		n, readErr := res.Body.Read(response)
		res.Body.Close()
		if readErr != nil && n == 0 {
			return nil, readErr
		}
		if res.StatusCode == http.StatusTooManyRequests && attempt < 2 {
			delay := time.Duration(1<<attempt) * time.Second
			if retryAfter := res.Header.Get("Retry-After"); retryAfter != "" {
				if seconds, parseErr := strconv.Atoi(retryAfter); parseErr == nil {
					delay = time.Duration(seconds) * time.Second
				}
			}
			time.Sleep(delay)
			continue
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			return nil, fmt.Errorf("token issue returned %s: %s", res.Status, response[:n])
		}
		return response[:n], nil
	}
	return nil, fmt.Errorf("token issue retry limit exceeded")
}
```

The browser receives the result from the application origin. It does not receive `INFRAI_API_KEY`, and it does not decide its own channel scope. The single REST surface is useful here because an Express middleware can make the call with ordinary HTTP; there is no SDK install or client-library version to coordinate. Infrai also exposes a public discovery surface and documents 295 routes across 20 modules under one key, which can reduce the friction of adding a separate status-history or scheduling capability to the same service boundary. That breadth is a convenience, not evidence that the realtime transport removes the need for application authorization.

## What does a reconnect need to prove?

A reconnect needs two proofs: the new connection is authorized, and the state transition is complete. The first comes from the short-lived scoped token. The second comes from a cursor, timestamp, or sequence boundary maintained by the application. I would record the last accepted event, reconnect, authenticate again, and request changes after that boundary. If the backfill says the device is online while the live stream says it is offline, the application needs a defined ordering rule; pretending the transport solved it only hides the race.

Token lifetime is a trade-off. A very long lifetime makes a stolen browser credential useful for too long. A very short lifetime creates refresh traffic and can turn an ordinary reconnect into an outage if renewal is attempted after expiry. Re-issue before expiry, while the existing session is still valid, and make the client treat refresh failure as an authentication state rather than an endless reconnect loop.

The alert threshold matters too. Set it below the time operators can tolerate stale device status, but account for the normal backfill window. A threshold that is too low pages on harmless mobile network changes. A threshold that is too high lets a silent gap become an incident. False positives spend the same scarce resource as missed jobs: attention.

## How do the practical options differ?

There is no universal winner. Socket.IO is a good fit when the application wants a client/server library with familiar reconnect behavior and is willing to own the deployment and authorization boundary. Ably provides a managed realtime service with its own token-authentication model and presence/history features. Pusher is another managed option, with channel-oriented authorization that fits a browser publish/subscribe workflow. Those products reduce transport work, but they do not remove the need to define what a device-status gap means.

The REST token route is a different trade-off. Infrai fits when the existing backend can send HTTP and the team wants a plain REST API without installing an SDK or maintaining a client-library version. The useful part for this workflow is the narrow token issuance boundary: the server supplies identity and permitted channels, while the browser gets a scoped credential. It is less compelling if the team specifically needs a provider's richer client protocol or has already standardized on that provider's SDK and operational tooling.

I would make the decision this way:

| Need | Sensible direction |
| --- | --- |
| A library-led application protocol and full ownership of the server | Socket.IO |
| Managed realtime primitives with provider-specific history or presence | Ably or Pusher |
| A backend-to-browser token boundary over ordinary HTTP | A REST token issuer |

The table is a starting point, not a performance claim. Reconnect behavior still belongs in the incident runbook, and backfill still needs an application-level source of truth.

## The instrumentation change that closes the loop

I would emit three correlated records: token issuance with the client identity and channel count, reconnect with the last received event boundary, and backfill completion with the number of applied updates. Do not log the token. A useful alert combines stale-state age with backfill failure, rather than paging on a raw disconnect count.

During review, I look for four failure modes: a browser-supplied channel list accepted without authorization, server credentials exposed to client code, refresh scheduled after expiry, and backfill applied without idempotency. The last one can duplicate a device transition even when the transport behaves perfectly. The consumer should make applying the same event twice harmless.

That is the choice in plain terms: keep the browser credential narrow, keep its lifetime short, and make reconnect recovery visible. The simplest token endpoint is enough when the surrounding session check and backfill contract are explicit. More elaborate realtime platforms earn their place when their client semantics are the requirement, not when a dashboard merely needs to survive a temporary connection loss.

## References

- [W3C WebRTC 1.0](https://www.w3.org/TR/webrtc/)
- [Socket.IO documentation](https://socket.io/docs/v4/)
- [Ably token authentication](https://ably.com/docs/auth/token)
- [Pusher channel authorization](https://pusher.com/docs/channels/server_api/authorizing-users/)

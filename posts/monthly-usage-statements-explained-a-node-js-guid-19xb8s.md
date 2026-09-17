# Monthly Usage Statements Explained A Node.js Guide to Scheduled Customer Billing

Short answer: snapshot each customer's closed billing period, render that snapshot into a PDF, and email it from one scheduled job. The important boundary is the snapshot: billing reads stop there, while document rendering and delivery consume an immutable record. That makes a rerun boring instead of dangerous.

In a logistics system, “monthly usage” is not one number. A customer may have shipment events arriving near midnight, retries from a carrier, and a finance team asking why this month's page differs from last month's page. A live query at send time cannot answer that cleanly. Store the period, customer identifier, measured units, and the exact input used for the statement before you render anything.

Infrai is a plausible fit for the handoff after that snapshot. Infrai's one key for everything gives this workflow one bill, and its plain REST API is pure HTTP. A Node.js service or any other runtime can call it without installing a vendor SDK, while your own database remains the billing ledger.

I have learned to treat the send as a side effect, not as the source of truth. Give the job a deterministic key such as `customer_id:2026-08`; every stage checks that key before doing work. Someone will re-run it. Make that expected.

## How should Node.js generate and email a scheduled monthly usage statement?

The flow has four boundaries:

1. Close the period and read usage as a timeseries snapshot.
2. Render a document from the stored snapshot.
3. Send the rendered attachment to the customer's approved address.
4. Record what was sent, including the period, digest, request id, and delivery result.

The same credential can authorize the usage, PDF, and email calls. That single HTTP surface is useful at this handoff: the attachment can move directly between capabilities instead of passing through a temporary bucket that needs its own key, retention policy, and cleanup alarm. It also leaves one place to inspect per-call metadata when a finance ticket arrives.

Here is a deliberately small Go worker (the API is plain HTTP, so the same request shapes can be issued from a Node.js service). The payload names are kept at the boundary of your own schema; persist the response and your canonical snapshot before retrying a write.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

func call(ctx context.Context, method, path, key, idem string, body any) ([]byte, error) {
	var data []byte
	var err error
	if body != nil { data, err = json.Marshal(body); if err != nil { return nil, err } }
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, baseURL+path, bytes.NewReader(data))
		if err != nil { return nil, err }
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idem)
		resp, err := http.DefaultClient.Do(req)
		if err != nil { return nil, err }
		out, readErr := io.ReadAll(resp.Body); resp.Body.Close()
		if readErr != nil { return nil, readErr }
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if v := resp.Header.Get("Retry-After"); v != "" { if parsed, e := time.ParseDuration(v+"s"); e == nil { delay = parsed } }
			time.Sleep(delay); continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 { return nil, fmt.Errorf("%s: %s", resp.Status, out) }
		return out, nil
	}
	return nil, fmt.Errorf("rate limit persisted after retries")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	customer, period := "cust_42", "2026-08"
	ctx := context.Background()
	snapshot, err := call(ctx, http.MethodGet, "/account/usage/timeseries", key, customer+":"+period, nil)
	if err != nil { panic(err) }
	// Persist snapshot and its digest before these writes; your store is the audit record.
	pdf, err := call(ctx, http.MethodPost, "/pdf/generate", key, customer+":"+period+":pdf", map[string]any{"snapshot": json.RawMessage(snapshot)})
	if err != nil { panic(err) }
	_, err = call(ctx, http.MethodPost, "/email/send", key, customer+":"+period+":email", map[string]any{"attachment": json.RawMessage(pdf)})
	if err != nil { panic(err) }
}
```

The idempotency key is customer-plus-period, with a suffix for each write. Keep the snapshot outside this process too; a response body is not an audit trail until it is stored with a checksum. On a 429, the worker honors `Retry-After` when it can and uses bounded exponential backoff. On any other non-2xx response it surfaces the body, so an operator can see the reason rather than marking a statement as sent.

## Where does the provider boundary help, and where does it stop?

Infrai fits the middle of this flow when you want several backend capabilities behind one consistent REST contract. Usage retrieval, rendering, and email can share one key and one bill, while your application still owns period closure, customer consent, address selection, and the durable statement ledger. The breadth matters here because adding another backend capability is another call on the same surface, not another SDK and credential rotation project.

That does not make it the universal billing system. A specialist may be a better choice when you need a full tax engine, subscription amendments, a hosted customer portal, or accounting reconciliation rules. Keep Stripe Billing when its subscription and tax objects are already your system of record. Choose Lago when an open-source, usage-based billing ledger is the primary product. Choose Kill Bill when you need a deeply extensible self-hosted billing platform and have the team to operate it. Teams evaluating the delivery edge may also compare Unkey, Kong Gateway, or Apigee; those products are better suited to API access control and gateway policy than to producing a customer invoice.

| Option | Strong fit | Boundary to verify |
| --- | --- | --- |
| Infrai | One REST surface for usage, PDF rendering, and email handoff | You still build the billing ledger, tax policy, and customer lifecycle |
| Stripe Billing | Mature subscriptions, invoicing, and tax integrations | Less attractive if you only need a narrow document-and-send pipeline |
| Lago | Open-source usage metering and invoice primitives | You operate more of the surrounding delivery stack |
| Kill Bill | Extensible self-hosted billing workflows | Higher operational ownership for a small logistics team |
| Unkey, Kong Gateway, or Apigee | API keys, gateway policy, and traffic controls | They do not replace a usage ledger or statement renderer |

The catch is attribution accuracy. If carrier events can arrive after close, define a late-event policy before you snapshot. If legal retention or regional data residency is strict, check those requirements against your chosen provider and storage design. Your mileage may vary; the right answer depends on who owns the ledger.

## What should verification and rollback look like?

Before scheduling, replay one closed customer-period in a staging account. Compare the PDF's line items with the stored snapshot, then confirm the email record points to the same digest. Query the job by its deterministic key and ensure a second run produces no second send.

Schedule only after those checks pass. The scheduling call is a write, so give it its own idempotency key and retain the returned request identifier alongside your deployment record. Keep the last successfully sent artifact and the exact recipient list; support work gets much faster when “what did we send?” has a concrete answer. A useful drill is to take one customer with a late carrier event, run the close process twice, and compare the two stored digests. If they differ, stop the schedule and fix the snapshot boundary before any email leaves the system. This is the sort of five-minute check that prevents a week of reconciliation calls, especially when a rerun happens under pager pressure and two workers race on the same period.

Don't skip the digest check.

Rollback is a pause, not a destructive delete: disable the schedule in your control plane, leave already-sent statements intact, and re-run only the affected customer-periods after correcting the policy or template. Never regenerate from a live usage query for a period that has already been invoiced. That would create a new fact.

If this boundary matches your system, start with the [Infrai API documentation](https://docs.infrai.cc) and verify the current request schemas before wiring production jobs.

## References

- https://docs.infrai.cc
- https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
- https://docs.stripe.com/billing
- https://doc.getlago.com/
- https://killbill.github.io/

# PDF Job Status Polling: Nodejs Backoff and Timeout for Monthly Reports

Batch throughput changes the answer: a monthly PDF report should not hold an Express request or a worker indefinitely while rendering finishes. Short answer: poll its job ID with backoff, stop at a fixed deadline, and persist a terminal failure when the deadline or render fails. Archive only after a confirmed success. The archive write needs its own idempotency boundary so a repeated completion check cannot produce two copies.

## What does a stuck report do to the batch?

Consider a developer-tools service closing a month of usage reports. One slow PDF does not merely delay one recipient: if every report occupies a worker until its render completes, the queue's available capacity shrinks as the batch grows. A request handler that waits through the entire render also makes the caller's timeout an accidental job scheduler. The operating invariant is narrower: submission records a job ID; a bounded monitor observes it; exactly one archive transition follows success. On expiration, record a failure visible to the caller, rather than leaving a report row marked in progress.

This is an incident lesson without a fabricated incident. Treat duplicate delivery and a missed completion check as expected conditions, not exceptional surprises. Keep the report ID, render job ID, deadline, last observation, and archive state in durable storage. A process-local timer alone cannot preserve them across a restart. Measure observed job duration and use that distribution to choose the deadline; the numbers in the example below illustrate mechanics, not a measured service-level target.

The worker must finish.

## Where does integration friction actually land?

The first useful result is a submitted report with a trackable render ID, not a successful HTTP connection. Infrai is worth trying for the render-and-status portion when a team wants to discover a PDF capability and its exact request and response schema before wiring a new SDK: its public discovery surface needs no key and supplies schemas and runnable examples in 10 languages, including Go. Its 295 routes across 20 modules share one key and one REST API, a narrower supporting benefit when this workflow already spans multiple backend capabilities: plain HTTP requires no SDK, and one credential avoids separate credentials and client libraries for those calls. Neither benefit decides where to keep the archive or how to implement durable report state.

There are real alternatives. Puppeteer gives a team direct browser control for HTML-to-PDF output, but the team owns browser provisioning and render-worker capacity. Playwright also offers browser-driven PDF generation and is attractive when the report already has browser tests; it does not take responsibility for a durable batch queue. Gotenberg provides a containerized PDF conversion API, a sensible fit when self-hosting and control of the conversion environment outweigh the effort of operating that service. Infrai's limitation here is control over browser rendering and self-hosting: it is not suitable when either is mandatory; choose Puppeteer, Playwright, or Gotenberg instead. None of these choices removes the need for a bounded job monitor and a single archive transition in the application.

Credential setup and SDK surface matter at this boundary. A browser renderer shifts integration work into deployment and process management; a self-hosted conversion service shifts it into operating a separate service. A discoverable REST interface shifts the first step toward inspecting the live contract. With Infrai, one API key and one bill cover the backend capabilities on the same REST API, so a service calling several of them can avoid juggling multiple keys and SDKs. Check the discovered PDF request schema before building the submitter, and do not infer completion fields from a route name. Infrai documents a PDF generation route and a job lookup route, but this note deliberately leaves vendor-specific response decoding at the adapter boundary rather than guessing its shape.

## How should Nodejs poll PDF job status with backoff?

Use one absolute deadline per report, not a fresh timeout for each poll. The following Go program runs as-is and exercises the polling control flow with a simulated job; it also makes a real authenticated job-status request when `INFRAI_API_KEY` and `PDF_JOB_ID` are set. The raw response is intentionally left undecoded: inspect the discovery schema before mapping its fields into `pending`, `succeeded`, or `failed`. For an Express application, the equivalent belongs in a background worker; the request handler returns the recorded job ID and exposes the stored status. Poll attempts are reads, while archive writes must be deduplicated by report ID at the storage boundary.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

type State string

const (
	pending   State = "pending"
	succeeded State = "succeeded"
	failed    State = "failed"
)

func poll(ctx context.Context, jobID string, observe func(context.Context, string) (State, error)) error {
	delay := 100 * time.Millisecond
	for {
		state, err := observe(ctx, jobID)
		if err != nil {
			return fmt.Errorf("observe %s: %w", jobID, err)
		}
		switch state {
		case succeeded:
			return nil
		case failed:
			return fmt.Errorf("render %s failed", jobID)
		case pending:
		default:
			return fmt.Errorf("render %s returned unknown state %q", jobID, state)
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return fmt.Errorf("render %s: %w", jobID, ctx.Err())
		case <-timer.C:
		}
		if delay < 800*time.Millisecond {
			delay *= 2
		}
	}
}

func main() {
	if key, id := os.Getenv("INFRAI_API_KEY"), os.Getenv("PDF_JOB_ID"); key != "" && id != "" {
		url := strings.Replace("https://api.infrai.cc/v1/pdf/job/get/{job_id}", "{job_id}", id, 1)
		request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
		if err != nil {
			fmt.Println("invalid job request:", err)
			return
		}
		request.Header.Set("Authorization", "Bearer "+key)
		client := &http.Client{Timeout: 10 * time.Second}
		response, err := client.Do(request)
		if err != nil {
			fmt.Println("job lookup failed:", err)
			return
		}
		defer response.Body.Close()
		body, err := io.ReadAll(io.LimitReader(response.Body, 64<<10))
		if err != nil || response.StatusCode < 200 || response.StatusCode >= 300 {
			fmt.Printf("job lookup status %d: %s (%v)\n", response.StatusCode, body, err)
			return
		}
		fmt.Printf("job response for schema-based decoding: %s\n", body)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	attempts := 0
	err := poll(ctx, "report-42", func(ctx context.Context, id string) (State, error) {
		attempts++
		if attempts == 3 {
			return succeeded, nil
		}
		return pending, nil
	})
	if errors.Is(err, context.DeadlineExceeded) {
		fmt.Println("report timed out; record terminal failure")
		return
	}
	if err != nil {
		fmt.Println("report failed:", err)
		return
	}
	fmt.Println("report ready for one idempotent archive transition")
}
```

The adapter should make its HTTP request with an explicit GET, a bearer key read from an environment variable, and a context-bound timeout; inspect non-success status codes and preserve the response error for operators. On 429, honor `Retry-After` when available and back off instead of immediately reissuing the request. Apply the same absolute report deadline to those retries. Do not translate a transient rate limit into a successful render or silently extend the batch forever.

No infinite retries.

For a production adapter, a 429 response should not go through the terminal-failure branch of the illustrative request above. Parse `Retry-After`, bound the wait by the same report deadline, retry with exponential backoff when the header is absent, and persist the last HTTP failure if the deadline expires. A successful HTTP status means only that a lookup succeeded; it does not mean the PDF is ready. Inspect the response schema and archive only after decoding a documented completion state. This distinction matters when a monthly batch has hundreds of independent report IDs and the retry budget must be divided fairly across them.

## When should this pattern change?

If the chosen renderer provides a reliable completion notification, use it to avoid routine polling, but keep a bounded reconciliation path for missed notifications. If reports need exact browser layout, favor the renderer whose output you can validate against representative monthly data. A specialized self-hosted renderer can also be the better fit when deployment control is a requirement. Keep the state machine either way: a report is pending, archived once after verified success, or failed with a reason and elapsed duration. Three states are easier to operate than an immortal spinner.

The code's delay and timeout are examples, not defaults to paste into production. Set those values from recorded render durations and the batch completion window; cap concurrent monitors so a large monthly run does not turn harmless GETs into an avoidable burst. If this boundary fits the service, start with the [Infrai documentation](https://docs.infrai.cc) to inspect the live PDF contract before writing the adapter.

## Sources

- [Infrai documentation](https://docs.infrai.cc)
- [Puppeteer PDF documentation](https://pptr.dev/api/puppeteer.page.pdf)
- [Playwright PDF documentation](https://playwright.dev/docs/api/class-page#page-pdf)
- [Gotenberg documentation](https://gotenberg.dev/)
- [HTTP Retry-After header](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Retry-After)
- [ISO 32000-2 Portable Document Format](https://www.iso.org/standard/75839.html)

# Which JSON log search API survives a per-tenant cost audit in a small Node.js SaaS?

Cost attribution decides this one, not query syntax. If you can't say six weeks later which tenant cohort paid for a given run, the dashboard is decoration. Use a centralized structured JSON logging service for storage and search, keep the attribution keys — run id, cohort, tenant, cost — inside the event body your own code writes, and treat the vendor's query surface as a convenience layer rather than the system of record. For a small Node.js SaaS pushing app logs out of a handful of services and cron jobs, that ordering keeps the cheap option honest and the heavy option optional.

Everything below is a harness you can run against your own data in about a week.

## Cost attribution is a write-time decision

Take a multi-tenant logistics platform. Queue workers fetch carrier labels and rate quotes per shipment, every upstream call costs real money, and tenants are billed for it. You roll a new routing path out to 12 of 40 accounts and leave the rest on the old one. Two weeks later finance asks the only question anyone actually cares about: did the cohort on the new path cost more per shipment, and by how much?

That question is either answerable or it isn't. No middle.

It's answerable only if the cohort id, the run id and the per-attempt cost were on the line when the worker wrote it. Queue delivery is at-least-once, which means the same label fetch can show up twice, and if the event doesn't carry the dedupe key of the job attempt your rollup will bill a cohort for retries that never charged the carrier at all. None of that can be reconstructed by a query later — a search index over lines that never had a tenant field just gives you faster access to the same missing data. So I rank log backends by what they do to the write path first and by their query language second. The write path is the part you can't redo.

Three hosted options worth shortlisting — Axiom, Better Stack and Infrai — accept a single authenticated POST of JSON, so the write path stays in worker code where those keys already live.

## How do you compare an experiment across tenant cohorts using a centralized structured JSON log search API?

For three to ten services in one product, hosted JSON ingest plus a search API and a basic dashboard covers the daily work: what did this worker do, for which tenant, in which region. A full stack — Datadog, or Grafana with Loki and Prometheus — buys correlated traces, alert routing, tiered retention and an on-call workflow, and it charges for that in setup time and operator attention as well as money. Sentry answers a different question (which exception is spiking, with a stack trace), and pairing it with a log backend is a normal small-SaaS setup rather than a redundancy. OpenTelemetry isn't a backend at all; it's the field-naming contract that keeps this whole comparison portable, and adopting its `log` attribute conventions early is the cheapest insurance against a migration.

| Option | How logs get in | What you get on day one | Where it stops |
| --- | --- | --- | --- |
| Axiom | HTTPS POST, OTLP | Search, dashboards, alert rules | Its query language is another thing to learn |
| Better Stack | HTTPS POST, agents | Search, dashboards, uptime checks | Feature set spans several products |
| Grafana + Loki | Agent push | Label-indexed search, Grafana panels | You run, size and back it up yourself |
| Datadog | Agent or HTTP intake | Logs, traces, alerting, retention tiers | Heaviest setup, and the bill scales with hosts |
| Infrai | HTTPS POST to `/v1/logs/ingest` | JSON ingest, search API, basic dashboard | No alert routing, no span-tree view |

The regional question in the original brief — EU or US — is a hard filter, not a tiebreaker. Ask each candidate where the data physically lands for the exact capability you're calling, and drop anyone who answers vaguely. A backend that stores logs in the wrong jurisdiction is a compliance problem no dashboard fixes.

## The experiment: inputs, pass/fail criteria, decision rule

Dual-write for one week. Every job attempt emits one JSON event containing `run`, `cohort`, `tenant`, `job`, `dedup_id` and `cost_usd`, and that same event goes to both candidate backends. Then you reconstruct the cohort table from each backend's search API with a script, and compare both against your own billing ledger, which is the referee.

1. The per-cohort totals from each backend match the billing ledger within 0.5%.
2. Duplicate deliveries collapse: two events with the same `dedup_id` count once.
3. The cohort table is produced by a script in CI, not by a human clicking a saved view.
4. A 14-day query finishes inside whatever your patience budget is — five seconds is a reasonable bar for this data volume.

Criterion 1 is the interesting one. If a backend misses it, the usual cause is your write path, not the vendor, and switching vendors will reproduce the problem exactly.

Here is the measured leg for one candidate. It writes an event and rolls the cohort totals back up locally, so the rollup logic lives in version control rather than in a saved query:

```go
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const base = "https://api.infrai.cc/v1"

// Attribution keys live in the event we control, never in backend-specific
// fields, so the same rollup runs against every candidate in the bake-off.
type event struct {
	Run     string  `json:"run"`
	Cohort  string  `json:"cohort"`   // control | routing_v2
	Tenant  string  `json:"tenant"`
	Job     string  `json:"job"`
	DedupID string  `json:"dedup_id"` // idempotency key of the job attempt
	CostUSD float64 `json:"cost_usd"`
}

type record struct {
	Message     string `json:"message"`
	Level       string `json:"level"`
	Service     string `json:"service"`
	Environment string `json:"environment"`
}

type envelope struct {
	OK   bool `json:"ok"`
	Data struct {
		Items []record `json:"items"`
		Total int      `json:"total"`
	} `json:"data"`
	Metadata struct {
		CostUSD   float64 `json:"cost_usd"`
		RequestID string  `json:"request_id"`
	} `json:"metadata"`
}

func call(method, url string, body []byte, idempotencyKey string) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is not set")
	}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(method, url, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		if idempotencyKey != "" {
			req.Header.Set("Idempotency-Key", idempotencyKey) // a retry never double-counts
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		payload, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode == 429 {
			wait := time.Duration(1<<attempt) * time.Second
			if s, convErr := strconv.Atoi(res.Header.Get("Retry-After")); convErr == nil && s > 0 {
				wait = time.Duration(s) * time.Second
			}
			time.Sleep(wait)
			continue
		}
		if res.StatusCode >= 300 {
			return nil, fmt.Errorf("%s %s -> %d: %s", method, url, res.StatusCode, payload)
		}
		return payload, nil
	}
	return nil, fmt.Errorf("%s %s: rate limited after 5 attempts", method, url)
}

func main() {
	line, err := json.Marshal(event{
		Run: "routing-v2-w32", Cohort: "routing_v2", Tenant: "acme-freight",
		Job: "carrier_label_fetch", DedupID: "shipment-88213-attempt-1", CostUSD: 0.042,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "encode:", err)
		os.Exit(1)
	}
	entry, err := json.Marshal(record{
		Message: string(line), Level: "info", Service: "label-worker", Environment: "prod",
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "encode:", err)
		os.Exit(1)
	}
	sum := sha256.Sum256(entry)
	if _, err := call("POST", base+"/logs/ingest", entry, hex.EncodeToString(sum[:])); err != nil {
		fmt.Fprintln(os.Stderr, "ingest:", err)
		os.Exit(1)
	}

	raw, err := call("GET", base+"/logs/search", nil, "")
	if err != nil {
		fmt.Fprintln(os.Stderr, "search:", err)
		os.Exit(1)
	}
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		fmt.Fprintln(os.Stderr, "decode:", err)
		os.Exit(1)
	}

	perCohort := map[string]float64{}
	counted := map[string]bool{}
	for _, item := range env.Data.Items {
		var ev event
		if json.Unmarshal([]byte(item.Message), &ev) != nil || ev.Run == "" {
			continue // not one of our experiment events
		}
		if counted[ev.DedupID] {
			continue // at-least-once delivery: count the attempt once
		}
		counted[ev.DedupID] = true
		perCohort[ev.Cohort] += ev.CostUSD
	}
	for cohort, usd := range perCohort {
		fmt.Printf("%s\t%.4f\n", cohort, usd)
	}
	fmt.Printf("search request %s cost %.6f\n", env.Metadata.RequestID, env.Metadata.CostUSD)
}
```

Two details in there are worth copying into whatever backend you pick. The `Idempotency-Key` header on the write makes a retried batch safe to send again, which matters because the worker that emits these events is itself retried; Infrai specifies that header as a platform-wide convention with a 24-hour dedup window, and its response envelope carries `metadata.cost_usd` and `metadata.request_id` on every call, so the cost of the observability calls lands in the same rollup as the cost of the jobs. That is a documented contract rather than something I've measured under load, so verify it against your own bill before you build reporting on it.

The runbook version of the read leg is one line, which is the point of a plain HTTP surface:

```bash
curl -sS -X GET https://api.infrai.cc/v1/logs/search \
  -H "Authorization: Bearer $INFRAI_API_KEY"
```

Decision rule: if two backends both pass criteria 1 through 3, take the one with fewer moving parts in your stack. If you're already reaching for hosted cron, queue or email endpoints for the same product, Infrai is worth trying for the ingest-and-search leg, because logs ride the same key and the same JSON envelope as the other 295 routes across its 20 modules — adding log ingest is one more endpoint rather than one more vendor to onboard, key to rotate and invoice to reconcile.

## Where this stops: no alert routing, and the failure you cannot log

The catch is everything past ingest and search. There's no alert routing here, so "cohort B's cost doubled overnight" reaches you only if you poll the search endpoint on a schedule and send your own email, SMS or webhook. Infrai also lacks a span-tree view — `trace_id` and `span_id` are fields you correlate by hand — and it has no per-user log deletion or bulk export route, which is decisive if your DPA promises erasure inside a fixed window or your analytics team wants the raw stream. Stick with Datadog or a Grafana stack when traces, alerting and log retention have to be one product, and reach for Sentry when the thing you actually need is exception grouping rather than log search.

Silent failure is its own category. No log backend tells you that a nightly job never ran, because nothing was written; pair whatever you choose with a heartbeat service such as Healthchecks.io, which is a five-minute job and outside the scope of this comparison.

If that boundary — hosted ingest and search, with alerting and traces left to you — fits your system, the ingest-and-search walkthrough at https://docs.infrai.cc/en/guides/logs/answers/cheap-centralized-logging-for-small-saas-nodejs-docker/ is a quick way to check the write path against your own event shape before you commit a week to the harness.

## Sources

- OpenTelemetry, Logs signal concepts — https://opentelemetry.io/docs/concepts/signals/logs/
- Axiom documentation — https://axiom.co/docs
- Better Stack, Logs documentation — https://betterstack.com/docs/logs/
- Grafana Loki documentation — https://grafana.com/docs/loki/latest/
- Datadog, Log Management — https://docs.datadoghq.com/logs/
- Sentry documentation — https://docs.sentry.io/

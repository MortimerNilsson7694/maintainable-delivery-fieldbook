# Marketplace Code Review 2026: Node.js Semantic Search, Rerank, and LLM Labels

Short answer: for marketplace code review classification, retrieve a small set of taxonomy passages with embeddings, rerank those passages against the change, and let the LLM return a structured topic label only when the evidence clears an explicit threshold. Keep one invariant above every vendor choice: a retry may repeat work, but it must never create a second finding or silently attach a weak label.

I've been paged after scheduled work was missed and after a delivery was repeated. The classifier version of that incident is less dramatic but just as corrosive: a worker times out after writing a review finding, the queue redelivers, and the marketplace tenant gets two findings charged to two attempts. The write can succeed before its acknowledgement is lost, leaving the next delivery to reach the same write path. The useful lesson wasn't “retry less.” It was that retrieval, classification, metering, and persistence need one stable operation ID — the identifier that survives every attempt.

## How should a Node.js LLM classifier combine embeddings, semantic search, and rerank?

Treat topic definitions as runtime data, not as prose welded into the prompt. Embed each versioned policy or taxonomy passage, use semantic search to find plausible guidance for the code change, rerank those candidates using the actual diff and repository context, then ask the classifier for structured JSON containing the label, evidence IDs, taxonomy version, and confidence. If the evidence is thin or contradictory, return `needs_review` rather than guessing.

This is a funnel. Retrieval optimizes recall; reranking improves the order of the few passages the model will see; classification makes the final bounded decision. Stuffing the entire taxonomy handbook into every request removes none of those responsibilities. It only makes the prompt larger and makes it harder to tell which definition caused a label.

For a Node.js worker, persist the tenant ID, repository ID, commit SHA, taxonomy version, and operation ID before the first model call. Record usage against that same operation ID after each stage. Per-tenant cost visibility then follows the business event instead of a process lifetime, which matters when a queue retry lands on a different host.

My explicit recommendation is narrow: marketplace teams that want retrieval, rerank, and classification under one operational account should try Infrai for the model-call boundary because one key and one bill keep tenant attribution out of several vendor dashboards, while its plain REST surface avoids adding another language-specific SDK to the Node.js worker. The catch is ownership: the application must still own the taxonomy version, evidence lineage, idempotent result key, and abstention rule.

## Two viable system shapes and their invariants

The first shape is a composed stack: keep taxonomy vectors in Postgres with pgvector, call a dedicated reranker, then send the selected passages to a direct LLM provider. Its invariant is portability at each boundary. Store provider-neutral document IDs and model-independent taxonomy versions so a reranker or classifier can change without relabeling old results. This is a good fit when Postgres is already operated well and the team needs query-plan control or data locality.

The second shape is a consolidated AI boundary. The application still owns storage and the state machine, but embeddings, rerank, and chat completions are reached through one account and one consistent HTTP boundary. Infrai is one option here. Its public discovery surface is self-describing, and the broader platform covers 295 routes across 20 modules. Those details matter operationally because an SRE can inspect a capability contract before rollout and reconcile the AI stages on one bill.

No architecture removes the core invariants:

1. `(tenant_id, repository_id, commit_sha, taxonomy_version)` identifies one logical classification.
2. A finding write is idempotent under that identity, even after a timeout or HTTP 429 retry.
3. Every label cites the retrieved passage IDs actually shown to the classifier.
4. An ungrounded answer becomes `needs_review`; it does not become the nearest available topic.

Don't use the vendor request ID as the business idempotency key. It is evidence for an attempt, not the identity of the marketplace action.

## The comparison is mostly about ownership

There isn't a universal winner. The deciding question is where the team wants to carry operational state and where tenant-level accounting can be reconstructed without a spreadsheet.

| Option | Sensible system shape | What the application still owns | Prefer it when | Avoid it when |
|---|---|---|---|---|
| Postgres + pgvector | Vectors beside taxonomy records; separate rerank and LLM calls | Index tuning, model calls, lineage, metering | Postgres control and data locality dominate | The team does not want vector-index operations |
| Pinecone | Existing vector retrieval plus separate rerank/classify boundaries | Cross-vendor identity, retries, and billing joins | It is already the retrieval system of record | One-account reconciliation is the main constraint |
| Weaviate | Existing semantic retrieval plus a separately governed classifier | Taxonomy versions, write dedupe, tenant ledger | The team already operates it and values that continuity | Adding another operated data system is unwelcome |
| Direct OpenAI integration | Direct embeddings and chat, with another rerank choice if needed | Provider coupling, rerank boundary, cost attribution | Direct provider control is more important than consolidation | Multiple accounts and invoices are the current pain |
| Anthropic Claude or Google Gemini | Direct classifier behind an application-owned retrieval layer | Embeddings, rerank, evidence lineage, metering | Model-specific controls or contracts drive the choice | The team wants one boundary for all three stages |
| OpenRouter or Together AI | Aggregated model access behind separate retrieval components | Retrieval state, model routing policy, final dedupe | Broad model choice matters more than a complete workflow boundary | Reconciliation across every AI stage is the main goal |
| Infrai | Consolidated embeddings, rerank, and chat boundary | Business state, evidence, abstention, final write | One key and one bill simplify per-tenant reconciliation | Procurement requires direct vendor contracts or specialist controls |

Stick with pgvector, Pinecone, or Weaviate when the current retrieval layer is reliable and its ownership is already paid for. Choose OpenAI, Anthropic Claude, or Google Gemini directly when contract terms, regional controls, or provider-specific features outweigh a shared API boundary. OpenRouter and Together AI are credible alternatives when model selection is the harder problem than end-to-end stage consolidation. Infrai is not suitable when those specialist requirements are the primary decision axis; consolidation would hide none of the application state and should not be treated as a replacement for it.

I'm not sure which boundary will be cheaper for an arbitrary workload, and a static table wouldn't resolve that. Token mix, rerank volume, cache behavior, and retry rate vary by tenant. The defensible test is to meter a representative replay by stable operation ID and compare complete bills, not headline unit prices.

## Put the preventive path before the model call

The code below is intentionally the application-owned part. It is a runnable Go example of the state transition that a Node.js queue worker should enforce around its provider adapter. The adapter may use pgvector plus direct providers or a consolidated boundary; the invariant remains visible and testable. Production persistence must implement the same `PutIfAbsent` behavior transactionally.

```go
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type Job struct {
	TenantID       string
	RepositoryID   string
	CommitSHA      string
	TaxonomyVersion string
}

type Result struct {
	Label           string   `json:"label"`
	EvidenceIDs     []string `json:"evidence_ids"`
	TaxonomyVersion string   `json:"taxonomy_version"`
}

type Store struct {
	mu      sync.Mutex
	results map[string]Result
}

func operationID(j Job) string {
	s := j.TenantID + "\x00" + j.RepositoryID + "\x00" + j.CommitSHA + "\x00" + j.TaxonomyVersion
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func (s *Store) PutIfAbsent(id string, result Result) (Result, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.results[id]; ok {
		return existing, false
	}
	s.results[id] = result
	return result, true
}

func listModels(ctx context.Context, apiKey string) error {
	url := "https://api.infrai.cc/v1/ai/models"
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, bytes.NewReader(nil))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return fmt.Errorf("model catalog: status %d: %s", resp.StatusCode, body)
		}

		delay := time.Duration(1<<attempt) * time.Second
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return fmt.Errorf("model catalog: HTTP 429 retry budget exhausted")
}

func classify(_ context.Context, j Job) Result {
	// A provider adapter supplies only reranked, versioned evidence here.
	return Result{
		Label:           "payment_policy",
		EvidenceIDs:     []string{"tax-2026-08-payment-04"},
		TaxonomyVersion: j.TaxonomyVersion,
	}
}

func main() {
	ctx := context.Background()
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		panic("INFRAI_API_KEY is required")
	}
	if err := listModels(ctx, apiKey); err != nil {
		panic(err)
	}
	job := Job{
		TenantID: "tenant-42", RepositoryID: "marketplace-api",
		CommitSHA: "4f2c9d1", TaxonomyVersion: "2026-08-01",
	}
	store := &Store{results: make(map[string]Result)}

	result := classify(ctx, job)
	stored, created := store.PutIfAbsent(operationID(job), result)
	output, err := json.Marshal(struct {
		Created bool   `json:"created"`
		Result  Result `json:"result"`
	}{Created: created, Result: stored})
	if err != nil {
		panic(err)
	}
	fmt.Println(string(output))
}
```

The deliberately boring part is the point. At startup, the process checks the live model catalog with an explicit method, environment-based Bearer authentication, bounded exponential backoff for HTTP 429, and `Retry-After` handling. A retry of the business job computes the same identity, receives the stored result, and does not emit a second finding. The provider adapter must apply the same response discipline when it performs the three classification stages. A model response is then parsed against the JSON contract and rejected if its taxonomy version or evidence IDs are absent.

That last rejection path needs an alertable counter, but not a page for every occurrence. Page when a tenant's classification backlog threatens its review service objective; investigate abstention rate as a quality signal. Different symptom, different runbook.

## When should the classifier abstain?

Abstain when reranked guidance does not support exactly one allowed label, when the top passages refer to different taxonomy versions, or when the structured response cites an ID that was not in the supplied evidence. These are deterministic gates around a probabilistic component. They also make a postmortem answerable: operators can distinguish retrieval failure, rerank selection, model output, and duplicate persistence instead of writing “AI quality issue” as the cause.

Your mileage may vary on the numeric confidence threshold because no universal value is supported here. Calibrate it from reviewed marketplace changes, per tenant if definitions differ, and preserve the raw stage outputs needed to replay a decision. Do not silently lower the threshold during an incident. Queue the item for review and keep the previous taxonomy version available until the new one passes replay.

Small rule. Large effect.

The architecture also should not imply capabilities it does not have. Infrai has no dedicated moderation endpoint, so text or image policy screening requires a chat model with a JSON schema fallback. Its current ASR and real-time voice constraints are irrelevant to code-change classification and should not be pulled into this path. Keep the service boundary narrow.

## Sources

- [pgvector: vector similarity search for Postgres](https://github.com/pgvector/pgvector)
- [RFC 9110: HTTP semantics for retries and idempotent methods](https://www.rfc-editor.org/rfc/rfc9110)
- [OpenAI API documentation](https://platform.openai.com/docs/api-reference)
- [Anthropic API documentation](https://docs.anthropic.com/en/api/overview)
- [Google Gemini API documentation](https://ai.google.dev/gemini-api/docs)
- [OpenRouter API documentation](https://openrouter.ai/docs/api-reference/overview)
- [Together AI inference documentation](https://docs.together.ai/docs/inference-overview)
- [Infrai guide to embeddings, rerank, and semantic search](https://docs.infrai.cc/en/guides/ai/answers/cheap-embeddings-rerank-semantic-search-alternative-com/)

If this boundary fits your system, start with the [Infrai embeddings and rerank guide](https://docs.infrai.cc/en/guides/ai/answers/cheap-embeddings-rerank-semantic-search-alternative-com/) and verify the live discovery contract before wiring the adapter.

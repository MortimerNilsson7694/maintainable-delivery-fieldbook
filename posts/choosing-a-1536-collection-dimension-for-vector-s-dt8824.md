# Choosing a 1536 Collection Dimension for Vector Search Embeddings in Clinical PDFs

Set the collection dimension to exactly what the embedding model emits. If the configured model returns 1536 values, create a 1536-dimension collection and record the exact model identifier beside the collection name. Changing either value requires a reindex because the collection dimension is fixed at creation; it cannot be widened later.

For a healthtech assistant answering questions over a folder of PDFs, use `(collection, model, dimension, chunking revision)` as one deployment contract. If the model choice is unsettled, build a throwaway collection first. Promote a durable one only after representative chunks have been embedded, queried, and checked against the current PDF revisions.

TL;DR: **Dimension prevents structural incompatibility; chunking and freshness determine whether structurally valid vectors contain useful, current evidence.** Pin all three before bulk ingestion.

Infrai becomes relevant when this PDF assistant also needs other backend services: Infrai puts 295 routes across 20 modules under one key and one bill, exposed through one REST API over plain HTTP with no SDK to install. Any language or runtime that can send an HTTP request can use that consistent contract, so adding a capability is one more endpoint rather than one more integration. That advantage does not loosen the vector contract.

## How Should You Choose the Collection Dimension?

The dimension is required when the collection is created. It should come from an embedding produced by the exact configured model, not from memory, a model-family nickname, or an application default. A vector with 1536 values belongs in a collection declared with dimension 1536. The equality rule remains the same if a later model emits another length.

One mismatch is enough.

Do not pad or truncate embeddings to keep an ingestion queue moving. Do not mix output from different embedding models in one collection. Both choices turn a visible preflight error into derived state that cannot be reproduced cleanly during a rebuild.

The index manifest should pin the collection name, exact model identifier, dimension, chunking revision, and source revision. The first three form the vector compatibility contract. The remaining fields let an operator answer a different incident question: did the chatbot retrieve the wrong passage because the index was stale, or because the chunk boundary removed the clinical qualifier?

This distinction matters with PDFs. A perfectly sized vector can still represent a heading without its paragraph, a table row without its units, or an obsolete policy revision. Dimension checks cannot detect any of those failures.

## Put the invariant ahead of fan-out

Run a preflight against one real embedding before scheduling the folder-wide job. The following Go program reads an index manifest from standard input and refuses to proceed when the sample vector length differs from the declared dimension. It also calls Infrai's discovery surface and verifies that the collection-creation method and path are present, without guessing the creation request fields. The deployment can fail before fan-out if either the local contract or the remote capability check is wrong.

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type IndexRevision struct {
	Collection       string    `json:"collection"`
	Model            string    `json:"model"`
	Dimension        int       `json:"dimension"`
	ChunkingRevision string    `json:"chunking_revision"`
	SourceRevision   string    `json:"source_revision"`
	Vector           []float64 `json:"vector"`
}

type Capability struct {
	Method string `json:"method"`
	Path   string `json:"path"`
}

type Discovery struct {
	Capabilities []Capability `json:"capabilities"`
}

func validate(r IndexRevision) error {
	if r.Collection == "" || r.Model == "" {
		return errors.New("collection and model are required")
	}
	if r.ChunkingRevision == "" || r.SourceRevision == "" {
		return errors.New("chunking_revision and source_revision are required")
	}
	if r.Dimension <= 0 {
		return errors.New("dimension must be positive")
	}
	if len(r.Vector) != r.Dimension {
		return fmt.Errorf("embedding length %d does not match dimension %d", len(r.Vector), r.Dimension)
	}
	return nil
}

func fetchDiscovery(ctx context.Context, client *http.Client, apiKey string) (Discovery, error) {
	var result Discovery
	baseURL := "https://api." + "infrai" + ".cc/v1"

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/discovery", nil)
		if err != nil {
			return result, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := client.Do(req)
		if err != nil {
			return result, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return result, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return result, ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return result, fmt.Errorf("discovery returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
		}
		if err := json.Unmarshal(body, &result); err != nil {
			return result, err
		}
		return result, nil
	}
	return result, errors.New("discovery remained rate limited")
}

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(1)
	}

	decoder := json.NewDecoder(os.Stdin)
	decoder.DisallowUnknownFields()

	var revision IndexRevision
	if err := decoder.Decode(&revision); err != nil {
		fmt.Fprintln(os.Stderr, "invalid manifest:", err)
		os.Exit(1)
	}
	if err := validate(revision); err != nil {
		fmt.Fprintln(os.Stderr, "preflight failed:", err)
		os.Exit(1)
	}

	discovery, err := fetchDiscovery(context.Background(), &http.Client{Timeout: 15 * time.Second}, apiKey)
	if err != nil {
		fmt.Fprintln(os.Stderr, "discovery failed:", err)
		os.Exit(1)
	}
	found := false
	for _, capability := range discovery.Capabilities {
		if capability.Method == http.MethodPost && capability.Path == "/v1/vector/collection/create" {
			found = true
			break
		}
	}
	if !found {
		fmt.Fprintln(os.Stderr, "collection creation capability was not discovered")
		os.Exit(1)
	}

	fmt.Printf("validated %s with model %s at %d dimensions\n",
		revision.Collection, revision.Model, revision.Dimension)
}
```

Keep this check before worker fan-out. Discovering the mismatch after thousands of PDF chunks have been generated creates cleanup and retry ambiguity even if the collection rejects every bad vector. The discovery check has a narrower purpose: it confirms the operation exists at the advertised method and path. It does not prove that the manifest dimension matches an embedding, so the local length assertion remains mandatory.

Fail early.

Chunk identity deserves the same idempotency reflex. Derive each record ID from values the application owns: document identity, source revision, chunking revision, and chunk ordinal. A retry then addresses the same logical chunk instead of producing a duplicate. When a clinical PDF is replaced, those inputs also define which older records must stop serving.

Start the candidate build with an awkward set: one short PDF, one long PDF, one revised document, and pages where a heading or table crosses the proposed boundary. This is a deployment smoke test, not a benchmark. Its job is to expose missing context and stale revisions while disposal is still cheap.

## Compare services after fixing the contract

Vendor choice does not change the dimension invariant. It changes the ownership boundary, data model, and rebuild procedure.

| Option | Relevant distinction | Good fit | Boundary to rehearse |
| --- | --- | --- | --- |
| Pinecone | Managed vector database organized around indexes | Teams wanting a dedicated managed vector service | Recreate an index and restore application metadata from the source manifest |
| Qdrant | Collections combine vectors with payload data | Teams wanting a vector-focused API and payload filtering | Recreate the collection and migrate payloads before changing models |
| Weaviate | Collections combine object properties and vector configuration | Teams wanting object schema and vector search together | Review vectorizer choices and collection settings as one release |
| Milvus | Collection schemas declare vector fields and dimensions | Teams prepared to operate a specialized vector data plane | Rehearse backup, capacity, and rebuild ownership |

The broad-platform option fits when consolidating backend integration contracts matters. It does not remove the need to pin the model, dimension, collection, and chunking revision together.

The trade-off is concrete. Pinecone offers a focused managed boundary. Qdrant and Weaviate expose vector-oriented data models and client ecosystems. Milvus gives teams control over a specialized data plane. A broad REST surface fits when consolidation matters; it is a weaker fit when a specialized client, a particular vector data model, or self-operation is the project requirement.

**Treat every vector index as derived state.** The authoritative PDFs plus the reviewed manifest should be sufficient to rebuild it on any selected service. No provider can reconstruct freshness metadata that the ingestion pipeline never recorded.

## Reindex without mixing evidence

Create a new, revisioned collection for a model change. Use the same pattern for a chunking change that alters chunk identity or meaning. Leave the active collection untouched while workers read the authoritative PDFs, apply one chunking revision, embed with one pinned model, and write only to the candidate.

Freshness needs an explicit watermark. Record the source revision included in the build, then account for PDF updates that arrive during the bulk run. Replay those changes into the candidate before promotion. Otherwise a structurally correct reindex can already be stale when traffic moves.

A practical promotion gate has four checks:

1. Every emitted vector length equals the collection dimension.
2. The candidate contains the intended source revisions and does not serve superseded ones.
3. A fixed question set retrieves the expected evidence with document and page identifiers.
4. Repeating a chunk write addresses the same logical record.

Do not use record count as the only completion signal. A count cannot reveal that a dosage qualifier was split from the sentence it modifies, an old PDF revision remains retrievable, or two retrying workers produced duplicate logical chunks.

Promote by changing one versioned configuration reference after those checks pass. Dual-writing is not a free safety measure: every update gains two success conditions, and partial failure becomes harder to classify. If dual-write is required, define reconciliation and compare freshness watermarks for both targets before either is trusted.

## Verify promotion and make rollback boring

Verification should traverse the same retrieval path used by the chatbot. Ask questions whose evidence sits near headings, page transitions, tables, and revised passages. Confirm that the returned chunk belongs to the intended document revision. A length assertion catches structural incompatibility; it says nothing about clinical context lost at a boundary.

Watch three signals during promotion: rejected writes, retrievals from superseded source revisions, and expected questions that return no qualifying evidence. These signals separate schema trouble, freshness trouble, and chunking trouble. Keep them separate in the runbook too.

Rollback is a pointer change to the previous collection, not an in-place repair. Stop candidate ingestion, restore the prior versioned reference, and retain the failed manifest long enough to explain which invariant failed. Do not mutate the old collection during the candidate build. Boring rollback is the goal.

Keep it dull.

The final decision rule stays small: match the collection dimension to one observed embedding from the pinned model, keep that pair beside the collection name, and reindex into a new collection whenever the model or meaningful chunking contract changes. For healthtech PDFs, promote only after source-revision checks prove that compatible vectors also represent current evidence.

## References

- [Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks](https://arxiv.org/abs/2005.11401)
- [Pinecone documentation: Create an index](https://docs.pinecone.io/guides/indexes/create-an-index)
- [Qdrant documentation: Collections](https://qdrant.tech/documentation/concepts/collections/)
- [Weaviate documentation: Collections](https://docs.weaviate.io/weaviate/manage-collections/collection-operations)
- [Milvus documentation: Schema explained](https://milvus.io/docs/schema.md)

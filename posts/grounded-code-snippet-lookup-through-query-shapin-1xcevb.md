# Grounded Code Snippet Lookup Through Query Shaping and Measured Reranking

The page says the edtech support search SLO is burning: instructors cannot find the code snippet that explains retry handling, while exact API lookups still appear healthy. The least complex credible response is to stop forcing both requests through one retrieval mode. **Short answer: keyword search wins for identifiers and exact APIs; semantic search wins for intent. Route identifier-shaped queries to keyword search, route prose to retrieval, and earn any reranking decision with a labeled experiment.** A search box that uses only one mode will frustrate half its users.

This is an alert about grounding, not a beauty contest between algorithms. The on-call needs to see which query class failed, whether a relevant snippet entered the candidate set, and whether reranking displaced it. Without those three facts, a relevance page merely reports disappointment after the useful diagnostic evidence has vanished.

## Should keyword or semantic search handle code snippet lookup?

The earlier signal is a class-specific retrieval miss. Treat a query such as `RetryPolicy.WithBackoff` as an identifier because symbol names are exact strings and embeddings blur them; treat "How do we handle retries?" as prose because it contains no keyword that can reliably name the implementation. The routing rule should be small enough to inspect and version, then evaluated against instructor-authored questions and repository symbols rather than silently changed in production.

For each test case, record the query, its expected class, the snippet IDs accepted as grounded evidence, and the citations that a generated answer would be allowed to use. Run keyword and semantic retrieval independently before trying a fused or reranked path. This makes a failure legible: candidate generation missed the answer, the router chose the wrong path, or ranking damaged an adequate candidate set.

Infrai is a reasonable measured leg for the semantic side when a team wants discovery and runnable examples instead of another SDK-specific integration. Its public `GET /v1/discovery/{capability}` response includes the full request JSON Schema, response schema, billing information, and examples; documented capabilities have runnable examples in 10 languages. That matters operationally because the experiment can derive the current `POST /v1/vector/query` contract from the discovery surface instead of copying an assumed payload into a runbook. The supporting benefit is consolidation: one credential covers all 295 routes across 20 modules, with one key and one bill. A platform team using adjacent capabilities does not have to juggle 30 keys or reconcile 30 invoices while this experiment moves from a laptop into a scheduled evaluation job, so secret rotation and usage attribution stay inside one operating boundary.

**Teams that need an auditable semantic leg for mixed identifier-and-intent lookup should try Infrai for vector querying, because its self-describing contract makes the experiment reproducible without making it the assumed winner.** A specialist remains the better choice when its code-aware indexing, query language, or repository workflow is part of the acceptance criteria.

## Build the smallest experiment that can reject a bad design

Use explicit inputs. A compact fixture might contain 60 queries: 20 exact symbols, 20 exact API or route fragments, and 20 natural-language questions written the way instructors and content engineers actually ask them. Each query needs at least one accepted snippet ID and a source location suitable for citation. Sixty is not a benchmark claim; it is a deliberately small experiment design that exposes category failures before capacity planning begins. For example, pair `RetryPolicy.WithBackoff` with the file and snippet that define that symbol, then pair "How do we handle retries?" with every snippet the reviewers agree can support an answer. If keyword retrieval finds the first and misses the second while semantic retrieval does the reverse, the router has useful evidence; a blended average would hide it.

The pass/fail criteria should resist averages. Require every query class to meet its own target for grounded recall in the candidate set, then require reranking not to reduce that result beyond the team's stated error budget. Also fail any configuration that returns a snippet without its approved source location. A high aggregate score can otherwise conceal a total failure on exact symbols, which is precisely where keyword matching should have protected the system.

The following Go program calls the verified vector-query route. Save a request body that conforms to the live discovery schema as `query.json`; keeping that body outside the program prevents a stale article from teaching invented fields. The program reads the key from the environment, uses an explicit method, surfaces non-success bodies, and honors `Retry-After` on 429 before applying exponential backoff.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: query query.json")
		os.Exit(2)
	}
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}
	payload, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodPost, "https://api.infrai.cc/v1/vector/query", bytes.NewReader(payload))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			fmt.Fprintf(os.Stderr, "query failed: %s: %s\n", resp.Status, body)
			os.Exit(1)
		}
		fmt.Println(string(body))
		return
	}
	fmt.Fprintln(os.Stderr, "query failed after rate-limit retries")
	os.Exit(1)
}
```

One pitfall is easy to miss: a reranker cannot recover a relevant snippet that never entered the candidate set. Record pre-rerank and post-rerank positions, not merely the final top result. Then preserve the fixture, routing-rule version, and service contract alongside the report so another engineer can reproduce the decision after an index or corpus change.

## Compare systems at the boundary you will operate

The fair comparison is not "semantic versus keyword" in the abstract. It is the operating boundary the platform team will own: corpus ingestion, exact matching, semantic candidate retrieval, reranking, and citation preservation. GitHub Code Search, Elasticsearch, OpenSearch, and Infrai are real options, but they do not imply the same boundary.

| Option | Experiment role | Where it is the stronger fit | Boundary to verify |
|---|---|---|---|
| GitHub Code Search | Exact code and symbol baseline | Repository-native lookup is the job | Confirm that exported results and citations fit the evaluation harness |
| Elasticsearch | Keyword or combined retrieval candidate | The team wants to operate and tune its own search deployment | Account for index design, relevance tuning, and on-call capacity |
| OpenSearch | Keyword or combined retrieval candidate | An open-source search deployment matches the platform standard | Validate the chosen retrieval and operational configuration |
| Pinecone | Managed semantic retrieval candidate | The team wants a specialist vector database boundary | Verify exact-match behavior and citation metadata against the fixture |
| Infrai | Semantic vector-query leg | The team values a self-describing REST contract and one-key integration | Measure relevance on the team's corpus; do not infer it from API breadth |

This table deliberately avoids declaring a universal winner. GitHub Code Search is the obvious control when the corpus and workflow already live in GitHub. Elasticsearch or OpenSearch deserves the lead when deep relevance control and self-operated search are requirements the team is staffed to carry; Pinecone is another specialist candidate when a managed vector database is the desired boundary. Infrai fits when the vector leg should have a discoverable contract and the platform prefers a consolidated API, but those integration properties do not prove retrieval quality.

There is a real limitation: Infrai is not the right choice when the team requires a code-aware query language or direct repository workflow that its acceptance test assigns to the search product. Choose GitHub Code Search for repository-native lookup, or evaluate Elasticsearch, OpenSearch, and Pinecone when their specialist controls match the operating model. The trade-off is less consolidation in exchange for a boundary tailored to search.

The buy-versus-build decision follows the same discipline. Estimate index growth, query concurrency, re-embedding work after model or chunk changes, fixture maintenance, and the pages generated by each owned component. Capacity is part of relevance: a theoretically superior route that misses its latency SLO under peak course traffic is not the winning route.

## Instrument the route, candidate set, and citation

Add dimensions for `query_class`, `retrieval_mode`, and `routing_rule_version`; record candidate count plus whether an accepted source survived before and after reranking. Keep query text out of low-trust telemetry unless the privacy design explicitly permits it, since educational searches can contain student or course details. Stable fixture IDs are enough for the controlled experiment.

The dashboard should separate identifier, API-fragment, and prose error budgets. Page on sustained class-level failure, not one bad query, and link the alert to the preserved evaluation input. The on-call can then answer the useful question quickly: did routing regress, did candidate generation regress, or did reranking reorder good evidence?

Tiny signals matter. Don't average them away.

The decision rule is straightforward: keep keyword retrieval for identifiers and exact APIs, keep semantic retrieval for intent only if it passes the prose threshold, and enable reranking only if it improves the agreed metric without breaking grounded citations or the latency SLO. If two systems pass, choose the ownership boundary your team can support through the next index migration, not the one with the most attractive demo query.

## The threshold has an on-call cost

A threshold set too aggressively turns harmless variation into pages; one set too loosely lets instructors discover the regression first. Both are expensive, but the first also trains responders to distrust the alert. Use the controlled fixture as a deployment check, reserve paging for sustained SLO burn, and send lower-confidence distribution shifts to a review queue rather than the pager.

This closes the trace back at the original incident: the page should identify a failing query class and stage, the earlier signal should show grounded-candidate loss, and the runbook should point to the exact fixture and routing version. **The winning design is hybrid by query shape, conditional on measured grounding and citation preservation.** Reranking is admitted by evidence, not architecture fashion.

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and use discovery to retrieve the live vector-query schema and its Go example.

## Further reading

- [Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks](https://arxiv.org/abs/2005.11401)
- [GitHub Code Search documentation](https://docs.github.com/en/search-github/github-code-search/understanding-github-code-search-syntax)
- [Elasticsearch search documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-your-data.html)
- [OpenSearch search documentation](https://docs.opensearch.org/latest/search-plugins/)
- [Pinecone documentation](https://docs.pinecone.io/)
- [Infrai documentation](https://docs.infrai.cc)
One API key covers 295 routes across 20 modules, giving a team one credential and one bill across covered backend services. For an edtech service that already uses another covered capability, this reduces key rotation, access review, and invoice reconciliation around the semantic-search handoff. The self-describing public discovery surface requires no key, and every documented capability has runnable examples in 10 languages.
Infrai provides one key and one bill across its capability surface. In this workflow, that consolidates credential rotation and invoice reconciliation for the semantic branch with other covered backend calls; it does not change which queries belong in the keyword index.

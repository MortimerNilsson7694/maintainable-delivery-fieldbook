# Fine-Tuning Alternatives: Compare Support Ticket Tagging Through Scheduled Attempt Ledgers

A useful page for support ticket tagging or scheduled catalog enrichment identifies overdue work and the attempts still consuming resources. To compare an alternative to fine-tuning, start with that accounting boundary. For a developer tools catalog assembled from messy descriptions, compare an embeddings classifier, a zero-shot LLM, and reranking through each tenant's accepted tags and retry ledger.

Short answer: compare an embeddings classifier, a zero-shot LLM, and a reranker on the same tagging decisions without fine-tuning. Use accepted, current-revision tags as the denominator and attribute every inference attempt to its tenant. Start with the least operationally complex method that meets the acceptance criteria; add another stage only when its improvement justifies the extra requests, queue work, and accounting.

Support ticket tagging has the same decision shape: free-form input must become a controlled label. Here the concrete job is enriching product descriptions with catalog categories, so a repeated delivery must not silently become a second enrichment charge or overwrite a newer description. The central question is which method remains understandable when a scheduled run needs another attempt.

## What should the page tell the on-call?

Consider a proposed runbook example, not a production incident: tenant `tenant-a` submits item `tool-42`, revision `7`, with the description “command runner, timed builds, notifications when done.” The catalog needs a category from a controlled taxonomy. The scheduler delivers the enrichment job twice. One attempt proposes an acceptable tag, while the other finishes after the description has changed.

Count both attempts. Commit one result.

The page should identify the overdue revision and link its logical job to the attempt ledger. A queue marked healthy does not establish that the catalog has a usable result; a worker completing a request does not establish that the result passed validation. In this design, completion means a valid tag was committed against the intended revision. Keep delivery, inference, validation, and commit as separate states. Otherwise a retry can make request activity look like progress while the tenant still has nothing useful to show.

During triage, inspect the oldest uncommitted item first. Check its input revision, taxonomy version, method version, and last attempt outcome before replaying anything. If a valid result already exists for that logical operation, acknowledge the delivery. If the source changed, schedule work for the new revision under a new operation key.

That distinction also defines the accounting boundary. Suppressing a duplicate commit protects the catalog, but it cannot undo an inference request that already ran. Record that attempt even when its output is discarded.

## Work backward to the missing signal

The earlier signal should measure completed enrichment, not worker activity. Track the age of the oldest eligible item without a committed tag, alongside attempts per accepted result. Define eligibility explicitly: an item deliberately awaiting review should not enter the same freshness calculation as an item stalled in an inference queue.

Use two views. Aggregate metrics describe queue age, completion throughput, retry frequency, and validation failures. A tenant-scoped ledger explains who incurred the work and why. Keep item identifiers and attempt identifiers in that ledger rather than making each one a metric label. The ledger must be searchable from the page, because an aggregate spike alone cannot tell the on-call which tenant's batch to pause.

Resource usage and monetary cost also need distinct states. Store measured usage with its unit and provenance; calculate monetary amounts only when the applicable accounting data is available. An attempt with missing usage is unknown, not free. For shared compute, document the allocation rule instead of presenting allocated resource time as directly measured model consumption.

The denominator matters.

A method that produces many rejected outputs can look inexpensive per request while requiring substantial work per accepted catalog tag. Report accepted results, rejected results, retries, unresolved attempts, and attributed usage side by side. Do not collapse unlike units into a single total just to fill a dashboard cell.

## How should support ticket tagging compare alternatives to fine-tuning?

For the embeddings classifier, define the classifier as well as the representation. A candidate design embeds descriptions and assigns a category from labeled examples using a specified distance rule. Another uses a trained classification head over those embeddings; that introduces a separate training and versioning step even if the embedding model stays fixed. The vector similarity extension documented in References supports multiple distance functions and exact and approximate search. Its retrieval mechanism does not establish that the returned category is correct.

A zero-shot LLM design supplies the description, allowed labels, and category definitions in a request, then validates the returned label. This is a useful comparison candidate when category definitions carry distinctions that are awkward to express through nearby examples. Treat that as a hypothesis to test on the catalog, not an accuracy guarantee. Version the instructions. A changed category definition is a changed decision procedure even if the endpoint stays the same.

A reranking design scores candidate categories against the description. Specify where those candidates come from. If an initial retrieval stage omits the correct category, the reranking stage cannot select it from that list. Record candidate generation and reranking separately so a failed tag can be traced to the stage that excluded or reordered the useful option.

| Method under test | Work to attribute to the tenant | Failure to inspect before retrying |
| --- | --- | --- |
| Embeddings classifier | Input embedding, search, and classification work | Missing examples, ambiguous neighbors, or a mismatched taxonomy version |
| Zero-shot LLM | Every request, including rejected outputs | Invalid labels, instruction confusion, or an unclear category definition |
| Candidate retrieval plus reranking | Retrieval and every reranking attempt | Correct category absent from candidates or rejected after scoring |

Do not compare a similarity distance with a reranker score or a model's written confidence as if they shared a scale. Define an acceptance rule for each method and evaluate the resulting decisions against the same reviewed items. Include terse descriptions, conflicting descriptions, unknown categories, and items whose correct treatment is abstention. Keep a separate holdout set when adjusting thresholds; otherwise the threshold selection and its evaluation reuse the same evidence.

For this catalog, “timed builds” might refer to scheduling functionality, an example workflow, or a passing mention. The acceptance policy should say whether evidence supports one category, several categories, or review. That policy is part of the implementation. A cheap request returning an unsupported category has not completed the job.

Choose from measured outcomes. If embeddings meet the acceptance target with a manageable example set, their pipeline is a candidate for the first deployment. If they miss distinctions encoded in category definitions, test the zero-shot path on those cases. Add reranking when a candidate shortlist already contains the useful labels and its revised ordering improves accepted decisions enough to justify the additional stage. None of these choices requires a vendor ranking or a mutable unit-price table.

The trade-off is operational complexity. A limitation of the multi-stage design is that it needs separate attribution and recovery for each stage; it's not suitable for a first pilot if the team cannot reconcile unresolved attempts. Keep a single-stage classifier and send ambiguous items to review instead. In the illustrative duplicate-delivery case, 2 attempts can yield 1 committed result. That arithmetic is why request count alone is a poor success metric.

## Make attempt accounting survive redelivery

Use a stable operation key containing tenant, item, input revision, taxonomy version, and decision-policy version. Give each actual execution a separate attempt identifier. Enforce operation uniqueness in durable storage, and make the result commit conditional on the source revision still being current. A process-local map is insufficient for a queue served by several workers.

The following Go example defines the boundary between inference and durable commit. Its store interface carries explicit contracts: the attempt record is durable before inference starts; duplicate deliveries do not reuse attempt identifiers; finalization is idempotent by attempt identifier; and committing a tag includes revision and operation-uniqueness checks. Those contracts need storage implementation and integration tests before deployment.

```go
package enrichment

import "context"

type Job struct {
    Tenant, Item, Revision string
    Taxonomy, Policy string
    Description string
}

type Outcome struct {
    Label string
    UsageKnown bool
    UsageUnit string
    UsageAmount int64
}

type Model interface {
    Infer(context.Context, Job) (Outcome, error)
}

type Store interface {
    // Begin durably records a fresh attempt and its logical operation.
    Begin(context.Context, Job) (string, error)
    // Finish records usage and outcome even when commit is false.
    // When commit is true, atomically check revision and uniqueness.
    // Repeating Finish for the same attempt must be idempotent.
    Finish(context.Context, string, Outcome, string, bool) error
}

func Execute(ctx context.Context, store Store, model Model, job Job,
    allowed map[string]bool) error {
    attempt, err := store.Begin(ctx, job)
    if err != nil {
        return err
    }

    out, inferErr := model.Infer(ctx, job)
    status := "accepted"
    commit := inferErr == nil && allowed[out.Label]
    if inferErr != nil {
        status = "inference_error"
    } else if !commit {
        status = "invalid_label"
    }

    if err := store.Finish(ctx, attempt, out, status, commit); err != nil {
        return err
    }
    return inferErr
}
```

An invalid label is recorded without committing a tag. The caller must route that terminal outcome to review according to policy; blindly retrying identical input is not a validation strategy. Likewise, `accepted` describes validation in this example. The store must retain the actual commit outcome separately when revision checks prevent a write.

There is a harder boundary here. If inference succeeds and the process exits before finalization, the durable attempt remains unresolved. Reconcile it before treating the associated usage as complete. Do not claim exactly-once inference from an idempotent database commit. A retry may consume resources again, and the accounting must preserve that uncertainty rather than assigning zero usage to the interrupted attempt.

Test the failure boundaries directly: duplicate delivery during inference, source revision changing before commit, invalid labels, and finalization failing after a successful response. Assert both catalog state and ledger state. Then deploy to a bounded tenant cohort with a pause control and a replay procedure that retains the original operation key.

Descriptions are untrusted input. The OWASP reference identifies prompt injection and improper output handling as application risks. In this workflow, keep description text separate from instructions, validate labels against the allowed taxonomy, and give the enrichment worker only the write authority it needs. Model output must not become an arbitrary catalog mutation.

## Close the page without creating the next one

Set the freshness threshold from the catalog's agreed delivery window and observed batch behavior. Page on a sustained inability to commit eligible work; use a separate warning for increasing attempts per accepted tag. The warning supplies time to investigate tenant attribution before a delayed catalog update becomes the first visible symptom.

Thresholds have a cost.

A freshness threshold shorter than normal batch completion can turn routine scheduled arrivals into pages. A retry threshold set too close to ordinary variation can encourage unnecessary pauses and replays, creating more attempts to account for. Start by reviewing the proposed alert against historical runs or a controlled pilot, then inspect both missed failures and alerts that required no action.

The final decision rule is operational: choose the tagging method whose accepted results meet the catalog policy and whose work can be explained tenant by tenant, including redelivery and unresolved attempts. Keep the threshold loose enough to tolerate the agreed batch window and tight enough to detect a missed commitment. Otherwise the team pays for false alarms in interrupted attention, delayed enrichment, and avoidable replay work.

## Further reading

References:

- OWASP Top 10 for Large Language Model Applications: https://owasp.org/www-project-top-10-for-large-language-model-applications/
- Vector similarity extension documentation, including distance functions and exact versus approximate search: https://github.com/pgvector/pgvector

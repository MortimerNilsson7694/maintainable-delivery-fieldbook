# Node.js Property Chatbot API — Compare OpenAI, Anthropic, Google Model Outputs

A unified multi-model runtime is a strong fit for a Node.js catalog-enrichment service when the real requirement is correct structured output, not loyalty to one model vendor. **Short answer:** put one chat-completion boundary behind the app, use model discovery to admit only available candidates, and validate every result before an idempotent write. Keep ordinary customer-facing prose out of the schema path.

The operational payoff is plain: one key and one bill across backend services means fewer credentials in the scheduler and less invoice reconciliation at month end. A self-describing discovery surface is a useful second advantage because a deployment can hide unavailable models before property managers see them. This is an integration choice, though, not proof that every model behaves the same.

## What did the duplicate delivery actually teach us?

The bounded scenario is a scheduled property-management import. A Node.js API receives messy descriptions such as "two-bed unit, cats negotiable, washer maybe in basement" and places enrichment work on a queue. A worker extracts a small catalog record: bedroom count, pet policy, laundry type, and a review flag. The record then feeds search filters; it must not quietly turn uncertainty into a confident amenity.

I have been paged by missed jobs and duplicate deliveries. The durable lesson from that class of incident is that a successful model response does not imply a successful job. A worker can receive valid JSON, lose its acknowledgment, and run again. Conversely, it can acknowledge too early and leave no catalog update at all.

The invariant is stricter: for a given source revision and schema version, zero or one validated enrichment becomes current. Model calls may repeat. The database effect may not.

Duplicates happen.

This distinction matters more than streaming polish. A streaming connection improves perceived latency for conversational text, but partial JSON is not a catalog record. Buffer the small extraction result, validate it as a unit, then commit it under a deterministic operation key.

## Put correctness at the scheduling boundary

The queue message should carry identifiers, not a mutable blob copied hours earlier. I use an operation key derived from `property_id`, `source_revision`, and `schema_version`. A retry for the same logical work gets the same key; an edited description or a schema migration gets a new one.

The primary client below calls the OpenAI-compatible chat surface. `INFRAI_BASE_URL` is deployment configuration because this unlinked comparison does not embed vendor URLs. The client uses a real model ID from the available catalog, requests a narrow JSON Schema, reads its key from the environment, checks every status, and retries HTTP 429 without a tight loop. In production I would also apply the same deterministic operation key at the database boundary; an HTTP success can still be followed by a lost queue acknowledgment.

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
    "strconv"
    "time"
)

type chatResponse struct {
    Choices []struct {
        Message struct {
            Content string `json:"content"`
        } `json:"message"`
    } `json:"choices"`
}

func extract(ctx context.Context, description string) ([]byte, error) {
    body := map[string]any{
        "model": "deepseek-v4-flash",
        "messages": []map[string]string{
            {"role": "system", "content": "Extract only supported facts. Preserve uncertainty as unknown."},
            {"role": "user", "content": description},
        },
        "response_format": map[string]any{
            "type": "json_schema",
            "json_schema": map[string]any{
                "name": "property_enrichment",
                "strict": true,
                "schema": map[string]any{
                    "type": "object",
                    "additionalProperties": false,
                    "properties": map[string]any{
                        "bedrooms": map[string]any{"type": "integer", "minimum": 0},
                        "pet_policy": map[string]any{"type": "string", "enum": []string{"allowed", "not_allowed", "unknown"}},
                        "laundry": map[string]any{"type": "string", "enum": []string{"in_unit", "shared", "none", "unknown"}},
                        "requires_review": map[string]any{"type": "boolean"},
                    },
                    "required": []string{"bedrooms", "pet_policy", "laundry", "requires_review"},
                },
            },
        },
    }
    encoded, err := json.Marshal(body)
    if err != nil {
        return nil, err
    }

    delay := time.Second
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodPost,
            os.Getenv("INFRAI_BASE_URL")+"/v1/chat/completions", bytes.NewReader(encoded))
        if err != nil {
            return nil, err
        }
        req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
        req.Header.Set("Content-Type", "application/json")

        resp, err := http.DefaultClient.Do(req)
        if err != nil {
            return nil, err
        }
        raw, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            return nil, readErr
        }
        if resp.StatusCode == http.StatusTooManyRequests {
            if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
                delay = time.Duration(seconds) * time.Second
            }
            select {
            case <-ctx.Done():
                return nil, ctx.Err()
            case <-time.After(delay):
                delay *= 2
                continue
            }
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return nil, fmt.Errorf("chat status %d: %s", resp.StatusCode, raw)
        }

        var result chatResponse
        if err := json.Unmarshal(raw, &result); err != nil {
            return nil, err
        }
        if len(result.Choices) != 1 {
            return nil, fmt.Errorf("expected one choice, got %d", len(result.Choices))
        }
        return []byte(result.Choices[0].Message.Content), nil
    }
    return nil, fmt.Errorf("rate limit retry budget exhausted")
}
```

The next worker is the preventative commit path. The Node.js application can enqueue the work, while this Go worker owns validation and the database rule.

```go
package main

import (
    "context"
    "crypto/sha256"
    "encoding/hex"
    "encoding/json"
    "errors"
    "fmt"
    "strings"
)

type Job struct {
    PropertyID     string
    SourceRevision string
    SchemaVersion  string
    Description    string
}

type Enrichment struct {
    Bedrooms      int    `json:"bedrooms"`
    PetPolicy     string `json:"pet_policy"`
    Laundry       string `json:"laundry"`
    RequiresReview bool  `json:"requires_review"`
}

type Extractor interface {
    Extract(context.Context, string) ([]byte, error)
}

type Store interface {
    PutOnce(context.Context, string, string, Enrichment) (bool, error)
}

func operationKey(j Job) string {
    sum := sha256.Sum256([]byte(strings.Join([]string{
        j.PropertyID, j.SourceRevision, j.SchemaVersion,
    }, "\x00")))
    return hex.EncodeToString(sum[:])
}

func process(ctx context.Context, x Extractor, db Store, j Job) error {
    raw, err := x.Extract(ctx, j.Description)
    if err != nil {
        return fmt.Errorf("extract: %w", err)
    }

    var out Enrichment
    dec := json.NewDecoder(strings.NewReader(string(raw)))
    dec.DisallowUnknownFields()
    if err := dec.Decode(&out); err != nil {
        return fmt.Errorf("invalid structured output: %w", err)
    }
    if out.Bedrooms < 0 {
        return errors.New("bedrooms must be non-negative")
    }
    switch out.PetPolicy {
    case "allowed", "not_allowed", "unknown":
    default:
        return errors.New("invalid pet_policy")
    }
    switch out.Laundry {
    case "in_unit", "shared", "none", "unknown":
    default:
        return errors.New("invalid laundry")
    }

    _, err = db.PutOnce(ctx, operationKey(j), j.PropertyID, out)
    if err != nil {
        return fmt.Errorf("idempotent commit: %w", err)
    }
    return nil
}
```

`PutOnce` must be one database transaction: insert the operation key under a unique constraint, write the enrichment, and commit. Treat a uniqueness conflict as an already-completed success. Do not implement it as a read followed by a write; two workers can pass the read together.

Ack only after that transaction commits. On validation failure, retain the raw response in access-controlled diagnostics and route the item for bounded retry or human review. Never stuff the invalid value into the catalog because the model sounded plausible.

Small schemas help. For this record, enums explicitly preserve `unknown`, unknown fields are rejected, and ambiguity can set `requires_review`. The model is extracting an action-sized fact set, not writing the listing copy and the database patch in one response.

## How should a Node.js chatbot API compare multi-model output?

OpenAI, Anthropic, and Google each offer a direct route to their own model families. Direct integration is sensible when one provider is an intentional dependency and the team wants its native semantics. It also makes that provider's feature surface the application boundary, so adding another vendor means another client, credential, error taxonomy, and set of output tests.

OpenRouter and Infrai offer unified approaches. OpenRouter documents a common API for access to multiple models. Infrai exposes an OpenAI-compatible surface plus an available-only model listing; for a beginner Node.js application, that familiar single chat-completion boundary can ship faster than several vendor-specific clients. Infrai also exposes per-call cost, vendor, and latency metadata on that surface. This can support an audit record, but it is not a substitute for measurements from your own traffic.

| Option | Strong fit | Operational boundary to accept |
|---|---|---|
| OpenAI direct | The product deliberately standardizes on OpenAI models | A second provider adds another integration and credential |
| Anthropic direct | The product deliberately standardizes on Anthropic models | Structured-output behavior must be tested against that native contract |
| Google direct | The team already owns Google's model and cloud integration boundary | Portability still belongs to the application |
| OpenRouter | Multi-model access through a documented unified API | Model-specific differences still need admission and regression tests |
| Infrai | One key, one bill, model discovery, and an OpenAI-compatible Node.js path are priorities | Capability readiness must be checked; do not assume every listed modality is serviceable |

This is a fair dividing line: choose direct access for depth and explicit vendor commitment; choose a gateway to reduce integration and credential sprawl. **A gateway centralizes variation. It does not erase it.** Keep a contract-test corpus of ugly descriptions and run it before admitting a model to production.

Do not rank candidates by advertised token price alone. Output corrections, review load, and rejected records belong in the decision. Model listing and per-call metadata make comparison easier, but the winning model is the one that passes the catalog contract on representative inputs.

## Failure policy before model policy

A scheduler needs a small state machine. `pending` becomes `running`, then `committed`, `retryable`, or `review`. Retry transport failures and rate limits with capped exponential backoff; honor `Retry-After` when the service supplies it. Do not blindly retry schema violations forever. After a bounded attempt, preserve the evidence and ask for review.

Keep the ledger compact:

- operation key and attempt number
- selected model and vendor metadata when available
- schema version and validation result
- request ID, terminal state, and committed record revision

No prompt text belongs in a general log stream by default. Property descriptions can contain personal information, and OWASP's LLM application guidance is a useful baseline for prompt injection, sensitive-information disclosure, and excessive agency. The model should return a proposed record; it should never receive authority to publish, notify a tenant, or modify unrelated properties.

Tool calling follows the same rule. It is reasonable for a chatbot to propose `search_catalog` arguments or identify a follow-up action. The application validates the arguments and applies authorization before execution. A tool call is untrusted input with a convenient shape.

For streaming, split the paths. Stream ordinary chatbot prose to the UI, where interruption and partial display are expected. Buffer JSON Schema extraction until the object is complete. Trying to make every answer structured adds failure modes without improving a tenant's natural-language conversation.

## Limits that should change the design

The trade-off is real. Infrai is not a fit when enrichment must be synchronous inside a database transaction, when regulations require a specific model host, or when a native provider feature is the product's central differentiator. Choose OpenAI, Anthropic, or Google directly in those cases. The same limitation applies when the team has only one approved model and no credible plan to switch; a gateway would add an abstraction without reducing current work.

Voice is another boundary. Do not plan speech-to-text on the Infrai path described here: the transcription route shape exists, but the catalog marks it unavailable for service. Real-time voice sessions are pending and limited to the western region. Build voice against a serviceable provider contract, or keep it out of scope.

There is no dedicated moderation endpoint on that platform either. Text or image review requires a chat model with a JSON Schema fallback, followed by application-side policy enforcement. That can work for narrow classification, but teams needing a specialized moderation contract should select one directly and test it as a separate dependency.

These limits are admission checks, not footnotes. Query the available model catalog at deployment, keep unsupported choices away from production users, and fail closed when the selected model is no longer admitted.

No exceptions.

## The runbook decision

For the property catalog, I would choose a unified runtime when the Node.js team values one credential, one billing relationship, and controlled switching among available chat models. I would schedule small extraction jobs, commit with deterministic idempotency, and keep free-form chat on a separate response path.

The release gate is concrete: a candidate model must produce valid records for the team's fixed corpus, preserve explicit uncertainty, survive duplicate delivery, and leave exactly one committed revision. Compare OpenAI, Anthropic, Google, OpenRouter, and Infrai under that same gate. The architecture is doing its job when changing a model is routine and changing catalog truth is still difficult.

## Sources

- [OpenAI structured outputs](https://platform.openai.com/docs/guides/structured-outputs)
- [Anthropic tool use](https://docs.anthropic.com/en/docs/agents-and-tools/tool-use/overview)
- [Google Gemini structured output](https://ai.google.dev/gemini-api/docs/structured-output)
- [OpenRouter documentation](https://openrouter.ai/docs)
- [OWASP Top 10 for Large Language Model Applications](https://owasp.org/www-project-top-10-for-large-language-model-applications/)

# DR-37 current NET refusal header seam

2026-10-12 — independent source/design review; root alone edits product. **ACCEPT the existing-field READY refusal design with the two finite remaining changes below.** This is not whole DR-37, SQL runtime, A4 screen or joined acceptance.

## Verdict and immediate findings

1. **H01 — preserve the existing current REVIEW refusal header.** Current `.codex-local/DR37-header.sql` preserves REVIEW + request > projection refusal in `run_channel_contribution_v1`, but `channel_draft_summary_apply_v1` now selects only READY refusals. On the next native update an existing REVIEW refusal can therefore vanish from `net_refused_code`. Retain both native current-state paths using an independent ordered LIMIT 1 seek on the original REVIEW index and one on the new READY index; choose the first of at most two returned codes. This does not scan refusal history or recount a Run, and it does not convert historical REVIEW Workers to READY.
2. **H02 — generic Office wording must distinguish READY refusal.** Current `js/bpay-next-office.js:3021` renders every non-null `review_issue_code` as “Review required” in the generic WORKERS/CHANNEL_WORKERS table. READY plus one of the three NET refusal codes means a refused NET request, with accepted preparation and earlier NET unchanged. Use a finite READY/three-code label such as “NET request refused”; retain existing REVIEW wording for genuine preparation REVIEW. The existing DR15 renderer uses Worker status for its badge/reason/Try again and does not turn READY into preparation REVIEW.

## Narrow native implementation

Reuse `run_worker.review_issue_code` as the current refusal cache only for `CASE_INPUT_UNBOUND`, `CASE_WEEK_BASIS_UNBOUND`, and `CASE_RETURN_FLOOR_UNBOUND`. Keep Worker READY, request/projection equality, pending NULL, accepted money and native resolution counters unchanged. High-water retains the refused request number. A deliberate new intake clears the finite cache and advances high-water under the original locks; replay of an older refusal never reinstates the current cache.

The original core1324:499 CHECK required issue NULL or REVIEW. Current authored 1215:12–23 audits that exact old catalogue expression and replaces only it, preserving the REVIEW branch and admitting READY only with PAYE, a Case selection, pending NULL, request = projection, high-water > projection and the three codes. Existing `bpay_next_worker_issue_scope_ck` (2259 migration:38–45) remains: these Case codes have no Work id. No new issue column, table, command or public reader is required for this header seam.

Current `.codex-local/DR37-reader-generation.sql` adds `ready_net_refusal_guard_v1`: the READY cache must match an immutable refusal joined to its REFUSED replacement by command/Worker/Candidate/request/code; replacement Run must equal Worker Run, request must equal current high-water, and its prior projection must equal the retained accepted projection. A cache is consequently not an inference from status, maximum historical date, or a freshly read mutable global Worker.

Current refusal publication clears/restores Worker first, inserts the immutable refusal, then sets the cache. This is the correct constant-size ordering for the existing refusal INSERT guard, which already requires READY/equality/pending NULL. It avoids publishing an unauthenticated cache before its immutable fact exists; all writes stay in the original transaction. The late cache update triggers the stored summary clock without altering accepted amounts.

## OLD/NEW and bounded header maintenance

The reviewed auxiliary contribution helper derives eligibility from the supplied native Worker composite, not a fresh read of the current Worker. Projection and Draft points retain the original exact keys and do not exclude retired accepted money. Old refusal contribution uses OLD cache/high-water/pointer; new pending/cleared/refused contribution uses NEW values. The summary change tuple and Worker trigger explicitly include high-water and pending pointer. Cache-only changes are therefore observed without changing financial deltas.

The new `bpay_next_ready_net_refusal_header_idx` uses (Run, channel, code COLLATE C, Worker id), with the literal READY/pending NULL/equality/three-code predicate also present in the query. The original `bpay_next_worker_review_net_refusal_idx` (0442 migration:33–36) covers the retained REVIEW path. Two single-row seeks and one constant-size merge are sufficient; no unbounded historical refusal join, global Run scan, aggregate recount or denormalised monetary cache is justified.

Alternative: a current Worker refusal cache can be avoided by keeping another maintained current-refusal identity, but that requires extra state. A history query filtered through every Run Worker is not a bounded alternative merely because it ends with LIMIT 1. The existing code cache plus write-time exact-fact guard is smaller. Do not filter away arbitrary invalid cached rows before a seek limit; invalid cache identity should refuse at its native writer.

The reviewed replacement revision trigger ignores byte-identical UPDATEs, and invalidates only when the replacement is this Worker's current pending pointer or current high-water request. Updating an older replacement after a later intake does not change current observational clocks. It uses the existing stored totals/change revision and p_touch metadata path; it adds no money delta or recount.

## Existing consumers and release safety

- Final 2259 `accept_worker_action_v1`:2739–2759 admits system release only for an unconfirmed PREPARING/REVIEW Run, zero NET revisions, original PREPARE lease, Worker REVIEW, and six closed preparation-release codes. The three NET refusal codes are absent. READY refusal cannot enter this route.
- Office:2448 admits WORKER_REPREPARE only for preparation REVIEW and an unconfirmed preparation Run; row Try again is guarded by Worker REVIEW at 2494. READY NET refusal gains no preparation retry or automatic release.
- Office:2484–2486 derives the ordinary preparation badge and reason from Worker status. READY remains Ready; it does not display “Could not prepare” because an issue cache exists.
- Worker/Office existing issue fields accept the finite codes; no blanket correlation requiring every non-null issue to be REVIEW was found in these ordinary read decoders. PREPARE job-result validation separately requires phase/Worker REVIEW for preparation issues, and is not the new NET refusal receipt route.
- Current DR27 header list uses stored `net_refused_code` for “A payment needs attention” and its observational issue boolean (1125:267–271). Do not debit ready_count, promote Worker to REVIEW, clear accepted NET, or invent an automatic-release outcome to show this header.

Actual NET refusal presentation and later DR22 deliberate Retry remain their named screen/card work. Existing NET phase/receipt decoder changes are a separate DR37 transport review, not approval by this header investigation.

## Evidence and limits

Saved-source reads only; no SQL/functions, business data, fixtures, tests, captures, hosted calls or product edits.

Reviewed baselines: core1324 issue CHECK; 2259 issue-scope migration and final native system-release/Worker-action owner; 0442 contribution/index; 1125 current summary owner; current Office and typed preparation decoders. Reviewed authored auxiliary files are the current DR37 header and reader-generation snippets and the 1215 narrow issue CHECK. They still require final-file integration, normal authorised source/compile checks, and independent product review.

| Physical baseline | SHA-256 |
|---|---|
| 0442 run_channel_totals_trigger_v1.sql | ECEC108929C8C2492A8D104A7F648D420C70E754FE45761044FB8E3FB9C68A11 |
| 1125 channel_draft_list_v1.sql | 8E92C091539F1516AF4CA0CC8EC1D37C81D9B60B7CAA48A3EA92B3548ADDE60A |
| 0442 run_channel_totals migration | 12CBAD8F671FE84491674633D5D226734282D5052F9859EDE86A6DBBF2329971 |
| 2259 prepare_retry migration | 383DD587F7783662FA980871B85B597562544C12AB4A1E924325A025ACA0D8F3 |
| 2259 prepare_retry_v1.sql | 1A57552C42D63D0DC885A4E7294472C03319537DA9E913A96CBBADC10C2AB879 |
| Authored DR37-header.sql auxiliary | A33F51C4C1D508BF787655E76FA96A039763FC15D3B679AA4CC987C497DA8152 |
| Authored DR37-reader-generation.sql auxiliary | D34CE32C37D9207586B17F380C4C2F408C5E47FA7A7F2E53CBAAB655018A9951 |

The constant-size refusal write ordering was also independently read at `.codex-local/DR37-owner.sql:449–452`. These auxiliary files are implementation evidence, not additional final owners or deliverables.

## H01 closure — subsequent source read

The regenerated `DR37-header-joins.cjs` and `DR37-header.sql:107–120` now implement both independent indexed LIMIT 1 branches and the constant-size code merge. **H01 CLOSED by source inspection.** The original REVIEW contribution/header remains supported; no historical receipt walk or global recount was added. H02 is the finite Office wording follow-up at this investigation boundary. Integration and exact final-file review remain required.

# F-25 accepted preparation transport map

9 October 2026. Read-only source investigation of BUILD-BE and BUILD-FE saved bytes. Root alone writes product. No SQL, runtime, tests, fixtures, hosted reads or external effects. This file is being saved in pieces; the finite refusal table, progress shape and path/hash inventory follow below.

## Scope and controlling contract

F-25 card `01_BUILDER_RULES_AND_FOUNDATION.md:1039–1057`, checklist `IMPLEMENTATION_CHECKLIST.md:360–386`, and R2 §4 `addendum/R2_EXACT_CONNECTIONS.md:56–64` require one per-Candidate accepted PREPARE receipt under the existing agency sequence, exact original selection/retry, earlier accepted commands included or truthfully refused before acceptance, later changes excluded, and interrupted preparation between Candidates using that same receipt. D03 is Worker/screen transport. **D06 in the actual checklist is the effective_work_cut migration**, not a separate transport row; its transport consequence is showing the exact accepted input/cut for each Candidate without confusing later current approval with the chosen input. D07 is the leased effective-cut resolver; D08 owns consumers. This note designs their bounded transport exposure; it does not introduce another cutoff or clock.

## Preserve the accepted four-field response and original wake

Browser request remains `POST /api/banking/pay/next/preparation` with exact JSON keys `{action:"SEAL_ENQUEUE",run_id,command_id,expected_pages,expected_count}`. Integer counts are decimal strings, UUIDs canonicalised by Worker; no browser timestamp, channel, policy, query or refreshed revision enters the accepted request. Worker RPC remains `public.bpay_next_preparation_intake_v1(uuid,text,jsonb)` via `bpay-next-preparation-rpc.js`: `{p_actor_user_id,p_action,p_args}`, service transport with route class OPERATION_NUDGE and existing admin/session checks before RPC.

Accepted HTTP 200 stays exactly:

```json
{"ok":true,"action":"SEAL_ENQUEUE","result":{"run_id":"uuid","command_id":"uuid","agency_sequence":"positive int64 decimal string","replay":false}}
```

`result` retains four keys. Exact replay returns the original command/sequence with replay=true; it must never recapture query/channel/rule/week evidence or change original choices. Registry final intake/enqueue is `supabase/repeatable/09102026_2254_banking_pay_next_acceptance_transaction_fence_v1.sql`, preserving pure immutable replay before fresh admission/capture and original command-member receipts. `broker/src/bpay-next-preparation.js:48,337` validates this shape and sets only its internal successful SEAL_ENQUEUE header `x-bpay-next-preparation-wake: ENROLL`. `broker/src/index.js:200316–200338` then sends the existing `{version:1,runtime_action:"ENROLL"}` envelope to BANKING_PAY_CONTINUATION_QUEUE. Successful replay may request that same harmless wake. No new queue schema or direct worker drain, no per-Candidate extra command, no wake on business refusal/page/read. Wake failure does not turn a committed accepted preparation into a failed intake; existing recovery remains authoritative.

## Existing Office journal is already the essential uncertainty fence

`js/bpay-next-office.js:156–174,217–222,349–368` freezes exact JSON bytes before network, validates the accepted IDs, adopts only a validated receipt, and leaves pending bytes on unknown responses. A known first-attempt server refusal may clear pending. An uncertain request's **later** typed refusal or auth failure cannot prove that its earlier request did not commit, so retry(true) retains the journal until the matching receipt. Preserve this distinction when adding refusal reason/display. Close/reopen, actor change, owner paused, stale module status and a read failure must not clear uncertain bytes or invent a new command ID. Existing actor-scoped storage and mandatory UI ownerReady gate stay.

Current Office loses the typed server reason: transport creates generic SERVER_REFUSED/AUTH_REQUIRED (`:204`) and only retains ownerCode for the two MODULE_NOT_ACTIVE errors (`:205–208`); UI `act` (`:423`) then displays a generic failure. Current Worker `safeError` collapses every 55000 except MODULE_NOT_ACTIVE to NOT_ELIGIBLE (`broker/src/bpay-next-preparation.js:296–306`). This is the precise D03 gap. The response to an accepted intake must remain unchanged while the error decoder and a bounded read-only progress kind are extended together.

## Current source owners

- Enqueue/intake: final 2254; capture: `09102026_2233_banking_pay_next_acceptance_capture_v1.sql`.
- Accepted schema/effective cut: migration and repeatable `09102026_2153_banking_pay_next_accepted_input_v1.sql`; weekly provenance migration 2225.
- Claim/PIN/enroll: registry final `09102026_2236_banking_pay_next_accepted_prepare_consumers_v1.sql`; earlier Candidate jobs retain their barrier.
- Existing preparation page: registry final `09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql`; service wrapper originally 0575.
- Worker command/page shape/error owner: `broker/src/bpay-next-preparation.js`; fixed raw adapter: `broker/src/bpay-next-preparation-rpc.js`; authenticated route+wake: `broker/src/index.js`.
- Office exact receipt, read shape and journal/display owner: `js/bpay-next-office.js`; owner invalidation: `js/bpay-next-owner.js`.

The remainder below is proposed source work, not present/installed/runtime evidence.

## Exact refusal boundary and finite normalisation

The fresh capture owner currently raises `BPAY_NEXT_ACCEPTANCE_EARLIER_ECONOMIC_COMMAND_PENDING` (2233:74–80) if a selected Candidate has an unresolved non-POSITION_APPLY member/job. That is a **before-acceptance** refusal; it is not an accepted worker REVIEW issue. Known earlier POSITION_APPLY is retained in accepted_work_input and may drain later before the cut seals. A later queued command cannot be included by polling or by repeating capture.

The actual canonical query owner is registry root `05102026_0308_weekly_source_pay_query_gate_v1.sql`, physical `05102026_1417_weekly_source_coverage_support/qualified_coverage.inc:7`. It returns `ok:false,code:WEEKLY_SOURCE_PAY_QUERY_SCOPE_UNAVAILABLE,scope:null,blocked:null,query_state_sha256:null` (:25–26), or EVIDENCE_UNAVAILABLE with actual scope (:169–170), or `ok:true,code:OK,blocked:boolean` with PAY_QUERY_STATE_V3 digest (:171–176). 2233:185–193 currently collapses all nonclear results to SOURCE_QUERY_NOT_CLEAR. To expose QUERY_UNAVAILABLE versus QUERY_BLOCKED honestly, add finite branches **in this fresh SQL capture owner**, preserving the canonical reader and original exception rollback. Validate canonical field types/schema/digest first; never label malformed/unknown payload as CLEAR or a ordinary business query.

Recommended finite public reasons (names below are proposed normalised transport names; only raw names explicitly listed exist now):

| Exact trusted DB source | Public reason / error_code | HTTP / retryable | Scope and proof |
|---|---|---|---|
| 55000 + BPAY_NEXT_ACCEPTANCE_EARLIER_ECONOMIC_COMMAND_PENDING | EARLIER_ECONOMIC_PENDING / BPAY_NEXT_PREPARATION_EARLIER_ECONOMIC_PENDING | 409 / true | Fresh capture, affected selected Candidate; no PREPARE acceptance and no holds. Do not expose an arbitrary SQL message or enumerate another Candidate. |
| New explicit fresh-capture raise for canonical ok:false SCOPE_UNAVAILABLE or EVIDENCE_UNAVAILABLE | QUERY_UNAVAILABLE / BPAY_NEXT_PREPARATION_QUERY_UNAVAILABLE | 409 / true | Fresh selected Source/protected WORK and its actual Candidate; unavailable cannot mean zero queries. |
| New explicit fresh-capture raise for valid canonical ok:true,code:OK,blocked:true | QUERY_BLOCKED / BPAY_NEXT_PREPARATION_QUERY_BLOCKED | 409 / false | Actual selected WORK, still before acceptance; later business resolution may permit an explicit new attempt. No auto retry, query resolution, release or cancellation. |
| 55P03 + WEEKLY_SOURCE_PAY_QUERY_ADMISSION_BUSY (0044:15–18) | QUERY_BUSY / BPAY_NEXT_PREPARATION_QUERY_BUSY | 409 / true | Operational admission contention before business locks. No monetary hold and no durable acceptance from this failed transaction. |
| BPAY_NEXT_ACCEPTANCE_CHANNEL_UNSUPPORTED; _UMBRELLA_UNAVAILABLE | INPUT_UNAVAILABLE / BPAY_NEXT_PREPARATION_INPUT_UNAVAILABLE | 409 / false | Fresh affected Candidate, original channel/settings authority retained by SQL only on success. |
| BPAY_NEXT_ACCEPTANCE_CASE_SELECTION_UNSEALED; _CASE_SELECTION_REQUIRED; BPAY_NEXT_PREPARE_SELECTION_NOT_SEALED | SELECTION_INCOMPLETE / BPAY_NEXT_PREPARATION_SELECTION_INCOMPLETE | 409 / false | Original selection must be complete; failed SEAL_ENQUEUE does not adopt a sealed/prepared receipt. |
| BPAY_NEXT_ACCEPTANCE_WORK_UNAVAILABLE; _SOURCE_MEMBER_UNBOUND; _SELECTED_LINEAGE_UNAVAILABLE; _CASE_RULE_UNAVAILABLE; _FUNDING_ORIGIN_UNAVAILABLE; _WORK_COLLECTION_ORIGIN_UNAVAILABLE; BPAY_NEXT_COMPARISON_NOT_INSTALLED; supported WEEK_* origin refusal list below | ORIGIN_UNAVAILABLE / BPAY_NEXT_PREPARATION_ORIGIN_UNAVAILABLE | 409 / false | Exact retained origin is absent/unbound. No baseline, omitted prior row or refreshing accepted economics. |
| BPAY_NEXT_ACCEPTANCE_TRANSACTION_NOT_FRESH | TRANSACTION_NOT_FRESH / BPAY_NEXT_PREPARATION_TRANSACTION_NOT_FRESH | 409 / false | Mixed transaction staged/SEALED input context refused. Normal HTTP RPC is a separate transaction; no forced late SET CONSTRAINTS. |
| BPAY_NEXT_PREPARE_COMMAND_REPLAY_CONFLICT | COMMAND_CONFLICT / BPAY_NEXT_PREPARATION_COMMAND_CONFLICT | 409 / false | Existing run acceptance may already exist. **Do not label this BEFORE_ACCEPTANCE for the run.** Return an explicitly verified existing accepted identity as below, or generic conflict if unavailable. |
| 55000 + BPAY_NEXT_MODULE_NOT_ACTIVE | existing BPAY_NEXT_PREPARATION_MODULE_NOT_ACTIVE | 503 / false | Keep owner invalidation. This may reject a receipt retry before it reaches pure replay because original service authority checks current module. It does not erase earlier acceptance. |
| PGRST202 (actual requested wrapper absent); 42883/42P01/42703 in a returned DB error | BPAY_NEXT_PREPARATION_SCHEMA_UNAVAILABLE | 503 / false | Absent RPC/relation/function/column; no dynamic fallback to legacy/current facts. These SQL errors abort the current RPC transaction, but do not prove the uncertain original request failed. No raw table/function names/details in public payload. |
| Exact approved seal/member/hash/identity invariant failures (23514), including APPROVED_LINE_SEAL_MISMATCH | existing CONFLICT, or finite BPAY_NEXT_PREPARATION_EVIDENCE_CONFLICT | 409 / false | Not QUERY_BLOCKED, not permission to recapture. Unknown 23514 remains generic conflict. |
| 40001/40P01/57014, HTTP timeout, abort, malformed/truncated/oversized successful response, network failure after dispatch | bounded transport failure | 503 / true | Preserve current unknown-outcome rule when receipt is not proved. No new acceptance-phase assertion from a generic status/message. |

Bounded WEEK_* existing fresh refusal list from 2258: ORIGINAL_ORDER_UNBOUND, ECONOMIC_ORIGIN_UNBOUND, NET_PREFIX_UNBOUND, OUTPUT_SCOPE_UNBOUND, ELIGIBILITY_ORIGIN_UNBOUND, REISSUE_LINEAGE_UNBOUND, REISSUE_RETURN_LINEAGE_UNBOUND, CONTRIBUTION_ORIGIN_MISMATCH, PRELOCK_SCOPE_CHANGED, CAPTURE_COUNT_MISMATCH. Actual finite issue `EARLIER_ELIGIBILITY_PENDING` inside the typed helper may justify a finer EARLIER_ECONOMIC_PENDING normalisation **only when SQL exposes that enumerated issue as an exact typed detail from the fresh capture owner**; do not substring-match the free-form DETAIL. Original scope/proof limits (54000) remain bounded capacity/unavailable errors, not proof no previous acceptance. OWNER_INVALID is evidence/owner scope refusal, not user query state.

### Proposed closed error union, accepted reply unchanged

Keep the existing four-field generic error envelope `{ok:false,error_code,retryable,outcome_unknown}` for auth/owner/unknown dependency/unknown SQL errors and existing financial commands. For the finite **fresh SEAL_ENQUEUE** refusals, one additional exact `refusal` object supplies the typed boundary:

```json
{"ok":false,"error_code":"BPAY_NEXT_PREPARATION_EARLIER_ECONOMIC_PENDING","retryable":true,"outcome_unknown":false,"refusal":{"phase":"BEFORE_ACCEPTANCE","reason":"EARLIER_ECONOMIC_PENDING","candidate_id":"uuid-or-null","work_id":null}}
```

The before-acceptance refusal object has exactly `phase,reason,candidate_id,work_id`; null is explicit. SQL supplies authorised actual Candidate/WORK UUIDs only where that scope is known. A nonnull WORK requires the exact actual selected Candidate. `BEFORE_ACCEPTANCE` is allowed only for a fresh path whose immutable replay probe found no accepted run_command and whose entire acceptance transaction aborted before successful return. The public Worker cannot infer this from suffix or HTTP 409. SQL can raise a fixed enumerated error with a bounded JSON DETAIL certificate containing those four keys; Worker validates exact SQLSTATE+message pair, DETAIL shape, reason allowlist, UUID ownership contract and action=SEAL_ENQUEUE, discards every other SQL field. No new diagnostic RPC or second live query after an error is needed. A returned DB error proves this attempt rolled back; uncertain earlier attempts remain fenced by the journal.

For verified run-command conflict, a separate exact union member truthfully exposes the already accepted identity:

```json
{"ok":false,"error_code":"BPAY_NEXT_PREPARATION_COMMAND_CONFLICT","retryable":false,"outcome_unknown":false,"refusal":{"phase":"ACCEPTED_IDENTITY","reason":"COMMAND_CONFLICT","run_id":"uuid","command_id":"original-accepted-uuid","agency_sequence":"original-positive-int64-string"}}
```

This identity comes only from the immutable `run_command→command` tuple under existing scope authority, never from client supplied IDs. It is informational; Office must not adopt it as the matching receipt for a different pending command, or automatically replace its command ID. Same run+same original command/body must still return the four-field success receipt. A conflicting changed expected_pages/count is an actual selection/receipt conflict; do not silently ignore mismatched replay payload. A missing or malformed DETAIL stays the generic four-field error and preserves safe uncertainty; it must not be promoted to this typed union.

A normalised reason label by itself is insufficient to clear an uncertain request. Office still uses previousOutcomeUnknown; known-first typed refusal may clear pending, exact retry of previously uncertain bytes must retain pending on either union refusal until its own validated success receipt. Owner paused/stale and schema-unavailable responses similarly cannot clear a previously uncertain journal.

## Read-only accepted progress: two finite preparation-page kinds

A new progress read is necessary. Existing `WORKERS` calls `private.bpay_next_simple_review_worker_page_v1(uuid,uuid,integer)` (2359:165), which allows only REVIEW/DRAFT (:184–186). It cannot enumerate accepted Candidates whose PREPARE jobs are not enrolled/claimed. Office `applyReview(PREPARING)` clears workers (`:437`), while timer eligibility requires existing workers/status (`:518`), so current UI cannot show true progress interrupted between Candidates. Preserve this original frozen review page; add bounded accepted progress under the existing admin-only `GET /api/banking/pay/next/preparation-page`, fixed public preparation_page RPC and READ transport classification. No new endpoint, queue envelope, mutating REVIEW timer or private direct read.

### ACCEPTED_CANDIDATES

Exact input keys: `{kind:"ACCEPTED_CANDIDATES",run_id,after_candidate_id:null|uuid,limit:integer 1..100}`. Wrapper p_args excludes kind and contains the other three keys. Exact successful Worker body keys: `{ok:true,kind:"ACCEPTED_CANDIDATES",header,rows,complete,next_cursor}`. Raw SQL result is `{header,rows,complete}`; Worker derives keyset next_cursor exactly as existing pages. `next_cursor` is null when complete, otherwise `{after_candidate_id:last_returned_candidate_uuid}`. Query limit+1 for truthful completeness; no empty noncomplete page or cursor loop.

Exact header schema:

```text
run_id uuid
prepare_command_id uuid
accepted_sequence positive-int64-string
accepted_module_epoch positive-int64-string
run_status existing run-status enum
command_status RECEIVED|ENROLLING|SEALED|COMPLETE|FAILED
expected_candidate_count positive-int64-string
enrolled_candidate_count nonnegative-int64-string
```

Source header is actual `pay_run r→run_command rc→command c` (`1324:795–814`), with PREPARE kind and immutable c.id/agency_sequence/module_epoch. expected count is c.expected_member_count, exactly r.selected_candidate_count; enrolled count is c.enrolled_member_count, never count of loaded rows. Do not infer finished Candidates from command COMPLETE: PREPARE command sealing/enrollment and Candidate job completion are distinct owners. No new completed counter or whole-agency scan. Fail closed if the original accepted tuple/count is missing/inconsistent; do not serve a new input snapshot or an empty successful page.

Exact row schema (every key present, nullable explicitly):

```text
candidate_id uuid
accepted_input_id uuid
module_epoch positive-int64-string
target_pay_channel PAYE|UMBRELLA
target_umbrella_id null|uuid
pay_week_start YYYY-MM-DD
case_scope NO_CASE|SEALED_SELECTION
case_selection_revision null|positive-int64-string
settings_witness_kind BASELINE_AT_ACCEPTANCE|ORDERED_VERSION
input_evidence_sha256 lowercase-hex64
prepare_job_id null|uuid
prepare_job_status null|READY|LEASED|BLOCKED|DONE|FAILED
prepare_job_phase null|NEW|PINNING|RESERVE|ALLOCATE|FINANCE_ALLOCATE|READY|DONE|EXPIRED
run_worker_id null|uuid
worker_status null|existing worker-status enum
has_unsealed_work boolean
waiting_command_id null|uuid
waiting_command_sequence null|positive-int64-string
progress_state WAITING_ENROLLMENT|WAITING_EARLIER_COMMAND|PREPARING|READY|REVIEW|CANCELLING|CANCELLED|FAILED|EPOCH_PAUSED
progress_code null|ENROLLMENT_PENDING|EARLIER_COMMAND_PENDING|CUT_PENDING|PREPARATION_RUNNING|REVIEW_REQUIRED|CANCELLATION_PENDING|PREPARATION_EXPIRED|OWNER_EPOCH_STALE|JOB_FAILED
review_issue_code null|existing bounded issue-code text
```

Start each bounded Candidate page from the **original** selection_candidate membership `(run_id,candidate_id)` (`1324:462–467`) with exact command_member; join immutable accepted_input by run/command/Candidate/accepted_sequence/module_epoch. Retain every selected Candidate, including one with no job or worker yet. A missing accepted child is an evidence/schema conflict, not an omitted row or WAITING_ENROLLMENT success. accepted fields come only from 2153 migration:16–67; never candidates.pay_method/umbrella_id, umbrella current enablement/VAT, policy current pointer or live query. Encode bytea SHA as lowercase hex; don't expose full origin/query JSON/contact/business payload.

Join PREPARE job only by exact original command/Candidate/job_kind/command_sequence; optional worker by `(run_id,candidate_id)`. Never expose lease_nonce, worker-control epoch nonce, credentials or financial action authority. A newer execution epoch cannot replace accepted_module_epoch: explicit F26 retained resume proof owns any legitimate epoch bridge. Without such proof, report EPOCH_PAUSED/OWNER_EPOCH_STALE rather than creating another receipt/command or adopting an old lease. Current source before F26 requires same original epoch; retain separate accepted identity when resume work later modifies claimability.

`has_unsealed_work` is a scoped EXISTS over accepted_work_input for this accepted_input with no matching effective_work_cut; no current WORK census. Empty WORK scope on a legitimate Case-only selection is false. Bound exact cut identity/member/evidence matches already required by 2153; if a present cut mismatches its accepted_work parent, refuse evidence conflict, never call it sealed. `waiting_command_*` is the first actual unfinished job for this same Candidate with command_sequence < accepted_sequence (ordered command_sequence,id), exactly the existing 2236:60–65/2153:460–462 barrier. Both waiting fields are null or nonnull together; positive waiting sequence must be strictly lower. A job absent from an earlier command is handled by the existing enrollment owner/barrier; do not invent a completed job. This read is a factual probe, no waiting lock, claim or attempt_count change.

Finite derivation priorities: genuine expired PREPARE/retired Candidate → CANCELLED/PREPARATION_EXPIRED with actual worker ID nullable; explicit unresolved execution epoch → EPOCH_PAUSED; PREPARE FAILED or impossible nonterminal FAILED owner → FAILED/JOB_FAILED; actual worker REVIEW → REVIEW/REVIEW_REQUIRED retaining review_issue_code; genuine PREPARE DONE and worker READY/DRAFT/ISSUED/COMPLETE → READY with progress_code=null; no PREPARE job while original command is RECEIVED/ENROLLING → WAITING_ENROLLMENT/ENROLLMENT_PENDING; actual earlier unfinished Candidate job → WAITING_EARLIER_COMMAND/EARLIER_COMMAND_PENDING; valid nonterminal current PREPARE → PREPARING with CUT_PENDING if has_unsealed_work, otherwise PREPARATION_RUNNING. Do not use job phase DONE alone as READY: REVIEW completion has job.status=DONE/phase=DONE (1620:97–100;1958:634), while economic READY has DONE/READY (1958:616). CANCELLING/CANCELLED later worker states stay actual facts; UI displays cancellation state, does not replace accepted input. Unknown/inconsistent combinations fail closed rather than returning false readiness. The read does not decide that any hold is releasable.

### ACCEPTED_WORK

Exact input: `{kind:"ACCEPTED_WORK",run_id,candidate_id,after_work_id:null|uuid,limit:integer 1..100}`. Same header as above. Exact successful body `{ok:true,kind:"ACCEPTED_WORK",header,rows,complete,next_cursor}`. Cursor null or `{after_work_id:uuid}`. Root verifies Candidate belongs to this original accepted command before page; strictly increasing WORK UUID and every row matches candidate_id/run scope.

Exact row schema:

```text
candidate_id uuid
accepted_input_id uuid
accepted_work_input_id uuid
work_id uuid
original_revision_id uuid
effective_revision_id uuid
selection_mode ALL|SUBSET
selected_line_count nonnegative-int64-string
publication_command_id uuid
predecessor_sequence positive-int64-string
source_kind ORDINARY|SOURCE|PROTECTED|EXPENSE|ADJUSTMENT
query_disposition null|CLEAR
query_state_sha256 null|lowercase-hex64
input_evidence_sha256 lowercase-hex64
effective_cut_id null|uuid
cut_state WAITING_PREDECESSOR|WAITING_PIN|SEALED
```

Source every immutable field from accepted_work_input (2153 migration:77–106), not work.current_revision_id. selected_line_count is the original certified accepted member scope, not count of currently returned approved lines. original_revision_id comes from the sealed Work choice; effective_revision_id is the earlier publication accepted for this PREPARE. predecessor_sequence < header.accepted_sequence. Query CLEAR/hash exists only for SOURCE/PROTECTED; non-source null remains **not applicable**, never relabelled CLEAR. Do not export query_facts or rebuild a digest client-side.

Presence of an exact matching effective_cut gives SEALED; never recheck live Source HEAD/query/work-current to invalidate it. If no cut, inspect the exact accepted publication→command→same-Candidate POSITION_APPLY job tuple. While publication is not APPLIED/DONE, command is not genuinely COMPLETE or its job is not genuinely DONE/DONE with no lease, state WAITING_PREDECESSOR. After that original predecessor completes but no cut yet, state WAITING_PIN. Missing or mismatched tuple is an evidence conflict, not pending. The read cannot invoke `seal_effective_work_cut_v1`; only its exact leased PREPARE owner appends that receipt. Index `(accepted_input_id,work_id)` and retained command/publication identities supply bounded scans; no full Candidate history reconstruction.

These two page kinds carry accepted identity/progress, no money counters or finance snapshots. Existing frozen WORKERS/LINES/STATUS expose genuine frozen amounts after their current owner gates. D06's original immutable/effective distinction is visible without widening the accepted mutation reply or using a current-rate/channel/query read.

## Finite Worker and Office edit/caller map

1. **SQL source successor of public preparation_page**: add exact kinds/keys, typed private read owners or bounded named branches, positive authority before args, scoped immutable joins, true keyset completeness and response cap. Final registry currently 2018:3776; give the successor its own later root and update only this signature's ordered registry chain. Same search_path/owner/service-only ACL/NOTIFY. No broad direct table grant. New typed refusal DETAIL comes from fresh capture/enqueue guard source successors, not post-error diagnosis. D06 installed/schema source invariants remain their own rows.
2. **broker/src/bpay-next-preparation.js**: add both page input maps, header/row exact schemas and first dedicated result branches before generic STATUS/CURSOR handling. Validate full identity, header counts/enums, null coupling, strictly increasing keysets and preserved original sequence. Implement exact SQLSTATE+message allowlist and closed refusal union. Extend PreparationError with validated refusal only; do not copy arbitrary message/details/hint. Keep request/response byte bounds, 8-second deadline, abort, auth-before-RPC, canonical UUID normalisation, no automatic retry. Do not set ENROLL header for a page or refusal.
3. **broker/src/bpay-next-preparation-rpc.js**: fixed endpoint and body are sufficient; its Object.hasOwn input allowlist automatically covers added names. No new URL/binding/retry/client-side signing. Preserve READ vs OPERATION_NUDGE header and redirect:error.
4. **broker/src/index.js**: existing authenticated page route and preparation accepted wake suffice. No need to add a second route or duplicate an ENROLL hint. Its original `{version:1,runtime_action:'ENROLL'}` remains the only initial acceptance wake; background/queue/minute-tick recover durable jobs under original barriers. Any code change here must be justified by an actual missing caller, not by this design alone.
5. **js/bpay-next-office.js transport**: validate old four-key generic error OR exact five-key typed error union; reject extra/unknown fields, impossible reason/phase/status/outcome tuples and invalid UUIDs. Preserve serverCode/retryable/refusal on Failure separately from its local category and ownerCode. Existing two MODULE_NOT_ACTIVE callbacks still invalidate shared owner. Read transport adds exact two kinds and dedicated header/row checks; raw RESPONSE_INVALID remains uncertain for a dispatched mutation.
6. **Office controller**: retain original accepted identity as `accepted_prepare:{command_id,agency_sequence}` (nullable before acceptance) when SEAL_ENQUEUE returns its four-key receipt. A journal version successor may add that one bounded field; upgrade current v3 by retaining all existing IDs, case/work intent and **literal pending.bytes** unchanged. For old sealed journals with no accepted identity, read the original run's accepted header and bind only after exact run/authorised membership checks; never enqueue/capture to discover it. New/old pending bytes must still satisfy existing requestBody equality. READ success cannot clear pending, alter its command, select current inputs or make can_confirm=true. A progress header conflicting with retained accepted identity is a visible evidence error, not automatic adoption. `REVIEW` remains the explicit existing owner action which enables Confirm only from an actual all_ready receipt.
7. **Office UI**: after accepted SEAL_ENQUEUE, or explicit Resume of the same run, show accepted command/sequence and ACCEPTED_CANDIDATES. Opening a row loads its ACCEPTED_WORK page, including original/effective revision, exact pending predecessor and sealed cut; use labels “Accepted preparation”, “Waiting for earlier work”, “Preparing”, “Review required”, not payment-ready counts inferred from a loaded page. Show captured channel/week and state without recalculation. Pending before acceptance is distinct copy: “Not accepted: earlier changes for this Candidate are still pending” or “Not accepted: Pay Query evidence is unavailable/open”. Keep original selection on known refusal; provide only explicit same-request retry/review action. No automatic new UUID or refreshed Work choices. Accepted later-query changes cannot reject/recapture this Draft; original query remains CLEAR at this cut, informational later-approval flag remains separate.
8. **Polling/lifecycle**: use a single bounded READ of the visible accepted Candidate or Work page while progress indicates nonterminal work, on the existing 2-second timer. No REVIEW mutation/claim/step/drain/current WORK census. Keep renderEpoch/disposed/actor/ownerReady checks and cancel scheduled timers on close, tab/scope change or owner pause. Polling a later page does not prove whole-run completion; header maintained enrollment counts and explicit Review remain authorities. A source/read/schema failure retains last immutable acceptance and uncertain journal, pauses controls, and permits explicit factual refresh. Original queue recovery drives progress; Office never becomes its worker.

Do not widen history, PAYE_NET/return/cancel contracts for this F25 transport work. F08/DR37 later contracts remain separate; existing accepted money/projection/journal fields stay. Query-only version owners must publish zero financial jobs and use the existing agency order, as defined by the earlier producer map; this transport never writes them.

## Normal source-gate additions, not a fixture/testing programme

Root may extend the existing normal source-unit gates; this investigation ran none. Reuse `tests/bpay-next-preparation.test.js`, `tests/bpay-next-preparation-rpc.test.js` and FE `tests/unit/bpay-next-office.test.cjs`. Existing gates already cover four-field SEAL receipt and successful replay wake (BE:16/26/55–59), no wake on errors (:45), known DB refusal versus unknown transport (:143), closed key/type validation, read timeout versus mutation unknown (:314), exact request journal and reopen (:166–190), older journal migrations (:120–142), actor switch/late response (:204–213), and pending REVIEW cannot enable Confirm (:261–270). Preserve those behaviors. Do not copy fixture/e2e/browser runners or author scenario/seed tools.

Finite meaningful new cases for those same gates:

- Known trusted fresh BEFORE_ACCEPTANCE reasons survive Worker and Office as their exact finite code/reason/scope; unknown SQL suffix/detail/extra keys/non-SEAL actions cannot receive this phase. Canonical ok:false unavailable and valid blocked:true remain distinct; malformed gate schema is never accepted/CLEAR.
- Conflicting existing command uses verified ACCEPTED_IDENTITY and does not adopt another command as the original pending receipt. Exact same command/body returns four-field success and original sequence; mismatched expected page/count refuses. Neither progress read nor known refusal emits the ENROLL header.
- A first known refusal clears only its current noncommitted pending request; the same refusal after an earlier unknown result leaves **byte-identical** pending journal across close/resume/owner stale/actor change. New journal adoption preserves old pending bytes and accepts only matching original run/command/sequence.
- Both new page kinds validate exact shape/header/run/sequence/UUID/null couplings and monotone cursor/completeness, including accepted Candidate with no job/worker, accepted Case-only Candidate with zero WORK, earlier original publication still pending, partial sealed cuts, READY versus terminal REVIEW, real expiry, and epoch mismatch. No real data or fixtures needed: use the existing in-memory raw transport source units.
- Later Candidate channel/query/current-revision fields do not appear in the new SQL accepted read projections or client selection request. A sealed cut remains displayed from its exact receipt. Browser read result cannot set can_confirm or clear pending; explicit REVIEW all_ready remains its only readiness authority.
- Existing source gate asserts the fixed RPC targets/body/classification and original queue wake are preserved; the added names are finite, no fallback URL/old RPC/live query/claim/step/retry path. Root's normal compile/source checks remain separate from later connected app acceptance.

No source-only enum/schema/receipt assertion proves complete Worker→queue→minute-tick→Office behavior. F25-D04 before/after-click connected journeys remain later NOT_RUN until the approved post-build app testing.

## Display details and scope correction

Apply start-sheet §2 and OR44/OR46: later pay-method change leaves this accepted channel untouched, original selection remains visible, and real background preparation is resumable. Use familiar Office labels and Payment Advance (never expose LOAN as the user label), monetary text from existing frozen owners, and concise mapped refusal copy instead of raw SQL messages. Source query-only actions do not create invoice/debt/payment changes. Do not introduce a switch-permission or Golden Key flow; start sheet already removes them.

The stated CANCELLING/CANCELLATION_PENDING tuple describes an actual later worker.status=CANCELLING; actual CANCELLED stays CANCELLED. These are factual lifecycle observations, not fresh input choices or permission to release. PREPARE DONE/READY worker READY after parent CONFIRM is already valid; do not require worker DRAFT. Confirmed channel addresses and existing financial controls keep their current parent+worker gates.

Current 2258 source changed during root implementation: its stage owner now takes `(uuid,uuid,uuid,uuid,uuid[])` including explicitly prelocked run IDs. This note's transport dependency is the fresh capture boundary, not a new caller to that helper. Re-read current capture/registry before product edit; do not restore the earlier four-argument definition from a design note or use this hash checkpoint to roll back root's concurrent work.

## Source checkpoint

The following are raw saved-file SHA-256 values at this note's final source checkpoint, not body/closure hashes, installed proof or authority to overwrite newer root edits. BUILD-BE baseline a6f7423eb71e6e7cd173e705c2290e5408a1d077; BUILD-FE baseline aada67455afe5a3b6828a713c7062832c0617782. Root alone authors product successors and updates their final registry. Read current disk immediately before editing.

| Tree | Exact relative physical path | Raw SHA-256 |
|---|---|---|
| BUILD-BE | `broker/src/bpay-next-preparation.js` | `defd708e500ea645ee272d326a7b0ed15f03c8b07dc280d3c9590933548c8ed6` |
| BUILD-BE | `broker/src/bpay-next-preparation-rpc.js` | `642a7d989f526c821b36eb58b1bb00f32439da953bc260c35ac290601fd37c34` |
| BUILD-BE | `broker/src/index.js` | `91f6327ed3ba135c1055fd06da94c5f5dec64df9bad8612277e5581ed603af34` |
| BUILD-BE | `broker/src/bpay-next-job-step.js` | `a103b8e4d6551ebe879392c3fbe2087ea2b8aeea41b47c8a0e1c0cefabcfa0c5` |
| BUILD-BE | `broker/src/bpay-next-background.js` | `77faa6d64335f9c54a5508f09df30fa02763861c9b89b3959b8f43349ad0edf8` |
| BUILD-BE | `docs/bpay-next/final-definer-closures.json` | `e659539f4a5c71f2edb9bf92febeba9b70fd1769bb6982c08ce3545564872c9f` |
| BUILD-BE | `supabase/repeatable/09102026_2254_banking_pay_next_acceptance_transaction_fence_v1.sql` | `e26f1d705341d4942103ac55caad3f51a7ca84673de270ec2697d6e5eaf869ab` |
| BUILD-BE | `supabase/repeatable/09102026_2233_banking_pay_next_acceptance_capture_v1.sql` | `7270db42561fc96504a547ffaed49267313aab604de3c81bdc5825bbed0de25c` |
| BUILD-BE | `supabase/repeatable/09102026_2236_banking_pay_next_accepted_prepare_consumers_v1.sql` | `2881978637f38c72a6db81ed982f11a0e684bae6bac54a24bfbbb76979c3203c` |
| BUILD-BE | `supabase/repeatable/09102026_2153_banking_pay_next_accepted_input_v1.sql` | `4462d4e9a45909ba5e428b1916f74a7eeacf99b73add469b64eae8f185e28810` |
| BUILD-BE | `supabase/repeatable/09102026_2258_banking_pay_next_week_origin_capture_v1.sql` | `3eda6eeb43021bf0c07c01f79bc4cc8a9ffa88df2d40db3f84413a6794dabdd1` |
| BUILD-BE | `supabase/repeatable/09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql` | `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b` |
| BUILD-BE | `supabase/repeatable/03102026_2359_banking_pay_next_simple_review_confirm_v1.sql` | `e69026ef2315b9dd4d6905b8a63fdde07d17152b56e1b7d99674e10e05ade561` |
| BUILD-BE | `supabase/repeatable/05102026_0044_bpay_next_source_pay_query_admission_v2.sql` | `01405bc854fb7b2e23cd740154f0302a8956cdd144b44c1f3e4eda67db80dcdd` |
| BUILD-BE | `supabase/repeatable/05102026_1417_weekly_source_coverage_support/qualified_coverage.inc` | `45a8a0bc2364318d734c1ec678d79fa617d549a62c3a532ef851c928773aee6e` |
| BUILD-BE | `supabase/migrations/09102026_2153_banking_pay_next_accepted_input_v1.sql` | `0289b53ad745d5ff5c3a3130ce596e0b5a155e3645a84f1c57b154f8774a5182` |
| BUILD-FE | `js/bpay-next-office.js` | `a4ba8ea0b63f3723a062dea7f915e91ed193c3c20c03e9dfae7765e8de25a356` |
| BUILD-FE | `js/bpay-next-owner.js` | `4947d2e61cd4fa734cbaa99b05c415bad8552865d286233123cc48f34e1ca922` |
| BUILD-BE | `tests/bpay-next-preparation.test.js` | `c1526acd3b6f57f4c60ccf39e2d59d6b21f012f9e258cf9025f224a45fdddeb7` |
| BUILD-BE | `tests/bpay-next-preparation-rpc.test.js` | `dc65cf3e1b0d0d8934c57226357e70fcc00324809d5f537f42667eb4725676e7` |
| BUILD-FE | `tests/unit/bpay-next-office.test.cjs` | `6e905cab92c36eafbea0425524038f7e3157fba2014d4711fa1164dcfeca6048` |

**Design complete.** This is a finite implementation map and scoped source checkpoint only. No product files edited, SQL/runtime/test/browser/provider/hosted work run, or acceptance/installation claimed.

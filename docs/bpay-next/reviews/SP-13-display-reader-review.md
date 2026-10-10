# SP-13 display reader independent source review

Verdict: **PASS — bounded source review of the complete new0956 reader and its actual recorded interface.** No actionable source finding. No product edits or SQL, tests, fixtures, runtime, remote, data or business execution by this reviewer. This does not claim installation, whole D01/gate verification, full SP-13 completion or application acceptance.

Reviewed full `supabase/repeatable/10102026_0956_weekly_source_pay_query_open_display_v1.sql`, full `design/SP-13-display-interface.md`, SP-13's exact card/checklist scope, the current gate relevance in `05102026_1417_weekly_source_coverage_support/qualified_coverage.inc`, ScopeV2 and the native manual-review/incident/comparison table definitions. BD049's actual interface supersedes the alternative proposal in `design/SP-13-investigation.md`.

## Contract and relevant rows

Exact signature is `private.weekly_source_pay_query_open_display_v1(uuid,integer,text,uuid) RETURNS jsonb`, default limit100. Required root, limit1–100 and both-or-neither cursor validation are explicit; query_kind is finite HOURS_INCIDENT/MANUAL_REVIEW; UUID parameter is natively typed. Invalid inputs raise22023. Four parameters, six reply keys and eight row fields exactly match the recorded interface.

Manual selection at44–49 matches the gate: OPEN, exact Candidate/Client/Contract, work date in the configured root week date-6..date, across owning cycles/cutoffs. Incident selection at51–60 matches the gate: OPEN, exact Candidate/Client, missing-event-date OR configured week and NULL-or-same first owning comparison Contract. LEFT JOIN retains an incident with missing event/comparison; the immutable first comparison establishes relevance independently of the current pointer. There is no current-publication filter or dependency on provisional upload display grouping.

All relevant OPEN hours incidents remain present, including CANDIDATE_TIMESHEET_MISSING and an incident covered by a qualified WAIT receipt. Their presence is informational: the reader does not qualify receipts, compute whether payment is blocked, resolve anything or invent a per-row blocking disposition. The current five-key gate remains the blocking/digest authority.

## Paging and unavailable evidence

At40–71, `selected MATERIALIZED` applies native relational keyset/order and limit+1 **before** detail/JSON construction; `page MATERIALIZED` takes the first limit rows. Both use `(query_kind COLLATE "C",query_id)` ascending; the native UUID is retained throughout. Continuation is strictly greater than the supplied typed key. JSON order and the two last-page-key selections use the same ordering; no timestamp/offset/first20 paging or lexicographic text UUID substitute exists.

Details join only those bounded page IDs. The actual PKs and unique `(incident_id,revision_number)` guarantee the manual actor/first-comparison detail joins do not multiply a page identity. Thus at most100 returned rows receive display JSON; the extra selected row only determines whether another page exists. `v_more` uses the raw selected count; last_kind/id come from the returned page, not the lookahead row. For a valid scoped page, complete is raw exhaustion and next_cursor is NULL exactly when complete; an empty valid page completes normally.

Selected contradictory/missing evidence is retained. Manual event/cycle/actor/time checks and incident event/cycle/first-owner/current-last-comparison/contract/fingerprint/time checks produce finite `evidence_available=false`; NULL boolean results are explicitly coalesced to false. The page then has ok:false/EVIDENCE_UNAVAILABLE, with genuine rows and usable continuation retained. A good page does not claim that every relevant query in another page is qualified. `complete` means page-set exhaustion, not clearance, and the interface states this distinction.

Scope unavailable is deliberately the recorded non-page error: ok:false, SCOPE_UNAVAILABLE, scope:null, queries:[], complete:false, next_cursor:null. The recorded consumer checks ok/code before interpreting pagination. This is not represented as a successful empty or CLEAR page. Actual unexpected SQL/permission/identity/constraint errors propagate; there is no catch-all conversion to clear or evidence unavailable.

## Native display facts

Each row is exactly `query_kind,query_id,reason,opened_by_user_id,opened_by_name,opened_at_utc,work_date,evidence_available`.

Manual reason/user/time/date are the real `weekly_source_manual_reviews.office_reason/opened_by_user_id/opened_at_utc/work_date`. The optional name is `tms_users.display_name` for that exact native actor FK; NULL/empty is preserved rather than naming a substitute user.

Incident identity is the native incident ID; its timestamp is `created_at_utc`, date is the same `weekly_work_events.work_date`, and reason is the current comparison's stored `issue_family` only when it belongs to that incident. Invalid/foreign comparison is not passed off as a genuine reason; evidence remains unavailable. Incident opener UUID/name are explicitly NULL because the incident table has no stored Office opener. No response/delivery/audit actor is guessed. Other contradictory comparison scope/tail facts preserve the actual value while marking the row unavailable.

Finite timestamps use the actual stored timestamptz converted to explicit UTC ISO8601; nonfinite timestamps are NULL with unavailable evidence. Date-only work_date is YYYY-MM-DD. No UK presentation string or invented timestamp enters the gate/digest or reader result. Later display formatting and honest incomplete/error handling belong to WB-07/WB-14.

## Ownership and item boundary

The reader is PL/pgSQL/STABLE/SECURITY DEFINER with `search_path=pg_catalog,pg_temp`; all relations/private helper names are explicit. It calls only the existing ScopeV2 helper, not the full facts/gate JSON, admission helper, coverage receipt helper or financial owner. It contains no business write, row/advisory lock, query mutation, module transition, producer protocol, command, backfill, trigger or public RPC. Existing Rule-B accepted snapshot/pin and all five-key gate/digest contracts remain untouched.

Logical owner postgres and EXECUTE revocation from PUBLIC, anon, authenticated and service_role match owner-only D02. One managed transaction and final schema-cache notification are present. The declared later owner-definer consumer is WB-14 ISSUE_DETAIL under WB-07's paged display responsibility; no Worker/Office changes are requested by this SP-13 card. D03 explicitly records limit/cursor/exact keys/native columns/ACL and the future 21-open-query check as NOT_RUN.

Fresh saved-byte identities read again immediately before this review was saved:

| Source | SHA-256 |
| --- | --- |
| `10102026_0956_weekly_source_pay_query_open_display_v1.sql` | `e0c83140e923ab4d7fd856abb09ca6419c43c0c6d1c9056f03a7013e86f74a9b` |
| `design/SP-13-display-interface.md` | `38f721b4f2e818e35a4bdd4ef58fa01fe84f2387eec9349988202825bf6f9286` |

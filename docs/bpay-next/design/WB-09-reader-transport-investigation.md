# WB-09 reader and transport investigation

Date: 2026-10-10. Source-only investigation; no SQL, tests, runtime, business actions or hosted calls were executed. Root authors product. This note covers WB-09 sequence 55 and its seven checklist objects, with W1/W3/W4/W12/W13/W14 and R2 Exact Connections §5. Rule B remains binding.

## Exact scope

Build the ten initial kinds: `HEADER`, `CANDIDATES`, `ACTION_REQUIRED`, `BLOCKED`, `ISSUE_DETAIL`, `CANDIDATE_GROUPS`, `GROUP_LINES`, `PICK_CANDIDATES`, `PICK_CLIENTS`, `EXPORT_ROWS`. Each has one private typed reader; one service-only public `public.bpay_next_workbench_page_v1(p_request jsonb)` dispatches them. New backend `broker/src/bpay-next-workbench.js`, exact GET `/api/banking/pay/next/workbench-page`, and frontend `js/bpay-next-workbench.js::loadWorkbenchPage(kind,params)` are named deliverables.

`CANDIDATE_ESTIMATE` belongs to WB-27, `RESOLUTION_CONTEXT` to WB-29 and `SNOOZES` to WB-39. Workbench command ownership remains its later named card. Do not activate parked F25 readers/resolvers, change enroller/financial commands, seed on opening, calculate money in a reader, or build WB-13 UI now. WB-44 supplies the existing durable seed/metadata/linked-Case continuation barrier.

## Reusable source and precise limitations

* `supabase/repeatable/04102026_0580_banking_pay_next_office_status_v1.sql:7`: `private.bpay_next_current_work_page_v1(uuid,date,uuid,integer)`, the exact current NX021 factual reader. It uses a fixed literal SELECT of 13 named columns, optional fixed keyset clause, bound `EXECUTE ... USING`, native `(week_ending_date,id)` order and limit 1–100. It joins one current revision. It does not certify payable eligibility or replace the maintained Workbench projection.
* That reader starts a 512-byte envelope allowance, caps at 120000 bytes, and can stop before the next row when the byte budget is reached. WB-09 explicitly requires typed oversize refusal. Reuse its fixed-query/bound-value technique, not its partial byte truncation. Do not edit NX021 to change existing callers.
* `broker/src/bpay-next-preparation.js:23,76,220,234,358,388,419`: closed `BPAY_NEXT_PREPARATION_PAGE_INPUTS`/ROWS; native UUID/date and exact decimal-text types; reject duplicate/unknown URL keys; default 50, legal limits 1–100; cursor-pair validation; strict rows/order/scope; 8000 ms deadline; actual streamed 128 KiB byte limit; typed errors and no-store replies. This is the local transport pattern for new `BPAY_NEXT_WORKBENCH_PAGE_INPUTS`/ROWS.
* `broker/src/bpay-next-preparation-rpc.js`: fixed endpoint and service headers; trusted Office actor comes from the authenticated dependency, never a caller actor field. New public signature is one `p_request`, so construct its closed request server-side with that trusted actor and validated query fields.
* `09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql:3776`: current `public.bpay_next_preparation_page_v1(uuid,text,jsonb)` dispatches CURRENT_WORK to 0580. Its authority helper is defined in `04102026_0575_banking_pay_next_preparation_service_v1.sql:7`: service-role JWT, active admin and NEXT owner checks. **This helper contains `FOR SHARE` on user/control rows. A new STABLE reader must not call this locking helper.** Copy the factual service/admin/NEXT predicates into the new read-only authority boundary without row locks; retain both Worker and database authorization.
* `js/bpay-next-office.js:256–289,323`: existing frontend raw transport uses `authFetch`, TEST/LIVE environment header, actor checks before and after awaited reads, fatal UTF-8 decoding, actual stream-byte bound, cancellation and 8-second deadline. Reuse this narrow read pattern without command journal, accepted preparation calls, owner fallback or automatic revision restart.

## Maintained projection authority

`10102026_1252_banking_pay_next_workbench_summary.sql` creates the existing maintained revision, Candidate metadata, group, item, Candidate summary and scope summary tables. `1316` adds genuine written-off/zero-certificate fields. Current `1429_banking_pay_next_workbench_current_continuation_v1.sql` supplies adoption/pending task continuation. These are the Workbench read owners; current-work or live public Candidate/Contract rates must not substitute for their financial facts.

| Kind | Narrow factual source / unique key |
| --- | --- |
| HEADER | singleton revision/adoption/dirty/pending metadata plus one exact natural scope-summary key |
| CANDIDATES / PICK_CANDIDATES | Candidate summary restricted to one `client_scope`, `pay_channel`, `section`; metadata display ordering plus non-null `candidate_id` |
| ACTION_REQUIRED / BLOCKED / ISSUE_DETAIL | maintained item/group rows with exact typed WORK/CASE origin and non-null item UUID tie-breaker |
| CANDIDATE_GROUPS | exact Candidate group rows; unique non-null `group_key` |
| GROUP_LINES | exact maintained group-item facts; non-null item UUID after the named stable sort keys |
| PICK_CLIENTS | genuine distinct non-ALL client scope from the maintained scope/group authority; non-null native client UUID |
| EXPORT_ROWS | bounded keyset walk of the same maintained typed facts, not an all-run JSON array or a new financial CSV owner |

Candidate summary natural key is `(candidate_id,client_scope,pay_channel,section)`; scope summary is `(client_scope,pay_channel,section)`. Returning ALL and constituent rollups together would duplicate counts and amounts. Restrict to one normalized rollup scope before paging. Group keys discriminate WORK/CASE; do not use nullable work IDs as universal cursors. Item natural uniqueness includes component/member/portion; native UUID `id` provides the final non-null tie-breaker.

Stored money is exact numeric/decimal text. Return named server amounts and unknown counters/flags. `unknown_amount_count>0` cannot become a known zero total; nullable line amounts stay null. READY, RESERVED, WAITING, RECOVERY and ZERO facts remain separate. Query qualification remains the current classifier's positive AVAILABLE decision, not a display-reader recomputation.

Existing indexes include Candidate/group; group client/channel; item Candidate/section/group/component/member/portion, work/component/segment, Case/component, future eligible-date/UUID, and client/channel/section/Candidate/UUID; summary section/client/channel/display-name/Candidate. An explicitly C-collated sort must have matching actual support: do not claim an unspecified-collation name index covers it. Index refinement is WB-10's named owner; WB-09 must nevertheless define exact stable order and avoid unbounded raw scans or joining all histories.

## Closed page and adoption contract

Use fixed finite first/subsequent query shapes per kind; no caller SQL identifiers, arbitrary sort expressions, opaque financial strings, physical `row.*` JSON, mutable terms refresh or whole-run aggregation. Select named typed columns before JSON. Apply the raw limit (+ one look-ahead row for truthful completion) before JSON construction. All legal page sizes walk the same ordered identities.

Every successful response returns its exact kind, maintained `workbench_revision`, completion flag, typed rows and closed next cursor. Cursor keys must contain the full stable kind-specific sort tuple and unique final key. Validate cursor shape and type before casts; normalize UUIDs; reject partial tuples, unknown keys, duplicate URL keys, unsafe integers and out-of-scope identities. PostgreSQL bigint identities/counts and money cross JavaScript as canonical exact text; only bounded limit/row counts use Number.

Bind the walk to the trusted actor, effective agency connection, owner/module epoch, official pay date, normalized filter/scope/sort, adoption revision and maintained projection revision. Current module control is a singleton, not an invented agency-UUID authority; agency namespace must be the actual existing trusted connection/context. TEST/LIVE environment alone is not agency identity. The public reader's single STABLE statement snapshot must bind header, rows and cursor to the same observed revision.

Changed revision/owner/scope/date or incomplete adoption gives a typed stale/unavailable refusal, never an empty success or silent adoption of a new revision. HEADER can truthfully expose UNSEEDED/SEEDING/ERROR and dirty/pending state. Other lists must not certify partial READY results while the applicable maintained barrier remains dirty. Opening/paging must not run seed, drain, claim, refresh, enrollment, pin, clock or producers.

W13 requires a retained per-page scope/cursor receipt, including empty/skipped windows. Keep the closed returned page identity as a factual value retained by its caller. A STABLE read-only wrapper cannot INSERT a new database audit/page receipt. No existing database read-receipt authority was found in these reused owners; do not claim one or invent a command protocol. Financial selection/admission receipts remain their named later owners.

Budget the complete database response, cursor and headers within 120000 SQL bytes. A too-large row **or complete response** produces a typed refusal, not dropped rows or a zero amount. Worker caps actual upstream bytes and the serialized outward envelope at 128 KiB, even if Content-Length is absent or false. Reuse fatal decoding, abort/cancel and deadline; after timeout, cancel a late response body and do not allow it to mutate client state. Missing/invalid response, malformed successful JSON, scope/order disagreement and transport failure are typed failures, never `[]`/0. Read failures have no economic unknown outcome or auto-retry business command.

## Exact Worker and frontend seams

1. `broker/src/index.js:198774–198846`: add only normalized exact GET workbench-page to `nextOfficeRoute` and dispatch inside that block. Existing `requireUser` yields 401, explicit admin yields 403, then PF13 environment guard, then construction of service RPC/dependency. Preserve that order before any owner lookup/RPC; no general NEXT-prefix authorization or dispatch.
2. `broker/src/bpay-next-legacy-fence.js:162–174`: add only the exact GET exemption alongside preparation-page/owner-status/advice. Keep wrong methods, unknown NEXT routes and unrelated legacy financial paths fenced. Do not pre-exempt a not-yet-built Workbench command route or Source/settings/invoice paths.
3. `js/main.js:5162–5177`: the actual wrapper is named **`authFetch`**, not `apiFetch` in this saved source. Its exact `nextBankingPath` list prevents raw NEXT replies from being cloned/read by optional API logging. Add exact GET workbench-page there; preserve all other logging behavior. No `apiFetch` symbol was found under current FE `js`.
4. New FE helper should accept the existing API/authFetch/current-actor/environment dependencies and call one raw exact GET, carrying `X-CloudTMS-Banking-Environment`. Require the initiating actor still current before request and after body read, and retain a supplied abort/lifetime boundary. Validate success kind, closed rows, revision/cursor/scope and bound bytes. Throw/return the defined typed failure; do not catch into empty lists or invent UI/status calls. Later screen owners decide explicit restart after stale scope.
5. Public SQL is STABLE SECURITY DEFINER, fixed search path, audited repository owner, explicit revoke PUBLIC/anon/authenticated and service-role grant only; private readers stay private. Recheck admin/service/NEXT in SQL without locks. Final source must include one controlling RPC `NOTIFY pgrst,'reload schema'`, be registered in the current master chain, and retain all older named owners without unrelated edits.

## Confirmed official date helper

Fresh full definition read: `supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql:185280–185379`, `public.pay_banking_official_date_context_v1(timestamptz DEFAULT NULL)`; the only CREATE definition found in current repeatables. It is STABLE SECURITY DEFINER and reads `settings_defaults.id=1` without row locks. It uses `coalesce(p_as_of_utc,statement_timestamp())`, converts once through `Europe/London`, validates configured weekday 0–6, and computes `days_to_official=((configured_weekday-current_London_DOW)+7)%7`.

`current_official_pay_date` is the first configured weekday on or after the London business date; `next_official_pay_date` equals current, and `following_official_pay_date` is current +7 days. **There is no bank-holiday lookup or weekend/bank-holiday adjustment in this body.** Do not describe it as such or invent that policy in WB-09. Current NEXT snooze owner `10102026_1100_banking_pay_next_snooze_v1.sql:132` already calls this genuine helper with NULL.

WB-09 should call the existing helper with `NULL::timestamptz`, retain its factual date/configuration context in the closed scope and use exact typed fields needed by HEADER/cursors. Do not expose `p_as_of_utc` as a browser date override. Missing settings raises P0001 `BANKING_PAY_DATE_CONFIGURATION_MISSING`; invalid weekday raises P0001 `BANKING_PAY_WEEKDAY_CONFIGURATION_INVALID`, both with structured DETAIL. Map these finite configuration failures to typed read unavailability, not zero/empty rows or user-input success. The body returns `configuration_fingerprint` of timezone/configured weekday/settings update time, plus current/next/following/pay-week dates and factual server/London timestamps; reuse actual values instead of a second frontend clock or settings reinterpretation. Source owner/ACL remains postgres, service_role only (PUBLIC/anon/authenticated revoked). No date-owner rewrite is necessary.

Full 26052026 source SHA-256 at this read: `8b3cb3e112ae227a80bf2e661272264c3d6145d0e2ff6ad8d88e8eee2db1553f`.

## Source hashes at investigation

* NX021 0580: `ab95db05d07150d131ba027951878b9833e4e5b5e1cb9f4873cfa55fddc3a1ba`.
* preparation Worker: `adef1957995903841072a7731bbae8131334330feafb09da835628ba52b01da9`.
* preparation RPC: `642a7d989f526c821b36eb58b1bb00f32439da953bc260c35ac290601fd37c34`.
* legacy fence: `afdbdda4ac440ada0a6267523a4a60398ae47feec85fcf8772f0fb2a2c6dfef8`.
* shared index: `eb507f1fa44ae63b75f3c86a86dc5217753ad6fea5fd9321dcce55d70fad2c14`.
* 1252 schema: `aa9a42864893856c9ae02479edb1172cdf39c764a6ca2e9e77f2fd11253affab`.
* FE main: `3e651b6f4497a723487f80b0b452b3330b41bd1f0ba89b47aa2f7ed0afd29dae`.
* FE office transport: `9a946927ad0329c5da6e8da550543ec0cce6d34ac6dfd6b9e0e188a21af6bab4`.

These are investigation snapshots, not execution evidence or whole-card acceptance.

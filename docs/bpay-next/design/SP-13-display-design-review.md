# SP-13 display reader design review

**PASS — finite display interface design; cursor wording finding closed.** Read the complete SP-13 card05:457–482 and all three checklist deliverables1101–1103, the current saved interface/BD049, actual ScopeV2/facts/gate and original manual/incident schema. This is design/source review before the new reader; no implementation, SQL or app acceptance is certified here. D01's function-by-function existing gate/digest/ReceiptV3/two-clear-call verification remains the builder's separate required source record.

## Exact proposed authority and interface

The four-argument `private.weekly_source_pay_query_open_display_v1(uuid,integer,text,uuid) RETURNS jsonb` with limit default100 and both cursor arguments defaultNULL is the narrow new object named by SP-13. STABLE SECURITY DEFINER, logical postgres owner, fixed pg_catalog/pg_temp path and revocation from PUBLIC/anon/authenticated/service_role preserve owner-only use. WB-14's future owner-definer ISSUE_DETAIL consumer supplies the root after its own authority; WB-07's existing five-key classifier retains its own gate responsibility. No new public RPC, grant, Worker, screen, writer, admission, trigger, money owner or ReceiptV3 decision is needed.

The closed six reply fields are exactly `ok,code,scope,queries,complete,next_cursor`. Scope is the actual existing nine-field ScopeV2 including raw stored booking identity and legitimately nullable exceptional-family fields; no independent trim/first/latest substitute. A successful page is ok:true/code:OK. A selected row's unavailable evidence sets ok:false/code:WEEKLY_SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE while retaining that page's rows and actual continuation. Completion means raw-page exhaustion only and never means query clear. A scope-unavailable error is explicitly a non-page result with no resumable cursor; the consumer must inspect ok/code before using pagination. Actual unexpected SQL/permission errors propagate rather than converting to successful empty evidence.

Each emitted row has exactly eight fields: `query_kind,query_id,reason,opened_by_user_id,opened_by_name,opened_at_utc,work_date,evidence_available`. No blocked, receipt exemption, digest or invented actor is added. IDs are typed UUID identities serialised as strings; work_date is DATE/YYYY-MM-DD; timestamp is the actual stored timestamptz rendered UTC; genuine absent metadata remains JSONnull. The later UI owns London display formatting.

## Raw OPEN incidents are the correct inclusion set

Current `05102026_1417_weekly_source_coverage_support/qualified_coverage.inc:30–43` selects manual OPEN rows with exact Candidate/Client/Contract and configured root week-6..week, across all source report cutoffs. Lines46–69 select **all** relevant OPEN incidents with exact Candidate/Client, NULL-or-configured-week actual work date, and immutable first-owner contract NULL-or-match. First revision1 establishes relevance independently of a suspect current pointer; inner-joining only a current comparison would silently lose an owning incident. The display should use that same relevance, preserving missing event/owning/current rows for explicit unavailable evidence.

All five typed issue families are legitimate actual incident metadata in plan6 schema1457. Facts157–158 treats CANDIDATE_TIMESHEET_MISSING as nonblocking and can exempt other incidents through actual qualified WAIT receipts, yet every relevant incident remains in its state census/digest160–166. Therefore raw informational OPEN display must neither filter to supposed blocking incident kinds nor repeat receipt qualification. Showing an actual nonblocking/exempt incident does not change the gate's answer; WB-07 alone decides blocking from the existing gate. This is required separation of display evidence from financial admission, not a new query policy.

## Genuine metadata and selected-row integrity

Manual fields are real `private.weekly_source_manual_reviews.office_reason`, `opened_by_user_id`, `opened_at_utc`, `work_date` (migration02102026_1600:14–18). Stored reason is limited1–1000 characters. `public.tms_users.display_name` exists and is nullable in structural baseline4500–4516 (display_name4509); use that real nullable name, never email/password/fallback synthetic identity. No recorded historical actor-name snapshot is invented.

Incidents have real `id`, `created_at_utc`, Candidate/Client/event/cycle/group, current comparison pointer, and actual joined event work_date (migration15092026_1534:1416–1435). Their reason is current typed comparison.issue_family1457. There is no recorded Office opener in this incident relation, so both actor fields correctly remain NULL. Do not invent an Office actor from a resolution, upload creator or current staff user.

For selected manual rows, the current facts' evidence checks34–36 are actual event identity/Candidate/Client/work_date and source-cycle group. For selected incidents, facts71–79 checks actual event date/Candidate/Client, cycle group, immutable first-owner presence/contract, current comparison presence/incident ownership, actual backward tail and contract, issue family and material fingerprint. Mirror these finite metadata availability checks without calling the full facts/gate, without qualifying receipts and without dropping the original row. A missing display name is legitimate NULL, not a reason to hide a recorded query. Actual malformed/missing source evidence remains unavailable; no guessed replacement comparison or current-most-recent timestamp is selected.

## Bounded stable paging and closed error semantics

Kinds are exactly HOURS_INCIDENT and MANUAL_REVIEW. Validate root, limit1–100 and all-or-none typed cursor; invalid kind/pair/input raises22023. Use deterministic `(query_kind COLLATE "C",query_id)` native UUID ascending ordering and strictly-greater continuation. The cursor is the **last returned** pair, not the lookahead pair, so the extra row cannot be skipped. Both kind and ID are required; no timestamp cursor or manufactured shared UUID identity.

Apply scope/relevance and keyset to the finite raw union, choose limit+1 rows before detail JSON or selected-row metadata enrichment, return firstlimit, derive more from the lookahead and return exactly `{query_kind,query_id}` when more exists. First-owner/event reads needed for raw relevance are permitted before this bound; expensive detail/tail integrity work belongs only to selected rows. Never generate a full facts JSON and slice it. At limit20 with21 eligible rows, page1 has20/complete:false/a real last-row cursor; next page has1/complete:true/NULLcursor. This is a design expectation, not an executed21-query test.

The initial wording said NULL cursor exactly when complete while its scope-failure shape correctly had complete:false/NULL. The fresh interface now scopes that invariant to **valid-scope pages** and explicitly marks scope failure as a non-page/no resumable cursor handled by ok/code first. Finding closed. On valid-scope evidence-unavailable pages, pagination stays honest even when the final raw page is complete; complete does not override the error. The future consumer must preserve an earlier unavailable/incomplete result across requested pages rather than letting a later clean page imply clearance. Existing gate shape/digest remains untouched.

## Limits and reviewed sources

No product was written and no SQL, tests/runtime, data, fixtures, hosted/network/provider or secret operation was performed. The full gate/receipt/digest/clear-placement D01 inventory and new0956 actual source review are separate work. Future WB-07/WB-14 consumer joins, actual21-open-query display and joined app/queue acceptance remain NOT_RUN. Rule-B selected objects and all parked F-25 protocols remain unchanged.

Reviewed raw SHA-256:

- `docs/bpay-next/design/SP-13-display-interface.md`: `38f721b4f2e818e35a4bdd4ef58fa01fe84f2387eec9349988202825bf6f9286`.
- `supabase/repeatable/05102026_0057_weekly_source_pay_query_scope_v1.sql`: `18317d4152fa8e8df4c17ac7895046453a3e26d534b10c45b1fafb50b91a983d`.
- `supabase/repeatable/05102026_1417_weekly_source_coverage_support/qualified_coverage.inc`: `45a8a0bc2364318d734c1ec678d79fa617d549a62c3a532ef851c928773aee6e`.
- `supabase/repeatable/05102026_0308_weekly_source_pay_query_gate_v1.sql`: `fb70e23a80c7686606b5cbf8f17e2878ca37224f5752067d903e9a253a3cbfa8`.
- `supabase/migrations/02102026_1600_weekly_source_manual_review.sql`: `cadac3950ea836a35005eecf1f0b064bdf935d90d6f8b74f2a3377b7ae0afce4`.
- `supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql`: `1fc7833c7054cb56da725d15280ff757f3b78e1046b80f12c337020695374c0d`.

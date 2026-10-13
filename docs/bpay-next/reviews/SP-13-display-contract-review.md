# SP-13 display reader contract review

10 October 2026. Independent second source review of `10102026_0956_weekly_source_pay_query_open_display_v1.sql` and `design/SP-13-display-interface.md` against SP-13 D02/D03 and the actual Source schema. **PASS for the saved SQL and recorded contract; no actionable correctness/type/provenance/pagination mismatch found.** No product changes, SQL, runtime, tests, data, hosted operations or acceptance programme were executed by this reviewer.

## Verified contract and source

The actual signature at `0956:10–17` matches the recorded four-argument private function: root UUID, integer limit default 100, nullable text query kind and nullable UUID query id. It is STABLE SECURITY DEFINER, pinned `pg_catalog,pg_temp` search path, logical `postgres` owner, and revoked from public/anon/authenticated/service_role at 139–141. There is no public RPC or service permission expansion, admission/row/advisory lock, query mutation, monetary owner or new digest/protocol. It calls only the existing ScopeV2 identity reader, not the gate/facts or receipt qualifiers.

- **Input/error closure (`0956:27–37`):** null root/limit, limit outside 1–100, partial cursor or unknown kind raises actual 22023. Kind is exactly HOURS_INCIDENT or MANUAL_REVIEW; UUID remains typed. Scope NULL gives the recorded six-key non-page unavailable reply with empty queries, complete=false and no resumable cursor. There is no catch-all exception masking permission/identity/integrity errors.
- **Same raw relevance (`0956:40–67`):** manual OPEN rows use exact scope Candidate/Client/Contract and configured root workweek across all cutoffs. Incident OPEN rows preserve the factual reader's exact Candidate/Client, missing-event-or-week predicate and immutable revision-1 owning Contract NULL-or-same predicate. LEFT JOINs preserve an owning incident with missing event/owning/current comparison; the reader does not inner-join it away or treat it as an empty clear page.
- **Deterministic finite page (`0956:40–71, 124–136`):** `selected AS MATERIALIZED` applies the strict native `(query_kind COLLATE "C",query_id)` greater-than keyset, same ascending order and limit+1 before details/JSON. `page AS MATERIALIZED` takes only limit rows. The extra raw row alone sets `v_more`; both cursor operands come from the same final ordered page row. For valid scope, next_cursor exists exactly when more raw rows remain; complete is raw exhaustion. Empty exhausted pages correctly return `[]`, complete=true, cursor=NULL. Native distinct IDs plus the kind discriminator avoid any cross-table collision.
- **Typed real row sources (`0956:72–114`):** both UNION branches have text kind/reason/name, UUID query/actor IDs, timestamptz opening time, date work_date and boolean availability. Manual reason is actual `office_reason`, actual opener UUID joins `public.tms_users.id`, and only actual nullable `display_name` is rendered. Empty stored name becomes NULL; no email/name guessing. Incident reason is its own current comparison `issue_family` only when that comparison belongs to the incident. Its opening time is actual incident `created_at_utc`; both actor fields are explicit typed NULL because incidents have no Office opener column. No fabricated actor is used.
- **Positive per-row evidence (`0956:76–101`):** manual rows check real event scope/date, owning cycle/group, actual opener row and finite recorded opening time. Incident rows check event scope/date, cycle/group, genuine revision-1 Contract, exact current comparison/incident/Contract, highest real comparison revision, issue family/material fingerprint and finite incident creation time. Missing/contradictory row evidence becomes evidence_available=false; it remains in the page. Nullable boolean expressions are explicitly coalesced to false before aggregation.
- **Closed projection and unavailable page (`0956:115–136`):** every row has exactly eight keys; reply has exactly six keys. Real times are rendered from stored timestamptz in UTC ISO8601 with microseconds; work_date is date text. Unknown actor/name/reason/date/time remains NULL. `bool_and` covers all returned page rows; malformed page evidence produces the recorded evidence-unavailable code while retaining its rows and honest continuation. An empty qualified page is informational OK, not a financial clear assertion.

Actual column/type evidence: `02102026_1600_weekly_source_manual_review.sql:3–23` defines native UUID identity/actor FKs, date work_date, timestamptz opened_at_utc and bounded actual office_reason; `15092026_1534_weekly_source_plan6_schema.sql:1416–1481` defines incident UUIDs, actual timestamptz created_at_utc, comparison UUID/nullable Contract, typed issue_family and unique `(incident_id,revision_number)`; structural baseline `22082026_1500_cloudtms_test_structural_baseline.sql:4500–4509` defines `tms_users.id` and nullable text `display_name`. These are actual schema columns, not guessed display fields.

## Exact shapes and consumer limits

Reply keys: `ok, code, scope, queries, complete, next_cursor`.

Row keys: `query_kind, query_id, reason, opened_by_user_id, opened_by_name, opened_at_utc, work_date, evidence_available`.

Cursor keys: `query_kind, query_id`, using the same C-collated kind/native UUID ordering. Scope remains the original nine-key ScopeV2. No gate blocked flag, digest or receipt is added.

The design correctly distinguishes raw OPEN informational records from the financial gate: a listed incident may be non-blocking or qualified by genuine WAIT receipt under the existing gate. This reader never repeats that policy. Likewise `complete=true` establishes page exhaustion only, and `ok=false` must be handled before interpreting rows/cursor. WB-07 keeps the original gate; later WB-14 ISSUE_DETAIL consumes this private page through its own actual owner. D03 records that future consumer and does not claim it is already implemented. No request for a new Worker endpoint or screen is introduced here.

Real 21-open-query pagination and Banking detail presentation remain **NOT_RUN**. This source PASS does not claim local SQL compilation, installation, caller integration or whole SP-13 joined acceptance.

## Fresh saved-source SHA-256

| File | SHA-256 |
|---|---|
| `supabase/repeatable/10102026_0956_weekly_source_pay_query_open_display_v1.sql` | `e0c83140e923ab4d7fd856abb09ca6419c43c0c6d1c9056f03a7013e86f74a9b` |
| `docs/bpay-next/design/SP-13-display-interface.md` | `38f721b4f2e818e35a4bdd4ef58fa01fe84f2387eec9349988202825bf6f9286` |

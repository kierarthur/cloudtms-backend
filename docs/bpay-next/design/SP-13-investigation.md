# SP-13 investigation and bounded display-reader design

10 October 2026. Read-only source investigation; no product edits, SQL, tests, fixtures, runtime, business calls, remote access or secret reads. This is design/source evidence, not installation or in-app acceptance.

## Exact scope and sequence

Read full `05_SOURCE_AND_PROTECTED_PAY.md:457–485`, all three SP-13 deliverable rows plus the item row, master sequence43 and the builder start sheet. Required dependencies are F-05, F-06 and SP-12. SP-07 is the preceding sequence42 task; this note does not build ahead of it. The card's explicit Worker/Screen deliverable is **none**.

| Deliverable | Actual remaining work |
| --- | --- |
| D01 — ScopeV2/ReceiptV3/PAY_QUERY_STATE_V3 | Record exact current definitions and file:line checks for SRQ-016–019/071/076–079/301/308/311–314. Reuse the present bodies unless a concrete mismatch is found; no new gate policy or money/Draft facts. |
| D02 — `private.weekly_source_pay_query_open_display_v1` | Missing in live SQL, Worker and Office source. Add one Source-owned STABLE, SECURITY DEFINER, owner-only paged display reader using genuine stored facts. |
| D03 — SP-13/WB-07 interface record | Record closed signature, reply/row/cursor types, native columns, ACL, limits and the exact future consumer. Do not substitute the current grouped Office query list or an unpaged first20 list. |

Rule B remains binding: existing expected revision, captured Candidate terms, accepted query snapshot and existing pin_v2 are the Draft authority. No parked F-25 resolver, producer certificate, new command, pin/enroller/1610/1620/0280 or Worker-route change is proposed. SP-13 adds a live display reader, not a delayed money-stage query check.

## D01 current source map and reuse

All paths below are under `supabase/repeatable` unless marked otherwise. These are the finite verification seams; no confirmed mismatch was found in the inspected contract/output/clear placement sources. They do not replace final source/ACL/compile checks by the product owner.

| Rule | Current source and exact source observation |
| --- | --- |
| SRQ-016–019 / RQ-263 | `05102026_0057_weekly_source_pay_query_scope_v1.sql:9–80`. Explicit null-root22023, canonical current WEEKLY/HOURS nonadjustment root, raw/trim collision resolution, raw `v_root.booking_id` returned, self_bill OR explicit Source classifier, exactly one ordinary contract-week row; exceptional family absent gives both family fields NULL. Nine return keys: root_timesheet_id, family_booking_id, target_family_id, candidate_id, client_id, contract_id, week_ending_date, root_version, family_bound_version. Version values are text. |
| SRQ-076 / 314 / RQ-262 | `05102026_0308_weekly_source_pay_query_gate_v1.sql:12–20` is SQL/STABLE/SECURITY DEFINER, owner-only; removes only facts' `hours_incidents` and `manual_reviews`. Five exact keys remain: ok, code, scope, blocked, query_state_sha256. `qualified_coverage.inc:21–26,168–175`: Scope unavailable has three NULL facts; evidence unavailable retains qualified scope and NULL blocked/digest; OK has boolean blocked and hex SHA. Real errors are not caught/converted. |
| SRQ-077 | `05102026_1417_weekly_source_coverage_support/qualified_coverage.inc:28–79`. Manual relevance is exact Candidate/Client/Contract and configured root week across source cycles. Incident relevance uses immutable first owning comparison independently of suspect current pointer, actual work-event date and candidate/client, then validates the current comparison's identity/contract and latest incident-owned revision. A missing owning/current comparison or contradictory event/cycle is unavailable, not clear. |
| SRQ-078 / 313 | `qualified_coverage.inc:30–43,160–175`. Manual projection is id, source_group_id, source_cycle_id, work_event_id, candidate_id, client_id, contract_id, work_date, state. Hours projection is id, source_group_id, source_cycle_id, work_event_id, candidate_id, client_id, state, work_date, current_comparison_revision_id, comparison_contract_id, issue_family, material_comparison_fingerprint, latest_qualified_exception. Arrays ordered by native id; UTF-8 SHA-256 of canonical JSONB `{schema_version:'PAY_QUERY_STATE_V3',scope,manual_reviews,hours_incidents}`. Missing-Timesheet remains nonblocking while represented. No display actor/time/reason is added to this digest. |
| SRQ-079 / 308 / 311 / 312 | `qualified_coverage.inc:83–157`, `05102026_0114_weekly_source_pay_query_evidence_v1.sql`, `05102026_0245_weekly_source_pay_query_c1_receipt_v1.sql`, `05102026_0258_weekly_source_pay_query_next_receipt_v1.sql`. Latest family event is selected by event_sequence for the exact durable work event; exact evidence approval and unique financial generation are qualified, including genuine generation-less WAIT audit rows. Exactly one qualified lane with its corresponding stored evidence may exempt; partial/conflicting lanes are unavailable. Retained physical identity need not remain today's canonical root. Legitimate sibling-superseded publication proof is retained; no Banking job-state digest is added. |
| SOURCE_LOCAL exact output | `0114:377–379`: lane, publication_request_id, orchestration_run_id, generation_id, request_sha256, receipt_state. Actual COMPLETE/PENDING_FREEZE receipt, exact RETIRED request and approval creation run are checked; no PREPARING adoption. |
| RETAINED_C1 exact output | `0245:146–150`: preceding six identities/state fields plus c1_publication_id, c1_head_revision, c1_receipt_sha256; receipt_state PUBLISHED. Nonnull retained C1 publication/checkpoint/step proof; generation PUBLISHED or SUPERSEDED, not substitute Source-local receipt. |
| DIRECT_NEXT exact output | `0258:21–28`: lane, orchestration_run_id, generation_id, work_id, revision_id, command_id, agency_sequence, accepted_family_bound_version, prepared_request_sha256, publication_request_sha256, source_manifest_sha256, receipt_state COMPLETE. Stored receipt and original immutable Work/revision/publication/command membership are qualified; run COMPLETE, generation PUBLISHED/SUPERSEDED, deterministic Source command, accepted bound version <= current. COMPLETE is derived, not a fabricated receipt column. |
| SRQ-071 / 301 / RQ-261 | Exactly two call sites: `15092026_1534_weekly_source_finalisation_v1.sql:2736` after successful cycle/report CURRENT CAS and before `bpay_next_source_current_publish_revision_v1`; `15092026_1534_weekly_source_correct_final_source_v1.sql:2039` after successful CURRENT CAS and before `bpay_next_source_current_apply_correction_v1`, with original completion receipt later. Prepared/retained/APPLIED replay paths return earlier. Private helper `05102026_0428_weekly_source_manual_review_commands_v2.sql:249–322` preserves captured opening provenance and same contained event/scope; rejects same uploaded report as a remedy. No xmin/GUC/time certificate is invented. |
| SCX-044 / selected witness | Retain each of0057/0114/0245/0258/0308/1417 include/`05102026_0645_weekly_source_selected_source_witness_v1.sql`. 0645 checks exact selected genuine Source evidence and configured workweek, rather than manufacturing protected family identity. Record one BUILD_LOG source/commit entry for each. |

Registry currently selects facts physical `05102026_1417_weekly_source_coverage_support/qualified_coverage.inc` through root0308; the manual Final clear helper remains root/physical0428. Preserve transaction-neutral include placement before the SQL gate wrapper. A display-reader addition must not replace or reorder these authorities.

The clear-site census contains the two actual calls plus the helper definition/ACL entries; no additional clear invocation was found. Current CAS/replay placement should be retained, not recopied into a new finalisation engine merely for SP-13 verification.

## D02/D03 concrete minimal proposed interface

Record this interface before coding (new absent function; no migration is needed):

```sql
private.weekly_source_pay_query_open_display_v1(
  p_root_timesheet_id uuid,
  p_limit integer default 50,
  p_after_query_kind text default null,
  p_after_query_id uuid default null
) returns jsonb
language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
```

Logical owner postgres under existing release adaptation. Revoke ALL from PUBLIC, anon, authenticated and service_role; private owner calls only. No service wrapper/HTTP route/Office call in SP-13. No admission advisory key, money owner, module/business lock, query mutation, notification or generic catch-all.

Input: nonnull root; limit1–100; cursor either both NULL or both nonnull; query_kind only `HOURS_INCIDENT` / `MANUAL_REVIEW`. Bad/null/partial cursor or limit raises22023. Use native UUID comparison and text `COLLATE "C"` consistently for `(query_kind,query_id)` keyset/order. Do not use offsets, timestamp order, JSON casts to numeric or a first-row winner.

Closed success object, seven keys:

```text
{ok:true, code:'OK', root_timesheet_id:uuid, scope:ScopeV2,
 rows:[DisplayRow...], complete:boolean,
 next_cursor:null | {query_kind:enum, query_id:uuid}}
```

`complete=true` iff no extra raw row exists, with next_cursor NULL. If more rows exist, complete=false and next_cursor is the last returned row's exact typed key. Empty qualified page is complete=true. Limit+1 raw rows determine continuation; output has at most limit rows. Failure union has only `{ok:false,code,root_timesheet_id,scope}` and no page/complete/cursor fields: SCOPE_UNAVAILABLE retains NULL scope; EVIDENCE_UNAVAILABLE retains qualified scope. This avoids an incomplete page with an unusable NULL cursor. Permission/identity/constraint errors retain their genuine exceptions. Use the existing full `WEEKLY_SOURCE_PAY_QUERY_*_UNAVAILABLE` codes, not a new business outcome.

Closed DisplayRow, ten keys/types:

```text
query_kind enum; query_id uuid; work_event_id uuid; work_date date text;
reason_kind 'OFFICE_REASON'|'ISSUE_FAMILY'; reason text;
opened_by_user_id uuid|null; opened_by_display_name text|null;
opened_at_utc timestamptz; comparison_revision_id uuid|null
```

Actual field provenance:

| Branch | Stored authority |
| --- | --- |
| MANUAL_REVIEW | `private.weekly_source_manual_reviews`: id, work_event_id, work_date, office_reason, opened_by_user_id, opened_at_utc (`migrations/02102026_1600_weekly_source_manual_review.sql:3–22`). Optional current `public.tms_users.display_name` through that exact opened_by_user_id FK; NULL/blank display is not a fabricated name. reason_kind OFFICE_REASON; comparison_revision_id NULL. |
| HOURS_INCIDENT | `public.weekly_discrepancy_incidents`: id, work_event_id, created_at_utc (`migrations/15092026_1534_weekly_source_plan6_schema.sql:1416–1442`). Work date from the same `public.weekly_work_events` row; issue reason is exact qualified `weekly_issue_comparison_revisions.issue_family` (`1444–1467`); comparison_revision_id is the actual current qualified comparison id. reason_kind ISSUE_FAMILY. Both opener user/name fields NULL: this table has no stored Office opening actor or office_reason. Do not infer one from the latest delivery/response event, first user, or unrelated audit. |

The issue code is the incident's actual machine reason, not invented Office prose. WB-07 can present the finite issue-family wording with existing Source labels. Pass the real timestamp, not an Office-formatted summary string; later Banking formatting uses the shared London date formatter. A genuine absent display name must not alter actor identity or query semantics.

### Bounded selection algorithm

1. Call existing ScopeV2 reader once; preserve its actual unavailable/error semantics. Do not derive scope from current provisional uploads or Banking positions.
2. Select manual OPEN rows using **exact** gate predicates `qualified_coverage.inc:38–43`: same Candidate/Client/Contract, work_date within root configured week; validate event/cycle identity as the facts reader does.
3. Select incident OPEN rows using **exact** gate relevance `qualified_coverage.inc:46–65`: same Candidate/Client; include missing-event/date or first-owning-comparison cases for qualification, rather than silently filtering them away; immutable first-owner contract determines scope across publication cutoffs. Qualify event/cycle/current comparison/last incident-owned revision as at66–79. Unqualified required reason/date/current comparison means EVIDENCE_UNAVAILABLE, not an apparently complete empty page.
4. UNION ALL native relational rows. Apply typed keyset, stable order and `LIMIT p_limit+1` **before** `jsonb_build_object/jsonb_agg`. Only the returned page receives JSON/display-name projection. Separate bounded native existence/qualification probes are acceptable; no prebuilt unbounded facts array is paginated after JSON.
5. Return all gate-relevant OPEN query identities. Missing-Timesheet or protected-covered incidents can still be OPEN; do not invent a per-row block calculation or resolve/exclude them just because current qualified coverage makes them nonblocking. The existing five-key gate remains the sole block/digest answer; this reader supplies display facts only.

Do not call `weekly_source_pay_query_facts_v1` to build its complete unbounded JSON arrays and then slice them for display. Do not copy the Office cycle/publication-specific grouped reader's relevance: that reader has a different grouping/current-publication scope and can omit cross-cutoff relevant queries. The correct reuse is the native stored fields and exact gate predicates, not its row cap or group label.

This is live STABLE per-statement pagination, not a newly frozen multi-request query snapshot. A later query change may change the next requested page; WB-07 owns honest live/incomplete/error presentation, not a new snapshot protocol.

## Current callers and future handoff

Current Rule-B selection consumer `10102026_0151_banking_pay_next_rule_b_selection_consumers_v1.sql:66–68` captures the existing gate into `bpay_next_run_work_query_snapshot`; its prepare consumer at454–515 validates the exact five keys and uses that retained selection result. SP-13 must leave this authority unchanged.

Existing Worker job-step (`broker/src/bpay-next-job-step.js:370`) and Source busy adapter (`broker/src/bpay-busy.js`) retain their exact Source55P03 behavior. Existing Office presentation (`js/weekly-source-presentation-v1.js:24–28` in BUILD-FE) recognizes only the proved rolled-back finite busy response and preserves manual retry semantics. These are SP-12/SP-14 transport seams, not new SP-13 deliverables.

WB-07 sequence53 is the exact future consumer, dependent on WB-05/WB-08/SP-13/F-16. Its existing design map is `design/WB-01-07-current-closure.md:59`; no actual paged display consumer is installed yet. WB-07 owns requested pages and honest incomplete/error wording. Workbench/public reader/Worker/Banking detail transport belongs to WB-07 and its named supporting callers, not a speculative new SP-13 endpoint. SP-15 sequence113 owns later Source Office query workflow. Record the future >20/21-open-query acceptance, the card's Monday block/later genuine Final/replay/provisional/unrelated-receipt cases and actual shared-date display as **NOT_RUN**. Do not tick them through schema loading or this note.

## Source identity checkpoint

Saved source SHA-256 at investigation time:

| File | SHA-256 |
| --- | --- |
| 0057 scope | `18317d4152fa8e8df4c17ac7895046453a3e26d534b10c45b1fafb50b91a983d` |
| 0114 local receipt | `bb6290374ea7857d1ec995565c78267ff9dc761b7d7f4fe05f460ed2903f9b2f` |
| 0245 C1 receipt | `e7e7717499f6e081fb2d2a5445167814efeb1c98ec6848cf828fd874b549a9bf` |
| 0258 NEXT receipt | `9888caaf5720e1078340474354a41cf55b0d90ed721b1366d24f133d9ec77978` |
| 0308 gate root | `fb70e23a80c7686606b5cbf8f17e2878ca37224f5752067d903e9a253a3cbfa8` |
| qualified_coverage.inc | `45a8a0bc2364318d734c1ec678d79fa617d549a62c3a532ef851c928773aee6e` |
| 0645 selected witness | `1d455ca1a9088ce591b046e9c4659b5994967a5f901808faf5ab7fbd1ce04c8d` |
| 0428 manual review/clear owners | `1e9f16241ffa5e9249a7e8bf796693b81b633c2268cd1864147223451628e8d6` |
| 1534 Final | `19018ea7be5e06a510fdd748409bcd84a18d0e31724daa19afb0a4471349a95b` |
| 1534 CorrectFinal | `55786d5a85f69447054677f02df2406c710438987539c8f21efbe0ebbec552f5` |
| Manual review table migration | `cadac3950ea836a35005eecf1f0b064bdf935d90d6f8b74f2a3377b7ae0afce4` |
| Original Source schema migration | `1fc7833c7054cb56da725d15280ff757f3b78e1046b80f12c337020695374c0d` |

The mutable build registry was `bea4d4b2243716d3c39e0733751ade316587aa8197e5a036e9f62ae8198b81bc` when read; preceding SP-07 work may update it. Re-read current saved product bytes and registry immediately before implementation/review. No blocker requiring a new protocol or owner question was found.

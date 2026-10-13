# PF-04 required history source-gate design

10 October 2026. Read-only design investigation; no product edits, SQL, runtime, tests, fixtures or hosted calls. This is the required gate-only design for sequence 22. Cleanup/health source is a separate owner investigation; all future in-app checks remain NOT_RUN.

## Scope and authoritative selection

Read full PF-04 card `06_CONTRACT_MOVES_DAILY_PAYMENT_SUMMARY_PERFORMANCE.md:1741–1762` and checklist D01–D05. Dependencies: F-06, F-11, PF-06. The card asks for a finite static gate over **capture, position apply, prepare, confirm and transfer build**, permitting justified point-history reads, forbidding the audit reader in refresh/calculation, and adding no parser, proof programme, budget system or privileged repair. D05 belongs in existing `scripts/bpay-next-source-gates.mjs`.

NX inventory evidence: `BPAY_NEXT_BUILD_SPEC_20261009_v2/evidence/NEXT_INVENTORY.md` rows NX-001 (position apply), NX-018 (flat Source binding), NX-040/047 (case preparation), NX-060–064 (CLAIM/PIN/capture/hold/completion/CONFIRM), NX-105/106 (begin/build transfer), NX-144 (on-demand 52-section history). This older inventory supplies names, not authority to use its historical bodies. Current `docs/bpay-next/final-definer-closures.json` and managed canonical execution order select every final implementation below. Rule B/BD019–020 supersedes parked F-25 producer work.

Reuse the already imported `inventory`, `definerInventory`, `routineDefinitions`, `scanManagedSql`, `readManagedClosure` and existing comment/body extraction. After `verifyFinalDefiners()` succeeds, select the registry-designated final signature/root/physical definition and confirm it is the actual final execution definition. Check **all** these current finals, even if unchanged or the historical root is immutable. Do not inherit the existing select-star baseline allowance, waive the whole 2018 file or only scan changed filenames. An absent/duplicate/unmatched target fails closed.

## Finite routine-work roots

All names below are `private`; signature types are exactly current registry identities. Files are under `supabase/repeatable/`. `2018` means `09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql`; `0151` means `10102026_0151_banking_pay_next_rule_b_selection_consumers_v1.sql`.

| Role/NX | Current signature | Current final source and definition line |
| --- | --- | --- |
| Position apply, NX-001 | `bpay_next_apply_position_page_v1(uuid,uuid,bigint,text,text,integer)` | `03102026_1430_banking_pay_next_position_writer_v1.sql:456` |
| Prepare CLAIM, NX-060 | `bpay_next_claim_prepare_job_v1(uuid,integer)` | `0151:79` |
| Prepare PIN, NX-060 | `bpay_next_pin_selected_work_v1(uuid,uuid,bigint,bigint)` | `0151:181` |
| Capture, NX-061 | `bpay_next_capture_position_page_v1(uuid,uuid,bigint,uuid,text,integer)` | `2018:739` |
| Hold/reserve, NX-062 | `bpay_next_hold_fresh_position_page_v1(uuid,uuid,bigint,uuid,text,integer)` | `2018:1161` |
| No-case completion, NX-063 | `bpay_next_complete_no_case_worker_v1(uuid,uuid,bigint)` | `03102026_1630_banking_pay_next_no_case_completion_v1.sql:8` |
| Case prepare, NX-040/047 | `bpay_next_prepare_case_page_v1(uuid,uuid,bigint,bigint,integer)` | `0151:550` |
| CONFIRM, NX-064 | `bpay_next_confirm_simple_run_v1(uuid,bigint)` | `2018:1445` |
| Begin transfer, NX-105 | `bpay_next_begin_simple_transfer_v1(uuid,uuid)` | `2018:1524` |
| Transfer CLAIM, NX-106 | `bpay_next_claim_simple_transfer_job_v1(uuid,integer)` | `04102026_0130_banking_pay_next_simple_transfer_members_v1.sql` |
| Transfer build, NX-106 | `bpay_next_build_simple_transfer_page_v1(uuid,uuid,bigint,bigint,integer)` | `2018:1655` |
| Existing delegated case begin | `bpay_next_begin_case_transfer_v1(uuid,uuid)` | `04102026_0410_banking_pay_next_case_transfer_post_v1.sql:6` |
| Existing delegated case build | `bpay_next_build_case_transfer_page_v1(uuid,uuid,bigint,bigint,integer)` | same `0410:200` |
| Existing delegated destination build | `bpay_next_build_destination_group_page_v1(uuid,uuid,bigint,bigint,integer)` | `04102026_1827_banking_pay_next_candidate_destination_group_v1.sql:359` |

Do not scan unrelated settlement/return/cancel routines merely because they share the 2018/0410 physical file. These operations legitimately write/post outcomes and are not this card's routine-work roots.

## Existing helper closure — keep it explicit

The roots already use the following finite helpers; guard their current final bodies as supporting routine work so an audit scan cannot be hidden one call down. Resolve each through the same registry, rather than copying SQL into this gate:

- `bpay_next_reconcile_work_collection_v1(uuid,uuid,text,uuid)` → 2018:3955; `bpay_next_expired_work_collection_scope_v1(uuid,uuid,uuid)` → `04102026_0510:248`.
- `bpay_next_work_collection_origin_v1(uuid,uuid,uuid)` → `04102026_1519:11`.
- `bpay_next_bound_source_snapshot_v1(uuid,bigint)`, `bpay_next_source_disposition_seed_v1(uuid,text,uuid,uuid,uuid)`, `bpay_next_source_book_seed_proof_v1(uuid,text,uuid,uuid,uuid)`, `bpay_next_source_binding_members_v1(uuid)` → `06102026_1246` (585, 420, 295, 11 respectively).
- `bpay_next_initial_tsfin_bound_assert_v1(uuid)` and its certificate helper `bpay_next_initial_tsfin_certificate_v1(uuid)` → current `07102026_0018` registry definitions (assert:187, certificate:10).
- `bpay_next_source_command_id_v1(uuid)` → `03102026_1540:9`; existing `weekly_source_pay_query_admit_v2()` → `05102026_0044`. These supply identity/admission, not history reconstruction.
- `bpay_next_publish_no_case_week_fact_v1(uuid,uuid)` → `04102026_0420:7`.
- `bpay_next_capture_instruction_destination_v1(uuid)` → `04102026_1827:115`.
- Pure `bpay_next_case_allocation_row_v1(text,numeric,numeric,numeric,numeric,numeric,numeric,numeric)` → `04102026_0290:9`.
- `bpay_next_value_source_for_target_v1(numeric,text,text,boolean,uuid,date)` → `06102026_1157:932`; shared `pay_finance_selected_quote_v1(public.pay_finance_component_resolution_mode_enum,numeric,text,text,numeric,numeric,numeric,numeric,numeric,boolean)` → same file:10. Pure quote arithmetic must not acquire history access.
- `bpay_next_channel_drafts_page_v1(uuid)` → `06102026_0236:6`; CONFIRM's bounded current-channel result helper, not the audit reader.

Existing command-receiver/admission and Candidate expense-family authority helpers used by these roots should retain their final source boundaries: `bpay_next_receive_command_v1(uuid,text)` → frozen `03102026_1404_banking_pay_next_command_intake_v1.sql`; `_candidate_expense_source_family_v1(uuid)` → `26092026_0202_banking_pay_stage2_source_authorisation_v1.sql`. They are not grounds for a new producer framework. Inspect direct routine calls in each guarded body: an added call to a history/audit reader must fail immediately. A new delegated routine-work helper needs a finite named entry/review, rather than automatic permission from being called by an allowed root. The existing extractor must distinguish function calls from INSERT column lists and type constructors; do not treat every `private.name(` text hit as a callable routine.

## History relations and legitimate exact reads

Fixed audit-ledger relation list for this gate:

`private.bpay_next_financial_effect`, `private.bpay_next_case_event`, `private.bpay_next_transfer_outcome`, `private.bpay_next_internal_receipt`, `private.bpay_next_return_cash`.

These are the actual effect/event/outcome/receipt/cash ledgers from the current NX history graph. Record the list directly in the existing source guard, not a new manifest/digest subsystem. Treat a new ledger name found through a changed NX audit section as a design change to review. Do not classify all 52 history sections as banned ledgers: current positions, controls, net projection, allocation state, worker-week facts and retained flat binding versions are operational authorities, even though the audit reader also displays them. Do not substitute an audit scan for those authorities.

Current guarded bodies have exactly **four ledger read occurrences**, all positively justified points:

| Guarded final routine | Exact allowed read predicate | Actual uniqueness |
| --- | --- | --- |
| `bpay_next_prepare_case_page_v1` | `not exists(select 1 from private.bpay_next_case_event e where e.id=v_rule.original_funding_event_id and e.case_id=v_case.id and e.event_kind='FUNDED' and e.occurred_at_utc=v_rule.original_funded_at_utc)` (`0151:913–914`) | `case_event PRIMARY KEY(id)`; extra identity/outcome checks must remain |
| `bpay_next_source_book_seed_proof_v1` | `select e.* into strict v_effect from private.bpay_next_financial_effect e where e.id=p_seed_effect_id;` (`1246:338`) | `financial_effect PRIMARY KEY(id)` |
| `bpay_next_reconcile_work_collection_v1` | `select e.* into strict v_effect from private.bpay_next_financial_effect e where e.id=p_effect_id;` (`2018:4006`) | `financial_effect PRIMARY KEY(id)` |
| same reconciliation helper | `join private.bpay_next_case_event ce on ce.id=v_effect.operation_item_id and ce.case_id=b.case_id` (`2018:4011`) inside the exact posted-recovery scope query | `case_event PRIMARY KEY(id)`; retain command/case/component/transfer and amount tuple witness checks at 4015–4023 |

There are **no** permitted transfer_outcome/internal_receipt/return_cash reads in this current guarded set. Do not introduce a generic Candidate, case, WORK/component, worker, run, date, range, MAX/latest, aggregate or LIMIT allowance: none is itself proof of a unique ledger point. Adding LIMIT does not convert an audit scan into a point lookup.

Related retained/request identities, which are not broad financial-history permission:

- Reconciliation joins `bpay_next_outcome_request r` on `r.command_id=v_job.command_id` (2018:4015 and 4030). Its **PRIMARY KEY(command_id)** proves one request; additional transfer/worker identities remain required. This is current command evidence, not request enumeration.
- PIN's retained protected receipt joins `receipt.revision_id=v_revision.id` (0151:438–439); **UNIQUE(revision_id)**. Retained publication joins exact `(work_id,revision_id)` (443–444); **UNIQUE(work_id,revision_id)**. Both serve the admitted source revision's immutable authority. `publication PRIMARY KEY(command_id)` also supports position application/reconciliation's exact command point.
- `weekly_source_entitlement_publication_receipts` has PK(id), unique request_digest, and unique `(decision_bundle_id,bundle_revision,request_digest)`. Its actual flat binding joins must keep a complete one-row key when used as retained evidence. Never replace them with an older-receipt scan; deeper Source admission/history ownership is outside this narrowly named Banking gate.

Do not grandfather a history scan solely because it is historical/applied/unchanged. Conversely, the exact legitimate point read above must not be “fixed” by deleting its financial evidence check.

## Minimal gate shape and acceptance

Implement one finite assertion inside `scripts/bpay-next-source-gates.mjs`, called from its existing all-source verification after final-definer selection. Use existing statement/body extraction and comment normalization. Keep executable literals/dynamic SQL visible; a comment containing a table name is not a read. Reject unclassified/dynamic history access in a guarded body, direct NX-144/audit calls, history readers through qualified or search-path unqualified spelling, and any unapproved occurrence of a listed ledger in a FROM/JOIN read.

For each approved occurrence, match the **whole local read/query clause**, finite alias and the complete unique equality, not “body contains an ID equality somewhere.” Check every occurrence independently and reject leftovers. Preserve the exact funding subquery and the two complete PK SELECT statements above; for reconciliation's join, close the ON clause at the next JOIN and retain the actual surrounding proof query. Any `OR`, alternate UNION/read, replaced RHS, range predicate, removed unique-key part or extra occurrence invalidates that approval. Avoid a broad SQL parser/alias inference engine: changes to these four known shapes can fail closed for manual source review. Normalize whitespace/case/comments, not semantics, and never use a whole-file historical exemption.

Direct audit-reader denial includes current `public.bpay_next_payment_history_page_v1(jsonb)` (NX-144), `public.audit_events_list(text,text,text,text,integer,integer,text,text)` and `public.pay_finance_case_audit_actor_index(uuid)`. No guarded routine currently needs them. G01/reconstruction helpers must not gain entry through a renamed callable; new/unknown history/audit delegation requires explicit review. Retain the existing executable source gate's own authority boundaries.

Reuse existing finite source-gate test units with a few source-string mutations: valid current point read remains accepted; removed/altered point equality, injected OR or second ledger read, Candidate-scoped aggregate, direct qualified/unqualified history-reader call, hidden delegated helper and comment-only text each get the expected result. This is the card's named static check, not an in-app/runtime proof, fixture cluster or new harness. Registry-selected successor coverage must survive a root move/include, and checking unchanged final targets is mandatory.

The gate can demonstrate the narrow source restriction. It cannot prove query latency, year-of-history performance, cleanup retention or the five operational health measures. Those remain the separate D01–D04 implementation/future joined checks; do not mark PF-04 installed or app-tested from D05.

## Source checkpoint

At investigation read: `scripts/bpay-next-source-gates.mjs` SHA-256 `1cde1003b32de9f73ddd4a92ab5d02b33801bc75d7eb0356bc1fe8875cd4b740`; registry `a2699dead66e4c2601ad7b41822555c587a9524e086c4cd41709468724b61f0d`; sealed contract `eb12b8736984bf36b0fd0b1731182ed2b7d18ffdf97235de6d0f32ff3f56aec0`. Re-read current saved files before product editing; paths/line numbers here are a source census, not permission to freeze future final definitions.

## Direct-call census follow-up

Saved `PF-04-routine-call-census.json` maps **33 definitions: 14 roots plus 19 explicit helpers/boundaries**, each exact signature to actual direct private/public routine callee names, separate `pg_catalog` builtin names, excluded DML/cast targets, and current final provenance. It was derived by the existing source-only statement/signature scanner and manual DML/type classification; no gate, SQL or business routine was invoked. Existing INSERT column lists were excluded. There are no unclassified qualified calls or unqualified known routine calls in the selected current bodies. This is a finite design census, not a new runtime manifest or recursive proof framework.

One precision correction to the earlier registry wording: `private.weekly_source_pay_query_admit_v2()` is **not registered in the Bpay-specific chain registry**. It has a current single canonical repeatable definition `05102026_0044_bpay_next_source_pay_query_admission_v2.sql`, selected through the existing `definerInventory`, while the other 32 definitions agree with their registry finalRoot/finalPhysical. Require that exact current single authority; do not manufacture a registry entry or missing signature. Final execution selection remains mandatory for every function.

The exact existing direct calls outside those 33 definitions are only:

| Current caller | Existing boundary callee and actual current final source |
| --- | --- |
| `private.bpay_next_initial_tsfin_certificate_v1(uuid)` | `private.weekly_source_effective_inventory_v1(uuid)` → `15092026_1534_weekly_source_ordinary_pay_projection_v1.sql:873` |
| same | `private.weekly_source_inventory_approval_basis_v2(uuid,jsonb)` → `04102026_2120_weekly_source_approval_basis_v2.sql:137` |
| same | `private.weekly_source_sha256_jsonb_v1(text,jsonb)` → `15092026_1534_00_weekly_source_private_classifiers_v1.sql:28` |
| `private.pay_finance_selected_quote_v1(...)` | `public._pay_convert_paye_to_umbrella(numeric,numeric,numeric,boolean)` → `26052026_2100HRS_NEW_FUNCTIONS.sql:601` |
| same | `public._pay_convert_umbrella_to_paye_ex(numeric,numeric)` → same source:620 |
| same | `public._pay_umbrella_vat_calc(numeric,numeric,boolean)` → same source:569 |

The first two are the existing Source effective-inventory/approval-basis authorities used to certify initial TSFIN. Their current relation references are Source heads/components, actual TSFIN and expense authority/materialisation, with original scope/approval inputs; they are not calls to the Banking audit reader. The JSON helper and three conversion helpers have no relation FROM/JOIN reads. Preserve these six exact **caller→callee** edges at their existing source boundaries; do not permit arbitrary new helper delegation merely because it has a familiar schema or naming prefix. This does not authorise Source protocol changes or a broader Source graph investigation.

The gate can use the fixed caller/callee sets in the existing module: a changed qualified helper call not in that caller's census fails closed; an unqualified history/audit call also fails. Keep SQL conditional forms/builtins and declared type/INSERT targets distinct from custom routine calls. The source census is not an assertion that an unknown unqualified call is safe: an added custom unqualified delegation must also fail/review, rather than evade the finite edges through search_path. No general recursive call graph is required.

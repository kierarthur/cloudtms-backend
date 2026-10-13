# SP-15 Source refresh-hook source review

11 October 2026. Independent read-only review of BD125, full SP-15 card `05_SOURCE_AND_PROTECTED_PAY.md:515–542`, exact D01–D05 checklist and current2337 batch. No product edits, SQL execution, data/fixtures, tests, hosted calls, captures or commits. This is a SQL/source slice, not whole-item or joined acceptance; all runtime/application journeys are **NOT_RUN**.

**Current verdict: SQL SOURCE PASS.** Final amendment below independently closes P15-01 and records the current saved batch. The OPEN checkpoint is historical.

## Historical checkpoint — OPEN

**Essential finding P15-01:** Local completion at `11102026_2337_banking_pay_next_source_query_workbench_refresh_v1.sql:3290–3292` suppresses its direct refresh whenever `v_manual_review_id` is nonnull. The original resolution branches at `3274–3283` only call the refreshed resolver for APPROVE/AMEND/WITHDRAW/RECONCILE. RECORD_NOT_WORKED is a native accepted stage action (`10102026_0853:1279`;2337 direct NEXT admission2539/2752–2753) and remains separate from review resolution. If the existing OPEN review is found for that event, Local writes/completes the protected decision but performs neither resolver refresh nor its own hook. Keep original resolution semantics and narrow the skip predicate to a resolution branch that actually ran, or perform the one bounded direct refresh after completion. Do not manufacture resolution for RECORD_NOT_WORKED.

## Parent preservation already inspected

The `.codex-local/SP15-parents.json` metadata contains twelve definitions: eleven existing owners and one new identity-only helper. Every one of the eleven recorded parent raw SHA256 values matches its current saved parent file. Static original/body comparison shows only new targeted refresh calls and their two narrow condition wrappers; no removed or changed predecessor line was found. Current body/parent inverse validation and exact helper qualification review continue. This checkpoint does not claim SQL compile, catalogue parity, ACL installation or financial runtime proof.

Initial reviewed2337 raw SHA256: `C7FD3DAE73A6E0313C6922FE36EA9191EAFC140A4DFDEB9B0F24FA6B0BAECA8F`. Later corrections must be recorded separately against their current bytes.

## Final saved-source amendment — SQL SOURCE PASS

P15-01 is closed at current `3292–3294`: Local completion refreshes directly if no review exists **or** the request is outside APPROVE/AMEND/WITHDRAW/RECONCILE. For those four kinds with a review, the existing resolver at `3276–3285` performs the refresh after its genuine resolution. RECORD_NOT_WORKED with an OPEN review therefore retains its original separate behaviour and still refreshes the changed gate-input state. Completion/request/receipt/step evidence precedes both paths. Exact early Local receipt replay remains unchanged and performs no refresh.

The direct NEXT publisher now calls the existing refresh owner on its actual accepted `v_stage.work_id` (`2858–2860`) after immutable publication receipt and completion step, with explicit NEXT gating. It does not infer the published Work through a second mapping. The generic identity helper is reserved for Source events whose writers do not already hold that accepted Work ID. Its current Work join additionally requires the genuine `work_kind='SOURCE'` at19.

### Complete parent and ownership comparison

Static independent comparison verified all eleven complete successor bodies. For every signature, the recorded original body is present verbatim in the saved physical parent, the metadata changed body is present verbatim in the saved2337 file, the recorded parent SHA256 matches disk, and removing **only the exact new refresh insertions/condition wrappers** restores the complete original body byte-for-byte after normalising CRLF to LF. No original line was removed, reordered or rewritten. This includes complete parameter/default/return/security/search_path declarations, request schemas, original actor/session/permissions, workflow/generation/current-comparison checks, admission placement, locks, idempotency/collision/replay, original Source and financial operations, original replies and exception handling.

| Exact existing signature | Current saved physical parent | Current2337 hook |
| --- | --- | --- |
| `public.weekly_source_manual_review_open_v1(jsonb)` | `05102026_0428_weekly_source_manual_review_commands_v2.sql` |128; only actual new OPEN, after review/audit/command write; existing-open no-op and exact replay unchanged. |
| `public.weekly_source_manual_review_resolve_v1(jsonb)` | same0428 |318; after actual RESOLVED/audit/command evidence. |
| `private.weekly_source_manual_reviews_finalised_v1(uuid,uuid)` | same0428 |390; inside only the original qualified later-Final clear branch. Both actual Final/Correct Final fresh-CURRENT CAS callers and old replay exclusions remain inherited. |
| `public.weekly_source_query_accept_system_hours_atomic_v1(jsonb)` | `15092026_1534_weekly_source_query_delivery_v1.sql` |487; per actual accepted/resolved incident, exact comparison Contract/event. |
| `public.weekly_source_manager_review_respond_atomic_v1(jsonb)` | same1534 query delivery |788; after the original actual manager response/event/notice write. |
| `public.weekly_source_candidate_response_submit_atomic_v1(jsonb)` | same1534 query delivery |1106; after the actual response branch, preserving complete original Candidate/session/current generation/comparison/replay checks. |
| `public.weekly_source_query_sync_atomic_v1(jsonb)` | same1534 query delivery |1524,1612; after genuine changed comparison or resolution, using its exact incident/current comparison Contract. |
| `private.weekly_source_candidate_submission_compare_sync_v1(uuid,uuid,uuid,uuid,bytea,timestamp with time zone)` | `15092026_2203_weekly_source_candidate_app_contract_v1.sql` |2269,2317; actual comparison change/resolution only, native membership Contract/event. |
| `public.weekly_exceptional_pay_publish_next_v1(jsonb)` | `04102026_0345_banking_pay_next_protected_source_v1.sql` |2858–2860; actual accepted published stage Work, after complete receipt/step. |
| `public.weekly_exceptional_pay_complete_local_v1(jsonb)` | `04102026_1257_weekly_source_local_protected_decision_v1.sql` |3292–3294; direct completion or inherited refreshed resolution, as qualified above. |
| `public.weekly_exceptional_pay_wait_atomic_v1(jsonb)` | `10102026_0821_banking_pay_next_protected_wait_permission_v1.sql` |3581; after actual WAIT event/pending-target/family/run completion; original permission and exact early replay unchanged. |

The saved registry selects exactly these eleven existing successor identities plus the **one new private** helper. There is no new public function identity, public table/trigger, command kind, route, producer, financial formula, input witness/certificate/digest or continuation protocol. Later SP-09/SP-28 work is not implemented by this source slice.

### Identity, bounds, locks and absent Work

`private.bpay_next_source_query_refresh_v1(uuid,uuid)` at4–29 first positively gates NEXT at10. It resolves one exact durable event PK, the supplied native Contract with matching Candidate/Client, the native ordinary primary Contract Week (`additional_seq=0`, non-adjustment) containing the event date, and that physical WEEKLY/HOURS root. It compares the **raw** stored booking value to Work's native booking key, plus exact Contract/Candidate/native week and SOURCE kind (12–21). There is no trim/family-history heuristic, nullable Contract inference, synthetic Monday or guessed Work UUID.

Native `contract_weeks` Contract/date/additional uniqueness is `22082026_1501_cloudtms_test_structural_finishing.sql:4527`; its `is_adjustment` is NOT NULL in baseline1500:2114. Work raw booking is NOT NULL/1–200 characters and unique in core1324:116,128. The resolver's Work IDs are capped at two before `array_agg`; zero is an explicit no-op, multiple distinct identities refuse23514, and exactly one calls the existing owner. It creates no Work, approval, Source absence, financial input or family identity. This is a finite native identity seek; no universal timing assertion is made.

Eligibility is deliberately left to the current classifier. The helper imposes no current/unrevoked/unarchived/payable/APPROVED filter, so an existing revoked or blocked Source Work can be reclassified. An unpublished event with no native Banking Work remains absent. The unchanged WB owner `10102026_1438:324–327` applies actual NEXT authority and Candidate-control → Work locks; its existing exact ORIGIN task and linked-Case continuation at328–350 remain responsible for bounded propagation. The new helper does not acquire an extra Case lock, adopt a lease, run financial allocation or invent a new projection task.

All new call sites remain within their original admitted outer transaction after actual business writes. Original Source query-admission/session/collision checks precede them, as proved by the complete-parent inverse. No caught error converts a failed refresh into a successful partial Source result. Original early replay returns remain before every new call; repeated action identity returns the native original result rather than reopening a query or refreshing/repricing it.

### Security and current-source boundaries

The new helper has fixed `pg_catalog,private` search_path, logical postgres owner and explicit revocation from PUBLIC/anon/authenticated/service_role (`4–5,3592–3593`); it is reachable only through the original owned callers. Existing public Source owners retain the same service-only ACL identities; the two inherited private helpers remain owner-only (`3594–3624`). Final pgrst reload is present at3625. No protected browser function or table membership was added. Actual installed owner/ACL/catalogue/parity and Candidate STOP-CHECK receipts remain builder checks, not database observations made here.

The source-gate addition is the exact new private signature and finite three-callee list (`scripts/bpay-next-source-gates.mjs:49,208`), not a blanket SQL/history/dynamic waiver. This review verifies source, not execution of that gate. BD125 and the registry define the current merge; the existing earliest admission, Rule-B capture/economic owners, E2 exclusions and SP-14 behaviour remain unchanged.

### Final reviewed raw SHA256

All paths are relative to BUILD-BE. These hashes prove reviewed saved bytes only.

| Artifact | SHA256 |
| --- | --- |
| `supabase/repeatable/11102026_2337_banking_pay_next_source_query_workbench_refresh_v1.sql` | `058E400F131B5F82D53F3D3AAF299B44D37D305C6D2413380DA4FC3C454625D0` |
| `docs/bpay-next/design/SP-15-refresh-hook.md` (BD125) | `6CB54EACD9A735F67AFA874E853F2B09A24FCA75A160B90DAA74FFC57DE4544C` |
| `docs/bpay-next/final-definer-closures.json` | `6D804AFB96533069147FE3B4FF3CB2A6B2A50D5157D94C85153D8D2676C66EFA` |
| `scripts/bpay-next-source-gates.mjs` | `B79018A6AE185813243B17000B4F9327C58AF692D38F409FB8AB4A2582C1681A` |
| `.codex-local/SP15-parents.json` (ignored comparison material, not product authority) | `6342AB9D8D0DA958379217F66CFD11B55BDAF8008AA163F1336CA17429BC3B67` |

No essential SQL/source finding remains in this reviewed batch. Office message/opener, Source workspace command-id/hide/render checks, runtime queue/Source publication/retired or revoked Work journeys, contention, timing, joined acceptance and A4 remain separate, **NOT_RUN** by this reviewer. No whole SP-15 item PASS is asserted.

## Final physical-range and retained Source seam amendment

**Current verdict: SQL and retained D03/D05 SOURCE PASS.** The final helper at15 now uses `cw.week_ending_date BETWEEN e.work_date AND e.work_date+6`. For native date values this is exactly the former containing-week predicate (`e.work_date BETWEEN cw.week_ending_date-6 AND cw.week_ending_date`); it supplies both lower and upper bounds to the existing Contract/date index. Its Contract/Candidate/Client/raw-booking/physical WEEKLY-HOURS/SOURCE qualifications, LIMIT2/ambiguity refusal, absent-Work no-op and existing refresh call are unchanged. The eleven complete predecessor bodies and financial operations remain as inspected above. This is source equivalence, not an EXPLAIN, timing or business execution result.

D03 reuses the actual Office command route without a new transport: `broker/src/weekly-source/routes.js:24–29` maps OPEN/RESOLVE/QUERY_SYNC/REMIND to the existing exact owners;260–266 requires authenticated admin Office identity,570–575 bounds/parses the request and overrides the actor with that identity,664–672 awaits the original RPC/refusal recording, and1099–1100 dispatches the existing POST commands path. No browser financial facts, new command kind, payload restructuring or automatic Source retry was introduced by SP15. The complete-parent comparison above verifies the native Source/Candidate permission, generation, current-comparison and replay predicates.

D05 OPEN freezes one `{source_row_id,reason,command_id}` in `workspace-actions.js:1068`, posts the same `state.pending` at1069 and retains it on failure at1072; its reason becomes disabled once pending at1056. RESOLVE clones the accepted action payload once at923, assigns its one `command_id` at927 outside the click/retry handler, and every retry at943 sends that same closure payload. Original actor/frame qualification and busy guards remain at931–941,948–957 and1053–1064. `import-workspace.js:1359–1366` copies/serialises the supplied command payload without generating another identity. This proves retention within the existing confirmation/retry lifetime; it does not claim a reload-persistent browser journal that these native Source controls do not provide.

The file-detail and completed-report buttons are hidden when the authoritative `pay_query_open` is true (`workspace-actions.js:1014`;`combined-review.js:150`); their click paths retain exact source-row IDs (`workspace-actions.js:1037–1039`;`import-workspace.js:1138–1143`). Native reader `01102026_1201_weekly_source_combined_workspace.sql:1274–1280,1474–1480` sets that fact from the exact Source-group/event OPEN manual review **or** OPEN discrepancy incident. Current combined reader09102026_1130:36 delegates history to that unchanged report-history owner. Current OPEN2337:54–75 and RESOLVE169–191 compare original command/actor/request identity before fresh admission, return the retained result on exact replay, and refuse collisions; no retry reopens resolved membership. These are read-only source checks; screen pixels and all joined/application journeys remain **NOT_RUN** here.

Final reviewed byte identities (the earlier hash table is a historical checkpoint):

| Artifact | Current raw SHA256 |
| --- | --- |
| SQL2337 | `7A5B4B56A424B2A07A8A54C895C4841CE6E0C2CBB0E9A80D06C27842FDE5C96A` |
| BD125 | `6CB54EACD9A735F67AFA874E853F2B09A24FCA75A160B90DAA74FFC57DE4544C` |
| Final-definer registry | `6D804AFB96533069147FE3B4FF3CB2A6B2A50D5157D94C85153D8D2676C66EFA` |
| Source gate | `B79018A6AE185813243B17000B4F9327C58AF692D38F409FB8AB4A2582C1681A` |
| Ignored exact-parent comparison metadata | `A3FD6EE0B1B5E1944CA72D80A4F3B4F5576EFF0AE62A58075DE48A05101AC59E` |
| BE `broker/src/weekly-source/routes.js` | `3641498765ED87D9D2C450E07CFC3DCA05A9601725976D2E82E315F40DDE4B11` |
| FE `js/weekly-source/workspace-actions.js` | `66345020EF21FDB2B71DB996B8D9FBFF100F1BBC9D4694DC2659B60CAB5B416A` |
| FE `js/weekly-source/import-workspace.js` | `7D4621851BF5C5E5B14C0DBD2F149291A16F34AE050778A952CFC203855D9BB1` |
| FE `js/weekly-source/combined-review.js` | `F950A841AA7B54ADBDF0591EF378453BFF3E8F01287E45AFE8449196F5C6B99E` |

No essential source defect remains in the reviewed range or retained D03/D05 seams. Builder-reported sole-PG17 reload is not execution performed or independently observed by this reviewer. Fresh catalogue/install receipts are separate builder evidence; no whole-item/runtime/A4 PASS is asserted here.

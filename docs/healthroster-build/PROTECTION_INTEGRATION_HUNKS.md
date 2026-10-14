# Protection/query source integration

This package is source only. Root owns canonical publication/FIN/read/route composition, ACL, release registration, TEST fixtures and installation. Do not publish an old complete owner over the report/money/F1/D10 union. Support pages are one 1438 closure, all `.sqlinc`.

## Checking publication: `public.weekly_source_projection_publish_atomic_v1(jsonb)`

Add `v_signed_family_proof jsonb` to the current complete owner. Immediately after the existing `private.weekly_source_pay_query_admit_v2()` and **before any group/cycle/scope/publication row lock or declared-scope advisory lock**, add:

```sql
v_signed_family_proof:=private.weekly_source_publication_signed_family_fence_v1(v_publication.id,null);
```

The helper discovers the complete signed-week physical roots through plain reads and calls the unchanged native I-1 owner once. It does not parse schedules or weaken per-sheet isolation. Empty means an explicitly proved empty discovery set. It does not take the Candidate serial gate: checking publication is not one of its four native job kinds.

The current owner then takes group SHARE before its original cycle/scope locks, using its already discovered exact group UUID. Preserve report-scope and correction arms. After all original cycle/scope/publication/upload re-reads and before any publication/mapping/comparison DML, add:

```sql
if v_cycle.source_group_id is distinct from v_group.id then
  raise exception 'WEEKLY_SOURCE_PROJECTION_SCOPE_CHANGED' using errcode='55000';
end if;
perform private.weekly_source_publication_signed_family_fence_v1(v_publication.id,v_signed_family_proof);
```

Any changed discovery set, including empty→nonempty, scope/profile/coverage/generation or group change, refuses `WEEKLY_SOURCE_PUBLICATION_SIGNED_ROOT_SET_CHANGED` / 40001. Never acquire a second family set after the outer cycle lock. The comparator's signed Timesheet `FOR UPDATE` remains unchanged and is reentrant under this complete fence. An exact CURRENT replay remains the current owner's normal replay; no comparison audit is validation/READY authority.

Replace only the current checking-recheck condition before `weekly_source_candidate_prefinal_publish_recheck_v1`:

```sql
if v_publication.correction_session_id is null
   and v_publication.authority_scope_kind='CYCLE'
   and (exists(select 1 from public.weekly_source_format_profiles profile
     where profile.id=v_upload.source_format_profile_id and profile.profile_code='NHSP_PREFINAL_RELEASED_V1')
     or exists(select 1 from public.weekly_source_row_resolutions resolution
       join public.weekly_source_upload_rows source_row on source_row.id=resolution.upload_row_id
       where source_row.upload_id=v_upload.id
         and resolution.generation=coalesce(v_publication.projection_generation,v_publication.authority_scope_version)
         and resolution.mapping_state='RESOLVED'
         and private.weekly_source_is_import_authoritative_v1(resolution.client_id,resolution.contract_id,source_row.work_date))) then
  perform private.weekly_source_candidate_prefinal_publish_recheck_v1(v_actor,v_publication.id);
end if;
```

For an explicitly empty checking file that must resolve missing signed shifts, root must extend the second arm with the same authoritative signed-root discovery, rather than using “some resolved row exists” as the sole trigger. Use `cardinality(private.weekly_source_publication_signed_roots_v1(v_publication.id))>0` as that additional arm. This includes signed claims omitted entirely by the file; report scopes never enter this branch. Original import outreach follows as before. The complete new recheck already invokes unsigned expected-week materialisation and scoped Monday dispatch. It never creates a worker question for an unfinalised row.

## Finalisation: exact selection before automatic close

After the real Surrey/Magnit selection and native final publication succeed, in the same outer FIN transaction:

```sql
perform private.weekly_source_unprotected_not_finalised_close_v1(v_cycle.id,null);
```

The report finaliser calls the same helper with its actual report-scope ID, after its exact accepted report/export membership and pay-finalisation admission. The helper verifies the report's current native publication guard and effective dated rules, then checks four separate self-bill facts. It preserves any selected pay or accepted-report membership. Missing/foreign/stale/partial protection/report evidence raises and rolls back; it is never interpreted as false. No email/digest/empty intent is produced by closing. The shared incident contact retirement is also used by the native protected completion owner.

## Source protected owners and lock graph

Pages 16–20 are complete Source-owned successors of the actual current prepare-family, prepare-action, action-context and stage owners. They add no financial calculation, schema/version reinterpretation or Banking function definition. Normal admission → native physical I-1 → existing Source run/target/request locks → group SHARE → original cycle lock. Exact family/root/group selectors are rechecked. The native prepare-family existing null→non-null base-week root growth and rotation fences remain intact; its late lock is reentrant or on a freshly minted unpublished root only.

The stage's immutable exact replay is an early read-only lookup; a fresh stage takes I-1 then repeats the original request lock/replay check before normal stage effects. Optional bulk/automatic metadata must exactly match the immutable sealed proof on either replay branch; adding, removing or changing it refuses. With neither optional key, the original stage request/hash/DTO business path remains intact. Only optional bulk/automatic stages upgrade the first cycle lock to UPDATE. Original mutation tables, native function calls, state/hash domains, ACL and source/NHSP business arms are retained, with the declared optional proof/certificate calls. Source and pure tests check these boundaries; they are not database/concurrency proof.

Money owns complete family-first FIN and standalone lineage successors. Do not install a competing lineage definition from this package. Complete-local already takes the native Candidate serial gate then I-1 before Source target/request/generation/run/root/current-fin locks. Its single-root entitlement publication re-enters the same gate/I-1; the immediate/core bodies have no direct group/cycle locks. Its bounded TSFIN path remains its existing authority. Native stage current-decision capture takes the already held target-family lock and current-decision upsert only. No Banking owner is edited.

**Remaining joined graph gates:** Candidate submission-complete and workflow/finalisation/TS/CW outer callers; Mode A/issued-query comparison callers; source publication/build/stage caller union; native Banking external entries and any lock held before they call Source; group settings/Client save; correction prepare/commit; exact source-generation/module ownership stability; real two-session tests with both winner orders. Family-first FIN plus pages 16–20 alone is not a complete concurrency proof. Activation stays held.

## Delivery, read composition and pending features

P2b uses the existing QD manager generation, `weekly_manager_cohort_due_events`, message-intent/render/dispatch and secure-manager-review owners. One nullable comparison FK narrows the existing EARLY_ALL event to its exact (a) revision (including its proved unsigned changed-source lifecycle). NULL legacy events retain their existing scope except the new dated import-authoritative (a) exclusion, preventing another ordinary timer from bypassing Monday admission. The publication's actual London clock owns the first Monday; deferred audit has no future mail intent. Native accepted batch business facts own unchanged questions; changed clocks can re-ask. Disable/manual-only route choices remain respected.

D10 continues its separate existing `hr_issue_emails`/`mail_outbox` issued-HR query machinery unchanged. Root controls actual safe-recipient/outbox delivery admission; local source staging is not sent-mail proof. Manager secure pages, grouped/partial submissions, reminders, provider retry/ambiguity and read projection have separate acceptance gates.

U1/U2 composition must consume the real active-owner first-Authorise/Create gate and scoped withheld amount. Existing V3 exception facts and approved I-1 entitlement inventory do not carry that pending amount; no hold amount, proposed difference, paid flag or READY is inferred. The positional query-group successor is complete, but the full new hold/status/read DTO union awaits that exact producer. Root owns shared readers and routes. Unsupported new actions stay disabled until their source and joined gates are finished.

## Native bulk producer and bounded rounds

Root's existing command route should invoke `orchestrateWeeklyProtectedBulk` from the adjacent `protected-bulk-action.mjs`, injecting the authenticated active pay actor and the same actual native `dataRpc` and `calculateWeeklySnapshot` dependencies used by the existing protected action. Never forward browser actor, staged request, financial payload, phase or completion flag. The only browser fields are `source_cycle_id`, `mode`, `expected_selection_sha256`, `bulk_request_key`.

The adjacent stage-only module contains a complete unchanged individual composition plus trusted stage extraction. Root may integrate its declared extraction into the current owner or mount this adjacent source after source review; never overwrite a newer individual successor. The original individual file was not edited. `stageWeeklyProtectedAction` calculates/stages through the real existing producer and returns only the actual immutable request ID/hash/context. It does not call C1 or complete_local.

The public service-only `weekly_source_protect_all_signed_atomic_v1(jsonb)` supports PREVIEW and internal COMMIT phases ADMIT/ROUND/APPLY. Existing authoritative audit stores the initial exact server selection and each actual native round result. Request key is ASCII `[A-Za-z0-9:_-]{16,120}`; no new receipt table. ADMIT uses the existing group SHARE/cycle UPDATE mutex and exact post-lock group CAS, then releases that RPC transaction before preparation. APPLY accepts exactly one actual staged request; Candidate serial admission precedes one complete native I-1 array, then group UPDATE/cycle UPDATE; post-lock original claim, sealed proof, Source CAS and current selection recheck precede the unchanged complete_local savepoint. No remaining Candidate gate can be acquired after a family lock.

The trusted Worker performs at most eight rounds by default (bounded 1–32). One shift globally per round preserves native pending-generation uniqueness and the complete prior vector. Each next round must retain the original source/signature/claim or prove its own exact completed native receipt. Partial results and continuations are explicit; a whole command is not atomic. Full multi-family/native runtime acceptance remains a root gate.

Root must install these actual purpose owners before the new command can run:

```text
private.weekly_source_bulk_producer_ready_v1(uuid) returns boolean
private.weekly_source_bulk_source_cas_v1(uuid,uuid[]) returns jsonb
private.weekly_source_shift_on_self_bill_context_v1(uuid,uuid,jsonb) returns jsonb
```

Source CAS closed shape is `{schema_version:WEEKLY_SOURCE_BULK_SOURCE_CAS_V1,source_cycle_id,client_ids:<sorted UUID array>,source_cas_sha256:<hex>}`. Root's census owns exact current export/report/rules/binding/resolution/final snapshot/revision/projection/receipt/financial IDs and pointers, excluding only protection-stage rows. Actual readiness comes from existing controlled activation/latest VERIFIED release and full native/client/NEXT/security acceptance. All relevant OPEN question Clients remain in CAS even when skipped/protected, so a completed round cannot shrink the original census. Do not narrow the readiness scope silently to enabled self-bill groups; document/close any import-authoritative NULL-rule/non-self-bill activation gap without changing a business setting by guesswork.

The sealed stage input `bulk_signed_selection_proof` has only `expected_selection_sha256`, `expected_source_cas_sha256`, `incident_id`, `bulk_request_key`. Native stage recomputes actual eligibility/CAS under locks and seals actual claim/actor/source/financial/request/target hashes in the existing complete vector. `private.weekly_source_bulk_own_stage_v1(uuid,uuid,uuid,text,text,text)` returns only exact validated exclusion IDs after fresh Source CAS and complete native request/approval/generation/run/claim proof. The context object is `WEEKLY_SOURCE_BULK_OWN_STAGE_CONTEXT_V1`, with cycle/actor/key/selection/Source CAS/sorted Clients/exclusions. Root's overload reuses the same complete four-fact bodies and normal strict readers, excluding only that sealed unaccepted own WAIT. Never copy a browser flag, use a GUC, clone classification truth or stub a CAS/ready owner.

## S18b automatic reconciliation

Root's existing import/finalisation dispatcher may invoke `orchestrateWeeklyAutomaticReconcile` from `protected-automatic-reconcile.mjs` for an actual cycle/work event, with the same native dependencies, only after actual capability qualification. No scheduler or route is mounted in this package. The service-only `weekly_source_automatic_reconcile_atomic_v1(jsonb)` PREVIEW checks the configured global actor's current pay permission, native qualified WAIT, applicable CLIENT_SELF_BILL/S18b immutable rule version, exact self-bill membership and final Source clock/net tuple. SAME_FILE membership uses actual bound export/S25 window; SEPARATE_FINAL_REPORT uses actual bound accepted report. No settlement or generic paid flag is used.

Root must supply:

```text
private.weekly_source_automatic_reconcile_producer_ready_v1(uuid) returns boolean
private.weekly_source_automatic_reconcile_delta_assert_v1(uuid,uuid) returns jsonb
```

Readiness is actual controlled activation/latest VERIFIED release plus a qualified complete native assertion. Its absence/false, or missing assertion, raises `WEEKLY_SOURCE_AUTOMATIC_RECONCILE_DELTA_UNAVAILABLE` before prepare/calculator. Null/inactive/ineligible configured actor returns the truthful decision text. Equal clocks alone never certify money.

The trusted stage-only input `automaticReconcileSelectionSha256` becomes native optional `automatic_reconcile_selection_sha256`. Stage recomputes selection before unaccepted event writes, seals `automatic_reconcile_proof` in the existing complete vector, and invokes root's certificate after actual native generation/request/components/stage-step writes but before the same transaction returns. Any assertion refusal rolls back the entire stage RPC. A later concurrent change after successful preflight can leave an existing native preparation run; preserve/recover it truthfully. No claim of zero preparation writes is made.

The accepted root certificate is closed: schema `WEEKLY_SOURCE_AUTOMATIC_RECONCILE_DELTA_V1`, exact publication request/actor IDs, prior inventory digest, complete vector SHA, decimal text `delta_pay_ex_vat='0.00'`, qualification SHA. Root must prove complete native canonical identity/membership/finance/policy/rates/additional/expense equality; certificate total alone is insufficient. See `AUTOMATIC_RECONCILE_COMPONENT_CENSUS.json` for exact actual producers and the native stage-step-only K→K+1 bound normalisation restriction. The immutable generated JS fixture pins transport/identity shape only; it is not native approval proof.

COMPLETE takes native Candidate gate, complete I-1, group UPDATE and cycle UPDATE, reasserts actual complete delta, then invokes unchanged complete_local. Only actual PUBLISHED creates the “Cleared automatically · hours matched” authoritative audit. SAVED_PENDING_FREEZE refuses and rolls back this completion. An exact server-proved pending stage is recovered without recalculation. The ordinary manual reconcile path remains unchanged. Missing proof stays a named engineering refusal; no fabricated zero or cleared state is emitted.

## Final managed registration

Register `supabase/verification/13102026_1438_weekly_source_protection_queries_v1.sql` for actual NEW and UPGRADE after all final definitions and final security. Union every actual signature from `PROTECTION_SOURCE_MANIFEST.json`, including the due trigger, with root's current central **105 tables (39 immutable/19 lifecycle/47 stateful), 157 private/139 service signature** union; do not replace it with this older lane's registries. Final closure generators, exact first-use source compilation, per-route oracles, native component fixtures/round recovery and the joined lock/runtime graph remain root gates.

# F8 exact integration inputs

Source only, 13 October 2026. Root owns the controlling normal index, combined FIN/report/read owners, ACL union, release JSON and all installation. These additions do not close runtime, financial, document, phone or activation gates.

## Shared finaliser

Money's G1 lock, unconditional after `weekly_source_pay_query_admit_v2()` and before the first cycle lock and any revision write:

```sql
perform 1 from public.weekly_source_groups
where id=(select source_group_id from public.weekly_source_cycles where id=v_cycle_id) for update;
```

Immediately before the existing `if not private.weekly_source_import_is_prepared_v1(v_upload.id)` in the complete controlling FIN engine, after existing actor/current-publication/correction-session proofs:

```sql
perform private.weekly_source_final_upload_assert_v1(v_upload.id);
```

This call is unconditional for ordinary and correction PREPARE execution modes. Its NULL-rule/NHSP result preserves legacy admission. Keep the existing prepared check, source cutoff, selected-publication hashes, exclusion acknowledgement, overlap checks and native NEXT hooks. A Wednesday file stays ineligible when Finalise is opened on Thursday. Full-week checks also apply to legacy `import_use`, correction-purpose and saved preparation arms. Do not gate on current wall time in place of `uploaded_at_utc`.

The current helper covers only the three existing weekly export profiles. Root's new report producer must union a purpose-bound report timing arm after its scope/profile contract freezes; report coverage is exempt from the complete-week export rule. The helper currently refuses an enabled-rule unknown profile, so the unpublished report cannot accidentally become actionable through this export arm.

## Ordinary and combined read projections

Add a local `v_final_upload_gate jsonb` to the controlling ordinary Workspace reader. After the existing upload/publication has been selected and authorised, resolve:

```sql
v_final_upload_gate:=private.weekly_source_final_upload_gate_v1(v_upload.id);
```

For export files with enabled rules, expose this exact optional object as `finalise.file_admission`. When its `allowed` is false: keep the current import/check record and ordinary Office checks; do not create an issue; clear Finalise ready/blocked movement rows and the actionable finalise payload; set `finalise_enabled=false`; suppress `prepare_action`. The grey file line uses `file_admission.message`. Do not suppress current functioning signed Journey panels or introduce any mail claim.

The existing Prepare-action branch must additionally require `(v_final_upload_gate->>'allowed')::boolean`. The combined Finalise reader must carry the ordinary producer's `file_admission` into each existing scope object; it must never infer permission or recompute S12 in the browser. Existing PAY permissions remain queries-only and receive no Finalise field/action. Legacy NULL-rule output can omit the new field entirely.

These are required read-owner integration hunks, not installed producers in this isolated lane. Root's PP/read-owner union must make them concrete before Office publication. Office must render the supplied amber warnings and grey file message through its existing preview and report controls. An unsupported producer stays disabled.

## Complete adjacent owners and prerequisites

The five new repeatables in this worktree are complete definitions. Prepare, Client Save, Upload Context and upload-stage changes are byte-preserving outside the declared guards, checked against their exact saved input bodies. Root must copy/union these definitions into its latest owner closure; it must not overwrite a later report, PP or money branch with the older input bodies.

Client Save takes its existing client advisory gate, discovers the old dated group, locks old/new group rows in sorted UUID order before the native new-group lock, rechecks the membership and refuses any CURRENT old-group revision bound to the client by a scoped cycle, exact report or client manifest. This covers ROSTER, NHSP, import-only and agency groups without a rule/financial-state exception. FIN and every new report finaliser must use unconditional G1 before their cycle/revision writes. The client owner then preserves its existing policy/membership writes and stale-open cycle handling. The global revision-table lock was rejected because it could invert cycle locks.

Private owner-only ACL union signatures:

```text
private.weekly_source_final_upload_facts_v1(text,date,timestamp with time zone,timestamp with time zone,text,date,date,jsonb,date)
private.weekly_source_final_upload_gate_v1(uuid)
private.weekly_source_final_upload_assert_v1(uuid)
private.weekly_source_rule_coverage_floor_v1(uuid,uuid)
private.weekly_source_upload_coverage_context_v1(uuid,uuid)
private.weekly_source_upload_coverage_floor_assert_v1(uuid,uuid,text,date)
```

No new public RPC is introduced. Existing public Prepare, Client Save, Upload Context and upload-stage signatures/grants are retained. Add `supabase/verification/13102026_1412_weekly_source_timing_coverage_group_guards_verification.sql` to both `verificationFiles` and `newVerificationFiles`; its relative SQL vector input is part of the release closure. It deliberately fails until the actual controlling FIN has both G1 and the timing assertion.

## Facts and remaining proof

Coverage floor is the start of this client's first applicable rule week: exact retained RULE_SET activation receipts, including disabled historical assignments, intersected with the client's dated membership and same-agency/environment immutable version; exact frozen revision/client witness also qualifies. Never-enabled draft assignments and other clients' earlier weeks do not contribute. Malformed claimed activation or no provable applicable date refuses. The controlled source census found only assignment INSERT/forward clipping and the feature transition as writers; no RPC delete/clear path. `STATEFUL_SERVER_OWNED` denies table access to browser/service roles but does not provide an immutable DELETE trigger, so this is a qualification about controlled source/RPC history, not absence of arbitrary owner SQL.

S16 uses the latest WAIT family event's saved work date and nonwithdrawn approval in this exact client/group. Last completed Contract week uses a noncancelled native Contract week that has ended in UK local time. Widening warning is conditional: absent finalised shifts inside the wider complete export range will be cancelled. None of these warnings changes pay, invoice, source or permission authority.

S11 source closure remains with the current factual timestamp/state producer: Surrey requires actual Finalised Date plus By; Acacium timestamps support chasing and never make a line final by themselves. No F8 code substitutes uploaded time for a source row's finalised time.

Pending root proof: actual installed role/ACL and function closure; helper SQL vectors; real PREPARE/FINALISE stale, short-coverage and Wednesday/Thursday cases including historically prepared paths and Magnit cutoff; exact-client floor through disable and late membership; native CURRENT-revision group-move negatives including NHSP/import-only, sorted-lock graph/concurrency with every finaliser; real service/PostgREST content/count semantics and CE1–CE6/CN1–CN2/I9; actual Office warnings/grey lines; protected release, NEW/UPGRADE and off-PC source backup before runtime/Office publication. No SQL vector or source mock is document/phone/financial acceptance.

# F-25 acceptance capture and enqueue source review

9 October 2026. Independent read-only review of `supabase/repeatable/09102026_2233_banking_pay_next_acceptance_capture_v1.sql`, fresh saved SHA-256 `27d2a3316477ace13929590ffc5573a1d75c4e172bd3eb9df3b09629ab5bec7c`. Compared with the F-25 card/checklist, accepted-cutoff/case/query lock designs, locked 2153/2225 schema, six 2153 guards and actual Source/case owners. Root alone writes product. No SQL, runtime, tests, fixtures, hosted reads or product edits were performed. **CHANGES REQUIRED — the two capture findings below; full F-25 remains incomplete.**

## Concrete findings

### F25-C01 — P1: selected automatic case origin bypasses its genuine qualification

2233:278–280 obtains any `bpay_next_work_collection` matching case/component and labels it WORK_COLLECTION. This does not establish the existing `private.bpay_next_work_collection_origin_v1(case,component,Candidate)` contract at 1519:18–59: READY and paid qualification, exact applied revision/approved/realised observation, immutable original line/tax/channel, actual conserved case/component relationship and original policy. A collection may retain the same valid case/component FKs while its qualification is CURRENT_APPROVAL_UNBOUND or CURRENT_SOURCE_UNBOUND; the new capture still accepts that row as this Draft's economic origin. The accepted-case schema only constrains the referenced ID, so it does not close this gap.

For **selected** automatic cases, prove the actual qualified owner once at fresh acceptance, under exact original WORK/position/collection/case/FK locks acquired before the clock, and retain its genuine original identity/policy. Preserve the current selected Source-collection refusal in final ordinary case owner 1958:264–270 until its real comparison owner is installed. Do not make a raw Source collection an ordinary accepted origin. Explicit excluded items must preserve their exclusion semantics rather than acquire financial eligibility just because their identity is retained. The old owner also checks genuine CASE_CREATE component/source/principal shape at 1958:288–297; retain those checks in the accepted path rather than treating a case-level request FK alone as complete origin qualification.

### F25-C02 — P2: three accepted version FK targets are first locked after the clock

2233:97–109 selects typed Candidate/Umbrella/policy version children without row locks; 112–114 locks only their parent `bpay_next_input_version_event` rows. The accepted-input INSERT after `receive_command_v1` (337, 358) has exact FKs to the **typed child** `(event_id,Candidate)`, `(event_id,Umbrella)` and `(event_id,new_policy_id)` targets (2153:473–479). Locking a parent event is not locking those three target tuples. Their RI checks therefore take new KEY SHARE locks while the agency clock is held, contrary to the asserted final-tail contract at 336 and the agreed original business/FK evidence order.

Prelock the exact selected typed children before the clock, with deterministic order and unchanged identity/epoch applicability. Alternatively provide a concrete existing same-target lock proof; no such lock is present in these two bodies. Keep accepted-input INSERT guards free of later business locking and preserve the immutable receipt hashes and original epochs. This finding concerns explicit source lock closure; no observed contention or runtime failure is claimed.

## Sound source behavior

Exact immutable run/command retry returns before fresh capture. Concurrent enqueue rechecks the accepted command after the run lock. The producer uses the existing Source admission key and module SHARE, locks run and Candidate controls before selected WORK/revision and stages typed records before the sole existing PREPARE clock. Unsupported target channel and unavailable Umbrella fail explicitly. Raw floor remains BOUND/UNBOUND with no synthesized zero; actual current-epoch settings witnesses and baseline choice are retained.

Source WORK captures the real full query-facts result, complete scope/digest and genuine 2153 origin before allocation. The stage trigger independently compares those exact canonical results; later insertion consumes the exact same-transaction record. Missing or blocked evidence refuses instead of manufacturing CLEAR. ALL retains every effective line; SUBSET maps original selected lines by the full component/source-component/kind/date/tax tuple and requires exact member count. Original choice revision is distinct from effective revision, and a failed/ambiguous mapping refuses. Initial certificate/binding hashes are copied from the genuine retained parent proof, rather than reconstructed from a later current head.

Case capture retains the exact sealed selection, selected/excluded flags, rule/status/due/funding choice and real FUNDED event/time. It copies no principal/funded/recovered/paid/held balances. The enqueue tail creates member identities without advancing background enrollment, hashes accepted inputs using the same UTF8 schema, inserts typed durable children then consumes staging. The existing deferred consumed assertion remains active. Both functions are private, logically postgres-owned, fixed-search-path and revoked from PUBLIC/anon/authenticated/service_role; no new public capability, bypass or external effect is added.

## Pending dependencies and review limits

The referenced `private.bpay_next_stage_accepted_week_v1(uuid,uuid,uuid,uuid)` at 290 is not yet defined at this checkpoint. It must qualify exact original weekly basis/eligibility command/projection/outcome references and prelock every real FK target before allocation; a live contribution counter is not historical authority. This is a known unfinished dependency, not an assertion that invocation has passed.

The true outer public SEAL_ENQUEUE owner must enter Source admission before its existing authority/run locks; the nested entry in 2233 is insufficient by itself. Background enrollment must validate/reuse the accepted member identities instead of inserting conflicting identities or claiming early enrollment. Root reports a new 2236 successor for those and downstream CLAIM/PIN/query-pin changes; that source is reviewed separately after this capture checkpoint. Actual CASE/WEEK INSERT provenance, other accepted consumers, producer-finalization/lock closure and authorized verification remain required. Neither this review nor a static catalogue gate marks full F-25 built, installed or app tested.

## 2236 dependency follow-up

Fresh 2236 SHA-256 `2881978637f38c72a6db81ed982f11a0e684bae6bac54a24bfbbb76979c3203c` passes its separate five-routine source review in `F-25-consumers-review.md`. Its public outer probe now enters Source admission before the original locked authority/run work for fresh SEAL_ENQUEUE, and its enroller validates/reuses accepted member identities before creating jobs. Those two previously pending dependencies are closed for this source checkpoint. Capture C01/C02 and the undefined weekly helper remain open; full F-25 remains incomplete.

## Fresh C01/C02 correction review

Fresh capture SHA-256 `7270db42561fc96504a547ffaed49267313aab604de3c81bdc5825bbed0de25c`. C01 is closed for selected ordinary collection provenance: the producer locks original WORK/position, calls the actual qualified collection owner and requires a returned ID; Source comparison refusal is restored. C02 is closed by exact KEY SHARE locks on all three selected typed version FK targets before the PREPARE clock. The original/effective lineage, canonical Source stage certification and immutable enqueue tail remain intact. **CHANGES REQUIRED** persists solely for C03 below in this capture checkpoint, with known unfinished weekly/other F-25 dependencies unchanged.

### F25-C03 — P2: automatic qualification now incorrectly applies to excluded items

The enclosing case-item loop at 255–257 includes all exact sealed selected **and excluded** items. Its new block at 291–299 unconditionally refuses a Source collection or unavailable `work_collection_origin_v1`, even when `v_case_item.is_selected=false`. In the real existing ordinary owner, both Source refusals at 1958:116–142 explicitly require `si.is_selected`; financial instruction capture at 253–255 likewise considers selected items only. A Draft with a legitimate sealed exclusion of a Source collection, or an excluded automatic collection whose present qualification is unbound, previously avoided that unsupported financial lane. This new acceptance block instead refuses the entire Draft despite the explicit exclusion.

Require supported financial origin qualification/Source comparison refusal for selected items. For excluded items retain the exact genuine collection/component and original policy identity and is_selected=false without promoting them to eligible instructions or requiring a comparison they will not execute. Preserve the complete sealed exclusion receipt and its immutable FKs; do not drop excluded items, silently select them, or weaken selected-origin qualification. No runtime failure or actual affected business row was observed by this reviewer.

## C03 selected-only successor verdict

Fresh final capture successor 2247 SHA-256 `fcbbd3c4e6a5f9fe41aa4762317beedadcf5f5a67ee03dfb43f59141c326b85a`. C03 is closed at 360–378: financial collection qualification and Source comparison refusal are inside `v_case_item.is_selected`; excluded rows retain exact identity/policy and the explicit exclusion flag. C01/C02 remain closed. No remaining origin/exclusion/typed-FK defect was found in that changed block.

The separate `F-25-settings-producers-review.md` records new SP01: fresh acceptance must reject an existing FINALISING/CLOCK_SEALED current-transaction latch before its new business locks. That review owns the fresh clock-fence finding; this follow-up does not claim the entire capture path passes while SP01 is open. The weekly helper and other F-25 dependencies remain unfinished. No SQL/runtime/tests or product edits were performed.

## Fresh acceptance phase fence follow-up

Fresh 2247 SHA-256 `d8b27fc254df323871eb0734812f499ccdc9c327a41cfa81018066d1aee2d911`; 2254 `e26f1d705341d4942103ac55caad3f51a7ca84673de270ec2697d6e5eaf869ab`. SP01 is now closed by phase checks before new admission/business locks in the public fresh probe, private enqueue after exact replay, and stage entry. C01/C02/C03 remain closed. **PASS — source review of the implemented capture/enqueue subset**, with no remaining concrete finding in those saved changes. The missing weekly helper and full F-25 downstream/producers remain incomplete; no invocation, installation, runtime or app-test success is asserted.

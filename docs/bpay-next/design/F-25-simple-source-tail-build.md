# F-25 simple Source tails: concrete chunk 4 build

10 October 2026. Independent saved-source design only. No SQL, runtime, tests, fixture, hosted or product execution/edit was performed. Root alone authors successors. This is a build design, not installation, runtime verification or whole-F25 completion.

The shortest defensible implementation is a **Source-specific prepare-all/receive-all split**, followed by finite completion metadata. Move all current Source business updates and current evidence discovery before the first original `POSITION_APPLY` clock. Initial TSFIN binding and protected receipt/response have actual publication dependencies, so unchanged helpers cannot all run before that clock. Their remaining work must consume a privately prepared exact plan, preheld existing FK targets and only this owner's new rows. No second `QUERY_INPUT_VERSION` command, global terminal-latch relaxation or new payment job is appropriate.

## Exact current dependencies

| Owner / physical authority | Current sequence | Dependency that prevents a simple call move |
|---|---|---|
| `public.weekly_source_first_authorise_v1(uuid,uuid,text,uuid)`, `09102026_2018...sql:3528–3618` | Pure identity :3561; Source admission :3569; original lock :3572; unchanged real core :3592; initial stage :3603; original pair :3605; initial binder :3609; return :3612. | Core produces the genuine `root_authorisation_id`. Stage uses that exact ID as revision/source event. Binder :148 invokes current certificate, which requires an actual sealed publication at `0018:32–70`; it also takes worker/WORK/revision locks :127–130 after publication. |
| `public.weekly_exceptional_pay_publish_next_v1(jsonb)`, `04102026_0345...sql:429–947` | Pure authorised immutable replay :517–540; fresh admission :545; booking/root locks :559; family/run mutex :585–587; actual authority/current inputs; approval/generation/events/pending target :841–905; stage :907; pair :910; generation/family/run updates :912–925; receipt :926; payment event :931; real step :937; return :943. | Protected receipt's `agency_sequence` is NOT NULL and positive. Its actual command FK is immediate. The step response calls `bpay_next_protected_receipt_json_v1`, which reads that actual receipt and exposes its actual sequence as text. No valid preclock guessed sequence exists. |
| Original pair `03102026_1540...sql:25–151`; publication primitive `03102026_1430...sql:7–101` | Candidate KEY SHARE; worker-control UPDATE; WORKs UPDATE; revision union SHARE; physical roots KEY SHARE; all-or-none replay; then each WORK publishes/seals and receives in WORK-ID order. | Each primitive still locks its selected revision **FOR UPDATE** :47–48. Pair's earlier FOR SHARE is weaker. The second member can upgrade after the first clock. Each primitive also invokes retention :67 before its own receive. Retention inserts `timesheet_financial_retention ON CONFLICT`; the second insert/unique conflict remains after the first clock. |
| Common HEAD receipt publisher `06102026_1301...sql:9–88` | Stage members ordered by booking/root :38–55; direct binding lockset :61; pair :62; component binding :68–83. | Common binder needs real publication tuples. Its lockset is not a proof of every binding-row or RI target lock, and common outer owners still perform head/bundle/push/Local work after this trigger. Do not declare chunk 5 closed by fixing these two simple owners. |

Initial core is the final physical `26092026_0202...sql:1411–1656` included by `28092026_1234...`. Preserve it. It checks real actor, accepted lock result/current family, already-authorised/root-integrity/manual/open-hours refusal; calls the unchanged ordinary Authorise owner exactly once :1559; derives signature and authorisation generation; and inserts the genuine root-authorisation record :1625. It returns the actual event UUID :1638. It has no universal completed-replay success shortcut: already authorised returns its existing refusal. Do not add a fabricated successful replay or put a financial publication inside this core, which is also used by two-root common publication.

The actual retention function is `26052026_2100HRS_NEW_FUNCTIONS.sql:183391`: sorted exact Timesheet KEY SHARE :183427–183432, then retention INSERT :183445–183449. It does **not** UPDATE Timesheet. Moving this exact owner before the first Source receive removes its postclock uniqueness/callback exposure without inventing a stronger Timesheet UPDATE lock.

## 1. Prepare every original member before receiving any

Factor the Source path of `bpay_next_publish_staged_revision_v1` into two private parts; proposed names below are design names, not existing functions. Preserve the current public/private signatures and return contracts with full successors and F-04 chains.

`source_member_prepare` contains exactly the current preclock work: module check, WORK/revision identity and physical-current checks, actual retention mark, exact immediate predecessor lookup, `current_revision_no`/predecessor integrity, revision approved/sealed timestamps, WORK pointer/no/state/timestamp update. It returns a finite retained plan: Candidate, WORK, selected revision, actual physical root/version, source kind/event, deterministic command UUID, revision number, predecessor revision and actual original all-or-none replay classification. No money/counter/history snapshot or recalculation is added.

The true pair owner must first:

1. Preserve its one/two-array shape, duplicate, same-Candidate and exact source-event checks. Resolve real replay **before any fresh pointer/seal preparation**: every exact command/publication exists or none exists; partial remains a hard original refusal. Existing sealed revision without its publication remains refusal. Retain this owned result; do not infer it later from `xmin`, timestamps or status.
2. Acquire current Candidate KEY SHARE, worker-control UPDATE, all WORK UPDATE locks in native WORK UUID order; acquire selected revision UPDATE locks before the first receive, and the remaining current/applied/predecessor immutable revision union in stable order at its necessary read/FK strength. Do not rely on SHARE then upgrade on the second member.
3. Run every member's preparation in original WORK-ID publication order. All retention insert/unique handling, physical/root reads, WORK/revision writes and existing predecessor FK target protection finish here. Prehold actual approved-line/other completion FK targets identified below. No second member may discover a new member, predecessor or physical root after the first receive.
4. Prepare the owned Source certificate/finite remaining callback manifest while the input transaction is OPEN. In fresh mode register the exact one/two deterministic command IDs and remaining original receive order. Replayed pair returns existing exact sequences and creates no new Source clock.

`source_member_receive_prepared` accepts **only** that current-xid/current-epoch prepared plan under the exact active Source producer. It performs the unchanged original `receive_command(...,'POSITION_APPLY')`, member INSERT, publication INSERT and command SEALED UPDATE from `1430:88–99`, then returns its actual sequence. All existing business RI targets are already protected; command/member/publication references after receipt are exact same-owner newly created records or preheld actual predecessor/Candidate/WORK/revision rows. No current Source reader, retention insert, business lock, pointer or revision preparation remains here.

The current `0002` receiver intentionally refuses an ordinary command while any query producer exists. Its successor must allow **only the prepared original Source member** at its recorded index, exact command kind/UUID/WORK/revision/event/epoch; the second pair member may consume the remaining original slot while the parent input transaction remains CLOCK_SEALED. Every unrelated command still refuses. Source finish never calls `receive_command`.

Leave ordinary callers' original publication behavior intact. A Source-specific factored path is sufficient; do not make unrelated ordinary calls depend on a Source manifest. Replayed publication must still return its original sequence before fresh discovery.

## 2. First-authorise: prepare exact I-1 binding, append after original clock

Use the public first-authorise wrapper as true root. Establish `SOURCE_FIRST_AUTHORISE` producer/admission/module ownership before :3572, preserving the original role/input/identity/rotation checks and the core's actor/Office authority. The semantic cause hash must use finite original request terms (Timesheet, expected identity/signature, actor), not invent a universal command UUID or log raw evidence. Later completion is anchored to the **actual** new root-authorisation ID and its existing decision digest.

After successful core and `stage_source_current(root,event,NULL)`, perform member preparation above. The staged current I-1 authority is already exact: `1550:112–155` holds root, Contract, current financial snapshot and actual live authorisation; :159–165 permits initial snapshot only when event=actual authorisation and no HEAD exists; :191 onward retains canonical inventory/approval basis and exact approved lines. These remain the source of truth.

Split `0018` certificate/binder into a fresh private preparation and a bounded completion, keeping existing external certificate/bound-assert semantics:

* Before the clock, validate all the current I-1 checks in `0018:34–47,72–101` against the actual staged/prepared sealed revision and actual I-1 reader. Preserve origin/basis/detail/policy/component hashes, component line number/key/evidence/amount/classification checks. Publication is not yet an accepted fact: store its **expected deterministic command UUID** as a prepared dependency, and require the actual corresponding publication only at completion. Do not simply omit publication proof from the committed certificate.
* Resolve exact absence or stored binding replay before clock under the existing Candidate worker-control mutex. Current fresh public core normally supplies a new event; `0018:131–146` existing replay never advances the pointer and must remain separate. A component already bound for the Candidate is the original refusal, not a new version or repaired historical binding.
* Retain a bounded private prepared plan tied to producer/current xid/epoch and its canonical digest; references plus original origin/basis/component digests and planned own binding UUIDs are sufficient. A loose caller JSON/boolean is not proof. Root must provide actual typed plan storage/validation in its successor if no existing record can prove this prepared packet; `2352` group/member schema alone does not store every initial binding operand.
* After the one original pair receive, complete by matching the actual command/publication to the preclock plan: exact ID/WORK/revision/Candidate/revision number, current epoch, `POSITION_APPLY`, sealed actual command/member1, genuine queued publication, exact source event. Consume the retained I-1 plan; **do not rerun current inventory or acquire worker/WORK/revision locks here**. Append the same binding version/current pointer with the original values/digest formula from `0018:157–178`. Only this owner's fresh metadata rows and preheld targets are touched. No position, book seed, financial effect, hold or job is created.

The binding digest uses deterministic command UUID, not agency sequence (`0018:158–159`). However `2010:142–145` has **immediate** composite FKs to actual publication tuples, and the existing certificate requires those tuples, so unchanged binding cannot precede receipt. Changing just its call order fails. A further fully-preclock binding arrangement would require a separately reviewed narrowly deferrable publication FK plus exact prepared-publication validator and named own constraint closure; it is unnecessary for this bounded append design and must not be introduced implicitly.

Actual initial-binding existing targets to acquire/resolve before receipt:

| Target | Required identity / strength |
|---|---|
| Candidate / root / Contract / TSFIN | Exact staged Candidate KEY SHARE; actual physical Timesheet KEY SHARE (authorisation owner may hold stronger); Contract KEY SHARE/SHARE as already held; exact current financial snapshot SHARE as in1550. Initial binding stores actual snapshot ID. |
| Authorisation | Exact current initial event SHARE and its actual root/version/generation/signature/digest. New own authorisation row needs no external discovery after clock. |
| WORK / selected revision | Existing worker-control UPDATE; WORK UPDATE; selected revision UPDATE during member preparation. No postclock lock upgrade. |
| Approved/classification lines | Complete exact staged revision's line census; every referenced line ID/key/component KEY SHARE before clock. Same line serves classification for initial binding. Empty genuine I-1 inventory creates zero bindings. |
| Existing bindings/versions | Any actual matching Candidate/component binding or stored initial version resolved beforeclock; fresh original refusal remains. No generic history traversal or absence-based synthetic component. |
| New publication and binding rows | Exact prepared command/publication of this same owner; new version UUID and new pointer tuple bind to the complete actual line census. Deferred cyclic pointer/version checks reference these exact new rows; close only named own constraints, not SET CONSTRAINTS ALL. |

Initial binding tables are outside the19 canonical PAY_QUERY relation inventory; do not add fictitious query-tail targets for them. Their exact postclock metadata closure still needs the private Source plan validator. Initial canonical root-authorisation event is staged before clock with ordinary ordinal, so this route needs **no reserved postclock query ordinals** if no other real canonical gate callback remains. Finish the Source group only after complete initial binding and its named FK checks; external `initial_tsfin_certificate/bound_assert` remains the original full proof once committed.

## 3. Protected NEXT: move business updates, preinsert owned step, retain two finite completion targets

Preserve role/shape/key/actor gates :492–514 and the original exact replay/auth branches :517–540, :561–581 and :591–607. An exact immutable completed reply returns before fresh producer entry. Fresh `SOURCE_PROTECTED_NEXT` ownership enters at :545 before booking/root locks; keep original lock order and locked revalidation. Do not move financial clock ahead of real Office authority, approval/current Source/rate/component checks.

Keep approvals/generation/family-event/target-event/pending-target writes :841–905 and `protected_current_decision_capture(:881)` preclock. That helper actually locks the family FOR UPDATE and upserts its genuine current-decision tuple (`0475:659–673`); it is not a postclock pure projection. Keep stage :907–908 before changing its dependent family/generation state.

Move unchanged **:912–925** immediately after successful stage/member preparation and before receive: prior generation SUPERSEDED, new generation PUBLISHED/result, family ownership/domain/current generation/hashes/lifecycle/count/bound_version, run COMPLETE/result. The source task remains transactional: failure of later original publication rolls these actual rows back. Preserve prior-before-new order for the unique one-PUBLISHED-generation index; unchanged supersession fields and source manifests remain. These writes require no command sequence.

Move original payment event :931–935 preclock too, substituting only `private.bpay_next_source_command_id_v1(v_approval.id)` for its currently returned publication UUID. That is exactly the original UUID derivation, not a new command. The payment event records no agency sequence and is outside PAY_QUERY journal. Preserve its actual idempotency key, lifecycle/payload and event kind. Prove actual publication subsequently matches that UUID before commit.

Compute actual `v_step=max(sequence)+1` before clock while the **original run FOR UPDATE** mutex is held. Resolve the exact `v_key` global step uniqueness before clock. The concrete way to remove postclock unique insertion exposure without an invented global key lock is to insert the real owner step **once** preclock with its actual new UUID, run/sequence/key, original owner/signature/request/before hashes, `outcome='STARTED'`, object response `{}` and NULL completion/after/response hashes. `plan6:2336–2354` permits this shape and steps are STATEFUL_SERVER_OWNED in the ACL contract. After publication update **this own row** to the exact current completed tuple/response instead of inserting a second step. It never commits STARTED on successful publication; exceptions roll back it. Keep whatever actual19-tuple INSERT/UPDATE events applicability requires; do not silently discard a meaningful intermediate tuple. Do not journal payment/audit rows.

At the last business tail, use original prepared one-member receive. The only remaining gate writes are:

1. INSERT actual immutable `bpay_next_protected_source_receipt` keyed by the original run, with the returned actual sequence and all unchanged hashes/IDs/bound-version.
2. UPDATE that exact own preinserted `PUBLISH_NEXT` step to current COMPLETE fields and actual `protected_receipt_json(run,false)` response.

Register precisely these INSERT/UPDATE targets (run ID for receipt, exact own step UUID for step), actual Candidate/root/scope hashes, before clock; reserve exactly the required maximum two postclock ordinals within the existing1000 bound. Do not register the generation/family/run updates after clock—they were moved. `2357` stage uses only owned producer cursor in SOURCE_PUBLICATION/TAIL_PREPARED; CLOCK_SEALED parent input_transaction remains immutable. These two rows need no new Source lifetime nesting edge: root continues directly, with the exact prepared manifest. Current initial Source enter/leave limitation QL01 is therefore not globally loosened by this simple slice.

Existing protected completion FK targets must be preheld explicitly:

* Original run FOR UPDATE, original family FOR UPDATE, original actor `tms_users(id)` KEY SHARE (plain initial actor lookup is insufficient for later receipt RI), actual Candidate/root KEY SHARE.
* New approval and generation are same-owner records inserted before clock; existing prior generation gets actual update protection preclock, preserving superseded-by FK to own new generation. Selected staged WORK/revision are fully prepared and already held.
* Protected receipt's immediate targets are run, family, actor, approval, generation, WORK, command and composite(WORK,revision), exactly as0340 declares. All existing targets are held; only command is new after receipt and must be this exact prepared command. No invented receipt column/FK or duplicate substitute witness.
* Step's existing run FK is acquired on its preclock INSERT. Its global idempotency and run/sequence uniqueness are resolved by that real insertion, not by a postclock plain absence probe. The own-row UPDATE after clock reuses its existing tuple; response helper is a plain read of the exact own receipt/approval, no business lock.
* Event/query-child/source-member RI targets Candidate/root/WORK/revision and producer/group must be preheld or same-owner new records. Existing table/owner scopes are typed; source-group metadata is not an excuse to reread live family qualification after clock.

The actual final witness must prove **all** generation/family/run/receipt/step links before source finish. For WAIT qualification, use existing `weekly_source_pay_query_next_receipt_v1(root,approval,generation)` as the exact retained authority once the real step is COMPLETE:0258 proves scope, latest WAIT event/approval/actor/schedule/source hashes, run COMPLETE/hash, generation PUBLISHED or SUPERSEDED/result/vector, exact target event, actual original revision/publication/command/member and one correct complete step/full response. Do not broaden it to treat NOT_WORKED/ACCEPTED_SOURCE as WAIT proof; successful other protected actions still have their genuine exact original publication/receipt/step completion witness, and query classification retains the existing canonical semantics.

## 4. Complete actual Source association without another command

Fresh INITIAL_TSFIN certificate uses actual authorisation/root/version and exact staged source event; DIRECT_NEXT_PROTECTED uses actual approval/source event, original run/family/generation/receipt/step identity. Each of these simple owners has **one** actual member. CURRENT_HEAD remains its genuine1/2receipt arrays. Member numbering is native WORK UUID order from the publisher, never caller array or head/booking order.

Use2352 actual group/member fields: producer/current xid/epoch/origin/member_count/owned `is_fresh_receive`; actual member Candidate/root/version/WORK/revision/source event/command/sequence/epoch. Validate exact command derivation, kind, actual one Candidate member, complete publication tuple, no duplicate/missing/extra member. Typed member rows reference actual command ID/sequence/epoch and exact publication4tuple. Do not create a query metadata header for fresh Source.

For each actual raw event associate only the root(s) its retained OLD/NEW scope matches. Preserve old_scope_match/new_scope_match. One-root simple event anchors its sole command. Real two-root event anchors the last member in certified publication order and retains both associations. Actual chronological transaction ordinal breaks same-sequence ties; UUID order does not replace event order.

Finish after original binder or completed protected step, close all own receipts/associations and named deferred checks, then mark producer FINISHED. Parent latch remains CLOCK_SEALED without UPDATE. Deferred assertion queued at producer creation only proves finished/completed ownership; it must not complete business at COMMIT. Preserve empty/no-op/immutable replay semantics. A later genuine new qualification over an old replayed publication receives the later standalone query sequence, never assigns the old original command's sequence to new mutation. Do not claim this recovered branch is built merely from the fresh simple path.

## 5. What this design does not close

1301 is not itself either simple owner's tail; initial and direct protected call1540 directly. Factor original pair preparation so1301 can later reuse it, but preserve its immediate real receipt attachment/byte ownership until a full successor explicitly handles common outer tails. Component binder1246 still discovers/current binds after pair; its lockset covers worker, WORK/revision/position union, **not all binding/version/FK targets**. Common0556 head/bundle/push immediate work, Local1257 receipt/manual/contact work and0700 pending-release→Local callback remain chunk5. Earlier clocks there cannot be declared final by the new pair split alone.

Current0002 manual-only completion/assertion and receiver must get exact source-origin/witness branches before these hooks are attached;2357 finite standalone lifetime is not complete Source support. The actual19-relation writer/applicability census, remaining Source/standalone owners, immutable acceptance proof and later epoch policy remain full-F25 work. All source designs require root review against final saved successor bytes; no verification is claimed here.

## Saved source hashes

SHA256 of read source; subsequent successors do not authorise editing these immutable predecessors.

| Path below `supabase/` | SHA256 |
|---|---|
| repeatable/09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql |7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b|
| repeatable/04102026_0345_banking_pay_next_protected_source_v1.sql |0ea79ae265cffb2370926e1728f483b8846e7539ffcac428f5d5fa557a56ac44|
| repeatable/06102026_1301_banking_pay_next_source_receipt_binding_v1.sql |24cf44354735d4a452eb3334847294e486ee17905bdc085f402e77b7ed1d75d9|
| repeatable/03102026_1540_banking_pay_next_source_ordered_publish_v1.sql |f4af5a88d1ede025fdf227df2442a2c49b801104bf02a99a466366663378a4da|
| repeatable/03102026_1430_banking_pay_next_position_writer_v1.sql |395ead5cf46a1399ef4859f056d89508b0862f9b0fda8dde92454107f17602fa|
| repeatable/03102026_1550_banking_pay_next_source_stage_v1.sql |5388ce61af1cbd27acfdd4eeb69cc9654247d818e18745200bdd9557e83584b2|
| repeatable/07102026_0018_banking_pay_next_initial_tsfin_daily_evidence_v1.sql |fa256bfa14b93321e1749c7cbee2505baca4f4da2eef5d11ea6457bdfae120e2|
| repeatable/06102026_1246_banking_pay_next_source_book_binding_v1.sql |2e9a297ff362e41dbfbadc869b211b37176cf76d06edd3e78808a2e4c2a57340|
| repeatable/26092026_0202_banking_pay_stage2_source_authorisation_v1.sql |7a9e74ccfa75be919b21aa5126864b239197b7f466816264140f1e19f02bd93d|
| migrations/04102026_0340_banking_pay_next_protected_source_receipt.sql |3b2f62d68c8138f871dd5736dfd3176d7b762b90154eb3c44b771e0dc3b9deab|
| migrations/09102026_2010_banking_pay_next_source_book_binding.sql |d2641717de3967aa30e200d75f9e4fde801b31c7eeef9cc8db84efb36d9266ea|
| migrations/09102026_2011_banking_pay_next_initial_tsfin_umbrella_work_shape_v1.sql |8dd6232ccda068022e9ab8faef163d89eeaa883720a0e95e441db56177a7aeef|
| migrations/15092026_1534_weekly_source_plan6_schema.sql |1fc7833c7054cb56da725d15280ff757f3b78e1046b80f12c337020695374c0d|

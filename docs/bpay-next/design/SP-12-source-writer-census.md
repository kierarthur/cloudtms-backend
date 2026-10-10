# SP-12 current Source writer admission census

10 October 2026. Independent read-only source investigation of BUILD-BE at HEAD `3241f945773bccf64f13f6258ad126c52f17c8ab`, with saved disk authoritative. Card `05_SOURCE_AND_PROTECTED_PAY.md:420–453` and all ten SP-12 checklist deliverables read. No SQL, tests, runtime, fixtures, business data, hosted calls, secrets or product edits. This note is design evidence, not proof of installation, executed contention or complete writer closure.

## Binding scope and result

The genuine admission helper and pin TRUNCATE protection already exist. Most current Source owners and the actual Banking PIN boundary admit before their first business lock. Three Source Candidate entry points and genuine Source-local C1 staging do not. Other C1 evidence and pending-state writers are explicitly retained in the matrix below; they cannot be silently counted as admitted simply because their earlier preparation RPC was admitted. The appendix resolves the current pending watch/claim/failure/reopen rows as either gate-equivalent operational transitions or genuinely inherited material publication; it leaves no proposal to add their first key in a deep helper.

The root's latest scope clarification distinguishes SP-12's explicitly named, small existing-key integration from the stopped F-25 producer protocol. A necessary existing `weekly_source_pay_query_admit_v2()` call, gated by actual NEXT ownership at the named fresh boundary, is in scope under that clarification. **No query versions/certificates/producer records, new lifecycle/protocol/latch, resolver, financial ordering redesign, Candidate comparison rewrite or F-25 parked code is proposed.** Historical F-25 notes supply search leads only; their proposed producer implementation is superseded by Rule B and is not current authority.

The census is deliberately not an all-rows-YES certificate. Missing and applicability-unresolved rows below must be resolved by the actual owner before full closure. WB-02, WB-05 and SP-15 refresh additions remain future explicit owners.

## Reused authority and security

`supabase/repeatable/05102026_0044_bpay_next_source_pay_query_admission_v2.sql:10–22` defines `private.weekly_source_pay_query_admit_v2() returns void`, VOLATILE SECURITY DEFINER, `search_path=pg_catalog`. Its exact transaction try-lock is `hashtextextended('CLOUDTMS:BPAY_NEXT:SOURCE_PAY_QUERY_ADMISSION:V2',0)`. Miss raises exactly `55P03 / WEEKLY_SOURCE_PAY_QUERY_ADMISSION_BUSY`; success is reentrant until transaction end. Owner is `postgres`; PUBLIC/anon/authenticated/service_role execution is revoked at 110–115. A Source/Banking SECURITY DEFINER owner calls the private helper; browser readers do not.

The same file's actual row guard rejects every non-INSERT at 33–35. Its final attachments are BEFORE INSERT/UPDATE/DELETE row guard (101–104) and BEFORE TRUNCATE statement guard (105–108) on `private.bpay_next_run_work_query_pin_v2`. Therefore TRUNCATE is source-protected through the immutable refusal. Do not add a duplicate migration for installed `05102026_0044_bpay_next_run_work_query_pin_v2.sql`. No separately created SP-12 “admission evidence” table was identified; dormant F-25 scaffolding is not evidence of an active admission protocol.

Protected-boundary file verification was a static five-file comparison only: every declared SHA in `supabase/release/protected-boundary-lock.json` matched its current saved file. This is not a database STOP-CHECK execution or authority to edit a frozen object. The three Candidate owners below are in 2203, rather than one of those five locked files; the root must preserve their actual named signatures, ACL and final source gate.

## File aliases used in the matrices

All files below are under `supabase/repeatable/` unless stated otherwise.

| Alias | Actual current physical file |
| --- | --- |
| Q | `15092026_1534_weekly_source_query_delivery_v1.sql` |
| C | `15092026_2203_weekly_source_candidate_app_contract_v1.sql` |
| M | `05102026_0428_weekly_source_manual_review_commands_v2.sql` |
| P | `15092026_1534_weekly_source_protected_pay_publisher_v1.sql` |
| A | `15092026_1534_weekly_source_protected_action_orchestration_v1.sql` |
| X | `15092026_1534_weekly_source_protected_pay_c1_publication_v1.sql` |
| L | `04102026_1257_weekly_source_local_protected_decision_v1.sql` |
| H | `04102026_0556_banking_pay_next_source_factual_legacy_boundary_v1.sql` |
| I | `17092026_0300_weekly_source_entitlement_publication_v1.sql` |
| R | `17092026_0700_weekly_source_pending_entitlement_release_v1.sql` |
| N | `04102026_0345_banking_pay_next_protected_source_v1.sql` |
| F | `15092026_1534_weekly_source_finalisation_v1.sql` |
| CF | `15092026_1534_weekly_source_correct_final_source_v1.sql` |
| U | `15092026_1534_weekly_source_upload_publication_v1.sql` |
| RB | `10102026_0151_banking_pay_next_rule_b_selection_consumers_v1.sql` |

Registry finalPhysical wins over old same-signature definitions. In particular: manual 1601 is superseded by M; old projection rows is superseded by `07102026_2215`; 0557's public first-authorise wrapper is superseded by 2018; first-authorise core is now 0806; WAIT is now 0821; PIN is now RB. References below are to the winning body or explicitly labelled nested helper.

## Actual admitted entry matrix

| Current signature / entry | Gate write or affected scope | Admission → first business lock | Source status |
| --- | --- | --- | --- |
| `public.weekly_source_manual_review_open_v1(jsonb)` M | review INSERT M:218 | M:177 → operation advisory178; query-family advisory200 then OPEN row204 | YES, after immutable receipt replay |
| `public.weekly_source_manual_review_resolve_v1(jsonb)` M | review state M:499 | M:371 → operation advisory372; current review396 | YES, after immutable receipt replay |
| `private.weekly_source_manual_reviews_finalised_v1(uuid,uuid)` M | Final-qualified review resolution M:310 | M:267 → contained review rows287 | YES nested; Final/CorrectFinal outer key precedes earlier locks |
| `public.weekly_source_query_accept_system_hours_atomic_v1(jsonb)` Q | incident state Q:698 | Q:684 → incident687 | YES |
| `public.weekly_source_manager_review_respond_atomic_v1(jsonb)` Q | resolving branch incident state Q:1210 | Q:1082 → original advisory1088 | YES; nonresolving operational branches are not fake gate transitions |
| `public.weekly_source_candidate_response_submit_atomic_v1(jsonb)` Q | resolving branch Q:2709 | Q:2573 → operation advisory2576 | YES direct; later nested entry does not cure earlier Candidate outer locks |
| `public.weekly_source_query_sync_atomic_v1(jsonb)` Q | incident/comparison INSERTs and pointer/state Q:4895/4968/4987/5066 | Q:4771 → cycle4772 | YES |
| `public.weekly_exceptional_pay_prepare_family_v1(jsonb)` P | actual family/run P:615/645 | P:220 → original prepare lock223 | YES |
| `public.weekly_exceptional_pay_prepare_action_v1(jsonb)` A | actual run A:241 | A:83 → family87 | YES |
| `public.weekly_exceptional_pay_wait_atomic_v1(jsonb)` final 0821 | WAIT family/target events146/180 and actual family/run269/280 | 0821:83 → family87; complete no-write replay retains its original bypass | YES, actual Office permission at68; WAIT has no financial calculator |
| `public.weekly_exceptional_pay_publish_next_v1(jsonb)` N | actual approval/generation/events/NEXT receipt N:841–937 | N:545 → current root helper559 (not merely later family advisory585) | YES, original immutable receipt replay before admission |
| `public.weekly_exceptional_pay_complete_local_v1(jsonb)` L | actual Local receipt/family/generation/request completion L:255–441 | L:125 → original family144 | YES, exact immutable Local replay precedes fresh key |
| `private.weekly_source_entitlement_publish_core_v1(jsonb,text,jsonb,uuid,text,uuid,jsonb,jsonb)` H | original HEAD/components/receipt/root authority H:1611–2070 | H:329 → original shared owner lock path340 | YES current core; old I core is not the final definer |
| `private.weekly_source_entitlement_publish_immediate_v1` I | delegates genuine HEAD publication I:3115 | I:3000 → original locking census/family resolution before row3069 | YES, no claim that3069 is the first transitive lock |
| `public.weekly_source_first_authorise_v1(uuid,uuid,text,uuid)` final2018 | delegates initial authorisation core and original Source completion | 2018:3569 → `bpay_source_authorise_lock_v1`3572 | YES; final0806 core query-family advisory108 inherits this key |
| `public.weekly_source_first_authorisation_withdraw_v1(uuid,uuid,text,uuid)` H | initial authority withdrawn H:2512 / old HEAD H:2761 | H:2208 → original lock/census path before row2265 | YES; nested protected withdrawal must inherit this outer |
| `private.weekly_source_pending_entitlement_release_apply_v1(uuid,bigint,bytea,text,uuid,uuid)` R | real pending release/common HEAD R:1254–1546 | R:1211 → exact pending bundle1232 | YES after plain lease discovery; public JSON wrapper R:2722 delegates without a prior business lock |
| `public.weekly_source_projection_rows_apply_atomic_v1(uuid,uuid,jsonb)` final0710_2215 | current projection/work-event facts | 2215:116 → publication advisory117, original publication/cycle rows130–140 | YES; historical1534:174 is superseded |
| `public.weekly_source_projection_publish_atomic_v1(jsonb)` U | publication/currentness; inherited prefinal comparison U:3101 | U:2823 → original publication advisory2824 | YES; comparison is nested in this same transaction |
| `private.weekly_source_finalise_engine_v1` F; public Final delegates | Final/currentness plus nested manual Final closure | F:1355 → cycle1358 | YES; named Source factual Final remains genuine |
| `public.weekly_source_correct_final_prepare_atomic_v1(jsonb)` CF | prepared correction authority | CF:1347 → cycle1350 | YES |
| `public.weekly_source_correct_final_apply_atomic_v1(jsonb)` CF | applied Final/currentness and nested manual closure | CF:1656 → cycle1659 | YES after original APPLIED no-write replay |
| `public.weekly_source_timesheet_lineage_ensure_atomic_v1(uuid,uuid)` | actual Timesheet/root lineage | `1509_1534_timesheet_lineage`:253 → cycle260 | YES, discovery reads before key are re-read after owner locks |
| `public.bpay_next_financial_job_v1(text,uuid,text,jsonb)` actual0280 | fresh PREPARE STEP PINNING dispatch | 0280:165 → module SHARE172, before child owner locks | YES only exact fresh leased NEW/PINNING cursor; terminal/advanced/stale paths preserve original non-writing handling |
| `private.bpay_next_pin_selected_work_v1(uuid,uuid,bigint,bigint)` RB | query-pin INSERT plus named expected revision | RB:226 → module SHARE231 → run236/control240/job242 | YES final Rule-B body; do not restore dormant accepted-cut producer logic |

## Actual missing entry and inherited-path limits

| Exact current signature / source | First lock / material effect | Actual key / required bounded disposition |
| --- | --- | --- |
| `public.weekly_source_candidate_app_submit_atomic_v1(uuid,text,uuid,jsonb,timestamptz)` C | locking `_candidate_session_context_v1(...,true)` C:2786; operation advisory2841 / generation2852; materialise2927 and response later | NONE. Existing service check2785 precedes session helper. Under the root's BD045 clarification, gate only the existing admission at this named early NEXT seam, preserving original auth/body/replay/business semantics. Admission in the later response cannot prove outer ordering. |
| `public.weekly_source_candidate_self_submit_atomic_v1(uuid,text,jsonb,timestamptz)` C | locking session context3020; nested materialise3047 | NONE. Existing service check3019 precedes session helper. Same bounded early NEXT call; no new producer metadata. |
| `public.weekly_source_candidate_check_materialise_atomic_v1(jsonb,timestamptz)` C | operation advisory2291 → workflow2296; compare2721 writes incident/comparison | NONE. Pure typed request, request-kind, hours-only and hash validation2267–2290 precede first lock. Existing-key integration belongs before2291. Keep complete existing workflow/candidate authority and replay. |
| `private.weekly_source_candidate_submission_compare_sync_v1(uuid,uuid,uuid,uuid,bytea,timestamptz)` C | cycle lock1294; material incident/comparison1755–1867 | NONE direct. Inherits correctly from U:2823→prefinal U:3101→C:2018, but not from the three Candidate paths above. Do not fix first acquisition in this deep helper. |
| `private.weekly_source_candidate_prefinal_publish_recheck_v1(uuid,uuid)` C | delegates comparison C:2018 | Source publication parent inheritance YES; no stand-alone admission certificate is claimed. |
| `public.weekly_exceptional_pay_stage_c1_request_v1(jsonb)` X | existing staged-request FOR UPDATE1160, family1179; approval/generation/family/target/request/step writes1647–1878 | NONE. **Still genuinely used by Source-local Save**, orchestrator724, before Local complete. Earlier prepare and later complete are separate RPC transactions. This gap is not retired provider execution and cannot be omitted. Add only the existing-key early NEXT integration at this named fresh/staged entry; preserve original request/replay. |

## Other concrete write rows requiring honest classification

These actual final bodies contain no admission. This table reports the concrete facts; it does not authorise a broad owner rewrite or certify unreachable branches. C1 historic reconciliation is distinct from new external C1 work retired by SP-02/OR-45.

| Current function / physical source | First explicit lock; concrete write | Admission / disposition |
| --- | --- | --- |
| `public.weekly_exceptional_pay_record_c1_unknown_v1(jsonb)` X | immutable outcome SHARE158; request/family/run233–238 | NONE; actual pending recovery metadata. Final appendix distinguishes its terminal-state refusal from genuine qualification. |
| `public.weekly_exceptional_pay_record_c1_checkpoint_v1(jsonb)` X | immutable checkpoint SHARE363; checkpoint467 and request/generation/family/run478–513 | NONE; exact terminal-state guard and original receipt predicate require classification, not table-name inference. |
| `public.weekly_exceptional_pay_record_c1_recovery_v1(jsonb)` X | request592; request/generation/family/run638–691 | NONE; same retained historical reconciliation classification required. |
| `public.weekly_exceptional_pay_complete_c1_publication_v1(jsonb)` X | request1962; completed generation/request/family/run/step2077–2136 | NONE; actual historic C1 qualification writer, unlike Source-local replacement. No new provider operation is proposed. |
| `private.weekly_source_local_protected_pending_released_v1()` L | receipt493; qualifying generation/family/Local receipt/run/request522–564 | No own key, appropriately nested AFTER state UPDATE; actual release-apply parent R:1211 inherits. First acquisition must never be added to this row trigger. |
| `private.weekly_source_pending_entitlement_bundle_save_v1` R | pending543; pending INSERT/UPDATE649/660 | No own key. Genuine common-publication callers must inherit from I/H; raw private helper is not an independent public API. |
| `private.weekly_source_pending_release_watch_page_v1` R | pending847; state944/959 | No own key. Applicability to accepted Local pending qualification requires explicit current predicate classification. |
| `private.weekly_source_pending_entitlement_release_claim_page_v1` R | pending1043; state1046 | No own key. Lease-only fields are operational; actual state transition must be checked against the existing Local pending receipt predicate before calling this gate-irrelevant. |
| `private.weekly_source_pending_release_technical_failure_v1` R | pending1810; state1830/1857 | No own key; direct failure wrapper can be a separate transaction. Distinguish technical counters from genuine pending qualification state. |
| `private.weekly_source_pending_release_manual_review_v1` R | state1904 | No own key; calls from admitted release-apply inherit; other caller roots require proof. |
| `private.weekly_source_pending_release_superseded_v1` R | state1952 | No own key; same inherited-root requirement. |
| `public.weekly_source_pending_entitlement_bundle_reopen_v1(uuid,text,uuid)` R | pending2517; state/revision2606 | No own key. Public actual state writer; do not label closed on the admitted release-apply's behalf. |
| `private.weekly_source_first_authorisation_withdraw_protected_v1` `1709_0600` | approval2661 and family event2690; parent holds scope | Nested genuine withdrawal under H:2208; retain inherited admission, not a new deep entry. |

## Nonmaterial, dynamic and recursive distinctions

For the manual/incident/comparison tuple, the current Source reader consumes actual review state/scope, OPEN incident scope/current comparison, immutable first/latest comparison, exact latest WAIT/approval/publication qualification and original receipt identity. It does not consume every outreach field. Existing incident writes Q:521/542 (submission-complete), Q:2448 (scheduler), Q:3060/3108 (send manager), Q:3793 (manager generation), Q:4322 (Candidate outreach generation), and delivery aggregate `1509_2311`:1309 change operational manager/candidate flags. Their absence of this key is not, by itself, a material-gate defect. The resolving Q:1210/2709 branches remain separately enumerated above. Do not treat every table-name hit as a relevant state transition.

Source scan covered explicit INSERT/UPDATE/DELETE/MERGE for the nineteen actual qualification relation names across saved managed repeatable `.sql/.inc/.sqlinc` bodies, and used current finalPhysical to discard registered predecessors. Relevant current owner bodies in the matrices use static statements; `EXECUTE FUNCTION` attachment and ACL `GRANT EXECUTE` text are not dynamic business SQL. This is a static search aid, not a general SQL parser/proof framework or certification against arbitrary string-built caller SQL. Dynamic/transitive scope/identity/rotation helpers require actual outer call-path proof; the current root's `1709_0200` locking census takes booking/family locks at235/244 and is not itself an early-entry owner. I/H's preheld entry is what protects their nested use. No SQL invocation occurred.

Generic I1, receipt publishing 1301/1540, 1550 deep financial hooks, row triggers, nested comparison and deferred assertions do not acquire the key first on behalf of an uncovered outer owner. The real deferred pending release is a separately leased request: R:1211 is the genuine fresh boundary. Earlier HTTP preparation never implies transaction inheritance. Module ownership remains the existing actual control decision; the global key does not replace authority or permit money owner changes.

## Pending owner rows and acceptance

WB-02 withdrawal refresh, WB-05 publication refresh, SP-15 Query OPEN/RESOLVE refresh, WB-07 STABLE reader and SP-36 MyTMS retry consumer remain their explicit later owners. Append each real installed final root / key line / first-lock line when authored. Do not invent today's rows or claim future closure. SP-12 D05–D08 transport/UI and D06 frozen Candidate runtime STOP-CHECK belong to the root/other reviews; SP-06's protected-only busy adapter is already finite, but it does not implement the shared mapper or Candidate `RETRY_SAME_REQUEST` consumer.

All real OPEN/PIN order, contention, original request retry, cursor1/17/100, replay/unknown recovery, eventual progress/starvation and timing checks remain NOT_RUN. No latency SLA, fixture harness, activation or full writer certificate is claimed.

## Saved source hashes

| Source | SHA-256 |
| --- | --- |
| 0044 helper/pin guard | `01405bc854fb7b2e23cd740154f0302a8956cdd144b44c1f3e4eda67db80dcdd` |
| C2203 Candidate contract | `2f28f6b60ef929617e2a2e5075e0b63875e21d0eea096d9ab63bcc501e3840d6` |
| X1534 C1 publication | `c74ea692273600ce753ee92c09efca5e812d8a61c4a536058c2a405c9be3f790` |
| Q1534 Query delivery | `dcfa67862e75cff20e06d0694910ea603d329f3f0dc20fd351fca07cf598332d` |
| R1709_0700 pending release | `177400e1d5f1be23e789ef4cd3596cdac1f9860fe94602a5a031bdd1142488c6` |
| H0556 current common publisher | `64ef7e6d04e1b652af722cb9dd050310fcd8e1cd249f59b9359428a310b70933` |
| RB0151 actual selection consumers | `2698331479177a246d2ee581f71aa1514a1479f05bf6258c1ceb1f4f490104cf` |
| protected-boundary lock | `60d2d3d8a74535cf52c5213803d19b0654af48955b6096726e2b7a882545f30c` |

The matrix's remaining C1 applicability limits are explicit, not invented forbidden F-25 evidence work. The root must refresh saved hashes/final roots after its own successor changes. This note is not the final SP-12 completion census while a concrete missing key or unresolved relevant writer remains.

## Pending writer classification appendix — completed read-only

The actual Local query receipt reader `05102026_0114_weekly_source_pay_query_evidence_v1.sql:186–213` accepts a pending row only through the exact retained bundle ID, decision/bundle revision, actor/Candidate, canonical request digest and complete member-root/booking/version/head tuple. At189 the accepted pending state set is **PENDING, RELEASING, MANUAL_REVIEW**. It does not expose `pending_revision`, technical failure count, lease, watch or due-time fields in the returned receipt. Consequently no generic pending-state journal or key is needed solely for changes within this exact equivalent set while all retained qualification operands remain pinned.

| Previously inventoried row | Actual source disposition |
| --- | --- |
| R watch944/959 | Only watch/census/due-time/observation metadata changes; the source explicitly leaves `state` and `pending_revision` untouched at955–958. Gate-irrelevant, no missing admission claim. |
| R claim1046 | PENDING/expired RELEASING → RELEASING plus lease/revision/due-time metadata. Existing query qualification treats both states alike; original bundle/request/member tuple is preserved. Operational, not a new accepted input owner. |
| R technical failure1830/1857 | RELEASING → MANUAL_REVIEW or PENDING, both admitted by the same existing reader; technical counters/reasons/revision/lease cleared. Gate-equivalent while retained identity/digest stays fixed. The direct failure wrapper need not invent a new query producer. |
| R manual-review1904 | Same admitted pending-state set; deeper calls from release-apply inherit its key anyway. No first key in this deep helper. |
| R superseded1952 | SUPERSEDED leaves the accepted pending set and therefore genuinely changes qualification. The only current function call sites found are release-apply R:1349 and1464, after its early key1211 and locked bundle1232. This material branch has actual same-transaction inherited admission. |
| R public reopen2606 | Original locked MANUAL_REVIEW → PENDING, retained decision/bundle/Candidate/request/member tuple unchanged. The superseding **audit reopen generation is `pending_revision`**, not a changed qualification `bundle_revision` or HEAD. Audit identity is preserved; this is not a new ordered Source publication or F-25 input version. Gate-equivalent. |
| R bundle-save649/660 | The actual caller is I immediate publication3032, after I admission3000. Canonical request/member identity is preserved by the existing saver. Genuine new pending/member evidence inherits the outer key. |
| L pending-released callback522–564 | Actual release changes Local receipt PENDING_FREEZE→COMPLETE and original generation/request/family. The admitted release-apply parent owns this transaction; no key may be acquired first by the AFTER state trigger. |

This closes the pending table-name false positives by actual reader semantics rather than a blanket historical exception. Any later owner changing canonical request/decision/bundle/member identity, moving into/out of the reader's admitted state set, or adding a new external caller must append a new real row before it is counted as closed.

The current Source identity/rotation lock helpers (`17092026_0200`, locked family235/244; 0900 pinned-owner guard) are nested locking authority, not independent query command/publication owners. Preserve the actual admitted I/H/first-authorisation/withdrawal call paths and their original guards. A raw generic Timesheet/rotation caller cannot claim inheritance solely from a GUC or a previous HTTP request; no new generic caller is certified by this source-only census.

## C1 qualification versus retired external work

The actual `05102026_0245_weekly_source_pay_query_c1_receipt_v1.sql` qualification reader requires PUBLISHED/SUPERSEDED generation53, request `state='PUBLISHED'` with completed timestamp76, COMPLETE run111, exact immutable latest PUBLISH checkpoint114–127 and matching completion step145. A progress checkpoint, UNKNOWN or RECOVERY_REQUIRED request alone is not an accepted receipt. The current unknown writer refuses any PUBLISHED/REFUSED/FAILED/RETIRED request at X:194; the checkpoint writer refuses PUBLISHED and the other terminal/unknown states at X:400. These prepublication operational transitions do not withdraw an already qualifying receipt. Their raw table-name hits are not evidence that every progress action needs a new gate owner.

In contrast, `public.weekly_exceptional_pay_complete_c1_publication_v1(jsonb)` moves genuine historical C1 evidence into the reader's accepted publication/COMPLETE tuple. It has no admission before its first request FOR UPDATE1962. If this historical completion remains an admitted NEXT agency reconciliation entry, it is an additional exact existing-key boundary gap. New external C1 work remains retired; Source-local Save does **not** call this function. The current Worker reference is the retained `banking-pay/weekly-source-c1-durable-publication.mjs` historical executor, whereas the actual current protected orchestrator uses stage724→Local complete286. Do not restore or invoke the retired executor to satisfy a census. Record the actual current admission/retirement authority for historical completion, or gate only that named genuine completion entry; do not silently mark it YES.

The same distinction applies to recovery metadata: preserve its original state/identity guards and classify against the full receipt predicate. A retrospective “all C1 rows are obsolete” assertion would incorrectly hide genuine Source-local stage writes; a blanket first-key call inside all C1 deep helpers would be an unnecessary broader change. The four directly demonstrated gaps are the three named Candidate entries and the genuine Source-local staged-request boundary. Historical C1 completion is the additional explicit owner-dependent row, not a guessed current outbound job.

## Final finite historical-C1 disposition — SP-02 path re-read

**Current active NEXT Worker matrix: exclude external C1 complete/recovery as retired-runtime historical owners; preserve their SQL and native obligations. The definite current admission additions remain the three Candidate entries and Source-local stage.** This is a caller/authority disposition, not a claim that the retained SQL routines themselves reject NEXT or already take the key.

1. `broker/src/weekly-source/protected-action-orchestrator.mjs:560–572` routes an existing staged request to `completeLocalDecision` (Source-local owner) and returns `C1_DURABLE_RECOVERY_NOT_REQUIRED` for an external recovery request. Its actual later stage call724 is followed by Local completion286. It does not call the external durable executor or `complete_c1_publication_v1`.
2. The complete, unknown, checkpoint and recovery names are retained in `banking-pay/weekly-source-c1-durable-publication.mjs`. A current `broker/src` source caller/import census found no caller of this module's exported `publishDurableWeeklySourceC1`, `stageAndPublishDurableWeeklySourceC1` or `recoverDurableWeeklySourceC1` outside the same retained library. Its completion call292 occurs only after its external publisher result; its recovery path344 calls the external adapter before recording recovery345. Those are retired external workflow paths, not new Source-local ownership.
3. `banking-pay/weekly-source-c1-raw-rpc.mjs` independently requires the actual DB admission before any external C1 HTTP attempt: the bounded recursive call to `bpay_next_source_legacy_finance_admit_v1` happens at its preflight block, before the requested external fetch. `10102026_0715_banking_pay_next_source_retired_finance_entry_v1.sql:17–24` requires the service role, uses the genuine shared module callback lock, and raises **PT410/BPAY_NEXT_LEGACY_WRITER_RETIRED under NEXT**, PT503 under DISABLED. This is the actual SP-02 retirement authority. An earlier completed request cannot authorise a fresh external operation. The preflight explicitly is not a cross-transaction lease, and no external attempt/hosted verification was made here.
4. `public.weekly_exceptional_pay_complete_c1_publication_v1(jsonb)` itself remains service-callable SQL. Its old PUBLISHED branch1986–2009 is exact no-write replay; fresh terminal reconciliation2010–2056 demands the genuine original request/generation/run/actor and terminal PUBLISH checkpoint. SP-02 deliberately preserves real already-admitted terminal evidence. **There is no direct NEXT/PT410 condition in this SQL routine, and no admission call; neither is invented by this census.** Current absence of a Worker entry plus the retired external transport justifies excluding it from the current active NEXT app writer additions, not labelling it `takes key: YES`.
5. Any F-18/F-19 genuine in-flight historical reconciliation or a later owner which intentionally reintroduces this metadata completion must explicitly append its actual current admitted caller/first-lock row and satisfy the existing-key condition before calling it. Existing terminal obligations are not declared nonexistent, closed or obsolete by a source scan. This remains an honest readiness/reconciliation dependency, with no actual counts or in-flight state observation.

**Recovery cannot withdraw an already PUBLISHED qualifying tuple.** In `X:606–615`, a previously recorded recovery must match its same idempotency key and exact result; it returns without writes. It can therefore replay after later publication without changing it. Every fresh recovery reaches `X:618–626`, which requires both `unknown.state='RECOVERY_REQUIRED'` and `publication.state='RECOVERY_REQUIRED'` plus the genuine actor. A PUBLISHED request refuses before the REFUSED updates638–653 or the SUBMITTED update691–693. Nested checkpoint695 applies its own PUBLISHED refusal400. Thus fresh recovery is a prepublication operational reconciliation; it is not a missing writer capable of invalidating an accepted PUBLISHED C1 receipt. Retain these original guards and do not add an unnecessary F-25 evidence protocol or broad recovery rewrite.

| Fresh supporting source | SHA-256 |
| --- | --- |
| SP-02 `0715` retirement entry | `358abee947f0b03aeba9cbbbcadc17f6a7fe54bc56260951592b8a2279470044` |
| current C1 raw transport | `fb6c9a55414821c6a17effeb1a5f2e0dfe2fa10c8743f72dfb050ed62e51eb8f` |
| retained external durable library | `cb39107459dd0e002234db92316ecb8a91a0b80f6874f1e2ee7275ab233d5e18` |

These are static source identities only. No retired executor, complete/recovery RPC, Source mutation, network operation or tests were invoked.

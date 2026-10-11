# SP-12 writer census and implementation design

Sequence 41, 10 October 2026. Root design before product edits; source-only investigation against BUILD-BE 3241f945773bccf64f13f6258ad126c52f17c8ab and BUILD-FE d124c705e178e084f78fb94aee4c91631ca2fdd4. Exact scope/dependencies are in SP-12-SCOPE.md; the independent boundary review and two source/caller investigations below are part of this design.

BD045: the card explicitly requires the existing admission try-lock at missing true outer entry points. These calls introduce no F-25 producer/version protocol. Preserve Rule B and all parked files. Patch only the three named Candidate bodies and definite Source-local stage; classify retained historical completion before final closure. The first key never belongs in a trigger, deep comparison, generic receipt or money helper. Existing correctly admitted definitions remain byte-for-byte untouched.

Candidate app/self submit: original service check, original session helper's nonlocking authentication read (p_lock=false), positively NEXT admission, then the unchanged original p_lock=true context and all remaining statements. This meets plain auth before key and original locked revalidation without changing the shared helper. Direct materialise: after original pure typed/hash validations and before its existing advisory. Source-local staged request: after pure exact request/stream validations, before original request FOR UPDATE (including replay); business/auth/replay statements remain unchanged. All five protection hashes match; no protected file/signature/security contract changes.

D03 actual local PG17 read-only catalogue confirms enabled BEFORE TRUNCATE FOR EACH STATEMENT guards on pin_v2 and Rule-B snapshot, private owner-only ACL/RLS, and owner-only VOLATILE SECURITY DEFINER admission/guard helpers. Reuse these guards and schema; no duplicate migration or TRUNCATE operation.

Worker design: extend only shared bpay-busy's exact structured SQLSTATE/message map; retryable=true only for this new Source code. Candidate errorResponse adds RETRY_SAME_REQUEST only to that exact classification. The real public broker otherwise drops these required flags, so add one finite safe passthrough for the fully proven closed 409 shape; preserve all unrelated error/transport/diagnostic behavior. Protected-route mapping already supplies this shape; retain original protected contracts.

Office design: preserve exact original action/payload/nonce on closed Source admission busy, offer deliberate Try again with fixed copy, and never auto-resubmit. Reuse existing pending requests; fill only genuine actual caller retention gaps identified below. Earlier committed steps are not claimed rolled back. Unknown-result behavior stays separate. finalise-batch marks closed busy as existing READY (with local presentation busy flag), not CHECK_REQUIRED; successful members are never resubmitted. Small existing UI helpers/combined batch owners are necessary support, not new command kinds or persisted lifecycle states. MyTMS source consumer is SP-36 later.

BD046: Candidate CI's dated subset does not contain Source2203,0044 or NEXT schema. Preserve its fixture/runtime programme. Add only invocation of existing source gates to that named workflow (full history required by existing gate); actual managed/local order is all migrations, original2203,0044 helper, newly dated body-only Candidate successor. Record and verify this real order; do not pretend the old subset installs the Source chain or enlarge its fixtures.

Validation: local PG17 changed-SQL load, db:check, node --check, db:contract:seal-local/coupling and existing source gates; rederive unchanged public/Candidate browser-membership counts/seals. No fixtures, contention experiment, rehearsal or new tool. Install file+actual implementation commit appended after local commit. WB-02/WB-05/SP-15 append their actual refresh rows later; source census then rechecked. Real contention/replay/pages1/17/100/progress and app checks remain authored NOT_RUN; full end-to-end SP-12 closure requires SP-36.

## Read-only writer investigation

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


## Read-only caller investigation

# SP-12 caller and error-transport census

Read-only design, 10 October 2026. Full card `05_SOURCE_AND_PROTECTED_PAY.md:420–453` and checklist D05–D08/D10 read. This note covers only the named error transport and genuine Office callers. The Source/Banking writer census, entry placement and TRUNCATE qualification are separate D01–D04 work. Binding Rule B remains in force: no Candidate producer/materialise/compare rewrites, parked F25 protocol, new command or automatic retry. SP-36 owns the fresh complete MyTMS app consumer; application/installed/whole Worker acceptance is NOT_RUN.

## Exact server busy contract

`broker/src/bpay-busy.js` currently recognizes only structured PostgREST `error.json.code === '55P03'` plus one exact message (`BPAY_SOURCE_FAMILY_BUSY` or `BPAY_APPROVAL_EVIDENCE_BUSY`), with an actual integer HTTP 4xx/5xx status. Extend that same finite map with **only** `WEEKLY_SOURCE_PAY_QUERY_ADMISSION_BUSY` and copy **“Another Source update is in progress. Try again in a moment.”** Retain existing two-code behavior. Do not classify arbitrary 55000, raw string regexes, HTTP 409 alone, timeout or unknown transport as known rollback.

The Source busy response must retain clean 409, `ok:false`, this exact `error_code`, `failed_rpc_outcome:'ROLLED_BACK'`, `automatic_retry:false`, and `retryable:true`. Earlier requests in a workflow may have committed; `workflow_outcome:'NOT_ESTABLISHED'` remains truthful. A shared-map addition must not claim an entire workflow rollback. The existing protected-only adapter in `weekly-source/routes.js:979` already recognizes this exact SQLSTATE/message pair; the ordinary `routeErrorResponse` still relies on the shared map and otherwise uses generic conflict/raw transport text. Reuse one finite shared classification rather than widen the generic fallback.

## Candidate error boundary: actual missing public transport

1. `broker/src/candidate-app-backend.js::errorResponse` (`:836`) already returns the shared busy envelope as a clean 409 plus `retryable:true` and `request_id`; it currently omits `retry_class`. Add **`retry_class:'RETRY_SAME_REQUEST'` only when the new exact Source admission busy was proven by that recognizer**. Existing Banking busy codes, arbitrary SQL exceptions, unknown transport and Candidate stale-context errors do not inherit this class.
2. The actual Source request handlers are `handleCandidateWeeklySourceRequest` (`:6324`, APP draft/submit) and `handleCandidateWeeklySourceSelfSubmit` (`:6348`). These retain body/idempotency input and use the existing RPC boundary; no owner or request-shape change is needed for this transport work. Do not edit their business semantics or their DB producers.
3. **Supporting public-broker correction is genuinely required.** `candidate-broker/src/candidate-broker.js::publicSafePrivateResponse` (`:1151–1208`) rebuilds the generic private error with only `ok,error_code,request_id` and drops retryable/retry_class/rollback/copy/automatic_retry. The Source request/self-submit paths use this generic adapter, not the Daily-specific error schemas. Adding retry_class only to the private backend will therefore not reach MyTMS.
4. Add a narrow branch to that existing generic public error adapter for the exact expected private 409 Source busy envelope, including original `ok:false`, exact code, `retryable:true`, `retry_class:'RETRY_SAME_REQUEST'`, `failed_rpc_outcome:'ROLLED_BACK'`, and `automatic_retry:false`. Rebuild only its finite safe public fields/fixed copy; never forward an arbitrary private body, raw DB details or all retry classes. Malformed/unproven envelopes retain the existing fail-closed generic behavior. This is transport support for D06, not a new public route/command, protocol or producer.
5. Real private `candidate-private-api/wrangler.jsonc` points at `broker/src/candidate-private-worker.js`, which imports the current Candidate backend directly. The normal backend shares that implementation. `candidate-synthetic-private-api/src/index.js` currently implements only the APP_READY private proof route; it has no Source submission/errorResponse business path. Preserve its current proof/error contract; do not add a dormant duplicated Source handler. Future release must prove compatible real/synthetic private versions before the public broker (B2.17). No runtime/config/deployment evidence was obtained in this review.

## Office: safe copy and actual retained inputs

Office Checks has no independent JS controller. `import-workspace.js:690` renders its rows/actions; the shared `.ws-row-action` dispatch (`:1529`) forwards their server payload/context to `workspace-actions::handleAction`. Its actual manual/protected/recheck owners below must provide the retry behavior; a standalone unused helper would not satisfy the screen row.

| Actual caller | Current retention | Minimum scoped correction |
|---|---|---|
| `workspace-actions::openCommand` (`:846`) | One modal retains `command,payload`; RESOLVE_MANUAL_REVIEW creates its UUID once at open. Failed button already says Try again. | Map exact typed Source busy to fixed copy without treating it as failed evidence/REVIEW. Keep same payload and key on explicit retry. |
| `openManualReview` (`:951`) | `state.pending` holds original source row, reason and one command UUID; reason locks once pending. | Fixed code-driven busy copy and Try again label; preserve pending and original command. |
| `openProtectedMatch` (`:874`) | `state.pending ||= ...` retains the exact RECHECK_SOURCE request; selection/reason lock on pending. | Fixed busy copy/Try again; no new key or comparison request. |
| Protected shift/review saves | SP-06 now holds original action/payload/key and makes one POST per explicit click. Main approved-hours modal holds its original frozen command. | Distinguish proven busy copy + Try again from unknown “Retry exact request”. Keep existing unknown retention, typed refusal and actor/frame guards. |
| `import-workspace` RECHECK_SOURCE (`:1520`) | `session.pendingRecheck` already retains the original request_id/payload until success. | Code-driven copy + explicit retry; do not rebuild from a refreshed workspace. Do not discard its original scope as a side effect of busy repaint. |
| Query outreach bulk (`:1371`) | `buildOutreachRequest` uses the server action request but rebuilds current selection every click. | Retain the actual action and completed payload before POST; a busy retry bypasses current selection rebuilding. Lock only the original failed intent until explicit retry/cancel. |
| Combined review outreach (`:1121`) | Per-owner `requests` array is rebuilt from the current selected rows on every click; successful rows are removed. | Retain the actual remaining per-owner request array and current failed index; explicitly retry that same original payload, retaining successful earlier owners and never automatically resubmitting them. |
| Single FINALISE_WEEK (`:1604`) | Rebuilds payload/acknowledgement from current workspace each click. | Retain actual first payload/command before POST and reuse on deliberate busy retry. Do not overload `session.finaliseRetry`, which is an already-finalised pay-follow-up task. |
| Rate warning acceptance (`:1683`) | Builder recreates the current selected key list plus the server acceptance payload/proof. | If this request is a census participant, retain its actual completed first request before POST; explicit busy retry uses that request. No financial recalculation or new acceptance identity. |
| Upload acceptance: `openPreview` (`:575`) / `openBatchPreview` (`:531`) | Rebuilds upload acceptance from editable current coverage/intent (batch from entry model) per click. | For an actual busy participant, retain the literal acceptance payload per current preview/entry before POST; reuse on explicit retry and preserve accepted entries. Do not rerun upload/parsing or generate a new key. |
| Combined finalisation | `finalise-batch.js::create/run` already retains frozen `item.request`, skips completed items and retries only on a later explicit run. | Reuse this retention. Its current busy catch becomes CHECK_REQUIRED and visible “Result needs checking”/“Check saved results” in `combined-finalise.js:44`; give exact busy a finite Try again presentation rather than evidence/unknown wording. No new DB Stage or automatic same-item resubmit. |

These additions are local retained-request UI support, not a new command journal/replay protocol. For rebuilt import actions, retain the **actual first endpoint/action/body** and original continuation; do not implement a generic blind global reissue that loses the caller's success/partial-batch behavior. Only confirmed busy gets the transient copy. Unknown responses keep their existing original-request recovery/uncertainty behavior; a receipt parser failure must not be turned into a rollback assertion.

`import-workspace::requestJson` (`:945`) currently exposes status, `error_code` as `error.code`, and details but drops retryable/rollback/automatic_retry. Preserve only the exact finite busy metadata needed for code-driven UI classification, retaining original unknown transport fields. `friendlyWorkspaceError` has no busy mapping today and returns generic temporary-unavailable copy. `workspace-actions::plainMessage` also lacks the busy mapping and callers frequently pass only `error.message`; map from the actual structured `error.code` (and proven envelope facts where available), not text extraction alone. Use the exact fixed copy before any generic sanitiser so no raw DB body reaches the screen. Existing busy-disabled controls, initiating actor/frame checks and one-attempt behavior remain intact.

## Frozen STOP-CHECK source receipt

The saved `supabase/release/protected-boundary-lock.json` has exactly five declared frozen files. Fresh raw SHA-256 comparisons of all five saved files match their declarations:

| Frozen file | SHA-256 |
|---|---|
| migrations/22082026_0951_candidate_mytms_browser_isolation.sql | 98b2ed33447cf3c659a81f2772b5f097516158415d6fbcf95faa873ae601a9a4 |
| verification/22082026_0952_candidate_mytms_browser_isolation_verification.sql | 0feb5cdf864c893a9015e2955f5051673ca57d0eb1323b711dbac939a8d62d02 |
| migrations/22082026_1402_candidate_named_legacy_relation_isolation.sql | c5bbd78e2a2b4880bd41df421ef7ed93aae4e8de818a493767c81fbd4bd98db0 |
| repeatable/22082026_1402_candidate_named_security_definer_browser_isolation.sql | 48a585617017a518316fea11e15e502aa603ffe98b872838ca9abfda0157b080 |
| verification/22082026_1402_candidate_named_legacy_security_verification.sql | b013a4b81152d4461c6c8410e2466064ad200b3cf47ecb6279fa4ed7fe1e23ba |

This is a read-only saved-file hash audit, not an executed release/security verification, installed seal or permission to weaken any boundary. The named Worker error files are not entries in this frozen list. Keep the lock and all five files unchanged; any locked Candidate definer/file conflict stops that part for the root/owner. No Candidate SECURITY DEFINER was added/removed by this investigation.

## Source receipts

| Source | SHA-256 |
|---|---|
| BE broker/src/bpay-busy.js | c2fbf3b00a2d5f10811703fa2548587317a737eaabb5beef8ef2e5c80fd3ad7c |
| BE broker/src/weekly-source/routes.js | 3641498765ed87d9d2c450e07cfc3dca05a9601725976d2e82e315f40dde4b11 |
| BE broker/src/candidate-app-backend.js | 45826c5fd1d8232f2d9321ad4f4895b418fe302951e35d823e95bd68ceb2fe83 |
| BE candidate-broker/src/candidate-broker.js | a5b1ea663a73d5474f53a0e6393e206267bbc9a7582aac1a3cd519ac88d86a84 |
| BE broker/src/candidate-private-worker.js | 096be554abd0426ded5743ad7ac345dd22d7bc5da04e833e366bfe056800f671 |
| BE candidate-synthetic-private-api/src/index.js | a281b4b61ec8cce3575758ce3ff819c43d0655a78e5b0c2096e9920475d5c012 |
| BE protected-boundary-lock.json | 60d2d3d8a74535cf52c5213803d19b0654af48955b6096726e2b7a882545f30c |
| FE weekly-source/workspace-actions.js | 4c86b1b39870169db9e5cbbf3d9fe5f9d8795d8d68e13a86fbc296302bfb0b17 |
| FE weekly-source/import-workspace.js | bbaeea7cdc62d110b27b940483a6d565ffdc90e9a51cc2e032940cc09cc4061e |
| FE weekly-source/protected-shift-editor.js | a9725b6713da4ee875f67c7b48d9a5509ad9c0b54589949d97f761662a26d81e |
| FE weekly-source/finalise-batch.js | f099aba88dec4007347069b6021e9db73048579194bd4ff7b910d59bf8320231 |
| FE main.js | 3a51abe4360398ad21136c54587c89e1ee8c0f3197ecc4bef763208b61c38a27 |

No product source was written, and no test, SQL, runtime, fixture, hosted, provider or secret operation was performed. Future SP-12/SP-14/SP-15/SP-36 OPEN/PIN both-order/replay/page/timing/eventual-progress acceptance remains NOT_RUN; no bespoke fixture/hash/lab programme is proposed.

## Historical completion disposition

The retained Worker outbound C1 transport is NEXT-refused by installed0715 and current protected orchestrator560–572 resumes only Local completion; no broker entry imports the old executor. Nevertheless public.weekly_exceptional_pay_complete_c1_publication_v1(jsonb) remains service-callable SQL and materially creates0245's qualifying tuple. The card requires every missing gate writer, so add only its positively NEXT existing-key call after original pure validation, before original first request FOR UPDATE. This preserves historical terminal reconciliation and adds no outbound operation/route. Do not silently certify an un-fenced SQL entry retired. Unknown/checkpoint reject PUBLISHED; recovery requires RECOVERY_REQUIRED on both request/outcome, while its recorded replay does not write. Those operational writers cannot withdraw a qualifying tuple and remain unchanged. Final design therefore has five body-only successors (three Candidate, stage, historical complete).

## Current built writer closure (supersedes prebuild missing rows)

### Independent identity/rotation scope refinement (design before successor)

The independent census review proves four actual public material scope writers: current0500 manual-upsert and current2035 rotate/confirmed/QR restore. Scope0057 accepts ordinary self-bill current roots without a live Source authorisation, so the existing managed-root refusal alone does not close these independent entries. SP-12 explicitly names identity/mapping/rotation owners and missing admission in their final definers. New0922 adds only a positively NEXT existing-helper call after the original plain validation and before the first original business lock; original mutable guards and rechecks remain unchanged. For manual-upsert, its existing catch converts every55P03 into LOCK_TIMEOUT, so add a bare rethrow only for the exact55P03/WEEKLY_SOURCE_PAY_QUERY_ADMISSION_BUSY pair before that old branch. Preserve all other error handling, signatures, security, owners, ACLs and economic statements. No new migration, producer protocol, identity resolver, trigger, financial calculation or external action. Raw direct callers and outer same-transaction boundaries are recorded in reviews/SP-12-census-review.md; additional true outer entries must be admitted before their own first lock rather than relying on late nested calls.

BUILDER DECISION BD047: the named identity/mapping/rotation census must include the actual first locking RPC in a same-transaction call chain. Current Candidate finalise, its single-flight wrapper, paper-target preparation, Office combined-hours paper preparation and ordinary bulk manual-upsert reach the qualified primary-root mapping owner after earlier locks or broad retry catches. New0931 adds only early NEXT admission in these five current definers. Reuse the child's actual plain environment/feature/service-context and nonlocking session authentication before the single-flight key; atomic finalise and paper-target retain original locking authentication and all mutable workflow checks after the key. No new financial calculation, command, receipt, signature, permission or lifecycle state. Bulk takes the key outside its old catch/retry and before the correction prebranch for valid nonnull week IDs, because that branch can continue into the admitted child after locking. Keep its original null-week response and every original correction/retry statement. The early key serializes these existing entry paths; no first acquisition moves into their nested owners. Expense-only shells and read-only identity helpers remain excluded by the actual scope predicate. Source review and original-statement comparison are required before completion.

The same independent scope analysis identifies current0500 public.timesheet_authorise_generic_atomic as an actual revoked_at restoration owner:0057 rejects revoked roots, and this existing owner clears that marker. Authorised_at_server alone is not a gate operand, so native bulk authorisation remains unchanged. Include only generic's early NEXT admission in0931, after duplicate original primitive checks and before original entry diagnostics/business locks. Its exact Source55P03 rethrow occurs immediately after stacked diagnostics before any diagnostic write, preserving all other old errors and all financial statements. Reviewed-authorise has no preceding business locks and inherits this corrected child; current Source/Candidate callers already inherit their corrected outer key. This is the sixth0931 body and the narrowest actual closure, not a generic financial-authority rewrite.

Current0900 public.timesheet_weekly_chain_delete_apply is the final finite primary-root removal entry: its existing read-only preview permits an ordinary unpaid/unmanaged WEEKLY parent, unlike standard_delete and adjustment-only deletion, which cannot remove Scope0057's primary hours tuple. Add only early NEXT admission after its original plain preview/actor validation and before its original guarded locking base call. The seventh0931 body retains all deletion authority, expected-member/signature reproving, blockers and original statements; no delete is executed by this build. Standard/adjustment deletion stays unchanged. No new destructive behavior or public trigger is introduced.

The preceding NONE entries are the preserved prebuild investigation. The following five-row table is the initial0853 build checkpoint; the final current closure below supersedes its inventory/seal and extends the finite actual writer graph. Correctly admitted original rows remain reused; helper, deep compare, public signatures, ACL membership, pin/snapshot and all F-25 parked files remain unchanged. The retained historical complete SQL receives the required key regardless of its retired outbound Worker caller. No external publication is enabled.

| Exact entry | Current root | Key line | First original business-lock seam |
|---|---|---|---|
| public.weekly_source_candidate_app_submit_atomic_v1(uuid,text,uuid,jsonb,timestamp with time zone) | 10102026_0853_weekly_source_pay_query_entry_admission_v1.sql | 92 | 95: v_context:=private._candidate_session_context_v1( |
| public.weekly_source_candidate_self_submit_atomic_v1(uuid,text,jsonb,timestamp with time zone) | 10102026_0853_weekly_source_pay_query_entry_admission_v1.sql | 356 | 359: v_context:=private._candidate_session_context_v1( |
| public.weekly_source_candidate_check_materialise_atomic_v1(jsonb,timestamp with time zone) | 10102026_0853_weekly_source_pay_query_entry_admission_v1.sql | 510 | 513: perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended( |
| public.weekly_exceptional_pay_stage_c1_request_v1(jsonb) | 10102026_0853_weekly_source_pay_query_entry_admission_v1.sql | 1234 | 1237: select request.* into v_publication |
| public.weekly_exceptional_pay_complete_c1_publication_v1(jsonb) | 10102026_0853_weekly_source_pay_query_entry_admission_v1.sql | 2043 | 2046: select request.* into strict v_publication |

Local PG17 load315 migrations/825 repeatables compiles all five current bodies; current-contract seal2eb4364ae357a11ecefd0296fc5eef7dde6da4d113825d0620300a3040d867bc. General catalogue829/76/0/md555bbf326922f59d683823d40f5f3544a; full general and Candidate named verifier DO both pass. Existing nine source gates and61 existing Worker/broker/source checks pass. Five protection hashes match, original-five-body statement preservation independently reviewed. Public membership did not change, so pinned verifier constants stay correct in this commit.

Candidate managed/local install order: all315 migrations (including query-pin0044 and Rule-B0151) precede repeatables; original2203 precedes0044 helper, which precedes0853. The historical CI fixture array stays unchanged; its new source-only step runs the existing actual source-gate suite. SP-36 remains the actual MyTMS consumer owner; WB-02/WB-05/SP-15 must append their real future refresh rows. No Source activation, compatibility release, owner switch or joined acceptance has occurred. Real OPEN/PIN/orders/pages1/17/100/liveness/replay and queue/app timing remain authored NOT_RUN. Actual local BE/FE implementation commits are recorded in the completion receipt after local commit.


## Final current SP-12 source closure — 10 October 2026

The initial investigations and0853 checkpoint above are historical. Current closure covers the five0853 rows above plus these eleven actual existing identity/outer/scope entries. Every row takes the existing transaction admission key before its first original business lock: YES. This is the finite traced current graph; future WB-02/WB-05/SP-15 refresh rows must be appended when they land, and SP-36 owns the actual MyTMS consumer. No blanket claim that whole0200/0900 files are nested substitutes for these exact entries.

| Exact current signature | Root/key line | Original pure prefix and first lock | Scope/caller disposition |
|---|---|---|---|
| `public.contract_week_manual_upsert_atomic(uuid,uuid,jsonb,jsonb,jsonb,jsonb,jsonb,uuid,boolean,timestamp with time zone,text,jsonb)` | 0942:145 | Original pure input/context normalization107–142; first CW row lock195. Admission also precedes optional entry diagnostics148. | Creates/rebinds primary root; exact busy rethrow0942:2100 precedes diagnostic writes. |
| `public.timesheet_route_version_rotate(uuid,uuid,text,uuid,boolean)` | 0922:2184 | Plain target booking read2179–2181; first trimmed booking advisory2189, raw2191, family rows2194. | Both existing feature branches inherited; original locked managed guard/rechecks retained. |
| `public.timesheet_route_version_confirmed_v1(uuid,uuid,text,text,text,uuid,text,text,text,boolean,timestamp with time zone)` | 0922:2393 | Original IDs/signature/context input2374–2377, plain target/environment/feature/service checks2379–2390; first paper-family advisory2402, booking2403, rows2405. | Covers prior workflow/route locks before nested admitted rotate. |
| `public.timesheet_qr_restore_version(uuid,uuid,text,uuid)` | 0922:2733 | Plain booking read2728–2730; first trimmed booking advisory2738, raw2740, family rows2743. | Legacy/core branch dispatch remains original; no new validator/security contract. |
| `public.candidate_submission_finalize_atomic_v1(uuid,text,uuid,integer,text,text,timestamp with time zone,jsonb)` | 0931:117 | Original environment/feature/service/key checks91–104 plus existing nonlocking session context109–114; first workflow row lock120; original locked context127. | Non-null session authenticates before key; original null-session service/replay behavior and locked mutable checks remain. |
| `public.candidate_submission_finalize_single_flight_v1(uuid,text,uuid,integer,text,text,timestamp with time zone,jsonb)` | 0931:1073 | Original workflow/generation input1035–1037 and duplicated exact child pure environment/feature/service/key/plain session checks1050–1071; first original global try-advisory1080, workflow key1094. | True outer before both single-flight keys; original capacity deferred reply retained1083–1090. |
| `public.candidate_weekly_paper_target_prepare_v1(uuid,text,uuid,integer,timestamp with time zone)` | 0931:1195 | Original workflow/generation validation1161–1164; original Office plain context or existing session context(false)1168–1193; original context(true)1216 then advisory1225. | Before transitive locking authentication; original repeated locked checks/replay body retained. |
| `public.candidate_office_expense_paper_prepare_v1(uuid,text,uuid,uuid,uuid,integer,text,timestamp with time zone)` | 0931:1493 | Original primitive actor/operation/update/workflow/generation/key checks1486–1490; first operation row lock1505, update1513, workflow1518. | Actual combined-hours paper path can reach primary root after these locks; independently callable outer now keyed. |
| `public.contract_week_manual_upsert_bulk_process_atomic(uuid,uuid,jsonb,jsonb,jsonb,jsonb,jsonb,uuid,boolean,timestamp with time zone,text,jsonb,jsonb,text,jsonb)` | 0931:1580 | NEXT + nonnull p_week_id1579; before continuing correction transition1588 and before original retry LOOP1614/child1626. | Preserves original null-week/correction flow; key is outside retry handling and cannot first acquire after correction locks. |
| `public.timesheet_authorise_generic_atomic(uuid,uuid,uuid,timestamp with time zone,text)` | SP14/A3 final11102026_2223; predecessor0939/0931 retained | Original primitive validation before original diagnostics/first business lock. Lock-free canonical-root/native Source applicability pre-read; Source-managed, revoked restoration and unknown/unresolved metadata take original admission. Known ordinary unrevoked bypasses. Exact Source busy RAISE and original locked target/expected version/mutation/rate/response tail retained. | Owner11Oct A3 narrows obsolete every-NEXT admission; fresh catalogue and SOURCE evidence in SP14 completion/reviews. No new query producer or late key. |
| `public.timesheet_weekly_chain_delete_apply(uuid,uuid,uuid[],uuid[],uuid[],text)` | 0931:2419 | Original read-only preview/actor validation2414–2415; before guarded original base call2488 (first transitive Timesheet lock in unchanged base1276); original outer Contract lock2510. | Actual WEEKLY primary-root removal entry; no changed delete authority, expected sets, blockers or downstream statements. |

0942 is a new successor of the already locally installed0922 manual-upsert. It moves only the existing exact55P03/WEEKLY_SOURCE_PAY_QUERY_ADMISSION_BUSY bare rethrow immediately after stacked diagnostics, before optional diagnostic writes. The other three identity definitions remain0922 and the seven outer definitions remain0931. No original business statement, money calculation, signature, configuration, owner or ACL changes. Weekly-chain preview is read-only in its actual body, but its declaration does not explicitly say STABLE; no volatility inference is used.

Reused correctly admitted Source publication/query/Local/release rows remain listed above. Reviewed actual exclusions: read-only identity/preview helpers; expense-only shells outside Scope0057 primary hours; native bulk authorised_at-only stamping; standard delete that refuses WEEKLY primary parents into chain-delete; adjustment/additional removal. Office ROUTE_CONFIRM/FINALISE adapters and reviewed-authorise inherit corrected children before any original business lock. Pending receipt operational transitions with the same qualifying tuple do not create a new gate writer; actual release apply already owns the key before its pending-row lock. Dynamic/recursive owners and immutable receipt publication remain covered by the earlier concrete matrix.

Current pin0044 and Rule-B0151 snapshot guards include the actual immutable TRUNCATE/UPDATE/DELETE protection, service-only admission and private owner/ACL/RLS boundaries; no TRUNCATE or business mutation was executed. Five declared protection files retain exact raw hashes. Source review preserves Candidate original session authentication and its locked mutable checks; the frozen lock is unchanged.

Local PG17:315 migrations/828 repeatables, final0942 compiled with0 migrations/4 repeatable closures; contract seal ede67308bf497f65977f44f6628810c14a561b0e395c2a79dbbf62c69b8b0310, coupling PASS. Nine actual source gates PASS, existing Worker61 and Office129 source checks PASS; syntax/diff PASS. Fresh public browser catalogue829/76/0/md555bbf326922f59d683823d40f5f3544a, full general and Candidate verifier DO PASS. No public SECURITY DEFINER identity/ACL membership changed, so verifier constants remain correct. Protected release verification remains PENDING.

Office source commit daffbd90bdcce410b2dcbd306548fde51f60ce0e. Backend implementation commit is recorded in SP-12-COMPLETION.md after committing this batch. All future both-order OPEN/PIN, contention, replay/unknown, pages1/17/100, liveness/no-starvation, joined Worker/queue/Office/app and timing acceptance remain authored NOT_RUN. Rule B and all parked F-25 files stay unchanged.

## WB02/WB05 current hook census — sequence52

Saved current authoritative producer map: design/WB-05-source-owner-investigation.md (all NX004–019/023/025 and17 selectors). The earlier matrices remain historical receipts. No new admission protocol/key/outer command is introduced; these private hooks inherit the existing true outer admission before its first business lock.

| Current exact hook owner | Admission before first outer business lock | Current exact hook and scope |
|---|---|---|
| private.bpay_next_source_withdrawal_guard_v1() final1332 | public.weekly_source_first_authorisation_withdraw_v1 final0556:2227 before callback-mode/family locks and original root lock2265; immutable no-write replay exempt, conflicting replay audits take key2208 | NEXT transition1332:379–382; Candidate control391–394 before WORK402; exact work pointer419–422/financial revision423–426/refresh427. Original Source financial-dependency refusal retained. |
| private.bpay_next_ordinary_authorisation_trigger_v1() final1332 | Generic material revocation/restoration/identity outers retain SP12 admitted0931/0922/0942 before their first original locks; native auth-only stamping is a proved excluded non-scope mutation | Exact ordinary withdrawal1332:352–360 refresh once; genuine reauthorisation publishes through common1346. No separate key inside AFTER trigger. |
| private.bpay_next_publish_staged_revision_v1(uuid,uuid,uuid) final1346, Source/protected call path | Genuine Source firstAuthorise2018:3569 before Source lock3572; protected0345:545 before current-root lock559; receipt-producing immediate0300:3000/core0556:329/local1257:125/release0700:1211 before their respective original first locks, as exact current NX table records | Same publisher217 Processing before clock221; final1346 apply426–435 refreshes only complete current native authority, older approved pointer retains dirty. Private pair1403 inherits the key and Candidate order; finite Contract/Client preholds before its first clock close second-member FK scope. No Source evidence version/proof writer. |
| private.bpay_next_publish_source_staged_pair_v1(uuid[],uuid[],uuid[]) final1403 | Same actual admitted outer Source/protected owners above; 1–2 exact members, no independent public entry or service grant | Adds only exact selected Contract FOR SHARE and Client FOR KEY SHARE before first publication; existing complete replay skips these additional locks. Original receipt/physical/member locks retained; all current work contributions reach1346 Processing then actual final apply. |

These current hook rows are source-built and locally compiled only, not hosted-installed or a both-order contention test. Actual implementation hashes are recorded in WB02/WB06/WB05 completion/checklist receipts. SP15 will add genuine query OPEN/RESOLVE refresh rows; SP36 owns the actual MyTMS consumer. Whole SP12 ITEM stays open for those true remaining owners; no future YES certificate is claimed.

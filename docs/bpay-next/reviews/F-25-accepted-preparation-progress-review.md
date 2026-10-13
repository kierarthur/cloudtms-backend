# F-25 accepted preparation progress — independent source review

9 October 2026. **PASS — final saved SQL source review.** All previously requested finite fixes are closed. Reviewed the full new uninstalled2336 SQL against immutable2153, core1324,2018 public page,2236 enrollment/PREPARE consumers, current2306/2308 terminal owners, expiry0510, and the accepted transport design. Root alone writes product. This review executed no SQL, tests, runtime, browser, fixtures or hosted operations; it makes no installed/release/runtime claim.

## Exact reviewed objects

- `private.bpay_next_accepted_preparation_header_v1(uuid)` —2336:5.
- `private.bpay_next_accepted_candidate_page_v1(uuid,uuid,integer)` —2336:25.
- `private.bpay_next_accepted_work_page_v1(uuid,uuid,uuid,integer)` —2336:108.
- `public.bpay_next_preparation_page_v1(uuid,text,jsonb)` —2336:172.

## Closed findings and current evidence

| Earlier concern | Final saved closure |
|---|---|
| Whole-run header membership/count scans | Header2336:10–17 reads its single run→run_command→PREPARE command and actual maintained selected/enrolled counters. No full selected count/anti-join remains. Candidate35–38 materializes the original selected UUID keyset window LIMIT limit+1 before LEFT JOIN member/accepted/progress. Exact member_no and command/Candidate/accepted sequence/module checks47–51; each actual emitted missing accepted/member row refuses61–62 rather than being silently omitted. |
| NULL expected count could evade IF | Header16 explicitly rejects null expected/enrolled counts;14–17 require positive expected=actual selected count, enrolled within range and original SEALED selection. Original kind/sequence/epoch preserved. |
| Missing completed POSITION_APPLY job reported pending | Work135–140 refuses missing exact job when original command COMPLETE, enrolled>0 or publication APPLIED/DONE; genuine one-member SEALED/enrolled0 pre-enrollment remains pending under current2236:395–413. Publication/command/member identity, exact predecessor<accepted sequence and original epoch/job tuple are validated. |
| Present cut omitted evidence/PREPARE owner linkage | Work143–150 validates all parent WORK/Candidate/revision/publication/predecessor/selected count, cut evidence hash and actual original PREPARE job command/Candidate/sequence/module. Candidate63–72 applies the same retained cut tuple/hash/job checks to present cuts before deriving has-unsealed progress. Migration2153's generic job/hash constraints are no longer treated as the whole exact receipt proof. |
| Expiry root could display READY | Actual run_status CANCELLING or worker CANCELLING takes CANCELLING/CANCELLATION_PENDING79 before REVIEW/READY. Original CANCELLED/DONE-EXPIRED retirement74–75 and epoch priority76–77 remain. Existing expiry0510:79 sets root CANCELLING before ordered cleanup; completed original jobs need not be rewritten merely for this display. |
| READY lacked sealed/full original PREPARE/cut closure | Candidate81–83 requires actual PREPARE DONE/READY, genuine READY/DRAFT/ISSUED/COMPLETE worker, no unsealed accepted WORK, original command SEALED/COMPLETE and maintained enrolled=expected. Unknown combinations refuse91. No all_ready/Confirm authority is derived from page completeness. |
| SQL byte bound omitted header | Candidate99 and Work163 now cap the full raw `{header,rows,complete:false}` object at120000 bytes before accepting an entire next row. false is conservatively at least true's serialized length. First oversized row raises54000; later stop preserves last emitted key. Empty case-only page/header is bounded by its closed fixed scalar shape. |

## Full source behavior review

Header preserves original immutable run/PREPARE identity, accepted agency sequence/module and real maintained expected/enrolled counts. It does not calculate a completed Candidate total, new economic input, valuation or financial readiness. Each bounded Candidate window is the original selection, not today's current WORK/Candidate census. Accepted_input FKs and original acceptance/enrollment remain membership authority.

Both request key sets use the original preparation_args helper2018:3621: exact object keys/32KiB cap, canonical UUID scalar types, explicit nullable after cursor and integer1..100 limit. Public authority precedes kind/args180: service JWT plus active admin and positive stable NEXT owner via0575:7–20. Candidate outside original accepted run scope fails lookup; no new input is created. Public function's source-body comparison to2018 found exactly two hunks: the two new kind-key map rows and the two private dispatch branches. Every existing preparation/current/frozen review/status branch is otherwise unchanged.

Private helpers are STABLE SECURITY INVOKER with fixed pg_catalog/private search_path; public wrapper SECURITY DEFINER retains pg_catalog/private/public. Owner postgres and all private EXECUTE revocations352–357; original public service-role-only EXECUTE358–360 and schema notify361. No direct table/browser grant, arbitrary owner, new endpoint, job claim, command receive, clock or financial mutation.

Candidate page includes accepted Candidates before job/worker creation and after interruption between Candidates. PREPARE job is optional at genuine ENROLLING stage, exact original command/Candidate/kind/sequence; actual execution epoch retained separately. Worker uses run/Candidate. Case-only accepted scope with zero WORK stays visible, has_unsealed_work=false; its Work page is empty and complete=true. Current accepted immutable target channel/Umbrella/week/case-selection/evidence are projected without live Candidate/Umbrella/policy/Source query reads, finance counters, leases, contacts, tokens or origin/query JSON.

WAITING_ENROLLMENT requires absent job and worker with genuine RECEIVED/ENROLLING command. Current NEW/PINNING/RESERVE/ALLOCATE/FINANCE_ALLOCATE nonterminal states remain PREPARING or WAITING_EARLIER_COMMAND from exact existing same-Candidate earlier unfinished-job sequence barrier. CUT_PENDING comes solely from this accepted input's missing effective cut. REVIEW remains the genuine worker REVIEW terminal, separate from PREPARE DONE/READY economic completion. FAILED does not become ready. CANCELLING/CANCELLED are factual lifetime states, not authority to release holds.

EPOCH_PAUSED keeps original accepted epoch when current active NEXT epoch or job execution epoch differs; no F26 bridge/resume is invented. A non-NEXT active owner is still rejected by original public authority as MODULE_NOT_ACTIVE before page construction, preserving positive owner gating and UI invalidation. Historical cut validation compares original accepted epoch/job identity, not today's claimability/lease; a sealed accepted receipt is not re-evaluated using current Source HEAD/query/current revision.

Work page retains original/effective revision difference, certified original selected-line count, exact publication command/predecessor sequence and captured query CLEAR/hash only for SOURCE/PROTECTED. Cut SEALED is genuine immutable receipt; no invocation of seal_effective_work_cut or work-current lookup. Without cut, exact predecessor APPLIED/DONE publication, COMPLETE/sealed fully enrolled command and actual POSITION_APPLY DONE/DONE no-lease tuple produce WAITING_PIN; otherwise genuine pending predecessor remains WAITING_PREDECESSOR. Evidence gaps after enrollment refuse as above. Selected member/price/counter history is not rebuilt.

Strict increasing Candidate/WORK UUID cursor, limit+1 lookahead and complete flag match the transport design. Extra row is not packed; exact-limit terminal page stays complete=true. A byte stop returns whole rows and truthful incomplete flag; it cannot return empty incomplete and loop at the same cursor. Existing unique/FK identities prevent join fanout. Indexes selection(run,Candidate), accepted_input(run,Candidate/command,Candidate), accepted_work(accepted_input,WORK), effective_cut(accepted_input,WORK) and job(command,Candidate,kind)/job earlier barrier underpin the finite scoped reads. Scoped per-Candidate cut EXISTS remains the explicit design dependency; no whole agency/run history aggregation was introduced.

## Integration limits

This is source PASS for the reviewed2336 checkpoint. Final-definer ordered chain/private helper closure registration, root compilation/dependency checks and required normal source-unit tests remain root's build integration duties. Existing accepted mutation result stays the four-field original receipt; existing ENROLL wake remains unchanged. Worker/Office must validate the closed new envelopes and cannot clear uncertain request journal or enable Confirm merely from this read. Root is implementing that transport separately; SQL source PASS is not transport PASS.

## Saved-byte checkpoint

- Source `supabase/repeatable/09102026_2336_banking_pay_next_accepted_preparation_progress_v1.sql`: 29176 bytes; SHA-256 `e019512a0efde50be464513bd60a231d221a3a403f12a80c212ce4e90bc1c2dd`.
- Review saved 2026-10-09T22:44:42.191882+00:00.

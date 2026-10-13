# SP-12 backend admission, transport and scope review

10 October 2026. **PASS for the final saved five SQL entry successors, three Worker changes, registry delta and source-only Candidate CI support. All three initial source findings are CLOSED.** Read-only comparison against the complete SP-12 card05:420–454, all eleven deliverable rows, BD045/046/master census, the surviving original2203/X1534 definitions, F-04 authority and B2.22/B4.4. This reviewer ran no SQL/runtime/tests/app/fixture/hosted actions and changed no product source. D07/D08 Office implementation, final census/commit receipt and SP36 MyTMS are separate owners; this is not whole in-app SP-12 completion.

## Final SQL scope and preservation

`10102026_0853_weekly_source_pay_query_entry_admission_v1.sql` contains exactly the five named existing public SECURITY DEFINER definitions and owner assertions, one transaction and the required pgrst reload notification. It adds no function identity, schema, table, trigger, producer, command kind, money computation or outbound action. Original parameter names/defaults, return types, volatility, search_path and security mode remain exact. Independent full-definition comparison finds **only insertions** in each of the five functions: every original declaration and original body statement is retained in its original sequence, including authorization, request hashes, collision/replay, locks, financial/Source mutations, audit and replies. The changed definitions use CREATE OR REPLACE; existing service-only privilege membership is preserved. The saved current catalogue lists exactly postgres/service_role EXECUTE for each of these five functions; no browser role or PUBLIC grant is introduced. Actual local remeasurement/seal is builder evidence, not performed by this reviewer.

| Entry | Actual new admission → original first business lock |
|---|---|
| Candidate app submit |0853:92 → original session context95–97 with p_lock=true (private helper's session FOR UPDATE at07082026_2059:633–635); original APP advisory and receipt handling remain later. |
| Candidate self submit |0853:356 → original session context359–361 with p_lock=true; original workflow lookup and direct materialise call remain afterward. |
| Direct Candidate materialise |0853:510 after original pure typed request/kind/hours-only/hash checks → original materialisation advisory513 and workflow FOR UPDATE518. |
| Genuine Source-local stage |0853:1234 after original exact stream/request/type/count validation → original publication-request FOR UPDATE, including its exact replay/collision branch. Earlier/later RPC transactions are not counted as inheritance. |
| Historical C1 completion |0853:2043 after original pure schema/UUID/hash/key validation → original request FOR UPDATE2049. This service-callable SQL root materially qualifies a retained receipt even though old outbound Worker transport is retired; no outbound route/executor was restored. |

Each call is the existing owner-only try-lock under the database's actual active_owner='NEXT' plain read. It occurs before any original business/module row lock, not in a deep comparator/row/deferred trigger. Original mutable facts are revalidated by the original later locked statements. Existing correctly admitted Source owners, the shared session helper and private comparator remain untouched. The global admission key is operational contention, not an input version, accepted cutoff clock or financial authority.

APP adds the original p_lock=false authentication, selected Candidate guard, and a complete exact duplicate of original p_body shape/key/type validation before admission. Its original p_lock=true context and original checks remain intact afterward. SELF adds the same nonlocking authentication/selected guard and exact shape/primitive casts; **it does not move the original workflow SELECT before admission**. The helper's p_lock=false branch and STABLE email eligibility closure contain only plain reads. Thus plain input/auth are validated first, while original locked context re-proves actual selection/account/currentness. Original UUID normalization, request fingerprints, hashes and immutable replay results were not substituted or recomputed with new semantics.

## Registry, protected boundary and Rule B

Registry semantic delta is exactly one existing app_submit chain updated and four new self_submit/materialise/stage/complete chains appended. All five point to0853 with their exact original2203/X1534 predecessor/allowed physical source and correct final closure. No other existing chain is changed/removed and no other top-level semantic key changes. Existing private comparator still points to original2203. The five protected-boundary files and lock remain untouched; current Candidate signatures/public membership remain unchanged. This is the explicitly named SP-12 boundary change, with semantic auth/replay/facts preserved, rather than a permission inferred from not being listed.

All37 F25 parked paths remain absent. No stopped version/latch/certificate/witness/digest/true-tail/alternate-clock/setting trigger/resolver protocol is restored. Existing Rule-B expected_revision_id, captured selection_candidate fields, run_work_query_snapshot and pin_v2 retain their original ownership. Existing pin/snapshot TRUNCATE guards and private ACLs are reused; no duplicate migration or trigger is added. The master design's pending-state and historical receipt classifications are factual scope distinctions, not blanket exclusions based on a file or table name.

## Exact known-busy transport

`bpayBusyCode` still requires an actual integer4xx/5xx status and structured object `error.json.code==='55P03'` plus an exact message from its closed map. The sole added message is WEEKLY_SOURCE_PAY_QUERY_ADMISSION_BUSY. No arbitrary55000, different55P03 message, raw string/status alone, timeout or unknown transport is promoted to rollback retry. Existing two busy-code behavior is retained. New Source envelope has clean409, ok=false, exact error_code, fixed safe copy, retryable=true, failed_rpc_outcome=ROLLED_BACK, automatic_retry=false, workflow_outcome=NOT_ESTABLISHED; prior committed workflow requests are not called rolled back. Raw SQL/body text is absent.

Candidate private errorResponse adds RETRY_SAME_REQUEST only when that actual Source busy classification was established. Existing Candidate/Office status/error paths remain otherwise unchanged. Public broker preserves the already bounded private response read and adds a branch only for status409 plus all actual exact ok/code/retryable/retry_class/rollback/automatic-retry/workflow facts. It rebuilds fixed safe fields/copy rather than forwarding arbitrary private JSON. Missing/mismatched facts do not gain retry_class. Existing unrelated TEST diagnostic/challenge/duplicate-expense handling is retained; the genuine private known-busy early return carries no transport diagnostic. No command/body/idempotency input or normal/public/private binding/config was changed. Release still requires real/synthetic private versions before public broker; source review does not authorise deployment. SP36 owns actual MyTMS retained-request consumer.

## Candidate CI support

The workflow now fetches full Git history and invokes actual existing `node --test tests/bpay-next-source-gates.test.js` after db:check. The historical literal install_files array and its fixture path list compare **exactly unchanged** with HEAD. No Source/NEXT schema is appended to that incomplete old runtime subset; no fixture, tool or test programme was introduced. Existing managed inventory date order supplies0044 helper before0853 successors, with all migrations before repeatables. Record actual file+commit and use the existing final-definer/source gate evidence rather than claiming the old fixture array installs the Source chain.

## Initial findings and final closure

1. **Malformed APP copied-validation prefix — CLOSED.** Initial raw SQL42237b... truncated the fingerprint pattern and omitted the complete copy/admission. Fresh final raw SQLabc874... has the entire exact original pure block, the NEXT call and unchanged original locking body. All five definitions were independently rediffed after correction.
2. **CI loaded exports without executing the gate — CLOSED.** Initial `node scripts/bpay-next-source-gates.mjs` had no CLI call. Fresh workflow uses the existing actual source-gate test invocation above.
3. **RPC reload notification absent — CLOSED.** Fresh0853:2264 contains `notify pgrst, 'reload schema';` before commit.

No remaining concrete backend source/scope finding was found in this bounded batch. Before D04/completion tick, refresh the prebuild census NONE rows to the actual final0853 admission/first-lock lines and record the real committed file versions; the old row text is investigation history. Existing ordinary compile/source/seal/security checks belong to the builder. All real OPEN/PIN contention orders, original retry/unknown recovery, pages1/17/100, eventual progress/queue/app timing and SP36 consumer acceptance remain authored **NOT_RUN**.

## Full definition comparison hashes

These are complete source CREATE OR REPLACE definition hashes, not pg_get_functiondef catalogue hashes.

| Function | Original definition SHA-256 | Final0853 definition SHA-256 |
|---|---|---|
| public.weekly_source_candidate_app_submit_atomic_v1 | bacebf22fe592c5ae522133a29c329d6b3438b1e7e6efecd00b443629bd78715 | c3a28a848ff55983143e2e1d231aea062fee9bb2b86bd2a21c6c2a4ac551e965 |
| public.weekly_source_candidate_self_submit_atomic_v1 | 7c370d4c7ec1e8b1fcafe6e0fd1a16ae19f7039d2c93358417c1cd6ceb81d8fc | 9fe5c6b7781eefc666e6d6f1cafeeef91ff895490a7b868cbdbd007978a9af4b |
| public.weekly_source_candidate_check_materialise_atomic_v1 | 4b7db1c22bb8e0d9d8c13b783f6c031f31b464ed61261ff5860f03e12cf93d69 | d4b13c8ff109290882f34a0cffa9cf8fa2bfd1abbb20ef2446ac51f0acf9ef64 |
| public.weekly_exceptional_pay_stage_c1_request_v1 | 1efd461061b3ae793162347ce8ffdb57760ca3dbe1c202925061ac86ef4e9188 | 2c311b23730e311166f5df8ebd86cbc051579352f43693720c142f8c195b4ff2 |
| public.weekly_exceptional_pay_complete_c1_publication_v1 | 7571a944a79d86bc98474eebda6b0b19415e0c7d20f9f964af904913032ac908 | 1ea853b18f55875eeeb9aaded7fc76b6afa0e563ea77cd01fbc6a7680c6d1ff7 |

## Reviewed raw source snapshot

| Source | SHA-256 |
|---|---|
| supabase/repeatable/10102026_0853_weekly_source_pay_query_entry_admission_v1.sql | abc874b2d4ed90529ad591ea08a0a5131a6bc32c86109911631c8516e18b2e94 |
| broker/src/bpay-busy.js | f9de3b1fd722ce592868fe5b13f65d01494c88d83d082fcd1bd214bc99c16932 |
| broker/src/candidate-app-backend.js | 162dc06cead09de49fc9d7b60f1189eac3c5420544f7872bb158a428f4e8379b |
| candidate-broker/src/candidate-broker.js | 4b8904edf3dc787ffdd04f3ed89afa19d9fe236ee35506e0dba20b1154976685 |
| .github/workflows/candidate-db-runtime.yml | 341d2d57569c9cbecd3261cfa7a4681918aa36358ce1ad6cc36ad9b75aaf53ec |
| docs/bpay-next/final-definer-closures.json | 24a3fb220fdb7e7d62d2aa3d6e959705caf890036cc4e3c77d6d4634f4d0b37d |
| docs/bpay-next/design/SP-12-census.md | 9a36e43eb039f3aef6d6ff76cb0748af64b8584a222a1a5e4733431d65b3cbcc |
| docs/bpay-next/design/SP-12-SCOPE.md | b0f8017bcc33505abcf46c6b1820f8a0ef23ac9d44cfc1303a5d5b2a4252510f |
| supabase/release/current-contract.json | 6b7d3f4066a8d20575081a37b200065404057c33dfe6277a7300b2d6fb25e453 |
| supabase/release/protected-boundary-lock.json | 60d2d3d8a74535cf52c5213803d19b0654af48955b6096726e2b7a882545f30c |

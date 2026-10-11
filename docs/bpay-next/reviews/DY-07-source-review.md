# DY-07 completed source review

11 October2026. **SOURCE PASS: no essential defect found in the completed scoped batch.** Independent static review only. No product/checklist change, SQL execution, database, test, application, fixture, screen or hosted action. This is not an installed/runtime/joined acceptance receipt.

## Scope / complete exact parent

Full card06:1041–1076, checklist D01–D05 at2610–2626, F03/master sequence94, BD105 scope and saved DY-07-current-investigation govern. Completed SQL is `supabase/repeatable/11102026_1119_banking_pay_next_daily_single_flight_adapter_v1.sql`.

Independent complete-statement comparison extracted the entire original1832 public.cloudtms_office_candidate_adapter_v1 and entire1119 successor. Both contain564 lines. Replacing the successor's single nested single-flight callee back to atomic reconstructs the complete normalized parent **exactly**. New file contains one function definition, one single-flight call and zero raw atomic calls. This comparison covers the full unchanged adapter, not just its finalise fragment. Original1832 bytes remain unchanged.

Successor194–222 keeps both FINALISE_EXECUTE and FINALISE_REPLAY_LOOKUP in the original common branch. Workflow/generation/idempotency admission, expected row signature, observed time, materialisation JSON, actor injection, replay_probe_only/replay_key_probe_only conditions, native service context open and result contract merge are exact parent text. Normal deferred return still reaches context_close221 before return222. No new acquisition protocol, recovery process or raw fallback was introduced.

## Current wrapper / Source / security

Current SP12 single-flight remains `10102026_0931_weekly_source_pay_query_outer_entry_admission_v1.sql:1019–1124`; its whole saved file has no current Git diff and retains raw SHA below. Native Source/query admission remains ahead of global and workflow/generation non-waiting gates; the raw atomic call inside that already-gated wrapper remains intentional. No04092026_1420 reinstall, Source producer/admission rewrite or new money owner occurs.

1119 retains original SECURITY DEFINER, postgres owner, original search_path and exact existing service-only public adapter signature. Tail573–576 revokes PUBLIC/anon/authenticated, grants service_role and sends transactional PostgREST schema reload. Existing same-signature CREATE OR REPLACE does not add another public definer. No new Candidate-named signature or current-release security inventory membership is added; current-release.json and protected lock have no current Git diff. Catalogue/security membership0 is root's normal check, not independently executed here.

Final-definer registry15187+ correctly records1119 as finalRoot/finalPhysical with exact1832 predecessor and1119 closure. The existing wrapper13030+ remains0931. No broad owner/gate exemption or signature expansion is needed by this batch.

## Worker / exact20s propagation

Current Git diff is confined to4insertions/4deletions in index.js and2insertions/1deletion in candidate-app-backend.js. The former changes only the four authorised caller lines; the latter only adds the qualified Office options expression.

- Index28181–28184: both existing BEGIN_CANONICAL_DAILY_SAVE branches pass timeoutMs20000. Original fresh begin-context key/process/payload remain.
- Index28320–28324: Office FINALISE_EXECUTE has timeoutMs20000; non-Office invokes single-flight with the same finalisationPayload and timeoutMs20000. Original stable business key `${idempotencyKey}:finalise` and captured materialisation arguments are unchanged.
- Native Office funnel10856–10865: only FINALISE_EXECUTE and FINALISE_REPLAY_LOOKUP receive existing MANAGER_FINALISATION_RPC_TIMEOUT_MS=20000; every other action retains undefined options.
- rpcCall1137–1153 forwards those options to deps.rpc. Actual createCandidatePrivateDependencies index198426–198430 forwards its third argument to sbRpc. Generic sbRpc165017–165032 uses the finite explicit timeout to create AbortController/timer. The option is therefore not dropped in the real dependency path.
- Existing non-Office native replay/finalise calls keep their original20s constant. Manager non-waiting R2 lease and sequential recovery are unchanged.

Source-only diff supports the narrow four-line modification claim; root separately records preservation of original mixed-newline bytes. This reviewer does not substitute a normalized comparison for a raw-byte claim.

## Native pending202 / replay / unknown results

safeFinalisationResult5742+ preserves finalisation_pending/single_flight_deferred. Actual Candidate retry8358 and Office retry11296 use202 when deferred and200 otherwise. Existing PHONE/EMAIL manager accepted-background paths keep202 and their own render/finalise owner. Deferred adapter replies retain original contract and flags; there is no false success conversion, regenerated business key, fresh-input replacement or raw retry fallback.

All original replay/context semantics are preserved by the complete SQL inverse and small Worker diff. Timer expiry keeps existing thrown transport/error/recovery handling; it is not used as proof that the atomic transaction failed. No new command journal or general transport rewrite is justified.

## Protected STOP-CHECK / evidence limits

`design/DY-07-boundary-check.json` records pre-product check10:20:24.420Z, exact planned paths, existing adapter/wrapper signatures, all five expected/actual hashes, local parent/wrapper MD5 comparison and public membership delta0. Fresh independent static hashing in this review again matches all five protected files. Current lock is file-only with no separate object array; the selected paths are not listed. No locked file was changed.

Root reports normal db345/902, source12/12 and syntax PASS before sole-local SQL compilation. These reported checks are distinct from this independent source verdict. Local installation/catalogue/export/security proof remains root's normal completion work. D04 real Candidate/manager/Office contention/20s/202/login-responsiveness/same-key joined acceptance remains AUTHORED_NOT_RUN; no application or gate-holding fixture was run by this reviewer.

## Final inspected bytes

| File | Raw SHA-256 |
| --- | --- |
| 11102026_1119_banking_pay_next_daily_single_flight_adapter_v1.sql | 138323f2d728a40c1391681f301f97021d0e277f2a93f980aa4cffcafc97f9ee |
| broker/src/index.js | fcf488bc88ee955bfe560d49cc5d61a1055c5c87eab4087487e843cac731bb65 |
| broker/src/candidate-app-backend.js | d805cade15bbab1786b97ca4ffb7c346f24c07c93c0a0efdb02a92524a742c94 |
| Current0931 wrapper/source outer entries, unchanged | 7fdddc57b5f1ca6aff196567a10dd09b393185b17efdfe9b58bb766aea7e847b |
| DY-07-scope.md | 688275ab2ca4f17ddf8d24a616cdc2169aa424e0f5636f3dec6391e2909a955b |
| DY-07-boundary-check.json | c2139c7f0d9c7a9da04a5b960c307f354a132110adc8b00f0217731e9921435a |
| protected-boundary-lock.json, unchanged | 60d2d3d8a74535cf52c5213803d19b0654af48955b6096726e2b7a882545f30c |

**Verdict: scoped SOURCE PASS; exact wrapper routing, full parent preservation, bounded caller options and native pending/replay behavior are correct in current source. Joined application acceptance remains NOT_RUN.**

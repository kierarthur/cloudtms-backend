# Prepared output array forward — independent source critic

Reviewed 13 October 2026. Verdict: **P2 ADDRESSED IN SOURCE — no new definite source defect found; acceptable as an adjacent guarded forward for root composition and native qualification.** This verdict does not establish actual PostgreSQL array round-trip success, writer capture, financial acceptance, release readiness or Report A/B qualification. It is not final Part B.

Reviewed lane: `C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026`; branch `codex/healthroster-v2-report-build-13102026`; HEAD `63f7ba41030371b5340b3ba4e1da5a95e4cc531a`. The saved local origin tracking ref matched HEAD. No fetch, push or other Git mutation was performed. Concurrent separate Report A/B work was outside this review and was not edited, regenerated, restored or committed. Root remained `cea15289e0283e3383e71f0acb8b18ce184647c2`, with its existing untracked source-only verifier census preserved.

Packet: `docs/healthroster-build/proposals/report-prepared-output-array-forward/`. Exact `SOURCE_MANIFEST.json` SHA-256: `efc2881e0863a2bf09954742f8178819cb7e27d3bc30e79a56b92a6944fbd889`. Package digest: `5b6c51947afe84d5eeb9b28973ef167bbc6fab369d05fee25067feeaba9c1100`. All 11 full saved files were read; all 10 member byte lengths/hashes and the package digest were independently recomputed.

## Finding disposition and exact scope

The previous P2 concerned a valid native multidimensional `hr_crosscheck_issues text[]` inherited by the unchanged ordinary bounded writer. The frozen codec flattened the native representation into JSON arrays and required each immediate element to be null/string, refusing nested native arrays. The adjacent forward replaces only this field's encoding and validation, and adds fixed parser configuration to the two existing functions.

The complete codec uses `p_fin.hr_crosscheck_issues::text`. SQL NULL remains JSON null; a native array becomes its PostgreSQL canonical array output string. The complete codec and retained reader both accept only null/string, cast strings to `text[]`, re-encode to text and require exact equality. This supports native dimensions and lower bounds without guessing or flattening them. Native array parsing, rather than a JavaScript parser, validates rectangularity, quoted delimiters, quotes/backslashes and elements. Noncanonical or malformed text is refused; the local handlers map `invalid_text_representation`, `array_subscript_error` and `program_limit_exceeded` to the existing closed `55000/WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID` refusal. The exception is not converted to an amount or zero.

Both functions set `array_nulls=on` at the function definition. This supplies fixed parsing semantics for unquoted NULL elements versus quoted `"NULL"` strings even when the caller has `array_nulls=off`. The setting is parser configuration, not financial, actor, request or capture authority. Function SET semantics restore the caller's setting on exit; the required actual success/failure restoration proof remains held. No `set_config` or `current_setting` authority was added to either production owner, and the source's fixed `search_path=pg_catalog, pg_temp`, STABLE volatility and SECURITY DEFINER declarations remain intact.

The original capture/guard compares the entire closed financial output in addition to the unchanged whole-row JSON digest. Because PostgreSQL `to_jsonb` does not preserve array lower bounds, the complete 95-column JSON fingerprint alone cannot distinguish a lower-bound-only change. The new native array text equality preserves those bounds and can independently refuse that drift. Neither hash domain nor existing capture/trigger body was modified. Actual interval drift refusal is still a native test, not proved by source equality.

The field name, source version, 90-field closed membership, 39 numeric strings and nine JSON-text fields are preserved. The stored array transport representation changes from JSON array to native array text in this unmounted proposal; no schema migration, durable row, helper, function signature or public DTO is added. The original nullable attachment pair, historical two-NULL behavior, exact receipt integrity, scope/Candidate/version checks and original eight-key writer/ten-field marker reconstruction remain unchanged. The reader still returns only retained `total_pay_ex_vat` text; no current TSFIN amount/status, report totals, caller amounts, recalculation or unknown-to-zero fallback was introduced.

The codec's three complete functions include unchanged supported-purpose and independent hash owners. The retained reader is a complete existing owner successor. Their full reversals match the frozen original bytes, rather than merely matching selected fragments. The complete generator forward has exactly four changed lines: 41 (two fixed configurations), 71 (array field JSON kind), 74 (native array round-trip validator) and 122 (native text serialization). Replacing exactly those lines restores the entire original generator. No generator was invoked and no frozen packet was rewritten.

## Security and contract assessment

No new helper, relation, column, grant or service route is introduced by this forward. The original owner-only helper registrations and intended service_role/anon/authenticated/PUBLIC execution denial remain the source authority. Existing ordinary apply/writer/V1 receipt/context/INSERT guard/public DTO/native timing and all other owner bodies remain frozen.

The independent ACL verifier's general private-helper checks retain owner/effective-role ACL checks and require a search-path configuration to exist (`10_complete_independent_acl_verifier.sqlinc:1140–1179`); they do not require the entire proconfig array to contain only one setting. Adding `array_nulls=on` therefore has no identified source conflict with that verifier. The exact two routine configurations still require native verification: `search_path=pg_catalog, pg_temp` and `array_nulls=on`, with no extra functional settings, correct provider-neutral ownership and unchanged role denial.

Root's `supabase/release/export_contract.sql` retains every functional routine configuration except explicitly excluded `plpgsql_check.*` instrumentation. The provider-owner SQL adapter likewise strips only the exact provider instrumentation settings, not `array_nulls`. The full generated contract must consequently include this intentional two-routine proconfig delta and their changed normalized definition hashes. A passing old ACL inventory alone cannot seal those configs or prove parser restoration.

This forward does not reinstall the original fixed 239 ACL package. The original 105/239/142 was a source declaration, not an installed census. Root alone must calculate the exact combined private signature union with Q/A0 creator and every other reviewed dependency, preserve all prior entries, update central/final and independent verification and seal the complete exact composition. No final combined count or installed state is certified here. The original 2302 standalone verifier envelope review also remains unchanged: its ON_ERROR_STOP/BEGIN/ROLLBACK commands require the previously reviewed root normalization before a full outer transaction rehearsal.

## Independent preservation and source checks

The frozen original 30-file packet remains at commit `54e06721ab030e5060272ec6a0a58bf47a17587d`, manifest hash `80a2577cbf5016f1a76e323717809c5553070e47b0b9a80e11b1ef3a0af34c70`, package digest `f196d1f0b089307333c17458b7502d28a02023228caab070c384f8827cce10c1`. All 29 original member byte lengths/hashes passed again. All 15 controlling root source input hashes passed again. The complete original critic was included at exact SHA-256 `180167ebb3f3979b97056668ef5de46ee96447bc61edd85a4158f23899ef4284`.

Complete reversal hashes:

| Forward | Exact frozen reversal SHA-256 |
| --- | --- |
| Complete codec | dd735cdeabd778609eb30a4da2e852f57ba555e2fc029606966b699d14510008 |
| Complete retained amount reader | 7ed19c7ceab15a98d0e9ec75c7fd3f969d2dfa11e2625bbe873913a1889057fb |
| Complete generator | 4efb27933b4b305d15ca97c2a0b21ce40916d9b217cca1d6a59e5afe72b4012f |

`node --test array-forward.test.mjs`: **18 tests, 18 pass, 0 fail/skip/cancelled**. Four MJS files passed `node --check`. The official repository SQL scanner identified three statements in the complete codec page, one complete retained-reader statement and one datatype DO recipe. That proves statement packaging only, not PostgreSQL/PLpgSQL compilation.

The 18 cases cover full owner reversals; all original member hashes; native serializer selection; shared canonical round-trip source; fixed parser setting; six lossless JSON transports (2D, non-1 lower bounds, ordinary 1D, NULL leaves/quoted NULL/escaped characters, empty array and SQL NULL); explicit native datatype recipe; preserved 90/39/9 field census; retained 95-column JSON digest plus closed lower-bound comparison; unchanged helper/schema/writer/receipt/ACL boundary; complete generator forward; exact full critic hash; and official SQL packaging. In particular, the six JSON transport tests preserve already-written strings—they do not run PostgreSQL casts or demonstrate an actual native array result.

`NATIVE_ARRAY_DATA_CASES.sqlinc` remains **UNEXECUTED**. It creates no financial/publication/receipt rows and carries only datatype cases and restoration of its local parser setting. Even a later successful datatype DO block would not prove the two actual compiled owners, their exact configs/restoration, a valid native financial composite or a genuine capture/receipt. Root must separately exercise those owners under caller `array_nulls=off`, malformed/canonical strings, dimensions/lower bounds/NULL/quoted NULL/quotes/backslashes, unchanged 1D/NULL behavior and lower-bound-only interval drift refusal under SEND_7.

Verified new manifest members:

| Relative packet file | SHA-256 |
| --- | --- |
| 02_complete_codec_array_forward.sqlinc | eea1f99feef1117dad7913139636b28178dbc191cb41a0622f0518a259288769 |
| 05_complete_retained_amount_array_forward.sqlinc | 84512f7af655c6d84a704f55c873e00e3890965c44b5469dc4e149ce594b678f |
| FORWARD_RECEIPT.json | 833e023e8c9622a98a28d685c50f559c01832d8c0a29a6038067b6b3963b6420 |
| NATIVE_ARRAY_DATA_CASES.sqlinc | 53848d9a8c2068edd37fbf3c6b046c7156e754dd7ac9f31d5f966ee3b8b94ba7 |
| REVIEW.md | 4287ae1893442bb5f10ab4d8ff95b69ac00338cec15d81f6bfa82914641c1e73 |
| SOURCE_CRITIC.md | 180167ebb3f3979b97056668ef5de46ee96447bc61edd85a4158f23899ef4284 |
| array-forward.test.mjs | 67012bcecbeb17ef1a9354cd1e68be83c5fde085bf172f4c5feceb77f2b91a8c |
| build-forward.mjs | 9f69719bebf67801fdc22660c35314ecaf52458739510522eb31178bb471cc9c |
| build-proposal.complete-array-forward.mjs | 923c9938d303cdecc774be82fe3f58b1d0a33fe6145d0d075da54473b734ec13 |
| freeze-source.mjs | 6b376a7267291b30ebf7ee69a4ff018ed6a9f603aec33026a9a5f157bb47e1da |

## Original holds remain

| Gate | Disposition |
| --- | --- |
| Actual PostgreSQL array round-trip, fixed function config/restoration and malformed input refusal | HELD; no native array pass claimed |
| Genuine canonical producer/native writer return ID/version, normalized 90-field output, 39-decimal/nine-JSON fidelity and known zero/nonzero | HELD |
| First capture/atomic receipt, temp ownership/forgery/collision/one-use, exact enabled trigger ordering, intervening rewrite, replay/history and fresh rollback | HELD |
| Complete generated contract/proconfig/definition hashes, combined central/independent ACL, browser isolation and provider-owner mapping | HELD |
| Date/CAS/write hash, F1 timing/capability, native actor/I1/family/G1 authority and lawful native financial operations | HELD; no existing predicate/lock weakened |
| Native SourceHEAD/LOCAL active entitlement, first-installed Bank authority, C1 active/withheld joins and headless DIRECT_NEXT application | HELD; NULL/unknown retained |
| Prepared/active/withheld/authorisation/invoice provenance/settlement distinctions | Preserved in source; no current-status, COMPLETE or amount-subtraction fallback |
| Report A/B admission/rendering, zero-pay/pay-isolation and financial first use | HELD; separate concurrent proposal not reviewed here |
| Authenticated API/PostgREST/Worker route and security negatives, runtime/deployment freshness | HELD |
| Official documents/renderer, Office/MyTMS release, TestFlight and physical phone acceptance | HELD |
| Root owner documentation/progress, exact combined source backup, release evidence and final handback | Root-owned; this report closes only the focused source finding at these pins |

Only this new critic report was written. Workspace/lane AGENTS, owner SEND_5/SEND_5B/SEND_7, DATABASE_RELEASE_BIBLE, Policy X and the previously loaded Postgres best-practices skill remain controlling. No database/lab connection, native SQL execution, private credential/auth-state read, business network/API call, Bank worktree access, Git mutation, generator run, root/source/frozen edit, deploy/release, payment/provider/settlement/email/drain, app upload or phone action occurred. All pre-existing and concurrent work was preserved.

# WB-34 changed-evidence screen and attachment review

13 October 2026. Independent read-only review of BD176 and the actual draft `13102026_0926_banking_pay_next_resolution_changed_evidence_v1.sql`. Root alone writes product. This receipt covers the native refusal attachment and its screen contract; it does not claim joined runtime, SQL execution, transport implementation, rendering or A4 acceptance.

## Initial checkpoint

**DESIGN: acceptable with the implementation qualifications below. SOURCE: review in progress.** The actual six-key attachment `{request_id,candidate_id,state,mappings,facts,unavailable_code}` supersedes Root's earlier five-key proposal. `CURRENT` contains the complete original requested-row mapping and unchanged native explicit W1 response; `UNAVAILABLE` has jointly null mappings/facts and a finite plain unavailable outcome. A missing counterpart must not become an omitted row or an invented zero.

Draft SQL SHA-256: `406BAC2D7B64EBE8C4935097152599F97DD11F6821029184640BF3E72BDE2B49`.

The wrapper preserves the native SAVE/CLEAR delegate and catches only SQLSTATE23514 plus `BPAY_NEXT_RESOLUTION_STALE`, `BPAY_NEXT_RESOLUTION_POSITION_BASIS_CHANGED`, or `BPAY_NEXT_RESOLUTION_BASIS_CHANGED`. Original replay/accepted receipts and other actions remain on their original paths. Native read failures return a small unavailable attachment while the same original stale SQLSTATE/message is raised. This is a known refused outcome; it must never acquire uncertain/retry semantics merely because evidence is unavailable.

The helper independently checks service role, current active admin, native agency, active NEXT epoch and original actor/Candidate/context identity before native facts. Mapping uses original revision plus approved-line UUID, same Work/Candidate and the unique current revision/component-key counterpart. Current approval/seal/application and exact family/code qualification remain required. Explicit W1 scope has `intent:null` and `reason:null`; the retained manual rate/amount is never used to compute changed evidence. No linked discovery or fresh financial scope is admitted.

The actual SQL measures both the native attachment and its escaped DETAIL JSON string against120,000 bytes, preserving room within the existing131,072-byte error transport. The Worker/browser must also bound their complete normalized fault envelopes, validate every original row exactly once and reuse the closed W1 validator with the mapped explicit scope. A malformed, foreign, incomplete or oversized attachment supplies no current-fact authority and cannot change the original known stale error.

## Required screen behavior

- Preserve the original typed rate/amount (including£18), reason, linked consent and expanded/scroll state. The separate changed-evidence region contains only validated native facts; it cannot replace those inputs, original context, owner IDs or request UUID.
- Use the supplied SCR29b refused layout: inline refusal and a compact current-facts comparison. Keep the original input visible. Raw internal codes, physical IDs, valuation dates and SQL diagnostics are not payroll copy. Show actual quantities/used rates, separately labelled current available rates, and any genuine saved/current treatment only where the native response supplies them.
- `UNAVAILABLE` displays a plain statement that current payment details could not be shown and the original refusal remains authoritative. No empty facts table, fabricated zero, guessed replacement rate or optimistic Ready state.
- A fresh review is a deliberate staff action. Stage the complete validated mapped current context, disclose changed facts, then obtain new consent before constructing a new SAVE. It must not auto-retry the original request with fresh counters, IDs, price, linked Works or date. Unknown original outcomes continue through the existing exact journal/readback path rather than this known-refusal comparison.
- No extra destructive control or new command is necessary. Close returns focus to the same live parent; the original editor remains available while the staff member reviews a refusal. Any old scope that cannot map exactly remains unavailable.

At this checkpoint the current Worker/FE resolution modules still contain the original closed four-key command-fault decoder and no changed-evidence implementation. Their future optional attachment must be permitted only on the three exact known stale faults with `outcome_unknown:false`; all other fault shapes stay unchanged. Final SOURCE closure requires rereading the concrete closed decoder and UI consumption, not this design receipt.

References: `WB-34-build-boundary.md` BD176; `WB-34-stale-evidence-facts.md`; `WB-34-screen-state-guidance.md`; original supplied SCR29b and the previously inspected T01/T04 layouts. Supplied files were not copied or changed.

## Fresh source checkpoint after Root's native and decoder corrections

**Native attachment SOURCE PASS; closed evidence decoder SOURCE PASS; command forwarding OPEN (E34-01).** Screen consumption and A4 remain unreviewed because no final changed-evidence UI is authored at this checkpoint.

The helper now derives a closed READ scope from the original command rows, removes only command expected-owner keys and sets every intent/reason to null. This admits the same refusal comparison for SAVE and CLEAR without treating a valid CLEAR as an invalid SAVE. It qualifies both original and current native basis at the exact requested family/code before publishing mappings. Those two earlier observations are closed. Exact same-Work/current-component-key points, native approved/current/applied identity, independent actor/agency/Candidate/epoch and all-or-nothing unavailable behavior remain intact.

Static inverse comparison confirms the complete public0721 defining body is retained outside the exact SAVE/CLEAR exception branch and its two additional local variables. Private0537 is called unchanged. No native financial replay, receipt, memory/reset, retry, snooze or unrelated action branch was replaced. New helper is private SECURITY INVOKER/STABLE with direct EXECUTE revoked from public/anon/authenticated/service_role; it is reached through the existing service-only definer admission.

Worker and browser `validateResolutionChangedEvidence` blocks are text-identical after line-ending normalization. They require the exact six-key state, original request/Candidate, complete original row count and unique original identities; map only one current revision per Work; reconstruct the bounded explicit READ scope; run the existing closed W1 validator; bind original agency/actor/Candidate/epoch and current basis component key. Several distinct Standard buckets may correctly share one line. Native nullable quote/unavailable facts remain governed by the original W1 validator. Mapping component-key256-character bound matches the native1324 approved-line CHECK. Native fault classification occurs before ancillary-detail decoding; malformed detail drops its authority while retaining the original exact known refusal.

The optional fifth `changed_evidence` fault key is allowed only on the three exact stale codes with false retryable/outcome_unknown. FE send retains the typed attachment separately as `error.changedEvidence`, after original actor/current authority and complete131,072-byte/fatalUTF8 response checks. Neither mutation body, owner pair, context nor retry UUID is rewritten by this addition. The new optional accepted-handoff callback occurs before journal settlement on execute and received recovery; its broader continuation UI remains a separate forthcoming source review.

**E34-01 — essential forwarding gap:** current `broker/src/bpay-next-workbench.js` command catch still returns only the original four keys. `resolutionCommandNativeFault` already validates and assigns `error.changedEvidence`, but that value is discarded before HTTP output. Consequently the editor cannot receive the required changed native evidence. Forward only this already-validated attachment on the exact known stale false-outcome path and bound the whole normalized fault; keep every other action/fault unchanged. Reported to Root immediately. This is an incomplete wire join, not a new reader requirement.

Current reviewed SHA-256 values:

| Saved source | SHA-256 |
|---|---|
| SQL0926 | `69118FC7098D4C16AA83FD4AFFD6C5017849E25574FE578695AA866DFE08689F` |
| BE resolution.js | `19EE29D29692A3874BA09130F2429FC8FFEB2FE7670405DF644BACCF04A43793` |
| BE workbench.js | `1C4BF028CD5EF7DE2278311E3B90C837555A2EDC2B2AC00F3AFF4CCF4EC224F3` |
| FE resolution.js | `8EB279467BDB8A65EDD0B6E60316D6DE6187FF7ACD427648EC09EEDAB61B7835` |

Only read-only source/Git/text/hash inspection was performed; static textual comparisons are not runtime tests or installed catalogue proof. No native money, API, database or screen operation was invoked.

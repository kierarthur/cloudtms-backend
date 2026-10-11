# FC-14 hold-transition slice — independent source review

Current verdict: **SOURCE PASS for the hold-transition slice only.** No remaining essential correctness defect was found in the migration, recorder and six calls in five existing money owners. The later reader insertion is outside this receipt. This is not an item acceptance or execution receipt. Product, database, runtime, fixtures, tests, screens and joined acceptance are NOT_RUN by this reviewer.

Historical corrected-assembly checkpoint on 11 October 2026 (superseded by the final amendment below):

- Migration `supabase/migrations/11102026_2129_banking_pay_next_case_hold_audit.sql`: SHA256 `FC78766225019ED94AEA759B963458C47AA973454E0E65E61968406824196295`.
- Repeatable `supabase/repeatable/11102026_2129_banking_pay_next_case_audit_v1.sql`: SHA256 `5607495051D2746752A62FAA7D702A2990FCDCF7A9187B5811746D57EA3412F4`.
- Scope: full FC-14 card 1316–1337, all ITEM/D01–D07 rows, BD121 and the saved six-site census/provenance.

## Closed initial findings

1. The first recorder used `PAYE_NET`, while the native NET owner admits `PAYE_NET_ENTRY` (0559:3027). The corrected recorder now uses `PAYE_NET_ENTRY` at 2129:38–40 and selects the exact NET request actor. The earlier form would have recorded a known NET actor as Unknown.
2. The initial assembler expanded the SQL regexp's literal `$'` as a replacement-string token, inserting the file footer into the prepare function. The corrected saved file retains the original regexp and places the owner/revoke/notification/commit footer after all definitions. No load is claimed. Parent reports the fix preceded any load.

These are historical findings, closed for the current repeatable hash above.

## Independent predecessor comparison

I extracted each complete saved function definition, removed only its exact new recorder-call lines, and compared it with the declared current parent. All five match exactly after CRLF/LF normalization; no SQL whitespace, predicate, expression, guard, replay, lock or money change is otherwise removed from the comparison. Parent source hashes independently match `FC-14-successor-provenance.json`.

| Current owner | Exact parent | New call sites | Inverse result |
|---|---|---|---|
| prepare Case page | 1939 case edit, SHA `7B723A61…8308F66` | 2129:487 ACTIVE | MATCH |
| PAYE NET Case page | 0559 worker reprepare, SHA `514FB6A0…A9A6` | 2129:835 RELEASED; 855 ACTIVE | MATCH |
| cancel Case worker page | same 0559 | 2129:1138 RELEASED | MATCH |
| post Case transfer member | 0410 Case transfer post, SHA `F56898DC…793D8` | 2129:1363 REALISED | MATCH |
| release prepare Case hold | 0814 preparation discard, SHA `29F3099E…379C` | 2129:1450 RELEASED | MATCH |

Thus the saved successor bodies preserve the current economic and execution-revision logic, original early replay returns and mutation order. All six calls occur after the actual native hold mutation in the existing transaction. They do not manufacture historical ACTIVE events for older holds.

## Current checks

The migration's `(hold_id,event_ordinal)` key admits one actual creation and one actual terminal transition, with native hold/instruction/result/job/command and composite component/worker identity foreign keys (migration:5–28). Literal source and target amounts and actual hold timestamps are copied, rather than recomputed (repeatable:57–72). UPDATE/DELETE and TRUNCATE use the existing immutable trigger; its current 03102026_1342 body:188–199 rejects every operation other than INSERT. Table and recorder revoke PUBLIC, anon, authenticated and service_role access (migration:31–36; repeatable:1475–1476).

The recorder gives the exact running RELEASE/REPREPARE request priority over an original PREPARE cause, using current worker execution revision and the native hold worker/Candidate (2129:21–29). This preserves retained economic rows without attributing a later action to the original Draft creator.

## Final hold-slice amendment

Current repeatable SHA256: `E57FB6A08F7DC40351DFED238797D677481DC82AF31F4710E6650D518415B8C9`. The migration remains `FC78766225019ED94AEA759B963458C47AA973454E0E65E61968406824196295`. The full repeatable now also contains readers not reviewed by this hold-slice receipt. I repeated the five inverse comparisons on these saved bytes: all five remain MATCH after removing only the six call lines and normalizing line endings. Current sites are 2129:789, 1137, 1157, 1440, 1665 and 1752; recorder ACL is 1777–1778.

Native actor closure is complete for this slice:

- Initial PREPARE uses the exact worker/run-command/job-command relation and retained Run creator (2129:50–54), rather than a current Office session or allocation-state job.
- Running worker RELEASE/REPREPARE precedes PREPARE attribution. The current target execution revision and `(job_id,target_preparation_revision)` / `(run_worker_id,target_preparation_revision)` unique paths select its exact typed action; OFFICE means its stored actor, SYSTEM means its explicit native initiator (2129:21–29; migration0559:27–82).
- Expiry and discard use their unique native job-bound worker/request tuples (2129:30–37; expiry0501 and discard0814 schemas), with the actual OFFICE/SYSTEM distinction. They do not guess an actor from the expired or discarded original job.
- PAYE_NET_ENTRY uses the command-primary-key NET request and exact worker/Candidate, whose native Case-state composite FK retains its economic preparation/selection membership (2129:38–40; migration0380:12–29).
- SIMPLE_CANCEL uses the unique consumed EX01 proof for the actual command and CANCEL_REQUEST action. Missing older proof evidence remains Unknown; there is no new credential, auth request, actor column or guessed Draft creator (2129:41–43; migration1302:9–25).
- CSV/INTERNAL settlement uses the exact command-primary-key outcome request; optional internal actor joins all native receipt/command/transfer/worker/Candidate fields (2129:44–49; core:853–868 and migration0480:74–109).

Missing actor evidence remains `UNKNOWN` with a null actor, distinct from a proven SYSTEM action (2129:56). The request and receipt unique keys bound every attribution lookup; hold/job/prior-transition reads are native primary-key points. The three new rowtype SELECTs were changed to explicit `h.*`, `j.*` and `t.*` at 2129:12,13,60 without altering their point predicates. No source-gate allowance is implied by this receipt; exact callee registration and normal source controls remain root's required checks.

The receipt stores the actual cause command plus native job/action label; worker-action labels `WORKER_RELEASE` / `WORKER_REPREPARE` distinguish action semantics even though their existing command kind is PREPARE. Ordinal1 is an actual inserted ACTIVE hold and ordinal2 its actual terminal transition. Exact replay compares cause, label, actor, reason and time before returning; copied native hold identity and money are immutable under the original hold guard (2129:60–72; current0310 hold guard).

Security posture is explicit: owner-only table/function ACLs, SECURITY INVOKER recorder and immutable UPDATE/DELETE/TRUNCATE refusal. The new table presently does not enable RLS; no browser/service grant exists, so this review does not misstate it as exposed. Any catalogue/RLS posture requirement is to be checked by the normal protected release controls. No broad public trigger, GUC, history backfill, money event, producer, financial equation, execution identity or lock-order change was added by the six successor calls. Source PASS is limited to these verified facts; no compile, catalogue, runtime, timing or whole FC-14 acceptance is claimed.

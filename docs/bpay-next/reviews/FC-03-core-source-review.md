# FC-03 core source review — sequence 101

Independent source review, 11 October 2026, Sol 6.1 Extra High. **SOURCE PASS — all reported source findings closed; final closure and hashes below.** This receipt covers the saved1536 migration/repeatable and existing Worker command/page integration; evolving Office footer and all A4 pixels are outside this review. No SQL/RPC, business data, test, fixture, hosted call, queue, authentication, payment or commit was executed by this reviewer. Joined acceptance remains NOT_RUN.

Read the complete FC-03 card and checklist, current investigation and BD113 implementation boundary. Compared the complete0330 creator/apply,1306 financial intake and1358 page predecessors against the new saved owners. Applicable AGENTS rules and the Workers review skill apply.

## Actionable findings

| ID | Severity | Exact source and failure | Smallest required repair |
| --- | --- | --- | --- |
| FC03-C01 | P1 | broker/src/bpay-next-command-rpc.js:4–5 ACTIONS omits CASE_CREATE. The actual index handler injects this adapter, which refuses the newly allowed command before SQL. | Add only CASE_CREATE to its existing fixed financial-intake action set; retain the original endpoint, actor, signal and no-retry behavior. |
| FC03-C02 | P1 | public financial intake1536 CASE_CREATE returns an action/result envelope; broker commandResult expects the existing flat five-field receipt. A successfully accepted Create is then reported RESPONSE_INVALID/unknown, and the successful-response ENROLL branch does not run. | Return the native accepted receipt flat, as the other financial actions do. |
| FC03-C03 | P2 | Extended private accept serializes Candidate uuid::text into v_input but compares all non-money token input fields literally. A valid uppercase Candidate UUID retained by direct Preview/public input is therefore refused on Apply. | Compare the Candidate identity semantically, or consistently canonicalize that typed field throughout Preview/Create, while retaining exact entered text operands. |
| FC03-C04 | P2 | The original14-argument wrapper now serializes numeric::text into a string grammar limited to two fractional digits. Original0330 numeric=trunc(numeric,2) accepted harmless scales such as300.000 and50.000. The wrapper thus narrows an unchanged native numeric contract. | Preserve the original numeric-domain guard before canonical penny text conversion; never round a subpenny operand into validity. Keep public string grammar strict. |
| FC03-C05 | P2 | preparation pageCall applies generic normalized(args), which lowercases every UUID-shaped string recursively. A legitimate uppercase UUID in reason, optional note or original-liability reference is changed, and Office's exact original-input comparison rejects the returned token. | Preserve raw CASE_PREVIEW entered strings; normalize only typed identity fields consistently. |
| FC03-C06 | P1 | Shared terms returns v_weekly::text after the retained ceil(...)/100 numeric division. That division's trailing scale exceeds the Worker positiveMoney one/two-decimal result grammar. The ordinary principal300/weeks6 derived-weekly Preview can therefore fail closed as RESPONSE_INVALID. | Canonicalize penny-valued response text without altering native arithmetic. Ensure native default-floor/threshold/override output text also matches its strict transport grammar. |

These are source-derived failures, not claimed runtime reproductions. Root was notified immediately as each concrete integration issue was established.

## Checks already supported by saved source

- Migration is additive to the existing immutable request: nullable bounded note, MANUAL_DEBT-only bounded nonblank liability reference, bounded nullable preview token. No new money table/owner, funding behavior, protocol or historical receipt update.
- Shared terms retain the original family/tax combinations, finite positive penny operands, nonnegative threshold/floor, CREDIT repayment prohibition, Monday schedule, ceil-to-penny derivation and coverage check. Four-digit last-date range is bounded before integer arithmetic. SQL remains authoritative; browser/Worker do not derive schedules.
- Preview's page branch precedes the generic locking authority helper. It uses active-admin/NEXT/Candidate/policy plain reads and no FOR SHARE/UPDATE, receipt, clock update, ENROLL or wake. Existing-repayment warning checks native current OPEN rule/component and actual outstanding, with no fictional quantitative amount or history reconstruction.
- First Create locks native module/admin, Candidate, Candidate worker control, exact command advisory guard and current valuation policy before fresh-context comparison; the agency sequence is last. Replay compares actor/Candidate, all original operands, note/reference and exact stored token before live Candidate/channel/floor/policy checks. Accepted request remains immutable.
- Context binds the native actor, owner epoch, financial view revision, source channel, nullable Candidate default floor and valuation policy ID. NULL floor remains NULL; no zero default or conversion formula is introduced.
- Full normalized-EOL predecessor comparisons were equal:0330 apply after removing only the exact WB08 refresh call;1306 intake after removing only CASE_CREATE insertion;1358 page after removing only CASE_PREVIEW insertion. Original claim/enroller/job/funding/recovery/settlement owners are retained.
- Apply adds exact Case Workbench refresh after financial-view revision and before job/command completion in the same transaction. Original lease/epoch, terminal APPROVED receipt, approval periods and separate unfunded Advance recovery are unchanged.
- Worker request/result schemas are closed; reason/note/reference are UTF-8 bounded2048, input12288, token24576, Create32768, other financial actions2048, response128KiB, dependency8 seconds. Explicit nullable operands are required. Preview returns one bounded complete row and never mutation wake. Existing command outcome-unknown and original-ID handling remain, subject to C01/C02.
- Private helper/accept/apply execution is revoked from PUBLIC/anon/authenticated/service_role. Public service entrypoints retain service_role-only EXECUTE and active-admin/NEXT checks. No TEST bypass, public private-owner grant, new command kind or arbitrary RPC selector.

## Initial reviewed-byte checkpoint

Hashes are SHA-256 of the actual saved bytes at the initial findings checkpoint; root repairs may supersede these and require a final refresh.

| File relative to BUILD-BE | SHA-256 |
| --- | --- |
| supabase/migrations/11102026_1536_banking_pay_next_case_notes.sql | a4feb96c06df1a436378384fb504dec67ef80a33e963a7c5d4d223829adb3211 |
| supabase/repeatable/11102026_1536_banking_pay_next_case_create_intake_v1.sql | 323ec7240780f6caf6fab89c02605b37ca325a6daf8c14ce0038f741ecf3aa27 |
| broker/src/bpay-next-command.js | da695705237a5ef769d128f909cde3017fff942f860b70ecf47a59056e5c7692 |
| broker/src/bpay-next-preparation.js | 4ad1d28afae17e046d6e676edf0115731f677beab37b9da65d4f65445ac87271 |
| broker/src/bpay-next-case-contract.js | f93e3ead14d7acab3ff1d30e3e6711ac542840f09aa2fcfb26d91835a8e29a92 |
| broker/src/bpay-next-command-rpc.js | 2cca59a13f87fb4eef36d3ac2ad021568306cb698cd1c573398b3ef22115cfa8 |

No item, joined acceptance or screenshot-policy PASS was claimed by the initial pending source receipt.

## Final source closure — current saved1536 and Worker bytes

**SOURCE PASS.** Re-read the repaired saved source and exact native adapters. C01–C06 are closed; no remaining essential source defect was found in this reviewed scope. The initial findings and initial hashes above are retained as the checkpoint history.

| Finding | Final evidence |
| --- | --- |
| C01 closed | Existing fixed RPC ACTIONS now includes only the required CASE_CREATE addition. It sends the authenticated actor and exact arguments to the original financial intake using the existing signal/endpoint. |
| C02 closed | Public CASE_CREATE constructs the explicit flat five-field native receipt. Worker checks Case/command identity, accepted-pending phase and replay. The original successful command response reaches the unchanged ENROLL handler. |
| C03 closed | Token Candidate is compared as uuid; the remaining non-money strings remain exact. The public branch still binds raw entered input to token input, and accepted replay still compares the stored token. |
| C04 closed | Principal, weekly, threshold and override have raw nonfinite/subpenny guards before canonical text shaping; all four harmless numeric scales are preserved through the original private wrapper. The shared helper continues checking positivity/nonnegative domains and CREDIT restrictions. |
| C05 closed | Only CASE_PREVIEW bypasses the generic recursive string normalizer, preserving UUID-looking reason/note/reference byte content. Its closed input and exact token/input validation are retained. |
| C06 closed | Derived weekly and nullable threshold/override/floor result text is canonical penny text. Native Candidate floor is independently validated as finite, nonnegative and exactly penny-valued before truncation for display. NULL remains NULL. |

The additional Office-review consequence finding is also closed in the BE contract: returned total equals entered principal; threshold and override equal their inputs; effective floor equals the override or actual token default; supplied weekly and weeks match their native echoes. Integer-penny comparisons validate equality without deriving a schedule or allocating money. The retained first/last Monday and week-distance checks still apply.

The final complete predecessor comparison was repeated on the exact repaired SQL:0330 apply is identical after removing only the exact Case refresh statement;1306 financial intake is identical after removing only the CASE_CREATE insertion;1358 page is identical after removing only CASE_PREVIEW insertion. The new private context/terms/preview are write-free, and Preview's complete call path contains no business lock, command, receipt or wake.

Source-gate changes were inspected read-only: one exact five-field financial result constructor, one exact Preview result constructor and one named private Preview callee allowance. There is no generic constructor escape, dynamic callee allowance or broad predecessor waiver. The finite FinancialAction declaration now includes CASE_CREATE. No Worker binding, global request state, credential or platform API was introduced.

This source PASS does not establish installed catalogue state or normal application behavior. Root's initial loader preceded its source-gate completion; that ordering is not represented here as a successful final loaded review. Reviewer did not execute gates, syntax, SQL or any runtime/business journey. The evolving Office footer, real A4 images and joined acceptance remain outside this source approval and NOT_RUN by this reviewer.

### Final SHA-256 manifest

| File relative to BUILD-BE | SHA-256 |
| --- | --- |
| supabase/migrations/11102026_1536_banking_pay_next_case_notes.sql | a4feb96c06df1a436378384fb504dec67ef80a33e963a7c5d4d223829adb3211 |
| supabase/repeatable/11102026_1536_banking_pay_next_case_create_intake_v1.sql | 853ddce45455defed9f715a2593267794f3b224e469a9eaccde2aeca6e44a470 |
| broker/src/bpay-next-command.js | 8a5048b81bdaae4a6569df185e6c77a5a8cffe809b9cc5528b30222e96ae91b8 |
| broker/src/bpay-next-preparation.js | 10ae24bc8b65f95a7821c55d014768ad63ef79faf676c11a831bf935a3bb357e |
| broker/src/bpay-next-case-contract.js | 91d2bc7499d1b786ef743e5ae1a9997446d90d4fe1fe84647e84d0892b3fc135 |
| broker/src/bpay-next-command-rpc.js | 8fd79b23ed2b783d2e2459ec08cf17ecd88d7fd6aa1d4198483fa44399b00423 |
| broker/src/bpay-next-preparation-rpc.js | 642a7d989f526c821b36eb58b1bb00f32439da953bc260c35ac290601fd37c34 |
| scripts/bpay-next-source-gates.mjs | b606a7513eed581ec907922bd4c0cc6a75c4ea9bb421b9d2d82baf0940bc2b50 |

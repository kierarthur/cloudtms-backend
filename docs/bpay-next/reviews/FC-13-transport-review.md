# FC-13 transport source review

12 October 2026. **SOURCE PASS for this transport slice; no essential outstanding finding.** Root alone writes product. Reviewed the current saved Worker/Office contracts against FC-13 D04/D05/D07, BD148 (`design/FC-13.md`) and the actual 1646 SQL request, Preview, admission and terminal-receipt outputs. This is not whole-item, native-window, installation or business acceptance. All joined journeys, database first use, hosted routes, timing, tests and screenshots are **NOT_RUN** by this reviewer.

## Closed contracts and native correspondence

- `broker/src/bpay-next-case-contract.js:145–231` and `FE js/bpay-next-case-preview.js:254–340` expose the same eight-field input, raw native context, Preview/token/cursor, six known issues and terminal receipt. The nullable target Umbrella/VAT tuple is qualified by channel. Decimal money stays bounded strings; BigInt checks validate pennies and revisions without deriving valuations. Input is limited to 8,192 UTF-8 bytes; token to 32,768; Preview to 120,000. Scheduled Cases use distinct source and target terms and the retained UK effective date; Credit forbids repayment terms. Unknown authority remains null with a known issue, including honest zero-balance handling.
- `1646:261–275/326–385` is the corresponding reader ground truth: closed args, at most 100 ordered native component rows, lookahead/cursor from the last emitted ordinal, exact raw context, and the same bounded Preview/token. Worker `bpay-next-preparation.js:774–790/809` decodes only the named input/cursor and checks authenticated actor, requested Case/revision, row order/uniqueness, target-money conservation and cursor/context continuity. Office `:1134–1145` retains the existing GET reader and validates the same output. Absent `input` in the URL is restored as JSON null by the Worker schema; it is not mistaken for a selected input.
- `bpay-next-command.js:62/83/235–260/348–365` and Office `:245/274/760–805` agree on the exact five command operands and admission receipt. Full request ceiling is 49,152 bytes; the existing bounded body reader already permits that ceiling. Token input, Case UUID and expected revision must match the submitted operands. Admission binds command/Case/Candidate/revision; only Manual Debt returns the command UUID as new-rule UUID, otherwise rule UUID is null. SQL `1646:420–432/460–461` independently binds actor, Candidate, current epoch, exact input/token and original replay identity; the first acceptance recomputes the native Preview and refuses changed context.
- `1646:508–538` supplies the exact 12-field native receipt. `channel_receipt` is nullable on ordinary `CASE_COMPONENTS` reads and required only when the explicit `channel_command_id` was requested. The Worker `:386/469/779/1035–1046` and Office `:313/606/1134/1288` require its exact command, Case and Candidate binding. PENDING has no applied fields/time/issue; BLOCKED has no applied Case revision, zero component count and a known issue; CASE_CHANNEL_RESOLVED requires expected revision + 1 and positive applied count. Historical/ordinary rows do not acquire fabricated receipts.
- `bpay-next-job-step.js:64/78/163–165/234–249` accepts only the native NEW claim phase and a completed CASE_CHANNEL_RESOLVED/BLOCKED STEP receipt; it rejects a PENDING terminal result. SQL `1646:2152–2153` attaches that exact receipt to the original common job output. Existing lease/owner/result checks remain in place. Known action/page refusal maps are finite (`command.js:404`, `preparation.js:1061`); no arbitrary issue enum was added.

## Recovery, authority and routing

Office `:1033–1064` retains actor/NEXT qualification before dispatch and after reply. `:1079–1084` validates the saved action/body and sends **the original bytes**, not a reconstructed proposal. Existing controller restoration `:1565–1608` retains its actor-qualified key, bounded 65,536-byte journal and historical schema adoption; `:1636–1667` persists before dispatch, retains the unknown request after timeout/lost/malformed response, and does not clear it merely because a subsequent identical retry receives a definite refusal. The new action uses those existing paths. This verifies shared transport support only: this controller's base key is actor-qualified, while agency/epoch/native-window qualification remains the calling owner's responsibility; no new screen recovery claim is made here.

The four transport allowlists add only CASE_CHANNEL_RESOLVE: `bpay-next-command-rpc.js:4`, `bpay-next-job-rpc.js:4`, `bpay-next-runtime-rpc.js:15`, `bpay-next-financial-queue.js:29`. Their current diffs change only that literal. Existing fixed RPC destinations, awaited queue recovery, fresh CLAIM and `{id,kind}` queue identity remain unchanged. Background has no new card-specific path. The authenticated actor comes from the existing Office-user dependency (`command.js:443–448/484–489`, `preparation.js:1122–1124/1152–1157`), not the token supplied by the browser. SQL retains final actor/current-epoch authority and original accepted replay checks.

No new public route, auto-retry, browser financial step, money calculation, producer, proof/hash/signing protocol or protected-action authentication system is introduced in this slice. Existing inactive/module and unknown-result errors keep their original treatment. BD148's downstream partial-source/cross-channel preparation/NET/transfer dependency remains **BLOCKED_ON_CM-13a**; transport acceptance does not certify those consumers or tick the whole ITEM.

## Reviewed byte identities

Paths below are relative to BUILD-BE unless prefixed FE. Hashes are raw SHA-256 of saved disk contents at this source checkpoint; subsequent Root edits require a focused amendment.

| File | SHA-256 |
| --- | --- |
| broker/src/bpay-next-case-contract.js | AEA16FDE71D8F704465418B0E83A8AA9CC09B01F07E587070661A1B0D631991E |
| broker/src/bpay-next-command.js | 2AD2580B7FBF86FD53434E0D54AE48A28125607FE7508D6073897EDBF5819BF7 |
| broker/src/bpay-next-command-rpc.js | A1EE88CFC37DEAB4E20FBD109D5F4BD70740B04BDEA0004E6B505C3E3A383BAB |
| broker/src/bpay-next-preparation.js | D5015DAE2CB4E6A2337FFE99D2584A1756E05FEBD6BA795F17C1E47D7EB6985D |
| broker/src/bpay-next-job-step.js | 50729687B91E0AA9B99195CA6E50CE19E2A81A62A3EA6711C1EF320D8661D2C0 |
| broker/src/bpay-next-job-rpc.js | FD39B0999B1625A267B13CE0B394A3108300687368BF0E91D6755D25CB7B7460 |
| broker/src/bpay-next-runtime-rpc.js | 97F043498D6C8CC2030742BA59EF826F6FFC093CCBB1A937CA6FEE22644BBB84 |
| broker/src/bpay-next-financial-queue.js | CFC112F97AD79A8E209D5D644EAEF49581D7EDD487FC2003875EE5EE039C56E1 |
| broker/src/bpay-next-background.js (retained) | 7858985806B52D045EC88300946C47EBC2FA4B1DA01C7E23B57E16DBBDDE7EDD |
| FE js/bpay-next-case-preview.js | 683E30C21BA542058CA7D1D58901A37B827B76E9FE5B76E16385E221101FEBB4 |
| FE js/bpay-next-office.js | 855E05ACA32B3D8F0E439BE6EC53DF69EBAD74B09724C42AE6B910E361C4B426 |
| 1646 SQL reply-ground-truth checkpoint | E4E3B335AE66C17481AE21C57AE163DCCAB0CE8C23A3FEE4C333FF5CAC54F6F9 |

The SQL hash identifies the source used for transport correspondence, not an independent complete-SQL-batch verdict. No product files were written and no runtime verification was performed.

## New Preview fields — historical finding checkpoint

Root subsequently authored card-required Candidate-balance groups, a native conversion percentage, per-component ERNI/VAT percentages and the explicit 8,192-byte cursor bound. The earlier PASS/hashes above identify the prior transport checkpoint; these additions require the following amendment before current-source closure.

**T13-01 CLOSED (original finding retained):** the initial balance-row validator in BE `bpay-next-case-contract.js:213–215` / FE `bpay-next-case-preview.js:322–324` admitted LOAN/ADVANCE kinds but rejected native NOT_APPLICABLE tax treatment and PAYMENT_ADVANCE subtype. SQL Preview included those native component families, so an otherwise valid Candidate with an open Loan/Payment Advance would have caused the whole Preview to be refused. Root corrected only the balance-row enum; the selected channel-quote component row remains narrower. Current BE `:218–222` / FE `:327–331` matches native schema `04102026_0300:23–45/107–110`.

**T13-02 CLOSED (original finding retained):** the first Candidate-balance expression used funded principal minus realised uses for every LOAN/ADVANCE component, incorrectly reporting an unfunded payout as zero and a fully funded payout as still outstanding. Current SQL `1646:304–320` now copies the existing direction-sensitive `CASE_COMPONENTS` read expression (`12102026_1536:2521–2524`, also `11102026_1738:145–148`): RECOVERY uses funded Loan/Advance or approved other principal minus recovered/write-off; PAYMENT uses approved minus funded/paid-credit/write-off. Native schema `0300:107–110` makes RECOVERY exactly the DEDUCTION branch. This is a read of maintained native amounts, with no money-owner change.

## Current Preview amendment — SOURCE PASS

Fresh saved-source recheck closes both findings with **no essential outstanding transport finding**. Candidate balances are complete same-Candidate OPEN/PAUSED component groups by kind, subtype, instruction kind, tax and source channel. They cannot net different money kinds or recovery/payment purposes. Each group has a string amount or null plus the closed unavailable reason; duplicate groups and more than 128 groups are rejected by both decoders. This is a Candidate-scoped native aggregate, not a claim of constant physical work or a timing proof.

The whole-Case `source_pay_channels` field comes from SQL's distinct native component channels (`1646:325–326`), with at most PAYE and UMBRELLA; it is not inferred from a browser's partial page. BE `:206–222` and FE `:315–331` accept the same closed Preview keys and validate the finite channel/group arrays. Unknown principal/quotes retain their prior null semantics.

`conversion_pct` is the retained quote's ERNI policy percentage where converted taxable components agree (`1646:321–324`); differing or unavailable rates produce null. Per-row `erni_pct`/`vat_rate_pct` come directly from the same native `quote_operands`; non-taxable or unavailable quotes return null. The BE/FE `channelPercent` scalar accepts bounded nonnegative numeric strings without calculating a conversion. The percentage is policy information, not a claim that a manually authorised amount equals the automatic percentage quote. Shared money valuation remains unchanged.

`caseChannelCursor` now explicitly bounds both input/output canonical JSON to **8,192 UTF-8 bytes** (BE `:204–205`, FE `:313–314`); SQL's final reply guard retains the same emitted-cursor limit (`1646:409–411`). Full Preview/token/request/input ceilings remain as recorded above.

The schedule terms additionally expose `target_payment_week_count` and `last_target_payment_week` alongside the smaller final positive instalment. SQL `1646:78–87` retains the original native helper's nominal week count and derives the actual positive-payment count from those exact returned terms; it no longer refuses spare nominal weeks. BE `:178–194` / FE `:287–303` verifies pennies, payment-count ceiling, final amount/date and both source/target nominal spans. Credit still carries no repayment terms. This does not turn a nominal schedule into an assertion that earnings/hold/threshold/floor gates can never postpone actual recovery.

Current raw byte identities superseding only the changed earlier rows:

| File | SHA-256 |
| --- | --- |
| BE broker/src/bpay-next-case-contract.js | DE0E678903AADE1DCD2043EF6F4E88D2E95971A7773E019051794DABD7FB03D6 |
| FE js/bpay-next-case-preview.js | 90A38C4CB3C204248F3FE974934E94ED4D8A3A485A1742AA7341E3C1CE3956D1 |
| 1646 SQL native reply checkpoint | 4DE995FF3034A9E067EB55E5D7F802027AB8F1BCF7C1A73FC5092819125CADCD |

Fresh hashes of command/command-RPC/preparation/job-step/job-RPC/runtime/financial-queue and FE Office still match the earlier table. The separately authored `js/bpay-next-case-channel.js` window is another reviewer's scope; this amendment does not certify its journal/current-frame/UI implementation or A4 pixels. All prior source-only and CM-13a dependency boundaries remain in force; joined acceptance remains NOT_RUN.

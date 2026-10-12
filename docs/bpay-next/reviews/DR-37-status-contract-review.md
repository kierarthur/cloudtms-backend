# DR-37 status and transport source review

Verdict: **CONTRACT AND RENDERER SOURCE PASS — C01/C02/R01/R02/R03 CLOSED**. Independent saved-source review only; no SQL, RPC, tests, fixtures or runtime acceptance was performed. Separate current A4: SCR-60/review.md records twelve actual PNGs and 36 PASS / 0 FAIL / 6 N/A; P37-01–04 closed. Whole future host/actions and joined business acceptance remain outside both receipts.

Scope: the current Worker command/read/job decoders, mirrored native Office contracts, and authored native SQL constructors in `.codex-local/DR37-{command,dispatch,owner,status,worksheet-view,worksheet-page}.sql`. The public financial intake predecessor is the registered 11102026_2041 owner.

## Essential findings

| ID | Source | Finding and minimum correction |
|---|---|---|
| C01 CLOSED | `broker/src/bpay-next-job-step.js`, `resultInput`, NULL replacement branch | Before repair, NULL replacement metadata admitted active `FINANCE_ALLOCATE`. The migration explicitly refuses unfinished pre-protocol Case NET jobs, and the native owner only returns NULL metadata for historical DONE `PROJECTED`/`REVIEW` replay. The repaired branch requires `replay && done` and one of those two terminal phases. New replacements retain the complete typed block. |
| C02 CLOSED | Worker/Office `validNetReplacement` | Before repair, the helper accepted an active replacement with a NULL pending pointer and `COMPLETE`/`REFUSED` with a non-NULL pointer. Both repaired helpers require the exact pointer for `PRECHECK`, `STAGING`, `COMMITTING`, `CANCEL_REQUESTED`, `CANCELLING`, and NULL for `COMPLETE`/`REFUSED`. `CANCELLED` permits the exact pointer or NULL: certified NET cleanup retains it until the original cancellation consumes the certificate. Equal revisions remain valid for that certified pending case. |

Both findings were sent to the code owner and saved before repair. The repaired saved bytes were independently re-read: C01 now requires NULL metadata to be a DONE `PROJECTED`/`REVIEW` replay; C02 enforces active/terminal pointer rules identically in Worker and Office while preserving both certified `CANCELLED` pointer states. No essential contract finding remains open.

## Reviewed behavior

- Worker and Office STATUS and worksheet rows contain the same twelve added fields. Unknown extra keys remain rejected. Native UUID, nullable money and non-negative/positive signed-bigint string validators remain finite; ordered counts enforce `committed <= validated <= staged <= expected`, `COMPLETE` commits the exact expected count, and `REFUSED` commits zero.
- STATUS derives `net_pending` from the native pending pointer **or** request/projection inequality. This preserves the certified cancellation with equal revisions and prevents treating its NET job DONE as completed Worker cancellation. Current pointer identity is compared with the current replacement.
- Case admission receipts have four explicit nullable extension fields and closed stage/phase mappings. Non-NULL replacement identity is the original command UUID. Terminal `PROJECTED`, `REFUSED`, `CANCELLED` replay remains an original receipt; it does not imply a later current high-water value or overwrite accepted projection facts. A historical Case admission replay has all four extension values NULL. Plain no-Case PAYE NET keeps its original six-key receipt.
- Native command output adds the explicit four-key projection only to frozen Case PAYE NET and CASE_PAYOUT. A read-only complete source comparison, normalizing CRLF/LF only, proved that removing precisely those two additions leaves the entire original 2041 intake function identical (19,916 characters). Original action/request/actor checking and other response constructors remain in place. The native job constructor emits seven explicit nullable metadata keys; the native owner and dispatcher qualify new receipts against their exact stored replacement, original job, Worker and count tuple.
- Existing page/response bounds remain in use: job page rows are bounded by the requested 1–100 limit and replay visits zero; worksheet scope/cursor and 128 KiB response checks remain unchanged. No new queue phase or command kind is introduced by these contracts.

## Final source fingerprints at contract closure

| Saved file | SHA-256 |
|---|---|
| `broker/src/bpay-next-command.js` | `F18B320E8AB3111B87686F94122F39C5C6747B149E0A062BEC500FE5BC9B6F4A` |
| `broker/src/bpay-next-preparation.js` | `6C988A16B6AAA9C22E6D94CB4C6A1BA889F476179885168CDFFAB7FCA3738211` |
| `broker/src/bpay-next-job-step.js` | `05A0A880732EB5A51721453D0325E5855FAD830C20D5B5C217C14FF77645AA1D` |
| FE `js/bpay-next-office.js` | `755913F6FBE3DD4C33F6F7FE2F51E20C4D48C0C0D032F08D5B427BF726A1F289` |
| `.codex-local/DR37-command.sql` | `02F7C43AC9EB036992A59DE712CD75BD935F3642185C2E14713178A3CD1D77C6` |
| `.codex-local/DR37-dispatch.sql` | `E3857CEA718628B93BD4C086BA5E8AC4534A1ED0BD2489386C2F452C1043DF2C` |
| `.codex-local/DR37-owner.sql` | `9E54FC1968CDFA87A6FBB67E62491FB41F6918B0170AB42FFA7E4F128DA2C843` |
| `.codex-local/DR37-status.sql` | `DDB30AA3F437FE67F966D1AC1A99B10B42D02B39EA68D4AADB366A081DD03E15` |
| `.codex-local/DR37-worksheet-view.sql` | `B726B1F7588A6599F9424E911A0326E5ECE66FFEDCFFD384BA8B3F5AEE3C10A7` |
| `.codex-local/DR37-worksheet-page.sql` | `371A9B8CB9C7651D154FF8597F3F58F698CDE1B73A2EF28E745F2A23E04D9DF7` |

Authored source evidence only. The ignored SQL copies are not proof of installation or final registry ownership. Joined/runtime, business money/holds, future DR-22 retry and EX-27 cancellation, and A4 pixels remain NOT_RUN / owning-card work.

## Worksheet renderer extension — findings saved before captures

Reviewed saved native Office worksheet and generic accepted Worker copy (approximately lines 2226–2302 and 3044–3045), against the DR-37 card and the three native refusal codes. Office SHA-256 at this read: `C5F042B8C9BF130BAE085540E21EB605418AB9378C125D74278B4CCCE447B5DA`.

| ID | Finding and minimum correction |
|---|---|
| R01 OPEN | Retained worksheet rows after a context/read error: Enter checks `!error`, but double-click and phone Open/caret do not. `act()` clears the error before opening the retained row. Use one row-opening guard for the error and disable row controls until successful refresh; Search/Refresh may remain usable. |
| R02 OPEN | `state()` and tone give `payout_only` precedence over `REFUSED`, `APPLYING` and `REQUESTED`. A genuine Case payout replacement can therefore conceal its current refusal/pending outcome behind a neutral Payout only badge. Show the current replacement outcome first and retain payout-only as its factual qualifier. |
| R03 OPEN | Generic accepted Worker copy says `Accepted projection current for this worker.` whenever `net_pending` is false. A first refused NET has request/projection equality 0/0, no accepted projection and false pending, so this sentence falsely claims an accepted projection. Qualify it by the actual refusal and projection identity; preserve the separately displayed previous accepted projection where one exists. |

The new refusal badge/amount wording otherwise preserves accepted NET in the main cell, places requested/refused NET separately underneath, and uses the existing exact three one-sentence support messages. The generic READY row now says PAYE net not accepted only for those three codes, preserving ordinary preparation REVIEW behavior. Row click/highlight and Arrow/Home/End remain native; current single rows gain phone Open without a redundant caret. No deliberate retry/cancel control or future parent host was invented.

Fresh saved-source repair read: Office `75F8F62AEF66C0B14FD47B6E870F1D39DC3B6B93AC5836FAA8D4CABF9CBEEB73`. R02 CLOSED: outcome precedes payout-only for both label and tone, with a separate factual payout-only qualifier. R03 CLOSED: generic copy now branches on actual pending, refusal and projection identity and distinguishes no previous accepted NET from an existing accepted NET. R01 normal Open/double-click/caret/phone path CLOSED, but its expanded `Open Timesheet detail` button still needs the same stale guard: the button label list currently omits that exact label. This remaining narrow condition was reported immediately. A4 findings are saved separately in SCR-60/review.md.

Final source re-read: R01 fully CLOSED. The shared worksheet button guard now includes the exact expanded `Open Timesheet detail` label alongside Open and both caret labels, while the shared row opener rejects stale error before act can clear it. Search and Refresh remain usable. R02/R03 outcome priority and actual accepted/no-prior projection wording remain intact. Current preparation bytes restore the pre-existing mixed line endings; the finite replacement/pointer rules are unchanged. Current Office at this read: `34804A80191481FCC010D7598E734B2345EF802C19E6FE9CE99D0C6E9FA191E6`. The original finding rows above record discovery chronology rather than current open defects. Contract/source acceptance does not approve unresolved phone pixels or claim any joined financial runtime result.

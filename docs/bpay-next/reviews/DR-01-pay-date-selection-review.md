# DR-01 independent pay-date / selection source review

10 October 2026. Source-only review of DR-01 D01–D04/D09–D11 and the supporting current public intake. No SQL, tests, gates, application, database, fixtures or hosted calls executed. Product changes remain the builder's responsibility. This is not whole-card or joined acceptance.

Reviewed current files:

| File | SHA256 |
| --- | --- |
| `supabase/migrations/10102026_1621_banking_pay_next_selection_page_skip_receipts_v1.sql` | `f6f1d5b75c5ee3050e269eb38ea35403663c89e0471b6566ff458989e77bf232` |
| `supabase/repeatable/10102026_1621_banking_pay_next_preparation_pay_date_v1.sql` | `5389cbae12055cd4071d45554ebb6e5ed83f5730e3aefc011b30c9e58e2c90dc` |
| `docs/bpay-next/design/DR-01-pay-date-and-skip-receipts.md` | `255309fc296ddfb9b286f9f1162d51027e7c0d0f07518a491ae0f18d7d0c4dde` |
| `docs/bpay-next/design/DR-01-snooze-scope-followup.md` | `1813f86576376b548081197a09dc25726d6cd6bc3a8efa6b3a62beaa8e37c63c` |

Full card03:39–66, all current checklist rows, BD065 and its snooze follow-up were read against actual1590 CREATE/ID append,0570 choices/ALL helper,1236 intake/run boundary,0151 Rule B seal, core1324 page relations and1100 historical snooze helper.

## Findings requiring closure

**DRP01 — P2: a concurrent same-run CREATE can refuse its genuine original supplied date before the conflict/replay path.** At1621:28 the first SELECT cannot see another transaction's uncommitted run. At45–47 the caller's supplied date is compared with this transaction's newly derived official date before the INSERT/ON CONFLICT at50–55 waits for that transaction. A first transaction can have created the same actor/run with the prior official date while still uncommitted across a London rollover; an exact duplicate with the original supplied date then takes this fresh branch and returns `BPAY_NEXT_PAY_DATE_NOT_OFFICIAL`, although the accepted competing row will have exactly that original date. The second stored-row validation at62–64 is sound but unreachable in this case. Move the fresh supplied-date comparison to the already-present locked stored-row check after ON CONFLICT. A genuinely new wrong-date request still raises and rolls back its attempted INSERT; a winning original row is validated by its stored actor/date. Retain the first visible-row replay branch and existing expiry.

**DRP02 — P2: required controlling-repeatable schema notification is missing.** Current1621 ends with public intake REVOKE/GRANT followed directly by COMMIT (426–429). F23/B2.14 requires the exact `NOTIFY pgrst, 'reload schema';` after the final definition/ACL and before COMMIT. A notification in an older source or the loader does not satisfy the final source-owner requirement. Add the ordinary exact notification; no new object or interface is needed.

**DRP03 — preservation question to resolve: the ID-only path now rejects pending/withdrawn work with no current basis before its existing PIN REVIEW.** At146–148 every eligible ID requires `revision_worker_basis` for `current_revision_id`. The existing0570 ALL helper explicitly retains pending/withdrawn WORK for1600 REVIEW (0570:83–106); current original1590 ID append does not require an approved/current revision. A WORK with a NULL current revision previously retained its explicit ALL choice and reached the existing missing-approval/withdrawn review path, but now fails the whole upload with `BPAY_NEXT_SELECTION_BASIS_UNAVAILABLE`. The explicit-choice path already requires approved/current/applied revision and is unaffected. Either preserve this ID-only inherited admission for work that cannot qualify for a payable whole-snooze decision, keeping the fail-closed basis check for genuinely approved chosen revisions, or record a card-grounded decision that this rejection is intended. Do not alter PIN/Rule B to compensate. This needs a finite disposition rather than a claim of unchanged ID-path refusal semantics.

## Boundaries that pass this source review

- Optional date has exactly two CREATE input key sets. A visible retained run is checked before the new date helper, validates actor and stored date, returns stored `official_pay_date`, and keeps the original elapsed72-hour deadline. The date is never silently ignored. The actual helper remains the existing STABLE service-only official-date authority; no holiday calendar was invented.
- Pure text comparison of the full1236 public intake CREATE definition against1621 finds only its conditional CREATE key set and added authoritative-date receipt field. Other action branches, typed validation, server authority, segment/Case actions, SEAL_ENQUEUE and120000-byte output cap remain intact. Service-only EXECUTE, owner and search path remain closed.
- The existing1236 pay-run INSERT trigger captures `snooze_control_sequence` and the London `snooze_business_date`; UPDATE pins both. Both append owners now consume these retained values, with no second cutoff or delayed PIN refresh.
- Fresh eligibility uses the original retained London week and actual WORK week identity. Future WORK is individually skipped before member/choice/Candidate writes. New choices retain the exact original three-key request, including skipped expected revision/mode. New ID pages retain the exact original ID array. Exact page replay returns stored skips before any date, approval or snooze reclassification; changed skipped tuples conflict.
- Attempted `item_count` stays1–100. Accepted WORK alone increments selection ordinals/counts, choice/sealed counters and Candidate membership. Pages remain contiguous, original first ordinal is retained, and an all-skipped page does not fabricate membership. Both paths deduplicate native UUIDs before fresh processing, including differently cased ID spelling.
- WHOLE_TIMESHEET qualification copies the actual1100 at-cut semantics: latest event for that snooze at/before the run cut; active/expiry from retained event; exact chosen revision's immutable whole basis. It deliberately uses no current active/expiry/control filter. Segment/expense/Case snoozes cannot skip the entire WORK. Missing required run cut/basis is refused rather than assumed clear, subject to DRP03's inherited nonpayable ID path.
- The additive page fields are bounded JSON arrays; original choices length equals attempted count, skipped length cannot exceed it, and both are byte-capped. Existing private table roles remain revoked. No public table/trigger, money owner, Rule B snapshot, PIN, enroller, Source producer or parked F25 protocol is changed. Page JSON semantics are supplied by the named root-locked owner, not claimed to be independently proved by these length CHECKs. Existing terminal cleanup remains able to delete temporary upload pages.

The unchanged0151 seal and1590 enqueue still refuse zero-WORK selection with uploaded pages. The new all-skipped receipt is valid feedback, not evidence of an executable empty Draft or mixed Case-only admission. BD065 explicitly retains that WB21/DR02 sequencing boundary. WB40 whole-work hold and WB11 holiday handling remain their named future owners. Worker/Office strict transport changes are outside this SQL review; no complete DR-01 tick is justified here.

Verdict at these hashes: the bounded skip/snooze/schema/authority changes pass static source review; DRP01/DRP02 need correction and DRP03 needs an explicit preservation disposition before the SQL subset receives an unqualified PASS.

## Fresh source closure

Re-read the corrected complete1621 source and re-compared the complete public intake against1236. Final repeatable SHA256: `e8415355b5a11f2e75e0bc532f5610b920d542799b89725bba25493d1f1af641`. Migration remains byte-identical at `f6f1d5b75c5ee3050e269eb38ea35403663c89e0471b6566ff458989e77bf232`.

| Finding | Disposition at final source |
| --- | --- |
| DRP01 | **Closed.** Fresh supplied-date comparison before INSERT has been removed. After ON CONFLICT the locked actual row is checked for original actor and supplied date (47–61); a wrong fresh date raises within this transaction and rolls back the insertion, while an exact competing original row remains authoritative. Visible retained replay is unchanged. |
| DRP02 | **Closed.** Final exact `notify pgrst, 'reload schema';` follows public intake ACL and precedes COMMIT (434–435). |
| DRP03 | **Closed.** ID-only basis/snooze qualification is restricted to APPROVED work with a nonnull current revision whose actual same-WORK revision is approved and sealed (143–168). NULL revision, pending, withdrawn and unsealed work retain their original membership/ALL-choice path into the existing PIN review/refusal behavior; no PIN rewrite occurred. A genuinely approved/sealed chosen revision still refuses missing basis, and the explicit-choice path retains its original stricter current/applied check. |

The newly qualified `r.*`/`w.*` local row reads preserve private rowtype behavior. Native UUID duplicate refusal, retained page tuple and skips, future-week cutoff, historical WHOLE_TIMESHEET predicate, actual-only ordinals/counts and private/public permissions were re-read. Full intake still differs from1236 only in the two authorized CREATE lines. The four registry signatures select1621; no Rule B/PIN/enroller/producer definition is included.

**Final SQL/schema subset verdict: PASS, with no remaining concrete finding at these hashes.** This remains static source review: no compile, SQL, tests, page walk, timing, business action or runtime was executed by the reviewer. The all-skipped non-executable boundary, Worker/Office strict transport integration and later WB21/WB40/WB11 owners remain explicit; this verdict does not mark the whole DR-01 card complete.

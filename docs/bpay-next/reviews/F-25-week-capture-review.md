# F-25 actual weekly capture source review

9 October 2026. Independent source-only review. No SQL, runtime, tests, fixtures, hosted reads or product edits were executed. Full F-25 remains incomplete; this is not an installation or application verification claim.

Fresh sources reviewed:

| Source | Raw SHA-256 |
| --- | --- |
| `supabase/repeatable/09102026_2258_banking_pay_next_week_origin_capture_v1.sql` | `125c6c135aa344009a4ac9e29782e93877d1d6affb8c8fd58765b576786e96e3` |
| `supabase/migrations/09102026_2300_banking_pay_next_accepted_case_week_terms_v1.sql` | `6ff6b61e18a8f1682373b6bf92cb6c9e3ae56f7b9e822710961dd7f797c0b9af` |
| `supabase/repeatable/09102026_2303_banking_pay_next_accepted_case_terms_capture_v1.sql` | `5a75a461c083db0bd13888850f1e7a46392755c2ac5219b1527facf89fc58833` |

Initial checkpoint: changes were required. The fresh final source verdict at the end closes these findings.

## WC01 — nonexistent successor projection timestamp

**P1.** 2258 line 534 compares predecessor retirement with `p.created_at_utc`. Canonical `bpay_next_net_projection` has `accepted_at_utc` (core migration line 643) and no `created_at_utc` column. The PL/pgSQL prefix query cannot resolve this reference when prepared, so affected capture fails even if its actual request set is empty.

Use the real successor projection `accepted_at_utc`, retaining exact successor command, worker and projection-number checks. Existing case NET owner 0390 lines 500–510 inserts the projection with the transaction timestamp default and retires the predecessor with that same transaction timestamp. A guessed new column or a loose non-null retirement test would not preserve that lineage proof.

## WC02 — reverse older-run header lock order

**P2.** 2303 lines 52–54 hold the selected Candidate's worker control before 2258 line 486 takes KEY SHARE on the older pay-run header. KEY SHARE conflicts with FOR UPDATE. Existing internal settlement owner 0490 lines 173–174 takes that older header FOR UPDATE first and then waits for the same Candidate control. Concurrent old settlement and fresh acceptance can therefore deadlock: old header → control versus control → old header. The comment at 2258 lines 481–485 does not remove this conflict.

Acquire the exact older-run FK header scope before Candidate control under a consistent existing root order, then revalidate the complete bounded census under Candidate control and refuse any new scope that was not prelocked. Preserve pre-clock FK closure and the existing monetary owner's authority. Do not defer these FK locks until after the agency clock or change them to an incompatible SHARE lock under the same reverse order.

## Confirmed changes and remaining review

WD01 is corrected in 2258 lines 322–331: genuine zero OWN INTERNAL_ZERO is checked against its completed destination/group/anchor and zero own amount; unsplit settlement requires total projection cash zero. The later exact original-scope guard remains.

2300 adds an actual finite nullable case due date with an empty CASE/WEEK receipt precondition and no invented backfill. It retains an object-shaped finite economic proof, raises its byte bound to 262,144, and 2258 refuses above 245,760 before clock allocation. Existing private-table RLS, ACL and UPDATE/DELETE/TRUNCATE immutability remain inherited. 2303 captures the actual case header due date from the preheld case row.

The producer does enumerate actual earlier workers before testing contributions and keeps missing/pending economic or eligibility receipts as refusal. Final PASS awaits the concrete corrections above and completion of the remaining exact output, receipt-chain and source comparison review.

## Fresh correction review

Root's saved successors now have 2258 SHA-256 `9faeed12e29874c2c9738487d622ac15f5f5050f29b5168e21a40a4c0fd89ebc`, 2303 SHA-256 `51500fa446314a64324e8d2a42441c08791ba6d1aea3f204e14fb86cc075af05`, and 2300 SHA-256 `ad70ce3e0a7531d52209f0566412628f0c5c59a03fa63a534fab2535b3b950ef`.

**WC01 and WC02 are closed in these saved sources.** The prefix uses actual `accepted_at_utc`. 2303 prelocks and retains the real earlier-run header IDs before Candidate control; the five-argument weekly helper requires each revalidated run ID in that retained set and takes no older header lock under Candidate control. It retains pre-clock FK closure. The exact nonempty sealed selected OVERPAYMENT/GROSS_DEDUCT proof now skips unrelated weekly capture. The temporary due-date addition has been removed: current 2300 only raises the finite proof cap under an empty WEEK receipt precondition; no CASE table change remains. The old due-date checkpoint above describes superseded source, not the final migration.

## WC03 — bind a reissue return to the immediately preceding settled transfer

**P2, open source-proof gap.** 2258 lines 577–592 compare a reissue's return-cash ID with the prior return, but a RETURN of a reissue is not compared with the immediately preceding retained settled reissue transfer. The loop overwrites `v_reissue` with that return's transfer before checking the final observed contribution. The selector proves the alternating payment states, shared original and cash, but does not explicitly prove this particular settlement-to-return transfer edge. The existing owner 2018 lines 3503–3506 requires that exact `reissue_transfer_id` match.

Before overwriting `v_reissue`, require an applicable RETURN with non-null `reissue_original` to have `transfer_id IS NOT DISTINCT FROM` the retained previous reissue settlement transfer. Keep the existing alternating-state, original-transfer and cash guards. This closes the design's required immediate receipt ancestry without reading a financial counter or recalculating money.

## WC04 — narrow early predecessor header scope to weekly-affected cases

**P2, open admission regression.** The 2303 lines 50–63 earlier-header census runs for every selected Candidate before NO_CASE and the exact NOT_AFFECTED proof are evaluated. It raises at 1,000 headers. A NO_CASE or proven ordinary OVERPAYMENT/GROSS_DEDUCT request can therefore acquire a new weekly-history limit/refusal even though its later weekly helper is skipped and existing owners do not depend on those prior-week inputs.

Restrict the early prelock/cap scope to Candidates whose actual sealed selected cases require weekly capture, using the same complete nonempty NOT_AFFECTED proof and retaining fail-closed behavior for missing or contradictory identities. Later capture must reprove the selection classification under its existing owner locks. Preserve the prelock-set containment check for affected candidates.

Apart from these two remaining gaps, the full 2258 read confirms bounded actual-worker enumeration, pending/missing owner refusal, full request-number prefix, genuine predecessor retirement, exact instruction/DRAFT/NET result ID and UTF-8 digest tuples, independent economic/eligibility commands, observed contribution comparison, pre-clock typed/FK closure, append-only staged record and canonical retained proof. No new clock or financial calculator is introduced. This remains a finite source subset review; full F-25 is not complete.

## Final fresh source verdict

**PASS for the implemented weekly capture, affected-case prelock/capture and finite proof-cap migration subset. WC01–WC04 and WD01 are closed; no concrete open defect remains in this reviewed subset.**

Final source hashes:

| Source | Raw SHA-256 |
| --- | --- |
| 2258 weekly origin/capture | `3eda6eeb43021bf0c07c01f79bc4cc8a9ffa88df2d40db3f84413a6794dabdd1` |
| 2303 acceptance successor | `5464cb512af63dacd67a30f60e17893a74e423b5c6f42b0a88ff9201ce7480f0` |
| 2300 finite proof-cap migration | `ad70ce3e0a7531d52209f0566412628f0c5c59a03fa63a534fab2535b3b950ef` |

2258 lines 581–585 now require a reissued RETURN to name the previously retained settled reissue transfer before updating that identity. Existing alternating-state, original and immediate return-cash lineage checks remain. 2303 lines 54–66 limit early header prelocks to actual sealed selected weekly-affected case scopes, using the exact complete nonempty OVERPAYMENT/GROSS_DEDUCT proof through LEFT JOIN identity validation. NO_CASE and proven NOT_AFFECTED candidates acquire no prior-week header census/limit. The later locked staged-case classification reproves the same exemption, and affected capture retains exact prelock-set containment and the full worker census.

Current 2300 has only the empty-WEEK capture precondition and the object-shaped proof byte-cap replacement. It introduces no column, backfill, default, grant, owner transition, counter or other DDL. 2258 enforces its smaller producer cap before allocation, leaving room for the bounded staged typed record. Existing accepted-table RLS/ACL/immutability remain inherited. This migration's actual locked hash and local compilation are root's subsequent release checks, not evidence produced by this reviewer.

No SQL or runtime was executed. Full F-25 still has separate case/Source/book/query producer and retained-consumer work; this PASS does not mark the whole card complete.

## WC05 — removed case due-date field remains in the capture assignment

**P1, changes required for 2303 at SHA-256 `5464cb512af63dacd67a30f60e17893a74e423b5c6f42b0a88ff9201ce7480f0`.** Fresh case-prepare review found that line 305 still assigns `v_case_input.case_due_date:=v_case.due_date`. The locked 2153 accepted-case row type and final 2300 migration have no `case_due_date` field. That assignment cannot resolve for the actual row type when this capture branch executes. The earlier PASS above incorrectly treated the assignment as removed when only the temporary migration column had been removed; the 2303 capture PASS is withdrawn pending this correction.

Remove the unapplied 2303 assignment. The agreed ordinary-case due-date authority is the exact immutable original CASE_CREATE request; automatic WORK_COLLECTION cases retain their original NULL due date. 2308 already uses those authorities. This finding does not reopen the finite 2300 migration or the independently reviewed 2258 weekly selectors, and WC01–WC04 remain closed. No SQL, runtime or tests were executed. Full F-25 remains incomplete.

### WC05 fresh correction

**Closed; source PASS restored for the 2303 capture subset at SHA-256 `db987eb3eba2ba945e48bbc7ac76b5e68f2ac6ffa8122d8360c67f80a1327f7b`.** Fresh saved lines 304–306 contain retained status/revision and rule next-due capture, with only a comment naming immutable CASE_CREATE due-date authority; the nonexistent field assignment is gone. No accepted-case due-date column is introduced. The independently reviewed 2258 and 2300 hashes above are unaffected. No concrete defect remains in this finite capture subset; full F-25 remains incomplete and no runtime or tests were executed.

## WC06 — SQL alias must preserve the retained JSON field path

**P1, changes required for 2258 at SHA-256 `004031d9c041d30c113f54253b8d689ebcbc5d03841c5d3f38d30084e3492885`.** The SQL source correctly aliases the request operand `q.entered_paye_net AS net_entered_paye_net` at line 49, avoiding the duplicate name already inherited from `w.*`. Request/projection comparisons and source output now refer to that unambiguous SQL alias. However line 214 deliberately preserves the retained proof JSON key `entered_paye_net`, while line 601 was also changed to read `{net_origin,net_entered_paye_net}`. That key is absent from the emitted proof and the downstream operand becomes NULL, making lines 602–608 reject genuine retained positive PAYE payroll-net history against the real observed contribution.

Restore only the JSON field path at line 601 to `{net_origin,entered_paye_net}`. Keep the new SQL alias and its request comparisons; retain the original proof/wire key. This correction does not change the economic calculation, accepted proof contract or other WC01–WC05 closures. No SQL, runtime or tests were executed. The 2258 alias revision awaits this concrete correction; full F-25 remains incomplete.

### WC06 exact immutable-successor closure

**Closed; source PASS for the final weekly origin/capture chain.** Root retained the already compiled 2258 predecessor and created `09102026_2324_banking_pay_next_week_origin_json_operand_v1.sql`. Fresh source comparison extracted the complete 228-line five-argument stage helper from both files: they are byte-for-byte equal after replacing only `{net_origin,net_entered_paye_net}` with `{net_origin,entered_paye_net}`. The complete successor changes no other function-body statement, lock, predicate, proof, staged row, count or command authority. Its wrapper retains SECURITY DEFINER, fixed private search path, logical owner and full PUBLIC/browser/service-role direct execution revocation.

| Final source | Raw SHA-256 |
| --- | --- |
| 2258 origin and eligibility selectors; immutable stage predecessor | `004031d9c041d30c113f54253b8d689ebcbc5d03841c5d3f38d30084e3492885` |
| 2324 final five-argument accepted-week stage helper | `a17b8bb2916509dca3cbac267f0c78c1442389482226747f358b93b5de685508` |
| 2303 corrected acceptance capture | `db987eb3eba2ba945e48bbc7ac76b5e68f2ac6ffa8122d8360c67f80a1327f7b` |
| Locked 2300 finite proof-cap migration | `ad70ce3e0a7531d52209f0566412628f0c5c59a03fa63a534fab2535b3b950ef` |

The 2258 selector's new SQL request alias is used consistently in request/projection/cash/CASE_PAYOUT predicates and the emitted operand value. Its retained JSON key remains `entered_paye_net`; final 2324 reads that exact key at line 195. Reversing only those alias edits in memory reconstructs the previously reviewed 2258 SHA `3eda6eeb43021bf0c07c01f79bc4cc8a9ffa88df2d40db3f84413a6794dabdd1`, confirming the finite alias-only source delta. This was a read-only source/hash comparison, not SQL or test execution.

The saved final-definer registry assigns `private.bpay_next_stage_accepted_week_v1(uuid,uuid,uuid,uuid,uuid[])` to 2324 with 2258 as its predecessor and 2324 as the final closure (lines 12307–12318). The two SQL selectors remain final at 2258. WC01–WC06 and WD01 are closed in this final source chain; no concrete open defect remains in the finite reviewed weekly subset. Whole F-25, including the separate 1519 managed-work closure, remains incomplete. Root's reported compilation and source gates are separate evidence; this reviewer executed no SQL, runtime or tests.

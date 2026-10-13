# WB-45 independent source review

12 October 2026. Initial SOURCE checkpoint saved before further review. Root alone authors product. No SQL/RPC/business execution, tests, fixtures, hosted calls, product edits or checklist changes by this reviewer. Files remain uninstalled/unlocked at this checkpoint.

Reviewed approved card `02_WORKBENCH.md:1582–1602`, all working D05/D06/D07/D09 rows and shared-owner context, `review/R2.md:98–110,330–336`, and approved contract SHA256 `863de6469a027e4433ec2e430691d1aabd4f92dc3537882346c00f40d8310c73`.

Current source hashes:

- Migration `12102026_2044_banking_pay_next_independent_component_resolutions.sql`: `bd9aff14d525765d095919848f1daa2375b64a569a8c8327817a196b9c6b8869`.
- Repeatable `12102026_2044_banking_pay_next_independent_resolution_quote_v1.sql`: `715916235122e8f72669c6d5a69e5bd2d4a11382879fedf97b377e179151d295`.

The two named private tables, seven small basis/state/audit/ACTIVE/quote functions and guards attach only to new WB45 objects. Existing money/Source/producer owners are retained. Composite Work/revision/line FKs, NULL-safe independent ACTIVE/version indexes, immutable row economics, append-only events, deferred SAVED/CLEARED consistency and owner-only forced-RLS/revokes are present. Quote uses the original retained included-positive/snooze-excluded seam and does not replace a partial residual with full saved manual amount. Untouched mixed manual bucket units retain NULL unknown bucket source money. Fresh ACTIVE and explicit historical-version quote are separate.

Review is **IN PROGRESS**, not SOURCE PASS. Exact publication/result qualification, NEED applicability, numeric bounds and crossroutine null/currentness checks are being completed. Compile/readiness and all future WB29/runtime/application journeys remain separate NOT_RUN.

## Fresh source findings — 12 October

Reviewed migration `3a33a223a94a8bc2d532921503adeda95e9bcf6cdc1fc4b2878643b11c29408b` and repeatable `eb3c5b0cc092f708c1283b08a42f74359f9c298b80ebdc7649dbcc05c7b8c2cb` (before the subsequent CASE parenthesisation reported by root).

- **W45-S01 CLOSED:** NEED now validates sealed native Work/line/family/code and retained EX08 member/receipt identity without invoking the quotable-basis helper (`repeatable:114–131`). Missing rates/classification can therefore retain the actual unresolved need. My earlier suggestion to admit EX08 COMPATIBLE members as NEED is withdrawn: updated card04:322,325,334–335 and R4:275–277, recorded by BD114 and `WB-29-current-investigation.md:59–61`, require COMPATIBLE Ready to remain distinct from genuinely unresolved NEEDS_DECISION. No EX08 producer rewrite is required here.
- Exact unconstrained manual-rate precision/range and native quote-triplet penny/range validation are present. Original/published ACTIVE uniqueness and NEED approved-line identity are indexed. Retained quote now verifies native run-worker Candidate/channel/Umbrella/VAT and run pay date against the immutable decision (`repeatable:234–241`); no late Candidate facts replace those operands.
- **W45-S02:** quote `IF` at line218 embeds an unparenthesised CASE whose inner THEN conflicts with the PL/pgSQL IF delimiter. Root reports the narrow parenthesisation is now saved; fresh bytes still need confirmation. This reviewer did not compile or execute it.
- **W45-S03 OPEN:** mapped lookup accepts a deliberately changed published source amount/channel (`guard:90–99`, design paragraph11), but quote still compares approved Position money to the **original** basis at249 and always supplies the **original** channel to1157 at276. An untouched original £160 -> own published £192 decision is found by ACTIVE yet rejected by quote; an already-target published basis can also be converted from the old channel again. The mapping must qualify the actual native boundary amounts/channel or prove the original Position basis is preserved through publication. This is a helper/contract mismatch, not authority for an early WB29 publisher/money rewrite.

SOURCE closure remains pending W45-S03 and the requested partial-manual residual semantics check. Native installation/compile reports belong to root's separate evidence; all app/future consumer journeys remain NOT_RUN.

## Residual manual support and CASE closure

Fresh repeatable SHA `95a148488dc878eadba258c17f8d7ba0319aa9d56dd6c565f10808c574282a7d` confirms W45-S02 is **CLOSED**: line218 now parenthesises the complete CASE expression. Root reports the original2044 migration locked and local installation underway; this reviewer has not invoked those checks.

**W45-S04 — explicit remaining-basis manual amount:** card1594/1600 and R2:102–104 require exact remaining basis/treatment and forbid substituting a full saved manual quote or guessed proportion. Current1157:452 admits native `remaining_source_amount`;524–563 forbids MANUAL_AMOUNT for genuine rate-bearing rows but permits it for actual amount-led rows. An amount-led component A£160/R£120/H£0 therefore has a provable remaining £40 scope on which staff can deliberately enter a manual treatment. The current decision does not retain its save-time Position scope, and quote270–271 universally refuses non-full manual choices, so it cannot distinguish that explicit remaining-scope choice from a previously saved full-scope amount.

The smallest support is an immutable saved native component A/R/H and source-basis/boundary tuple on the same named decision, captured by the eventual original W2 save owner under the existing Work/Position locks. Quote may apply the explicit MANUAL_AMOUNT only when its exact native A/R/H/channel/included scope still equals that saved scope. Scope changes refuse; no scaling, division or full-saved substitution. Partial replacement-rate still refuses without actual remaining-unit authority. Position has composite native key `(work_id,component_key)` and retained `applied_revision_id/source_basis_channel` (`1324:328–347`), so no extra table, allocation calculator, Position identity or command is required. The card does not authorise all partial manual-rate cases merely because residual money is known. An additive successor is needed if this support is adopted after the original migration lock; the locked migration must remain unchanged.

W45-S03 remains separately open: native current position owner0016:181–198 sets its approved source from the newly applied revision, while retaining earlier realised/held source-basis semantics. A mapped decision cannot generally assume that approved money still equals its original basis. This observation is source evidence, not an executed scenario.

## Final additive-successor SOURCE closure

Re-read the complete2100 migration/repeatable and amended contract, and compared2100 against the exact installed2044 parent. **Scoped SOURCE PASS; W45-S01–S04 are closed.** No further essential source defect found. This verdict supersedes the pending statements above and applies only to the four WB45 owning deliverables, not the future WB29 integration or app acceptance.

- **S03 CLOSED:** guard2100:100–117 now explicitly requires mapped SOURCE kind/head/channel/Umbrella/component amount/used rate/charge/units and prior classification/policy facts unchanged. A distinct complete sealed physical revision may carry the mapping; intentionally different TARGET remains in the immutable decision. The unsafe £160→published SOURCE£192 example now refuses the mapping, while TARGET£192 on preserved SOURCE£160 can quote once. This matches card1595's prohibition on manufacturing earnings/debt from future treatment and removes the original-channel reconversion ambiguity. The native command/predecessor and exact published JSON checks remain. No existing publisher/Position/money owner is changed.
- **S04 CLOSED:** one bounded, NOT NULL `original_position_basis` column is added by2100, without changing the installed migration. Insert guard86–95 requires exact native Position applied boundary, approved money and the complete A/R/H/source-basis tuple, including finite realised/held operands. Existing economic immutability automatically protects the new column. MANUAL_AMOUNT290–299 uses the explicit saved amount only for actual amount-led mode and identical native saved scope with `included==residual`; A160/R120/H0 plus entered40 is accepted by source logic, while a saved A160/R0/full160 cannot be reused after R120. Partial manual-rate and narrowed/ambiguous bucket attribution still refuse; no pro-rata or inferred units.
- The initial successor diff changed only its header, decision-guard Position/publication qualification and quote's manual-scope branch. The final ACTIVE amendment below changes one additional function; four other bodies, signatures, ACLs and new-table-only trigger attachments remain exact2044 content. Registry promotes all seven existing signatures to2100. The stable quote remains advisory; the future original W2 writer must own Work/Position locks before capturing the tuple, and accepted Create must pin IDs/versions as expressly assigned to WB29. This source review does not claim those callers already exist.

Final SHA256 at inspection:

| File | SHA256 |
|---|---|
| 2044 migration (unchanged) | `3a33a223a94a8bc2d532921503adeda95e9bcf6cdc1fc4b2878643b11c29408b` |
| 2044 repeatable (unchanged installed parent) | `95a148488dc878eadba258c17f8d7ba0319aa9d56dd6c565f10808c574282a7d` |
| 2100 remaining-basis migration | `a1741e391a0dc79f1b178c1e6d5e0a11db117c5c1f87257e582ab3bceec3a400` |
| 2100 remaining-quote repeatable | `4b03f7325a0ac79d3e459cd231ee2817a53d99b7120e7a1acdf4e5cd7483ef64` |
| Amended independent residual contract | `b4ba0d3dd267a155618686d2d6ee825691fa151db7546beff00a4e109f9b7629` |
| Final-definer registry | `d47b874d400a08fa07abf9c3b2a57109f53ab21ac53d1f7388017aedfba318f5` |

No tests, compilation, SQL/RPC/business calls, data, hosted probes or product edits were performed by this reviewer. Root's installation/check evidence is separate;2100 was uninstalled when reviewed. Runtime/queue/UI and all future joined application acceptance remain **NOT_RUN**.

### Final no-decision ACTIVE amendment

Fresh complete diff and ACTIVE189–229 re-read: **SOURCE PASS retained** at repeatable SHA `54a3b81cf74d1f86f7547b8948bca3d24a48493b23ed47e9cb949f9142b2f556`, superseding the prior `4b03f732…` repeatable hash above. The input shape/family/code/target/date validation runs first; an indexed original-or-own-published exact ACTIVE-key absence returns no rows before calling the stricter quotable-basis helper. Therefore an ordinary/Source no-decision path with honestly missing used rate retains the unchanged original valuer's admission instead of acquiring a new resolution refusal. A matching decision still reconstructs and checks its exact native basis/Work and rejects multiple compatible answers. It adds no fallback decision, money or need event. Other2100 and installed2044 bytes are unchanged at the hashes above. All runtime/application limitations remain as stated.

# WB-33 saved-resolution reader source review

13 October 2026; sequence144; independent read-only server review. Full owning card `02_WORKBENCH.md:1219–1239`, exact ITEM/D01–D07 checklist rows, R2:272–274, shared W12–14, BD174 `WB-33-build-boundary.md`, current0417 parents and first0537 batch were read. Root alone writes product. No SQL/RPC/database/business execution, tests, fixtures, hosted action, browser, capture generation or product edit was performed. Joined application acceptance is **AUTHORED_NOT_RUN**; this receipt does not pass the whole item or pending UI.

## Current verdict: SOURCE PASS — SQL and finite BE/FE read contract only

Final correction review below controls the current verdict. The first-batch findings and byte table are retained as explicitly historical evidence. Native windows, A4, whole-item completion and joined application acceptance are outside this server/contract receipt.

## Historical first-batch findings — now closed

**R33-S01 — matching frozen linked owners can be silently absent.** New `13102026_0537_banking_pay_next_saved_resolution_clear_review_v1.sql:47–49` filters to positive known taxable AVAILABLE / ACTION-or-READY / CURRENT items before inspecting saved owners or accepted scope62–68. Fully RESERVED/WAITING saved linked owners therefore cannot receive the intended IN_DRAFT exclusion75–77. Widen only the bounded discovery/metadata path to native saved owners, derive the same literal family/basis/target/mode signature, then disclose matching protected owners as excluded. Do not widen eligible Clear membership. Additionally75–77 currently labels every surviving mapped linked owner IN_DRAFT **before** its signature is constructed/matched97–111; metadata qualification must precede exclusion so an unrelated frozen owner is not counted as part of the disclosed closure. Root independently identified the first filter problem; this review confirms the exact source.

**R33-S02 — per-line mapped-owner prefix has no excess guard.** ACTIVE-owner query69–72 uses `LIMIT101`. The loop immediately deduplicates73 and filters by frozen/native quote/signature before a per-query overflow refusal. Global `cardinality(v_seen)>1000`74 does not detect truncation at101: the first101 owners can have unmatched signatures while a later matching saved owner is never visited, yet148 declares `complete:true`. Refuse a physical101st owner before semantic filtering/dedup, or use an equivalent bounded materialized prefix with explicit sentinel. Do not turn an incomplete closure into a complete smaller one. Candidate item1001/1MiB check38–41 is good but does not bound this independent owner inventory semantically.

**R33-S03 — review required: linked known manual quotes currently depend on an unrelated suggested quote.** Linked eligibility113 and root signature116 require `v_row.suggestion` nonnull even when the exact persisted-owner quote89–94 succeeded and returned a known amount. Current explicit W1 separately computes suggestion and saved/manual quote (0417:241–255), so suggestion availability is not the same fact as known exact saved-owner value. Establish the actual necessary native signature operands for each mode; preserve unavailable signature facts as explicit exclusion where needed, but do not refuse a known manual saved closure merely because the alternative SUGGESTED_EQUIVALENT_BASIS quote is unavailable. This is a focused remaining source question, not a request for another calculator or owner.

## Source already sound in this batch

The new helper is private STABLE SECURITY INVOKER with fixed search_path and owner-only ACL9–11,155–156. It calls the original offer context32 and current explicit W1 rather than a locking writer, Source seed or money mutation. Root/current/applied identity33–36, bounded Candidate physical prefix before filtering38–41, native current group/item metadata/FVR qualification54–58, accepted ALL/SUBSET protection62–68 and same Candidate/Contract linkage are explicit. Actual owner mapped based-on/published revision+line and actual persisted version are inspected69–86. The final root/context output remains120000 bytes151; scope/owners intermediate bounds132 are retained. Known root amount remains native quote text with NOT_CLEARABLE/unavailable reason rather than a guessed zero126–131.

The context successor adds only the closed clear_review delegate174–176 to complete0417. The command successor adds only actual qualified family variables/check: owner qualification502–505, STANDARD→WORK_BUCKET versus ADDITIONAL/ADJUSTMENT→GROSS507–510, after original receipt-first checks379–386 and393–398. This is the correct placement to retain old accepted mixed-body replay; SAVE/readback, locks, Source preflight, complete publisher/sibling carry, CLEARED event-before-state, accepted frozen refs, result64KiB and receipt remain inherited. The supplied inverse receipt reports exact context/command restoration to0417 after stripping those additions; independent textual inverse will be recorded after latest fixes.

Captured decision selection continues by retained exact ID/version without current ACTIVE-state filtering; the new reader and family guard do not rewrite original accepted treatment or old Source earnings. Rule B/E2 current/expected Work changes continue to hold back/system-release rather than reprice an accepted offer.

## Reviewed first-batch bytes

| File | SHA-256 |
| --- | --- |
| `supabase/repeatable/13102026_0537_banking_pay_next_saved_resolution_clear_review_v1.sql` | `721e56a0285cb6cc4a2d3e4f43eb3abb4c3146f8e655897328800fa8ada70948` |
| `docs/bpay-next/design/WB-33-build-boundary.md` | `5e61fe60a29893425282820287cb37ed5335a0adf4b0a0257e332583b26fa152` |
| `.codex-local/wb33-parent-inverse.json` | `5947073144ebaad17048554d620660172dac0e765fdcf8b5c35927f175c1d10c` |
| retained0417 parent | `041126bd6ed7bf4ace370829018f46d85b16b7b95ca56fdc44c4e994bd62d449` |

SQL is reported by Root as authored/uninstalled at this checkpoint; compile/runtime/catalogue/app acceptance was not observed or performed by this reviewer. Subsequent corrections must append current source verdict/hashes and preserve this finding history.

## Final current correction and transport review

Re-read current0537 and the matching BE `broker/src/bpay-next-resolution.js` / FE `js/bpay-next-resolution.js` from saved disk after all corrections. **R33-S01 closed:** physical discovery48–50 now includes RESERVED/WAITING for metadata; actual accepted ALL/SUBSET protection63–69 is retained. Eligible membership still separately requires AVAILABLE/ACTION-or-READY102, original native currentness and known exact owner quote145–149. Frozen membership only becomes IN_DRAFT after immutable actual-owner core match79–95 and current exclusion signature match136–142; relaxed R/H/window comparisons are exclusion-only, never eligible admission. **R33-S02 closed:** local owner sentinel75–76 refuses the physical101st row before dedup or compatibility filtering77ff. Item1001/1MiB and global1000 seen bounds remain explicit.

**R33-S03 closed:** actual exact native saved quote knownness145–149 owns eligibility/root signatures; an unavailable alternative suggestion no longer removes a known manual linked owner. Source/target/mode/rates/policy/date literal comparison still includes nullable suggestion facts where genuinely present. Native amount-led matching retains original source amount/quantity, exact Position A/R/H/source-channel tuple and native quote/charge/residual comparisons131–135; the correction does not introduce an arithmetic fork.

**R33-S04 closed (reported during correction review):** previously a caught W1 Source/basis failure or stale projection could report an unrelated owner as CHANGED before proving its signature. Current79–95 positively matches the actual saved mapped immutable basis plus native stored target/company/VAT/policy/date/mode/manual operands and amount-led original source amount/quantity. Only that matching potential owner may then become CHANGED97–99 or catch168–174. Missing/unprovable native authority does not become eligible, a guessed amount, an expanded closure or a broad Candidate exclusion.

Independent text inverse of both complete inherited SQL routines succeeds against0417 after removing only the clear_review delegate, two new family variables and the actual native-family check. The command native text is30283 LF-normalised characters; no money, lock, receipt, Source seed, publisher, sibling carry, captured decision or original replay branch differs. The new family check is still after both original receipt checks and exact actual persisted owner qualification. Owner/security/search_path/ACL remain unchanged; the new STABLE invoker helper is private and all browser/service execute grants are revoked. No migration/table/public function/command/route/producer/trigger/money owner was added in this batch.

The added BE/FE clear-contract block is independently byte-equivalent after normalising line endings (5026 characters). The finite request is exactly five native fields under existing `resolution.clear_review`, canonical IDs, real WORK_BUCKET/GROSS and nullable owner UUID; existing32KiB input bound remains. Response is the original explicit W1 shape plus closed clear_review metadata, max120000 UTF8 bytes, <=100 exact owners and <=1000 unique exclusions. It first validates the original W1 context/rows, then cross-binds each owner to exact Work/revision/line/family/code, native persisted ID/version, actual target/company, same Contract and literal reconstructed signature. Duplicate actual owner/row, mismatched root/linked identity, inconsistent amount-null/reason/class, undisclosed rows and invalid linked signature refuse. Owner money/rates/versions stay native decimal strings; no browser rate calculation was introduced. Linked rows must have known RESOLVED_AND_CLEARABLE value. Excluded rows are closed native Work/revision/reason facts, not mutation targets. The existing action normaliser, controller, retained request bytes and historical receipt decoder are unchanged; new input/output dispatch is additive only.

| Final current file | SHA-256 |
| --- | --- |
|0537 SQL | `09aeff09b7e0ca6e1874920e917c987b1f75467cf8e6312d09bf8d8c60d878c8` |
|BE resolution contract | `7db49529a107fa3fe6e96da2c1c287f8bc76c7074ebed7b6505dbed362ef723f` |
|FE resolution contract | `1c01bc2a743d1a6d4489b80802ab499334294f96cd240ff57c40a5f8542e2f98` |
|BD174 build boundary | `5e61fe60a29893425282820287cb37ed5335a0adf4b0a0257e332583b26fa152` |

At this source closure Root reports0537 uninstalled. No compile, first-use business query, timing, browser, queue, rendered/A4 or application acceptance is asserted by this reviewer. Native UI and wider WB34 adoption remain separate work; preserved Rule B/E2 and frozen retained decision economics are unchanged. **SOURCE PASS for this exact SQL/read-contract batch; whole WB33 remains open.**

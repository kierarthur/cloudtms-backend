# FC-01 independent Worker source review

11 October 2026. Read-only source review of BD115, the full FC-01 card (`03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:1012–1036`), the current 1738 SQL page contract and the complete Worker delta. No product changes, tests, compilation, SQL/database calls, business data, fixtures, hosted calls, screenshots or commits. SQL ownership, maintained projection and financial source correctness are covered by the separate SQL review. D08 and the whole ITEM remain open for FC-02; joined/runtime/performance checks remain NOT_RUN.

## Verdict: corrections required

1. **ACTIVE includes applicable snoozes.** `broker/src/bpay-next-preparation.js:433` accepts an OPEN/PAUSED row in ACTIVE even when `has_active_snooze=true`; SQL 1738 `:339` returns it. The actual legacy guide `20072026_0936_loans_snoozes_sort_page_size.sql:138` requires both `not has_active_snooze` and `not is_history`. Thus the same currently snoozed Case appears in ACTIVE and SNOOZED, contrary to the retained view semantics. Add the active-snooze exclusion to SQL's ACTIVE predicate and its matching closed Worker output check. No new reader or control is needed.

2. **The new CASE reply limit differs from BD115.** The approved design says output at most120KiB, while `broker/src/bpay-next-preparation.js:782,784,787` passes/decodes/emits the existing shared128KiB maximum for CASE_LIST and CASE_SUMMARY. The current SQL conservatively reserves24576 bytes and budgets120000 for rows, so ordinary native replies should fit; the closed transport still permits a larger reply than its specified contract. Use one CASE-only maximum, at most120KiB, for the RPC options, streamed response and final wrapped body; retain128KiB for unrelated existing kinds. This is a finite bound correction, not a protocol change.

## Source checks passed

- Both kinds extend only the original GET preparation-page allowlist and fixed native RPC. Existing service/admin/NEXT SQL qualification is retained; the Worker retains its authenticated admin guard, READ route class, no-store headers and no wake. New kinds do not enter a mutation, command journal or queue path (`preparation.js:33–35,441–446,747–788`; `preparation-rpc.js:4–16`; `index.js:199021–199027`).
- The query rejects unknown or duplicated keys before the special CASE parser. Filters use a closed six-key schema, explicit defaults, nullable Candidate/Client UUIDs and finite subtype/view/snooze enums; CASE_LIST permits only six sorts and5/10/20/50. CASE_SUMMARY admits no sort or page-size operand. The normalized JSON arguments remain32KiB bounded, each cursor16KiB, and carry the exact filters, context, sort and native last key (`:303–364,441–446`).
- Cursor/output validation uses exact nested shapes, authenticated actor, unchanged continuation context, scanned-count bounds, complete/null-cursor equivalence and strict forward progress. Empty matched pages can advance the raw scan cursor. Created nulls are last; Candidate/type direction changes only the primary key; timestamps retain native microseconds and text comparisons use UTF-8 byte order matching C collation (`:366–402`). No browser balance arithmetic or recalculation is added.
- Each list row has a closed flat schema and16KiB bound, is strictly ordered against the original cursor and next raw key, and matches Candidate/Client/subtype/history/snooze filters. Known native facts include positive Case revision, typed decimal strings, nullable due/recovery gaps, frozen Client qualification, paired protected-run/channel identity and active/indefinite/finite snooze conservation (`:310–319,409–438`). Unknown subtypes require a gap and the truthful tax-unavailable label.
- Summary has precisely the nine required independent categories. Each is a nonnegative bigint-safe string, stale alone can be unknown/null, and matched/gap/category counts cannot exceed the page's scanned/matched bounds. The first five distinct type categories cannot exceed matched membership; mixed/unresolved/stale/snoozed categories are not treated as an exclusive partition (`:307–309,403–408`). The Worker returns page deltas and never claims first-page whole-register totals.
- CASE_LIST/SUMMARY use the existing bounded streamed JSON reader and abortable2s deadline. Exact55000 current-context/date/projection refusals are preserved as closed409 errors; invalid arguments, authority failures, schema absence, oversized output and dependency timeout retain the original finite error mapping. Read failures remain `outcome_unknown=false`; no automatic retry occurs (`:456–469,701–741,759–791`).
- Complete diff inspection found only the two allowlisted kinds, their isolated parser/decoder helpers, three exact error codes, the CASE-only deadline extension and actor qualification on read validation. Original command schemas, reauth/Create behavior, existing page decoders and raw fixed RPC transport were retained.

## Reviewed byte identities

| File | SHA-256 |
| --- | --- |
| `broker/src/bpay-next-preparation.js` | `b1c3442b0793119300245cd23d4746f51c7cc89f0b7001ae86caa3bbde86782c` |
| `broker/src/bpay-next-preparation-rpc.js` (retained) | `642a7d989f526c821b36eb58b1bb00f32439da953bc260c35ac290601fd37c34` |
| `docs/bpay-next/design/FC-01-implementation-boundary.md` | `d7a07f598f73338eec7fa2b97f38bea43741e0c8c4eb9550f366669dfab411e2` |
| `supabase/repeatable/11102026_1738_banking_pay_next_case_list_v1.sql` (latest contract snapshot; concurrent root SQL review continues) | `4a7f011e75a6ae02ff42e2cf85f52df639d93ca5bf8be0bb33cd5ca71cb71639` |

Re-read repaired source and record fresh hashes before source closure. This review does not mark FC-01 built or establish application/runtime evidence.

## Fresh bounded-gap amendment — pending closure

Fresh Worker SHA-256 `a006667af5b9525697d33851e6a05ada99ce812eac8ba7f44090a638c01a1666`; SQL contract SHA-256 `6f2321eb2b4405689329ac5b57deddb699b7b2d363006fba2e88a39f0cd20c50`. Both first-pass fixes are present: known active snoozes are excluded from ACTIVE, and a CASE-only120000-byte maximum now covers RPC options, streamed read and final wrapper (`:760,783,785,788`). Legitimate long labels use a reviewed90000-byte row bound; native Case principal/active-hold counters remain exact while unsupported third-component facts become nullable gaps. The following concrete issues prevent closure of those new nullable paths:

3. **Nullable snooze counters reach unconditional BigInt conversion.** Schema `:317–319` now permits NULL has-active/count/mixed/unresolved facts, but `:425–426` still invokes `BigInt(row.indefinite_snooze_count)` and `BigInt(row.active_snooze_count)` without a NULL guard. A genuine SQL third-component gap therefore throws and becomes DEPENDENCY_UNAVAILABLE. Require the entire unknown snooze tuple (has-active, active/indefinite/stale counts, until date allNULL and explicit gap) or the entire known tuple before running comparisons. Reject partialNULL combinations. A NULL summary category must similarly have `gap_case_count>0` so unknown totals have a visible native qualification.

4. **Unknown snooze membership qualifies positive filters and type totals.** SQL `:357–359` and Worker `:434–435` exclude literal false/zero rather than require a positive true/count fact. Unknown rows therefore match SNOOZED, FINITE and INDEFINITE; SQL still increments their subtype category and matched count (`:371,382`). Card `:1034` requires SNOOZED to contain only snoozed Cases. The narrow repair is literal native true/false/count qualification for affected filtered views, retaining unsupported unknown rows with explicit gaps in ALL (and applicable history). Alternatively, retaining unknown memberships inside positive filters would make every membership-dependent total unknown and change the filter contract; it is broader than the original retained semantics.

Root was notified immediately. No tests, JavaScript execution or database actions were used to identify these source defects.

## Final fresh-source closure — SOURCE PASS

The current saved Worker, SQL transport contract and amended BD115 were independently re-read. This section supersedes every pending verdict and finding above; all four findings are closed. No additional essential Worker defect was found.

- ACTIVE now requires OPEN/PAUSED plus literal native `has_active_snooze=false`; SNOOZED requires literal true. FINITE and INDEFINITE require known positive native count qualification. SQL and Worker agree (`preparation.js:440–442`; SQL `:355–360`). Unsupported membership remains visible with an explicit gap in ALL/applicable history, without qualifying positive filters or inventing their type totals.
- The90000-byte exact row bound and120000-byte CASE-only whole reply bound agree with the amended design and SQL. RPC options, streamed response and final `{ok,kind,...}` wrapper all use the latter (`preparation.js:415,767,790,792,795`). Other page/command limits remain unchanged. SQL reserves24576 bytes for the ordinary envelope/cursor and backs up an excluded output member, preserving cursor conservation.
- Unsupported membership requires jointly NULL payout/mixed/unresolved/has-active operands, NULL due/stale and an explicit gap. Its snooze counts/date are jointlyNULL; known snooze membership requires all three counts and only then performs BigInt comparisons (`:425–434`). No NULL reaches an unguarded BigInt conversion. Existing narrower due or stale qualification remains supported. Native Case principal/outstanding/active-hold counters retain nonnullable typed values.
- The four component-dependent summary categories may be NULL; any such unknown category requires positive `gap_case_count`, while the five positively known type categories retain exact bigint-safe strings. All original per-page count bounds/conservation checks remain (`:403–409`).
- Closed actor/context/filter/cursor/row identities, six deterministic sorts,100-key summary continuation,5/10/20/50 list limits, explicit empty matched continuation, 2s dependency deadline, READ/no-wake behavior and finite error mapping remain as previously reviewed. BD115 now accurately states the native context, positive filter qualification, supporting Candidate prefix-index residual cost and90000/120000 byte contract. This is source correctness, not a claim of universal index cost or measured2s application timing.

Final reviewed SHA-256 identities:

| File | SHA-256 |
| --- | --- |
| `broker/src/bpay-next-preparation.js` | `6230abfe5a0b2ef1a22265fea18770d05cd420a29104021ec186f69ca9bdc71c` |
| `broker/src/bpay-next-preparation-rpc.js` (unchanged) | `642a7d989f526c821b36eb58b1bb00f32439da953bc260c35ac290601fd37c34` |
| `supabase/repeatable/11102026_1738_banking_pay_next_case_list_v1.sql` (reviewed transport contract) | `69a732e42f0012406053d464274a84a7dadc8f932484dffaf289f9196ee7c7c1` |
| `docs/bpay-next/design/FC-01-implementation-boundary.md` | `bc8ca1e6594215791f757fb5d998377ee0dc136f26b6978b916451b0410cdb97` |

No product changes, tests, SQL/database calls, fixtures or hosted/runtime actions were performed by this reviewer. Separate SQL ownership review and root's normal static/local installation checks are outside this Worker verdict. Joined230-Case/filter/count/performance acceptance remains NOT_RUN; D08 and FC-01 ITEM remain open until FC-02.

## Cursor encoding amendment — one exact preservation fix pending

Fresh Worker `fe59dddedd7bed0cb57bea90c29cdad088cbf02ac4c315ed664652f27828ab83`; SQL `690687c8f33a724403c2ee99592345c70f98234ff7787b0b99b1185bdbc5b0e0`. The new standard Base64 encoding applies only to the ordinary cursor's nullable `last.sort_value`, preserving raw row sort keys. Worker closes the ASCII grammar/11000-character bound, canonical re-encode, fatal UTF-8 and8192 decoded-byte bound; SQL decodes/re-encodes the same operand, applies8192 bytes and checks emitted cursor<=16384 and full native reply<=119936 before returning, reserving64 bytes for the existing Worker wrapper. Comparisons correctly decode cursor values before comparing with raw full-name row keys. Previous filter/gap/actor/bounds checks remain intact.

**Exact text preservation defect:** both new Worker `TextDecoder('utf-8',{fatal:true})` cursor calls (`:325,401`) use default leading-BOM consumption. PostgreSQL `convert_from` retains a leading U+FEFF as part of a valid native name. The Worker can therefore compare a different sort value and refuse a valid continuation. Set `ignoreBOM:true` alongside `fatal:true` in both cursor decode locations; preserve the fatal stream-reader behavior elsewhere. This is a codec-only correction, with no new identity, owner, protocol or financial input. Root was notified immediately; source closure for this amendment awaits the repair.

## Final cursor amendment closure — SOURCE PASS retained

Fresh re-read confirms both scoped cursor decoders now use `{fatal:true,ignoreBOM:true}` (`preparation.js:325,401`), retaining leading U+FEFF and the full original UTF-8 sort operand. This closes the preceding codec finding. The two cursor validation/comparison paths retain canonical standard Base64, exact nullability and the finite decoded/encoded bounds; row sort keys remain raw full text. SQL's emitted-cursor16384 and assembled-reply119936 checks are present, and the Worker whole reply remains120000 including its wrapper. Prior closed filter/null/actor/summary/source-retention conclusions still apply. No additional essential transport defect was found.

Final superseding SHA-256 identities: Worker `94f7889d6f2927573bff174d0738c284fe5354b1b1830193b4f2ebb1ecb3de85`; SQL transport contract `690687c8f33a724403c2ee99592345c70f98234ff7787b0b99b1185bdbc5b0e0`; amended BD115 `72524a33a91a654bf7984db5a084e78da68bd7b7d57cb48226411857805d051d`. Retained fixed-RPC transport hash remains `642a7d989f526c821b36eb58b1bb00f32439da953bc260c35ac290601fd37c34`.

SOURCE PASS; no reviewer tests/execution/database actions. D08/ITEM and joined application/performance checks remain open/NOT_RUN.

## Final local compile syntax amendment — SOURCE PASS retained

Fresh source inspection confirms SQL `:287` now parenthesizes only the CASE expression in the context/sort IF comparison. Replacing that one exact saved statement with its prior spelling reconstructs SHA-256 `690687c8f33a724403c2ee99592345c70f98234ff7787b0b99b1185bdbc5b0e0`, proving all other SQL bytes are unchanged. Current final SQL SHA-256 is `ab912386daf210e25a2d5b6f37038b00aac8081ca0ba27e059deffdb17f47231`; Worker remains `94f7889d6f2927573bff174d0738c284fe5354b1b1830193b4f2ebb1ecb3de85`. No transport or selection behavior changes, and SOURCE PASS remains current.

Root separately reports its required12 source gates before normal reload, sole local compile, six body/owner matches, metadata10 indexes/7 guards/current isolation, and export/seal/coupling `ac7310ef…` PASS. Saved compile-only evidence includes `docs/bpay-next/design/FC-01-local-metadata.json` and `FC-01-current-owner-catalogue.json`; those are root evidence, not reviewer-executed checks or business/app acceptance. No reviewer tests/SQL/database calls occurred. Joined/runtime/performance checks and D08/ITEM remain open/NOT_RUN.

## Final documentary check — source verdict retained, one provenance hash pending

`FC-01-page-contract.json` agrees with the reviewed existing route/RPC, exact two kinds, six-key defaults/enums, full context/filter/key cursor, canonical Base64 sort operand, nine chunk-delta categories,32768/16384/90000/119936/120000 byte limits and2s deadline. `FC-01-completion.md` and provenance retain exactly D01–D05/D07 as the six backend owning deliverables, preserve D08/ITEM for FC-02 sequence117, distinguish compile-only evidence from AUTHORED_NOT_RUN app/count/order/timing checks, and claim no early screen or money-owner change. Current catalogue now includes all seven source/body/owner matches, including D01's retained `private.bpay_next_current_case_page_v1(uuid,uuid,integer)` in0585. That addition correctly supersedes the earlier six-new/current-owner count without implying a product change.

One documentary correction was reported promptly: provenance still records page-contract hash `1156ff1d856599f29af55d7aa62eca9cb37fa15c25ece77e954e00c015052f2b`; actual saved page contract is `1b3adcbacd8ab65cad0355262b06b6409b188da22415fe73c88bb456476d3434`. Product SOURCE PASS is unchanged; documentary closure awaits only that exact hash repair. Reviewed completion SHA-256 `a59c2abb4959acbedd9ace80f85f0a3ed38988b065169dcb548cd6352d04b10b`; seven-owner catalogue `88dc94e9c586654f7a1280c07ce04d0627361a0591e15582583340fd0edaebe6`; pre-repair provenance `e9892f7d560e5284ad07e738565df6917e757af35f5bcf245ad23350edaf5728`. No reviewer product changes, tests or database calls.

**Final documentary PASS:** fresh saved provenance now contains the exact current page-contract hash. All five recorded source SHA-256 entries (migration, repeatable, Worker, source-gate script and page contract) match their current saved files. Final provenance SHA-256 `b0f7fb802775fd748697117bc7d4e6f05e45f79c02298ec82eed58630175d97e`. The stale hash finding is closed; SOURCE PASS retained, six backend owning deliverables documented, D08/ITEM still open and app/runtime/timing AUTHORED_NOT_RUN. This amendment uses file reads/hashes only; no product changes or execution.

# WB-29 frozen rate projection — actual transport map

13 October 2026. Read-only investigation for the saved `WB-29-frozen-rate-projection-join.md` design. Root owns product. This names actual modules and minimal supporting seams; it is not concrete product approval or whole WB-29 completion. No product edit, SQL/RPC/database, test, fixture, browser or hosted action was performed.

## Existing routes and owners

| Existing path | Actual owner and fixed native reader |
|---|---|
| POST `/api/banking/pay/next/frozen-detail` | `broker/src/index.js:199076` calls `handleBpayNextFrozenRead` from `broker/src/bpay-next-frozen-read.js`; `broker/src/bpay-next-frozen-rpc.js:4` supplies the raw Response from the fixed `public.bpay_next_frozen_detail_page_v1` endpoint. |
| GET `/api/banking/pay/next/advice/:instruction_id` | `broker/src/index.js:199108` calls `renderBpayNextFrozenAdvice` from `broker/src/bpay-next-frozen-advice.js`; `broker/src/bpay-next-frozen-advice-rpc.js:13` supplies the raw Response from fixed `public.bpay_next_original_csv_advice_page_v1`. |
| Office frozen detail | FE `js/bpay-next-office.js:1086`, `createTransport(...).frozenPage`; existing frozen child reads through it at 1963 and controller delegates at 1696. |
| Office original advice | FE `js/bpay-next-office.js:3157`, `printAdvice`, streams the same native HTML route. It does not decode advice JSON or call a second frozen endpoint. |

No new route, RPC, command, module or generic framework is necessary. The two fixed RPC adapter request sets remain unchanged: RATE_DETAILS sends its original actual line UUID, bucket cursor, page limit and server-supplied actor; advice additionally carries its original instruction UUID. Existing PREVIEW_PAGE route class, service transport, active-admin/NEXT checks and each native reader's subject proof remain unchanged.

## Current envelopes and precise extension

Standalone native0620 envelope is exactly `{rows,complete,origin,used_line}`. BE frozen-read:378–395 requires these four keys, validates the captured origin and RATE_DETAILS used-line object, retains original rows, and emits exactly `{ok,kind,rows,complete,origin,used_line,next_cursor}`. Add mandatory `selected_treatment` only when kind is RATE_DETAILS: native five keys and HTTP eight keys. The property is either the full qualified object or explicit NULL, including empty final pages; missing is not interchangeable with NULL. Every other kind retains its current key count and values.

FE Office:1089–1113 requires seven HTTP keys, validates FROZEN_ORIGIN/FROZEN_USED_LINE, removes those metadata keys and passes the five-key page to `shapePage`. RATE_DETAILS must instead require eight keys, validate/remove `selected_treatment` alongside origin/used_line, then use unchanged page/cursor checks. Preserve the same exact request shape, actor/NEXT/current-controller checks, bounded read, failure classification and read-only outcome semantics. The existing frozen child verifies origin revision equals the selected line and original origin metadata stays identical between reads; add exact selected-treatment identity checks against that selected line when non-NULL.

Original native2018 advice envelope is exactly `{rows,complete}`; add `selected_treatment` only for its RATE_DETAILS branch, giving three native keys. HEADER/WORKS/MEMBERS and all other detail kinds retain two keys. Advice is already server-rendered HTML, so Office has no new advice JSON validator or asset to add.

## Necessary existing advice adapter correction

Current `broker/src/bpay-next-frozen-advice.js:198–208` delegates its detail RPC to `handleBpayNextFrozenRead`, which now requires native origin/used_line. That advice RPC returns the native two-key2018 envelope. In addition, advice's existing post-delegate exact check accepts only five returned keys, while standalone frozen-read currently emits seven. These two retained assumptions disagree before any selected-treatment addition. This is source evidence, not a runtime finding.

The smallest supporting fix is to reuse the existing frozen-read `pageRows`/`nextCursor` validation inside the **same existing module** for an exact original-advice envelope, and call that shared static validation from advice's original instruction-scoped detail adapter. A small exported detail-page validation entry in frozen-read is sufficient; no new module, alternate browser route, fake captured origin/used_line or READY guard is justified. Keep its original schema/endpoint-pair/order/decimal checks; explicitly select native two-key advice / three-key RATE_DETAILS advice envelopes under the existing internal instruction caller. A validated internal advice result may retain its current five keys plus selected_treatment only for RATE_DETAILS. Standalone keeps its own four/five native and seven/eight HTTP shapes.

Do not weaken public standalone validation into an arbitrary optional-envelope union, infer advice from a missing origin, or fabricate origin from today's Work. Advice admission is the existing original instruction/transfer/member/line proof and intentionally survives retirement/current status; standalone retains its original READY/run REVIEW-or-DRAFT gate. Shared row validation is not shared status authority.

Advice's `page()` at167–194 validates HEADER/WORKS/MEMBERS native envelopes and retains page timeout/byte bound. Its `detailRows()` at210–218 currently yields rows only and drops envelope metadata. The actual used-rate loop at347–355 therefore needs a narrow page-aware RATE_DETAILS path: validate/retain/render selected metadata once per original line (even when there are zero Standard source rows), verify identical metadata across continuation pages, then preserve original rate row count against `line.expected_rate_detail_count`. Other detail loops are unchanged. Do not add a separate metadata fetch or per-bucket fanout.

## Closed selected-treatment validation

Use the exact object/bucket keys from the saved projection design, not a recursively permissive quote object. Enclosing identities: actual run_line/run_work/run_worker/Candidate/captured_revision/approved_line/component; source/target channels and joint Umbrella/VAT target; frozen valuation policy/window/date; once-only Source consumed; component ex/VAT/inc; buckets <=5, unique actual family/code. Bucket keys are exactly family/code, decision ID/version, treatment/mode, original approved units/source rate, Included units/attributed Source, nullable manual operands, target quote ex/VAT/inc/rate and matching policy/window.

Required interfield checks:

- Match non-NULL metadata line UUID to the requested actual run_line_id; source revision/component/worker/Candidate/policy/target to already validated native frozen line/context where available. Advice additionally checks its original HEADER worker/Candidate/Run target and MEMBERS line/run-work/revision/component and frozen target amounts. No browser identity is newly authoritative.
- PINNED has a jointly non-NULL exact decision UUID/positive bigint text version; NATIVE_DEFAULT has a jointly NULL pair. Never require current ACTIVE state. Saved modes/manual operands are closed and mutually exclusive, not inferred from units or amount.
- Every bucket policy/window equals its enclosing frozen context. STANDARD uses the original five native bucket codes; ADDITIONAL and ADJUSTMENT preserve actual native code, never force DAY or the rate-schedule family list (which includes MILEAGE for a different purpose).
- Monetary ex/VAT/inc and once-only component Source are nonnegative native decimal text; validate penny conservation using exact textual/scaled-BigInt validation, never Number/epsilon or browser repricing. Original rate/unit values preserve genuine NULL. Per-bucket attributed Source may be NULL for an honest intact all-manual mixed basis; do not fill it with component Source or insist sum(NULL/diagnostic fields) equals Source.
- Actual zero quote retains ex/VAT/inc zero and quote rate NULL. No payable rate/paid units are inferred. Original approved-zero Standard rows may lack a composed bucket; only exact native family/code matches explain a displayed row.
- No current available-rate object, live Source/Position, decision reason, private members or prefix_targets are permitted in this response.

## Actual canonical decimal reuse

BE frozen-read `field`:144–180 already has decimal text for native numeric(18,6) cells (12 integer digits/6 fractional), detail money (16/2), capturedDecimal (bounded unconstrained native decimal text) and canonical UUID/date/code checks. FE Office has matching `detailDecimal` at694, `detailMoney`, `capturedDecimal` at701 and FROZEN_* schemas at504–516. These existing source row validators remain unchanged.

The existing W1 module `broker/src/bpay-next-resolution.js` and FE mirror `js/bpay-next-resolution.js` already implement closed object checks, positive bigint version text, target PAYE-versus-Umbrella tuple, native family/code identity, mutually exclusive modes/manual operands and exact `scaled`/`cents`/zero validation. Its `quote()` validates exact money and penny identity; native selected quote rate already uses bounded decimal text rather than Number. These are concrete reusable validation patterns, not a reason to run W1/live context validation or reuse its whole response/Position assumptions. Frozen captured bucket source/null/prefix quantities differ from W1; only exact common primitives/patterns apply. Native SQL field precision must determine each new decimal bound; do not use Office's general MONEY (up to6fraction) for frozen penny totals, or reduce an unconstrained native quote to12integerdigits because the original source RateDetail happens to be numeric(18,6).

Original advice's `decimalEqual` at108–110 normalizes textual representation for equality without pricing; its field validators support native decimal text. Preserve escaped output (`escape/display/money`) for all rendered source/selected cells.

## Bounds, cursor and recovery

Frozen-read limits remain request2048, response128KiB, rows<=100, timeout8seconds; native public reply stays <=120000 including the full selected metadata once. Advice retains page128KiB/8seconds and chunk16KiB/backpressure, one page per nested level and original terminal complete/failed marker. FE's existing frozen read timeout/response bound remains original; no broader budget or captured8MiB transport is introduced.

Selected buckets <=5 are metadata, not source rows or continuation keys. Native source bucket order and `after_bucket`/complete/empty-final behavior remain unchanged; no target sort or new cursor. Validate the full frozen object before processing each source page and reject inconsistent later metadata rather than use fresh live facts. NULL/no-ref branches preserve original rows and do not invoke captured composer. Nonempty attachment qualification failure is an existing typed read refusal, not silent original-rate fallback. Retain existing frozen dependency/forbidden/not-found/timeout codes; add a finite named qualifier mapping only if actual SQL introduces a code. Read failures must remain read failures; no mutation unknown/retry journal is changed.

## Source hashes at investigation

| Actual source | SHA-256 |
|---|---|
| BE frozen-read | `79e2a9b16e1d09d879d121e011cee10f88ee2adff535fb0f39d1936a2bc217e3` |
| BE frozen-rpc | `6ddbc1ecb64873714e9a9c9a05bc4fcb2dae5d7fca95236a4f75976594a4e83c` |
| BE frozen-advice | `010d06386af89983e48c63a3eafcc2796eba929f0aa74def33202157b55c00c4` |
| BE frozen-advice-rpc | `acf94f66927911c3252f2f59d5e4bc4ab2ec4ae3d7e550e4d4d0100b8bf80549` |
| BE existing resolution validator | `f07c8e08474d5c01708658bcb5eb102ee70d80c8661826f6bae7d5deaf1845ee` |
| FE Office | `855e05aca32b3d8f0e439be6ec53df69ebad74b09724c42ae6b910e361c4b426` |
| FE existing resolution validator | `4457b1935f13f1cb5b2ede9804c72719ceae6fc1e88213717dfb476e5fff10ba` |

Actual index route locations were read without editing the large file. This map also discloses authorship of the preceding SQL projection-join recommendation; its independent design review remains separate. All concrete successor/transport source review and joined business acceptance remain pending/NOT_RUN.

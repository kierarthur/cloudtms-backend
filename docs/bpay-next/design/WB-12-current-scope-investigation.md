# WB-12 current scope investigation — 10 October 2026

Read-only seq60 investigation. No product edits, tests, SQL/DB/runtime/app/data/fixtures/hosted/network calls. Root alone writes product. Source inventory is not installation or screenshot acceptance.

## Exact scope and current authority

Read full approved card `02_WORKBENCH.md:499–539`, all eight WB12 ITEM/context/deliverable rows in the BUILD checklist, seq60/dependencies and master Workbench chain, R2-F06/F09 plus exact R2 WB12 replacement, current final-definer registry and native summary/read schemas. D06 `Workbench Ready candidate table in js/bpay-next-workbench.js` is the sole physical deliverable; D01–D05/D07 are explicit buildTask:false contexts, not seven duplicate rewrites. Dependencies WB09/WB11/F16 are current source. WB15 Candidate Banking child, WB17 Timesheet viewer, WB19 command/progress, WB21 actual Create, WB22 persistent choice memory, WB24 whole child membership and WB28 reset retain later ownership.

Owner A3 supersedes Previous/Next main scrolling, single-click opening and shown pay date: continuous bounded chunks; single click highlights, double-click/Enter opens and phone Open; no pay date before execution; dark even device-light; all visible data headers server-sort except actual finite exclusions. The parent explicitly supplies latest E6 deductions-only tick disabled/count skipped, and F/SCR160 uses this card's exact server count with existing HEADER, not another kind. Current saved BUILD_LOG:610–616 summarizes these directions. Exact local screen policy SDP47:202 and T01_ready_list reviewNotes confirm a deductions/repayment-only Candidate shows “Nothing to pay this week”, remains visible with tick disabled, is excluded from Candidates/Selected/Create count, and its debt remains outstanding. SCREEN_INVENTORY:88 names SCR160 title count from WB12 and assigns the actual dialog to WB21/DR01. No mockup Updating/Failed/rate chips may be invented: the spec explicitly assigns per-Candidate progress to WB19.

## Current physical winners and reused facts

| Exact object | Current final physical source | Relevant evidence |
|---|---|---|
| `private.bpay_next_workbench_kind_page_v1(text,jsonb,jsonb,integer)` | `supabase/repeatable/10102026_1634_banking_pay_next_workbench_read_limits_v1.sql` | common HEADER/CANDIDATES typed projection, all native sort/keyset/order/byte envelopes |
| `private.bpay_next_workbench_header_page_v1(jsonb,jsonb,integer)` | same1634 | SQL wrapper calls common HEADER |
| `private.bpay_next_workbench_candidates_page_v1(jsonb,jsonb,integer)` | same1634 | SQL wrapper calls common CANDIDATES |
| `public.bpay_next_workbench_page_v1(jsonb)` | same1634 | service-only active-admin/NEXT/STABLE5s, adopted/current barrier and full context |
| `private.bpay_next_workbench_group_contribution_v1(text)` | `10102026_1332_banking_pay_next_withdrawal_projection_v1.sql` | native selectable payment/deduction counts; no money rewrite needed |
| `private.bpay_next_workbench_apply_delta_v1(private.bpay_next_workbench_delta[],private.bpay_next_workbench_delta[])` | `10102026_1252_banking_pay_next_workbench_summary_v1.sql` | sole natural summary delta/current candidate count; reuse |
| metadata/set-bank-warning | `10102026_1429_banking_pay_next_workbench_current_continuation_v1.sql` | actual Candidate name/ref/bank state current metadata adoption |
| refresh_work/core | `10102026_1438_banking_pay_next_workbench_origin_projection_v1.sql` | bounded actual current complete membership; no producer change |
| `GET /api/banking/pay/next/workbench-page` | `broker/src/bpay-next-workbench.js` + exact index seam | existing strict closed decoder4s/128KiB/read-only/admin/environment/exact fence exemption |
| actual FE mounting | `js/bpay-next-workbench.js`, `js/bpay-next-office.js`, standard adapter/main | WB11 Ready host + shell context/filters/refresh callbacks; reuse actual standard frame |

Reader chain common1520 →1601 (eight genuine issue amount fields) →1634; other wrappers/public1520 →1634. Any genuinely needed WB12 successor composes the current1634 body and preserves1601 issue projection, ten kinds,5s/service ACL/barrier, exact native cursors and 120000-byte preassembly envelope. Future WB15/16/18/19 remain subsequent named reader editors; do not replace the reader from old1520 or add a second public reader.

## Existing CANDIDATES semantics — use rather than rebuild

1634:50–98 reads exactly one `(client_scope,pay_channel,section)` Candidate-summary natural universe. No Client uses client_scope ALL; Both uses pay_channel ALL (one combined stored amount, not two rows/sums). Effective section is READY in public:597 and both Worker/FE normalization. Actual current READY row predicate is **`s.payable_count>0 OR s.deduction_item_count>0`** (:52–53), not item_count. This already admits selectable payment Candidates and real deductions-only explanatory rows. Do not remove the latter: E6 requires disabled/excluded counts, not hiding its debt.

Projection: candidate_id, candidate_sort_name, tms_ref, bank_warning_code, metadata/projection revisions; exact integer-text item/contribution/segment/payable/selected/snoozed/held/queried/blocked/deduction/unknown counts; nullable amount families amount/waiting/ready NET add/gross and NET deduction/approved/paid/reserved/excluded/recovery/selected; has_deduction derives actual deduction_item_count. No current “resolver rate set” or per-Candidate Updating/Failed field exists. Display only facts supplied. Stored `selected_count/selected_amount` describes accepted native reservations, not local ticks; never label it the user's present exact selection.

Current server sorts are NAME, AMOUNT, DEDUCTIONS asc/desc. NAME uses actual candidate_sort_name COLLATE C, ref-null flag/coalesced ref, native UUID; AMOUNT uses unknown flag/exact numeric amount/native UUID; DEDUCTIONS uses actual deduction boolean/name/ref-null/ref/native UUID. Direction applies to every tuple key. Worker and FE independently validate row key order, full cursor/page_receipt/context and unknown amounts. Bank warnings are informational; do not disable earned pay based on a warning.

## Confirmed supporting gaps — finite reader/caller work only

### 1. Whole Ready Candidate count currently differs from its selectable universe

Common HEADER:41–47 uses scope_summary.candidate_count or mere existence of Candidate-summary row. Current delta1252:117–126 updates that count when **item_count** changes across zero, whereas group contribution counts selectable payments at:61 and selectable deductions at:67. A retained RESERVED-only/current certified-zero explanatory group can therefore contribute to HEADER READY Candidate count while CANDIDATES does not list it. E6 additionally requires a listed deductions-only Candidate to be excluded from Candidates/Selected/Create count. The table's positive Candidate control must use `payable_count>0`; `payable_count=0 AND deduction_item_count>0` remains visible/disabled with the required copy, no inferred financial collection.

Smallest source support: keep the existing HEADER kind, derive its READY count from the same natural Candidate summary scope with positive `payable_count`, and apply bounded exact local Candidate overrides there. Do not change the sole projection delta or build another money/producer authority solely for a screen count. An index for this natural READY/payment predicate may be necessary if reader uses a scope aggregate; current PK and section/channel/client indexes already give the exact universe. Count scan remains5s/byte-bounded; do not claim O(1) from an aggregate or invent a total cap.

### 2. Local override count is missing from HEADER's closed contract

Current public input has exactly13 keys (:543–547); no excluded Candidate identities/mode are accepted. Worker/FE COMMON mirrors11 browser arguments and strict context, and HEADER rejects nonempty search/after. Reusing current raw selected_count cannot count “All except N” local Candidate intent. Supporting a count inside HEADER requires a small explicitly typed optional/defaulted Candidate-only intent (e.g. bounded unique lowercase native UUID exclusions, or ALL/NONE with bounded exceptions), mirrored in SQL → Worker → FE → scope context/cursor validation. Root should choose one finite representation and record its exact envelope. Preserve legacy default behavior for ordinary HEADER reads and refuse intent on other kinds unless genuinely required; input remains under32KiB and no money amounts may be supplied. Exact count is server-owned over complete eligible current scope and must bind the same actor/agency/epoch/date/workbench/adoption context as the table. This is a read projection, not CREATE, a queue protocol or a new financial kind.

Between clicks retain bounded IDs/intent and show “Selected: All except N excluded” (or truthful NONE/overrides wording); do not call the server on each checkbox to calculate money. Obtain exact Candidate count through this existing HEADER support when the named confirmation/estimate caller needs it. Do not implement SCR160's CREATE dialog early under WB12; expose the finite count capability/intent to its named WB21 owner. Intent applies to complete eligible Candidate membership, not loaded rows; later WB24 joins child overrides and WB22 persists the actor namespace.

### 3. A3 visible TMS/bank data sort requires missing keys, not browser sorting

TMS ref and bank warning are real typed fields but cannot currently be selected as server sort keys. If shown as data columns under current owner A3e, add finite TMS_ASC/DESC and BANK_ASC/DESC (or exact root-chosen names) only for CANDIDATES, with truthful null placement and native UUID final tie-break. Suggested tuples: TMS `(tms_ref IS NULL,coalesce(tms_ref,'') COLLATE C,candidate_sort_name COLLATE C,candidate_id)`; bank `(bank_warning_code IS NULL,coalesce(bank_warning_code,'') COLLATE C,candidate_sort_name COLLATE C,candidate_id)`. Keep rank/order literal, no browser money/name sort. Add exactly matching seek/order/key construction/public cursor arity/type guards and Worker/FE enum/rowKey/keyCompare mirrors. Existing16xx indexes do not match these leading tuples; one genuinely necessary index per new tuple is enough. Candidate name subline alone does not create a separate sortable header; decide actual approved visible column arrangement rather than add unused sort keys.

## Existing indexes and narrow additions

Native1252 Candidate-summary PK `(candidate_id,client_scope,pay_channel,section)`; candidate_sort index `(section,client_scope,pay_channel,candidate_sort_name,candidate_id)`; scope-summary PK `(client_scope,pay_channel,section)`. WB10 migration `10102026_1634_banking_pay_next_workbench_read_indexes_v1.sql`:12 name/null/ref/UUID,13 exact amount/null/UUID,14 deduction/name/null/ref/UUID,15 name prefix,16 TMS prefix; each begins section,pay_channel,client_scope. These satisfy current sorts and scoped searches. The TMS prefix index is not a ref-first keyset sort index. Reuse all current native identity/item/group/seed indexes. No new tables or producer rewrites identified for D06; add only actual sort/count predicate indexes selected by the implemented query.

## Minimal current screen closure

Mount a bounded continuous Ready Candidate component into WB11.readyHost and wire scope, Refresh, availability/context and disposal through the existing shell/Office. One page/window at a time or finite three-page window, compact bounded cursor history; preserve real continuation and stale scope/revision refusal, no agency-wide rows/IDs cache. Page + HEADER count/amount must match the same common context before display. Default NAME_ASC. Server headers with aria-sort and root-confirmed finite new keys; no visible-page sorting/sums. Checkbox toggles are local intent only, complete Candidate universe semantics; deductions-only controls disabled and excluded from local/default count. Single click highlight; double-click/Enter/phone Open call existing genuine Candidate record until WB15 actual child exists, preserving that future join as OPEN. The exact bank chip may open the existing Candidate record. WB17 Timesheet View and WB19 candidate progress retain their named later implementation; do not fabricate actions/statuses from mockup literals.

Label amount stored SOURCE ex VAT/before pay-date valuation; distinguish ready_net_add_amount and deduction balances rather than combining SOURCE/gross/NET/cash or showing a fabricated estimated total. Unknown/unavailable stays dash/refusal, never GBP0. Headline total is page-independent server scope, not checked rows or loaded page. No official date is displayed. Empty copy should follow latest no-pay-date owner wording. Dark/keyboard/phone and actual local screenshot42-point review belong source+render closure; no test programme/scenarios/SQL fixtures.

## Snapshot hashes

- `supabase/repeatable/10102026_1634_banking_pay_next_workbench_read_limits_v1.sql` (BE): `391f32cbecaf4b32a311931d69ed7cbdb0dd85c0bae11a2cdf6a3a07a2e9c02d`.
- `supabase/migrations/10102026_1634_banking_pay_next_workbench_read_indexes_v1.sql` (BE): `886cdd1d4333b05ec92ff6cf39b775bfe0a9f3225b02a8da2e237673b794b4b2`.
- `supabase/repeatable/10102026_1252_banking_pay_next_workbench_summary_v1.sql` (BE): `2fe6c4c816613aaa554d012289124c47219fd228a2af229c2ceabdb8f0cc305b`.
- `supabase/repeatable/10102026_1332_banking_pay_next_withdrawal_projection_v1.sql` (BE): `9212749c48615fac296a1af071ad1b7bb187bfc19c92bcba5021bf4fdc602391`.
- `broker/src/bpay-next-workbench.js` (BE): `3234c8640877dd5d1dac8ffdddee675f8d9e3e2003f73bc52f191b3150f64c31`.
- `js/bpay-next-workbench.js` (FE): `d61ed1278432ca459ebf4537e5dc459390f9e1ced881e0039098ed2f90c43772`.
- `js/bpay-next-office.js` (FE): `34ec7991ec930be29f260b71522ab7d01c370f7430742e09b9755a972769efff`.
- `js/main.js` (FE): `7cde52d8f113f9c35f19f2f0be198390e2d742118c991b98e29ca06dcc645845`.

Saved current findings promptly; owner exact E6/F full prose requested from parent if an additional saved note exists. The explicit parent directions and saved policy above govern this finite design. No generic protocol/parser/proof framework or additional testing programme is recommended.

## Count contract precision

Preserve existing HEADER `selected_count`/`selected_amount` as stored accepted reservation facts. For a new local-intent read, return an explicitly named integer-text `selected_candidate_count` (and, only if needed by actual UI, excluded/eligible Candidate count) instead of silently repurposing the accepted segment counter. Default no-overrides yields the complete eligible Candidate count; deductions-only `payable_count=0` is excluded even though its explanatory CANDIDATES row stays visible. Scope Candidate filter yields1 only if that exact summary has positive payable_count, otherwise0.

The existing summary has complete payment/deduction counts, not a current browser child-override journal. Whole-Candidate intent can be counted from exact Candidate IDs and natural summary scope now. Do not claim exact selected **payment-segment** count from raw segment_count, which includes retained/current nonselectable membership, or selected_count, which is accepted native state. If the current UI needs the whole-Candidate selectable-member denominator, its genuine native count is payable_count + deduction_item_count for eligible Candidates; retain the plain unit “members” unless actual current server semantics justify “payment segments”. Arbitrary work/Case/component overrides and parent MIXED membership remain WB24/WB22; do not build their command/persistence owners under this source table item or fake a MIXED row from mockup data.

For finite intent representation, an ALL/NONE baseline plus bounded typed Candidate exceptions avoids materializing every agency Candidate merely to clear/select all. A single strictly byte-bounded request is a truthful admission envelope; if current local state cannot fit it, visibly refuse the exact count rather than truncate exceptions or pretend full scope. Future larger choice/upload semantics retain their actual named WB22/WB23 acceptance owners. Count remains a read, no lease, receipt journal, financial command, mutation or new kind.

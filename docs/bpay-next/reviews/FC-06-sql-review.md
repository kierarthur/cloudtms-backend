# FC-06 / BD118 — independent SQL source review

11 October 2026. **SQL SOURCE PASS; final scope/manifest/finite-gate amendment below is current. No essential source defect found.** The original pre-install checkpoint is retained as history. Root's saved local compile/catalogue evidence is recorded separately below; this reviewer performed no database execution. Transport/UI and A4 have separate receipts. Every joined economic/application journey remains AUTHORED_NOT_RUN / NOT_RUN.

Read the full FC-06 owning card at approved plan03:1130–1147, all six working checklist rows, current investigation/design boundary/design check and complete `FC-06-parent-provenance.json`. Read complete `supabase/repeatable/11102026_1910_banking_pay_next_manual_debt_reference_v1.sql`, actual current named parents and relevant existing request/metadata schema. Used the PostgreSQL review guidance for query bounds/security; no database, SQL execution, fixture, test, hosted/browser action or product edit was performed. Only this Markdown receipt was written.

## Exact narrow deltas and parent retention

The batch replaces exactly four existing private signatures: Preview `(uuid,jsonb)`, the existing seventeen-argument accept overload, current Case component page `(uuid,uuid,bigint,integer)` and Case-list row `(uuid,jsonb)`. It does not replace the original fourteen-argument wrapper, common terms/context/apply, public intake/page, command/queue, money, projector, table or trigger owners.

An independent in-memory source comparison extracted each complete function, normalized CRLF/LF only and removed its specific added reference block. The resulting four complete bodies equal the actual current parents exactly:

| Function | Actual current parent | Retained after removing the intended addition |
|---|---|---|
| `private.bpay_next_case_create_preview_v1(uuid,jsonb)` | `11102026_1536_banking_pay_next_case_create_intake_v1.sql` | Exact body equality; only fresh reference check added. |
| `private.bpay_next_accept_case_create_v1` seventeen-argument overload | same1536 | Exact body equality; only fresh post-replay reference check added. |
| `private.bpay_next_current_case_component_page_v1(uuid,uuid,bigint,integer)` | `10102026_2026_banking_pay_next_current_group_details_v1.sql` | Exact body equality; only saved nullable reference column added. |
| `private.bpay_next_case_list_row_v1(uuid,jsonb)` | `11102026_1738_banking_pay_next_case_list_v1.sql` | Exact body equality; only saved nullable reference JSON member added. |

The three actual parent raw SHA256 hashes equal the provenance JSON. Source comparison is not a SQL or runtime test.

## Findings

**Fresh reference and historical replay — PASS.** Preview:9–13 first calls original closed terms validation, then refuses Manual Debt whose original reference is null/blank using the existing `22023 / BPAY_NEXT_CASE_CREATE_METADATA_INVALID`; the original <=2048 UTF-8-byte/family/type validation remains in common terms. No receipt, write, lock or new helper is added to the STABLE Preview.

Accept:59–61 retains the original nullable common-input/terms handling. Original module/admin SHARE, Candidate SHARE, worker-control UPDATE, command advisory and exact prior-request read remain. Accept:80–98 compares every existing economic/input/metadata/token operand, including reference with null-safe equality, and returns the original exact receipt. **Only after that return**, lines99–101 require a reference for fresh Manual Debt, before fresh channel/context/short command-clock admission. A previously accepted identical null-reference Manual Debt retains replay; a changed reference/token/input conflicts as before. Current Candidate pricing/channel is not consulted to reprice an already accepted request. The unchanged fourteen-argument wrapper reaches the same seventeen-argument owner: historical exact null replay remains, while fresh Manual Debt cannot bypass the new check by omitting metadata.

**Nullable exact provenance — PASS.** Component page:197–200 adds a scalar lookup of original immutable `case_create_request.original_liability_reference`, qualified by the exact Case UUID, Candidate UUID and MANUAL_DEBT/MANUAL_DEBT on both request and native component. Case-list row:334–337 similarly qualifies exact request Case/Candidate and native Manual Debt family/subtype. No matching original creation request or historical null reference returns JSON null; there is no current Contract/Client inference, cross-Candidate row, arbitrary latest request, fallback or fabricated reference. The key is always present because it is an explicit selected column/JSON member; null is a legitimate value, not permission for a decoder to treat a missing key as null.

`04102026_0320_banking_pay_next_case_create_request.sql:11` already makes case_id unique. Each new scalar lookup therefore has a native indexed zero/one-row key; no all-Candidate/history scan, new index or LIMIT masking duplicate provenance is needed. Existing `11102026_1536_banking_pay_next_case_notes.sql` keeps reference nullable for old history and restricts non-null values to bounded nonblank Manual Debt. Storage/immutability is reused without row rewrite.

**Bounds and existing semantics — PASS.** Component page retains exact Case/Candidate/status scope, ordinal seek/limit1–100, original money/date/rule joins, byte-accounted JSON row accumulation and120000-byte refusal/page truncation. The added reference is included in the counted row bytes. Case-list row retains original full native monetary/control/classification/snooze logic and90000-byte complete-row limit; its reference is counted before the check. Original public Case-list assembly/envelope/continuation limits remain under1738. Neither addition changes ordering, counts, cursor units, page completeness, monetary formulas, reservations, source/channel valuation, remaining amounts or Rule B execution behavior. Existing reference escaping/UTF-8 bytes cannot bypass the native row/body limits.

**Security and compile-source inspection — PASS.** All four exact signatures retain original volatility, SECURITY INVOKER and fixed search_path/configuration. The batch sets the existing audited logical owner postgres and explicitly revokes PUBLIC/anon/authenticated/service_role on each private function. Public service dispatch/auth paths remain original; no browser/service table grant or public owner exposure is added. New expressions use existing typed columns/functions and valid nullable scalar JSON construction; no source compile hazard found. This conclusion is not evidence that PostgreSQL has compiled or loaded the file.

**Scope — PASS.** D03/D04 require fresh mandatory original reference and saved reference visibility. The four small existing-owner additions are necessary FC-03 supporting corrections, not a new server feature or money owner. No imported underpayment is reinterpreted, negative Timesheet manufactured, new command/proof/storage/producer installed or future Case page/control built. Required closed BE/FE CASE_COMPONENTS/CASE_LIST schema amendments and native Manual Debt form are pending root's separate source review; this SQL receipt does not certify them or the item as complete.

## Exact reviewed saved bytes

| File | SHA256 |
|---|---|
| new `11102026_1910_banking_pay_next_manual_debt_reference_v1.sql` | `644995FB19E4A58D0854DE8191980DC7E543015358D81D229785165377E2845C` |
| parent1536 | `853DDCE45455DEFED9F715A2593267794F3B224E469A9EACCDE2AECA6E44A470` |
| parent2026 | `278BD4D3DCCC1E7453A69D1D2E0E4BDCDE7075E5FE5181C5FE090C4A9DFC7E8A` |
| parent1738 | `AB912386DAF210E25A2D5B6F37038B00AAC8081CA0BA27E059DEFFDB17F47231` |
| `FC-06-parent-provenance.json` | `8833606B3DDA389CE64B61BF1356D862B95F420578D4B98E500FBF1FEBA7DFEB` |
| `FC-06-implementation-boundary.md` | `9B83B27A84B16DE5BFEDF2474E8515C7674889E19F9B8F779311BBF681323E4A` |

**Historical pre-install checkpoint:** the registry then still named the reviewed parent owners. The current final chain and root's subsequent local evidence are recorded below. The £120/4 weeks/INV-2291 PAYE gross-stage£30 outcome, NET-stage recovery, historical replay, persisted reference, actual queue and later Umbrella recovery are **NOT_RUN** by this reviewer.

## Final scope, D03 manifest and finite gate amendment

**PASS at the current saved bytes.** Re-read the concrete `FC-06-case-create-contract.json`, completion and successor/parent provenance, current registry delta, exact source-gate delta/consumer, and saved current-owner catalogue/local-metadata JSON. The installed 1910 raw source remains the same reviewed `644995FB…E2845C`; no SQL body changed after the exact inverse-parent review.

The final registry changes only the four reviewed private signatures to1910 and retains1536/2026/1738 as their exact predecessors. Terms/context/apply, original14-argument wrapper, public intake and preparation page remain original owners. The concrete D03 manifest matches that chain and the existing fourteen entered input members, original CASE_PREVIEW/CASE_CREATE/CASE_COMPONENTS routes, exact-pence money text, ManualDebt family/tax/schedule/reference/nullability and original command/token replay semantics. Its targeted readback is acceptance/read visibility, never payout or funding proof. No additional server action, producer, storage or future Umbrella/full Case-page behavior is specified. The separately saved `FC-06-source-review.md` covers the current closed transport/native form/journal; this amendment does not independently certify pixels or application journeys.

**Immutable reference semantics remain exact.** The required output member is nullable: other Case origins and historical ManualDebt without a saved reference return null. Every fresh accepted ManualDebt must supply a nonblank reference; any non-null output comes from its exact immutable request bound to Case UUID, Candidate UUID and MANUAL_DEBT/MANUAL_DEBT. The native unique case_id point lookup prevents arbitrary history selection. Storage stays nullable to preserve old exact replay; it is not permission for fresh admission to omit the reference, a decoder to omit the key, or another family to carry a reference.

**Finite source gate — PASS.** `scripts/bpay-next-source-gates.mjs:527–532` adds one full literal projection to the existing `private.bpay_next_current_case_component_page_v1(uuid,uuid,bigint,integer)` mapping, including the exact qualified reference scalar lookup. Both earlier legitimate projection literals remain. The existing consumer at705–729 still selects exactly one executable function by signature and permits only a matching literal prefix for the `page` constructor; other opaque financial rows, wildcard return projections and malformed constructors still fail. No generic serializer exemption, dynamic-SQL allowance, historical waiver or blanket table/function exclusion is introduced. Whitespace is normalized by the unchanged consumer; the new allowance remains specific to this exact projection.

**Root's saved compile-only evidence.** The catalogue records ten current/retained definitions with source/body MD5 equality, expected volatility/configuration/owner/ACL and `matches:true`; four are1910, while the six retained owners remain1536/1738. Local metadata records nullable bounded reference/note/token columns, the immutable UPDATE/DELETE trigger, owner-only table ACL, unchanged `839|76|0||338226c4e9b878c5aa92fc8e319dd7bf`, zero public SECURITY DEFINER membership delta and successful General/Candidate isolation DO results. Successor provenance/completion records root's sole managed localPG17 compile,12/12 source gates and export/seal/coupling `0a96f9c1de30080867e3f68f55428304266d43165cee405b2803749c6139145c` PASS. These are read saved root receipts, not reviewer execution or installed TEST/application acceptance.

| Current final evidence | SHA256 |
|---|---|
| exact source gate | `4AB6C6139770601665C0F117EFFF941CED5AE0E52B7D80B25EFA27948CDF0394` |
| final-definer registry | `674BEF14FC610CC56D448F1588CFD456245B9F2379F047C7BAD8410FB20F0B89` |
| D03 contract manifest | `A4CCDB6D21CF43A4738FF4994E0AE6168429F121AF3EEEBB59DFC3E41E4C5742` |
| current-owner catalogue | `32756FAD85602C805DAC53D37B67D9A7ADB1B1B3122102BA3A42C28CE625F488` |
| local metadata | `8708E26B5AAED4CBB7EA2EC3164287B19C04A328A5D7DF195C0205C1E8F721E0` |
| completion | `6A0DB70C2072292E9A4E05ACC0EDE540F452342E52E170C3DB118F152444BF65` |
| successor provenance | `C5162C9B81CF19622E3395AE0C2FBEAE699895A15B62E3C33A3BC22A8DD5ED40` |
| separate transport/form source review | `B71A1DF30C1FDFA5A5DD56215605CCC5D45ECC7C56B61FB9D438D59FF5430204` |

This closes the SQL/scope/D03-manifest/finite-gate source review. A4 and whole-item completion remain outside this receipt; joined replay/queue/financial/reference/parent journeys and timings remain **NOT_RUN**.

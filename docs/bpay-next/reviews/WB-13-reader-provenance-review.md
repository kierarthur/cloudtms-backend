# WB-13 reader amount-provenance review

10 October 2026, independent source-only review. Full WB13 card/R2 and the previous WB09 reader contract/field map used. Read1601 successor, original1520 helper, native item schema/classifier evidence, and both current closed ROWS definitions. No SQL, tests, fixtures, runtime, app, data or product edits.

Raw SHA256 checkpoint:

- SQL1601 `793b332a210575c3b0e938735fe6787a8a5f62698546d63630f5ddad796f834d`.
- Worker `a5e74957c7e680ecf4966e2d21bf135b5a3c373a871bcffebdab044288d80fb5`.
- FE helper `0f992753e87baf7cb82c09abc8f644a6533dbae805e8f9ee71e2c65750df9424`.

Initial verdict: SQL addition **source PASS**, caller schemas require RP01 correction. This is not whole WB13 or execution acceptance.

## RP01 — nullable native payroll stage rejected by new issue schemas

Worker ROWS ACTION_REQUIRED/BLOCKED/ISSUE_DETAIL set the new `payroll_stage` to non-null `stage` at120/168/216; FE mirrors that at122/170/218. The schema itself allows null payroll_stage. The real current WORK GROUP_EVIDENCE branch resets the item, sets direction INFORMATIONAL and leaves payroll_stage null (1438:273–299). It produces actual MISSING_APPROVAL/DATA_UNAVAILABLE and withdrawal evidence. The actual CASE missing-evidence branch likewise leaves stage null (1252:605–612).

SQL1601 correctly preserves this null. `field(null,'stage')` rejects it because only a `?` type admits null. A valid blocking/action/detail page containing that actual native evidence will be rejected wholesale, hiding precisely the missing-money facts the card requires. Change only the three added issue-stage types in both mirrors to `stage?`, retaining the closed enum when present. No native classifier change or manufactured GROSS_ADD is needed.

## Exact bounded addition inspected

Pure text comparison extracted the complete same-signature helper from1520 and1601, removed precisely the three appended eight-field JSON constructors, and found identical bodies (3 matching additions, remainder equal). No SQL/module was invoked. The additions are only:

- direction and payroll_stage from the same typed maintained item;
- approved_source_ex_vat, active_reserved_source_ex_vat, deduction_balance_source_ex_vat, recovery_source_ex_vat, included_source_ex_vat, gross_due_source_ex_vat, all direct `::text` casts.

These values preserve the existing Native approved/held/debt/recovery/included/current-positive provenance. They do not reinterpret nominal as recoverable cash, sum a visible page, refresh current authority or run allocation/headroom. Existing unknown-null constraints preserve NULL; no coalesce/rounding/browser Number is introduced. Signed native amounts remain exact text.

The three new constructors concatenate explicit named projections and remain below the PostgreSQL argument limit. No whole financial row, SELECT*, opaque payload, new signature, public entry, owner/classifier/producer or F25 protocol change. Existing private invoker behavior and retained ACL are preserved by CREATE OR REPLACE; no new grant. Current fixed EXECUTE templates, unique bound cursor/order/membership, STABLE snapshot, pending/error barrier, limit+1 lookahead, byte envelope and NOTIFY remain identical to1520. Worker/FE add the same eight keys to all three issue shapes and reject extra keys; six decimal fields are correctly nullable. Direction is genuinely NOT NULL/native enum, unlike payroll stage.

Only this review note was written. Root alone corrects product. Runtime and joined Office/Worker acceptance remain NOT_RUN; parked F25/Rule B is untouched.

## RP01 fresh closure — provenance SQL/ROWS subset PASS

Fresh disk read confirms `payroll_stage: 'stage?'` in ACTION_REQUIRED, BLOCKED and ISSUE_DETAIL in both mirrors. The existing optional-type validator permits exactly NULL or the closed native stage enum; it does not broaden direction or the six exact decimal fields. Genuine native GROUP_EVIDENCE is now admitted without synthesizing a stage. RP01 is closed; no remaining concrete defect found in the scoped eight-field SQL/ROWS addition.

Fresh hashes: SQL unchanged `793b332a210575c3b0e938735fe6787a8a5f62698546d63630f5ddad796f834d`; Worker `8d940bcae91e05f056fcf116a25096f8d1bea68302cd2fedd4e68460a0276a73`; FE `7ce1d01f857239130c4cb5277f572a8c122a32828580b438b4bc65a249ea4d64`. The FE hash also includes concurrent new WB13 UI code; that larger mount/render/action lifetime implementation is outside this requested nullable-stage closure and has not been granted a whole-file verdict here. No tests, fixtures, SQL or runtime execution.

# WP-F9 and WP-F8 source handoff

Authorised isolated source worktree: `healthroster-v2-date-guards-build-13102026`, branch `codex/healthroster-v2-date-guards-build-13102026`, base `83547d19a4e8407d5a330849f0e82736c33e48de`. Root owns integration, DB/tools, fixtures, installation, publication, source backup and central progress. No commit, push, DB, Docker, hosted call, app build/upload, real email or secret/auth read was made by this lane.

## F9 implementation

Four physical Timesheets reads selected `work_date`, which does not exist; one also selected absent `id`/`client_id`. Their errors were swallowed. PATCH had no pre-save date guard. The new focused reader uses only proved physical columns, combines actual schedule dates, the UK date of Daily `worked_start_iso`, current TSFIN date keys and declared legacy/typed external-source clocks, and noncancelled imported shifts. It filters current/nonrevoked/nonarchived Timesheets and preserves the existing draft/void/cancelled status exclusions. It never falls back to week ending, planned schedule, current Rota or PDF/settings dates.

Both existing save handlers read/check this range before any mutation. Their later normalisers reuse the checked bounds rather than attempting another fallible read after writing. The boundary-check response retains its field names and names actual violating dates. Clone-and-extend is byte-identical to the base owner; progression/rate/channel/WeekEnding guards and planned-range normalisation remain native.

The purpose GET reader requires valid JSON arrays, exact Content-Range totals, stable complete pagination, unique physical IDs and complete source reads. A SQL/network/malformed/truncated read refuses before save. The index's existing Miget-normalised fetch and existing service headers are retained. Every source failure has safe plain wording. No generic global `sbFetch` rewrite was made.

Explicit empty `actual_schedule_json=[]`, no Daily/worked end, no signature/authorisation/positive worked minutes and no current financial work witness contributes no date (including a new RECEIVED P0). A protected/signed/processed/economic HOURS record missing its factual dates remains held. This does not rely on a DRAFT enum value. Empty expense/mileage carriers remain date-free. Existing empty-schedule import originals are protected by current financial/import facts.

## F8 implementation and ownership

The saved upload-time S12/cutoff gate and full-week/floor checks are private, purpose-scoped and produce `WEEKLY_SOURCE_FINAL_UPLOAD_GATE_V1`. Checking remains accepted any day. Complete adjacent Prepare, Client Save, Upload Context and upload-stage definitions preserve their native bodies outside the declared guards. New optional coverage context produces three amber “Upload anyway” warnings through the actual preview owner/HTTP route; the adapter and direct SQL stage owner enforce the go-live floor. New closed refusal codes return plain UI wording; unrelated errors/output retain their original route behavior.

F1 Client Save input was read-only root `13102026_1134_weekly_source_rule_sets_v1.sql`, SHA256 `01079224c18c5762c5626fb2924155f3f4cc97714b0670f2b6d9dadb412bb25a`; the complete original function is a data-free source fixture. Latest saved Upload Context input is `08102026_1635_weekly_source_released_client_eligibility_v1.sql`. Original Prepare is `30092026_2052_weekly_source_import_preparation_v1.sql`; upload-stage is the latest complete `15092026_1534_weekly_source_upload_publication_v1.sql` owner. Existing applied inputs were never edited.

Exact root-only shared FIN/read/ACL/report integration is in `F8_INTEGRATION_HUNKS.md`. All dependent SQL installation, read producer/Office publication and activation gates remain pending; this lane does not claim them complete. S11 factual timestamp ownership remains with the money/parser lane. All prior Candidate/MyTMS/Office work stays yielded to root.

## Verification

Source checks cover real extracted PATCH/PUT/check handlers with auth first and no mutation on failure, all client-route date cases, BST Daily dates, current/old financials, cancellation, multi-page completeness, empty RECEIVED versus malformed/signed records, native unchanged clone/extend, actual preview/accept/HTTP warning behavior, exact complete-owner preservation and declared authority scope. SQL helper vectors/role verifier are saved source; they have not been executed by this lane.

Final source results and exact file hashes are recorded in `DATE_GUARDS_SOURCE_MANIFEST.json`. Root must review the complete source and run the pending installed/runtime/browser/concurrency proof before release. Tests under `tests/contracts` and the new Weekly Source suites are also imported by ordinary `tests/*.test.js` entry points.

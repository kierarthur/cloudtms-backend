# SP-12 identity admission successor review

**Verdict: source PASS for the four exact identity/mapping/rotation successor definitions in `10102026_0922_weekly_source_pay_query_identity_admission_v1.sql` and their current final-definer registry entries.** Independent read-only source review, 10 October 2026. This does not close the separately running outer-caller/writer census or establish installation, contention, runtime or in-app acceptance.

Read `.codex-local/SP-12-identity-targets.json`, the complete source text of its two current predecessor files, the full four extracted definitions, all insertion/catch/first-lock seams and the four registry entries. Text comparison reads source only: it does not invoke SQL, a routine, a test, a source gate or business logic. No product edit, test, fixture, SQL/DB/runtime or hosted operation was performed. Root alone authored product.

## Exact definitions and preserved source

| Signature | Current predecessor | New definition line | Complete definition comparison |
|---|---|---:|---|
| public.contract_week_manual_upsert_atomic(uuid,uuid,jsonb,jsonb,jsonb,jsonb,jsonb,uuid,boolean,timestamp with time zone,text,jsonb) | 03102026_0500_h1h2_successor_authorities_after_retry.sql | 7 | Exact original after removing one admission block and the one narrow busy rethrow block |
| public.timesheet_route_version_rotate(uuid,uuid,text,uuid,boolean) | 08082026_2035_timesheet_route_version_rotate.sql | 2140 | Exact original after removing one admission block |
| public.timesheet_route_version_confirmed_v1(uuid,uuid,text,text,text,uuid,text,text,text,boolean,timestamp with time zone) | 08082026_2035_timesheet_route_version_rotate.sql | 2322 | Exact original after removing one admission block |
| public.timesheet_qr_restore_version(uuid,uuid,text,uuid) | 08082026_2035_timesheet_route_version_rotate.sql | 2696 | Exact original after removing one admission block |

Each exact name has one original and one successor definition. The full comparison normalizes CRLF/LF only and then removes the literal new blocks; it compares the complete remaining CREATE definition, including parameter defaults, return type, language, SECURITY DEFINER, search path, declarations, every original statement, branches and body terminator. All four match exactly. Original character lengths are respectively 120257, 9515, 18298 and 5286; the successor increments are exactly the intended source blocks. No original money, identity, workflow, import-authority, managed-root, QR, audit, replay or notification statement was changed or omitted.

## Admission and error placement

Each new call is gated only by the existing scalar `private.bpay_next_module_control(id=1).active_owner='NEXT'`, using the established `private.weekly_source_pay_query_admit_v2()` owner helper. It adds no module row lock, new clock, producer/version protocol or alternative money owner. The helper remains the existing VOLATILE/SECURITY DEFINER transaction try-lock with its exact Source key and `55P03`/`WEEKLY_SOURCE_PAY_QUERY_ADMISSION_BUSY` miss.

- **Manual upsert:** admission at lines 144–147 follows the original GUC setup, plain settings read, JSON/input checks and queue-input parsing. It precedes the first original diagnostic write and the first actual contract-week `FOR UPDATE` at line 196. All later staged/booking/physical-row lock and validation ordering is preserved.
- **Rotate dispatcher:** admission at lines 2182–2185 follows the original non-locking target booking discovery and precedes the original trimmed booking advisory lock, differing raw booking lock and family row locks. Original managed/protected-root and import-authoritative refusal checks remain under their original compatible locks and before either downstream mutation owner.
- **Confirmed rotation:** admission at lines 2391–2394 follows original confirmation input checks, non-locking requested-row discovery, environment check and feature/service-context validation. It precedes the original `CANDIDATE_PAPER_FAMILY` advisory lock, booking key and family/target/workflow/QR row locks. Original signature/context reproving and workflow/notification handling remain intact.
- **QR restore:** admission at lines 2731–2734 follows original non-locking target booking discovery and precedes the original trimmed/raw booking and family row locks. Output columns remain table-qualified as in the original RETURNS TABLE owner. Managed-root refusal, route-confirmation retirement refusal and legacy private restore call remain unchanged.

The manual-upsert catch obtains the original stacked SQLSTATE/MESSAGE_TEXT and preserves its original diagnostics. Immediately before the existing generic 55P03 → `LOCK_TIMEOUT` translation, it now uses bare `RAISE` only when both values equal **55P03** and **WEEKLY_SOURCE_PAY_QUERY_ADMISSION_BUSY**. That retains the original exception identity; another 55P03 still uses the original LOCK_TIMEOUT handling. Arbitrary 55000, message substrings and unknown errors are not promoted to retryable rollback. Unique-violation handling and every other catch statement are unchanged.

## Security and chain

The same four existing signatures are replaced; no function is added or dropped. Full header comparison establishes unchanged defaults, return shapes, volatility defaults, SECURITY DEFINER and exact search paths. The new file uses one transaction, four audited logical `owner to postgres` clauses and one controlling `notify pgrst, 'reload schema'`. It contains no new GRANT/REVOKE or signature change. CREATE OR REPLACE preserves each existing ACL: the predecessor route/restore/confirmed wrappers deny public/anon/authenticated and grant service_role; manual-upsert retains its established predecessor ACL. The audited logical owner remains the repository owner for release-tool mapping, not a new provider-role assumption.

All four current registry rows select 0922 as both finalRoot and finalPhysical, add it to allowedPhysical and retain their actual immediate predecessor in the predecessor list. Earlier chain entries are preserved. The registry does not declare that this finite file proves all direct/transitive outer callers; that closure remains the required separate SP-12 census, including admission before any outer pre-lock.

No actionable source finding remains within this four-definition scope. Schema loading/security/coupling/source gates belong to root's authorised workflow; they were not run or inferred by this review. Full application acceptance remains NOT_RUN.

## Fresh source hashes

Paths relative to BUILD-BE.

| File | SHA-256 |
|---|---|
| .codex-local/SP-12-identity-targets.json | 5617b9e511852a0758f8bc804cf5ce5320eb41874fd48993e39a31c50a1eb590 |
| supabase/repeatable/10102026_0922_weekly_source_pay_query_identity_admission_v1.sql | be5f4196d41148936b7d7f34cadd5ca82312dd797a2c1496af72e0d37e135688 |
| supabase/repeatable/03102026_0500_h1h2_successor_authorities_after_retry.sql | 2246174a1df3ffb20da053b548b2224fe8b67afcc6f0e4fe3fabf3b35f876b67 |
| supabase/repeatable/08082026_2035_timesheet_route_version_rotate.sql | d61a1cd25137aad4387345dc6fe6c120c5448e28bb8f30f37ddfb091b376664a |
| docs/bpay-next/final-definer-closures.json | ab6ed6013e1a40f79219133d3cbe5cbf9d3df38f553985b83bbe460c0b6a23ab |

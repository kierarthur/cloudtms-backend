# First-install migration prerequisite review

Source-only investigation by the independent SQL reviewer, 13 October 2026. Authority: read saved source and write this review only. No database connection, SQL/RPC invocation, test, fixture, release, network, Git mutation or product edit was performed. The clean PostgreSQL 17 `42883` failure is reported by the root agent; this reviewer did not reproduce it. Saved disk contents are authoritative. The source commit at inspection was `93cfbe5433231be4915592e40665fde3ba3f1265`, with concurrent root edits to release packaging.

## Finding and exact census

The prerequisite is one existing function, `private.bpay_next_effect_immutable_v1()`. Two immutable migrations require it before the normal repeatable phase:

| Locked migration | References requiring the prerequisite |
| --- | --- |
| `supabase/migrations/11102026_2129_banking_pay_next_case_hold_audit.sql` | Lines 35 and 37: row UPDATE/DELETE and statement TRUNCATE triggers on `private.bpay_next_case_hold_transition`. |
| `supabase/migrations/12102026_0645_banking_pay_next_confirm_refresh_membership.sql` | Lines 55 and 57: row UPDATE/DELETE and statement TRUNCATE triggers on `private.bpay_next_confirm_refresh_member`. |

The source census used the current 386-entry migration lock, the configured immutable baseline files, and current repeatable definitions. It examined all 33 locked migrations from the first failing audit migration onward, including the undated tail. Of 2,386 known qualified routine names, 15 qualified later occurrences were found; four lacked baseline/earlier-migration authority. All four name the same helper above. Definitions and calls were distinguished manually for the later same-file guard.

No additional later custom-function prerequisite was found in trigger/default/CHECK expressions. Later defaults/checks use ordinary PostgreSQL functions and operators, including UUID generation, finite-time checks, JSON shape checks, lengths and transaction timestamps. This is a source access/dependency census, not executable SQL parsing or a compile proof.

The other later references already have authority before use:

- `13102026_0725_banking_pay_next_frozen_decisions_clear.sql` defines `private.bpay_next_decisions_clear_evidence_guard_v1()` at line 56, with owner/revoke clauses at 142–143, before its six triggers at 144–149. It requires no early repeatable installation.
- Undated `tms_ref_nums.sql` calls `public._audit_insert` and `public._change_bump` at 24–25. Baseline routine page `22082026_1503_cloudtms_test_routines_00.sql` defines these at 10 and 552.
- Earlier native custom DEFAULT: `03102026_1324_banking_pay_next_core.sql` defines `private.bpay_next_current_valuation_policy_id_v1()` at 62 before its column DEFAULT at 148.
- Earlier native custom CHECK: `15092026_1534_weekly_source_plan6_schema.sql` defines `private.weekly_source_uuid_array_is_distinct_v1()` at 2567 before checks at 2638, 3173, 3176, 3294 and 3295.

This closes the named later trigger/default/CHECK census. It does not certify execution of arbitrary earlier migration bodies or first-use business behavior.

## Existing helper authority

The sole saved definition is in `supabase/repeatable/03102026_1342_banking_pay_next_guards_v1.sql`, lines 188–199. It has no table, row type, other function or extension dependency:

```sql
create or replace function private.bpay_next_effect_immutable_v1()
returns trigger
language plpgsql
set search_path = pg_catalog, private
as $function$
begin
  if tg_op <> 'INSERT' then
    raise exception using errcode='23514', message='BPAY_NEXT_FINANCIAL_EFFECT_IMMUTABLE';
  end if;
  return new;
end
$function$;
```

It is a security invoker trigger function with the native default volatility/parallel/strictness settings. Line 287 assigns logical owner `postgres`; the grouped REVOKE at 288–294 includes this exact signature and removes privileges from `PUBLIC`, `anon`, `authenticated` and `service_role`. There is no explicit nonowner grant. Owner mapping must use the existing audited logical-owner mapping to `CURRENT_USER`, preserving Miget portability.

The canonical LF SHA-256 of the exact CREATE statement above, without a trailing newline, is `b73804883751b33d8f200643319ebd95309292b6b3cdf8ae1d8f9fcb6121052b`. The canonical whole-root hash is `d29decd59c88c2620fe658b5d96f0deace19063cd0eaddf150ea7cf09c7b60af`. The rest of that repeatable contains other routines and table triggers; installing the whole root early would exceed this prerequisite.

## Narrow managed mechanism

`docs/DATABASE_RELEASE_BIBLE.md` line 56 requires the immutable structural/routine baseline, the disabled-first bootstrap, every post-anchor locked migration, then changed repeatable authorities. The legacy bootstrap list in `current-release.json` belongs to the separate `LEGACY_UPGRADE` path and is not the NEW/UPGRADE prerequisite hook.

The appropriate seam is the shared managed migration unit: `compileManagedUnit` validates the exact consumer migration and helper source; `unitCheckpointSql` supplies the exact existing helper declaration and its owner/revoke clauses immediately before the unchanged consumer body, under the same writer guard, transaction and original migration ledger receipt. The helper and its trigger consumers therefore commit or roll back together. This requires no locked migration edit, backdated migration, permissive trigger shim, baseline rewrite, new business protocol or blanket early repeatable replay.

Required closed scope is the one source root and signature, exact source/recursive-closure/CREATE-statement hashes, and the two exact consumer migration content/closure hashes. Source extraction must find exactly one matching declaration. Unknown roots, altered include closures, altered declarations and changed consumers must fail before admission. The normal repeatable phase still installs and receipts the complete guards root; the prerequisite must not pretend that whole root was already applied.

NEW creates the helper only when absent. UPGRADE may retain an existing exact native helper; unexpected existing body, ownership, return type, security, volatility, parallelism, configuration or nonowner privilege must refuse. It must not silently replace an unreviewed installed routine. Use the existing owner mapper; no `SET ROLE postgres`, widened ACL or browser exposure is needed. No guard is invoked during packaging.

This belongs in the same exported unit compiler/wrapper consumed by managed NEW/UPGRADE and `bpay-next-local-schema.mjs` NEW/CHANGED. Local source at lines 161–172 compiles and applies migration units through the shared `compileManagedUnit`/`unitCheckpointSql`; no separate local workaround is required. Managed source similarly applies all migration units before preapply and ordinary repeatables. Keep final contract/ACL checks and the source inventory ledger truthful.

The exact prerequisite policy must also be part of `managedReleaseContext`'s `executionPolicy` manifest payload, alongside the owner RLS envelope, so admission/resume identity binds the new execution packaging. Adding only a metadata version is insufficient to bind the complete pinned policy.

## Concurrent implementation observation

During this read-only investigation the root saved `IMMUTABLE_GUARD_DEPENDENCY`, `immutableGuardDependencyFor`, `immutableGuardDependencySql` and the atomic unit wrapper in `scripts/cloudtms-managed-release.mjs`. The observed implementation limits consumers to the two native migrations, pins the source and declaration, creates the absent native helper plus mapped owner/revokes, and rejects an existing catalogue predecessor with unexpected native body/configuration/owner/ACL. These match the recommended seam. This is an inspection of source only; the reviewer has not executed the generated SQL.

**PREREQ-01 — open at this source checkpoint:** `managedReleaseContext`'s execution-policy payload did not yet include `IMMUTABLE_GUARD_DEPENDENCY`. The root was notified immediately. This is the remaining concrete packaging identity requirement; installation/compile success is a separate root verification step.

## Source receipt

Raw saved-file SHA-256 at the census checkpoint; canonical LF pins are explicitly identified above and in the engine policy, and may differ from raw CRLF bytes.

| File | SHA-256 |
| --- | --- |
| audit migration `11102026_2129` | `E6CB93B8F55EE253FF9FDFE9F3EB025448FA4128FCEFBCEE1C50D09FE48A8FD1` |
| confirm-membership migration `12102026_0645` | `28785F633847C5FECFD937793D8CDC841299F7847975DA779B8019A89A345763` |
| guards repeatable `03102026_1342` | `D29DECD59C88C2620FE658B5D96F0DEACE19063CD0EADDF150EA7CF09C7B60AF` |
| `scripts/cloudtms-managed-release.mjs` | `DE7E86E519F7F7E903CD9F4553A43D41E06686E832698318A09EF3EE5E8737F9` |
| `scripts/cloudtms-managed-execution-policy.mjs` | `276894D444081991165B476910CE3B81E7D7A9608ADDDB7F493BE5AC17C1A199` |
| `scripts/bpay-next-local-schema.mjs` | `DB65145E8B59C0F9ABF1D70C32AEE0A5F8D63D8E8B8625F1BF9685AA992C6C2A` |
| `supabase/release/migration-lock.json` | `1F7C7E8E1D7D085BDD29DD79C57CD44AD84E5687A6AE7EF8FC885C7EDCF75CF1` |
| `supabase/release/current-release.json` | `C04D629D9308C4B828CE5E9E800C49F49A651B2025E2A88D70A909589C528950` |

Verdict: one narrow existing-function prerequisite is necessary and sufficient for the named later trigger/default/CHECK source census. PREREQ-01 remains open at the implementation checkpoint above. This review makes no NEW replay, UPGRADE installation, local compile, data safety, runtime, A4 or release claim.

## Final saved-source closure — 13 October 2026

**SOURCE PASS. PREREQ-01 is closed.** The earlier checkpoint remains above as history. The final saved `managedReleaseContext` at lines 410–416 includes the complete `IMMUTABLE_GUARD_DEPENDENCY` object in `managedManifestHash`'s execution-policy payload. `managedEvidence` also carries its version. The manifest implementation serializes that payload with migration/repeatable source inventory, baseline and verification identities, binding the prerequisite policy to admission/resume identity.

The final source limits the prerequisite to the two exact consumers and one native root/signature. Independent text hashing verified both consumer content pins against the immutable migration lock and all three single-file recursive-closure pins using the engine's exact `path + NUL + canonical source + NUL` representation:

| Authority | Verified canonical closure SHA-256 |
| --- | --- |
| audit migration `11102026_2129` | `ee3378237e29e49f6b9ad5854afac300be42b72ea0024b94f3403106ffbcdd8d` |
| confirm-membership migration `12102026_0645` | `430ccb09bed6de441a5f150b2422d20a1c37f2293c2a8a3c9a1bab2e7ed89186` |
| guards root `03102026_1342` | `82ac1db0302dd5d77c1acbe6b708edc811a64dd8ac5f99dbe441a0cb5f7a180c` |

The wrapper additionally refuses another unit kind, an execution exception, an overlapping owner RLS envelope, changed source/body, extra source/include files, copied rather than exact scope identity, or altered extracted declaration. It verifies the executable consumer body equals the existing audited owner mapper's result. Only an absent helper receives the actual native declaration and owner/revoke clauses. An existing helper must retain the native body digest, trigger return type, PL/pgSQL language, owner, security invoker, volatile/parallel/strictness/leakproof attributes, exact search_path configuration and owner-only effective ACL. A mismatch raises before consumer DDL. No native guard invocation or nonowner grant is added.

The prerequisite, original unchanged migration body and original migration ledger write occur in one BEGIN/COMMIT packet under the existing writer assertions. The full guards repeatable remains in its ordinary phase with its own truthful receipt. The shared compiler/wrapper is used by managed NEW/UPGRADE and local NEW/CHANGED compilation. No new generic configuration entry, locked SQL edit, backdating, shim or business protocol was introduced for this fix.

The saved regression source at `tests/managed-database-release.test.mjs` lines 100–125 checks exactly two consumers, helper-before-consumer-before-ledger order, one transaction, owner/invoker/revoke guards, exclusion of unrelated guards/repeatable receipts/grants, mutated unit refusals and the unrelated native `0725` migration. This reviewer read the test source and did not execute it. The root separately reported 26/26 managed-source tests before the final policy inclusion and a second clean NEW replay in progress; those reports are not a reviewer runtime or completed replay claim.

Final raw saved-file receipts:

- `scripts/cloudtms-managed-release.mjs`: `BDE07CF05D42918830A95BF2C8136058B5F568817FCC76193EC5B2AC22B19BBF`.
- `tests/managed-database-release.test.mjs`: `2A61E7F92A437DDB6CD19BB551F205824F8CE7E59EE722A50DA666FFE5C0830E`.
- The two locked consumers, helper root, execution-policy module and local compiler retain the raw hashes recorded in the original receipt above.

No remaining mandatory source finding was identified in this narrow prerequisite mechanism/census. NEW replay, installed catalogue/contract verification and release completion remain separate evidence owned by the root. All earlier read-only/no-execution limits remain in force.

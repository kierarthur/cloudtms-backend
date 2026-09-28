// Stage 2 v5.3 (hosted A34_CLOSURE_FAILED): hosted TEST objects carry ACL entries granted TO the separate physical
// superuser role literally named `postgres` (residue of releases executed before the engine mapped ACL grantees).
// The A34 closure removes only that residue, guarded, before its unchanged fail-closed check.
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = relative => fs.readFileSync(path.join(repoRoot, relative), 'utf8').replaceAll('\r\n', '\n');
const A34 = 'supabase/repeatable/26092026_0207_banking_pay_stage2_recovery_order_floor_v1.sql';
const A25 = 'supabase/repeatable/26092026_0209_banking_pay_stage2_grants_v1.sql';
const TAG = 'v53_postgres_acl_residue';

test('A34 removes only guarded postgres ACL residue immediately before its unchanged closure check', () => {
  const text = read(A34);
  const start = text.indexOf(`DO $${TAG}$`);
  const end = text.indexOf(`$${TAG}$;`, start + 1);
  const verify = text.indexOf('DO $a34_verify$');
  assert.ok(start > 0 && end > start && verify > end, 'residue removal must precede the A34 closure check');
  assert.equal(text.indexOf(`DO $${TAG}$`, end), -1, 'exactly one residue removal step');
  const block = text.slice(start, end);
  const executable = block.split('\n').filter(line => !/^\s*--/.test(line)).join('\n');
  // the physical role is looked up by name; the step is a no-op when it is absent or IS the release owner
  assert.match(executable, /FROM pg_catalog\.pg_roles r WHERE r\.rolname = 'postgres'/);
  assert.match(executable, /IF v_role_oid IS NULL OR v_role_oid = v_owner THEN[\s\S]*?RETURN;/);
  assert.match(executable, /IF NOT v_role_super THEN\s+RAISE EXCEPTION[^;]*V53_POSTGRES_ACL_RESIDUE_ROLE_NOT_SUPERUSER/);
  // only the release owner's objects, only that grantee, never another grantee, never a GRANT
  assert.match(executable, /p\.proowner = v_owner/);
  assert.match(executable, /c\.relowner = v_owner/);
  assert.match(executable, /REVOKE ALL ON %s %s FROM %I', v_object\.kind, v_object\.object_name, v_role_name/);
  assert.equal((executable.match(/\bREVOKE\b/g) ?? []).length, 1);
  assert.doesNotMatch(executable, /\bGRANT\b/);
  assert.doesNotMatch(executable, /\b(anon|authenticated|service_role|PUBLIC)\b/);
  // contract neutrality is enforced before anything is revoked
  assert.ok(executable.indexOf('V53_POSTGRES_ACL_RESIDUE_NOT_CONTRACT_NEUTRAL') < executable.indexOf('REVOKE ALL ON'));
  assert.match(executable, /r\.residue_privileges <@ COALESCE\(r\.owner_privileges, '\{\}'::text\[\]\)/);
  assert.doesNotMatch(executable, /pg_catalog\.(coalesce|nullif|least|greatest)\(/i);
  // the fail-closed A34 predicate itself is unchanged
  const closure = text.slice(verify, text.indexOf('A34_CLOSURE_FAILED', verify));
  assert.match(closure, /WHERE a\.grantee <> p\.proowner\n\s+AND NOT \(c\.svc AND a\.grantee = 'service_role'::pg_catalog\.regrole AND a\.privilege_type = 'EXECUTE'\)\)/);
});

test('A25 grants closure is unchanged by v5.3', () => {
  const text = read(A25);
  assert.doesNotMatch(text, /v5\.3|v53_/);
  assert.match(text, /A25_GRANTS_CLOSURE_FAILED/);
});

test('Weekly Source owner assertions accept the provider-mapped logical owner', () => {
  const files = [
    'supabase/verification/15092026_1534_weekly_source_upload_publication_v1.sql',
    'supabase/verification/17092026_0200_weekly_source_rotation_authority_v1.sql',
    'supabase/verification/17092026_0600_weekly_source_first_authorisation_v1.sql',
    'supabase/verification/17092026_0610_weekly_source_withdrawal_supersession_v1.sql',
    'supabase/verification/17092026_1000_weekly_source_settlement_allocation_v1.sql',
    'supabase/verification/17092026_1100_weekly_source_candidate_view_producer_v1.sql',
    'supabase/verification/17092026_1200_weekly_source_audit_and_export_v1.sql',
    'supabase/verification/17092026_1400_weekly_source_ordinary_authorisation_guard_v1.sql',
  ];
  for (const file of files) {
    const text = read(file);
    assert.doesNotMatch(text, /(?:rolname|proowner\)|::text|\))\s*=\s*'postgres'/, `${file} still pins the owner name`);
    assert.match(text, /in \('postgres', current_user\)/, file);
  }
});

test('contract-neutral coupling attestation binds the exact changed bytes to the unchanged contract', async () => {
  const saved = process.exitCode;
  const coupling = await import('../scripts/verify-database-contract-coupling.mjs');
  process.exitCode = saved;
  const sha = text => crypto.createHash('sha256').update(text.replaceAll('\r\n', '\n'), 'utf8').digest('hex');
  const contract = read('supabase/release/current-contract.json');
  const files = { [A34]: read(A34), 'supabase/release/current-contract.json': contract };
  const attestation = { contract_sha256: sha(contract), files: [{ path: A34, sha256: sha(files[A34]) }] };
  const reader = overrides => relative => {
    if (relative === 'supabase/release/contract-neutral-changes.json') return JSON.stringify(overrides.attestation ?? attestation);
    return overrides[relative] ?? files[relative] ?? read(relative);
  };
  const withAttestation = [A34, 'supabase/release/contract-neutral-changes.json'];
  assert.equal(coupling.couplingFailure(withAttestation, reader({})), null);
  assert.match(coupling.couplingFailure([A34], reader({})), /changed without supabase\/release\/current-contract\.json/);
  assert.match(coupling.couplingFailure(withAttestation, reader({ [A34]: `${files[A34]}\n-- edit` })), /26092026_0207/);
  assert.match(coupling.couplingFailure(withAttestation,
    reader({ attestation: { ...attestation, contract_sha256: '0'.repeat(64) } })), /26092026_0207/);
  assert.match(coupling.couplingFailure([A34, A25, 'supabase/release/contract-neutral-changes.json'], reader({})), /26092026_0209/);
  assert.equal(coupling.couplingFailure([A34, 'supabase/release/current-contract.json'], reader({})), null);
  const committed = JSON.parse(read('supabase/release/contract-neutral-changes.json'));
  assert.equal(committed.contract_sha256, sha(contract), 'committed attestation is bound to the current contract');
  assert.deepEqual(committed.files.map(entry => entry.path), [A34]);
  for (const entry of committed.files) assert.equal(entry.sha256, sha(read(entry.path)), entry.path);
});

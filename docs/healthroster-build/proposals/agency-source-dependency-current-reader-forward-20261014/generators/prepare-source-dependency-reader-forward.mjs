import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
const here = path.dirname(fileURLToPath(import.meta.url));
const repo = path.resolve(here, '../../../..');
const dir = path.join(here, 'source-fence-reconciliation');
const root = path.resolve(process.argv[2] || '');
const pin = 'fa66183f01913e86651e82816f5e88f97e07123c';
const owner = 'supabase/repeatable/14102026_0430_healthroster_money_native_interfaces/11_03_cas_operational_projection.sqlinc';
const sha = b => crypto.createHash('sha256').update(b).digest('hex');
const raw = fs.readFileSync(path.join(root, owner));
assert.ok(raw.equals(execFileSync('git', ['-c', 'gc.auto=0', '-c', 'maintenance.auto=false', 'show', pin + ':' + owner], { cwd: root })));
const all = raw.toString();
const first = all.indexOf('create or replace function private.weekly_source_bulk_source_facts_v1(');
const end = all.indexOf('$function$;', first) + '$function$;'.length;
assert.ok(first >= 0 && end > first);
const before = all.slice(first, end);
const a = 'select root.timesheet_id,root.client_id,contract.candidate_id,root.contract_id,root.booking_id';
const b = 'select root.timesheet_id,contract.client_id as client_id,contract.candidate_id,root.contract_id,root.booking_id';
assert.equal(before.split(a).length, 2);
const forward = before.replace(a, () => b);
assert.equal(forward.split(b).length, 2);
assert.equal(forward.replace(b, () => a), before);
const contract = fs.readFileSync(path.join(root, 'supabase/release/current-contract.json'));
assert.ok(contract.equals(execFileSync('git', ['-c', 'gc.auto=0', '-c', 'maintenance.auto=false', 'show', pin + ':supabase/release/current-contract.json'], { cwd: root, maxBuffer: 64e6 })));
const relation = JSON.parse(contract).relations.find(r => r.schema === 'public' && r.name === 'timesheets');
assert.ok(relation && !relation.columns.some(c => ['client_id', 'candidate_id'].includes(c.name)));
const write = (name, value) => {
  const p = path.join(dir, name), data = typeof value === 'string' || Buffer.isBuffer(value) ? Buffer.from(value) : Buffer.from(JSON.stringify(value, null, 2) + '\n');
  assert.equal(fs.existsSync(p), false, 'No overwrite: ' + name);
  fs.writeFileSync(p, data);
};
write('02_complete_current_source_facts_with_contract_client.sqlinc', forward);
write('CURRENT_SOURCE_FACTS_BEFORE.sql.txt', before);
write('CURRENT_SOURCE_CLIENT_FIELD_REVERSE.json', { source_only: true, native: false, signature: 'private.weekly_source_bulk_source_facts_v1(uuid,uuid[])',
  current_commit: pin, current_source_path: owner, current_file_bytes: raw.length, current_file_sha256: sha(raw),
  current_definition_sha256: sha(before), forward_sha256: sha(forward), hunks: [{ before: a, after: b }],
  full_reverse: true, scope_predicates_unchanged: true, footer_hash_guards_unchanged: true, new_signature: false,
  schema_defect: 'Physical public.timesheets has no client_id; derive the unchanged DTO alias from the existing real Contract join.',
  no_null_fallback: true, native_seal: null, mount_ready: false });
write('CURRENT_TIMESHEETS_CONTRACT_SCHEMA.json', { source_only: true, native: false, commit: pin, source_path: 'supabase/release/current-contract.json',
  full_source_sha256: sha(contract), relation, raw_client_column_exists: false, raw_candidate_column_exists: false,
  scope: 'Saved/Git current contract source evidence; no hosted catalogue or native SQL call.' });
const own = path.join(repo, 'docs/healthroster-build/proposals/agency-native-paid-purpose-forward-20261014/construction/21_agency_legacy_original_document.sqlinc');
assert.ok(fs.existsSync(own));
console.log(JSON.stringify({ source_only: true, native: false, current_commit: pin, current_definition_sha256: sha(before), forward_sha256: sha(forward),
  full_reverse_hunks: 1, current_timesheets_has_client_id: false, current_timesheets_has_candidate_id: false,
  original_document_source_sha256: sha(fs.readFileSync(own)), no_schema_or_public_abi_change: true }));

import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
const here = path.dirname(fileURLToPath(import.meta.url));
const source = fs.readFileSync(path.join(here, '01_native_source_dependency_witness.sqlinc'), 'utf8');
const ddl = JSON.parse(fs.readFileSync(path.join(here, 'SOURCE_RAW_ID_DDL_MAPPING.json'), 'utf8'));
const mappings = [...source.matchAll(/\('([a-z_]+)','([a-z_]+)',array\[([^\]]+)\]\)/g)].map(m => ({
  field: m[1], relation: 'public.' + m[2], columns: [...m[3].matchAll(/'([a-z_]+)'/g)].map(x => x[1]),
}));
assert.equal(mappings.length, 32);
for (const mapping of mappings) test('UNIT_ONLY actual declared UUID columns: ' + mapping.relation, () => {
  const native = ddl.tables.find(t => t.relation === mapping.relation);
  assert.ok(native, mapping.relation + ' is in the selected raw DDL corpus');
  const sql = native.ddl.map(d => d.statement).join('\n');
  for (const c of mapping.columns) assert.match(sql,
    new RegExp('(?:^|[,(\\s])' + c + '\\s+(?:pg_catalog\\.)?uuid\\b', 'im'), c + ' must be an actual UUID declaration');
});
const uuid = /^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
function rawIds(rows, column) {
  if (!Array.isArray(rows)) throw new Error('FACTS_UNAVAILABLE');
  const ids = [];
  for (const r of rows) {
    if (!r || typeof r !== 'object' || Array.isArray(r)) throw new Error('RAW_ID_INVALID');
    if (r[column] != null) {
      if (typeof r[column] !== 'string' || !uuid.test(r[column])) throw new Error('RAW_ID_INVALID');
      ids.push(r[column]);
    }
  }
  return [...new Set(ids)].sort();
}
const id = '11111111-1111-1111-1111-111111111111';
test('UNIT_ONLY unbound raw child retains its genuine upload parent without an invented cycle', () => {
  assert.deepEqual(rawIds([{ id, upload_id: id, source_cycle_id: null }], 'upload_id'), [id]);
  const row = mappings.find(m => m.relation === 'public.weekly_source_upload_rows');
  assert.deepEqual(row.columns, ['id', 'upload_id']);
});
test('UNIT_ONLY NULL/unmapped source identity is not a zero UUID or inferred Client', () => {
  assert.deepEqual(rawIds([{ client_id: null }, {}], 'client_id'), []);
});
for (const v of [[], {}, true, 1, 'not-an-id']) test('UNIT_ONLY raw UUID scalar refusal: ' + JSON.stringify(v), () => {
  assert.throws(() => rawIds([{ id: v }], 'id'), /RAW_ID_INVALID/);
});
test('UNIT_ONLY current native facts owns its CAS once; no second potentially changed read is substituted', () => {
  assert.equal((source.match(/facts:=private\.weekly_source_bulk_source_facts_v1\(/g) || []).length, 1);
  assert.match(source, /'WEEKLY_SOURCE_BULK_SOURCE_CAS_V1',facts\)/);
  assert.doesNotMatch(source, /source_cas:=private\.weekly_source_bulk_source_cas/);
});
test('UNIT_ONLY exact all-state incident and current-comparison native selectors', () => {
  assert.match(source, /incident\.source_cycle_id=cycle\.id and incident\.client_id=any\(clients\)/);
  assert.match(source, /incident\.current_comparison_revision_id=comparison\.id/);
  assert.doesNotMatch(source, /incident\.state\s*=\s*'OPEN'/);
});
test('UNIT_ONLY current physical Candidate is derived identity, not falsely declared a raw Timesheet column', () => {
  assert.ok(!mappings.find(m => m.relation === 'public.timesheets').columns.includes('candidate_id'));
  assert.ok(!mappings.find(m => m.relation === 'public.timesheets').columns.includes('client_id'));
  assert.ok(!mappings.find(m => m.relation === 'public.timesheets_financials').columns.includes('contract_id'));
});
test('UNIT_ONLY movements stay the receipt-validated true PAY basis; no B invoice snapshot alias', () => {
  assert.match(source, /jsonb_array_elements\(facts->'pay_receipts'\) receipt/);
  assert.match(source, /\(unit->>'movement_id'\)::uuid/);
  assert.doesNotMatch(source, /invoice_only_snapshot_line_id|retained_paid_match.*zero|sum\(/i);
});
test('UNIT_ONLY typed native selectors keep group/client, coverage and literal singleton separate', () => {
  assert.match(source, /'group_client_pairs'/);
  assert.match(source, /'contract_coverage'/);
  assert.match(source, /'coverage_start',coverage_start,'coverage_end',coverage_end/);
  assert.match(source, /'global_settings_singleton',true/);
  assert.doesNotMatch(source, /00000000-0000-0000-0000-000000000000|'kind','FAMILY_UUID'/);
});
test('UNIT_ONLY read projection creates no retention, writer, queue, qualification or secondary store', () => {
  assert.doesNotMatch(source, /\b(insert into|update public|update private|delete from|create table|create trigger|set_config|pg_advisory|fence_acquire|qualified|withheld|paid_amount)\b/i);
  assert.match(source, /revoke all on function .* from public,anon,authenticated,service_role/);
  assert.match(source, /octet_length\(result::text\)>8388608/);
});
const current = fs.readFileSync(path.join(here, 'CURRENT_SOURCE_FACTS_BEFORE.sql.txt'), 'utf8');
const corrected = fs.readFileSync(path.join(here, '02_complete_current_source_facts_with_contract_client.sqlinc'), 'utf8');
const reverse = JSON.parse(fs.readFileSync(path.join(here, 'CURRENT_SOURCE_CLIENT_FIELD_REVERSE.json'), 'utf8'));
const schema = JSON.parse(fs.readFileSync(path.join(here, 'CURRENT_TIMESHEETS_CONTRACT_SCHEMA.json'), 'utf8'));
const hash = value => crypto.createHash('sha256').update(value).digest('hex');
test('UNIT_ONLY original actual reader references an undeclared physical Timesheet Client column', () => {
  assert.match(current, /select root\.timesheet_id,root\.client_id,contract\.candidate_id/);
  assert.ok(!schema.relation.columns.some(c => c.name === 'client_id'));
  assert.equal(schema.commit, 'fa66183f01913e86651e82816f5e88f97e07123c');
});
test('UNIT_ONLY corrected alias is the existing real Contract Client with the exact existing join', () => {
  assert.match(corrected, /select root\.timesheet_id,contract\.client_id as client_id,contract\.candidate_id/);
  assert.match(corrected, /from public\.timesheets root join public\.contracts contract on contract\.id=root\.contract_id/);
  const contract = ddl.tables.find(t => t.relation === 'public.contracts');
  assert.ok(contract.ddl.some(d => /\bclient_id\s+uuid\b/i.test(d.statement)));
  assert.doesNotMatch(corrected, /root\.client_id|null::uuid as client_id/);
});
test('UNIT_ONLY one unique reverse restores the ENTIRE actual native Source facts owner', () => {
  assert.equal(hash(current), reverse.current_definition_sha256);
  assert.equal(hash(corrected), reverse.forward_sha256);
  assert.equal(reverse.hunks.length, 1);
  const h = reverse.hunks[0];
  assert.equal(corrected.split(h.after).length, 2);
  assert.equal(corrected.replace(h.after, () => h.before), current);
});
test('UNIT_ONLY Source receipt/root/financial provenance and scope predicates remain exact', () => {
  const h = reverse.hunks[0];
  assert.equal(current.replace(h.before, () => h.after), corrected);
  assert.match(corrected, /WEEKLY_SOURCE_BULK_SOURCE_CAS_FINANCIAL_IDENTITY_INVALID/);
  assert.match(corrected, /contract->>'client_id'=financial->>'client_id'/);
  assert.match(corrected, /8388608/);
});
test('UNIT_ONLY the reader repair adds no column, helper, public ABI, fake fallback or mutation', () => {
  assert.equal(reverse.new_signature, false);
  assert.equal(reverse.signature, 'private.weekly_source_bulk_source_facts_v1(uuid,uuid[])');
  assert.doesNotMatch(corrected, /\b(add column|insert into|update public|delete from|create table)\b/i);
  assert.equal((corrected.match(/create or replace function/g) || []).length, 1);
});

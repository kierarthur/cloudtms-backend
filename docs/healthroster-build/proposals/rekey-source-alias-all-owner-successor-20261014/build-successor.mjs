import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';

const dir = path.dirname(fileURLToPath(import.meta.url));
const prior = path.join(path.dirname(dir), 'rekey-source-alias-current-native-successor-20261014');
const sha = b => createHash('sha256').update(b).digest('hex');
assert.ok(!fs.existsSync(path.join(dir, 'FROZEN_ARTIFACT_MANIFEST.json')));
const manifestBytes = fs.readFileSync(path.join(prior, 'FROZEN_ARTIFACT_MANIFEST.json'));
assert.equal(sha(manifestBytes), '44706e1eef850a647458d1af77e4a860520ca60d68801b1b91f88bae6135be07');
const manifest = JSON.parse(manifestBytes);
for (const a of manifest.artifacts) {
  const b = fs.readFileSync(path.join(prior, a.file));
  assert.equal(b.length, a.bytes);
  assert.equal(sha(b), a.sha256);
}
const input = 'forward/00_known_source_key.sqlinc';
const before = fs.readFileSync(path.join(prior, input));
const eol = before.includes(Buffer.from('\r\n')) ? '\r\n' : '\n';
const hunks = [
  {
    name: 'retain all-owner mismatch aggregate',
    before: '  v_cycle public.weekly_source_cycles%rowtype;v_count integer;v_event uuid;',
    after: '  v_cycle public.weekly_source_cycles%rowtype;v_count integer;v_event uuid;v_foreign_owner boolean;'
  },
  {
    name: 'aggregate entire genuine alias namespace before local admission',
    before: '  select count(distinct event.id),min(event.id::text)::uuid into v_count,v_event',
    after: `  select count(distinct event.id),min(event.id::text)::uuid,
    bool_or(event.candidate_id is distinct from p_candidate_id or event.client_id is distinct from p_client_id)
    into v_count,v_event,v_foreign_owner`
  },
  {
    name: 'do not erase foreign Candidate or Client before owner census',
    before: `    and event.candidate_id=p_candidate_id and event.client_id=p_client_id
    and resolution.candidate_id=event.candidate_id and resolution.client_id=event.client_id`,
    after: '    and resolution.candidate_id=event.candidate_id and resolution.client_id=event.client_id'
  },
  {
    name: 'single foreign owner refuses instead of returning new-key NULL',
    before: '  if v_count=0 then return null;end if;\n  return v_event;',
    after: `  if v_count=0 then return null;end if;
  if v_foreign_owner then raise exception 'WEEKLY_SOURCE_SOURCE_KEY_ALIAS_OWNER_CONFLICT' using errcode='55000';end if;
  return v_event;`
  }
].map(h => ({ ...h, before: h.before.replaceAll('\n', eol), after: h.after.replaceAll('\n', eol) }));
let source = before.toString();
for (const h of hunks) {
  assert.equal(source.split(h.before).length, 2, h.name);
  source = source.replace(h.before, h.after);
}
const after = Buffer.from(source);
for (const h of [...hunks].reverse()) {
  assert.equal(source.split(h.after).length, 2, h.name);
  source = source.replace(h.after, h.before);
}
assert.deepEqual(Buffer.from(source), before);
for (const side of ['before', 'forward']) fs.mkdirSync(path.join(dir, side), { recursive: true });
fs.writeFileSync(path.join(dir, 'before/00_known_source_key.sqlinc'), before);
fs.writeFileSync(path.join(dir, 'forward/00_known_source_key.sqlinc'), after);
fs.writeFileSync(path.join(dir, 'COMPLETE_OWNER_REVERSE.json'), JSON.stringify({
  source_only: true,
  controlling_packet: path.basename(prior),
  controlling_manifest_sha256: sha(manifestBytes),
  input,
  file: '00_known_source_key.sqlinc',
  before_bytes: before.length,
  before_sha256: sha(before),
  after_bytes: after.length,
  after_sha256: sha(after),
  hunks,
  unique_hunks: hunks.length,
  complete_owner_reverse_verified: true,
  inherited_three_owner_four_hunk_reversals_unchanged: true,
  canonical_mounted: false,
  native_proof: false
}, null, 2) + '\n');
const selected = [
  { packet: path.basename(dir), file: 'forward/00_known_source_key.sqlinc', bytes: after.length, sha256: sha(after) },
  ...['01_complete_identical_rekey_prior.sqlinc', '02_complete_upload_context.sqlinc', '03_complete_projection_rows.sqlinc'].map(file => {
    const b = fs.readFileSync(path.join(prior, 'forward', file));
    return { packet: path.basename(prior), file: 'forward/' + file, bytes: b.length, sha256: sha(b) };
  })
];
fs.writeFileSync(path.join(dir, 'SELECTED_SOURCE_RECIPE.json'), JSON.stringify({
  source_only: true,
  independent_review: 'PENDING',
  private_signature: 'private.weekly_source_known_source_key_v1(uuid,uuid,uuid)',
  private_acl: 'logical postgres; PUBLIC/anon/authenticated/service_role revoked; owner only',
  selected_source_order: selected,
  excluded_predecessor_leaf: path.basename(prior) + '/forward/00_known_source_key.sqlinc',
  new_signatures_beyond_frozen15: [],
  public_native_abis_unchanged: true,
  shared_banking_owner_changes: 0,
  stores_or_columns_added: 0,
  event_original_key_rewritten: false,
  canonical_mounted: false,
  sql_compiled: false,
  native_proof: false,
  full_paid_overlap_complete: false,
  full_primary_complete: false,
  source_and_native_gates: 'REFUSAL_ABI_AND_BOUNDARY.md'
}, null, 2) + '\n');
fs.writeFileSync(path.join(dir, 'CONTROLLING_INPUT_PINS.json'), JSON.stringify({
  source_only: true,
  own_published_source_commit: 'a67a44319e25e136fa34c61fb6ca4d270d85ceda',
  current_native_input_commit: '33927fff2f7b90b90241d746d9e35aa21c57bd07',
  controlling_packet: path.basename(prior),
  controlling_manifest_sha256: sha(manifestBytes),
  controlling_artifacts_unchanged: manifest.artifact_count,
  controlling_leaves: manifest.artifacts.filter(a => a.file.startsWith('forward/')),
  current_native_pins: JSON.parse(fs.readFileSync(path.join(prior, 'CURRENT_NATIVE_INPUT_PINS.json'))),
  original_finding: 'P1-REKEY1',
  independent_critic_path: 'C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/cloudtms-backend/codex_outputs/claude-stage2-continuation-20260925/HEALTHROSTER_BUILD_13102026/BUILD_MONEY_REKEY_SOURCE_ALIAS_SOURCE_CRITIC_14102026.md',
  independent_critic_sha256: '49b5a4e2580a4e7fabd36f0b0d2f2487960b55e5fe028369ebd96d0dd6bfec19',
  canonical_mounted: false,
  native_proof: false
}, null, 2) + '\n');
console.log(JSON.stringify({ complete_owner_reversals: 1, unique_hunks: hunks.length, after_sha256: sha(after), predecessor_preserved: true }));

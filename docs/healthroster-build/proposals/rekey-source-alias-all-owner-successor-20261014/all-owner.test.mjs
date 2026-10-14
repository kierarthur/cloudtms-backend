import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import test from 'node:test';
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { functionEvents, splitSqlStatements, stripSqlComments, sqlTokens } from '../a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';

const dir = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(dir, '../../../..');
const priorDir = path.join(path.dirname(dir), 'rekey-source-alias-current-native-successor-20261014');
const read = f => fs.readFileSync(path.join(dir, f), 'utf8');
const sha = b => createHash('sha256').update(b).digest('hex');
const known = read('forward/00_known_source_key.sqlinc');
const before = read('before/00_known_source_key.sqlinc');
const context = fs.readFileSync(path.join(priorDir, 'forward/02_complete_upload_context.sqlinc'), 'utf8');
const projection = fs.readFileSync(path.join(priorDir, 'forward/03_complete_projection_rows.sqlinc'), 'utf8');
const error = message => Object.assign(new Error(message), { sqlstate: '55000' });

// Finite source-predicate scaffolding. It evaluates the equality predicates
// selected from the actual saved SELECT over native-shaped relational facts.
// No SQL, database, financial calculation or native hash proof is performed.
const linkHash = link => sha('WEEKLY_SOURCE_WORK_EVENT_LINK_V1|' + JSON.stringify({
  work_event_id: link.work_event_id, upload_row_id: link.upload_row_id,
  resolution_id: link.row_resolution_id, link_kind: link.link_kind
}));
function fixture(owners) {
  const db = { links: [], resolutions: [], rows: [], uploads: [], cycles: [], events: [] };
  for (const [i, owner] of owners.entries()) {
    const event = {
      id: owner.event ?? 'E' + i, candidate_id: owner.candidate ?? 'C1', client_id: owner.client ?? 'L1',
      first_source_group_id: owner.group ?? 'G1', source_format_profile_id: owner.profile ?? 'P1',
      identity_kind: 'PROFILE_EXTERNAL_KEY', profile_external_key: owner.canonical ?? 'A'
    };
    const cycle = { id: 'CY' + i, source_group_id: owner.group ?? 'G1' };
    const upload = { id: 'U' + i, source_cycle_id: cycle.id, source_format_profile_id: owner.profile ?? 'P1' };
    const row = { id: 'R' + i, upload_id: upload.id, external_source_key: owner.key ?? 'B' };
    const resolution = {
      id: 'RR' + i, upload_row_id: row.id, work_event_id: event.id, mapping_state: 'RESOLVED',
      candidate_id: event.candidate_id, client_id: event.client_id
    };
    const link = { work_event_id: event.id, upload_row_id: row.id, row_resolution_id: resolution.id, link_kind: 'WORKED' };
    link.link_hash = linkHash(link);
    db.events.push(event); db.cycles.push(cycle); db.uploads.push(upload);
    db.rows.push(row); db.resolutions.push(resolution); db.links.push(link);
  }
  return db;
}
function sourceLookup(source, db, candidate = 'C1', client = 'L1', options = {}) {
  const v_row = { external_source_key: options.key ?? 'B' };
  const v_cycle = { source_group_id: 'G1' };
  const v_upload = { source_format_profile_id: 'P1' };
  if (!candidate || !client || !options.uploadRowId && options.uploadRowId !== undefined) return null;
  if (!v_row.external_source_key.trim() || options.groupActive === false || options.profileActive === false || options.eligible === false) return null;
  assert.ok(source.indexOf('weekly_source_upload_client_eligible_v1') < source.indexOf('if exists(select 1 from public.weekly_work_events event'));
  if (db.events.some(e => e.first_source_group_id === v_cycle.source_group_id
    && e.source_format_profile_id === v_upload.source_format_profile_id
    && e.identity_kind === 'PROFILE_EXTERNAL_KEY' && e.profile_external_key === v_row.external_source_key)) return null;
  const query = source.match(/from public\.weekly_work_event_source_links link([\s\S]*?)where link\.link_hash=/)?.[1];
  assert.ok(query, 'actual source join query');
  const comparisons = [...query.matchAll(/(\w+\.\w+)\s*=\s*(\w+\.\w+|p_candidate_id|p_client_id|'[^']*')/g)];
  assert.equal(comparisons.length, query.includes('event.candidate_id=p_candidate_id') ? 18 : 16);
  const joined = [];
  for (const link of db.links) for (const resolution of db.resolutions) for (const old_row of db.rows)
    for (const old_upload of db.uploads) for (const old_cycle of db.cycles) for (const event of db.events) {
      const scope = { link, resolution, old_row, old_upload, old_cycle, event, v_row, v_cycle, v_upload };
      const value = expr => {
        if (expr === 'p_candidate_id') return candidate;
        if (expr === 'p_client_id') return client;
        if (expr.startsWith("'")) return expr.slice(1, -1);
        const [alias, field] = expr.split('.');
        assert.ok(scope[alias] && Object.hasOwn(scope[alias], field), expr);
        return scope[alias][field];
      };
      if (comparisons.every(m => value(m[1]) === value(m[2])) && link.link_hash === linkHash(link)) joined.push(event);
    }
  assert.match(source, /count\(distinct event\.id\)/);
  const ids = [...new Set(joined.map(e => e.id))];
  if (ids.length > 1) throw error(source.match(/v_count>1 then raise exception '([^']+)'/)[1]);
  if (ids.length === 0) return null;
  if (/bool_or\(event\.candidate_id is distinct from p_candidate_id or event\.client_id is distinct from p_client_id\)/.test(source)
    && joined.some(e => e.candidate_id !== candidate || e.client_id !== client)) {
    throw error(source.match(/if v_foreign_owner then raise exception '([^']+)'/)[1]);
  }
  return ids[0];
}
function actualCaller(kind, db, candidate, client, source = known, options = {}) {
  const callerSource = kind === 'discovery' ? context : projection;
  const variable = kind === 'discovery' ? 'v_prior_work_event_id' : 'v_identical_rekey_prior';
  const branch = new RegExp(variable + ':=private\\.weekly_source_known_source_key_v1\\([^;]+\\);\\s*if ' + variable
    + ' is null then\\s*' + variable + ':=private\\.weekly_source_identical_rekey_prior_v1\\([^;]+\\);\\s*end if;');
  assert.match(callerSource, branch);
  let fallbackCalls = 0;
  try {
    let result = sourceLookup(source, db, candidate, client, options);
    if (result === null) { fallbackCalls++; result = options.fallback ?? 'ELIGIBLE_OTHER_EVENT'; }
    return { result, fallbackCalls, selected: result !== null };
  } catch (e) {
    const whitelist = kind === 'discovery'
      ? [...context.match(/if SQLERRM not in \(([^)]+)\) then raise; end if;/)[1].matchAll(/'([^']+)'/g)].map(m => m[1]) : [];
    if (e.sqlstate === '55000' && whitelist.includes(e.message)) return { result: null, prior_candidate_count: 2, selected: false, fallbackCalls };
    e.fallbackCalls = fallbackCalls;
    throw e;
  }
}
function isError(code) { return e => e.message === code && e.sqlstate === '55000' && e.fallbackCalls === 0; }
function actualCallerBatch(kind, keys, db, candidate, client) {
  const visited = [];
  for (const key of keys) {
    visited.push(key);
    try { actualCaller(kind, db, candidate, client, known, { key }); }
    catch (e) { e.failedKey = key; e.visited = visited; throw e; }
  }
}

test('all original15 artifacts and manifest remain frozen byte-identical', () => {
  const b = fs.readFileSync(path.join(priorDir, 'FROZEN_ARTIFACT_MANIFEST.json'));
  assert.equal(sha(b), '44706e1eef850a647458d1af77e4a860520ca60d68801b1b91f88bae6135be07');
  const manifest = JSON.parse(b); assert.equal(manifest.artifact_count, 15);
  for (const a of manifest.artifacts) { const raw = fs.readFileSync(path.join(priorDir, a.file)); assert.equal(raw.length, a.bytes); assert.equal(sha(raw), a.sha256); }
});
test('complete helper reverses through four unique hunks and caller three-owner four-hunk proofs stay exact', () => {
  const p = JSON.parse(read('COMPLETE_OWNER_REVERSE.json')); let s = known;
  assert.equal(p.unique_hunks, 4); assert.equal(sha(s), p.after_sha256);
  for (const h of [...p.hunks].reverse()) { assert.equal(s.split(h.after).length, 2); s = s.replace(h.after, h.before); }
  assert.equal(s, before); assert.equal(s, fs.readFileSync(path.join(priorDir, p.input), 'utf8')); assert.equal(sha(s), p.before_sha256);
  const originals = JSON.parse(fs.readFileSync(path.join(priorDir, 'COMPLETE_CURRENT_REVERSES.json')));
  assert.equal(originals.complete_owners, 3); assert.equal(originals.unique_hunks, 4);
  for (const q of originals.proofs) {
    let body = fs.readFileSync(path.join(priorDir, 'forward', q.file), 'utf8');
    for (const h of [...q.hunks].reverse()) { assert.equal(body.split(h.after).length, 2); body = body.replace(h.after, h.before); }
    assert.deepEqual(Buffer.from(body), execFileSync('git', ['show', originals.own_input_commit + ':' + q.input], { cwd: root, maxBuffer: 3e6 }));
  }
});
test('one same private stable read signature fixed search path and revoked ACL; no store, writer or amount classifier', () => {
  assert.equal(splitSqlStatements(known).meta.length, 0);
  const creates = functionEvents(known).events.filter(e => e.kind === 'CREATE'); assert.equal(creates.length, 1);
  let depth = 0; for (const t of sqlTokens(creates[0].body)) { if (t.kind === 'punctuation' && t.value === '(') depth++; if (t.kind === 'punctuation' && t.value === ')') depth--; assert.ok(depth >= 0); } assert.equal(depth, 0);
  assert.match(known, /private\.weekly_source_known_source_key_v1\(\s*p_upload_row_id uuid,p_candidate_id uuid,p_client_id uuid\s*\) returns uuid/);
  assert.match(known, /stable security definer\s*set search_path to 'pg_catalog','pg_temp'/);
  assert.match(known, /owner to postgres/); assert.match(known, /from public,anon,authenticated,service_role/);
  assert.ok(!/\b(insert into|update|delete from|create table|grant|round\(|sum\()\b/i.test(stripSqlComments(known)));
});
test('all-owner census retains every genuine join/hash and matches Candidate/Client only after count', () => {
  const query = known.slice(known.indexOf('from public.weekly_work_event_source_links'), known.indexOf('if v_count>1'));
  assert.ok(!query.includes('event.candidate_id=p_candidate_id') && !query.includes('event.client_id=p_client_id'));
  for (const f of ['resolution.upload_row_id=link.upload_row_id', 'resolution.work_event_id=link.work_event_id', "resolution.mapping_state='RESOLVED'", 'old_row.external_source_key=v_row.external_source_key', 'resolution.candidate_id=event.candidate_id', 'resolution.client_id=event.client_id', 'WEEKLY_SOURCE_WORK_EVENT_LINK_V1']) assert.ok(query.includes(f), f);
  assert.ok(known.indexOf('if v_count>1') < known.indexOf('if v_foreign_owner'));
  assert.match(known, /bool_or\(event\.candidate_id is distinct from p_candidate_id or event\.client_id is distinct from p_client_id\)/);
});
test('original eligibility/profile/canonical order and every byte outside four exact hunks remain', () => {
  const proof = JSON.parse(read('COMPLETE_OWNER_REVERSE.json')); let s = before;
  for (const h of proof.hunks) s = s.replace(h.before, h.after); assert.equal(s, known);
  assert.ok(known.indexOf('weekly_source_upload_client_eligible_v1') < known.indexOf('if exists(select 1 from public.weekly_work_events event'));
  assert.ok(known.indexOf('event.profile_external_key=v_row.external_source_key) then return null') < known.indexOf('select count(distinct event.id)'));
  assert.match(projection, /WEEKLY_SOURCE_PROFILE_EVENT_KEY_CONFLICT/);
});
test('actual current native input bytes/schema and unchanged three selected leaves retain precise pins', () => {
  const inputs = JSON.parse(read('CONTROLLING_INPUT_PINS.json')); assert.equal(inputs.current_native_input_commit, '33927fff2f7b90b90241d746d9e35aa21c57bd07');
  for (const p of inputs.current_native_pins.inputs) { const raw = execFileSync('git', ['show', p.commit + ':' + p.file], { cwd: root, maxBuffer: 4e6 }); assert.equal(raw.length, p.bytes); assert.equal(sha(raw), p.sha256); }
  const recipe = JSON.parse(read('SELECTED_SOURCE_RECIPE.json')); assert.equal(recipe.selected_source_order.length, 4);
  for (const p of recipe.selected_source_order) { const raw = fs.readFileSync(path.join(path.dirname(dir), p.packet, p.file)); assert.equal(raw.length, p.bytes); assert.equal(sha(raw), p.sha256); }
});
test('actual caller branches preserve independent reproof and propagate foreign owner refusal without catch or fallback', () => {
  assert.ok(!context.match(/if SQLERRM not in \(([^)]+)\)/)[1].includes('WEEKLY_SOURCE_SOURCE_KEY_ALIAS_OWNER_CONFLICT'));
  for (const code of ['WEEKLY_SOURCE_REKEY_MATCH_UNPROVEN', 'saved.contract_id', 'v_protected_choice is not null', 'EXACT_DURABLE_LINEAGE']) assert.ok(projection.includes(code));
  for (const kind of ['discovery', 'projection']) assert.equal(actualCaller(kind, fixture([{ event: 'E1' }]), 'C1', 'L1').result, 'E1');
});
test('single local owner and duplicate real link rows return exactly one existing event', () => {
  for (const db of [fixture([{ event: 'E1' }]), fixture([{ event: 'E1' }, { event: 'E1' }])]) {
    assert.equal(sourceLookup(known, db), 'E1');
    for (const kind of ['discovery', 'projection']) assert.deepEqual(actualCaller(kind, db, 'C1', 'L1'), { result: 'E1', fallbackCalls: 0, selected: true });
  }
});
test('actual predecessor foreign Candidate NULL reaches fallback; successor refuses both consumers in both row orders', () => {
  const db = fixture([{ event: 'E1', candidate: 'C1' }]); assert.equal(sourceLookup(before, db, 'C2', 'L1'), null);
  for (const kind of ['discovery', 'projection']) assert.equal(actualCaller(kind, db, 'C2', 'L1', before).result, 'ELIGIBLE_OTHER_EVENT');
  for (const rowOrder of [['B', 'C'], ['C', 'B']]) for (const kind of ['discovery', 'projection']) {
    assert.throws(() => actualCallerBatch(kind, rowOrder, db, 'C2', 'L1'), e => {
      assert.deepEqual(e.visited, rowOrder.slice(0, rowOrder.indexOf('B') + 1));
      return e.failedKey === 'B' && isError('WEEKLY_SOURCE_SOURCE_KEY_ALIAS_OWNER_CONFLICT')(e);
    });
  }
});
test('actual predecessor foreign Client NULL reaches fallback; successor refuses both consumers with native eligibility admitted', () => {
  const db = fixture([{ event: 'E1', client: 'L1' }]); assert.equal(sourceLookup(before, db, 'C1', 'L2'), null);
  for (const kind of ['discovery', 'projection']) assert.equal(actualCaller(kind, db, 'C1', 'L2', before).fallbackCalls, 1);
  for (const rowOrder of [['B', 'C'], ['C', 'B']]) for (const kind of ['discovery', 'projection']) {
    assert.throws(() => actualCallerBatch(kind, rowOrder, db, 'C1', 'L2'), e => {
      assert.deepEqual(e.visited, rowOrder.slice(0, rowOrder.indexOf('B') + 1));
      return e.failedKey === 'B' && isError('WEEKLY_SOURCE_SOURCE_KEY_ALIAS_OWNER_CONFLICT')(e);
    });
  }
});
test('mixed local plus foreign Candidate owner is counted, discovery selects none and reprojection raises', () => {
  for (const owners of [[{ event: 'E1' }, { event: 'E2', candidate: 'C2' }], [{ event: 'E2', candidate: 'C2' }, { event: 'E1' }]]) {
    const db = fixture(owners); assert.equal(sourceLookup(before, db), 'E1');
    assert.deepEqual(actualCaller('discovery', db, 'C1', 'L1'), { result: null, prior_candidate_count: 2, selected: false, fallbackCalls: 0 });
    assert.throws(() => actualCaller('projection', db, 'C1', 'L1'), isError('WEEKLY_SOURCE_SOURCE_KEY_ALIAS_AMBIGUOUS'));
  }
});
test('mixed local plus foreign Client and all-foreign two-owner histories refuse before any local subset', () => {
  for (const owners of [[{ event: 'E1' }, { event: 'E2', client: 'L2' }], [{ event: 'E2', client: 'L2' }, { event: 'E1' }], [{ event: 'E2', candidate: 'C2' }, { event: 'E3', client: 'L2' }]]) {
    const db = fixture(owners);
    assert.equal(actualCaller('discovery', db, 'C1', 'L1').selected, false);
    assert.throws(() => actualCaller('projection', db, 'C1', 'L1'), isError('WEEKLY_SOURCE_SOURCE_KEY_ALIAS_AMBIGUOUS'));
  }
});
test('two same-Candidate/client event owners also remain ambiguous; no min-id selection authority', () => {
  const db = fixture([{ event: 'E1' }, { event: 'E2' }]);
  assert.equal(actualCaller('discovery', db, 'C1', 'L1').prior_candidate_count, 2);
  assert.throws(() => actualCaller('projection', db, 'C1', 'L1'), isError('WEEKLY_SOURCE_SOURCE_KEY_ALIAS_AMBIGUOUS'));
});
test('different group/profile/key facts and non-genuine resolution/hash cannot claim this literal namespace', () => {
  for (const owner of [{ group: 'G2' }, { profile: 'P2' }, { key: 'C' }]) assert.equal(sourceLookup(known, fixture([owner])), null);
  for (const mutate of [db => { db.links[0].link_hash = 'mismatch'; }, db => { db.resolutions[0].mapping_state = 'UNRESOLVED'; }, db => { db.resolutions[0].candidate_id = 'C2'; }, db => { db.resolutions[0].client_id = 'L2'; }]) {
    const db = fixture([{ event: 'E1' }]); mutate(db); assert.equal(sourceLookup(known, db), null);
  }
});
test('no-owner new-key fallback remains; canonical and native eligibility retain predecessor disposition', () => {
  for (const kind of ['discovery', 'projection']) assert.equal(actualCaller(kind, fixture([]), 'C1', 'L1').fallbackCalls, 1);
  const canonical = fixture([{ event: 'E1', candidate: 'C2', canonical: 'B' }]);
  assert.equal(sourceLookup(known, canonical), null); assert.equal(sourceLookup(before, canonical), null);
  const foreign = fixture([{ event: 'E1', candidate: 'C2' }]);
  assert.equal(sourceLookup(known, foreign, 'C1', 'L1', { eligible: false }), null);
  assert.equal(sourceLookup(known, foreign, 'C1', 'L1', { groupActive: false }), null);
  assert.equal(sourceLookup(known, foreign, 'C1', 'L1', { profileActive: false }), null);
});

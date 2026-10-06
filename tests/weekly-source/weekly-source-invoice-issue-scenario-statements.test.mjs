import test from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
const file=new URL('../../supabase/verification/02092026_1833_weekly_source_invoice_issue_validator_v1.sql',import.meta.url);
const source=fs.readFileSync(file,'utf8').replaceAll('\r\n','\n');
const cases=['PROOF','A1','A2A','A2B','A2C','A2D','A3','A4','A5','A6','A7','A8','A9','A10','ORDERING','FORGED'];
const start=source.indexOf('create function pg_temp.iss_wp33_routes(p_case text)');
const end=source.indexOf("select pg_temp.iss_wp33_routes('PROOF');",start);
const helper=source.slice(start,end);
test('all original WP-33 assertions and undo subtransactions remain byte-identical and ordered',()=>{
  const chunks=[...helper.matchAll(/^  when '([A-Z0-9]+)' then\n([\s\S]*?)(?=^  when '|^  else\n)/gm)];
  assert.deepEqual(chunks.map(m=>m[1]),cases);
  assert.equal(crypto.createHash('sha256').update(chunks.map(m=>m[2]).join('').replace(/\n$/, '')).digest('hex'),
    'fd907b7225ff1e750efaecc6b4ba716562435307a92a84e2c0443c3afe3b267a');
});
test('each scenario is its own bounded SQL statement, never grouped into a loop',()=>{
  const calls=[...source.matchAll(/^select pg_temp\.iss_wp33_routes\('([A-Z0-9]+)'\);$/gm)];
  assert.deepEqual(calls.map(m=>m[1]),cases);
  assert.equal([...source.matchAll(/pg_temp\.iss_wp33_routes\(/g)].length,17);
  assert.doesNotMatch(helper,/statement_timeout|lock_timeout/);
  assert.match(helper,/raise exception 'WP33_UNKNOWN_VERIFICATION_CASE'/);
});
test('subjects are pinned once and reused, with no state-dependent reselection per case',()=>{
  assert.match(source,/create temporary table iss_wp33_subjects[\s\S]*?on commit drop;/);
  assert.equal([...source.matchAll(/insert into pg_temp\.iss_wp33_subjects/g)].length,1);
  assert.match(helper,/into strict v_exempt_inv,v_exempt_ts,v_present_inv,v_present_ts,v_contract\n  from pg_temp\.iss_wp33_subjects where singleton;/);
  assert.doesNotMatch(helper,/select s\.invoice_id/);
  assert.match(source.slice(0,start),/subjects\$;\n\n$/);
});

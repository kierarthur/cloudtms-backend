import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
const read = p => fs.readFileSync(new URL('../'+p, import.meta.url),'utf8').replace(/\r\n/g,'\n');
const replacementPath='supabase/repeatable/28082026_1235_candidate_daily_rota_clear_key_precedence_v1.sql';
const proofPath='supabase/verification/28082026_1236_candidate_daily_rota_clear_key_verification.sql';
const original=read('supabase/repeatable/18082026_0131_candidate_daily_first_generation_source_link_v1.sql');
const replacement=read(replacementPath);
const extract=sql => {
  const start=sql.indexOf('create or replace function public.candidate_daily_rota_generation_publish_atomic_v1(');
  const end=sql.indexOf('\n$function$;',start)+'\n$function$;'.length;
  assert.ok(start>=0 && end>start);
  return sql.slice(start,end);
};
test('Rota publication replacement changes only the two JSON extraction precedences',()=>{
  const expected=extract(original)
    .replace("||':'||v_item->>'item_key',160)","||':'||(v_item->>'item_key'),160)")
    .replace("left('clear:'||v_item->>'item_key',160)","left('clear:'||(v_item->>'item_key'),160)");
  assert.equal(extract(replacement),expected);
  assert.doesNotMatch(extract(replacement),/\|\|\s*v_item\s*->>/);
  assert.doesNotMatch(replacement,/pg_catalog\.(coalesce|nullif|least|greatest)\s*\(/i);
});
test('Rota clearing first-use proof is required for both UPGRADE and clean NEW',()=>{
  const release=JSON.parse(read('supabase/release/current-release.json'));
  for(const list of ['verificationFiles','newVerificationFiles']) assert.ok(release[list].includes(proofPath));
  const proof=read(proofPath);
  assert.match(proof,/^begin;/m);
  assert.match(proof,/^rollback;\s*$/m);
  assert.match(proof,/booked',true/);
  assert.match(proof,/system_blocked',true/);
  assert.match(proof,/unrelated availability was changed/);
  assert.match(proof,/replay duplicated the clear operation/);
  assert.match(proof,/service-only privilege boundary changed/);
});

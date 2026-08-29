const assert = require('node:assert/strict');
const test = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const localQuery = require('./fixtures/banking-pay-local-query.cjs');
const oracle = require('./fixtures/banking-pay-legacy-oracle.cjs')();
const enabled = Boolean(process.env.BANKING_MODAL_LOCAL_PSQL);
const literal = value => `'${JSON.stringify(value).replaceAll("'", "''")}'::jsonb`;
const sourcePath=path.resolve(__dirname,'../supabase/repeatable/28082026_1342_banking_pay_modal_visibility.sql');

test('one private visibility gate excludes indefinite snoozes without changing Snoozes or financial tables',()=>{
  const source=fs.readFileSync(sourcePath,'utf8');
  assert.doesNotMatch(source,/pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
  assert.doesNotMatch(source,/\b(?:INSERT INTO|UPDATE public\.|DELETE FROM|SECURITY DEFINER|GRANT EXECUTE)\b/i);
  assert.match(source,/HIDDEN_INDEFINITE_SNOOZE/);
});

test('every existing hidden marker and active indefinite snooze is excluded, while dated and cleared snoozes remain', {skip:!enabled},()=>{
  const rows=[{}, {snooze_state:'NONE'}, {snooze_state:'NOT_SNOOZED'}, {snooze_state:'CLEARED'}];
  for(const marker of ['is_hidden','hidden','hidden_indefinite_snooze','is_indefinitely_snoozed']) {
    for(const value of [true,false,1,0,'true','false','yes','no','on','off',null]) rows.push({[marker]:value});
  }
  for(const key of ['presentation_role','presentationRole','presentation_section','presentationSection','readiness_state','readinessState']) {
    rows.push({[key]:'HIDDEN'}, {[key]:'READY_TO_PAY'});
  }
  for(const state of ['SNOOZED','ACTIVE','INDEFINITE','DATED','NONE','NOT_SNOOZED','CLEARED']) {
    for(const until of ['', '2026-09-04']) {
      rows.push({snooze_state:state,snooze_until_date:until});
      rows.push({snooze_state:{state,snooze_until_date:until}});
      rows.push({blocked_snooze_state:state,snooze_until_date:until});
    }
  }
  const [actual]=localQuery(`SELECT jsonb_agg(private.pay_workbench_modal_hidden_v2(f) ORDER BY ord)
    FROM jsonb_array_elements(${literal(rows)}) WITH ORDINALITY AS data(f,ord);`);
  assert.deepEqual(actual,rows.map(row=>oracle.isHiddenDisplayRow(row)||oracle.getSnoozeInfo(row).isIndefinite));
  const [extra]=localQuery(`SELECT jsonb_agg(private.pay_workbench_modal_hidden_v2(f) ORDER BY ord)
    FROM jsonb_array_elements(${literal([
      {presentation_section:'HIDDEN_INDEFINITE_SNOOZE'},
      {row_json:{snooze_state:'INDEFINITE'}},
      {rowJson:{hidden_indefinite_snooze:true}},
      {case_components:[{snooze_state:'INDEFINITE'}]},
      {snooze_state:{state:'NONE'},row_json:{snooze_state:'INDEFINITE'}}
    ])}) WITH ORDINALITY AS data(f,ord);`);
  // A hidden component must not suppress its entire otherwise-visible parent.
  // Conflicting hidden duplicate payloads fail closed until refreshed.
  assert.deepEqual(extra,[true,true,true,false,true]);
});

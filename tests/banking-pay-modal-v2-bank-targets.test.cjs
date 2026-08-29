const assert=require('node:assert/strict');
const test=require('node:test');
const fs=require('node:fs');
const path=require('node:path');
const {spawnSync}=require('node:child_process');
const root=path.resolve(__dirname,'..');
const enabled=Boolean(process.env.BANKING_MODAL_LOCAL_PSQL);
test('bank target facts are exact, private and read-only; sensitive values are not returned',()=>{
  const source=fs.readFileSync(path.join(root,'supabase/repeatable/28082026_1924_banking_pay_modal_bank_targets.sql'),'utf8');
  assert.match(source,/STABLE SECURITY INVOKER SET search_path TO ''/);
  assert.match(source,/v_finance_cases_register/);assert.match(source,/edit_bank_details_allowed IS TRUE/);
  assert.match(source,/override_reason IS NOT NULL AND n\.override_hash=t\.bank_hash/);
  assert.doesNotMatch(source,/\b(?:INSERT INTO|UPDATE public\.|DELETE FROM|SECURITY DEFINER|GRANT EXECUTE|http_post|net\.)\b/i);
  assert.doesNotMatch(source,/pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
  for(const name of ['account_number','sort_code','beneficiary_name','result_json','meta_json'])assert.ok(!source.includes(name));
});
test('actual current bank owners, one-off route, results and mappings remain separate exact-hash facts',{skip:!enabled},()=>{
  const result=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,['-X','-q','-h','127.0.0.1','-p','55441','-U','postgres',
    '-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1','-f','tests/28082026_1924_banking_pay_bank_targets_runtime.sql'],
    {cwd:root,encoding:'utf8',timeout:60000});
  assert.equal(result.status,0,result.stderr||result.error?.message);
  assert.match(result.stderr,/PASS: bank target exact-owner/);
});

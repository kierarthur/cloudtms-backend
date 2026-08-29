const assert=require('node:assert/strict');
const test=require('node:test');
const fs=require('node:fs');
const path=require('node:path');
const {spawnSync}=require('node:child_process');
const root=path.resolve(__dirname,'..');
const source=fs.readFileSync(path.join(root,'supabase/repeatable/28082026_1708_banking_pay_modal_bank_sources.sql'),'utf8');
test('unchanged legacy bank-only presentation does not require a Ready or Blocked payment',()=>{
  const payee={candidate_id:'10000000-0000-4000-8000-000000006001',payee_entity_kind:'UMBRELLA',
    payee_entity_id:'10000000-0000-4000-8000-000000009991',bank_details_hash:'synthetic-shared-bank-hash',
    pay_channel:'UMBRELLA',blockers:['BLOCKED_NAME_CHECK'],name_check_status:'NEAR_MATCH'};
  const legacy=require('./fixtures/banking-pay-legacy-oracle.cjs')({},'ALL',[],{preview:{payees:[payee]}});
  const rows=legacy.collectPayeeReadinessBlockedLines();
  assert.equal(rows.length,1);
  assert.equal(rows[0].candidate_id,payee.candidate_id);
  assert.equal(rows[0].bank_details_hash,payee.bank_details_hash);
  assert.equal(rows[0].section_amount_display,undefined);
  const clientFilter=require('./fixtures/banking-pay-legacy-oracle.cjs')({client_id:'client-filter'},'ALL');
  assert.equal(clientFilter.rowMatchesActivePayFilters(rows[0],'blocked_for_pay'),true);
  assert.equal(clientFilter.rowMatchesActivePayFilters({...rows[0],client_id:'other-client'},'blocked_for_pay'),false);
});
test('bank-source collection uses current complete authority and keeps shared candidates separate',()=>{
  assert.match(source,/pay_workbench_modal_source_progress_facts_v2/);
  assert.match(source,/f\.source_state='CURRENT'/);
  assert.match(source,/s\.session_version=p_session\.version/);
  assert.match(source,/pay_workbench_modal_eligible_rows_v2/);
  assert.match(source,/NOT private\.pay_workbench_modal_hidden_v2\(r\.row_json\)/);
  assert.match(source,/DISTINCT ON \(p\.candidate_id,p\.bank_row->>'__payee_route_key'\)/);
  assert.match(source,/r\.candidate_id=p\.candidate_id AND r\.bank_row/);
  assert.match(source,/BANKING_PAY_V2_SOURCE_IDENTITY_MISMATCH/);
  assert.doesNotMatch(source,/LIMIT\s+(?:100|25|10)\b/i);
});
test('private bank-source facts contain no write, provider call, extra economic authority or query per candidate',()=>{
  assert.doesNotMatch(source,/\b(?:INSERT INTO|UPDATE public\.|DELETE FROM|SECURITY DEFINER|GRANT EXECUTE|http_post|net\.)\b/i);
  assert.doesNotMatch(source,/pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
  assert.match(source,/STABLE SECURITY INVOKER SET search_path TO ''/);
  assert.match(source,/FROM PUBLIC, anon, authenticated, service_role/);
  const loop=source.slice(source.indexOf('\n  LOOP'),source.indexOf('\n  END LOOP;'));
  assert.doesNotMatch(loop,/\b(?:SELECT|PERFORM|EXECUTE|INSERT|UPDATE|DELETE)\b/);
});
test('rollback-contained complete bank-source collection and all source fences',{skip:!process.env.BANKING_MODAL_LOCAL_PSQL},()=>{
  const result=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,['-X','-q','-h','127.0.0.1','-p','55441','-U','postgres','-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1','-f','tests/28082026_1709_banking_pay_bank_sources_runtime.sql'],{cwd:root,encoding:'utf8',timeout:60000});
  assert.equal(result.status,0,result.stderr||result.error?.message);
  assert.match(result.stderr,/PASS: 105 candidates share one bank owner/);
});

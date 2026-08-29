const assert = require('node:assert/strict');
const test = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const legacyOracle = require('./fixtures/banking-pay-legacy-oracle.cjs');
const localQuery = require('./fixtures/banking-pay-local-query.cjs');
const enabled = Boolean(process.env.BANKING_MODAL_LOCAL_PSQL);
const literal = value => `'${JSON.stringify(value).replaceAll("'", "''")}'::jsonb`;
const sqlPath = path.resolve(__dirname,'../supabase/repeatable/28082026_1333_banking_pay_modal_bank_action_facts.sql');
const candidate = '00000000-0000-4000-8000-000000000031';
const umbrella = '00000000-0000-4000-8000-000000000032';
const rowId = '00000000-0000-4000-8000-000000000033';

test('bank action projection is private, read-only, fingerprint-bound and cannot call a provider', () => {
  const source = fs.readFileSync(sqlPath,'utf8');
  assert.doesNotMatch(source,/pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
  assert.doesNotMatch(source,/\b(?:INSERT INTO|UPDATE public\.|DELETE FROM|SECURITY DEFINER|GRANT EXECUTE|http_post|net\.)\b/i);
  for (const action of ['acceptBankDetails','runBankNameCheck','ensurePayeeMap']) assert.ok(source.includes(action));
  assert.match(source,/REVOKE ALL/);
});

test('exact legacy bank-action guards survive all blocker combinations, owner/hash and name-check states', {skip:!enabled}, () => {
  const oracle = legacyOracle();
  const codes=['BLOCKED_BANK_DETAILS','BLOCKED_UMBRELLA_INACTIVE','BLOCKED_NAME_CHECK','BLOCKED_NO_PAYEE_MAP'];
  const fixtures=[];
  for (let bits=0;bits<16;bits++) for (const kind of ['CANDIDATE','UMBRELLA','UNKNOWN','']) {
    for (const nameStatus of ['','PENDING','PASS','FAIL','NEAR_MATCH','UNAVAILABLE']) {
      for (const override of [true,false]) for (const hash of ['test-fingerprint','']) {
        fixtures.push({payee_entity_kind:kind,payee_entity_id:kind==='UMBRELLA'?umbrella:candidate,
          candidate_id:candidate,preview_row_id:rowId,bank_details_hash:hash,
          name_check_status:nameStatus,name_check_has_override:override,
          blockers:codes.filter((_,index)=>bits&(1<<index))});
      }
    }
  }
  fixtures.push({payee_entity_kind:'CANDIDATE',payee_entity_id:'',bank_details_hash:'x',blockers:['BLOCKED_NAME_CHECK']});
  for(let start=0;start<fixtures.length;start+=300) {
    const batch=fixtures.slice(start,start+300);
    const [actual]=localQuery(`SELECT jsonb_agg(private.pay_workbench_modal_bank_action_v2(f) ORDER BY ord)
      FROM jsonb_array_elements(${literal(batch)}) WITH ORDINALITY AS data(f,ord);`);
    const expected=batch.map(meta=>oracle.renderAcceptBankDetailsButton(meta).match(/data-action="([^"]+)"/)?.[1] || null);
    assert.deepEqual(actual,expected,`bank guard parity starting ${start}`);
  }
  assert.equal(fixtures.length,1537);
});

test('line-specific bank blockers, exact hash and explicit false override never fall back to candidate truth', {skip:!enabled}, () => {
  const metadata={candidate_id:candidate,display_name:'Synthetic Candidate',tms_ref:'SYN-31',
    current_pay_method:'PAYE',is_ready_for_draft:false,blockers:['BLOCKED_NAME_CHECK'],
    payee_entity_kind:'CANDIDATE',payee_entity_id:candidate,bank_details_hash:'candidate-hash',
    name_check_status:'FAIL',name_check_has_override:true,payee_map_present:true};
  const rows=[
    {candidate_id:candidate,preview_row_id:rowId},
    {candidate_id:candidate,blockers:[]},
    {candidate_id:candidate,blockers:null},
    {candidate_id:candidate,blocked_reason_codes:'["blocked_name_check","BLOCKED_NAME_CHECK"]'},
    {candidate_id:candidate,blockedReasonCodes:'blocked_bank_details, blocked_no_payee_map'},
    {candidate_id:candidate,bank_details_hash:' ',payee_bank_hash:'must-not-replace-explicit-whitespace'},
    {candidate_id:candidate,name_check_has_override:false,payee_map_present:false},
    {candidate_id:candidate,payee_context:{blockers:[],name_check_has_override:false,payee_map_present:false}},
    {candidate_id:candidate,payee_context:{blocked_reason_codes:['BLOCKED_NAME_CHECK'],
      payee_entity_kind:'UMBRELLA',payee_entity_id:umbrella,bank_details_hash:'umbrella-hash',name_check_status:'PASS'}},
    {candidate_id:candidate,bank_details_hash_snapshot:'snapshot-hash',snapshot_bank_details_hash:'later-hash'},
    {candidate_id:candidate,rowId,is_ready_for_draft:'yes',current_pay_method:' umbrella '},
    {candidate_id:candidate,payee_context:{blockers:'not json',bank_details_hash_snapshot:'context-snapshot'}},
    {candidate_id:candidate,blockers:['BLOCKED_NAME_CHECK'],payee_context:{blockedReasonCodes:['BLOCKED_NO_PAYEE_MAP']}},
    {candidate_id:candidate,name_check_has_override:null,payee_map_present:null}
  ];
  const oracle=legacyOracle({},'ALL',[metadata]);
  const [actual]=localQuery(`SELECT jsonb_agg(private.pay_workbench_modal_bank_meta_v2(f,${literal(metadata)}) ORDER BY ord)
    FROM jsonb_array_elements(${literal(rows)}) WITH ORDINALITY AS data(f,ord);`);
  const canonical=value=>JSON.parse(JSON.stringify(value));
  assert.deepEqual(actual,rows.map(row=>canonical(oracle.getLineBankActionMeta(row))));
});

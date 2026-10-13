// E-only concrete callbacks. No connection, credentials or default dispatch.
// Root supplies ONE approved isolated connection inside its managed rollback.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {createHash,randomUUID} from 'node:crypto';
import {createRequire} from 'node:module';
import Module from 'node:module';
import * as producer from '../MIXED300_FACTUAL_PRODUCER_V1_20261006/mixed300-fixture-v1-6.mjs';
import {javascriptPins,FENCE_AFTER} from '../UMB_WORK_NO_CASE_NATIVE_V1_20261006/compose.mjs';
import {localBridge} from '../MIXED300_FACTUAL_PRODUCER_V1_20261006/run-mixed300-smoke-v1-7.mjs';
import {createC2SavedCallers,CALLER_PINS} from '../BPAY_NEXT_LOCAL_PAID_RECOVERY_C2_TAIL_V3_20261006.mjs';
import {produceParsedFinal,rawWorkedHtml as rawWorkedCsv} from './artifact-supplier.mjs';
import {resetReadinessDiagnostic,assertUnboundPoint,assertPreparedPoint,assertReadyPoint} from './fixture-readiness.mjs';
const HERE=path.dirname(fileURLToPath(import.meta.url));
const J2=path.resolve(HERE,'../SOURCE_BANKING_JOINED_REVIEW_V2_20261007');
const E=path.dirname(HERE);
const {factualSql,id:producerId,withLocalProducerTransport,W}=producer;
export const sha=x=>createHash('sha256').update(x).digest('hex');
const lit=x=>x==null?'null':"'"+String(x).replaceAll("'","''")+"'";
const json=x=>lit(JSON.stringify(x))+'::jsonb';
const UUID=/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
const CHANNELS=['PAYE','UMBRELLA'];
const lf=x=>x.replaceAll('\r\n','\n');
const once=(text,before,after)=>{assert.equal(text.split(before).length,2,'EXACT_FACTUAL_PREIMAGE');return text.replace(before,after);};
export const SOURCE_PINS=Object.freeze({
  '../MIXED300_FACTUAL_PRODUCER_V1_20261006/mixed300-fixture-api-v7.mjs':'e12d400a79c8bf73c913636ca736d25dd3625a51ab76c4594a541f6b4c722ab4',
  '../MIXED300_FACTUAL_PRODUCER_V1_20261006/mixed300-fixture-v1-6.mjs':'7443578077e6e2891ccda13ea50a741cb8abd8f51fa4c773d052906fe8adcc98',
  '../UMB_WORK_NO_CASE_NATIVE_V1_20261006/compose.mjs':'f7588f5487281fd13a4fa75ad06df9f3e638075dacb423754ed0013750ffff28',
  '../MIXED300_FACTUAL_PRODUCER_V1_20261006/run-mixed300-smoke-v1-7.mjs':'cd9dfb3a2dc93bcde3245c29c23aa1c4468c036d4afb17994f7fcd4a706e3946',
  '../BPAY_NEXT_LOCAL_PAID_RECOVERY_C2_TAIL_V3_20261006.mjs':'3b563836363f642d06476dce66bc227aafedf7c8566dd6c99ac92c8abf8898e4',
  '../SOURCE_BANKING_JOINED_REVIEW_V2_20261007/artifact-sha256.json':'276e068555e2b88389b28c920a44f8303304d04a3cd5a4b28586660f15fe04b4',
});
export function checkSourcePins(){
  for(const [file,expected] of Object.entries(SOURCE_PINS))assert.equal(sha(fs.readFileSync(path.resolve(HERE,file))),expected,file);
  for(const [file,expected] of Object.entries(CALLER_PINS))assert.equal(sha(fs.readFileSync(path.join(W,file))),expected.toLowerCase(),file);
  const manifest=JSON.parse(fs.readFileSync(path.join(J2,'artifact-sha256.json')));
  for(const row of manifest.artifacts)assert.equal(sha(fs.readFileSync(path.join(J2,row.file))),row.sha256,'SEALED_J2:'+row.file);
  return {j2:manifest.artifacts.length,financialCallers:Object.keys(CALLER_PINS).length};
}
export async function loadCurrentProducerOwners(){
  // Reuse the accepted 102-input supplier and its FULL fence inverse. The
  // producer's unchanged actual owner compiler adds the three real test-module
  // inputs from its original 105-input closure; no owner body is substituted.
  const qualified=javascriptPins();
  const original=lf(fs.readFileSync(path.join(E,'MIXED300_FACTUAL_PRODUCER_V1_20261006/mixed300-fixture-api-v7.mjs'),'utf8'));
  const compiler=original.slice(original.indexOf('export async function loadActualOwners('),original.indexOf('// Same process-local adapter pattern'))
    .replace(/^export /,'').trim();
  assert.equal(compiler,lf(producer.loadActualOwners.toString()),'EXACT_UNCHANGED_ACTUAL_PRODUCER_COMPILER');
  const inputs=structuredClone(producer.INPUTS),rows=inputs.javascript.filter(r=>r.path==='broker/src/bpay-next-legacy-fence.js');
  assert.equal(rows.length,1);assert.equal(rows[0].sha256,'5220FFD231B60A8E72BC9C99D7A3439960B47B1A96254C95304724EB80534591');
  assert.equal(qualified.find(r=>r.path===rows[0].path).sha256,FENCE_AFTER);
  // The accepted supplier already pins the real current Worker/read owners,
  // including its known next-paid evidence module. Preserve all original
  // producer-only parser/workbook inputs and union only qualified identities.
  const expected=new Map(inputs.javascript.map(r=>[r.path,r]));
  // index.js is the explicitly pinned stdin owner, not an esbuild imported
  // dependency. Do not count it again as a fabricated metafile input.
  for(const r of qualified)if(r.path!=='broker/src/index.js')expected.set(r.path,{...r});
  inputs.javascript=[...expected.values()].sort((a,b)=>a.path.localeCompare(b.path));
  const sourcePins=Object.fromEntries(Object.entries(producer.SOURCE_PINS).map(([file,hash])=>
    [file,qualified.find(r=>r.path===file)?.sha256??hash]));
  const load=new Function('SOURCE_PINS','sha','fs','path','W','INPUTS','createRequire','Module','HERE','assert',
    'return ('+compiler+');')(sourcePins,producer.sha,fs,path,W,inputs,createRequire,Module,producer.HERE,assert);
  return load(false);
}
const rates=pay=>Object.fromEntries(['paye','umb','charge'].flatMap(f=>['day','night','sat','sun','bh'].map(b=>[f+'_'+b,f==='charge'?40:pay])));

export function actualFacts(spec){
  assert.ok(CHANNELS.includes(spec.from)&&CHANNELS.includes(spec.to));
  assert.ok([14,17,20,23].includes(spec.destinationRate));
  const actor=randomUUID(),umbrella=randomUUID(),agency=randomUUID(),candidate=randomUUID(),group=randomUUID();
  const clients=['A','B'].map(kind=>({id:randomUUID(),name:'JMV '+kind+' '+candidate,family:'MAGNIT'}));
  const contracts=clients.map(client=>({id:randomUUID(),candidate,client:client.id,channel:spec.from,family:'MAGNIT'}));
  const cycles=clients.flatMap((client,i)=>['2026-09-06','2026-09-13'].map((week,generation)=>({
    cycle:randomUUID(),group,client:client.id,week,generation,kind:i===0?'A':'B'})));
  const p={actor,umbrella,agency,clients,contracts,
    candidates:[{id:candidate,channel:spec.from,displayName:'JMV worker '+candidate}],
    groups:[{id:group,client:clients[0].id,family:'MAGNIT'}],batches:[],slots:[],counts:{selected_work:2}};
  let sql=factualSql(p);
  // Explicit saved Staff-name mapping on the ONE freshly inserted Candidate.
  // Keep the real reference trigger and all raw source/approval facts intact.
  const aliasCandidate=p.candidates[0];
  sql=once(sql,'insert into public.candidates(id,display_name,email,active,roles,sort_code,account_number',
    'insert into public.candidates(id,display_name,email,active,roles,sort_code,account_number,account_holder,nhsp_hr_name_aliases');
  const aliasValues=`values(${lit(aliasCandidate.id)},${lit(aliasCandidate.displayName)},${lit('mixed300-candidate-0@example.test')},true,'["RMN"]','000000','00000001'`;
  sql=once(sql,aliasValues,aliasValues+','+lit(aliasCandidate.displayName)+','+json([aliasCandidate.displayName]));
  // Only genuine fixture input facts change; no source/economic/financial rows.
  sql=once(sql,'mixed300-actor@example.test','jmv-'+actor+'@example.test');
  sql=once(sql,'mixed300-candidate-0@example.test','jmv-'+candidate+'@example.test');
  sql=once(sql,'M300_MAGNIT_'+group.slice(-3),'JMV_'+group.replaceAll('-','').toUpperCase());
  // The reused producer's finite numbered client fixture derives its rate-row
  // IDs from the last three digits. These isolated UUID clients need fresh real
  // configuration row IDs; do not emit NaN or collide with another pair.
  for(const client of clients)sql=once(sql,
    `values(${lit(producerId(4000+Number(client.id.slice(-3))))},${lit(client.id)},'RMN',`,
    `values(${lit(randomUUID())},${lit(client.id)},'RMN',`);
  const b=contracts[1];
  const contractValues=`values(${lit(b.id)},${lit(b.candidate)},${lit(b.client)},'2026-01-01','2026-12-31',${lit(b.channel)},${json(rates(20))},`;
  sql=once(sql,contractValues,contractValues.replace(json(rates(20)),json(rates(15))));
  sql+=`insert into public.weekly_source_group_clients(source_group_id,client_id,valid_from,created_by_user_id)
values(${lit(group)},${lit(clients[1].id)},'2026-01-01',${lit(actor)});
insert into public.weekly_source_client_policies(source_group_id,client_id,effective_from,authority_mode,document_mode,self_bill_enabled,self_bill_correction_presentation,source_fixed_expenses_enabled,source_expense_vat_enabled,weekly_rate_classification_method,duration_break_tie_rule,manager_queries_enabled,manager_query_recipient,created_by_user_id)
values(${lit(group)},${lit(clients[1].id)},'2026-01-01','SOURCE_AUTHORITY','CHECK_ONLY',true,'FULL_REVERSAL_REPLACEMENT',true,false,'WHOLE_SHIFT_START_DAY',null,true,'jmv-manager@example.test',${lit(actor)});
`;
  for(const cycle of cycles)sql+=`insert into public.weekly_source_cycles(id,source_group_id,finalisation_week_ending,cutoff_at_utc,scope_client_id)
values(${lit(cycle.cycle)},${lit(group)},${lit(cycle.week)},((${lit(cycle.week)}::date-4)::text||' 18:00:00 Europe/London')::timestamptz,${lit(cycle.client)});
`;
  assert.doesNotMatch(sql.replace(/^--.*$/gm,''),/insert\s+into\s+(?:private\.|public\.(?:timesheets_financials|weekly_source_(?:entitlement_heads|final_revisions|root_authorisations)))|\b(?:update|delete|commit|rollback|disable trigger)\b/i);
  return {p,cycles,sql,spec:{...spec}};
}

export function pointExpression(root,sourceComponentId=null){
  assert.match(root,UUID);
  if(sourceComponentId!==null)assert.match(sourceComponentId,UUID);
  return `(select jsonb_build_object('canonical_work_id',b.canonical_work_id,'source_component_id',b.source_component_id,
    'component_key',b.canonical_component_key,'approval_work_id',v.approval_work_id,'approval_revision_id',v.approval_revision_id,
    'approved_line_id',v.approved_line_id,'head_id',v.head_id,'approval_source_channel',v.approval_source_channel,
    'source_book_state',p.source_book_state,'source_book_seed_kind',p.source_book_seed_kind,'source_book_seed_effect_id',p.source_book_seed_effect_id,
    'native_realised_paye',p.native_realised_paye,'native_realised_umbrella',p.native_realised_umbrella,
    'native_held_paye',p.native_held_paye,'native_held_umbrella',p.native_held_umbrella,
    'source_book_revision',p.source_book_revision,'source_comparison_revision',p.source_comparison_revision,
    'approved_source_ex_vat',l.source_pay_ex_vat,
    'binding_component_present',v.component_state='PRESENT',
     'binding_line_matches',(l.revision_id,l.component_key,l.source_component_id)=(v.approval_revision_id,v.approval_component_key,b.source_component_id),
     'publication_count',case when v.publication_command_id_2 is null then 1 else 2 end,
     'publication1_status',pub1.status,'publication1_phase',pub1.phase,'publication2_status',pub2.status,'publication2_phase',pub2.phase,
     'publication1_tuple_matches',(pub1.command_id,pub1.work_id,pub1.revision_id,pub1.candidate_id)=
       (v.publication_command_id_1,v.publication_work_id_1,v.publication_revision_id_1,b.candidate_id),
     'publication2_tuple_matches',v.publication_command_id_2 is null or
       (pub2.command_id,pub2.work_id,pub2.revision_id,pub2.candidate_id)=
       (v.publication_command_id_2,v.publication_work_id_2,v.publication_revision_id_2,b.candidate_id),
     'approval_current_matches',w.current_revision_id=v.approval_revision_id,
     'approval_applied_matches',w.applied_revision_id=v.approval_revision_id,
     'current_command_matches',exists(select 1 from private.bpay_next_publication cp
       where cp.work_id=w.id and cp.revision_id=w.current_revision_id and cp.candidate_id=b.candidate_id
       and ((cp.command_id,cp.work_id,cp.revision_id)=(v.publication_command_id_1,v.publication_work_id_1,v.publication_revision_id_1)
         or (cp.command_id,cp.work_id,cp.revision_id)=(v.publication_command_id_2,v.publication_work_id_2,v.publication_revision_id_2))),
     'current_entitlement_matches',exists(select 1 from public.weekly_source_entitlement_heads ch
       join public.weekly_source_root_authorisations ca on ca.current_entitlement_head_id=ch.id
         and ca.root_timesheet_id=ch.root_timesheet_id and ca.withdrawn_at_utc is null
       where ch.id=v.head_id and ch.state='COMMITTED_CURRENT' and ch.candidate_id=b.candidate_id
         and ch.root_timesheet_id=w.original_timesheet_id),
     'legacy_balances_zero',(p.realised_source_ex_vat,p.held_source_ex_vat,p.realised_target_ex_vat,p.realised_target_vat,
       p.realised_target_inc_vat,p.held_target_ex_vat,p.held_target_vat,p.held_target_inc_vat)=(0,0,0,0,0,0,0,0),
     'binding_control_state',case when pub1.status='APPLIED' and (v.publication_command_id_2 is null or pub2.status='APPLIED') then 'APPLIED' else 'NOT_READY' end,
    'binding_worker_state',case when pub1.phase='DONE' and (v.publication_command_id_2 is null or pub2.phase='DONE')
      and w.applied_revision_id=v.approval_revision_id then 'DONE' else 'NOT_READY' end,
    'receipt_qualification_state',cr.qualification_state,'target_pay_channel',cr.target_pay_channel,
    'comparable_paid_ex_vat',cr.comparable_paid_ex_vat,'comparable_held_ex_vat',cr.comparable_held_ex_vat,
    'selected_residual_ex_vat',cr.selected_residual_ex_vat,'source_binding_version_id',v.id,
    'original_effect_id',wc.original_effect_id,'original_approved_line_id',wc.original_approved_line_id,
    'original_source_pay_channel',wc.original_source_pay_channel,
    'original_paid_basis_qualified',case when wc.id is not null then
       private.bpay_next_work_collection_source_original_v2(wc) else false end,
    'original_effect_source_ex_vat',e.source_disposition_ex_vat,'original_line_source_ex_vat',ol.source_pay_ex_vat,
    'original_bucket_units',(select sum(d.approved_hours) from private.bpay_next_rate_detail d where d.approved_line_id=ol.id))
  from private.bpay_next_work rootw
  join private.bpay_next_source_component_binding b on b.candidate_id=rootw.candidate_id
  join private.bpay_next_source_component_binding_version v on v.id=b.current_binding_version_id
    and ${sourceComponentId===null?'v.approval_work_id=rootw.id':`b.source_component_id=${lit(sourceComponentId)}::uuid`}
  join private.bpay_next_work w on w.id=v.approval_work_id
  join private.bpay_next_position p on p.work_id=b.canonical_work_id and p.component_key=b.canonical_component_key
  join private.bpay_next_approved_line l on l.id=v.approved_line_id
  join private.bpay_next_publication pub1 on pub1.command_id=v.publication_command_id_1
  left join private.bpay_next_publication pub2 on pub2.command_id=v.publication_command_id_2
  left join private.bpay_next_source_comparison_receipt cr on cr.id=p.source_comparison_receipt_id
  left join private.bpay_next_work_collection wc on wc.work_id=p.work_id and wc.component_key=p.component_key
  left join private.bpay_next_financial_effect e on e.id=wc.original_effect_id
  left join private.bpay_next_approved_line ol on ol.id=wc.original_approved_line_id
  where rootw.original_timesheet_id=${lit(root)}::uuid)`;
}

async function joinedOwner(c){
  const memory=s=>`data:text/javascript;base64,${Buffer.from(s).toString('base64')}`;
  const uploadFile=path.join(W,'broker/src/weekly-source/upload-publication-owner.mjs');
  let upload=lf(fs.readFileSync(uploadFile,'utf8'));
  upload=once(upload,'function calculationFor(row, contract) {','export function calculateWeeklySourceContractRowForServer(row, contract) {');
  upload=once(upload,'function selectionFor(body, ordinal) {','const calculationFor=calculateWeeklySourceContractRowForServer;\nfunction selectionFor(body, ordinal) {');
  upload=upload.replace(/from '(\.\/[^']+)'/g,(_,f)=>`from '${pathToFileURL(path.resolve(path.dirname(uploadFile),f)).href}'`);
  const preview=lf(fs.readFileSync(path.join(J2,'destination-reprice-preview.mjs'),'utf8'))
    .replace("'./upload-publication-owner.mjs'",JSON.stringify(memory(upload)))
    .replace("'./economic-snapshot.js'",JSON.stringify(pathToFileURL(path.join(W,'broker/src/weekly-source/economic-snapshot.js')).href));
  const joined=lf(fs.readFileSync(path.join(J2,'source-banking-joined-review.mjs'),'utf8'))
    .replace("'./destination-reprice-preview.mjs'",JSON.stringify(memory(preview)));
  const {createWeeklySourceBankingJoinedReviewOwner}=await import(memory(joined));
  const rpc=async(name,args)=>{
    if(name==='weekly_source_destination_financial_preview_v2')return c.query(`public.${name}(${lit(args.p_actor)}::uuid,${json(args.p_base_request)},${args.p_calculations===null?'null::jsonb':json(args.p_calculations)},${json(args.p_financial_selections)})`);
    assert.equal(name,'weekly_source_destination_financial_accept_v2');
    return c.query(`public.${name}(${lit(args.p_actor)}::uuid,${json(args.p_base_request)},${json(args.p_calculations)},${json(args.p_financial_selections)},${lit(args.p_review_sha256)},${lit(args.p_idempotency_key)},${lit(args.p_reason)})`);
  };
  return createWeeklySourceBankingJoinedReviewOwner({rpc});
}
const selection=(id,mode,rate=null,partition=null)=>({component_id:id,selected_mode:mode,effective_pay_date:'2026-10-09',
  umbrella_vat_chargeable:false,manual_paid_amount_ex_vat:null,manual_replacement_rate:rate,paid_partition:partition,
  hold_choices:[],case_choice:null,case_hold_choices:[],reason:'Explicit synthetic finite joined fixture choice'});

export async function createActualCallbacks({c}){
  assert.equal(typeof c?.query,'function');assert.equal(typeof c?.sql,'function');
  resetReadinessDiagnostic();checkSourcePins();const owners=await loadCurrentProducerOwners(),office=await joinedOwner(c);
  const pairs=new Map();let helperInstalled=false;
  const readPoint=async(pair,kind)=>{assert.ok(['SOURCE','OWN_B'].includes(kind));
    const result=await c.query(pointExpression(kind==='SOURCE'?pair.sourceRoot:pair.destinationRoot,
      kind==='SOURCE'?pair.sourceComponentId??null:pair.ownBComponentId??null));
    assert.ok(result&&UUID.test(result.source_component_id),'ONE_CURRENT_BOUND_COMPONENT');return result;};
  const drainApproval=async(state,root)=>{
    const current=await c.query(`(select jsonb_build_object('work',w.id,'revision',w.current_revision_id,'amount',r.approved_source_ex_vat,'command',p.command_id)
      from private.bpay_next_work w join private.bpay_next_work_revision r on r.id=w.current_revision_id
      join private.bpay_next_publication p on p.work_id=w.id and p.revision_id=r.id where w.original_timesheet_id=${lit(root)}::uuid)`);
    assert.ok(UUID.test(current?.command));await state.api.enrollCommand(current.command,'POSITION_APPLY');return current;
  };
  const preparedProof=async(pair,point,run)=>c.query(
    `(with selected_worker as (select id from private.bpay_next_run_worker where run_id=${lit(run)}::uuid and candidate_id=${lit(pair.candidate)}::uuid),
      captures as (select rp.* from private.bpay_next_run_position rp join selected_worker sw on sw.id=rp.run_worker_id
        where rp.work_id=${lit(point.approval_work_id)}::uuid and rp.component_key=${lit(point.component_key)}),
      lines as (select rl.* from private.bpay_next_run_line rl join selected_worker sw on sw.id=rl.run_worker_id
        where rl.work_id=${lit(point.approval_work_id)}::uuid and rl.component_key=${lit(point.component_key)}),
      holds as (select h.* from private.bpay_next_hold h join lines rl on rl.id=h.run_line_id)
      select jsonb_build_object('worker_count',(select count(*) from selected_worker),'capture_count',(select count(*) from captures),
        'line_count',(select count(*) from lines),'hold_count',(select count(*) from holds),
        'captured_tuple_matches',exists(select 1 from captures rp
          where (rp.work_id,rp.captured_revision_id,rp.approved_line_id,rp.component_key,rp.source_binding_version_id,rp.canonical_source_work_id,rp.canonical_source_component_key)=
            (${lit(point.approval_work_id)}::uuid,${lit(point.approval_revision_id)}::uuid,${lit(point.approved_line_id)}::uuid,${lit(point.component_key)},
             ${lit(point.source_binding_version_id)}::uuid,${lit(point.canonical_work_id)}::uuid,${lit(point.component_key)})),
        'captured_zero_balances',exists(select 1 from captures rp where
          (rp.realised_source_ex_vat,rp.held_source_ex_vat,rp.realised_target_ex_vat,rp.realised_target_vat,rp.realised_target_inc_vat,
           rp.held_target_ex_vat,rp.held_target_vat,rp.held_target_inc_vat)=(0,0,0,0,0,0,0,0)
          and rp.approved_source_ex_vat=${lit(point.approved_source_ex_vat)}::numeric and rp.residual_source_ex_vat=rp.approved_source_ex_vat),
        'captured_book_matches',exists(select 1 from captures rp where rp.source_book_revision=1 and rp.source_comparison_receipt_id is null),
        'frozen_line_matches',exists(select 1 from captures rp join lines rl on rl.run_work_id=rp.run_work_id
          where (rl.work_id,rl.captured_revision_id,rl.approved_line_id,rl.component_key,rl.source_binding_version_id,rl.canonical_source_work_id,rl.canonical_source_component_key,rl.source_book_revision)=
            (rp.work_id,rp.captured_revision_id,rp.approved_line_id,rp.component_key,rp.source_binding_version_id,rp.canonical_source_work_id,rp.canonical_source_component_key,rp.source_book_revision)
          and rl.source_pay_channel=rp.source_pay_channel and rl.target_pay_channel=rp.source_pay_channel
          and rl.source_consumed_ex_vat=rp.residual_source_ex_vat and rl.frozen_ex_vat=rp.residual_source_ex_vat
          and rl.frozen_vat=0 and rl.frozen_inc_vat=rl.frozen_ex_vat and rl.updates_original_source_scalar=true),
        'reservation_matches_frozen_line',exists(select 1 from lines rl join holds h on h.run_line_id=rl.id
          join private.bpay_next_position p on p.work_id=rl.canonical_source_work_id and p.component_key=rl.canonical_source_component_key
          join private.bpay_next_source_component_binding b on b.canonical_work_id=p.work_id and b.canonical_component_key=p.component_key
          where b.current_binding_version_id=rl.source_binding_version_id and h.status='ACTIVE'
            and (h.work_id,h.component_key,h.source_reserved_ex_vat,h.target_amount_ex_vat,h.target_amount_vat,h.target_amount_inc_vat)=
              (rl.work_id,rl.component_key,rl.source_consumed_ex_vat,rl.frozen_ex_vat,rl.frozen_vat,rl.frozen_inc_vat)
            and p.source_book_state='READY' and p.source_book_seed_kind='ZERO' and p.source_book_seed_effect_id is null
            and p.source_book_revision=rl.source_book_revision+1 and p.realised_source_ex_vat=0
            and (p.held_source_ex_vat,p.held_target_ex_vat,p.held_target_vat,p.held_target_inc_vat)=
              (h.source_reserved_ex_vat,h.target_amount_ex_vat,h.target_amount_vat,h.target_amount_inc_vat)
            and p.native_realised_paye=0 and p.native_realised_umbrella=0
            and ((rl.source_pay_channel='PAYE' and p.native_held_paye=h.source_reserved_ex_vat and p.native_held_umbrella=0)
              or (rl.source_pay_channel='UMBRELLA' and p.native_held_umbrella=h.source_reserved_ex_vat and p.native_held_paye=0)))))`);
  const pay=async(pair,kind,amount)=>{
    const state=pairs.get(pair),point=await readPoint(pair,kind);
    assertUnboundPoint(point);
    assert.equal(point.binding_control_state,'APPLIED');assert.equal(point.binding_worker_state,'DONE');
    assert.equal(Number(point.approved_source_ex_vat),amount,'FULL_CURRENT_HEAD_AMOUNT_BEFORE_PAYMENT');
    assert.equal(await c.query(`(select state='COMMITTED_CURRENT' from public.weekly_source_entitlement_heads where id=${lit(point.head_id)}::uuid)`),true);
    const run=randomUUID(),command=randomUUID(),api=state.api,channel=point.approval_source_channel;
    await api.preparation('CREATE',{run_id:run,pay_date:'2026-10-09'});
    const work={run_id:run,page_no:'1',choices:[{work_id:point.approval_work_id,expected_revision_id:point.approval_revision_id,mode:'ALL'}]};
    await api.preparation('WORK_PAGE',work);assert.equal((await api.preparation('WORK_PAGE',work)).replay,true);
    await api.preparation('SEAL_ENQUEUE',{run_id:run,command_id:command,expected_pages:'1',expected_count:'1'});
    await api.enrollCommand(command,'PREPARE');
    const prepared=await readPoint(pair,kind);assertPreparedPoint(prepared,await preparedProof(pair,point,run));
    const review=await api.preparation('REVIEW',{run_id:run});assert.equal(review.all_ready,true);
    assert.deepEqual((await api.preparation('CONFIRM',{run_id:run,review_revision:review.review_revision})).drafts,[{run_id:run,pay_channel:channel}]);
    const page=await api.page('CHANNEL_WORKERS',{run_id:run,pay_channel:channel,limit:'100'});
    assert.equal(page.complete,true);assert.equal(page.rows.length,1);assert.equal(page.rows[0].candidate_id,pair.candidate);
    const worker=page.rows[0].run_worker_id;
    assert.equal(Number(page.rows[0].frozen_ex_vat),amount);assert.equal(Number(page.rows[0].frozen_vat),0);
    let net=null;
    if(channel==='PAYE'){
      net={command_id:randomUUID(),run_worker_id:worker,entered_paye_net:amount.toFixed(2)};
      await api.financial('PAYE_NET',net);await api.enrollCommand(net.command_id,'PAYE_NET_ENTRY');
    }else assert.equal(channel,'UMBRELLA'); // Genuine UM path has no PAYE_NET.
    const build={command_id:randomUUID(),run_worker_id:worker};
    const transfer=await api.financial('TRANSFER_BEGIN',build);await api.enrollCommand(build.command_id,'TRANSFER_BUILD');
    assert.equal(await c.query(`(select cash_amount=${amount} and status='MEMBERS_READY' from private.bpay_next_transfer where id=${lit(transfer.transfer_id)}::uuid)`),true);
    const issue={instruction_id:randomUUID(),transfer_id:transfer.transfer_id},csv=await api.financial('CSV_ISSUE',issue);
    assert.equal(csv.phase,'ISSUED_CSV');assert.equal(csv.payment_recorded,false);
    const settle={command_id:randomUUID(),transfer_id:transfer.transfer_id,receipt_id:'JMV-LOCAL-'+randomUUID(),
      amount:amount.toFixed(2),occurred_at:'2026-10-09T12:00:00Z'};
    await api.financial('CSV_SETTLEMENT',settle);await api.enrollCommand(settle.command_id,'CSV_SETTLEMENT');
    const paid=await readPoint(pair,kind);assertReadyPoint(paid,'POST_SETTLEMENT');
    assert.equal(Number(paid[channel==='PAYE'?'native_realised_paye':'native_realised_umbrella']),amount);
    assert.equal(Number(paid[channel==='PAYE'?'native_realised_umbrella':'native_realised_paye']),0);
    assert.equal(Number(paid.native_held_paye),0);assert.equal(Number(paid.native_held_umbrella),0);
    assert.equal(paid.source_book_seed_kind,'ZERO');assert.equal(paid.source_book_seed_effect_id,null);
    assert.equal(paid.original_paid_basis_qualified,true);assert.equal(Number(paid.original_effect_source_ex_vat),amount);
    assert.equal(Number(paid.original_line_source_ex_vat),amount);assert.equal(Number(paid.original_bucket_units),1);
    assert.equal((await api.financial('TRANSFER_BEGIN',build)).replay,true);
    if(net)assert.equal((await api.financial('PAYE_NET',net)).replay,true);
    assert.equal((await api.financial('CSV_ISSUE',issue)).csv_text,csv.csv_text);
    assert.equal((await api.financial('CSV_SETTLEMENT',settle)).replay,true);
    assert.deepEqual(await readPoint(pair,kind),paid,'EXACT_ORIGINAL_PAYMENT_REPLAY');
    return {effect_id:paid.original_effect_id,approved_line_id:paid.original_approved_line_id,
      run_id:run,worker_id:worker,transfer_id:transfer.transfer_id,instruction_id:issue.instruction_id,settlement:settle};
  };
  return {joinedOwner:office,readPoint,
    async createActualPair(spec){
      const facts=actualFacts(spec);await c.sql(facts.sql);
      if(!helperInstalled){await c.sql(fs.readFileSync(path.join(E,'SOURCE_JOINED_FIXTURE_HELPER_SYNTAX_V1_20261007/joined-native-owner-seams.sql'),'utf8'));helperInstalled=true;}
      const bridge=await localBridge(c,facts.p),api=createC2SavedCallers(bridge.connection,facts.p.actor);
      const state={...facts,bridge,api,roots:{},finals:{}};
      const pair={actor:{id:facts.p.actor},candidate:facts.p.candidates[0].id,sourceRoot:null,destinationRoot:null};pairs.set(pair,state);
      const env={SUPABASE_URL:'https://mixed300.invalid',SUPABASE_SERVICE_ROLE_KEY:'synthetic-local-only'};
      for(const kind of ['A','B'])for(const generation of [0,1]){
        const cycle=facts.cycles.find(x=>x.kind===kind&&x.generation===generation),ordinal=kind==='A'?0:1;
        const fact={slot:kind,candidate:pair.candidate,contract:facts.p.contracts[ordinal].id,
          client:cycle.client,date:kind==='A'?'2026-09-01':'2026-09-02',week:'2026-09-06'};
        const artifact=rawWorkedCsv({requestId:'JMV-'+pair.candidate+'-'+kind,staff:facts.p.candidates[0].displayName,
          workDate:fact.date,bootstrap:generation===0});
        const produced=await withLocalProducerTransport(bridge.fetchImpl,()=>produceParsedFinal({p:facts.p,
          b:{...cycle,rows:[fact]},artifact,owners,env,dataRpc:bridge.dataRpc,qualifyProducedRoot:bridge.qualifyProducedRoot,first:generation===0}));
        if(generation===0){state.roots[kind]=produced.root_timesheet_id;await drainApproval(state,produced.root_timesheet_id);}
        else{
          assert.equal(produced.root_timesheet_id,state.roots[kind],'REAL_LATER_FINAL_REUSES_ROOT');
          const app=await c.query(`pg_temp.jmv_approve_parsed_later_final(${lit(facts.p.actor)}::uuid,${lit(produced.root_timesheet_id)}::uuid,
            ${lit(produced.final_revision_id)}::uuid,${lit('jmv-app-'+pair.candidate+'-'+kind)})`);
          assert.equal(app.reply.published,true);await drainApproval(state,produced.root_timesheet_id);state.finals[kind]=produced.final_revision_id;
        }
      }
      pair.sourceRoot=state.roots.A;pair.destinationRoot=state.roots.B;
      const source=await readPoint(pair,'SOURCE'),ownB=await readPoint(pair,'OWN_B');
      assert.equal(Number(source.approved_source_ex_vat),20);assert.equal(Number(ownB.approved_source_ex_vat),15);
      pair.sourceComponentId=source.source_component_id;pair.ownBComponentId=ownB.source_component_id;
      state.ownBPayment=await pay(pair,'OWN_B',15);return pair;
    },
    async settleFirstPayment(pair,channel,amount){
      const state=pairs.get(pair);assert.ok(state);assert.equal(channel,state.spec.from);assert.equal(amount,20);
      const paid=await pay(pair,'SOURCE',20),b=state.p.contracts[1];
      // Actual destination configuration facts, never an old approved snapshot,
      // native balance, clock, economic row or financial history rewrite.
      await c.sql(`update public.contracts set pay_method_snapshot=${lit(state.spec.to)},rates_json=${json(rates(state.spec.destinationRate))}
        where id=${lit(b.id)}::uuid and candidate_id=${lit(pair.candidate)}::uuid;
        update public.candidates set pay_method=${lit(state.spec.to)},umbrella_id=${lit(state.p.umbrella)}::uuid where id=${lit(pair.candidate)}::uuid;`);
      return paid;
    },
    async makeActualMoveRequest(pair){const state=pairs.get(pair);assert.ok(state);
      return c.query(`pg_temp.jmv_genuine_move_request(${lit(pair.actor.id)}::uuid,${lit(pair.sourceRoot)}::uuid,
        ${lit(pair.destinationRoot)}::uuid,${lit(state.finals.A)}::uuid)`);},
    async explicitReviewedSelections(pair,spec,baseRequest){
      const a=await readPoint(pair,'SOURCE'),b=await readPoint(pair,'OWN_B');
      const first=[selection(a.source_component_id,spec.from===spec.to?'NATIVE_IDENTITY':'SUGGESTED_EQUIVALENT_BASIS'),
        selection(b.source_component_id,spec.from===spec.to?'NATIVE_IDENTITY':'SUGGESTED_EQUIVALENT_BASIS')];
      if(spec.from===spec.to)return first;
      // Unaccepted suggested preview exposes the exact server-owned immutable
      // original partition. Only the separately shown explicit manual17 choice
      // is returned for Source acceptance; no units are reconstructed from money.
      const preview=await office.preview({actor:pair.actor,baseRequest,financialSelections:first});
      const context=preview.joined_review.financial_context.components.find(x=>x.component_id===a.source_component_id);
      assert.equal(context.paid_partition.kind,'EXACT_FULL_PAID_ORIGIN');assert.equal(Number(context.paid_partition.units),1);
      assert.equal(context.paid_partition.effect_id,a.original_effect_id);
      return [selection(a.source_component_id,'MANUAL_REPLACEMENT_RATE',17,context.paid_partition),first[1]];
    },
    async driveActualAcceptance(pair){
      const state=pairs.get(pair),commands=await c.query(`(select coalesce(jsonb_agg(distinct j.id order by j.id),'[]') from (
        select v.publication_command_id_1 id from private.bpay_next_source_component_binding b
        join private.bpay_next_source_component_binding_version v on v.id=b.current_binding_version_id where b.candidate_id=${lit(pair.candidate)}::uuid
        union select v.publication_command_id_2 from private.bpay_next_source_component_binding b
        join private.bpay_next_source_component_binding_version v on v.id=b.current_binding_version_id where b.candidate_id=${lit(pair.candidate)}::uuid) j where j.id is not null)`);
      assert.ok(commands.length>=1&&commands.length<=2);
      const sequence=await c.query(`(select jsonb_agg(id order by agency_sequence) from private.bpay_next_command where id in (${commands.map(lit).join(',')}))`);
      for(const command of sequence)await state.api.enrollCommand(command,'POSITION_APPLY');
      return {source:await readPoint(pair,'SOURCE'),own_b:await readPoint(pair,'OWN_B')};
    },
    // Root owns signed/repeated/Case/held tails. Actual api and factual identity
    // are exposed directly, not replaced by another abstract operation callback.
    actualState(pair){assert.ok(pairs.has(pair));return pairs.get(pair);},
  };
}

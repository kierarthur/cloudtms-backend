// Source-only finite successors. No database client, execution adapter or latest-owner selection.
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {mkdirSync,writeFileSync,readFileSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
const base=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(base,'../../../..');
const sha=value=>createHash('sha256').update(value).digest('hex');
const native='e4661d28219b85c66b854d99c6522b55aae33139';
const source='d863577918ce7d439206296e91ad78de298e98b2';
const invoice='c86a1dfcf0098a26b6f0eac085c95c9b08f81074';
const candidate='975376fd97855248c76b0ccbe027eed9b50f25ff';
const joined='f5668bf45854ca36b47e713c0e8f3dd3ae4b852b';
const invoicePending='b45eae27977007991ba449c1345890c7a4ee5f85';
const sourceBase='docs/healthroster-build/proposals/source-pay-decision-v5-foundation-20261015';
const git=(commit,file)=>execFileSync('git',['show',`${commit}:${file}`],{cwd:root,encoding:'utf8',maxBuffer:8e6});
function complete(text,name){
 const marker=new RegExp('create or replace function '+name.replace(/[.*+?^${}()|[\]\\]/g,'\\$&')+'\\s*\\(','ig');
 const matches=[...text.matchAll(marker)];if(matches.length!==1)throw Error(`Expected one ${name}, got ${matches.length}`);
 const start=matches[0].index,open=text.indexOf('$function$',start),end=text.indexOf('$function$;',open+10);
 if(open<0||end<open)throw Error('Missing complete function '+name);
 return text.slice(start,end+12).trimEnd()+'\n';
}
const inputs=[],outputs=[],deltas=[];
function load(commit,file,name){const full=git(commit,file),body=complete(full,name);inputs.push({commit,path:file,signature:name,
 whole_file_sha256:sha(full),complete_function_sha256:sha(body),header_sha256:sha(body.slice(0,body.indexOf('$function$')))});return body;}
function publish(file,name,body,parent,changes){
 if(parent){let reverse=body;for(const d of changes.toReversed()){
  if(reverse.split(d.to).length!==2)throw Error('Nonunique reverse '+name+': '+d.reason);reverse=reverse.replace(d.to,d.from);
 }if(reverse!==parent)throw Error('Whole native reverse mismatch '+name);}
 const output=path.join(base,file);mkdirSync(path.dirname(output),{recursive:true});writeFileSync(output,body);
 const entry={path:file,signature:name,sha256:sha(body),bytes:Buffer.byteLength(body),header_sha256:sha(body.slice(0,body.indexOf('$function$'))),
 full_reverse_equals_input:parent?true:null,deltas:changes};outputs.push(entry);return entry;
}
function edit(parent){let body=parent;const changes=[];return {change(from,to,reason){
 if(body.split(from).length!==2)throw Error('Nonunique input seam '+reason);body=body.replace(from,to);changes.push({reason,from,to});
},get body(){return body;},changes};}
// This exact accepted original-A correlation supplements only the original CW
// pointer conjunct; it does not classify every original-eligible A as Source.
const originalCorrelation=`exists(
    select 1 from private.timesheet_contract_move_operations operation
    join private.timesheet_contract_move_members member on member.operation_id=operation.id
    join public.timesheets original on original.timesheet_id=p_timesheet_id
    join public.contracts original_contract on original_contract.id=original.contract_id
    where operation.id=v_alias.accepted_operation_id
      and (operation.state in('COMPLETE','SUPERSEDED') or private.timesheet_contract_move_accepting_v1(operation.id))
      and operation.purpose='CONTRACT_CORRECTION' and operation.accepted_review_id is not null
      and operation.invoice_authority in('SOURCE_FINANCIAL','MIXED')
      and operation.family_root_id=v_alias.family_root_id
      and operation.candidate_id=original_contract.candidate_id and operation.actual_client_id=original_contract.client_id
      and member.member_key=v_alias.member_key and member.family_root_id=v_alias.family_root_id
      and member.role=v_alias.member_role and member.from_timesheet_id=p_timesheet_id
      and member.from_version=original.version and member.from_contract_id is not distinct from original.contract_id
      and member.original_booking_id=original.booking_id
  )`;
{
 const name='private._candidate_expense_source_family_v1',parent=load(native,'supabase/repeatable/26092026_0202_banking_pay_stage2_source_authorisation_v1.sql',name),e=edit(parent);
 e.change('  v_source boolean;','  v_source boolean;\n  v_pending jsonb;v_physical jsonb;v_original_invoice boolean:=false;\n  v_alias private.timesheet_contract_family_aliases%rowtype;','Declared exact native pending/original witnesses');
 e.change('BEGIN\n  SELECT EXISTS (',`BEGIN
  -- The accessor validates the complete native withdrawal/CW/workflow proof.
  -- A registered revoked B cannot become an ordinary claim by returning false.
  v_pending:=private.timesheet_contract_native_pending_for_v1(p_timesheet_id);
  select alias.* into v_alias from private.timesheet_contract_family_aliases alias where alias.timesheet_id=p_timesheet_id;
  if found then
    v_physical:=private.timesheet_contract_current_placement_v1(p_timesheet_id);
    if v_pending is not null and v_alias.original_timesheet_id is distinct from p_timesheet_id then
      raise exception 'WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE' using errcode='55000';end if;
    if v_alias.original_timesheet_id=p_timesheet_id
      and v_physical->>'requested_timesheet_id'=p_timesheet_id::text
      and v_physical->>'family_root_id'=v_alias.family_root_id::text
      and v_physical->'legacy_origin'='false'::jsonb then
      v_original_invoice:=private.timesheet_contract_source_original_eligible_v1(p_timesheet_id)
        and ${originalCorrelation};
    end if;
  end if;
  SELECT EXISTS (`,'Certified pending refusal before ordinary ownership fallback; exact original-A invoice correlation');
 e.change(`      AND EXISTS (
        SELECT 1 FROM public.contract_weeks cw
        WHERE cw.contract_id=t.contract_id AND cw.week_ending_date=t.week_ending_date
          AND cw.additional_seq=0 AND NOT cw.is_adjustment
          AND cw.timesheet_id=t.timesheet_id
      )`,`      AND (EXISTS (
        SELECT 1 FROM public.contract_weeks cw
        WHERE cw.contract_id=t.contract_id AND cw.week_ending_date=t.week_ending_date
          AND cw.additional_seq=0 AND NOT cw.is_adjustment
          AND cw.timesheet_id=t.timesheet_id
      ) OR v_original_invoice)`,'Only original CW-current pointer may use exact immutable original-A invoice authority');
 publish('forward/01_source_family.sqlinc',name,e.body,parent,e.changes);
}
{
 const name='private.weekly_source_resolve_root_identity_v1',parent=load(native,'supabase/repeatable/17092026_0200_weekly_source_rotation_authority_v1.sql',name),e=edit(parent);
 e.change('  with scope as (',`  -- Actual native pending has no current row. Never let the old booking's
  -- canonical representative or historical original A masquerade as current.
  if private.timesheet_contract_native_pending_for_v1(p_timesheet_id) is not null then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE',
      'reason','CURRENT_ROW_CARDINALITY','timesheet_id',p_timesheet_id,
      'family_booking_id',(select t.booking_id from public.timesheets t where t.timesheet_id=p_timesheet_id)
    );
  end if;

  with scope as (`,'Exact certified pending failure before native booking resolver');
 publish('forward/02_root_identity.sqlinc',name,e.body,parent,e.changes);
}
{
 const name='private.weekly_source_lock_family_rows_v1',parent=load(native,'supabase/repeatable/17092026_0200_weekly_source_rotation_authority_v1.sql',name),e=edit(parent);
 e.change('  v_failure text;','  v_failure text;\n  v_requested uuid;v_physical jsonb;\n  v_physical_before jsonb:=\'{}\'::jsonb;v_physical_after jsonb:=\'{}\'::jsonb;','Exact per-request H103 witnesses');
 const pre=`  -- Native Candidate serial admission remains with the existing caller.
  -- Read only certified direct aliases here; preserve the native booking/row
  -- lock order below and compare all eight H103 facts after those waits.
  for v_requested in select id from pg_catalog.unnest(v_ids) id order by id loop
    if private.timesheet_contract_native_pending_for_v1(v_requested) is not null then
      return pg_catalog.jsonb_build_object(
        'ok',false,'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE','retryable',false,
        'reason','CURRENT_ROW_CARDINALITY');
    end if;
    if exists(select 1 from private.timesheet_contract_family_aliases alias where alias.timesheet_id=v_requested) then
      v_physical:=private.timesheet_contract_current_placement_v1(v_requested);
      v_physical_before:=v_physical_before||pg_catalog.jsonb_build_object(v_requested::text,v_physical);
    end if;
  end loop;

`;
 e.change('  -- Step 2 (first half): the booking identity of every requested row, read',pre+'  -- Step 2 (first half): the booking identity of every requested row, read','Pending preflight and full registered H103 discovery before existing locks');
 e.change('  -- Step 3: the booking identity must be unchanged since step 2.',`  for v_requested in select id from pg_catalog.unnest(v_ids) id order by id loop
    if private.timesheet_contract_native_pending_for_v1(v_requested) is not null then
      return pg_catalog.jsonb_build_object(
        'ok',false,'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE','retryable',false,
        'reason','CURRENT_ROW_CARDINALITY');
    end if;
    if exists(select 1 from private.timesheet_contract_family_aliases alias where alias.timesheet_id=v_requested) then
      v_physical:=private.timesheet_contract_current_placement_v1(v_requested);
      v_physical_after:=v_physical_after||pg_catalog.jsonb_build_object(v_requested::text,v_physical);
    end if;
  end loop;
  if v_physical_after is distinct from v_physical_before then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE','retryable',false,
      'reason','BOOKING_REPOINTED_DURING_LOCK');
  end if;

  -- Step 3: the booking identity must be unchanged since step 2.`,'Revalidate exact H103 after original advisory/Timesheet row waits before native booking resolution');
 publish('forward/03_family_rows.sqlinc',name,e.body,parent,e.changes);
}
{
 const name='private.weekly_source_pay_query_scope_v1',parent=load(source,sourceBase+'/forward/27_certified_current_native_scope.sqlinc',name),e=edit(parent);
 e.change('  -- This existing identity owner refuses raw/trimmed collisions, multiple',`  if private.timesheet_contract_native_pending_for_v1(p_root_timesheet_id) is not null then
    raise exception 'WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE' using errcode='55000';end if;
  -- This existing identity owner refuses raw/trimmed collisions, multiple`,'Certified pending refuses before existing current NULL/ordinary arms; Invoice duty has separate original authority');
 publish('forward/04_current_pay_query.sqlinc',name,e.body,parent,e.changes);
}
{
 const name='private.weekly_source_pay_decision_resolved_scope_v1',parent=load(source,sourceBase+'/forward/33_genuine_source_subject_and_scope.sqlinc',name),e=edit(parent);
 e.change('   primary_scope:=private.timesheet_contract_current_placement_v1(root);',`   if private.timesheet_contract_native_pending_for_v1(root) is not null then
    raise exception 'SOURCE_PAY_DECISION_SOURCE_RESOLUTION_REQUIRED' using errcode='55000';end if;
   primary_scope:=private.timesheet_contract_current_placement_v1(root);
   if primary_scope->>'current_timesheet_id' is null then
    raise exception 'SOURCE_PAY_DECISION_SOURCE_RESOLUTION_REQUIRED' using errcode='55000';end if;`,'Explicit family selection requires a genuine current physical allocation, before association/enrichment');
 e.change(`   subject:=private.weekly_source_pay_decision_source_subject_v1((scope_item->>'observed_source_row_id')::uuid);
   if scope_item->'source_witness' is distinct from subject->'source_witness' then`,`   if scope_item->>'family_root_id' is not null then
    if private.timesheet_contract_native_pending_for_v1((scope_item#>>'{current_placement,requested_timesheet_id}')::uuid) is not null
      or private.timesheet_contract_current_placement_v1((scope_item#>>'{current_placement,requested_timesheet_id}')::uuid)
        is distinct from scope_item->'current_placement' then
     raise exception 'SOURCE_PAY_DECISION_REVIEW_CHANGED' using errcode='40001';end if;
   end if;
   subject:=private.weekly_source_pay_decision_source_subject_v1((scope_item->>'observed_source_row_id')::uuid);
   if scope_item->'source_witness' is distinct from subject->'source_witness' then`,'Revalidate saved complete H103 after actual physical family wait before metadata/enrichment');
 publish('forward/05_decision_scope.sqlinc',name,e.body,parent,e.changes);
}
// Dedicated immutable Invoice duty read, not a current-pay bypass. The native
// scope shape/target-family validation is retained while physical currentness
// is replaced solely by the certified original-A invoice proof.
{
 const name='private.weekly_source_invoice_duty_scope_v1';
 const query=complete(git(source,sourceBase+'/forward/27_certified_current_native_scope.sqlinc'),'private.weekly_source_pay_query_scope_v1');
 const returnStart=query.indexOf("  return jsonb_build_object('root_timesheet_id',v_root.timesheet_id,"),returnEnd=query.indexOf('\nend;',returnStart);
 const nativeReturn=query.slice(returnStart,returnEnd);if(returnStart<0||returnEnd<returnStart)throw Error('Native exact scope return changed');
 const body=`-- The current PAY placement never substitutes for immutable original invoice duty.
create or replace function ${name}(p_timesheet_id uuid)
returns jsonb language plpgsql stable security definer set search_path='pg_catalog','pg_temp'
as $function$
declare
  v_root public.timesheets%rowtype;v_contract public.contracts%rowtype;
  v_family public.weekly_exceptional_pay_target_families%rowtype;
  v_alias private.timesheet_contract_family_aliases%rowtype;v_physical jsonb;v_booking text;v_count integer;
begin
  select alias.* into v_alias from private.timesheet_contract_family_aliases alias where alias.timesheet_id=p_timesheet_id;
  if not found or v_alias.original_timesheet_id is distinct from p_timesheet_id then
    -- Ordinary current scope retains its complete native owner; pending B
    -- refuses there, before NULL/unmatched/current fallback.
    return private.weekly_source_pay_query_scope_v1(p_timesheet_id);
  end if;
  if private.timesheet_contract_source_original_eligible_v1(p_timesheet_id) is distinct from true then return null;end if;
  v_physical:=private.timesheet_contract_current_placement_v1(p_timesheet_id);
  if v_physical->>'requested_timesheet_id' is distinct from p_timesheet_id::text
    or v_physical->>'family_root_id' is distinct from v_alias.family_root_id::text
    or v_physical->'legacy_origin' is distinct from 'false'::jsonb
    or not ${originalCorrelation} then
    raise exception 'WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE' using errcode='55000';end if;
  -- Retain every native Source ownership condition and the separate-component
  -- refusal. Original eligibility alone never makes a Timesheet Source-owned.
  if private._candidate_expense_source_family_v1(p_timesheet_id) is distinct from true then return null;end if;
  select t.* into strict v_root from public.timesheets t where t.timesheet_id=p_timesheet_id;
  select c.* into strict v_contract from public.contracts c where c.id=v_root.contract_id;
  if v_root.sheet_scope is distinct from 'WEEKLY'
    or v_root.line_type is distinct from 'HOURS' or v_root.is_adjustment is distinct from false
    or v_root.week_ending_date is null or v_root.version is null or v_root.version<1
    or nullif(btrim(v_root.booking_id),'') is null or v_contract.candidate_id is null or v_contract.client_id is null then return null;end if;
  v_booking:=btrim(v_root.booking_id);
  select count(*) into v_count from public.weekly_exceptional_pay_target_families f where btrim(f.root_family_booking_id)=v_booking;
  if v_count>1 then raise exception 'WEEKLY_SOURCE_TARGET_FAMILY_AMBIGUOUS' using errcode='55000';end if;
  if v_count=1 then
    select f.* into strict v_family from public.weekly_exceptional_pay_target_families f where btrim(f.root_family_booking_id)=v_booking;
    if v_family.root_timesheet_id is distinct from p_timesheet_id
      or v_family.root_family_booking_id is distinct from v_root.booking_id
      or v_family.candidate_id is distinct from v_contract.candidate_id or v_family.contract_id is distinct from v_root.contract_id
      or v_family.week_ending_date is distinct from v_root.week_ending_date
      or v_family.week_start_date is distinct from (v_root.week_ending_date-6)
      or v_family.bound_version is null or v_family.bound_version<1 then
      raise exception 'WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE' using errcode='55000';end if;
  end if;
${nativeReturn}
end;
$function$;
`;
 publish('forward/06_invoice_duty_scope.sqlinc',name,body,null,[]);
 deltas.push({purpose:'Dedicated exact immutable original-A Invoice duty',signature:name,original_query_scope_commit:source,
 original_query_scope_path:sourceBase+'/forward/27_certified_current_native_scope.sqlinc',exact_native_return_sha256:sha(nativeReturn),
 scope_keys:['root_timesheet_id','family_booking_id','target_family_id','candidate_id','client_id','contract_id','week_ending_date','root_version','family_bound_version'],
 note:'The native query actually has nine keys (not ten); no new field is introduced. All IDs/Contract/date/version remain original A.'});
}
const acl=outputs.map(o=>{
 const args=o.signature.endsWith('weekly_source_lock_family_rows_v1')?'uuid[],uuid':o.signature.endsWith('weekly_source_pay_decision_resolved_scope_v1')?'jsonb,uuid,boolean':'uuid';
 return `alter function ${o.signature}(${args}) owner to postgres;\nrevoke all on function ${o.signature}(${args}) from public,anon,authenticated,service_role;`;
}).join('\n')+'\n';
writeFileSync(path.join(base,'forward/99_acl.sqlinc'),acl);
const dependencyFiles=[
 ['00f25571ac7fb65f44351a52beced0710211138e','docs/healthroster-build/proposals/checklist-v6-contract-correction-20261016/forward/sql/02_physical_apply.sqlinc'],
 [candidate,'docs/healthroster-build/proposals/candidate-native-pending-20261016/forward/00_pending_binding.sqlinc'],
 [candidate,'docs/healthroster-build/proposals/candidate-native-pending-20261016/forward/03a_pending_read_enter.sqlinc'],
 [candidate,'docs/healthroster-build/proposals/candidate-native-pending-20261016/forward/04_current_placement.sqlinc'],
 [candidate,'docs/healthroster-build/proposals/candidate-native-pending-20261016/forward/05_family_lock.sqlinc'],
 [candidate,'docs/healthroster-build/proposals/candidate-native-pending-20261016/forward/07_native_withdrawal.sqlinc'],
 [joined,'docs/healthroster-build/proposals/candidate-joined-final-20261016/REVIEWED_SELECTION.json'],
 [invoice,'docs/healthroster-build/proposals/v6-invoice-native-20261016/forward/018_source_original.sqlinc'],
 [invoice,'docs/healthroster-build/proposals/v6-invoice-native-20261016/forward/021a_source_duty.sqlinc'],
 [invoicePending,'docs/healthroster-build/proposals/v6-invoice-native-20261016/forward/029_claim_financial.sqlinc'],
];
const dependencies=dependencyFiles.map(([commit,file])=>({commit,path:file,sha256:sha(git(commit,file))}));
const receipt={version:'SOURCE_PENDING_CURRENT_NATIVE_SOURCE_V1',qualification:'SOURCE_ONLY_NATIVE_REQUIRED_NOT_RUN',
 source_base_commit:native,source_pay_decision_frozen_commit:source,inputs,dependencies,outputs,
 acl:{path:'forward/99_acl.sqlinc',sha256:sha(acl)},derived:deltas,
 mount_order:outputs.map(o=>o.path).concat('forward/99_acl.sqlinc'),
 root_integration_requirements:['Install exact Candidate975/f566 pending full closure, Invoice018 original authority and Source d863 full closure first',
 'Invoice lane must replace only the full duty021a call with weekly_source_invoice_duty_scope_v1',
 'Keep all historical roots/final-definer companions and genuine native ordinary financial suffixes',
 'Generate/review current contract and finite exact final registry through sole root integrator',
 'Native ACL/currentness/concurrency/full first-use acceptance is NOT_RUN; source tests do not qualify install'],
 no_database_or_deployment:true,no_bank_body_edits:true,no_public_abi_change:true,policy_x:'UNCHANGED'};
writeFileSync(path.join(base,'SOURCE_RECEIPT.json'),JSON.stringify(receipt,null,2)+'\n');
console.log(JSON.stringify({functions:outputs.length,whole_native_reverses:outputs.filter(o=>o.full_reverse_equals_input).length,
 source_only:true,receipt_sha256:sha(readFileSync(path.join(base,'SOURCE_RECEIPT.json')))}));

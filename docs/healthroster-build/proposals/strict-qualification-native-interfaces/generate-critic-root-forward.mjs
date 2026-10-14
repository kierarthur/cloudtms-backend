import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const out=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(out,'../../../..'),sha=x=>createHash('sha256').update(x).digest('hex'),proofs=[];
const prior='fef4717f93c0925167d4b74332560781af421b4d';
const read=name=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','show',prior+':docs/healthroster-build/proposals/'+name],{cwd:own,maxBuffer:8_000_000}).toString('utf8');
function emit(file,before,hunks){let after=before;for(const h of hunks){assert.equal(after.split(h.before).length-1,h.count??1);after=after.replaceAll(h.before,h.after);}
 let reverse=after;for(const h of [...hunks].reverse()){assert.equal(reverse.split(h.after).length-1,h.count??1);reverse=reverse.replaceAll(h.after,h.before);}assert.equal(reverse,before);
 fs.writeFileSync(path.join(out,file),after.trimEnd()+'\n');proofs.push({file,input_sha256:sha(before),output_sha256:sha(after.trimEnd()+'\n'),entire_source_reverse:true,
 hunks:hunks.map(h=>({before_sha256:sha(h.before),after_sha256:sha(h.after),occurrences:h.count??1}))});}
emit('13_complete_nullable_physical_qualification_scope.sqlinc',read('protected-source-qualification-operational-gate/01_complete_qualification_scope.sqlinc'),[
 {before:"'candidate_id',contract.candidate_id,'client_id',contract.client_id)",after:"'candidate_id',contract.candidate_id,'client_id',sheet.client_id)",count:2}]);
emit('10_complete_bulk_test_scope.sqlinc',read('strict-qualification-native-interfaces/10_complete_bulk_test_scope.sqlinc'),[
 {before:"'candidate_id',k.candidate_id,'client_id',k.client_id) into actual",after:"'candidate_id',k.candidate_id,'client_id',s.client_id) into actual"}]);
const auth=read('strict-qualification-native-interfaces/OFF_RULE_AUTHORITY.sqlinc');
emit('OFF_RULE_AUTHORITY.sqlinc',auth,[{before:"  if auth->>'purpose'='AUTOMATIC_JOURNEY' then\n    if cardinality(clients)<>1",after:`  if auth->>'purpose'='AUTOMATIC_JOURNEY' then
    if not p_initial_open then
      -- ORIGINAL_CAPTURED_PHYSICAL_ROOTS: recheck the OPEN union of Source
      -- pay roots and signed evidence roots, including genuine physical NULL
      -- Client. Never recollect only surviving OPEN incidents after a round.
      if lease#>>'{original_census,schema_version}' is distinct from 'WEEKLY_SOURCE_QUALIFICATION_CENSUS_V1'
        or lease#>>'{original_census,source_cycle_id}' is distinct from c.id::text
        or lease#>>'{original_census,source_group_id}' is distinct from g.id::text
        or lease#>>'{original_census,week_ending}' is distinct from c.finalisation_week_ending::text
        or lease#>'{original_census,original_client_ids}' is distinct from auth#>'{fixture_manifest,client_ids}'
        or lease#>>'{original_census,census_sha256}' is distinct from encode(private.weekly_source_sha256_jsonb_v1(
          'WEEKLY_SOURCE_QUALIFICATION_CENSUS_V1',lease->'original_census'-'census_sha256'),'hex') then
        raise exception 'WEEKLY_SOURCE_RULE_QUALIFICATION_ORIGINAL_ROOT_CHANGED' using errcode='40001'; end if;
      for item in select value from jsonb_array_elements(lease#>'{original_census,original_root_identities}') value loop
        if not private.weekly_source_qualification_closed_object_v1(item,array['timesheet_id','booking_id','contract_id','candidate_id','client_id']) then
          raise exception 'WEEKLY_SOURCE_RULE_QUALIFICATION_ORIGINAL_ROOT_CHANGED' using errcode='40001'; end if;
        select jsonb_build_object('timesheet_id',sheet.timesheet_id,'booking_id',sheet.booking_id,'contract_id',contract.id,
          'candidate_id',contract.candidate_id,'client_id',sheet.client_id) into actual
          from public.timesheets sheet join public.contracts contract on contract.id=sheet.contract_id
          where sheet.timesheet_id=(item->>'timesheet_id')::uuid and (sheet.client_id is null or sheet.client_id=contract.client_id);
        if actual is distinct from item then
          raise exception 'WEEKLY_SOURCE_RULE_QUALIFICATION_ORIGINAL_ROOT_CHANGED' using errcode='40001'; end if;
      end loop;
    end if;
    if cardinality(clients)<>1`}]);
fs.writeFileSync(path.join(out,'CRITIC_NULLABLE_ROOT_REVERSE.json'),JSON.stringify({source_only:true,critic_predecessor:prior,
 original_source39_and_captured_once_items_preserved:true,physical_null_client_preserved:true,no_new_signature:true,no_native_execution:true,proofs},null,2)+'\n');

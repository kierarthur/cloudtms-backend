import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';
import{createHash}from'node:crypto';import{execFileSync}from'node:child_process';import{fileURLToPath}from'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..');
const commit='fa66183f01913e86651e82816f5e88f97e07123c',report='63965738',sha=b=>createHash('sha256').update(b).digest('hex');
const git=(c,f)=>execFileSync('git',['show',c+':'+f],{cwd:root,maxBuffer:8e6});
assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')));
for(const p of['before','forward','input'])fs.mkdirSync(path.join(dir,p),{recursive:true});
const money='supabase/repeatable/14102026_0430_healthroster_money_native_interfaces/';
const prereq='supabase/repeatable/14102026_0410_healthroster_protected_source_prerequisites/';
const proofs=[],rawPins=new Map();
function owner(file,name,output,hunks){
 const raw=git(commit,file),text=raw.toString(),marker='create or replace function private.'+name+'(';
 assert.equal(text.split(marker).length,2,name);const start=text.indexOf(marker),end=text.indexOf('$function$;',start)+'$function$;'.length;
 assert.ok(end>start);const b=Buffer.from(text.slice(start,end)+'\n'),before=b.toString(),eol=before.includes('\r\n')?'\r\n':'\n';
 hunks=hunks.map(h=>({...h,before:h.before.replaceAll('\n',eol),after:h.after.replaceAll('\n',eol)}));let after=before;
 for(const h of hunks){assert.equal(after.split(h.before).length,2,name+':'+h.name);after=after.replace(h.before,()=>h.after);}
 let reverse=after;for(const h of [...hunks].reverse()){assert.equal(reverse.split(h.after).length,2,h.name);reverse=reverse.replace(h.after,()=>h.before);}assert.equal(reverse,before);
 fs.writeFileSync(path.join(dir,'before',output),b);fs.writeFileSync(path.join(dir,'forward',output),after);
 proofs.push({name:'private.'+name,file,output,commit,source_sha256:sha(raw),before_bytes:b.length,before_sha256:sha(b),
  after_bytes:Buffer.byteLength(after),after_sha256:sha(after),hunks,whole_reverse_verified:true});
 if(!rawPins.has(file)){const saved='input/'+path.basename(file);fs.writeFileSync(path.join(dir,saved),raw);rawPins.set(file,{file,commit,saved,bytes:raw.length,sha256:sha(raw)});}
}
owner(money+'04_13_complete_nullable_physical_qualification_scope.sqlinc','weekly_source_protected_qualification_census_v1','01_complete_original_census.sqlinc',[
 {name:'exact signed evidence root uses existing physical Contract tuple, not missing sheet Client',before:`        and contract.client_id=(item->>'client_id')::uuid
        and (sheet.client_id is null or sheet.client_id=contract.client_id))) then`,after:`        and contract.client_id=(item->>'client_id')::uuid)) then`},
 {name:'never erase an unbound member of the original physical-root union',before:`  with ids as (
    select (item->>'evidence_timesheet_id')::uuid id from jsonb_array_elements(items) item`,after:`  if exists(
    select 1 from (
      select (item->>'evidence_timesheet_id')::uuid id from jsonb_array_elements(items) item
        where item->>'evidence_timesheet_id' is not null
      union select (item->>'timesheet_id')::uuid from jsonb_array_elements(facts->'root_identities') item
    ) original_id
    left join public.timesheets physical on physical.timesheet_id=original_id.id
    left join public.contracts native_contract on native_contract.id=physical.contract_id
    where physical.timesheet_id is null or native_contract.id is null
      or native_contract.candidate_id is null or native_contract.client_id is null
      or not(native_contract.client_id=any(clients))
  ) then
    raise exception 'WEEKLY_SOURCE_QUALIFICATION_ORIGINAL_ROOT_SCOPE_INVALID' using errcode='55000';end if;
  with ids as (
    select (item->>'evidence_timesheet_id')::uuid id from jsonb_array_elements(items) item`},
 {name:'same five census keys with genuine Contract-derived Candidate/Client',before:`    'contract_id',contract.id,'candidate_id',contract.candidate_id,'client_id',sheet.client_id)`,after:`    'contract_id',contract.id,'candidate_id',contract.candidate_id,'client_id',contract.client_id)`}
]);
owner(money+'04_13_complete_nullable_physical_qualification_scope.sqlinc','weekly_source_protected_qualification_assert_v1','02_complete_original_census_assert.sqlinc',[
 {name:'same captured physical identity keys with actual Contract-derived Client',before:`      'candidate_id',contract.candidate_id,'client_id',sheet.client_id) into actual`,after:`      'candidate_id',contract.candidate_id,'client_id',contract.client_id) into actual`},
 {name:'unbound Contract refuses instead of manufactured nullable physical Client',before:`      where sheet.timesheet_id=(original->>'timesheet_id')::uuid and (sheet.client_id is null or sheet.client_id=contract.client_id);`,after:`      where sheet.timesheet_id=(original->>'timesheet_id')::uuid
        and contract.candidate_id is not null and contract.client_id is not null;`}
]);
owner(money+'01_OFF_RULE_AUTHORITY.sqlinc','weekly_source_rule_qualification_scope_assert_v1','03_complete_off_scope_assert.sqlinc',[
 {name:'correct physical-field disposition in original-root comment',before:`      -- pay roots and signed evidence roots, including genuine physical NULL
      -- Client. Never recollect only surviving OPEN incidents after a round.`,after:`      -- pay roots and signed evidence roots with the physical root's actual
      -- Contract-derived Client. Unbound Contract is not a nullable Client
      -- proof. Never recollect only surviving OPEN incidents after a round.`},
 {name:'same automatic/close census from actual physical Contract Client',before:`          'candidate_id',contract.candidate_id,'client_id',sheet.client_id) into actual`,after:`          'candidate_id',contract.candidate_id,'client_id',contract.client_id) into actual`},
 {name:'real missing or unbound root/Contract cannot pass original census',before:`          where sheet.timesheet_id=(item->>'timesheet_id')::uuid and (sheet.client_id is null or sheet.client_id=contract.client_id);`,after:`          where sheet.timesheet_id=(item->>'timesheet_id')::uuid
            and contract.candidate_id is not null and contract.client_id is not null;`}
]);
owner(money+'05_OFF_RULE_NATIVE_PARENT_SCOPE.sqlinc','weekly_source_off_rule_native_parent_scope_v1','04_complete_native_parent_scope.sqlinc',[
 {name:'qualified root proves genuine non-null native Contract identity',before:`          if qualified and root.client_id is not null and root.client_id is distinct from contract.client_id then`,after:`          if qualified and (contract.client_id is null or contract.candidate_id is null) then`},
 {name:'every qualified discovered physical root keeps same parent tuple and explicit unknown refusal',before:`          if (root.client_id is not null and root.client_id is distinct from contract.client_id)
            or (contract_id is not null and contract_id is distinct from contract.id)`,after:`          if contract.client_id is null or contract.candidate_id is null
            or (contract_id is not null and contract_id is distinct from contract.id)`}
]);
owner(prereq+'26_q45_actor_event_closed_dto_cross_session_QUALIFICATION_STEP.sqlinc','weekly_source_protected_qualification_step_v1','05_complete_native_qualification_step.sqlinc',[
 {name:'explicit variable namespace for the native root identity predicate',before:`as $function$
declare scope_value jsonb;`,after:`as $function$
<<qualification_step_identity>>
declare scope_value jsonb;`},
 {name:'same native Contract/Candidate/Client tuple uses actual variables and no absent physical field',before:`        and contract.id=contract_id and contract.candidate_id=candidate_id and contract.client_id=client_id
        and (sheet.client_id is null or sheet.client_id=client_id)) then`,after:`        and contract.id=qualification_step_identity.contract_id
        and contract.candidate_id=qualification_step_identity.candidate_id
        and contract.client_id=qualification_step_identity.client_id) then`}
]);
owner(money+'16_10_complete_bulk_test_scope.sqlinc','weekly_source_protected_test_scope_assert_v1','06_complete_bulk_original_root_assert.sqlinc',[
 {name:'original physical-root census Client is the existing native Contract Client',before:`      'candidate_id',k.candidate_id,'client_id',s.client_id) into actual`,after:`      'candidate_id',k.candidate_id,'client_id',k.client_id) into actual`},
 {name:'missing/unbound roots remain mismatches, not erased original scope',before:`      where s.timesheet_id=(item->>'timesheet_id')::uuid and (s.client_id is null or s.client_id=k.client_id);`,after:`      where s.timesheet_id=(item->>'timesheet_id')::uuid and k.candidate_id is not null and k.client_id is not null;`}
]);
const schemaPath='docs/healthroster-build/proposals/agency-source-dependency-current-reader-forward-20261014/construction/CURRENT_TIMESHEETS_CONTRACT_SCHEMA.json';
const schemaRaw=git(report,schemaPath),schema=JSON.parse(schemaRaw);assert.equal(schema.commit,commit);
assert.ok(!schema.relation.columns.some(c=>c.name==='client_id'||c.name==='candidate_id'));
assert.ok(schema.relation.columns.some(c=>c.name==='contract_id'));
fs.writeFileSync(path.join(dir,'input/CURRENT_TIMESHEETS_CONTRACT_SCHEMA.json'),schemaRaw);
const mapPath='docs/healthroster-build/proposals/agency-source-dependency-current-reader-forward-20261014/construction/SOURCE_RAW_ID_DDL_MAPPING.json';
const mappingRaw=git(report,mapPath),mapping=JSON.parse(mappingRaw);
const physical=mapping.tables.find(t=>t.relation==='public.timesheets');
assert.ok(physical?.ddl.length);
fs.writeFileSync(path.join(dir,'input/RAW_TIMESHEETS_DDL.json'),JSON.stringify({source_only:true,containing_report_commit:report,
 source:mapPath,whole_mapping_sha256:sha(mappingRaw),physical},null,2)+'\n');
const contract=git(commit,'supabase/release/current-contract.json');assert.equal(sha(contract),schema.full_source_sha256);
fs.writeFileSync(path.join(dir,'COMPLETE_NATIVE_REVERSES.json'),JSON.stringify({source_only:true,commit,proofs,raw_input_pins:[...rawPins.values()],
 source_schema_pin:{containing_report_commit:report,file:schemaPath,sha256:sha(schemaRaw)},
 current_contract_sha256:sha(contract),Report_Source20_bulk_facts_reader_is_separate:true,
 old_frozen_nullable_physical_Client_concept_superseded_for_actual_T0_only:true,Source39_keys_count_actor_physical_Contract_Candidate_guards_preserved:true,
 financial_TF_Client_and_other_record_DTO_aliases_not_rewritten:true,SQL_compiled:false,native_proof:false},null,2)+'\n');
console.log(JSON.stringify({owners:proofs.length,unique_hunks:proofs.reduce((n,r)=>n+r.hunks.length,0),source_only:true,no_new_signatures:true}));

import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const root='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-v2-backend-13102026',output=path.dirname(fileURLToPath(import.meta.url));
const git=args=>execFileSync('git',args,{cwd:root,encoding:'utf8',maxBuffer:3000000}),commit=git(['rev-parse','HEAD']).trim();
if(git(['ls-remote','origin','refs/heads/codex/healthroster-v2-backend-13102026']).trim().split(/\s+/)[0]!==commit)throw Error('PUSHED_SOURCE_NOT_MATCHED');
const prefix='create or replace function public.weekly_source_projection_rows_apply_atomic_v1(';
const paths=['supabase/repeatable/13102026_1344_weekly_source_self_bill_report/13102026_1344_projection_rows.sqlinc','supabase/repeatable/13102026_1561_weekly_source_magnit_contract_price_v1.sql'];
const sha=s=>crypto.createHash('sha256').update(s).digest('hex');
const owners=paths.map(file=>{const text=git(['show',commit+':'+file]),start=text.indexOf(prefix);if(start<0||text.split(prefix).length!==2)throw Error('COMPLETE_OWNER_CARDINALITY');const end=text.indexOf('$function$;',start);if(end<0)throw Error('COMPLETE_OWNER_END');const sql=text.slice(start,end+11);const literals=sql.split(/\r?\n/).filter(line=>line.includes("!~'"));if(literals.length!==9||literals.some(line=>! /!~'\^[^\r\n']+\$'/.test(line)))throw Error('EXACT_NINE_NATIVE_REGEXES_REQUIRED');return {file,wholeCommittedBlobSha256:sha(text),definitionSourceSha256:sha(sql),sql};});
const probe=String.raw`do $hr_projection_null$ declare refused boolean:=false; begin
  begin perform public.weekly_source_projection_rows_apply_atomic_v1(null,null,null);
  exception when sqlstate '22023' then
    if sqlerrm='WEEKLY_SOURCE_PROJECTION_INPUT_INVALID' then refused:=true; else raise; end if;
  end;
  if not refused then raise exception 'HR_NATIVE_NULL_PROJECTION_NOT_REFUSED'; end if;
end; $hr_projection_null$;`;
const sql="do $hr_test_guard$ begin if current_database()<>'cloudtms_test_clone' or not exists(select 1 from private.cloudtms_database_identity where singleton and environment='TEST') or not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='LEGACY' and owner_epoch=1) or exists(select 1 from private.cloudtms_database_releases where status='APPLYING') then raise exception 'HR_TEST_IDENTITY_REFUSED'; end if; end; $hr_test_guard$;\nset local request.jwt.claim.role='service_role';\n"+owners.map((o,i)=>o.sql+"\n"+probe+"\nselect '"+(i===0?'REPORT':'M1')+"' compiled_owner, encode(sha256(convert_to(pg_get_functiondef('public.weekly_source_projection_rows_apply_atomic_v1(uuid,uuid,jsonb)'::regprocedure),'UTF8')),'hex') canonical_definition_sha256;\n").join('');
const record={status:'PREPARED_NOT_EXECUTED_SHARED_TURN_REQUIRED',sourceCommit:commit,sourceBranch:'codex/healthroster-v2-backend-13102026',physicalDatabase:'cloudtms_test_clone',label:'HR_REPORT_M1_FIXED_PROJECTION_DEFINITION_NULL_ONLY',scope:'Exactly two complete committed native projection definitions and native NULL22023 probes; no business rows, financial actions, new capability, API or install.',sqlSha256:sha(sql),bytes:Buffer.byteLength(sql),inputs:owners.map(({sql,...rest})=>rest),execution:{before:'Obtain exact current installed function definition only after shared turn is taken; verify exact definition hash before replacement in same sql.',trial:'Arthur rollback lab one short trial, statement5s/lock500ms, continue_on_error=false',after:'Fresh read-only exact installed definition restoration, physical database and no durable writes; retain shared turn until returned.',noFullRehearsal:true}};
const files=[['REPORT_M1_PROJECTION_SHORT_TRIAL_2_PLAN.json',JSON.stringify(record,null,2)+'\n'],['REPORT_M1_PROJECTION_SHORT_TRIAL_2.sql',sql]];
for(const [name,body] of files)fs.writeFileSync(path.join(output,name),body);
console.log(JSON.stringify({sourceCommit:commit,sqlSha256:record.sqlSha256,bytes:record.bytes,prepared:true,nativeProof:'NOT_RUN'}));


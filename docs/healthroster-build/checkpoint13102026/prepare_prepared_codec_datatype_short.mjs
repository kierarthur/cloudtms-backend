import fs from 'node:fs';import path from 'node:path';import {execFileSync} from 'node:child_process';import {fileURLToPath,pathToFileURL} from 'node:url';
const root='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-v2-backend-13102026',lane='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026',out=path.dirname(fileURLToPath(import.meta.url)),commit='63f7ba41030371b5340b3ba4e1da5a95e4cc531a',folder='docs/healthroster-build/proposals/report-prepared-output-array-forward/';
const {digest,scanManagedSql}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs')));
const git=args=>execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...args],{cwd:lane,encoding:'utf8',maxBuffer:3000000});
if(git(['ls-remote','origin','refs/heads/codex/healthroster-v2-report-build-13102026']).trim().split(/\s+/)[0]!==commit)throw Error('DATATYPE_SOURCE_REMOTE_DRIFT');
const inputs=[];function source(file){const text=git(['show',commit+':'+file]);inputs.push({file,gitBlobSha256:digest(text)});return text;}
const full=source(folder+'02_complete_codec_array_forward.sqlinc'),parsed=scanManagedSql(full),signature='private.weekly_source_report_prepared_financial_codec_v1(public.timesheets_financials)';
const owners=parsed.statements.map(s=>parsed.source.slice(s.start,s.end)).filter(s=>s.startsWith('create or replace function private.weekly_source_report_prepared_financial_codec_v1('));if(owners.length!==1)throw Error('COMPLETE_CODEC_OWNER_REQUIRED');
const fields=JSON.parse(source('docs/healthroster-build/proposals/report-prepared-output-retention/CODEC_FIELDS.json')),values={};
const baseline=source('supabase/baseline/22082026_1500_cloudtms_test_structural_baseline.sql');
for(const [key,type]of Object.entries(fields.types)){
 if(!type.endsWith(' not null'))continue;const native=type.replace(/ not null$/,'');
 if(native==='numeric'||native==='integer'||native==='bigint')values[key]=0;
 else if(native==='boolean')values[key]=false;
 else if(native==='text')values[key]='DATATYPE_ONLY_NOT_FINANCIAL_EVIDENCE';
 else if(native==='uuid')values[key]='13102026-2303-4000-8000-000000000001';
 else if(native==='timestamp with time zone')values[key]='2000-01-01T00:00:00+00:00';
 else if(native==='jsonb')values[key]={};
 else if(native.endsWith('_enum')){
  const expression=new RegExp('create type public\\.'+native+" as enum \\('([^']+)'",'i');
  const label=baseline.match(expression)?.[1];if(!label)throw Error('UNREVIEWED_NATIVE_ENUM_LABEL');values[key]=label;
 }
 else throw Error('UNREVIEWED_NATIVE_REQUIRED_TYPE_'+native);
}
values.id='13102026-2303-4000-8000-000000000001';values.timesheet_id='13102026-2303-4000-8000-000000000002';values.timesheet_version=1;
const sample=JSON.stringify(values);if(sample.includes('$codec_sample$'))throw Error('DELIMITER_COLLISION');
const fingerprint="select current_database() physical_database, (select md5(string_agg(n.nspname||'.'||p.proname||'('||pg_get_function_identity_arguments(p.oid)||')'||coalesce(p.prosrc,'')||coalesce(p.probin,'')||p.proowner::text||coalesce(p.proacl::text,'')||coalesce(p.proconfig::text,'')||p.prolang::text||p.prosecdef::text||p.provolatile::text,'|' order by p.oid)) from pg_proc p join pg_namespace n on n.oid=p.pronamespace where n.nspname in('public','private')) functions_hash, exists(select 1 from private.cloudtms_database_identity where singleton and environment='TEST') identity_test, (select active_owner||':'||owner_epoch::text from private.bpay_next_module_control where id=1) module, to_regprocedure('"+signature+"') is null codec_absent, current_setting('array_nulls') array_nulls;\n";
const guard="do $codec_guard$ begin if current_database()<>'cloudtms_test_clone' or not exists(select 1 from private.cloudtms_database_identity where singleton and environment='TEST') or not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='LEGACY' and owner_epoch=1) or exists(select 1 from private.cloudtms_database_releases where status='APPLYING') or to_regprocedure('"+signature+"') is not null then raise exception 'CODEC_DATATYPE_TEST_GUARD_REFUSED'; end if; end; $codec_guard$;\n";
const cases=source(folder+'NATIVE_ARRAY_DATA_CASES.sqlinc');
let sql=guard+fingerprint+owners[0]+"\nalter function "+signature+" owner to CURRENT_USER;\nrevoke all on function "+signature+" from public,anon,authenticated,service_role;\nset local array_nulls=off;\n"+cases+"\n";
sql+=`do $compiled_codec_cases$
declare r record; fin public.timesheets_financials; different_bounds public.timesheets_financials; encoded jsonb; second_output jsonb; checked integer:=0;
begin
 for r in select * from (values
 ('TWO_DIMENSIONS',ARRAY[['RATE_A','RATE_B'],['RATE_C','RATE_D']]::text[]),
 ('NON_ONE_LOWER_BOUNDS',$array$[0:1][3:4]={{RATE_A,RATE_B},{RATE_C,RATE_D}}$array$::text[]),
 ('ONE_DIMENSION',ARRAY['RATE_A','RATE_B']::text[]),
 ('NULL_STRING_QUOTES_BACKSLASH',ARRAY[null::text,'NULL','a,b',$item$a"b$item$,$item$a\\b$item$]::text[]),
 ('EMPTY_ARRAY',ARRAY[]::text[]),('SQL_NULL_ARRAY',null::text[])
 ) cases(case_name,native_array) loop
 fin:=pg_catalog.jsonb_populate_record(null::public.timesheets_financials,$codec_sample$${sample}$codec_sample$::jsonb);
 fin.hr_crosscheck_issues:=r.native_array;
 encoded:=private.weekly_source_report_prepared_financial_codec_v1(fin);
 if encoded->>'hr_crosscheck_issues' is distinct from fin.hr_crosscheck_issues::text
 or pg_catalog.current_setting('array_nulls')<>'off' then
 raise exception 'COMPILED_CODEC_NATIVE_ARRAY_OR_SETTING_FAILED' using errcode='55000',detail=r.case_name; end if;
 checked:=checked+1;
 end loop;
 fin.hr_crosscheck_issues:=$array$[0:1]={RATE_A,RATE_B}$array$::text[];
 different_bounds:=fin;different_bounds.hr_crosscheck_issues:=ARRAY['RATE_A','RATE_B']::text[];
 encoded:=private.weekly_source_report_prepared_financial_codec_v1(fin);
 second_output:=private.weekly_source_report_prepared_financial_codec_v1(different_bounds);
 if pg_catalog.to_jsonb(fin) is distinct from pg_catalog.to_jsonb(different_bounds)
 or encoded is not distinct from second_output or checked<>6 then
 raise exception 'COMPILED_CODEC_NATIVE_ARRAY_BOUND_DRIFT_NOT_VISIBLE' using errcode='55000';end if;
 if pg_catalog.has_function_privilege('anon','${signature}','EXECUTE')
 or pg_catalog.has_function_privilege('authenticated','${signature}','EXECUTE')
 or pg_catalog.has_function_privilege('service_role','${signature}','EXECUTE') then
 raise exception 'COMPILED_CODEC_PRIVATE_ACL_FAILED' using errcode='55000';end if;
end; $compiled_codec_cases$;
select 'DATATYPE_ONLY_NO_FINANCIAL_QUALIFICATION' scope, true native_codec_six_cases_and_bound_drift_pass;
`;
const checked=scanManagedSql(sql);if(checked.meta.length||checked.statements.length>250||Buffer.byteLength(sql)>2000000)throw Error('DATATYPE_SHORT_ENVELOPE_REFUSED');
const plan={status:'PREPARED_ONLY_OWNED_SEND7_TURN_AND_EXACT_ABSENCE_REQUIRED',sourceCommit:commit,inputs,bytes:Buffer.byteLength(sql),statements:checked.statements.length,sqlSha256:digest(sql),signature,scope:'Exact single new codec definition plus native datatype/typed NON-PERSISTED composites only. No table, business row, financial writer, receipt, signature, publication, capability, queue or external action. Synthetic zero/text/id/timestamps are type samples, not accepted money/date/actor evidence. Retained reader, capture/trigger/financial/contract/API qualification remains held.',freshVerification:'Complete public/private function fingerprint and original array_nulls equal; exact codec absent; physicalTEST/LEGACY:1.'};
fs.writeFileSync(path.join(out,'PREPARED_CODEC_DATATYPE_SHORT_PLAN.json'),JSON.stringify(plan,null,2)+'\n');fs.writeFileSync(path.join(out,'PREPARED_CODEC_DATATYPE_SHORT.sql'),sql);fs.writeFileSync(path.join(out,'PREPARED_CODEC_DATATYPE_SHORT_FRESH.sql'),fingerprint);console.log(JSON.stringify({prepared:true,bytes:plan.bytes,statements:plan.statements,sqlSha256:plan.sqlSha256,nativeProof:'NOT_RUN'}));

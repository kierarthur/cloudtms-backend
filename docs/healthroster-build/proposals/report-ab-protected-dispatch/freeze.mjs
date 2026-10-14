import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';import {spawnSync} from 'node:child_process';
const dir=path.dirname(fileURLToPath(import.meta.url)),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const signatures=JSON.parse(fs.readFileSync(path.join(dir,'PRIVATE_SIGNATURES.json'),'utf8')).signatures;
const owners=['01_certificate.sqlinc','02_release_feature_strategy.sqlinc','03_operational_dispatch.sqlinc','13_slot_validators.sqlinc'];
const definitions=owners.map(f=>fs.readFileSync(path.join(dir,f),'utf8').replaceAll('\r\n','\n')).join('\n');
const rows=signatures.map(signature=>{const name=signature.slice(0,signature.indexOf('('));
 const rx=new RegExp('create or replace function '+name.replaceAll('.','\\.')+'\\([\\s\\S]*?as \\$function\\$([\\s\\S]*?)\\$function\\$;');
 const match=definitions.match(rx);if(!match)throw Error('DEFINITION_NOT_FOUND '+signature);
 return {signature,body_sha256:sha(match[1]),returns:name.endsWith('_assert_v1')?'void':'jsonb',volatility:name.endsWith('dispatch_v1')?'v':'s'};});
const verifier=`-- Data-free declaration/body/registration/ACL and early null refusal only.
-- No financial, A/B, release, activation, feature, business or network call.
\\set ON_ERROR_STOP on
begin;
do $report_ab_declarations$
declare expected record; actual pg_catalog.pg_proc%rowtype;
begin
 for expected in select * from (values
${rows.map(r=>`  ('${r.signature}','${r.body_sha256}','${r.returns}','${r.volatility}')`).join(',\n')}
 ) items(signature,body_sha256,return_type,volatility) loop
  select * into actual from pg_catalog.pg_proc where oid=pg_catalog.to_regprocedure(expected.signature);
  if not found or actual.proowner<>current_user::regrole::oid or not actual.prosecdef or actual.prokind<>'f'
   or actual.provolatile::text<>expected.volatility or actual.pronargdefaults<>0 or actual.prorettype<>pg_catalog.to_regtype(expected.return_type)::oid
   or actual.proconfig is distinct from array['search_path=pg_catalog, pg_temp']::text[]
   or encode(sha256(convert_to(actual.prosrc,'UTF8')),'hex')<>expected.body_sha256
   or has_function_privilege('anon',actual.oid,'EXECUTE') or has_function_privilege('authenticated',actual.oid,'EXECUTE')
   or has_function_privilege('service_role',actual.oid,'EXECUTE')
   or exists(select 1 from aclexplode(coalesce(actual.proacl,acldefault('f',actual.proowner))) acl where acl.grantee=0 and acl.privilege_type='EXECUTE')
   or not exists(select 1 from private._weekly_source_acl_private_helper_contract_v1() registry where registry.function_signature=expected.signature) then
   raise exception 'REPORT_AB_PRIVATE_DECLARATION_FAILED' using errcode='55000',detail=expected.signature;
  end if;
 end loop;
end;
$report_ab_declarations$;
do $report_ab_early_refusal$
begin
 begin
  perform private.weekly_source_report_fin_certificate_assert_v1(null,null,null,null,null);
  raise exception 'REPORT_AB_NULL_CERTIFICATE_NOT_REFUSED';
 exception when sqlstate '55000' then
  if sqlerrm<>'WEEKLY_SOURCE_REPORT_FIN_RELEASE_UNQUALIFIED' then raise; end if;
 end;
 begin
  perform private.weekly_source_report_fin_strategy_v1(null);
  raise exception 'REPORT_AB_MISSING_REPORT_NOT_REFUSED';
 exception when sqlstate '55000' then
  if sqlerrm<>'WEEKLY_SOURCE_REPORT_FIN_RELEASE_UNQUALIFIED' then raise; end if;
 end;
end;
$report_ab_early_refusal$;
rollback;
`;
fs.writeFileSync(path.join(dir,'08_datafree_verifier.sqlinc'),verifier);
const result=spawnSync(process.execPath,['--test',path.join(dir,'report-ab-dispatch.test.mjs'),path.join(dir,'report-fin-native-collector.test.mjs')],{encoding:'utf8'});
fs.writeFileSync(path.join(dir,'SOURCE_TEST_RESULTS.txt'),result.stdout+result.stderr);
if(result.status!==0){process.stdout.write(result.stdout+result.stderr);throw Error('SOURCE_TEST_FAILED');}
const tests=Number(result.stdout.match(/(?:# |ℹ )tests (\d+)/)?.[1]);
const passed=Number(result.stdout.match(/(?:# |ℹ )pass (\d+)/)?.[1]);
if(!Number.isSafeInteger(tests)||tests!==passed)throw Error('SOURCE_TEST_COUNT_UNPROVED');
const javascript=fs.readdirSync(dir).filter(f=>f.endsWith('.mjs'));
for(const file of javascript){const syntax=spawnSync(process.execPath,['--check',path.join(dir,file)],{encoding:'utf8'});
 if(syntax.status!==0)throw Error('SYNTAX_FAILED '+file+' '+syntax.stderr);}
const inputs=JSON.parse(fs.readFileSync(path.join(dir,'CONTROLLING_INPUTS.json'),'utf8'));
const money=JSON.parse(fs.readFileSync(path.join(dir,'MONEY_DEPENDENCY_PINS.json'),'utf8'));
const lexical=JSON.parse(fs.readFileSync(path.join(dir,'METADATA_PIN_FORWARD_AND_LEXICAL_RECEIPT.json'),'utf8'));
const paired=['REPORT_ADMISSION_REVERSE.json','REPORT_ORDER_REVERSE.json'].map(file=>{
 const proof=JSON.parse(fs.readFileSync(path.join(dir,file),'utf8'));if(sha(fs.readFileSync(path.join(dir,proof.output)))!==proof.output_sha256)throw Error('PAIRED_NATIVE_OWNER_CHANGED');
 return {proof:file,input:proof.input,input_sha256:proof.input_sha256,output:proof.output,output_sha256:proof.output_sha256,source_commit:proof.source_commit,full_reverse:proof.full_reverse};});
const artifacts=fs.readdirSync(dir).filter(f=>f!=='SOURCE_MANIFEST.json').sort().map(file=>{const b=fs.readFileSync(path.join(dir,file));return {path:file,sha256:sha(b),bytes:b.length};});
const package_sha256=sha(JSON.stringify(artifacts));
fs.writeFileSync(path.join(dir,'SOURCE_MANIFEST.json'),JSON.stringify({schema_version:'REPORT_AB_PROTECTED_DISPATCH_SOURCE_PACKAGE_V1',
 root_commit:inputs.root_commit,status:'SOURCE_FROZEN_FOR_ROOT_CRITIC_NO_NATIVE_QUALIFICATION_NO_SELECTED_VARIANT',new_private_signatures:signatures,
 private_union:'Seven new owner-only signatures; complete Q/A0/Prepared/Money/later union is root-owned. Preserve the larger final union; never overwrite it with current-only 239.',
 money_dependency:{source_commit:money.source_commit,manifest_sha256:money.source_manifest.sha256,checks_sha256:money.source_checks.sha256,artifacts:money.artifacts.length},
 paired_native_owners:paired,recorder_abi:{request_fields:9,response_fields:13,report_slots:2,existing_bundle_exceptions:3,new_helper_signatures:7},
 raw_collector:{schema_version:'WEEKLY_SOURCE_REPORT_FIN_RAW_COLLECTOR_V1',max_trace_bytes:256*1024,max_bundle_bytes:2*1024*1024,max_variant_traces:14,
  absent_genuine_document_or_gate_mapper:'WAITING_NATIVE_GATES_NO_RECORDABLE_RECEIPT'},
 tests:{offline_cases:tests,passed,node_syntax:'PASS',javascript_files:javascript.length,sql_lexical_files:lexical.lexical.length,
  database_compile:'NOT_RUN',financial_ab:'NOT_RUN',native_document:'NOT_RUN',exposure:'HELD',
  release_design:'Complete same-Install2 source recorder/session/raw receipt rederivation. Root composition/review and every genuine native/remote qualification remain held; no third release.'},
 package_sha256,artifacts},null,2)+'\n');
console.log(JSON.stringify({artifacts:artifacts.length,package_sha256,manifest_sha256:sha(fs.readFileSync(path.join(dir,'SOURCE_MANIFEST.json')))}));

// Source-only finite programme. No connection, callback, caller SQL or executor.
import fs from 'node:fs';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url));
const sha=b=>createHash('sha256').update(b).digest('hex');
const check=(v,c)=>{if(!v)throw Error(c);};
export const ROOT='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';
export const MODES=Object.freeze(['READ COMMITTED','REPEATABLE READ','SERIALIZABLE']);
export const CANARY='f717d042-0000-4000-8000-000000000001';
const literal=s=>"'"+s.replaceAll("'","''")+"'";
export function definition(bytes,name){
 const text=bytes.toString('utf8'),prefix='create or replace function '+name+'(',start=text.indexOf(prefix);
 check(start>=0&&text.indexOf(prefix,start+1)===-1,'ISOLATION_COMPLETE_DEFINITION_UNIQUE_REQUIRED');
 const body=text.indexOf('as $function$',start),end=text.indexOf('$function$;',body+14);
 check(body>start&&end>body,'ISOLATION_COMPLETE_DEFINITION_REQUIRED');
 return Buffer.from(text.slice(start,end+11));
}
export function buildIsolationProgram({root,isolation}={}){
 check(typeof root==='string'&&path.isAbsolute(root)&&fs.realpathSync.native(root).replaceAll('\\','/')===ROOT,'ISOLATION_PHYSICAL_ROOT_REQUIRED');
 check(MODES.includes(isolation),'ISOLATION_EXACT_MODE_REQUIRED');
 const pins=JSON.parse(fs.readFileSync(here+'/SOURCE_PINS.json','utf8'));
 const selected=pins.definitions.map(p=>{
  const bytes=fs.readFileSync(path.join(root,p.file));check(sha(bytes)===p.fileSha256,'ISOLATION_WHOLE_SOURCE_CHANGED');
  const body=definition(bytes,p.name);check(body.length===p.bytes&&sha(body)===p.sha256,'ISOLATION_DEFINITION_CHANGED');return body.toString();
 });
 // These are valid native PREVIEW syntax and intentionally incomplete inputs.
 // They never assert real work, rates, a financial snapshot or permission.
 const request={version:'DAILY_RATE_CORRECTION_V1',request_id:'f717d042-0000-4000-8000-000000000002',actor_user_id:CANARY,
  timesheet_id:'f717d042-0000-4000-8000-000000000003',reason:'',context_digest:null,calculation_snapshot:null,rate_provenance:null};
 const expected=isolation==='READ COMMITTED'?'DAILY_RATE_ACTOR_NOT_ALLOWED':'DAILY_RATE_LOCKED_CONTEXT_ISOLATION_REQUIRED';
 const state=isolation==='READ COMMITTED'?'42501':'55000';
 const guard=`do $isolation_guard$ begin
 if current_database()<>'cloudtms_test_clone' or current_setting('transaction_isolation')<>${literal(isolation.toLowerCase())}
  or current_setting('transaction_read_only')<>'off' or current_user<>session_user
  or current_setting('server_version_num')::integer/10000<>17
  or not exists(select 1 from private.cloudtms_database_identity where singleton and environment='TEST' and customer_key is null)
  or not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='LEGACY' and owner_epoch=1)
  or exists(select 1 from private.cloudtms_database_releases where status='APPLYING') then
  raise exception 'DAILY_ISOLATION_NATIVE_IDENTITY_REQUIRED' using errcode='55000';end if;
 if exists(select 1 from public.tms_users where id='${CANARY}'::uuid) then
  raise exception 'DAILY_ISOLATION_CANARY_MUST_BE_ABSENT' using errcode='55000';end if;
end; $isolation_guard$;`;
 const assertions=`do $isolation_cases$
declare args jsonb:=${literal(JSON.stringify(request))}::jsonb; state text; message text; checks integer:=0;
begin
 -- Validate the genuine envelope first; no injected context/snapshot or mock.
 perform private.daily_rate_correction_request_assert_v1(args,'PREVIEW');
 begin perform private.daily_rate_correction_health_basis_v1(args,true);
  raise exception 'DAILY_ISOLATION_EXPECTED_REFUSAL_MISSING';
 exception when others then get stacked diagnostics state=returned_sqlstate,message=message_text;
  if state<>${literal(state)} or message<>${literal(expected)} then raise;end if;checks:=checks+1;end;
 -- An unlocked invocation must reach the real actor guard in every mode.
 begin perform private.daily_rate_correction_health_basis_v1(args,false);
  raise exception 'DAILY_ISOLATION_EXPECTED_ACTOR_REFUSAL_MISSING';
 exception when others then get stacked diagnostics state=returned_sqlstate,message=message_text;
  if state<>'42501' or message<>'DAILY_RATE_ACTOR_NOT_ALLOWED' then raise;end if;checks:=checks+1;end;
 begin perform private.daily_rate_correction_health_basis_v1(args,null);
  raise exception 'DAILY_ISOLATION_EXPECTED_LOCK_MODE_REFUSAL_MISSING';
 exception when others then get stacked diagnostics state=returned_sqlstate,message=message_text;
  if state<>'22023' or message<>'DAILY_RATE_LOCK_MODE_REQUIRED' then raise;end if;checks:=checks+1;end;
 -- Malformed requests remain rejected by the real request owner before mode.
 begin perform private.daily_rate_correction_health_basis_v1(args-'context_digest',true);
  raise exception 'DAILY_ISOLATION_EXPECTED_REQUEST_REFUSAL_MISSING';
 exception when others then get stacked diagnostics state=returned_sqlstate,message=message_text;
  if state<>'22023' or message<>'DAILY_RATE_REQUEST_INVALID' then raise;end if;checks:=checks+1;end;
 if checks<>4 or exists(select 1 from public.tms_users where id='${CANARY}'::uuid) then
  raise exception 'DAILY_ISOLATION_CASE_COUNTS_OR_VACANCY_CHANGED';end if;
end; $isolation_cases$;
select jsonb_build_object('programme','DAILY_HEALTH_ISOLATION_GUARD_V1','isolation',current_setting('transaction_isolation'),
 'assertions',4,'canaryAbsent',not exists(select 1 from public.tms_users where id='${CANARY}'::uuid),
 'businessFixtureRows',0,'positiveDailyBasis','NOT_RUN','postWaitCurrentness','NOT_RUN',
 'directRoleRuntime','NOT_RUN','bankingJoin','NOT_INCLUDED') as hr_isolation_result;`;
 const sql=[guard,...selected,assertions].join('\n\n');
 // No business DML, role switch, transaction/isolation override or dynamic SQL.
 check(!/\b(?:insert|update|delete|merge|execute|nextval|setval)\b/i.test(sql.replace(/for share/g,'')),'ISOLATION_EFFECTFUL_PROGRAMME_REFUSED');
 check(!/\b(?:begin isolation|set transaction|set local role|set session authorization)\b/i.test(sql),'ISOLATION_OUTER_CONTROL_REFUSED');
 return Object.freeze({sql,sha256:sha(sql),bytes:Buffer.byteLength(sql),isolation,definitions:4,assertions:4,businessFixtureRows:0,
  preQueryOuterTransactionRequired:true,native:'NOT_RUN',restoration:'NOT_RUN'});
}
export function renderPreQueryReviewScript(options){
 const programme=buildIsolationProgram(options);
 // Review artifact only: root must supply its real shared-turn, marker,
 // transaction identity, full original snapshots/observations and qualifier.
 // A syntactically correct BEGIN does not qualify a native execution route.
 const sql='begin isolation level '+programme.isolation.toLowerCase()+';\n'+programme.sql+'\nrollback;\n';
 return {...programme,sql,sha256:sha(sql),bytes:Buffer.byteLength(sql),route:'UNPROVIDED_ROOT_ADAPTER',execution:'HELD'};
}
export function toCurrentKierMcpInput(){throw Error('DAILY_ISOLATION_PREQUERY_MCP_ABI_UNPROVIDED');}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 process.stderr.write('DAILY_ISOLATION_SOURCE_ONLY_EXECUTION_HELD\n');process.exitCode=2;
}

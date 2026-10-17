// Finite source programme builder. No connection and no caller-supplied SQL.
import fs from'node:fs';import path from'node:path';import {fileURLToPath,pathToFileURL}from'node:url';import {createHash}from'node:crypto';
const here=path.dirname(fileURLToPath(import.meta.url));
export const sha=b=>createHash('sha256').update(b).digest('hex');
export const literal=v=>"'"+String(v).replaceAll("'","''")+"'";
const check=(ok,code)=>{if(!ok)throw Error(code);};
export const CASES=Object.freeze(['CONTACT_CONSTRUCTORS_V1','QT_ADMIN_CONTROLS_V1']);
export function fixturePredicate(pins,alias='u'){
 check(alias==='u'&&pins.fixtureIDs.length===6&&pins.fixtureEmails.length===6,'CONTACT_QT_EXACT_FIXTURE_SCOPE_REQUIRED');
 return `(${alias}.id=any(array[${pins.fixtureIDs.map(v=>literal(v)+'::uuid')}]::uuid[]) or ${alias}.email=any(array[${pins.fixtureEmails.map(literal)}]::text[]))`;
}
export async function buildProgram({root,caseId}){
 check(CASES.includes(caseId),'CONTACT_QT_EXACT_CASE_REQUIRED');
 check(fs.realpathSync.native(root).replaceAll('\\','/')==='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026','CONTACT_QT_PHYSICAL_ROOT_REQUIRED');
 const pins=JSON.parse(fs.readFileSync(here+'/SOURCE_PINS.json'));
 const managed=await import(pathToFileURL(root+'/scripts/cloudtms-managed-release-sql.mjs').href);
 const release=await import(pathToFileURL(root+'/scripts/cloudtms-db-release-lib.mjs').href);
 const transfers=[];
 const source=file=>{const row=pins.rootInputs.find(x=>x.file===file),b=fs.readFileSync(root+'/'+file);check(row&&sha(b)===row.sha256&&b.length===row.bytes,'CONTACT_QT_EXACT_SOURCE_REQUIRED');return b.toString();};
 const atomic=(file)=>{
  const raw=source(file),body=managed.exactAtomicBody(raw);const previous=process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER;
  let executable;try{process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER='CURRENT_USER';executable=release.mapExecutableSqlSource(body,file);}finally{if(previous===undefined)delete process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER;else process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER=previous;}
  // The only admitted semantic rendering is the repository's audited logical owner mapping.
  check(executable===body||executable===body.replace(/owner to postgres/gi,'OWNER TO CURRENT_USER'),'CONTACT_QT_OWNER_MAPPING_EXCEEDED');
  check(managed.scanManagedSql(executable).meta.length===0,'CONTACT_QT_NO_CLIENT_COMMAND_REQUIRED');
  transfers.push({file,completeOriginalSha256:sha(raw),exactAtomicBodySha256:sha(body),executableSha256:sha(executable),completeOriginalPreserved:true,
   onlyOuterClientTransactionFramingRemoved:true,onlyAuditedLogicalOwnerMapping:executable!==body});return executable;
 };
 let sql,expected;
 if(caseId==='CONTACT_CONSTRUCTORS_V1'){
  sql=["set local search_path=pg_catalog,public;",atomic('supabase/migrations/16102026_1205_weekly_contact_20_automatic_followup_tranche.sql'),atomic('supabase/migrations/16102026_1205_weekly_contact_30_submission_cohort_identity.sql'),
   "select jsonb_build_object('programme','CONTACT_CONSTRUCTORS_V1','completeConstructorUnits',2,'businessFixtureRows',0,'originalAssertions','EXECUTED_WITHOUT_SUBSTITUTION','outerIsolation',current_setting('transaction_isolation')) as programme_result;"].join('\n');
  expected={programme:caseId,completeConstructorUnits:2,businessFixtureRows:0,originalAssertions:'EXECUTED_WITHOUT_SUBSTITUTION',outerIsolation:'read committed'};
 }else{
  const actor=await import(pathToFileURL(root+'/'+pins.actorPreflightSource).href),original=await actor.buildActorProgram({root});
  const parsed=managed.scanManagedSql(original.sql),preflight=original.sql.slice(parsed.statements[0].start,parsed.statements[1].end);
  check(parsed.statements[0].tokens.slice(0,4).join(' ')==='SET LOCAL SEARCH_PATH ='
   &&parsed.statements[1].tokens[0]==='DO'&&preflight.includes('DAILY_ACTOR_NATIVE_STORE_INVARIANT_OR_TRIGGER_CHANGED')
   &&!preflight.includes('create or replace function'),'CONTACT_QT_EXACT_ACTOR_PREFLIGHT_ONLY');
  transfers.push({file:pins.actorPreflightSource,completeProgrammeSha256:original.sqlSha256,
   selectedUnchangedPrefixSha256:sha(preflight),selectedStatements:2,remainderNotSent:true,rawPrefixUnchanged:true});
  const helpers=pins.selectedHelpers.map(row=>{
   const body=fs.readFileSync(here+'/'+row.copy,'utf8');check(sha(body)===row.sha256,'CONTACT_QT_PRIVATE_HELPER_SOURCE_CHANGED');
   const expected=row.nativeExpected;
   check(expected?.owner==='postgres','CONTACT_QT_HELPER_NATIVE_OWNER_EXPECTATION_REQUIRED');
   return `do $helper_prior$ begin if to_regprocedure(${literal(row.signature)}) is not null and (
    encode(sha256(convert_to(pg_get_functiondef(to_regprocedure(${literal(row.signature)})),'UTF8')),'hex')<>${literal(expected.definition_sha256)}
    or (select pg_get_userbyid(proowner) from pg_proc where oid=to_regprocedure(${literal(row.signature)}))<>current_user) then
    raise exception 'CONTACT_QT_EXISTING_HELPER_SOURCE_OR_OWNER_CHANGED' using errcode='55000';end if;end;$helper_prior$;\n`+body+
    `\nalter function ${row.signature} owner to current_user;revoke all on function ${row.signature} from public,anon,authenticated,service_role;`;
  });
  const absent=`do $qt_absence$ begin if to_regclass('public.candidate_push_timing_policy') is not null or to_regclass('public.candidate_push_timing_commands') is not null
   or exists(select 1 from unnest(array['private.weekly_contact_assert_closed_v1(jsonb,text[],text[],text)',
   'private.weekly_contact_revision_v1(jsonb,boolean)','private.candidate_push_timing_policy_validate_v1(jsonb)',
   'private.weekly_contact_deployment_lock_v1(uuid,text)','private.candidate_push_timing_immutable_guard_v1()',
   'public.candidate_push_timing_get_v1(jsonb)','public.candidate_push_timing_save_v1(jsonb)']) n where to_regprocedure(n) is not null)
   then raise exception 'CONTACT_QT_ONE_TIME_NATIVE_ABSENCE_REQUIRED' using errcode='55000';end if;end;$qt_absence$;`;
  sql=[preflight,absent,...helpers,atomic('supabase/migrations/15102026_0800_candidate_push_timing_authority.sql'),atomic('supabase/repeatable/15102026_0800_candidate_push_timing_authority_v1.sql'),fs.readFileSync(here+'/qt-assertions.sql','utf8')].join('\n');
  expected={programme:caseId,fixtureUsers:6,policyRows:1,commandAuditRows:2,ordinaryPayInactiveDenials:10,sameTransactionRevocationRefusals:2,
   changedBodyRefusals:1,staleCasRefusals:1,effectiveRoleAclChecks:3,directRoleApi:'NOT_RUN',crossAgencyProof:'NOT_RUN',postWaitRace:'NOT_RUN',outerIsolation:'read committed'};
 }
 const parsed=managed.scanManagedSql(sql);check(parsed.meta.length===0&&parsed.statements.length<=1000&&Buffer.byteLength(sql)<=8000000,'CONTACT_QT_FINITE_MCP_ENVELOPE_REQUIRED');
 return {caseId,sql,sqlSha256:sha(sql),sqlBytes:Buffer.byteLength(sql),statements:parsed.statements.length,transfers,expected,
  businessFixtureRows:caseId==='QT_ADMIN_CONTROLS_V1'?6:0,bankBodiesIncluded:0,providerEffects:'NONE_AUTHORED',native:'NOT_RUN'};
}

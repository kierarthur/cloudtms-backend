// SOURCE ONLY: exact body expectations, full release append/reversal and finite exception evidence.
import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import assert from 'node:assert/strict';import {fileURLToPath} from 'node:url';
import {functionEvents,sqlTokens} from '../cn-a0-isolation-source-followup/sql-function-source-index.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(process.argv[2]),stamp='14102026_1216',sha=b=>crypto.createHash('sha256').update(b).digest('hex'),read=f=>fs.readFileSync(path.join(here,f),'utf8'),save=(f,v)=>fs.writeFileSync(path.join(here,f),typeof v==='string'?v:JSON.stringify(v,null,2)+'\n');
const coverage=JSON.parse(read('CURRENT_NATIVE_OWNER_COVERAGE.json')),forward=JSON.parse(read('COMPLETE_CURRENT_FORWARD_AND_REVERSE.json')),byId=new Map(coverage.routines.map(r=>[r.id,r]));
const wanted=new Set([...forward.changedCurrentOwners.map(r=>r.id),...forward.newPrivateFunctions]),expected=new Map();
for(const file of ['sql/'+stamp+'_a0_transaction_fence_helpers.sqlinc','sql/'+stamp+'_a0_native_child_raw_intents.sqlinc',...forward.reversals.map(r=>r.output)])for(const e of functionEvents(read(file)).events.filter(e=>e.kind==='CREATE'&&wanted.has(e.id)))expected.set(e.id,{signature:e.id,bodySha256:sha(e.body.replaceAll('\r\n','\n')),source:file,kind:'changed-or-new-proposed'});
assert.equal(expected.size,12);
for(const t of coverage.catalogueBeforeTriggers){const r=byId.get(t.routineId);assert.ok(r?.body);if(!expected.has(r.id))expected.set(r.id,{signature:r.id,bodySha256:sha(r.body.replaceAll('\r\n','\n')),source:r.file,kind:'unchanged-current-native-BEFORE-owner'});}
const exp=[...expected.values()].sort((a,b)=>a.signature.localeCompare(b.signature));save('SOURCE_EXPECTED_BODY_HASHES.json',{sourceOnly:true,observedNative:false,normalization:'CRLF to LF only',expect:exp});
const quote=s=>"'"+s.replaceAll("'","''")+"'",bodyValues=exp.map(e=>'    ('+quote(e.signature)+','+quote(e.bodySha256)+')').join(',\n');
save('sql/'+stamp+'_a0_transaction_fence_reviewed_bodies.sqlinc',`-- Source-exact prosrc expectations, not an observed native/canonical seal.
do $a0_source_bodies$
declare r record;v_body text;
begin
  for r in select * from(values
${bodyValues}
  ) expected(signature,sha256) loop
    select p.prosrc into v_body from pg_catalog.pg_proc p where p.oid=to_regprocedure(r.signature);
    if not found or pg_catalog.encode(extensions.digest(pg_catalog.convert_to(
      pg_catalog.replace(v_body,E'\\r\\n',E'\\n'),'UTF8'),'sha256'),'hex') is distinct from r.sha256 then
      raise exception 'CORRECTION_A0_REVIEWED_BODY_CHANGED' using errcode='55000';end if;
  end loop;
end;
$a0_source_bodies$;
`);
const guardRows=[['timesheet_validations','zzzz_healthroster_a0_reference_before_v1','private.timesheet_correction_a0_reference_before_v1()'],['invoice_lines','aaa_healthroster_a0_reference_before_v1','private.timesheet_correction_a0_reference_before_v1()'],['timesheets_financials','aaa_healthroster_a0_financial_before_v1','private.timesheet_correction_a0_reference_before_v1()'],['timesheets','aaa_healthroster_a0_booking_before_v1','private.timesheet_correction_a0_booking_before_v1()'],['candidates','zzzz_healthroster_a0_candidate_before_v1','private.timesheet_correction_a0_candidate_before_v1()'],...coverage.scopeTables.slice(5).map(t=>[t,'aaa_healthroster_a0_child_before_v1','private.timesheet_correction_a0_child_reference_before_v1()'])];
assert.equal(guardRows.length,14);const current=coverage.catalogueBeforeTriggers.map(t=>[t.table,t.name,t.routineId,t.definition]),currentKeys=new Set(current.map(t=>t[0]+'.'+t[1]));assert.ok(guardRows.every(t=>!currentKeys.has(t[0]+'.'+t[1])));const allKeys=[...current.map(t=>t.slice(0,2)),...guardRows.map(t=>t.slice(0,2))];
const helperValues=forward.newPrivateFunctions.map(f=>'    ('+[f,f.includes('fence_tokens_')?'sql':'plpgsql',f.includes('fence_tokens_')?'i':'v',f.includes('request_tokens_')?'search_path=pg_catalog, public, private, extensions, pg_temp':'search_path=pg_catalog, pg_temp'].map(quote).join(',')+')').join(',\n'),guardValues=guardRows.map(r=>'    ('+r.map(quote).join(',')+')').join(',\n'),nativeValues=current.map(r=>'    ('+r.map(quote).join(',')+')').join(',\n'),keyValues=allKeys.map(r=>'    ('+r.map(quote).join(',')+')').join(',\n'),scope=coverage.scopeTables.map(quote).join(',');
const nativeColumns=[...coverage.rawFieldPins.map(p=>[p.table,p.column,p.definition.type]),['timesheets','booking_id','text'],['invoice_lines','booking_id','text'],['timesheets','occupant_key_norm','text'],['candidates','key_norm','text'],['candidates','nhsp_hr_name_aliases','jsonb']],columnValues=nativeColumns.map(r=>'    ('+r.map(quote).join(',')+')').join(',\n');
save('sql/'+stamp+'_a0_transaction_fence_source_contract.sql',`-- Catalogue/SELECT-only prospective mandatory verifier; genuine outer RR compatible.
-- No native business owner, token acquire, DML or fixture is invoked here.
-- Complete canonical compiled CHECK/policy/index expressions and native schedules remain separate gates.
do $a0_source_contract$
declare v_oid oid;r record;v_count integer;
begin
  v_oid:=to_regclass('private.timesheet_correction_a0_transaction_fences');
  if v_oid is null or not exists(select 1 from pg_catalog.pg_class c where c.oid=v_oid and c.relkind='r'
      and c.relrowsecurity and c.relowner=(current_user::regrole)::oid and c.relchecks=2)
    or exists(select 1 from pg_catalog.pg_class c cross join lateral pg_catalog.aclexplode(
      coalesce(c.relacl,pg_catalog.acldefault('r',c.relowner))) a where c.oid=v_oid and a.grantee<>c.relowner)
    or exists(select 1 from pg_catalog.pg_trigger t where t.tgrelid=v_oid)
    or exists(select 1 from pg_catalog.pg_constraint c where c.conrelid=v_oid and c.contype not in('c','x')) then
    raise exception 'CORRECTION_A0_PRIVATE_ARBITER_CONTRACT_INVALID' using errcode='55000';end if;
  if (select count(*) from pg_catalog.pg_attribute a where a.attrelid=v_oid and a.attnum>0 and not a.attisdropped)<>6
    or exists(select 1 from(values(1,'token_kind','text',true),(2,'uuid_identity','uuid',false),
      (3,'text_identity','text',false),(4,'retained','boolean',true),(5,'intent_span','numrange',true),
      (6,'touch_bit','boolean',true)) e(num,name,typ,nn) where not exists(select 1 from pg_catalog.pg_attribute a
        where a.attrelid=v_oid and a.attnum=e.num and a.attname=e.name and a.atttypid=to_regtype(e.typ)
        and a.attnotnull=e.nn and not a.attisdropped and a.attidentity='' and a.attgenerated=''
        and (e.typ<>'text' or a.attcollation='pg_catalog."C"'::regcollation))) then
    raise exception 'CORRECTION_A0_ARBITER_COLUMNS_CHANGED' using errcode='55000';end if;
  if current_setting('block_size')::integer<>8192 or not exists(select 1 from pg_catalog.pg_extension e
      join pg_catalog.pg_namespace n on n.oid=e.extnamespace where e.extname='btree_gist' and n.nspname='public')
    or not exists(select 1 from pg_catalog.pg_collation c where c.oid='pg_catalog.default'::regcollation and c.collisdeterministic) then
    raise exception 'CORRECTION_A0_NATIVE_EXCLUSION_PROFILE_UNPROVED' using errcode='55000';end if;
  if (select count(*) from pg_catalog.pg_constraint c where c.conrelid=v_oid)<>4
    or (select count(*) from pg_catalog.pg_constraint c where c.conrelid=v_oid and c.contype='c' and c.convalidated
      and c.conname in('timesheet_correction_a0_fences_identity_shape','timesheet_correction_a0_fences_span_shape'))<>2
    or (select count(*) from pg_catalog.pg_policy p where p.polrelid=v_oid)<>1
    or not exists(select 1 from pg_catalog.pg_policy p where p.polrelid=v_oid
      and p.polname='cloudtms_miget_service_owner_all' and p.polcmd='*' and p.polpermissive
      and pg_catalog.pg_get_expr(p.polqual,p.polrelid,false)='true'
      and pg_catalog.pg_get_expr(p.polwithcheck,p.polrelid,false)='true'
      and (current_user::regrole)::oid=any(p.polroles) and 'service_role'::regrole::oid=any(p.polroles)
      and not exists(select 1 from unnest(p.polroles) x(roleid)
        where x.roleid not in((current_user::regrole)::oid,'service_role'::regrole::oid))) then
    raise exception 'CORRECTION_A0_ARBITER_CONSTRAINTS_CHANGED' using errcode='55000';end if;
  for r in select * from(values
${columnValues}
  ) e(table_name,column_name,type_name) loop
    if not exists(select 1 from pg_catalog.pg_attribute a where a.attrelid=to_regclass('public.'||r.table_name)
      and a.attnum>0 and not a.attisdropped and a.attname=r.column_name and a.atttypid=to_regtype(r.type_name)
      and(r.type_name<>'text' or exists(select 1 from pg_catalog.pg_collation c
        where c.oid=a.attcollation and c.collisdeterministic))) then
      raise exception 'CORRECTION_A0_RAW_NATIVE_FIELD_CHANGED' using errcode='55000';end if;
  end loop;
  for r in select * from(values('timesheet_correction_a0_fences_uuid_excl',2,'uuid','gist_uuid_ops','uuid_identity'),
      ('timesheet_correction_a0_fences_text_excl',3,'text','gist_text_ops','text_identity')) e(name,num,typ,opclass,column_name) loop
    if not exists(select 1 from pg_catalog.pg_constraint c join pg_catalog.pg_index i on i.indexrelid=c.conindid
      join pg_catalog.pg_class x on x.oid=i.indexrelid join pg_catalog.pg_am am on am.oid=x.relam
      where c.conrelid=v_oid and c.conname=r.name and c.contype='x' and c.convalidated and not c.condeferrable
        and not c.condeferred and c.conkey=array[1,r.num,5]::smallint[]
        and c.conexclop=array['pg_catalog.=(text,text)'::regoperator::oid,
          to_regoperator('pg_catalog.=('||r.typ||','||r.typ||')')::oid,'pg_catalog.&&(anyrange,anyrange)'::regoperator::oid]
        and am.amname='gist' and i.indisexclusion and i.indisvalid and i.indisready and i.indislive
        and i.indnatts=3 and i.indnkeyatts=3 and i.indexprs is null
        and i.indclass[0]=(select o.oid from pg_catalog.pg_opclass o join pg_catalog.pg_namespace n on n.oid=o.opcnamespace
          where n.nspname='public' and o.opcname='gist_text_ops' and o.opcmethod=x.relam)
        and i.indclass[1]=(select o.oid from pg_catalog.pg_opclass o join pg_catalog.pg_namespace n on n.oid=o.opcnamespace
          where n.nspname='public' and o.opcname=r.opclass and o.opcmethod=x.relam)
        and i.indclass[2]=(select o.oid from pg_catalog.pg_opclass o join pg_catalog.pg_namespace n on n.oid=o.opcnamespace
          where n.nspname='pg_catalog' and o.opcname='range_ops' and o.opcmethod=x.relam)
        and i.indcollation[0]='pg_catalog."C"'::regcollation
        and i.indcollation[1]=case when r.typ='text' then 'pg_catalog."C"'::regcollation::oid else 0::oid end
        and pg_catalog.pg_get_expr(i.indpred,i.indrelid,false)='('||r.column_name||' IS NOT NULL)') then
      raise exception 'CORRECTION_A0_EXCLUSION_ARBITER_CHANGED' using errcode='55000';end if;
  end loop;
  for r in select * from(values
${helperValues}
  ) e(signature,language,volatility,configuration) loop
    if to_regprocedure(r.signature) is null or not exists(select 1 from pg_catalog.pg_proc p
      join pg_catalog.pg_language l on l.oid=p.prolang where p.oid=to_regprocedure(r.signature)
      and p.prosecdef and p.proowner=(current_user::regrole)::oid and l.lanname=r.language
      and p.provolatile::text=r.volatility and p.proconfig=array[r.configuration] and not p.proleakproof)
      or exists(select 1 from pg_catalog.pg_proc p cross join lateral pg_catalog.aclexplode(
        coalesce(p.proacl,pg_catalog.acldefault('f',p.proowner))) a where p.oid=to_regprocedure(r.signature) and a.grantee<>p.proowner)
      or has_function_privilege('anon',to_regprocedure(r.signature),'EXECUTE')
      or has_function_privilege('authenticated',to_regprocedure(r.signature),'EXECUTE')
      or has_function_privilege('service_role',to_regprocedure(r.signature),'EXECUTE') then
      raise exception 'CORRECTION_A0_PRIVATE_HELPER_CONTRACT_CHANGED' using errcode='55000';end if;
  end loop;
  for r in select * from(values
${guardValues}
  ) e(table_name,trigger_name,signature) loop
    if not exists(select 1 from pg_catalog.pg_trigger t where t.tgrelid=to_regclass('public.'||r.table_name)
      and t.tgname=r.trigger_name and t.tgfoid=to_regprocedure(r.signature) and t.tgenabled='O'
      and t.tgtype=31 and t.tgqual is null and t.tgnargs=0 and t.tgattr=''::int2vector and t.tgconstraint=0) then
      raise exception 'CORRECTION_A0_RAW_ALL_MUTATION_GUARD_CHANGED' using errcode='55000';end if;
  end loop;
  for r in select * from(values
${nativeValues}
  ) e(table_name,trigger_name,signature,definition) loop
    if not exists(select 1 from pg_catalog.pg_trigger t where t.tgrelid=to_regclass('public.'||r.table_name)
      and t.tgname=r.trigger_name and t.tgfoid=to_regprocedure(r.signature) and t.tgenabled='O'
      and pg_catalog.pg_get_triggerdef(t.oid,false)=r.definition) then
      raise exception 'CORRECTION_A0_NATIVE_BEFORE_OWNER_CHANGED' using errcode='55000';end if;
  end loop;
  if exists(select 1 from pg_catalog.pg_trigger t join pg_catalog.pg_class c on c.oid=t.tgrelid
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relname in(${scope})
      and not t.tgisinternal and(t.tgtype&1)=1 and(t.tgtype&2)=2 and not exists(select 1 from(values
${keyValues}
      ) e(table_name,trigger_name) where e.table_name=c.relname and e.trigger_name=t.tgname)) then
    raise exception 'CORRECTION_A0_UNKNOWN_NATIVE_BEFORE_OWNER' using errcode='55000';end if;
  -- No positive SEND/atomic or isolation change: preserve all nine RR roots and snapshot assertions.
end;
$a0_source_contract$;
\\ir ${stamp}_a0_transaction_fence_reviewed_bodies.sqlinc
`);
save('SOURCE_GUARD_AND_VERIFIER_COMPOSITION.json',{sourceOnly:true,scopeTables:coverage.scopeTables,guardRows,unchangedCatalogueBeforeOwners:current.map(r=>({table:r[0],trigger:r[1],signature:r[2],definition:r[3]})),bodyExpectations:exp.length,outerRepeatableReadCompatible:true,nativeExecuted:false,canonicalConstraintPolicyIndexSeal:false});
const releaseFile='supabase/release/current-release.json',releaseBytes=fs.readFileSync(path.join(root,releaseFile)),release=JSON.parse(releaseBytes),verifier='supabase/verification/'+stamp+'_a0_transaction_fence_source_contract.sql',append={...release,verificationFiles:[...release.verificationFiles,verifier],newVerificationFiles:[...release.newVerificationFiles,verifier]},after=JSON.stringify(append,null,2)+'\n';
assert.ok(!release.verificationFiles.includes(verifier));save('release.current-release.complete-proposed.json',after);const reverse=structuredClone(append);reverse.verificationFiles.pop();reverse.newVerificationFiles.pop();assert.deepEqual(reverse,release);save('RELEASE_AND_REGISTRY_COMPOSITION.json',{sourceOnly:true,currentRootCommit:coverage.currentRootCommit,release:{input:releaseFile,beforeSha256:sha(releaseBytes),beforeBytes:releaseBytes.length,output:'release.current-release.complete-proposed.json',afterSha256:sha(after),afterBytes:Buffer.byteLength(after),beforeNEW:release.newVerificationFiles.length,beforeUPGRADE:release.verificationFiles.length,appendNEW:[verifier],appendUPGRADE:[verifier],reverseObjectEqual:true,reverseOriginalBytesBase64:releaseBytes.toString('base64')},registry:{appendPrivate:forward.newPrivateFunctions,appendPublic:[],appendService:[],preserveExistingA0NormalizationHelpers:4,preserveExistingA0CreatorHelpers:6,installedCensus:null,bankCompositionRequired:true}});
const exceptionRows=[],unmatched=[];
for(const writer of coverage.writers){const r=byId.get(writer.id),ts=sqlTokens(r.body),stack=[];for(let i=0;i<ts.length;i++){const t=ts[i];if(t.kind!=='word')continue;
  if(t.value==='begin')stack.push({kind:'begin',start:t.start,outer:!stack.some(x=>x.kind==='begin')});
  else if(t.value==='case'&&ts[i-1]?.value!=='end')stack.push({kind:'case',start:t.start});
  else if(t.value==='exception'&&ts[i+1]?.value==='when'){const b=[...stack].reverse().find(x=>x.kind==='begin');assert.ok(b,'Real EXCEPTION block '+r.id);b.exception=t.start;}
  else if(t.value==='end'&&!['if','loop'].includes(ts[i+1]?.value)){const b=stack.pop();if(b?.kind==='begin'&&b.exception!==undefined){const startLine=r.body.slice(0,b.start).split('\n').length,exceptionLine=r.body.slice(0,b.exception).split('\n').length,endLine=r.body.slice(0,t.end).split('\n').length,handler=r.body.slice(b.exception,t.end),protectedBody=r.body.slice(b.start,b.exception),conditions=[...handler.matchAll(/\bwhen\s+([^;]*?)\s+then\b/gi)].map(x=>x[1]);exceptionRows.push({routine:r.id,source:r.file,definitionSha256:r.definitionSha256,startBodyLine:startLine,exceptionBodyLine:exceptionLine,endBodyLine:endLine,wholeFunctionBlock:b.outer&&r.body.slice(t.end).trim().replace(/;$/,'')==='',directMutationsProtected:writer.mutations.filter(m=>m.bodyLine>=startLine&&m.bodyLine<exceptionLine),protectedBody,handler,containsLexicalRethrow:/\braise\s*;/i.test(handler),mayCatchConcurrency:conditions.some(c=>/\b(?:others|lock_not_available|deadlock_detected|serialization_failure)\b|55P03|40001/i.test(c)),literalPotentialCallees:r.literalPotentialCallees.filter(x=>protectedBody.includes(x.slice(0,x.indexOf('(')))),nativeInvokedPathProved:false});}}
}if(stack.length)unmatched.push({routine:r.id,stack});}
assert.equal(unmatched.length,0,'Complete direct-writer block association');
save('CURRENT_EXCEPTION_HANDLER_SOURCE_EVIDENCE.json',{sourceOnly:true,currentRootCommit:coverage.currentRootCommit,completePotentialDefinitions:coverage.selectedCompleteDefinitions,writerDefinitions:coverage.writerDefinitions,handlerExtractCount:exceptionRows.length,extractionMethod:'SQL-token BEGIN/CASE/END association; EXCEPTION WHEN only; full protected body and handler. All complete potential callees retained in CURRENT_NATIVE_OWNER_COVERAGE. Semantic caught-path/native rollback proof remains separate.',handlers:exceptionRows});
console.log(JSON.stringify({proposedBodies:12,totalBodyExpectations:exp.length,guards:guardRows.length,unchangedBeforeOwners:current.length,newVerifier:verifier,originalNEW:release.newVerificationFiles.length,originalUPGRADE:release.verificationFiles.length,handlerExtracts:exceptionRows.length,nativeRun:false}));

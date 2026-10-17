// Complete current-owner contribution only. Never writes I2 or contacts a DB.
import fs from'node:fs';import path from'node:path';import{fileURLToPath,pathToFileURL}from'node:url';import{execFileSync}from'node:child_process';import{createHash}from'node:crypto';
export const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..'),root=path.resolve(repo,'../healthroster-install2-14102026');
export const checkpoint='5ecc4c73113660edf48a5bca2f3a478453a81c2b',old='a6739abd7382759ebdbf8a58e7877835258ede9c',oldFolder='docs/healthroster-build/proposals/kept-pay-identical-rekey-current-root-forward-20261014';
export const sha=x=>createHash('sha256').update(x).digest('hex'),blob=(ref,p)=>execFileSync('git',['show',ref+':'+p],{cwd:repo,windowsHide:true,maxBuffer:64_000_000}),prior=p=>blob(old,oldFolder+'/'+p).toString();
export function splice(s,b,a){if(s.split(b).length!==2)throw Error('UNIQUE_CURRENT_ANCHOR_REQUIRED:'+sha(b));return s.replace(b,()=>a);}
export function apply(s,hunks){for(const h of hunks)s=splice(s,h.before,h.after);return s;}
export function reverse(s,hunks){for(const h of [...hunks].reverse())s=splice(s,h.after,h.before);return s;}
const write=(p,s)=>{fs.mkdirSync(path.dirname(path.join(here,p)),{recursive:true});fs.writeFileSync(path.join(here,p),s);};
const census=JSON.parse(fs.readFileSync(path.join(here,'CURRENT_CENSUS.json'))),allChanges=[...JSON.parse(prior('REKEY_CURRENT_CALLERS_AND_INPUT_PINS.json')).changes,...JSON.parse(prior('KEPT_CREDIT_CURRENT_OWNERS_AND_INPUT_PINS.json')).changes].filter(c=>c.signature);
const recipes=[],newChanges=[],outputs={};
for(const c of allChanges){const owner=census.owners.find(o=>o.signature===c.signature),s=fs.readFileSync(path.join(here,owner.file),'utf8').replace(/\n$/,'');if(sha(s)!==owner.completeCurrentStatementSha256)throw Error('CURRENT_WHOLE_OWNER_DRIFT');let after=apply(s,c.hunks);if(reverse(after,c.hunks)!==s)throw Error('FULL_REVERSE_REQUIRED');outputs[path.basename(c.forward_file)]=after+'\n';recipes.push({...owner,hunks:c.hunks,successorFile:'forward/'+path.basename(c.forward_file),completeSuccessorStatementSha256:sha(after),fullReverseByteEquality:true});}
for(const name of['01_identical_rekey_prior.sqlinc','05_kept_as_paid_facts.sqlinc','06_kept_as_paid_context.sqlinc','07_keep_as_paid_decide.sqlinc','08_credited_targets_admit.sqlinc','12_keep_ack_native_guard.sqlinc'])outputs[name]=prior('forward/'+name);
function amend(name,before,after,reason){const h={before,after,reason};outputs[name]=splice(outputs[name],before,after);newChanges.push({file:'forward/'+name,...h});}

// Select the latest genuine acknowledgement for the event before inspecting
// mutable current reason/facts. Reason is part of immutable decision identity,
// not a licence to select an older event acknowledgement.
const contextOld=outputs['06_kept_as_paid_context.sqlinc'],contextLf=contextOld.replaceAll('\r\n','\n');
const oldAck=contextLf.slice(contextLf.indexOf('  if v_reason is not null then\n'),contextLf.indexOf("  return jsonb_build_object('schema_version','WEEKLY_SOURCE_KEPT_AS_PAID_CONTEXT_V1'"));
const newAck=`  -- Latest actual event acknowledgement remains visible even when today's
  -- reason is unavailable/different. No reason/hash filter resurrects history.
  select audit.* into v_ack from public.audit_events audit
  where audit.object_type='weekly_source_kept_as_paid_decisions' and audit.object_id_text=v_event.id::text
    and audit.action='WEEKLY_SOURCE_KEEP_AS_PAID_ACKNOWLEDGED'
    and audit.after_json->>'schema_version'='WEEKLY_SOURCE_KEEP_AS_PAID_ACK_V1'
    and audit.after_json->>'work_event_id'=v_event.id::text
  order by (audit.after_json->>'acknowledgement_sequence')::bigint desc,audit.id desc limit 1;
  if found then
    select * into strict v_decision from public.weekly_source_kept_as_paid_decisions
      where id=(v_ack.after_json->>'decision_id')::uuid and work_event_id=v_event.id;
    if v_ack.after_json->>'reason' is distinct from v_decision.reason
      or v_ack.after_json->'credit_invoice_line_id' is distinct from coalesce(to_jsonb(v_decision.credit_invoice_line_id),'null'::jsonb)
      or v_ack.after_json->>'facts_fingerprint' is distinct from encode(v_decision.facts_fingerprint,'hex')
      or v_ack.after_json->>'actor_user_id' is distinct from v_ack.actor_user_id::text then
      raise exception 'WEEKLY_SOURCE_KEEP_AS_PAID_ACK_LINEAGE_UNBOUND' using errcode='55000'; end if;
  end if;
  if v_reason is not null then
    v_facts:=private.weekly_source_kept_as_paid_facts_v1(v_event.id,v_reason);
    v_applies:=v_ack.id is not null and not v_source_present and v_decision.reason=v_reason
      and v_decision.facts_fingerprint=v_facts;
  end if;
`;
const oldAckRaw=contextOld.includes('\r\n')?oldAck.replaceAll('\n','\r\n'):oldAck;
amend('06_kept_as_paid_context.sqlinc',oldAckRaw,newAck,'Latest genuine event ACK before current reason/facts; retain old actor/time/history.');

// Fresh read-only scope before BOTH replay branches. This exact authority
// grain is copied from the existing kept context preamble and calls the actual
// current Office permission owner. It never needs a new financial receipt.
const authorityStart=contextLf.indexOf('  select * into strict v_event'),authorityEnd=contextLf.indexOf('  v_source_present:=');
const authorityPrefix=contextLf.slice(authorityStart,authorityEnd);
const admission=`-- Owner-private historical ACK admission; no financial calculation or write.
create or replace function private.weekly_source_kept_as_paid_read_admit_v1(p_work_event_id uuid,p_actor_user_id uuid)
returns void language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_event public.weekly_work_events%rowtype; v_resolution public.weekly_source_row_resolutions%rowtype;
  v_publication uuid; v_upload uuid; v_cycle uuid; v_week date;
begin
  perform private.weekly_source_query_require_service_v1();
  if private.weekly_source_pay_actor_allowed_v1(p_actor_user_id) is distinct from true then
    raise exception 'WEEKLY_SOURCE_PAYMENT_AUTHORISER_REQUIRED' using errcode='42501'; end if;
${authorityPrefix}end;
$function$;
alter function private.weekly_source_kept_as_paid_read_admit_v1(uuid,uuid) owner to postgres;
revoke all on function private.weekly_source_kept_as_paid_read_admit_v1(uuid,uuid) from public,anon,authenticated,service_role;
`;
outputs['04_kept_read_admission.sqlinc']=admission;
const firstReplay="  select audit.* into v_audit from public.audit_events audit";
let decide=outputs['07_keep_as_paid_decide.sqlinc'];
// Two occurrences are deliberately identified by their different prefixes.
const replayHunks=[{before:"  v_request_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_KEEP_AS_PAID_REQUEST_V1',p_request);\n".replaceAll('\n',decide.includes('\r\n')?'\r\n':'\n'),after:"  v_request_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_KEEP_AS_PAID_REQUEST_V1',p_request);\n  perform private.weekly_source_kept_as_paid_read_admit_v1(v_event_id,v_actor);\n"},{before:"  perform pg_advisory_xact_lock(hashtextextended('weekly-source-kept-as-paid-event:'||v_event.id::text,0));".replaceAll('\n','\n'),after:"  perform pg_advisory_xact_lock(hashtextextended('weekly-source-kept-as-paid-event:'||v_event.id::text,0));\n  perform private.weekly_source_kept_as_paid_read_admit_v1(v_event_id,v_actor);"}];
for(const h of replayHunks)amend('07_keep_as_paid_decide.sqlinc',h.before,h.after,'Fresh actual event/group/client/date VIEW_PAY_WORK admission before revealing exact saved ACK.');

// New bigint counters cross every JSON boundary as canonical decimal strings.
amend('07_keep_as_paid_decide.sqlinc',"'acknowledgement_sequence',v_sequence,'idempotent_replay',false", "'acknowledgement_sequence',v_sequence::text,'idempotent_replay',false",'V6 canonical decimal-string counter in the retained actual result, including replay.');
amend('07_keep_as_paid_decide.sqlinc',"'actor_user_id',v_actor,'acknowledged_at_utc',v_acknowledged_at,'acknowledgement_sequence',v_sequence,", "'actor_user_id',v_actor,'acknowledged_at_utc',v_acknowledged_at,'acknowledgement_sequence',v_sequence::text,",'V6 canonical decimal-string counter in the immutable typed audit payload.');
amend('12_keep_ack_native_guard.sqlinc',"    or v_after->>'schema_version' is distinct from 'WEEKLY_SOURCE_KEEP_AS_PAID_ACK_V1'",`    or v_after->>'schema_version' is distinct from 'WEEKLY_SOURCE_KEEP_AS_PAID_ACK_V1'
    or jsonb_typeof(v_after->'acknowledgement_sequence') is distinct from 'string'
    or v_after->>'acknowledgement_sequence' !~ '^[1-9][0-9]{0,18}$'
    or (length(v_after->>'acknowledgement_sequence')=19
      and v_after->>'acknowledgement_sequence'>'9223372036854775807')
    or jsonb_typeof(v_after#>'{result,acknowledgement_sequence}') is distinct from 'string'`,'Validate canonical positive bounded PGbigint strings before any sequence cast; forbid numeric JSON in both retained representations.');

// Removing an existing genuine pointed credit changes the same facts census.
// It must use the same Candidate/root-before-invoice protocol, including a
// removal-only request. Plain unpointed adjustments retain their original arm.
const removalTarget=`exists(select 1 from public.invoice_lines removed
      cross join lateral jsonb_array_elements_text(case when jsonb_typeof(p_payload->'remove_invoice_line_ids')='array'
        then p_payload->'remove_invoice_line_ids' else '[]'::jsonb end) requested(value)
      where removed.invoice_id=p_invoice_id and removed.id=case when pg_input_is_valid(requested.value,'uuid')
        then requested.value::uuid else null end and removed.meta_json ? 'credited_work_event_id')`;
const invoiceRecipe=recipes.find(r=>r.signature==='public.invoice_apply_edits(uuid,jsonb,uuid)'),oldHook=invoiceRecipe.hunks[0].after;
const hookEnd="then p_payload->'add_adjustments' else '[]'::jsonb end) item where item ? 'credited_work_event_id') then";
amend('09_complete_invoice_apply_edits.sqlinc',oldHook,oldHook.replace("and exists(select 1 from jsonb_array_elements", "and (exists(select 1 from jsonb_array_elements").replace(hookEnd,"then p_payload->'add_adjustments' else '[]'::jsonb end) item where item ? 'credited_work_event_id')\n    or "+removalTarget+") then"),'Include genuine pointed-credit removals in the actual early native I-1 prehook.');
const oldFrameAdmission=`  if exists(select 1 from jsonb_array_elements(case when jsonb_typeof(p_payload->'add_adjustments')='array'
    then p_payload->'add_adjustments' else '[]'::jsonb end) requested where requested ? 'credited_work_event_id') then`;
amend('10_complete_adjustments_apply.sqlinc',oldFrameAdmission,oldFrameAdmission.replace(") then",')\n    or '+removalTarget+' then'),'Removal-only pointed credits require the same actual parent/PID/XID/full-payload frame.');
amend('08_credited_targets_admit.sqlinc','v_authority jsonb; v_ids uuid[]; v_id uuid;', 'v_authority jsonb; v_ids uuid[]; v_add_ids uuid[]; v_id uuid;','Separate add eligibility from immutable removal identity.');
amend('08_credited_targets_admit.sqlinc',"  if jsonb_typeof(p_payload) is distinct from 'object' or jsonb_typeof(p_payload->'add_adjustments') is distinct from 'array' then",`  if jsonb_typeof(p_payload) is distinct from 'object'
    or (p_payload ? 'add_adjustments' and jsonb_typeof(p_payload->'add_adjustments') is distinct from 'array')
    or (p_payload ? 'remove_invoice_line_ids' and jsonb_typeof(p_payload->'remove_invoice_line_ids') is distinct from 'array') then`,'Permit bounded genuine removal-only requests; native existing edit owner retains all field/value validation.');
amend('08_credited_targets_admit.sqlinc',`  select array_agg(distinct (item->>'credited_work_event_id')::uuid order by (item->>'credited_work_event_id')::uuid)
    into v_ids from jsonb_array_elements(p_payload->'add_adjustments') item where item ? 'credited_work_event_id';`, `  select array_agg(distinct (item->>'credited_work_event_id')::uuid order by (item->>'credited_work_event_id')::uuid)
    into v_add_ids from jsonb_array_elements(p_payload->'add_adjustments') item where item ? 'credited_work_event_id';
  if exists(select 1 from public.invoice_lines removed
    cross join lateral jsonb_array_elements_text(p_payload->'remove_invoice_line_ids') requested(value)
    where removed.invoice_id=p_invoice_id and removed.id=case when pg_input_is_valid(requested.value,'uuid')
      then requested.value::uuid else null end and removed.meta_json ? 'credited_work_event_id'
      and private.weekly_source_invoice_adjustment_line_valid_v1(removed.id,p_invoice_id) is distinct from true) then
    raise exception 'WEEKLY_SOURCE_CREDITED_SHIFT_INVALID' using errcode='22023'; end if;
  select array_agg(distinct target order by target) into v_ids from (
    select unnest(v_add_ids) target
    union all
    select (removed.meta_json->>'credited_work_event_id')::uuid
    from public.invoice_lines removed
    cross join lateral jsonb_array_elements_text(p_payload->'remove_invoice_line_ids') requested(value)
    where removed.invoice_id=p_invoice_id and removed.id=case when pg_input_is_valid(requested.value,'uuid')
      then requested.value::uuid else null end and removed.meta_json ? 'credited_work_event_id'
  ) actual;`, 'Actual valid removed-line membership joins the same complete distinct Candidate/root lock census.');
const beforeEligibility=`      or v_context->'source_present' is distinct from 'false'::jsonb
      or not (v_context->'invoiced'='true'::jsonb or v_context->'paid'='true'::jsonb)
      or v_context->'current_pay_head_movement_id'='null'::jsonb
      or jsonb_array_length(v_context->'root_timesheet_ids')=0 then`;
amend('08_credited_targets_admit.sqlinc',beforeEligibility,`      or jsonb_array_length(v_context->'root_timesheet_ids')=0
      or (v_id=any(coalesce(v_add_ids,'{}'::uuid[])) and (
        v_context->'source_present' is distinct from 'false'::jsonb
        or not (v_context->'invoiced'='true'::jsonb or v_context->'paid'='true'::jsonb)
        or v_context->'current_pay_head_movement_id'='null'::jsonb)) then`,'Adding requires current Keep eligibility; deleting an actual historical credit does not borrow today Keep eligibility.');
const afterEligibility=`      or v_context->'source_present' is distinct from 'false'::jsonb
      or not (v_context->'invoiced'='true'::jsonb or v_context->'paid'='true'::jsonb) then`;
amend('08_credited_targets_admit.sqlinc',afterEligibility,`      or jsonb_array_length(v_context->'root_timesheet_ids')=0
      or (v_id=any(coalesce(v_add_ids,'{}'::uuid[])) and (
        v_context->'source_present' is distinct from 'false'::jsonb
        or not (v_context->'invoiced'='true'::jsonb or v_context->'paid'='true'::jsonb)
        or v_context->'current_pay_head_movement_id'='null'::jsonb)) then`,'Repeat exact role/identity/add eligibility after actual family/group/cycle waits.');

// All target fingerprints are checked before this request's own removals or
// additions. The per-item membership remains strict; do not depend on STABLE
// SPI snapshot visibility to hide self-authored changes to the credit census.
const applyName='10_complete_adjustments_apply.sqlinc';
const perItem=`        select saved into v_credit_context from jsonb_array_elements(v_credit_frame->'native_contexts') saved
          where (saved->>'work_event_id')::uuid=(item->>'credited_work_event_id')::uuid;
        if v_credit_context is null or v_credit_context->>'facts_fingerprint' is distinct from encode(
          private.weekly_source_kept_as_paid_facts_v1((item->>'credited_work_event_id')::uuid,'CREDITED'),'hex') then
          raise exception 'WEEKLY_SOURCE_CREDITED_SHIFT_FACTS_CHANGED' using errcode='40001'; end if;`;
amend(applyName,perItem,`        select saved into strict v_credit_context from jsonb_array_elements(v_credit_frame->'native_contexts') saved
          where (saved->>'work_event_id')::uuid=(item->>'credited_work_event_id')::uuid;`,'Require exact already admitted target per item, without comparing against this request own earlier writes.');
const preDml=`  if v_credit_frame is not null then
    for v_credit_context in select value from jsonb_array_elements(v_credit_frame->'native_contexts') loop
      if v_credit_context->>'facts_fingerprint' is distinct from encode(private.weekly_source_kept_as_paid_facts_v1(
        (v_credit_context->>'work_event_id')::uuid,'CREDITED'),'hex') then
        raise exception 'WEEKLY_SOURCE_CREDITED_SHIFT_FACTS_CHANGED' using errcode='40001'; end if;
    end loop;
  end if;
`;
amend(applyName,"  if p_payload ? 'remove_invoice_line_ids' then",preDml+"  if p_payload ? 'remove_invoice_line_ids' then",'Revalidate entire native census under existing locks before any DELETE/INSERT; native race proof remains required.');

// Native audit reservation also prevents TRUNCATE from silently erasing the
// retained typed history. Other native audit actions keep their existing owner.
amend('12_keep_ack_native_guard.sqlinc','v_context text; v_after jsonb:=new.after_json; v_request jsonb;','v_context text; v_after jsonb; v_request jsonb;','Do not dereference NEW for statement-level TRUNCATE.');
amend('12_keep_ack_native_guard.sqlinc',"begin\n  if tg_op='DELETE' then",`begin
  if tg_op='TRUNCATE' then
    if exists(select 1 from public.audit_events where action='WEEKLY_SOURCE_KEEP_AS_PAID_ACKNOWLEDGED') then
      raise exception 'WEEKLY_SOURCE_KEEP_AS_PAID_ACK_IMMUTABLE' using errcode='55000'; end if;
    return null;
  end if;
  if tg_op='DELETE' then`,'Protect actual typed history against TRUNCATE without blocking other audit operations.');
amend('12_keep_ack_native_guard.sqlinc',"  get diagnostics v_context=PG_CONTEXT;","  v_after:=new.after_json;\n  get diagnostics v_context=PG_CONTEXT;",'Only INSERT/UPDATE paths read NEW.');

// The original decision relation is the ONLY original-decision store. ACKs
// remain typed native audit rows. DDL/trigger creation is one-time, not an
// unrepeatable suffix hidden inside a function repeatable.
const schema=prior('forward/00_complete_one_time_kept_decisions.sqlinc');
const schemaAt=schema.indexOf('-- One explicitly planned'),guard=outputs['12_keep_ack_native_guard.sqlinc'],triggerAt=guard.indexOf('create trigger zzzz_');
outputs['12_keep_ack_native_guard.sqlinc']=guard.slice(0,triggerAt);
const{scanManagedSql}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs'))),{routineDefinitions}=await import(pathToFileURL(path.join(root,'scripts/bpay-next-final-definers.mjs')));
const guardChain=JSON.parse(blob(checkpoint,'docs/bpay-next/final-definer-closures.json')).chains.find(c=>c.signature==='private._weekly_source_immutable_record_guard_v1()');
const guardSource=blob(checkpoint,guardChain.finalPhysical).toString(),guardParsed=scanManagedSql(guardSource),guardOwners=guardParsed.statements.map(s=>guardParsed.source.slice(s.start,s.end).trim()).filter(s=>routineDefinitions(s,guardChain.finalPhysical).includes(guardChain.signature));
if(guardOwners.length!==1)throw Error('EXACT_EXISTING_IMMUTABLE_OWNER_REQUIRED');
const immutableDefinition=guardOwners[0],immutableBody=immutableDefinition.split('$function$')[1];
if(!immutableBody||!immutableDefinition.includes('security definer'))throw Error('IMMUTABLE_GUARD_BODY_REQUIRED');
outputs['00_original_decision_schema.sqlinc']=`-- Logical one-time contribution. Root allocates fresh real managed paths.
-- Schema trigger dependency bootstraps the exact complete current immutable
-- owner for NEW; existing UPGRADE body/owner/security drift refuses first.
lock table public.audit_events in access exclusive mode;
lock table public.weekly_work_events,public.invoice_lines,public.tms_users in share row exclusive mode;
do $kept_prerequisite$
begin
  if exists(select 1 from pg_proc p where p.oid=to_regprocedure('private._weekly_source_immutable_record_guard_v1()')
    and (replace(p.prosrc,E'\\r\\n',E'\\n') is distinct from $immutable_expected$${immutableBody.replaceAll('\r\n','\n')}$immutable_expected$
      or not p.prosecdef or p.prokind<>'f' or pg_get_userbyid(p.proowner) not in(current_user,'postgres'))) then
    raise exception 'WEEKLY_SOURCE_IMMUTABLE_GUARD_PREDECESSOR_CHANGED' using errcode='55000'; end if;
  if to_regclass('public.weekly_source_kept_as_paid_decisions') is not null then
    raise exception 'WEEKLY_SOURCE_KEPT_DECISION_STORE_ALREADY_EXISTS' using errcode='55000'; end if;
end;
$kept_prerequisite$;
${immutableDefinition}
alter function private._weekly_source_immutable_record_guard_v1() owner to postgres;
revoke all on function private._weekly_source_immutable_record_guard_v1() from public,anon,authenticated,service_role;
`+schema.slice(schemaAt)+`\ncreate unique index weekly_source_keep_ack_request_identity
on public.audit_events((after_json->>'idempotency_key'))
where action='WEEKLY_SOURCE_KEEP_AS_PAID_ACKNOWLEDGED';
create unique index weekly_source_keep_ack_event_sequence
on public.audit_events(object_id_text,((after_json->>'acknowledgement_sequence')::bigint))
where action='WEEKLY_SOURCE_KEEP_AS_PAID_ACKNOWLEDGED';
`;
outputs['13_one_time_audit_guards.sqlinc']=`-- Separate one-time schema dependency; exact complete repeatable guard is
-- included before its four native triggers. No business rows are rewritten.
lock table public.audit_events in access exclusive mode;
`+outputs['12_keep_ack_native_guard.sqlinc']+guard.slice(triggerAt)+`\ncreate trigger zzzz_weekly_source_keep_ack_truncate_guard
before truncate on public.audit_events for each statement
execute function private.weekly_source_keep_ack_native_guard_v1();
`;
const declarations=Object.entries(outputs).flatMap(([file,s])=>routineDefinitions(s,file).map(signature=>({signature,file}))),unique=[...new Set(declarations.map(d=>d.signature))];
outputs['99_exact_function_acl.sqlinc']=`-- Concrete owner/private/service closure for this contribution only.\n`+unique.map(signature=>`alter function ${signature} owner to postgres;\nrevoke all on function ${signature} from public,anon,authenticated,service_role;\n${signature.startsWith('public.')?`grant execute on function ${signature} to service_role;\n`:''}`).join('')+`revoke all on function public.invoice_apply_edits(uuid,jsonb,uuid) from authenticator,supabase_admin;\nnotify pgrst,'reload schema';\n`;

for(const[name,s]of Object.entries(outputs))write('forward/'+name,s);
// Every bounded current-owner contribution reverses to actual current bytes;
// review corrections reverse separately to the immutable old proposal.
for(const r of recipes){let final=outputs[path.basename(r.successorFile)];for(const h of [...newChanges].reverse().filter(h=>h.file===r.successorFile))final=splice(final,h.after,h.before);if(reverse(final.replace(/\n$/,''),r.hunks)!==fs.readFileSync(path.join(here,r.file),'utf8').replace(/\n$/,''))throw Error('COMBINED_CURRENT_REVERSE_REQUIRED');r.completeSuccessorFileSha256=sha(outputs[path.basename(r.successorFile)]);}
const functions=Object.values(outputs).join('\n').match(/create or replace function\s+[\w.]+/gi)??[];
write('CURRENT_OWNER_EDITS.json',JSON.stringify({version:'CURRENT_KEPT_REKEY_COMPLETE_OWNER_RECIPE_V1',checkpoint,old,recipes,newChanges,authorityPrefixSha256:sha(authorityPrefix),authorityPrefixByteEquality:admission.includes(authorityPrefix),fullCurrentOwnerReversals:recipes.length,native:'NOT_RUN'},null,2)+'\n');
write('IMMUTABLE_GUARD_PREDECESSOR.json',JSON.stringify({checkpoint,...guardChain,sourceSha256:sha(guardSource),completeStatementSha256:sha(immutableDefinition),bodySha256:sha(immutableBody),oneTimeReason:'Exact existing complete native immutable owner is required by the newly planned schema triggers before repeatables in canonical NEW. Existing body/owner/security drift refuses.',native:'NOT_RUN'},null,2)+'\n');
write('CLASSIFICATION_DELTA.json',JSON.stringify({checkpoint,existingCurrentOwners:recipes.map(r=>({signature:r.signature,predecessorRoot:r.finalRoot,predecessorPhysical:r.finalPhysical,predecessorSha256:r.completeCurrentStatementSha256,successorFile:r.successorFile})),declarations,uniqueFunctions:unique.map(signature=>({signature,security:'DEFINER',owner:'postgres mapped by actual managed compiler',serviceExecute:signature.startsWith('public.'),newDeclaration:!recipes.some(r=>r.signature===signature)&&signature!==guardChain.signature})),originalDecisionRelation:'public.weekly_source_kept_as_paid_decisions',newAckRelation:false,auditHistoryOwner:'public.audit_events',integration:'Root must compose exact literal private/service classifiers, final graph/registry and managed schema/contract coupling from these deltas, preserving every unrelated owner. These proposal paths are not installed manifests.',native:'NOT_RUN'},null,2)+'\n');
write('BUILD_RESULT.json',JSON.stringify({checkpoint,old,completeCurrentOwners:recipes.length,outputs:Object.entries(outputs).map(([p,s])=>({path:'forward/'+p,sha256:sha(s),bytes:Buffer.byteLength(s)})),declarations:functions.length,rootEdits:0,dbConnections:0,native:'NOT_RUN',contractSeal:'NOT_RUN',managedMount:'NOT_RUN'},null,2)+'\n');
console.log(JSON.stringify({completeCurrentOwners:recipes.length,outputs:Object.keys(outputs).length,wholeCurrentReversals:recipes.length,native:'NOT_RUN'}));

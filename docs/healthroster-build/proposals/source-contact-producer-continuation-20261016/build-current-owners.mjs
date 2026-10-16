import {execFileSync} from 'node:child_process';
import {readFileSync,writeFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
const base=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(base,'../../../..');
const commit='90ffcf3f40644280fcdb5b791a62a86613896d6e',checkpoint='18f844f1c01c7d1cf293bc4e92a2ab7e0fb762eb';
const sha=s=>createHash('sha256').update(s).digest('hex'),inputs=[];
function source(file,ref=commit){const text=execFileSync('git',['show',`${ref}:${file}`],{cwd:root,encoding:'utf8',maxBuffer:8e6}).replace(/\r\n/g,'\n');inputs.push({source_commit:ref,path:file,sha256_normalized_lf:sha(text)});return text;}
function extract(text,name){const start=text.indexOf(`create or replace function ${name}(`);if(start<0)throw Error(name);const end=text.indexOf('$function$;',text.indexOf('as $function$',start))+12;return text.slice(start,end)+'\n';}
function once(text,old,next){if(text.split(old).length!==2)throw Error(`Expected unique ${old.slice(0,100)}`);return text.replace(old,next);}
function write(name,text){writeFileSync(path.join(base,'forward',name),text.trimEnd()+'\n');}
const prior='docs/healthroster-build/proposals/source-contact-recomposition-native-20261016/forward/';
let manager=extract(source(prior+'01_current_native_members.sqlinc',checkpoint),'private.weekly_contact_source_manager_members_v1');
manager=once(manager,' select * into strict v_intent from public.weekly_message_intents where id=p_intent_id;',` select * into strict v_intent from public.weekly_message_intents where id=p_intent_id;
 if exists(select 1 from public.weekly_contact_source_intent_captures where message_intent_id=v_intent.id) then
  if not private.weekly_contact_source_capture_current_v1(v_intent.id,v_at,true) then
   raise exception 'WEEKLY_SOURCE_RENDER_MEMBERSHIP_STALE' using errcode='40001';end if;
  select manifest_json->'manager_members' into strict result from public.weekly_contact_source_intent_captures where message_intent_id=v_intent.id;
  return result;
 end if;`);
write('20_native_manager_members.sqlinc',manager);
let input=source(prior+'02_native_render_input.sqlinc',checkpoint);
input=once(input,'    perform private.weekly_contact_source_candidate_render_assert_v1(v_candidate_generation.id);',`    if exists(select 1 from public.weekly_contact_source_intent_captures where message_intent_id=v_intent.id) then
      if not private.weekly_contact_source_capture_current_v1(v_intent.id,clock_timestamp(),true) then
        raise exception 'WEEKLY_SOURCE_RENDER_MEMBERSHIP_STALE' using errcode='40001';end if;
      select membership_hash into strict v_membership_hash from public.weekly_contact_source_intent_captures where message_intent_id=v_intent.id;
    else
      perform private.weekly_contact_source_candidate_render_assert_v1(v_candidate_generation.id);
      v_membership_hash:=v_candidate_generation.membership_hash;
    end if;`);
input=once(input,"'membership_hash',pg_catalog.encode(v_candidate_generation.membership_hash,'hex')","'membership_hash',pg_catalog.encode(v_membership_hash,'hex')");
write('21_native_render_input.sqlinc',input);
let stage=source(prior+'04_native_render_stage.sqlinc',checkpoint);
stage=once(stage,`    perform private.weekly_contact_source_candidate_render_assert_v1(v_candidate_generation.id);
    v_membership_hash:=v_candidate_generation.membership_hash;`,`    if exists(select 1 from public.weekly_contact_source_intent_captures where message_intent_id=v_intent.id) then
      if not private.weekly_contact_source_capture_current_v1(v_intent.id,clock_timestamp(),true) then
        raise exception 'WEEKLY_SOURCE_RENDER_MEMBERSHIP_STALE' using errcode='40001';end if;
      select membership_hash into strict v_membership_hash from public.weekly_contact_source_intent_captures where message_intent_id=v_intent.id;
    else
      perform private.weekly_contact_source_candidate_render_assert_v1(v_candidate_generation.id);
      v_membership_hash:=v_candidate_generation.membership_hash;
    end if;`);
write('22_native_render_stage.sqlinc',stage);
let mapper=extract(source('supabase/repeatable/includes/weekly_contact_source_memberships/010_native_logical_members.sqlinc'),'private.weekly_contact_source_logical_members_v1');
mapper=once(mapper,' select * into strict r from public.weekly_message_renders where id=c.message_render_id;',` select * into strict r from public.weekly_message_renders where id=c.message_render_id;
 if exists(select 1 from public.weekly_contact_source_intent_captures where message_intent_id=i.id) then
  return private.weekly_contact_source_capture_logical_members_v1(c.id);
 end if;`);
write('23_native_logical_members.sqlinc',mapper);
let budget=extract(source('supabase/repeatable/includes/weekly_contact_source_memberships/020_native_budget.sqlinc'),'private.weekly_contact_source_budget_v1');
budget=once(budget," for member in select value from jsonb_array_elements(current_members->'members') loop",` for member in select value from jsonb_array_elements(current_members->'members') loop
  if exists(select 1 from public.weekly_contact_source_ordinal_owners owner where owner.logical_contact_key=decode(member->>'logical_contact_key','hex')
   and owner.send_ordinal=(member->>'send_ordinal')::integer and owner.message_intent_id<>c.message_intent_id) then
   return jsonb_build_object('allowed',false,'reason','ORIGINAL_CAPTURE_OWNER','logical_contact_key',member->'logical_contact_key');end if;`);
write('24_native_budget.sqlinc',budget);
let membership=extract(source('supabase/repeatable/16102026_0214_weekly_contact_source_admission_v1.sql'),'private.weekly_contact_source_membership_v1');
membership=once(membership," v_current:=v_target.state='LEASED' and v_intent.state='RENDERED' and v_render.state='CURRENT';",` if exists(select 1 from public.weekly_contact_source_intent_captures where message_intent_id=v_intent.id) then
  return private.weekly_contact_source_capture_dispatch_membership_v1(v_target.id,p_agency,p_environment);
 end if;
 v_current:=v_target.state='LEASED' and v_intent.state='RENDERED' and v_render.state='CURRENT';`);
write('25_native_current_membership.sqlinc',membership);
let push=extract(source('supabase/repeatable/15102026_0803_candidate_push_dispatch_read_v1.sql'),'private.weekly_contact_push_dispatch_context_v1');
const start=push.indexOf(' select coalesce(jsonb_agg('),end=push.indexOf(' if jsonb_array_length(v_pairs)>500',start);
if(start<0||end<0)throw Error('push exact scope projection');
push=push.slice(0,start)+` if exists(select 1 from public.weekly_contact_source_intent_captures where message_intent_id=v_intent.id) then
  v_pairs:=private.weekly_contact_source_capture_pairs_v1(v_intent.id);
 else
`+push.slice(start,end)+' end if;\n'+push.slice(end);
push=once(push,' if v_current then\n',` if v_current and exists(select 1 from public.weekly_contact_source_intent_captures where message_intent_id=v_intent.id) then
  v_current:=private.weekly_contact_source_capture_current_v1(v_intent.id,clock_timestamp(),false);
 end if;
 if v_current then
`);
write('26_native_push_context.sqlinc',push);
let managerIntent=extract(source('supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql'),'private.weekly_source_query_manager_intent_v1');
managerIntent=once(managerIntent,"'MANAGER_MANUAL_SELECTED','MANAGER_MANUAL_RESEND'","'MANAGER_MANUAL_SELECTED','MANAGER_MANUAL_RESEND','MANAGER_AUTOMATIC_FOLLOWUP'");
write('27_native_manager_intent.sqlinc',managerIntent);
membership=once(membership,"'MANAGER_MANUAL_SELECTED','MANAGER_MANUAL_RESEND')","'MANAGER_MANUAL_SELECTED','MANAGER_MANUAL_RESEND','MANAGER_AUTOMATIC_FOLLOWUP')");
write('25_native_current_membership.sqlinc',membership);
writeFileSync(path.join(base,'NATIVE_OWNER_INPUTS.json'),JSON.stringify({version:'CONTACT_CAPTURE_NATIVE_OWNER_INPUTS_V1',inputs,
 qualification:'Exact finite complete source successors only; native PostgreSQL/runtime acceptance NOT_RUN'},null,2)+'\n');
console.log(JSON.stringify({complete_native_owners:9,inputs:inputs.length,status:'SOURCE_ONLY_NATIVE_NOT_RUN'}));

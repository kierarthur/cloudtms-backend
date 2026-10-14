// Separate construction successor; no DB, secrets, subprocess or network.
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import crypto from 'node:crypto';
const dir=path.dirname(fileURLToPath(import.meta.url));
const before=fs.readFileSync(path.join(dir,'01_NATIVE_HELPER_FIXTURE.sql'),'utf8');
const sha=s=>crypto.createHash('sha256').update(s).digest('hex');
if(sha(before)!=='af81c4452a29c8b4c14cc7d74a97be883dd030ad7eb10ac526d332ab189c8c4f')throw Error('INITIAL_FIXTURE_DRIFT');
const hunks=[];
let after=before;
const replace=(name,old,value)=>{if(after.split(old).length!==2)throw Error('FIXTURE_HUNK_NOT_UNIQUE '+name);hunks.push({name,before:old,after:value});after=after.replace(old,value);};
replace('bind intended successor ABI, retain initial draft',
 '-- Unmounted native helper first-use construction.',
 '-- Separate corrected first-use construction for the proposed all-owner\n-- alias census. Requires the independently reviewed OWNER_CONFLICT/55000 ABI;\n-- the earlier draft remains unchanged and must not be executed.\n-- Unmounted native helper first-use construction.');
replace('vacancy for both actual extra identities',
 "and not exists(select 1 from public.clients where cli_ref='CLI-991714')\n  and not exists(select 1 from public.candidates where tms_ref='CCR-991714')",
 "and not exists(select 1 from public.clients where cli_ref in('CLI-991714','CLI-991715'))\n  and not exists(select 1 from public.candidates where tms_ref in('CCR-991714','CCR-991715'))");
replace('actual second Candidate/client/Contract admitted before cases',
 "create function pg_temp.hr_alias_seed_upload(p_id uuid,p_cycle uuid,p_profile text)",
 `insert into public.clients(id,cli_ref,name) values('f7120000-0000-4000-8000-000000000040','CLI-991715','Synthetic other alias client');
insert into public.candidates(id,tms_ref,display_name) values('f7120000-0000-4000-8000-000000000041','CCR-991715','Synthetic other alias candidate');
insert into public.contracts(id,client_id,candidate_id,start_date,end_date,pay_method_snapshot,rates_json)
values('f7120000-0000-4000-8000-000000000042','f7120000-0000-4000-8000-000000000040',
 'f7120000-0000-4000-8000-000000000041','2026-01-01','2026-12-31','PAYE',
 '{"paye_day":10,"paye_night":10,"paye_sat":10,"paye_sun":10,"paye_bh":10,"charge_day":20,"charge_night":20,"charge_sat":20,"charge_sun":20,"charge_bh":20}');
insert into public.weekly_source_group_clients(source_group_id,client_id,valid_from,created_by_user_id)
values('f7120000-0000-4000-8000-000000000005','f7120000-0000-4000-8000-000000000040','2026-01-01','f7120000-0000-4000-8000-000000000001');

create function pg_temp.hr_alias_seed_upload(p_id uuid,p_cycle uuid,p_profile text)`);
replace('extra historical alias upload',
 "select pg_temp.hr_alias_seed_upload('f7120000-0000-4000-8000-000000000013','f7120000-0000-4000-8000-000000000008','HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1');",
 "select pg_temp.hr_alias_seed_upload('f7120000-0000-4000-8000-000000000013','f7120000-0000-4000-8000-000000000008','HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1');\nselect pg_temp.hr_alias_seed_upload('f7120000-0000-4000-8000-000000000030','f7120000-0000-4000-8000-000000000008','HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1');");
replace('foreign Candidate is a refusal, not a new key',
 " perform pg_temp.hr_alias_assert(private.weekly_source_known_source_key_v1('f7120000-0000-4000-8000-000000000024','f7120000-0000-4000-8000-000000000089',own_client) is null,'HR_ALIAS_WRONG_CANDIDATE');",
 ` begin
  perform private.weekly_source_known_source_key_v1('f7120000-0000-4000-8000-000000000024','f7120000-0000-4000-8000-000000000041',own_client);
  raise exception 'HR_ALIAS_FOREIGN_CANDIDATE_NOT_REFUSED' using errcode='P0001';
 exception when sqlstate '55000' then
  if SQLERRM is distinct from 'WEEKLY_SOURCE_SOURCE_KEY_ALIAS_OWNER_CONFLICT' then raise;end if;
 end;
 begin
  perform private.weekly_source_known_source_key_v1('f7120000-0000-4000-8000-000000000024',own_candidate,'f7120000-0000-4000-8000-000000000040');
  raise exception 'HR_ALIAS_FOREIGN_CLIENT_NOT_REFUSED' using errcode='P0001';
 exception when sqlstate '55000' then
  if SQLERRM is distinct from 'WEEKLY_SOURCE_SOURCE_KEY_ALIAS_OWNER_CONFLICT' then raise;end if;
 end;`);
replace('prove hidden mixed ownership before canonical precedence',
 "-- Genuine current canonical-key precedence is tested",
 `-- A second genuine typed fixture B link is owned by a different actual
-- Candidate/client. Both owners must survive the all-owner census; no local
-- subset is allowed to hide the foreign owner and return a UUID.
insert into public.weekly_work_events(id,candidate_id,client_id,work_date,identity_kind,profile_external_key,durable_identity_hash,first_source_group_id,source_format_profile_id)
select 'f7120000-0000-4000-8000-000000000043','f7120000-0000-4000-8000-000000000041','f7120000-0000-4000-8000-000000000040',
 '2026-09-15','PROFILE_EXTERNAL_KEY','D',decode(repeat('7a',32),'hex'),'f7120000-0000-4000-8000-000000000005',id
from public.weekly_source_format_profiles where profile_code='HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1' and active;
insert into public.weekly_source_upload_rows(id,upload_id,source_row_ordinal,external_source_key,source_candidate_identity,
 source_client_identity,work_date,start_at_local,end_at_local,break_minutes,actual_net_minutes,row_finalisation_state,finalised_by,normalised_row_hash)
values('f7120000-0000-4000-8000-000000000044','f7120000-0000-4000-8000-000000000030',1,'B','Synthetic other alias candidate',
 'Synthetic other alias client','2026-09-15','2026-09-15 09:00','2026-09-15 17:00',30,450,'SOURCE_WORKED','Synthetic fixture',decode(repeat('7b',32),'hex'));
insert into public.weekly_source_row_resolutions(id,upload_row_id,generation,candidate_id,client_id,contract_id,work_event_id,
 paid_minutes,mapping_state,contract_selection_method,work_event_match_kind,work_event_match_fingerprint,
 qualification_profile_fingerprint,qualifying_contract_count,qualifying_contract_set_hash,source_row_fingerprint)
values('f7120000-0000-4000-8000-000000000045','f7120000-0000-4000-8000-000000000044',1,
 'f7120000-0000-4000-8000-000000000041','f7120000-0000-4000-8000-000000000040','f7120000-0000-4000-8000-000000000042',
 'f7120000-0000-4000-8000-000000000043',450,'RESOLVED','DURABLE_LINEAGE','EXACT_DURABLE_LINEAGE',decode(repeat('75',32),'hex'),
 decode(repeat('76',32),'hex'),1,decode(repeat('77',32),'hex'),decode(repeat('7b',32),'hex'));
insert into public.weekly_work_event_source_links(id,work_event_id,upload_row_id,row_resolution_id,link_kind,link_hash)
values('f7120000-0000-4000-8000-000000000046','f7120000-0000-4000-8000-000000000043','f7120000-0000-4000-8000-000000000044',
 'f7120000-0000-4000-8000-000000000045','POSITIVE_SOURCE',private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_WORK_EVENT_LINK_V1',
 jsonb_build_object('work_event_id','f7120000-0000-4000-8000-000000000043'::uuid,'upload_row_id','f7120000-0000-4000-8000-000000000044'::uuid,
 'resolution_id','f7120000-0000-4000-8000-000000000045'::uuid,'link_kind','POSITIVE_SOURCE')));
do $native_mixed_alias$
begin
 begin
  perform private.weekly_source_known_source_key_v1('f7120000-0000-4000-8000-000000000024',
   'f7120000-0000-4000-8000-000000000003','f7120000-0000-4000-8000-000000000002');
  raise exception 'HR_ALIAS_MIXED_OWNER_NOT_REFUSED' using errcode='P0001';
 exception when sqlstate '55000' then
  if SQLERRM is distinct from 'WEEKLY_SOURCE_SOURCE_KEY_ALIAS_AMBIGUOUS' then raise;end if;
 end;
end;
$native_mixed_alias$;

-- Genuine current canonical-key precedence is tested`);
let reversed=after;for(const h of [...hunks].reverse()){if(reversed.split(h.after).length!==2)throw Error('REVERSE_HUNK_NOT_UNIQUE');reversed=reversed.replace(h.after,h.before);}
if(reversed!==before)throw Error('WHOLE_INITIAL_FIXTURE_REVERSE_FAILED');
const name='02_NATIVE_HELPER_FIXTURE_OWNER_CONFLICT_FORWARD.sql';
fs.writeFileSync(path.join(dir,name),after,{flag:'wx'});
fs.writeFileSync(path.join(dir,'OWNER_CONFLICT_FIXTURE_SOURCE_REVERSE.json'),JSON.stringify({status:'SOURCE_CONSTRUCTION_WAITING_REVIEWED_SUCCESSOR_AND_NATIVE',before:{file:'01_NATIVE_HELPER_FIXTURE.sql',sha256:sha(before)},after:{file:name,sha256:sha(after)},hunks,wholeReverseVerified:true,sqlExecuted:false,expectedOwnerConflictCode:'WEEKLY_SOURCE_SOURCE_KEY_ALIAS_OWNER_CONFLICT',expectedSqlstate:'55000',ownerSuccessorSourceFrozen:false},null,2)+'\n',{flag:'wx'});
console.log(JSON.stringify({file:name,sha256:sha(after),uniqueHunks:hunks.length,wholeReverseVerified:true,nativeExecuted:false}));

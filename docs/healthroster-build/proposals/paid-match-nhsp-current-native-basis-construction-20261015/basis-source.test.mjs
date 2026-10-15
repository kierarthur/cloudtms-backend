import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import test from 'node:test';import {createHash} from 'node:crypto';import {execFileSync,spawnSync} from 'node:child_process';import {fileURLToPath} from 'node:url';import {functionEvents,sqlTokens} from '../a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..'),read=f=>fs.readFileSync(path.join(dir,f),'utf8'),sha=b=>createHash('sha256').update(b).digest('hex'),p=JSON.parse(read('SOURCE_ADDITION_AND_INPUT_PINS.json')),source=read(p.forward);
test('new owner-private addition has exact saved hash and no actual prior declaration',()=>{
 assert.equal(sha(source),p.forward_sha256);const events=functionEvents(source).events.filter(x=>x.kind==='CREATE');assert.deepEqual(events.map(x=>x.id),[p.new_private_signature]);
 const g=spawnSync('git',['grep','-l','weekly_source_nhsp_paid_overlap_basis_v1',p.native,'--','supabase'],{cwd:root,encoding:'utf8'});assert.equal(g.status,1);assert.equal(g.stdout,'');assert.equal(g.stderr,'');
 assert.ok(source.includes('from public,anon,authenticated,service_role;'));assert.doesNotMatch(source,/grant execute|create table|alter table/);
});
test('all eight exact native dependencies match full or literal definition Git bytes',()=>{
 for(const d of p.inputs){const b=fs.readFileSync(path.join(dir,d.saved));assert.equal(b.length,d.bytes);assert.equal(sha(b),d.sha256);const raw=execFileSync('git',['show',d.commit+':'+d.file],{cwd:root,maxBuffer:8e6});
 if(d.signature){assert.equal(raw.length,d.source_bytes);assert.equal(sha(raw),d.source_sha256);const e=functionEvents(raw.toString()).events.find(x=>x.id===d.signature);assert.ok(e);assert.equal(sha(e.definition),d.definition_sha256);assert.equal(sha(e.body),d.body_sha256);assert.equal(b.toString(),e.definition+'\n');}else assert.deepEqual(b,raw);}
});
test('native current cache schema contains every directly referenced scope origin and event field',()=>{
 const schema=read('input/04102026_0470_current_schema.sql');
 for(const field of ['root_timesheet_id','family_booking_id','source_group_id','candidate_id','client_id','contract_id','week_ending_date','origin_kind','origin_id','movement_id','scope_id','work_event_id','final_revision_id','row_resolution_id','source_hash','contribution','active'])assert.match(schema,new RegExp('\\b'+field+'\\b'));
 for(const table of ['bpay_next_source_current_scopes','bpay_next_source_current_events','bpay_next_source_current_origins'])assert.ok(schema.includes('private.'+table));
 assert.doesNotMatch(source,/v_root\.(?:candidate_id|client_id)/);assert.ok(source.includes('(v_contract.candidate_id,v_contract.client_id)'));
});
test('actual viewer service WP-PP and dated cycle/root permissions precede native reads',()=>{
 assert.ok(source.indexOf('weekly_source_query_require_service_v1')<source.indexOf('v_payment:='));
 assert.ok(source.indexOf('weekly_source_pay_actor_allowed_v1(p_actor_user_id)')<source.indexOf('v_payment:='));
 assert.ok(source.includes("v_cycle.source_group_id,v_event.client_id,v_cycle.finalisation_week_ending"));
 assert.ok(source.includes('v_event.first_source_group_id,v_contract.client_id,v_root.week_ending_date'));
 assert.ok(source.includes("'VIEW_PAY_WORK'"));assert.doesNotMatch(source,/VIEW_SOURCE_PROGRESS|v_choice\.actor_user_id|actor_user_id\s*:=/);
});
test('pure native current event observation/revision owners are reused without mutations locks refresh or own selection',()=>{
 for(const file of ['input/current_event.sqlinc','input/current_observation.sqlinc','input/current_revision.sqlinc']){const s=read(file);assert.match(s,/stable security definer/i);assert.doesNotMatch(s,/for update|for share|insert into|update private\.|delete from/i);}
 assert.ok(source.includes('v_native:=private.bpay_next_source_current_event_v1(v_root_id,v_event.id);'));
 assert.doesNotMatch(source,/current_scope_v1\(|current_refresh_v1\(|source_nhsp_head_v1\(|capture_|source_present\s*:=|insert into|update private\.|delete from|for update|for share/i);
 assert.ok(read('input/current_event.sqlinc').includes('v_observation:=private.bpay_next_source_current_observation_v1(p_root)'));
});
test('one real classifier and root union stay exact with NHSP movement provenance kept separate',()=>{
 assert.equal((source.match(/weekly_source_work_event_payment_facts_v1\(/g)||[]).length,1);assert.ok(source.includes("to_jsonb(v_roots) is distinct from v_payment->'root_timesheet_ids'"));
 assert.ok(source.includes("'native_payment_facts',v_payment"));assert.doesNotMatch(source,/v_payment\s*:=\s*jsonb|v_payment\s*:=.*\|\||paid\s*:=|round\(|sum\(|amount_ex_vat.*[+*\/]/i);
 assert.ok(source.includes("'no_reversal_proved',null,'paid_writer_permission',null"));assert.doesNotMatch(source,/new_snapshot_line_id|previous_snapshot_line_id|snapshot_line_id/);
});
test('uncaptured position remains unavailable while native absence requires actual event plus observation',()=>{
 const s=source.slice(source.indexOf('    if v_scope_count=0'),source.indexOf('    if not exists(select 1 from private.bpay_next_source_current_scopes'));
 assert.ok(s.includes('private.bpay_next_source_current_events'));assert.ok(s.includes("'state','UNAVAILABLE'"));assert.ok(s.includes('v_complete:=false'));assert.doesNotMatch(s,/false,'|source_present.*false|paid.*false/);
 assert.ok(source.includes("v_native->'source_observed' is distinct from 'true'::jsonb"));assert.ok(source.includes("v_native->>'source_kind' is distinct from 'NHSP'"));
 assert.ok(source.includes("elsif v_native->'movement_id' is distinct from 'null'::jsonb"));
});
test('real final report movement/resolution/economic/lineage and origin hash are point-bound',()=>{
 for(const s of ["v_movement.source_profile_kind<>'NHSP_TRUST_BACKING_REPORT'","v_movement.pay_effect<>'NORMAL'","v_movement.source_line_kind<>'NHSP_PHYSICAL_POSITIVE'","v_resolution.mapping_state<>'RESOLVED'","lineage.family_booking_id=v_root.booking_id","economic.id=(v_native->>'economic_snapshot_id')::uuid","origin.origin_kind='MOVEMENT'","origin.source_hash=v_movement.movement_economic_hash and origin.contribution=1"]){assert.ok(source.includes(s),s);}
 assert.ok(source.includes("profile.id=v_upload.source_format_profile_id and profile.profile_code='NHSP_FINAL_BACKING_V1'"));
 assert.doesNotMatch(source,/v_event\.source_format_profile_id\s*=|event\.identity_kind\s*=|external_source_key|profile_external_key/);
});
test('root Client/Candidate currentness/Contract/date/raw booking and native scope are not erased',()=>{
 for(const s of ['v_root.revoked_at is not null','v_root.archived_at_utc is not null',"v_root.sheet_scope<>'WEEKLY'",'v_root.contract_id','membership.source_group_id=v_event.first_source_group_id','scope.family_booking_id=v_root.booking_id','scope.contract_id=v_contract.id','v_event.work_date not between v_contract.start_date'])assert.ok(source.includes(s));
 assert.ok(source.includes("using errcode='42501'"));assert.ok(source.includes("using errcode='55000'"));assert.doesNotMatch(source,/exception when|when others|upload.*client_id.*coalesce/i);
});
test('closed top-level DTO keys stay identical across inactive and actual-current branches with unknown native fields',()=>{
 const tokens=sqlTokens(functionEvents(source).events.find(e=>e.kind==='CREATE').body),keys=[];
 for(let i=0;i<tokens.length;i++){if(tokens[i].value==='v_result'&&tokens[i+1]?.value===':'&&tokens[i+2]?.value==='='&&tokens[i+3]?.value==='jsonb_build_object'){let depth=0,part=[];for(let j=i+4;j<tokens.length;j++){if(tokens[j].value==='(')depth++;if(tokens[j].value===')'){depth--;if(depth===0)break;}if(depth===1&&tokens[j].kind==='string'&&(tokens[j-1]?.value==='('||tokens[j-1]?.value===','))part.push(tokens[j].value);}keys.push(part.filter(k=>!['UNAVAILABLE','NEXT_NOT_ACTIVE'].includes(k)));}}
 assert.equal(keys.length,2);assert.deepEqual([...new Set(keys[0])].sort(),[...new Set(keys[1])].sort());
 assert.ok(source.includes("'root_timesheet_ids',null"));assert.ok(source.includes("'native_current_rows',null,'native_payment_facts',null"));
 assert.equal((source.match(/'WEEKLY_SOURCE_NHSP_PAID_OVERLAP_BASIS_INPUT_V1',v_result/g)||[]).length,2);
});
test('finite root/byte bounds refuse without truncating a supposedly complete certificate',()=>{
 assert.ok(source.includes('cardinality(v_roots)>200'));assert.ok(source.includes('octet_length(v_result::text)>524288'));assert.ok(source.includes("using errcode='54000'"));
 assert.doesNotMatch(source,/limit [0-9]|jsonb_array_elements.*limit|rows\s*:=.*slice/);
});
test('native financial UNKNOWN and unfinished no-reversal/writer permission remain explicit input fields',()=>{
 assert.equal(p.no_reversal_proved,null);assert.equal(p.paid_writer_permission,null);assert.equal(p.NHSP_writer_pipeline_complete,false);
 const facts=read('input/current_payment_facts.sqlinc');assert.ok(facts.includes("'paid_quantity_known',false,'withheld_amount_known',false"));
 assert.ok(source.includes("'state',case when v_complete then 'NATIVE_POSITION_INPUTS' else 'UNAVAILABLE' end"));
});

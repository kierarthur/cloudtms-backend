import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import path from 'node:path';import {fileURLToPath,pathToFileURL} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url));const read=p=>fs.readFileSync(path.join(here,p),'utf8').replaceAll('\r\n','\n');
const helper=read('forward/weekly-source-native-fixture-participants.mjs').replace("'./weekly-source-protected-release-qualification.mjs'",JSON.stringify(pathToFileURL(path.join(here,'reference/weekly-source-protected-release-qualification.mjs')).href));
const {buildNativeFixtureParticipants:build,buildNativeFixtureSourceMetadata:buildSource,createNativeFixtureParticipants:create,
 nativeParticipantSchema,nativeSourceSchema}=await import('data:text/javascript;base64,'+Buffer.from(helper).toString('base64'));
const uid=n=>'fa510000-0000-4000-8000-'+String(n).padStart(12,'0');
// Pure synthetic DTO vectors, never an installed release/native authority.
const context={environment:'TEST',expectedDatabase:'cloudtms_test_clone',mode:'UPGRADE',releaseId:'synthetic-unit-only',gitCommit:'a'.repeat(40),expectedHash:'b'.repeat(64)};
const input=()=>({schema_version:nativeParticipantSchema,authorization_id:uid(1),purpose:'NATIVE_FIXTURE_PARTICIPANTS',setup_actor_user_id:uid(2),
 expires_at_utc:'2026-10-16T23:59:00Z',reason:'Pure source test only',participants:{clients:[{id:uid(10),name:'Synthetic fixture'}],candidates:[{id:uid(20),first_name:'Synthetic',last_name:'Worker'}]}});
const source=()=>({schema_version:nativeSourceSchema,authorization_id:uid(3),purpose:'NATIVE_FIXTURE_SOURCE_METADATA',setup_actor_user_id:uid(2),
 expires_at_utc:'2026-10-16T23:59:00Z',reason:'Pure metadata test only',metadata:{agency_id:uid(4),source_group_id:uid(50),source_cycle_id:uid(51),
 membership_id:uid(52),policy_id:uid(53),client_id:uid(10),group_code:'WSQ_TEST_SYNTHETIC_UNIT',display_name:'Synthetic metadata',week_ending:'2026-10-11',
 cutoff_weekday:'1',cutoff_local_time:'09:00',contract_refs:[{contract_id:uid(60),row_sha256:'c'.repeat(64)}]}});
test('deterministic closed plan hash binds exact identity names and installed source',()=>{
 const a=build(context,input());assert.deepEqual(a,build(context,input()));
 for(const mutate of [i=>i.participants.clients[0].name+='x',i=>i.participants.candidates[0].id=uid(21),i=>i.setup_actor_user_id=uid(5)]){
  const i=input();mutate(i);assert.notEqual(a.authorization_sha256,build(context,i).authorization_sha256);
 }
 assert.notEqual(a.authorization_sha256,build({...context,gitCommit:'c'.repeat(40)},input()).authorization_sha256);
});
for(const [name,mutate]of [
 ['caller reference',i=>i.participants.clients[0].cli_ref='CLI-91301301'],['Candidate reference',i=>i.participants.candidates[0].tms_ref='CCR-91301301'],
 ['job title hook',i=>i.participants.candidates[0].job_titles=[uid(77)]],['alias assignment hook',i=>i.participants.candidates[0].key_norm='existing-worker'],
 ['bank authority',i=>i.participants.candidates[0].account_number='12345678'],['contact',i=>i.participants.candidates[0].email='worker@example.test'],
 ['financials',i=>i.financials={}],['fake creation clock',i=>i.participants.candidates[0].created_at='2026-10-01T00:00:00Z'],
 ['extra ID',i=>i.participants.clients.push({...i.participants.clients[0]})],['cross-table reused UUID',i=>i.participants.candidates[0].id=i.participants.clients[0].id],
 ['unsorted IDs',i=>i.participants.candidates.unshift({...i.participants.candidates[0],id:uid(21)})],['absent candidates',i=>i.participants.candidates=[]],
 ['oversized names',i=>i.participants.candidates[0].first_name='x'.repeat(81)],['unknown purpose',i=>i.purpose='AUTOMATIC_JOURNEY'],
 ['caller qualification',i=>i.qualified=true],['false timestamp',i=>i.expires_at_utc='not-a-time']])
 test('participant plan rejects '+name,()=>{const i=input();mutate(i);assert.throws(()=>build(context,i));});
for(const [name,value]of [['LIVE',{environment:'LIVE'}],['foreign database',{expectedDatabase:'uofvkfi5'}],['foreign mode',{mode:'NEW'}],['missing source',{gitCommit:''}]])
 test('reject '+name+' context',()=>assert.throws(()=>build({...context,...value},input())));
test('past metadata plan is deterministic and seals exact native Contract row hash',()=>{
 const a=buildSource(context,source());assert.deepEqual(a,buildSource(context,source()));const i=source();i.metadata.contract_refs[0].row_sha256='d'.repeat(64);
 assert.notEqual(a.authorization_sha256,buildSource(context,i).authorization_sha256);
});
for(const [name,mutate]of [
 ['caller cutoff timestamp',i=>i.metadata.cutoff_at_utc='2026-10-12T08:00:00Z'],['created clock',i=>i.metadata.created_at_utc='2026-10-01T00:00:00Z'],
 ['fake upload',i=>i.metadata.current_complete_upload_id=uid(80)],['feature enablement',i=>i.metadata.feature_enabled=true],
 ['financial row',i=>i.metadata.rates_json={}],['future state claim',i=>i.metadata.state='FINALISED'],['non-Sunday',i=>i.metadata.week_ending='2026-10-12'],
 ['invalid day',i=>i.metadata.week_ending='2026-02-31'],['invalid clock',i=>i.metadata.cutoff_local_time='24:00'],['numeric weekday',i=>i.metadata.cutoff_weekday=1],
 ['nonfixture code',i=>i.metadata.group_code='ORDINARY_GROUP'],['duplicate UUID',i=>i.metadata.source_cycle_id=i.metadata.source_group_id],
 ['altered Contract witness',i=>i.metadata.contract_refs[0].row_sha256='x'],['duplicate Contract',i=>i.metadata.contract_refs.push({...i.metadata.contract_refs[0]})]])
 test('metadata plan rejects '+name,()=>{const i=source();mutate(i);assert.throws(()=>buildSource(context,i));});
test('wrapper executes only the exact private registered native constructor, no hidden transport',async()=>{
 const sql=[];const session={execute:async s=>{sql.push(s);return '{"synthetic_mock":true}';}};
 assert.deepEqual(await create({session,actorUserId:uid(2),authorizationId:uid(1),turnBackendPid:123}),{synthetic_mock:true});
 assert.equal(sql.length,1);assert.match(sql[0],/^select private\.weekly_source_native_fixture_participants_create_v1\(/);
 assert.doesNotMatch(sql[0],/commit|begin|http|provider|advisory/);
});
test('all current native owner bytes are preserved outside finite recorded replacements',()=>{
 const m=JSON.parse(read('FINITE_REPLACEMENTS.json'));
 for(const item of m.changes){let s=read(item.reference);for(const e of item.edits){assert.equal(s.split(e.before).length-1,1);s=s.replace(e.before,e.after);}assert.equal(s,read(item.forward));}
});
test('current SQL arm requires actual registered completion before generated refs',()=>{
 const s=read('forward/001_native_participants.sqlinc');
 for(const key of ['WEEKLY_SOURCE_POST_VERIFIED_EVIDENCE_RECORDED','WEEKLY_SOURCE_NATIVE_FIXTURE_PARTICIPANTS_ADMITTED',
 'WEEKLY_SOURCE_NATIVE_FIXTURE_PARTICIPANTS_CREATED',"made->'replay' is distinct from 'false'",'CLIENT_CREATED_WITH_SETTINGS','event_sequence<a.event_sequence',
 'pg_current_xact_id()','l.pid<>pg_backend_pid()'])assert.ok(s.includes(key),key);
 assert.match(s,/return receipt; -- Exact existing result; never another INSERT or clock/);
 const clean=s.replace(/^\s*--.*$/gm,'');assert.doesNotMatch(clean,/nextval|setval|disable trigger|session_replication_role|execute.*http|config_json|set_config/i);
});
test('past metadata uses real defaults, native cutoff formula, true OPEN and no source/finance production',()=>{
 const s=read('forward/002_source_metadata.sqlinc');
 assert.match(s,/\(\(\(m->>'week_ending'\)::date\+g.cutoff_weekday\)\+g.cutoff_local_time\) at time zone g.timezone/);
 assert.match(s,/'OPEN',0,'NONE'/);assert.match(s,/clock_timestamp\(\).*'completed_at_utc'|'completed_at_utc',clock_timestamp\(\)/);
 assert.match(s,/not between|is distinct from/);assert.ok(s.includes("k.self_bill is distinct from true"));
 const inserts=[...s.matchAll(/insert into ([a-z_.]+)/gi)].map(m=>m[1]);
 assert.deepEqual(inserts,['public.weekly_source_groups','public.weekly_source_group_clients','public.weekly_source_client_policies','public.weekly_source_cycles']);
 assert.doesNotMatch(s,/insert into[^;]*created_at_utc/si);
});
test('new registration cannot enter the original pre-VERIFIED BULK path',()=>{
 const s=read('forward/weekly-source-protected-test-setup-evidence.mjs');
 assert.ok(s.indexOf("fail('NATIVE_FIXTURE_POST_VERIFIED_PHASE_REQUIRED')")<s.indexOf("exact(bundle,['schema_version','authorizations']"));
 const sql=read('forward/040_post_verified_record.sqlinc');
 assert.ok(sql.indexOf('weekly_source_native_fixture_participants_artifact_assert_v1')<sql.indexOf('weekly_source_post_verified_bulk_setup_artifact_assert_v1'));
 const js=read('forward/weekly-source-post-verified-evidence-controller.mjs');
 assert.ok(js.indexOf('if(row.schema_version===nativeSourceSchema)')<js.indexOf("if(Object.hasOwn(row,'purpose'))"));
 const census=JSON.parse(read('NATIVE_SOURCE_CENSUS.json'));assert.equal(census.controlling_reserved_namespace_guards.filter(x=>x.action==='UNCHANGED_BULK_ONLY_PRE_VERIFIED_NAMESPACE').length,2);
});

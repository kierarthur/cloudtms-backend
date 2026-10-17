// Source/structure tests only. No native result, connection or turn.
import test from'node:test';import assert from'node:assert/strict';import fs from'node:fs';import path from'node:path';import{fileURLToPath,pathToFileURL}from'node:url';import{createHash}from'node:crypto';
import{buildActorProgram,fixturePredicate}from'./build-program.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026',pins=JSON.parse(fs.readFileSync(here+'/SOURCE_PINS.json'));
const actual=await buildActorProgram({root}),record=JSON.parse(fs.readFileSync(here+'/PROGRAMME.json'));
const{scanManagedSql}=await import(pathToFileURL(root+'/scripts/cloudtms-managed-release-sql.mjs').href);
test('source programme equals frozen hash/counts and only one complete genuine actor definition',()=>{
 assert.equal(createHash('sha256').update(actual.sql).digest('hex'),record.sqlSha256);assert.equal(Buffer.byteLength(actual.sql),record.sqlBytes);
 const p=scanManagedSql(actual.sql),definitions=p.statements.map(s=>p.source.slice(s.start,s.end)).filter(s=>s.startsWith('create or replace function '));assert.equal(p.statements.length,7);assert.equal(definitions.length,1);
 const page=scanManagedSql(fs.readFileSync(root+'/supabase/repeatable/17102026_0248_nhsp_v6_daily_health_support/01_complete_helpers.sqlinc','utf8'));
 const owner=page.statements.map(s=>page.source.slice(s.start,s.end).trim()).find(s=>s.startsWith('create or replace function private.daily_rate_correction_health_actor_require_v1('));assert.equal(definitions[0].trim(),owner);
});
test('caller cannot add SQL, fixture identity or root path to the finite programme',async()=>{
 assert.equal((await buildActorProgram({root,sql:'select false;',fixtures:['caller']})).sql,actual.sql);
 await assert.rejects(()=>buildActorProgram({root:path.dirname(root)}),/REGISTERED_PHYSICAL_ROOT/);
 assert.throws(()=>fixturePredicate({...pins,fixtureIDs:[...pins.fixtureIDs].reverse()}),/EXACT_RESERVED_NAMESPACE/);
 assert.throws(()=>fixturePredicate({...pins,fixtureEmails:[...pins.fixtureEmails,'extra@example.invalid']}),/EXACT_RESERVED_NAMESPACE/);
 assert.throws(()=>fixturePredicate(pins,'caller'),/FIXED_ALIAS/);
});
test('native row store, genuine WP-PP, touch trigger and full owner/RLS boundaries are required before insertion',()=>{
 const s=actual.sql,insert=s.indexOf('insert into public.tms_users');assert(insert>0);
 for(const guard of ['DAILY_ACTOR_NATIVE_WPPP_OR_TOUCH_SOURCE_CHANGED','DAILY_ACTOR_NATIVE_OWNER_BOUNDARY_CHANGED','DAILY_ACTOR_NATIVE_STORE_BOUNDARY_CHANGED','DAILY_ACTOR_NATIVE_STORE_INVARIANT_OR_TRIGGER_CHANGED','DAILY_ACTOR_FIXTURE_NAMESPACE_NOT_VACANT','DAILY_ACTOR_NATIVE_PRIVATE_ACL_REQUIRED'])assert(s.indexOf(guard)<insert,guard);
 for(const hash of[pins.userStore.wp.definition_sha256,pins.userStore.touch.definition_sha256])assert(s.includes(hash));
 assert(s.includes("current_setting('transaction_isolation')<>'read committed'"));assert(s.includes('pg_get_userbyid(c.relowner)=current_user'));assert(s.includes('not c.relforcerowsecurity'));assert(s.includes('pg_inherits'));assert(s.includes('pg_rewrite'));
});
test('private effective ACL and real role transitions cannot be replaced by grants or a SET ROLE failure',()=>{
 const s=actual.sql;assert(s.includes("a.grantee<>p.proowner or a.privilege_type<>'EXECUTE' or a.is_grantable"));
 for(const role of['anon','authenticated','service_role']){assert(s.includes("has_function_privilege('"+role+"'"));assert(s.includes('set local role '+role+';rejected:=false;begin\n  perform private.daily_rate_correction_health_actor_require_v1('));}
 assert.doesNotMatch(s,/\bgrant\s|exception when insufficient_privilege then reset role/);
});
test('golden invariant remains native and all state transitions affect only fixture001',()=>{
 const s=actual.sql,updates=[...s.matchAll(/update public\.tms_users set ([^;]+);/g)].map(m=>m[1]);assert.equal(updates.length,4);
 for(const update of updates.slice(0,3))assert(update.endsWith("where id='"+pins.fixtureIDs[0]+"'::uuid"));
 assert(updates[3].endsWith("where id='"+pins.fixtureIDs[2]+"'::uuid"));assert(s.includes('get stacked diagnostics violated_constraint=CONSTRAINT_NAME'));
 assert(s.includes("violated_constraint<>'tms_users_payment_golden_implies_authoriser_chk'"));assert(s.includes('and payment_authoriser and payment_golden_key'));
 assert(s.includes('jsonb_build_array(u.id,u.email,u.role,u.is_active,u.password_hash,u.payment_authoriser,u.payment_golden_key) order by u.id'));
 for(const column of['created_at','updated_at','display_name','email_signature_html','last_login_at_utc','email_settings','grid_prefs_json','session_version'])assert(s.slice(s.indexOf('if current_user<>session_user then')).includes('u.'+column));
 assert(s.includes('DAILY_ACTOR_NATIVE_EXACT_AFTER_STATE_MISMATCH'));
 assert.doesNotMatch(s,/disable trigger|drop constraint|set constraints|insert into (?:public|private)\.(?!tms_users)/i);
});
test('actor result retains explicit unsupported-path holds and zero financial/queue effects',()=>{
 const s=actual.sql;assert(s.includes("'dailyBasis','NOT_RUN','postWaitRace','NOT_RUN','outerIsolation','read committed'"));
 assert.doesNotMatch(s,/daily_rate_correction_health_(?:basis|review_prepare|accept_basis|result_basis)_v1|\b(?:commit|rollback)\s*;|\b(?:http|dblink|net)\./i);
 assert.deepEqual(actual.directBusinessRelations,['public.tms_users']);assert.equal(actual.businessFixtureRows,6);assert.equal(actual.matrixChecks,12);assert.equal(actual.matrixDenials,10);assert.equal(actual.native,'NOT_RUN');
});

import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { EventEmitter } from 'node:events';
import { PassThrough, Writable } from 'node:stream';
import test from 'node:test';
import { ManagedPsqlSession, ManagedPsqlError } from '../scripts/cloudtms-managed-psql.mjs';
import { canonical, digest, exactAtomicBody, managedManifestHash, readManagedClosure, scanManagedSql } from '../scripts/cloudtms-managed-release-sql.mjs';
import { BASELINE_MIGRATION_PREFIX, BOOTSTRAP_PIN, MANAGED_EXCEPTIONS, executionPolicy } from '../scripts/cloudtms-managed-execution-policy.mjs';
import { acquireWriterSql, applyManagedRelease, assertBootstrapPrefix, assertConnectionPreservingException, assertWriterSql,
  compileBootstrap, compileManagedUnit, managedAdmissionSql, managedReleaseContext,
  OWNER_RLS_ENVELOPE, pendingInventory, prepareManagedSourceReaders, resetWriterSql, unitCheckpointSql } from '../scripts/cloudtms-managed-release.mjs';
import { inventory, readJson, repoRoot, executableSqlFile, mapExecutableSqlSource } from '../scripts/cloudtms-db-release-lib.mjs';

test('packaging lexer treats quoted/dollar bodies as opaque, not transaction boundaries',()=>{
  const source=`-- BEGIN;\n\\set ON_ERROR_STOP on\nbegin;\ncreate or replace function private.x() returns void language plpgsql as $fn$begin commit; end$fn$;\nselect 'COMMIT; \\connect other';\ncommit;`;
  const body=exactAtomicBody(source);
  assert.match(body,/begin commit; end\$fn\$/);assert.match(body,/'COMMIT;/);
  assert.equal(scanManagedSql(body).statements.length,2);
  assert.doesNotMatch(body,/^begin;|^commit;/im);
});

test('atomic packaging refuses unknown or nontransactional envelopes before writing',()=>{
  for(const source of ['begin;select 1;commit;begin;select 2;commit;','select 1;begin;commit;',
    'create index concurrently x on y(id);','call private.backfill();','vacuum public.x;',
    '\\connect other\nselect 1;','\\set ON_ERROR_STOP off\nselect 1;',
    'select pg_advisory_unlock_all();','set transaction isolation level serializable;',
    'begin;select 1;rollback;','select 1'])assert.throws(()=>exactAtomicBody(source),/MANAGED_SQL_/);
  assert.throws(()=>scanManagedSql("select 'broken"),/UNTERMINATED/);
  assert.throws(()=>scanManagedSql('do $x$ broken'),/UNTERMINATED/);
  assert.throws(()=>scanManagedSql('/* nested /* broken */'),/UNTERMINATED/);
  assert.equal(exactAtomicBody('/* BEGIN; /* COMMIT; */ */ select 1;').trim(),'/* BEGIN; /* COMMIT; */ */ select 1;');
});

test('session reset is fresh-file equivalent without releasing its advisory guard',()=>{
  assert.match(resetWriterSql,/RESET ROLE; RESET SESSION AUTHORIZATION; RESET ALL/);
  assert.match(resetWriterSql,/DEALLOCATE ALL; UNLISTEN \*; DISCARD TEMP; DISCARD PLANS; DISCARD SEQUENCES/);
  assert.doesNotMatch(resetWriterSql,/DISCARD ALL|pg_advisory_unlock/i);
  for(const variable of ['cloudtms_environment','bpay_backfill_batch','invoice_presentation_active_work'])assert.ok(resetWriterSql.includes(`\\unset ${variable}`));
  assert.match(assertWriterSql,/pg_backend_pid\(\)/);assert.match(assertWriterSql,/objsubid=1/);
});

test('exact replay policy covers only audited current exceptions and refuses changed closure',()=>{
  assert.equal(MANAGED_EXCEPTIONS.length,37);
  for(const entry of MANAGED_EXCEPTIONS){
    const closure=readManagedClosure(entry.path,repoRoot);
    assert.equal(executionPolicy(closure),entry);
    assert.ok(entry.rationale.length>120);
    assertConnectionPreservingException(closure,entry);
    assert.throws(()=>executionPolicy({...closure,closureHash:'0'.repeat(64)}),/POLICY_HASH_MISMATCH/);
  }
  assert.equal(executionPolicy({path:'supabase/migrations/unknown.sql'}),null);
  assert.throws(()=>assertConnectionPreservingException({path:'x',expanded:'\\connect elsewhere'}, {reason:'MODAL'}),/META_UNCLASSIFIED/);
  assert.throws(()=>assertConnectionPreservingException({path:'x',expanded:'do $$begin perform pg_advisory_unlock_all(); end$$;'}, {reason:'MODAL'}),/GUARD_COMMAND/);
});

test('whole current NEW selected inventory classifies with unchanged LF/closure pins',()=>{
  const current=inventory(),release=readJson('supabase/release/current-release.json');
  const anchor=current.migrations.findIndex(row=>row.path===release.controlPlaneMigration);
  const original=new Map(readJson(release.baselineRepeatableLock).repeatables.map(row=>[row.path,row.sha256]));
  let atomic=0,replay=0;
  for(const [kind,rows] of [['migration',current.migrations.slice(anchor+1)],['repeatable',current.repeatables.filter(row=>original.get(row.path)!==row.sha256)]]){
    for(const row of rows){
      const unit=compileManagedUnit(row,kind,{root:repoRoot,mapSource:mapExecutableSqlSource,executableFile:file=>executableSqlFile(file,{canonical:true})});
      if(unit.exception)replay++;else atomic++;
    }
  }
  assert.equal(replay,37);assert.ok(atomic>500);
});

function context(mode='UPGRADE') {return {mode,releaseId:'test-managed',gitCommit:'a'.repeat(40),expectedHash:'b'.repeat(64),
  environment:'TEST',customerKey:'test-customer',expectedDatabase:'test-managed-db',manifestHash:'c'.repeat(64)};}

test('admission binds full identity/manifest and resumes only the identical guarded attempt',()=>{
  const sql=managedAdmissionSql(context('NEW'));
  for(const value of ['a'.repeat(40),'b'.repeat(64),'c'.repeat(64),'test-customer','test-managed-db','NEW'])assert.ok(sql.includes(value));
  assert.match(sql,/status='APPLYING'[\s\S]*release_id<>/);
  assert.match(sql,/not \([\s\S]*manifest_sha256/);
  assert.match(sql,/coalesce\(r\.evidence_json->'managed_release' @>[\s\S]*,false\)/);
  assert.match(sql,/CLOUDTMS_NEW_MANAGED_BOOTSTRAP_RECEIPT_REQUIRED/);
  assert.match(sql,/evidence_json=private\.cloudtms_database_releases\.evidence_json\|\|excluded\.evidence_json/);
  assert.doesNotMatch(sql,/started_at_utc=|superseded|verified\.started_at|pg_advisory_xact_lock/);
  assert.throws(()=>managedAdmissionSql({...context(),gitCommit:'a'.repeat(12)}),/CONTEXT_INVALID/);
});

test('unit DDL and checkpoint share one exact transaction; replay preserves original includes',()=>{
  const c=context(),item={path:'supabase/migrations/test.sql',sha256:'d'.repeat(64)};
  const sql=unitCheckpointSql({item,kind:'migration',body:'create table public.new_once(id int);'},c);
  assert.equal(scanManagedSql(sql).statements.filter(row=>row.tokens[0]==='BEGIN').length,1);
  assert.equal(scanManagedSql(sql).statements.at(-1).tokens[0],'COMMIT');
  assert.ok(sql.indexOf('create table')<sql.indexOf('cloudtms_migration_ledger'));
  assert.ok(sql.lastIndexOf('CLOUDTMS_MANAGED_WRITER_GUARD_LOST')<sql.indexOf('cloudtms_migration_ledger'));
  const replay=unitCheckpointSql({item,kind:'repeatable',script:"\\ir 'unchanged/root.sql'",exception:{reason:'INVOICE_GATE'}},c);
  assert.match(replay,/\\ir 'unchanged\/root.sql'/);assert.match(replay,/CLOUDTMS_INVOICE_INSTALL_DEFERRED_ACTIVE_WORK/);
  assert.ok(replay.indexOf('CLOUDTMS_INVOICE_INSTALL_DEFERRED_ACTIVE_WORK')<replay.indexOf('cloudtms_repeatable_ledger'));
  assert.match(replay,/\{\?invoice_presentation_active_work\}/);
});

test('one exact historical FORCE-RLS seed receives only its pinned atomic owner envelope',()=>{
  const item=inventory().migrations.find(row=>row.path===OWNER_RLS_ENVELOPE.path);
  const unit=compileManagedUnit(item,'migration',{root:repoRoot,mapSource:mapExecutableSqlSource});
  assert.equal(item.sha256,OWNER_RLS_ENVELOPE.contentSha256);
  assert.equal(unit.closure.closureHash,OWNER_RLS_ENVELOPE.closureSha256);
  assert.equal(unit.closure.files.size,1);assert.equal(unit.ownerRlsEnvelope,OWNER_RLS_ENVELOPE);
  const original=canonical(fs.readFileSync(path.join(repoRoot,item.path),'utf8'));
  assert.equal(unit.body,exactAtomicBody(original));
  const sql=unitCheckpointSql(unit,context());
  assert.equal(scanManagedSql(sql).statements.filter(row=>row.tokens[0]==='BEGIN').length,1);
  assert.equal(scanManagedSql(sql).statements.at(-1).tokens[0],'COMMIT');
  assert.ok(sql.includes(unit.body));
  assert.ok(sql.indexOf('create policy')<sql.indexOf(unit.body));
  assert.ok(sql.indexOf(unit.body)<sql.indexOf('drop policy'));
  assert.ok(sql.indexOf('drop policy')<sql.indexOf('CLOUDTMS_MANAGED_OWNER_RLS_FINAL_POSTURE_CHANGED'));
  assert.ok(sql.indexOf('CLOUDTMS_MANAGED_OWNER_RLS_FINAL_POSTURE_CHANGED')<sql.indexOf('cloudtms_migration_ledger'));
  assert.match(sql,/on commit drop/);
  assert.doesNotMatch(sql,/NO\s+FORCE|DISABLE\s+ROW\s+LEVEL|ALTER\s+ROLE|SET\s+ROLE|row_security\s*=\s*off|grant\s/gi);
  for(const row of inventory().migrations.filter(row=>row.path!==item.path).slice(-10)){
    assert.equal(compileManagedUnit(row,'migration',{root:repoRoot,mapSource:mapExecutableSqlSource}).ownerRlsEnvelope,null);
  }
});

test('owner envelope refuses scope/body/hash/kind substitutions and changed accepted bytes',()=>{
  const item=inventory().migrations.find(row=>row.path===OWNER_RLS_ENVELOPE.path);
  const unit=compileManagedUnit(item,'migration',{root:repoRoot});
  for(const invalid of [
    {...unit,kind:'repeatable'}, {...unit,body:unit.body+'select 1;'},
    {...unit,ownerRlsEnvelope:{...OWNER_RLS_ENVELOPE}}, {...unit,ownerRlsEnvelope:null},
    {...unit,item:{...item,path:'supabase/migrations/other.sql'}},
    {...unit,item:{...item,sha256:'0'.repeat(64)}},
    {...unit,exception:{reason:'BACKFILL'}}, {...unit,closure:{...unit.closure,closureHash:'0'.repeat(64)}},
  ])assert.throws(()=>unitCheckpointSql(invalid,context()),/OWNER_RLS_ENVELOPE_(SCOPE_INVALID|SOURCE_MISMATCH)/);
  assert.throws(()=>compileManagedUnit({...item,sha256:unit.closure.closureHash},'repeatable',{root:repoRoot}),/OWNER_RLS_ENVELOPE_SOURCE_MISMATCH/);
  const root=fs.mkdtempSync(path.join(os.tmpdir(),'cloudtms-owner-rls-pin-'));
  try{
    const altered=canonical(fs.readFileSync(path.join(repoRoot,item.path),'utf8'))+'\n-- Changed accepted source\n';
    fs.mkdirSync(path.dirname(path.join(root,item.path)),{recursive:true});fs.writeFileSync(path.join(root,item.path),altered);
    assert.throws(()=>compileManagedUnit({...item,sha256:digest(altered)},'migration',{root}),/OWNER_RLS_ENVELOPE_SOURCE_MISMATCH/);
  }finally{fs.rmSync(root,{recursive:true});}
});

test('owner seed SQL requires empty policies/non-bypass owner and preserves exact final ACL/RLS',()=>{
  const item=inventory().migrations.find(row=>row.path===OWNER_RLS_ENVELOPE.path);
  const sql=unitCheckpointSql(compileManagedUnit(item,'migration',{root:repoRoot}),context());
  assert.match(sql,/current_user<>session_user/);
  assert.match(sql,/not rolsuper and not rolbypassrls/);
  assert.match(sql,/current_setting\('row_security'\)<>'on'/);
  assert.equal((sql.match(/not rolsuper and not rolbypassrls/g)||[]).length,2);
  assert.equal((sql.match(/current_setting\('row_security'\)<>'on'/g)||[]).length,2);
  assert.match(sql,/v_target\.relkind<>'r'/);
  assert.match(sql,/v_target\.relowner<>current_user::regrole::oid/);
  assert.match(sql,/not v_target\.relrowsecurity or not v_target\.relforcerowsecurity/);
  assert.match(sql,/exists\(select 1 from pg_catalog\.pg_policy where polrelid=v_target\.oid\)/);
  assert.match(sql,/a\.grantee<>v_target\.relowner/);
  assert.match(sql,/as permissive for all to %I using \(true\) with check \(true\)/);
  assert.match(sql,/v_policy\.polroles is distinct from array\[v_saved\.owner_oid\]::oid\[\]/);
  assert.match(sql,/count\(\*\) from pg_catalog\.pg_policy where polrelid=v_saved\.relation_oid\)<>1/);
  assert.match(sql,/v_target\.relacl[\s\S]*is distinct from[\s\S]*v_saved\.relation_acl/);
  assert.equal((sql.match(/create policy cloudtms_managed_installer_owner_seed/g)||[]).length,1);
  assert.equal((sql.match(/drop policy cloudtms_managed_installer_owner_seed/g)||[]).length,1);
  assert.doesNotMatch(sql,/cloudtms_miget_service_owner_all|candidate_manager_email_route_receipts/);
});

test('new owner-envelope version enters manifest/admission; old managed evidence cannot match',()=>{
  const current=inventory(),release=readJson('supabase/release/current-release.json');
  const c=managedReleaseContext({root:repoRoot,current,release,context:context('NEW')});
  const oldHash=managedManifestHash({root:repoRoot,current,release,
    executionPolicy:{bootstrap:BOOTSTRAP_PIN,baselinePrefix:BASELINE_MIGRATION_PREFIX,exceptions:MANAGED_EXCEPTIONS}});
  assert.notEqual(c.manifestHash,oldHash);
  const sql=managedAdmissionSql(c);
  assert.match(sql,/owner_rls_envelope_version/);assert.ok(sql.includes(OWNER_RLS_ENVELOPE.version));
  assert.ok(sql.includes(c.manifestHash));assert.ok(!sql.includes(oldHash));
  assert.match(sql,/CLOUDTMS_DATABASE_RELEASE_IDENTITY_MISMATCH/);
  assert.match(sql,/CLOUDTMS_NEW_MANAGED_BOOTSTRAP_RECEIPT_REQUIRED/);
});

test('pending inventory never blesses unrecorded objects or changed migration receipts',()=>{
  const current={migrations:[{path:'x',sha256:'1'}],repeatables:[{path:'r',sha256:'2'}]};
  assert.deepEqual(pendingInventory(current,{migrations:[],repeatables:[]}),current);
  assert.deepEqual(pendingInventory(current,{migrations:current.migrations,repeatables:current.repeatables}),{migrations:[],repeatables:[]});
  assert.throws(()=>pendingInventory(current,{migrations:[{path:'x',sha256:'3'}],repeatables:[]}),/LEDGER_MISMATCH/);
  assert.throws(()=>pendingInventory(current,{migrations:[{path:'absent',sha256:'1'}],repeatables:[]}),/LEDGER_MISMATCH/);
  assert.throws(()=>pendingInventory(current,{migrations:[...current.migrations,...current.migrations],repeatables:[]}),/LEDGER_DUPLICATE/);
});

test('bootstrap is atomic and receipts ORIGINAL baseline, not later current definitions',()=>{
  const current=inventory(),release=readJson('supabase/release/current-release.json');
  const c=managedReleaseContext({root:repoRoot,current,release,context:context('NEW')});
  const sql=compileBootstrap({root:repoRoot,current,release,context:c,mapSource:mapExecutableSqlSource});
  assert.ok(Buffer.byteLength(sql)<32*1024*1024);
  const controls=scanManagedSql(sql).statements.filter(row=>['BEGIN','COMMIT','ROLLBACK','CALL'].includes(row.tokens[0]));
  assert.deepEqual(controls.map(row=>row.tokens[0]),['BEGIN','COMMIT']);
  assert.match(sql,/CLOUDTMS_NEW_REQUIRES_EMPTY_SCHEMA/);assert.match(sql,/bootstrap_complete/);
  assert.ok(!sql.includes("values('supabase/migrations/03102026_1324_banking_pay_next_core.sql'"));
  for(const row of readJson(release.baselineRepeatableLock).repeatables)assert.ok(sql.includes(`'${row.path}','${row.sha256}'`));
  assert.ok(sql.includes("\\set cloudtms_environment TEST"));
  assert.equal((sql.match(/RESET SESSION AUTHORIZATION/g)||[]).length,release.baselineFiles.length+2);
  assert.ok(sql.includes("v_secret_id := vault.create_secret("));
  assert.throws(()=>assertBootstrapPrefix({...current,migrations:[{path:'supabase/migrations/backdated.sql',sha256:'a'.repeat(64)},...current.migrations]},release),/PREFIX_MISMATCH/);
  assert.throws(()=>assertBootstrapPrefix({...current,migrations:current.migrations.map((row,index)=>index===0?{...row,sha256:'a'.repeat(64)}:row)},release),/PREFIX_MISMATCH/);
});

test('reader preparation stays bounded, exact paired activation is separate from VERIFIED',async()=>{
  let calls=0;
  assert.deepEqual(await prepareManagedSourceReaders(async sql=>{assert.match(sql,/pg_backend_pid/);return ++calls===3?'t':'f';}),{calls:3});
  await assert.rejects(()=>prepareManagedSourceReaders(async()=> 'yes'),/INVALID_RESULT/);
  calls=0;await assert.rejects(()=>prepareManagedSourceReaders(async()=>{calls++;return 'f';}),/RESUME_REQUIRED/);
  assert.equal(calls,1000);
});

function fakeChild(onWrite) {
  const child=new EventEmitter();child.stdout=new PassThrough();child.stderr=new PassThrough();
  child.stdin=new Writable({write(chunk,_encoding,callback){queueMicrotask(()=>onWrite(String(chunk),child));callback();}});
  child.kill=()=>{queueMicrotask(()=>child.emit('close',null));return true;};
  child.stdin.on('finish',()=>queueMicrotask(()=>child.emit('close',0)));
  return child;
}
function ack(script,child,value='') {
  const start=script.match(/\\echo ([^\n]+)_START__/)[1]+'_START__';
  const end=script.match(/\\echo ([^\n]+)_END__/)[1]+'_END__';
  child.stdout.write(start+'\n'+value+'\n'+end+'\n');
}
test('persistent transport bounds streams, preserves exact output and one writer connection',async()=>{
  let starts=0;
  const session=new ManagedPsqlSession({args:[],spawnImpl:()=>{starts++;return fakeChild((script,child)=>{
    if(script==='\\q\n')return;
    const start=script.match(/\\echo ([^\n]+)_START__/)[1]+'_START__';
    const end=script.match(/\\echo ([^\n]+)_END__/)[1]+'_END__';
    const bytes=Buffer.from(start+'\r\nµ 2026-10-04 11:00:00.123456\r\n'+end+'\r\n');
    for(let i=0;i<bytes.length;i++)child.stdout.write(bytes.subarray(i,i+1));
  });}});
  assert.equal(await session.execute('select 1;'),'µ 2026-10-04 11:00:00.123456');
  assert.equal(await session.execute('select 2;'),'µ 2026-10-04 11:00:00.123456');
  assert.equal(starts,1);await session.close();
});
test('transport unknown disconnect does not echo server SQL/credential or retry',async()=>{
  let starts=0;
  const session=new ManagedPsqlSession({args:['postgresql://secret'],spawnImpl:()=>{starts++;return fakeChild((_script,child)=>{
    child.stderr.write('ERROR:  40P01\nDETAIL: secret SQL credential\n');child.emit('close',3);
  });}});
  await assert.rejects(()=>session.execute('secret SQL'),error=>error.sqlState==='40P01'&&!/secret|credential|DETAIL/.test(error.message));
  await assert.rejects(()=>session.execute('retry'),ManagedPsqlError);assert.equal(starts,1);await session.close();
});
test('transport rejects command/output caps, concurrent writes, timeout and invalid framing',async()=>{
  const make=(onWrite,options={})=>new ManagedPsqlSession({args:[],spawnImpl:()=>fakeChild(onWrite),...options});
  let session=make(()=>{}, {maxCommandBytes:4});await assert.rejects(()=>session.execute('12345'),/COMMAND_LIMIT/);await session.close();
  session=make((_script,child)=>child.stderr.write('x'.repeat(1000)),{maxOutputBytes:100});await assert.rejects(()=>session.execute('select 1;'),/OUTPUT_LIMIT/);await session.close();
  session=make(()=>{}, {timeoutMs:10});const pending=session.execute('select 1;');await assert.rejects(()=>session.execute('select 2;'),/CONCURRENT_COMMAND/);await assert.rejects(()=>pending,/COMMAND_TIMEOUT/);await session.close();
  session=make((script,child)=>child.stdout.write(script.match(/\\echo ([^\n]+)_END__/)[1]+'_END__\n'));await assert.rejects(()=>session.execute('select 1;'),/ACK_INVALID/);await session.close();
});

// This is a transport/control-flow model, NOT a PG execution or financial proof.
function smallRepository() {
  const root=fs.mkdtempSync(path.join(os.tmpdir(),'cloudtms-managed-test-'));
  const put=(file,source)=>{fs.mkdirSync(path.dirname(path.join(root,file)),{recursive:true});fs.writeFileSync(path.join(root,file),source);return {path:file,sha256:digest(canonical(source))};};
  const baseline='supabase/baseline/structural.sql',control='supabase/migrations/control.sql';
  put(baseline,'create table public.baseline(id int);');
  const anchor=put(control,'create table private.control(id int);');
  const next=put('supabase/migrations/next.sql','begin;create table public.nonidempotent_next(id int);commit;');
  const routine=put('supabase/repeatable/current.sql','create or replace function public.current_fn() returns int language sql as $$select 1$$;');
  routine.sha256=readManagedClosure(routine.path,root).closureHash;
  const bootstrap='supabase/baseline/22082026_1506_cloudtms_new_database_bootstrap.sql';
  put(bootstrap,fs.readFileSync(path.join(repoRoot,bootstrap),'utf8'));
  const lock='supabase/release/baseline-lock.json';put(lock,JSON.stringify({repeatables:[{path:'supabase/repeatable/original.sql',sha256:'d'.repeat(64)}]}));
  return {root,baselinePrefix:{count:1,sha256:digest(JSON.stringify([[anchor.path,anchor.sha256]]))},current:{migrations:[anchor,next],repeatables:[routine]},release:{baselineFiles:[baseline],controlPlaneMigration:control,
    bootstrapFile:bootstrap,baselineRepeatableLock:lock,verificationFiles:[],newVerificationFiles:[]}};
}
function modelWriter(state,failAt) {
  let owns=false;
  const writer={failure:null,finished:false,async close(){this.finished=true;if(owns)state.lock=false;},async execute(sql){
    state.calls.push(sql);
    if(sql===acquireWriterSql){if(state.lock)return 'f';state.lock=true;owns=true;return 't';}
    if(sql==='select pg_catalog.current_database();')return 'test-managed-db';
    if(sql.includes("to_regclass('private.cloudtms_database_releases')"))return String(state.control);
    if(sql.startsWith('select count(*)'))return String(state.control?1:state.foreignObjects??0);
    if(sql.startsWith('select exists(select 1 from private.cloudtms_database_releases r'))return String(state.receipt);
    if(sql.startsWith('select jsonb_build_object('))return JSON.stringify({migrations:state.migrations,repeatables:state.repeatables});
    if(sql.includes('create table public.nonidempotent_next')&&failAt==='before'){
      this.failure=new ManagedPsqlError('CONNECTION_CLOSED');throw this.failure;
    }
    for(const [table,target] of [['cloudtms_migration_ledger',state.migrations],['cloudtms_repeatable_ledger',state.repeatables]]){
      const regex=new RegExp(`insert into private\\.${table}[^;]*?values\\('([^']+)','([a-f0-9]+)'`,'g');
      for(const match of sql.matchAll(regex)){const old=target.find(row=>row.path===match[1]);if(old)old.sha256=match[2];else target.push({path:match[1],sha256:match[2]});}
    }
    if(sql.includes('create table public.baseline')){state.control=true;state.receipt=true;state.bootstrap++;}
    if(sql.includes('create table public.nonidempotent_next')){
      state.next++;if(failAt==='after'){this.failure=new ManagedPsqlError('CONNECTION_CLOSED');throw this.failure;}
    }
    if(sql.includes("set status='VERIFIED'"))state.verified=true;
    if(sql.includes("set status='FAILED'"))state.failed=true;
    return '';
  }};return writer;
}
function modelState(){return {control:false,receipt:false,lock:false,migrations:[],repeatables:[],calls:[],bootstrap:0,next:0};}
function modelApply(repository,state,{failAt,verify=()=>({sha256:'b'.repeat(64)})}={}){
  return applyManagedRelease({...repository,context:context('NEW'),openSession:()=>modelWriter(state,failAt),
    preapply:()=>{assert.equal(state.lock,true);},verify:()=>{assert.equal(state.lock,true);return verify();}});
}
test('connected NEW restarts after lost bootstrap/file replies without a second nonidempotent DDL',async()=>{
  const repository=smallRepository(),state=modelState();
  try{
    await assert.rejects(()=>modelApply(repository,state,{failAt:'after'}),/CONNECTION_CLOSED/);
    assert.equal(state.bootstrap,1);assert.equal(state.next,1);assert.equal(state.failed,undefined);
    assert.equal(state.lock,false);assert.equal(state.verified,undefined);
    await modelApply(repository,state);
    assert.equal(state.bootstrap,1);assert.equal(state.next,1);assert.equal(state.verified,true);
  }finally{fs.rmSync(repository.root,{recursive:true});}
});
test('connected NEW pre-commit failure reruns missing DDL and ledger together',async()=>{
  const repository=smallRepository(),state=modelState();
  try{
    await assert.rejects(()=>modelApply(repository,state,{failAt:'before'}),/CONNECTION_CLOSED/);
    assert.equal(state.next,0);assert.equal(state.migrations.some(row=>row.path.endsWith('/next.sql')),false);
    await modelApply(repository,state);assert.equal(state.next,1);assert.equal(state.verified,true);
  }finally{fs.rmSync(repository.root,{recursive:true});}
});
test('definitions complete plus failed verifier is not VERIFIED; fresh restart reruns verification',async()=>{
  const repository=smallRepository(),state=modelState();let attempts=0;
  try{
    await assert.rejects(()=>modelApply(repository,state,{verify:()=>{attempts++;throw Error('Verifier failed');}}),/Verifier failed/);
    assert.equal(state.failed,true);assert.equal(state.verified,undefined);assert.equal(state.next,1);
    await modelApply(repository,state,{verify:()=>{attempts++;return {sha256:'b'.repeat(64)};}});
    assert.equal(attempts,2);assert.equal(state.next,1);assert.equal(state.verified,true);
  }finally{fs.rmSync(repository.root,{recursive:true});}
});
test('unclassified execution refuses BEFORE a new bootstrap; arbitrary nonempty NEW refuses',async()=>{
  const repository=smallRepository();
  try{
    const file=path.join(repository.root,repository.current.migrations[1].path);
    const invalid='call private.unsupported();';fs.writeFileSync(file,invalid);repository.current.migrations[1].sha256=digest(invalid);
    const state=modelState();await assert.rejects(()=>modelApply(repository,state),/ENVELOPE_UNCLASSIFIED/);
    assert.equal(state.bootstrap,0);assert.equal(state.receipt,false);
    const nonempty=modelState();nonempty.foreignObjects=1;await assert.rejects(()=>modelApply(repository,nonempty),/NEW requires an empty/);
    assert.equal(nonempty.bootstrap,0);
  }finally{fs.rmSync(repository.root,{recursive:true});}
});

test('connected UPGRADE uses the same guarded atomic/checkpoint path, no bootstrap or ledger census',async()=>{
  const repository=smallRepository(),state=modelState();state.control=true;
  state.migrations=[repository.current.migrations[0]];let verifications=0;
  const apply=failAt=>applyManagedRelease({...repository,context:context('UPGRADE'),
    openSession:()=>modelWriter(state,failAt),preapply:()=>assert.equal(state.lock,true),
    verify:()=>{verifications++;assert.equal(state.lock,true);return {sha256:'b'.repeat(64)};}});
  try{
    await assert.rejects(()=>apply('after'),/CONNECTION_CLOSED/);
    assert.equal(verifications,0);assert.equal(state.bootstrap,0);assert.equal(state.next,1);
    await apply();assert.equal(state.next,1);assert.equal(state.bootstrap,0);assert.equal(verifications,1);
    assert.equal(state.verified,true);
  }finally{fs.rmSync(repository.root,{recursive:true});}
});

test('busy writer does not adopt another attempt or write admission/failure metadata',async()=>{
  const repository=smallRepository(),state=modelState();state.lock=true;
  try{
    await assert.rejects(()=>modelApply(repository,state),/WRITER_BUSY/);
    assert.equal(state.calls.length,1);assert.equal(state.calls[0],acquireWriterSql);
    assert.equal(state.lock,true);assert.equal(state.bootstrap,0);assert.equal(state.failed,undefined);
  }finally{fs.rmSync(repository.root,{recursive:true});}
});

test('only approved exact 40P01 unit may reconnect/reacquire; unknown failures never retry',async()=>{
  const repository=smallRepository(),state=modelState();state.control=true;state.migrations=[repository.current.migrations[0]];
  let starts=0,retries=0;
  const create=()=>{
    starts++;const writer=modelWriter(state);
    if(starts===1){const original=writer.execute;writer.execute=async sql=>{
      if(sql.includes('create table public.nonidempotent_next')){writer.failure=new ManagedPsqlError('CONNECTION_CLOSED','40P01');throw writer.failure;}
      return original.call(writer,sql);
    };}return writer;
  };
  try{
    await applyManagedRelease({...repository,context:context('UPGRADE'),openSession:create,
      deadlockRetries:file=>file.endsWith('/next.sql')?1:0,delay:async()=>{retries++;},preapply:()=>{},verify:()=>({sha256:'b'.repeat(64)})});
    assert.equal(starts,2);assert.equal(retries,1);assert.equal(state.next,1);
    assert.equal(state.calls.filter(sql=>sql===acquireWriterSql).length,2);
    assert.equal(state.calls.filter(sql=>sql.includes('$managed_admission$')).length,2);
  }finally{fs.rmSync(repository.root,{recursive:true});}
});

test('exact manifest binds verifier bytes and canonical LF, not just file paths',()=>{
  const repository=smallRepository();
  try{
    const file='tests/verifier.sql';fs.mkdirSync(path.join(repository.root,'tests'));fs.writeFileSync(path.join(repository.root,file),'select 1;\r\n');
    repository.release.verificationFiles=[file];
    const first=managedReleaseContext({...repository,context:context()}).manifestHash;
    fs.writeFileSync(path.join(repository.root,file),'select 1;\n');
    assert.equal(managedReleaseContext({...repository,context:context()}).manifestHash,first);
    fs.writeFileSync(path.join(repository.root,file),'select 2;\n');
    assert.notEqual(managedReleaseContext({...repository,context:context()}).manifestHash,first);
  }finally{fs.rmSync(repository.root,{recursive:true});}
});

import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {inventory} from '../scripts/cloudtms-db-release-lib.mjs';
import {digest,scanManagedSql} from '../scripts/cloudtms-managed-release-sql.mjs';
import {assertFreshNativeGate,assertFreshRestoration,compileFullRehearsal,runFullRehearsalWithinOwnedTurn,
 restorationSnapshotSql,activityGuardSql,REHEARSAL_VERSION} from '../scripts/healthroster-install1-full-rehearsal.mjs';
import {executePsqlWithinOwnedTurn,assertInstalledRehearsalContract,reviewedDiagnosticCodes,expectedContractDigestSql} from '../scripts/healthroster-install1-full-rehearsal.mjs';
import {renderFullRehearsalUnits,neutraliseReviewedVerifierEnvelope,closedRehearsalDiagnostic} from '../scripts/healthroster-full-rehearsal-protocol.mjs';
import {effectCensus,potentialCalleeNames} from '../scripts/healthroster-install1-verifier-census.mjs';
const candidatePin='2d405edae701ba905afaa4b91a3abfcf8847106e',base='07e66fb2b2742fbe634a41582f2ad3e004eed3b9';
const marker='hr_install1_'+'a'.repeat(32),hash='b'.repeat(64);
const identity={physicalDatabase:'cloudtms_test_clone',databaseOid:123,backendPid:456,turnId:'hr_turn_'+'c'.repeat(32),key:[20261013,1]};
const gate=()=>({checkedAt:new Date().toISOString(),environment:'TEST',physicalDatabase:'cloudtms_test_clone',identityTest:true,customerKeyNull:true,module:'LEGACY:1',
 candidatePin,pushedCandidatePin:candidatePin,cleanGit:true,sourceStaged:true,remoteTestPin:base,reviewedSqlRuntimeMatch:true,expectedContractHash:hash,sourceBindingSha256:hash,
 bank:{status:'COMPLETE',protectedConclusion:'success',sourcePin:base,sharedUnlockConfirmed:true,recordHash:hash},
 latestRelease:{status:'VERIFIED',commit:base},
 protectedRunsChecked:true,protectedActiveCount:0,applyingCount:0,phoneOrAcceptanceActive:false,
 route:'APPROVED_MACHINE_LOCAL_FALLBACK_PROCESS_ENV',borrowedLabCapability:false,secretsInArgvOrFiles:false,ownerNativeCallbackAuthorised:true});
const before=()=>({physicalDatabase:'cloudtms_test_clone',identityTest:true,customerKeyNull:true,module:'LEGACY:1',markerAbsent:true,applyingCount:0,otherActivityCount:0,
 ledgerFingerprint:hash,catalogueFingerprint:hash,businessCountsFingerprint:hash,businessRowsFingerprint:hash,relationCensus:hash,installed:{migrations:[],repeatables:[]}});
const after=()=>({...before(),connectedFresh:true,marker});

test('fresh gate refuses missing authority, wrong environment/module, concurrent release, dirty/unpushed or stale source',()=>{
 assert.equal(assertFreshNativeGate(gate(),{candidatePin,currentRemoteTestPin:base,expectedContractHash:hash}).module,'LEGACY:1');
 for(const delta of [{environment:'LIVE'},{physicalDatabase:'uofvkfi5'},{customerKeyNull:false},{module:'NEXT:2'},{cleanGit:false},{sourceStaged:false},
  {pushedCandidatePin:'0'.repeat(40)},{remoteTestPin:'0'.repeat(40)},{expectedContractHash:'0'.repeat(64)},{reviewedSqlRuntimeMatch:false},{protectedActiveCount:1},
  {applyingCount:1},{phoneOrAcceptanceActive:true},{borrowedLabCapability:true},{secretsInArgvOrFiles:true},{ownerNativeCallbackAuthorised:false},
  {checkedAt:new Date(Date.now()-16*60*1000).toISOString()},{checkedAt:new Date(Date.now()+60000).toISOString()},
  {bank:{...gate().bank,status:'CANDIDATE'}},{bank:{...gate().bank,sharedUnlockConfirmed:false}},
  {latestRelease:{status:'FAILED',commit:base}},{latestRelease:{status:'VERIFIED',commit:'0'.repeat(40)}}]){
  assert.throws(()=>assertFreshNativeGate({...gate(),...delta},{candidatePin,currentRemoteTestPin:base,expectedContractHash:hash}),/REFUSED|STALE|AUTHORISED/);
 }
});

test('fresh restoration requires all four identical fingerprints and the exact absent marker',()=>{
 assert.equal(assertFreshRestoration(before(),after(),marker).connectedFresh,true);
 for(const delta of [{connectedFresh:false},{customerKeyNull:false},{module:'NEXT:2'},{markerAbsent:false},{marker:'other'},
  {ledgerFingerprint:'d'.repeat(64)},{catalogueFingerprint:'d'.repeat(64)},{businessCountsFingerprint:'d'.repeat(64)},
  {businessRowsFingerprint:'d'.repeat(64)},{relationCensus:'d'.repeat(64)},{applyingCount:1},{otherActivityCount:1}]){
  assert.throws(()=>assertFreshRestoration(before(),{...after(),...delta},marker),/REFUSED|PERSISTED|MISMATCH/);
 }
});

test('source lexer distinguishes a quoted catalogue identity and INSERT column list from an executed queue call',()=>{
 const census=effectCensus("-- public.email_outbox_claim_ready_batch(99)\ninsert into public.mail_outbox(id) values ('public.email_outbox_claim_ready_batch(1)');\ndo $x$ begin perform private.harmless_v1(); end $x$;");
 assert.ok(census.calls.includes('private.harmless_v1'));assert.ok(!census.calls.includes('public.email_outbox_claim_ready_batch'));
 assert.ok(!census.calls.includes('public.mail_outbox'));assert.ok(census.writes.includes('public.mail_outbox'));
});
test('dynamic SQL literal edges survive masking, while catalogue literals alone confer no executed call',()=>{
 const census=effectCensus("-- 'public.must_not_appear()'\ndo $x$ begin execute 'select public.timesheet_qr_send_enqueue_v1($1)'; end $x$;");
 assert.ok(potentialCalleeNames(census).includes('public.timesheet_qr_send_enqueue_v1'));
 assert.ok(!potentialCalleeNames(census).includes('public.must_not_appear'));
 assert.ok(!effectCensus("select 'public.global_drain()'::text;").calls.includes('public.global_drain'));
 assert.deepEqual(potentialCalleeNames(effectCensus("select 'public.global_drain()'::text;")),[]);
});

test('every client fixture switch is cleared before and after its full native verifier savepoint',()=>{
 const sql='\\set BPAY_NEXT_PAIR_DETAIL_PROBE true\n\\if :{?fixture_option}\nselect 1;\n\\endif';
 const rendered=renderFullRehearsalUnits([{file:'supabase/verification/native.sql',kind:'verifier',sql,sqlHash:digest(sql),clientVariables:['extra_option']}]);
 for(const variable of ['BPAY_NEXT_PAIR_DETAIL_PROBE','fixture_option','extra_option','weekly_source_verification_outer_transaction']){
  assert.equal(rendered.sql.split('\\unset '+variable).length-1,2);
 }
 assert.ok(rendered.sql.includes(sql));
 for(const command of ['\\set AUTOCOMMIT on','\\set ECHO all','\\unset SHOW_CONTEXT']){
  assert.throws(()=>renderFullRehearsalUnits([{file:'supabase/verification/native.sql',kind:'verifier',sql:command,sqlHash:digest(command)}]),/OVERRIDE_REFUSED/);
 }
});

test('one outer REPEATABLE READ cannot absorb an incompatible native SERIALIZABLE requirement',()=>{
 const sql='begin isolation level serializable; do $x$ begin perform 1; end $x$; rollback;';
 const unit=neutraliseReviewedVerifierEnvelope({file:'supabase/verification/native.sql',kind:'verifier',sql,sqlHash:digest(sql)},
  {sourceSqlHash:digest(sql),removedCommands:['BEGIN ISOLATION LEVEL SERIALIZABLE','ROLLBACK']});
 assert.throws(()=>renderFullRehearsalUnits([unit],{outerIsolation:'REPEATABLE READ'}),/ISOLATION_REQUIREMENT_REFUSED/);
});

test('fresh activity exclusion has only own query PID and proved same-database turn PID',()=>{
 const sql=activityGuardSql(identity);assert.ok(sql.includes('pid <> pg_backend_pid()'));
 assert.ok(sql.includes('pid = 456 and datid = 123'));assert.ok(!sql.includes('application_name'));
 assert.throws(()=>activityGuardSql({...identity,databaseOid:0}),/REFUSED/);
});

test('snapshot compiles exact repository exporter and fresh relation names without retaining business payloads',()=>{
 const exporter=fs.readFileSync(new URL('../supabase/release/export_contract.sql',import.meta.url),'utf8');
 const sql=restorationSnapshotSql({identity,marker,relations:[{schema:'public',name:'timesheets',kind:'r'}],exportContractSql:exporter});
 assert.ok(sql.includes('businessRowsFingerprint'));assert.ok(sql.includes('row_hash'));assert.ok(!sql.includes('application_name'));
 assert.throws(()=>restorationSnapshotSql({identity,marker,relations:[{schema:'public',name:'timesheets; drop table x',kind:'r'}],exportContractSql:exporter}),/REFUSED/);
 assert.throws(()=>restorationSnapshotSql({identity,marker,relations:[{schema:'public',name:'timesheets',kind:'r'}],exportContractSql:exporter+' select 1;'}),/DRIFT/);
});

test('source-only actual current Install1 plan retains full 142 verifier bodies, 16 exclusions and final browser isolation',()=>{
 const old=process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER;process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER='CURRENT_USER';
 try{
  const current=inventory();
  const review=JSON.parse(fs.readFileSync(new URL('../docs/healthroster-build/install1-rehearsal/VERIFIER_SAFETY_REVIEW.json',import.meta.url),'utf8'));
  // Pure source model, deliberately not represented as an observed TEST ledger.
  const installed={migrations:current.migrations,repeatables:current.repeatables.filter(x=>!x.path.includes('14102026_0044'))
   .map(x=>x.path.includes('15092026_1534_weekly_source_acl_contract_v1')?{...x,sha256:'0'.repeat(64)}:x)};
  const expectedContractHash=digest(JSON.stringify(JSON.parse(fs.readFileSync(new URL('../supabase/release/current-contract.json',import.meta.url),'utf8'))));
  const contractDigest={connectedFresh:true,checkedAt:new Date().toISOString(),physicalDatabase:'cloudtms_test_clone',identityTest:true,
   customerKeyNull:true,expectedContractHash,expectedJsonbHash:hash};
  const plan=compileFullRehearsal({candidatePin,currentRemoteTestPin:base,expectedContractHash,contractDigest,installed,review,marker});
  assert.equal(plan.nativeFull,'NOT_RUN');assert.equal(plan.exclusionCount,16);
  assert.equal(plan.units.filter(x=>x.kind==='verifier').length,142);
  assert.equal(plan.units.at(-1).kind,'final_browser_verifier');assert.equal(plan.outerIsolation,'REPEATABLE READ');
  const tx=scanManagedSql(plan.sql).statements.filter(x=>['BEGIN','COMMIT','ROLLBACK'].includes(x.tokens[0])&&x.tokens[1]!=='TO');
  assert.deepEqual(tx.map(x=>x.tokens.join(' ')),['BEGIN ISOLATION LEVEL REPEATABLE READ','ROLLBACK']);
  assert.equal(plan.lineMap.length,2271);assert.equal(plan.sqlHash,digest(plan.sql));
  // Real psql reports a failing DO at its closing delimiter/semicolon line.
  // These are the exact three locations observed in the held native retry.
  for (const [closing,file,statement,code] of [
    ['end $released_review$;','supabase/verification/09102026_1130_weekly_source_released_review_visibility_v1.sql',60,'WEEKLY_SOURCE_GROUP_CARDINALITY_INVALID'],
    ['$verify_one_head$;','supabase/verification/17092026_0800_weekly_source_workbench_seams_v1.sql',19,'WEEKLY_SOURCE_HEAD_INVENTORY_MISMATCH'],
    ['$verify_fingerprint$;','supabase/verification/17092026_0800_weekly_source_workbench_seams_v1.sql',20,'ASSERTION_FAILED']
  ]) {
    const line=plan.sql.split('\n').findIndex(value=>value.trim()===closing)+1;
    assert.ok(line>0);
    const location=plan.lineMap.find(row=>line>=row.startLine&&line<=row.endLine);
    assert.equal(location.file,file);assert.equal(location.statement,statement);
    const diagnostic=closedRehearsalDiagnostic('psql:<stdin>:'+line+': ERROR: 55000: '+code,plan.lineMap,new Set(plan.diagnosticCodes));
    assert.equal(diagnostic.file,file);assert.equal(diagnostic.statement,statement);
  }

  assert.ok(plan.sql.includes('do $hr_contract_compare$'));
  assert.ok(plan.sql.indexOf('do $hr_contract_compare$')<plan.sql.lastIndexOf('rollback;'));
  assert.ok(plan.diagnosticCodes.includes('WP_PP_FIXTURE_ID_COLLISION'));
  const live=compileFullRehearsal({candidatePin,currentRemoteTestPin:base,expectedContractHash,contractDigest,installed,review,marker,observeLiveBusiness:true});
  assert.equal(live.fixtureObservationUnits.length,143);
  assert.deepEqual(live.units.map(r=>({file:r.file,sqlHash:r.sqlHash})),plan.units.map(r=>({file:r.file,sqlHash:r.sqlHash})));
  assert.equal(live.lineMap.filter(r=>r.phase==='fixture_observation').length,429);
  assert.equal(live.sql.split('HR_REHEARSAL_TRANSACTION_JSON ').length,2);
  assert.equal(live.sql.split('HR_FIXTURE_OBSERVATION_JSON ').length,144);
  assert.deepEqual(scanManagedSql(live.sql).statements.filter(x=>['BEGIN','COMMIT','ROLLBACK'].includes(x.tokens[0])&&x.tokens[1]!=='TO').map(x=>x.tokens.join(' ')),['BEGIN ISOLATION LEVEL REPEATABLE READ','ROLLBACK']);
 }finally{if(old===undefined)delete process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER;else process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER=old;}
});

function model({executionDelta={},beforeDelta={},afterDelta={},publishFailure=false,executionFailure=null}={}){
 const events=[],receipts=[],controller=new AbortController();
 const turn={identity,signal:controller.signal,assertHeld:async phase=>events.push('held:'+phase)};
 const callbacks={freshGate:async phase=>{events.push('gate:'+phase);return gate();},before:async()=>{events.push('before');return {...before(),...beforeDelta};},
  executeSameSession:async args=>{events.push('execute');assert.equal(args.noHarnessDeadline,true);assert.equal(args.settings.statementTimeout,0);
   if(executionFailure)throw executionFailure;
   return {sameSession:true,explicitRollback:true,exitCode:0,errorCollectionComplete:true,errors:[],installedContract:{matched:true,sha256:hash},...executionDelta};},
  freshAfter:async()=>{events.push('fresh');return {...after(),...afterDelta};},
  recordReceipt:async receipt=>{receipts.push(receipt);events.push('record:'+receipt.status);},
  publishManaged:async args=>{events.push('publish');assert.equal(args.turn,turn);if(publishFailure)throw Error('PROTECTED_FAILED');return {status:'ROOT_MANAGED_PUBLISHED'};}};
 return {events,receipts,controller,options:{turn,candidatePin,currentRemoteTestPin:base,expectedContractHash:hash,marker,review:{},callbacks,
  compilePlan:()=>({version:REHEARSAL_VERSION,sql:'begin; rollback;',sqlHash:hash,lineMap:[],currentCounts:{},pendingCounts:{},exclusions:[],expectedContractHash:hash,diagnosticCodes:[],sourceBinding:{sha256:hash}})}};
}

test('one supplied retained turn spans before/full/explicit rollback/fresh and canonical root publication callback',async()=>{
 const m=model();const out=await runFullRehearsalWithinOwnedTurn(m.options);
 assert.equal(out.receipt.status,'FULL_CLEAN_ROLLBACK_PROVED');assert.equal(out.receipt.turnId,identity.turnId);
 assert.ok(m.events.indexOf('before')<m.events.indexOf('execute'));assert.ok(m.events.indexOf('execute')<m.events.indexOf('fresh'));
 assert.ok(m.events.indexOf('fresh')<m.events.indexOf('publish'));assert.equal(m.events.filter(x=>x==='publish').length,1);
 assert.equal(m.events.at(-1),'held:install1-after-managed-release');
});

test('live observation admits active background sessions before SQL but requires genuine restoration before publication',async()=>{
 for(const otherActivityCount of [0,1,17]){
  const m=model({beforeDelta:{otherActivityCount,observeLiveBusiness:true}});
  const compiled=m.options.compilePlan;
  m.options.compilePlan=input=>{assert.equal(input.observeLiveBusiness,true);return {...compiled(),observeLiveBusiness:true};};
  const execute=m.options.callbacks.executeSameSession;
  m.options.callbacks.executeSameSession=input=>{assert.equal(input.observeLiveBusiness,true);return execute(input);};
  await assert.rejects(runFullRehearsalWithinOwnedTurn(m.options),/LIVE_RESTORATION_REQUIRED/);
  assert.ok(m.events.includes('execute'));assert.ok(m.events.includes('fresh'));
  assert.ok(m.receipts.some(r=>r.status==='RESTORATION_FAILED_HOLD_INSTALL'));assert.ok(!m.events.includes('publish'));
 }
});

test('before activity without complete observation and malformed activity counts fail before SQL',async()=>{
 for(const beforeDelta of [{otherActivityCount:1},{otherActivityCount:-1,observeLiveBusiness:true},
  {otherActivityCount:1.5,observeLiveBusiness:true},{otherActivityCount:'1',observeLiveBusiness:true},
  {applyingCount:1,otherActivityCount:0,observeLiveBusiness:true}]){
  const m=model({beforeDelta});await assert.rejects(runFullRehearsalWithinOwnedTurn(m.options),/BEFORE_REFUSED/);
  assert.ok(!m.events.includes('execute'));assert.ok(!m.events.includes('publish'));
 }
});

test('SQL errors or missing explicit rollback always get fresh proof and hold publication',async()=>{
 for(const executionDelta of [{errors:[{sqlstate:'23514'}]},{explicitRollback:false},{errorCollectionComplete:false},{exitCode:1}]){
  const m=model({executionDelta});await assert.rejects(runFullRehearsalWithinOwnedTurn(m.options),/HOLD_INSTALL|INCOMPLETE/);
  assert.ok(m.events.includes('fresh'));assert.ok(m.events.some(x=>x.startsWith('record:')));assert.ok(!m.events.includes('publish'));
 }
});

test('interrupted or incomplete execution and collected SQL errors never receive an intermediate SQL-pass receipt',async()=>{
 const thrown=model({executionFailure:Error('TURN_CONNECTION_CLOSED')});
 const originalHeld=thrown.options.turn.assertHeld;
 thrown.options.turn.assertHeld=async phase=>{if(phase==='install1-fresh')throw Error('TURN_CONNECTION_CLOSED');return originalHeld(phase);};
 await assert.rejects(runFullRehearsalWithinOwnedTurn(thrown.options),/TURN_CONNECTION_CLOSED/);
 assert.equal(thrown.receipts.length,1);assert.equal(thrown.receipts[0].status,'EXECUTION_INCOMPLETE_RESTORATION_UNVERIFIED');
 assert.equal(thrown.receipts[0].execution,null);assert.equal(thrown.receipts[0].restorationProved,false);
 assert.ok(!thrown.events.includes('fresh'));assert.ok(!thrown.events.includes('publish'));
 for(const [delta,status] of [[{explicitRollback:false},'EXECUTION_INCOMPLETE_RESTORATION_UNVERIFIED'],[{errors:[{sqlstate:'23514'}]},'SQL_ERRORS_FINISHED_RESTORATION_UNVERIFIED']]){
  const m=model({executionDelta:delta});await assert.rejects(runFullRehearsalWithinOwnedTurn(m.options),/INCOMPLETE|HOLD_INSTALL/);
  assert.equal(m.receipts[0].status,status);assert.ok(m.receipts.every(r=>r.status!=='SQL_PASS_FINISHED_RESTORATION_UNVERIFIED'));
  assert.ok(m.events.includes('fresh'));assert.ok(!m.events.includes('publish'));
 }
 const clean=model();await runFullRehearsalWithinOwnedTurn(clean.options);
 assert.equal(clean.receipts[0].status,'SQL_PASS_FINISHED_RESTORATION_UNVERIFIED');
 assert.equal(clean.receipts.at(-1).status,'FULL_CLEAN_ROLLBACK_PROVED');
});

test('restoration mismatch and lost turn prevent publication, with no duplicate dispatch on protected failure',async()=>{
 const m=model({afterDelta:{businessRowsFingerprint:'d'.repeat(64)}});
 await assert.rejects(runFullRehearsalWithinOwnedTurn(m.options),/MISMATCH/);assert.ok(!m.events.includes('publish'));
 const lost=model();lost.controller.abort(Error('TURN_LOST'));await assert.rejects(runFullRehearsalWithinOwnedTurn(lost.options),/REQUIRED/);
 assert.equal(lost.events.length,0);
 const failed=model({publishFailure:true});await assert.rejects(runFullRehearsalWithinOwnedTurn(failed.options),/PROTECTED_FAILED/);
 assert.equal(failed.events.filter(x=>x==='publish').length,1);
});

test('root psql adapter sends synthetic credentials only through environment and returns closed complete diagnostics',async()=>{
 const sql='\\echo HR_FILE_BEGIN 0\nselect 1;\n\\echo HR_FILE_END 0\nrollback;\n\\echo HR_EXPLICIT_ROLLBACK_COMPLETED\n';
 let called=0;
 const turn={identity,runOwnedChild:async args=>{
  called++;assert.ok(!args.args.some(x=>x.includes('synthetic-password')));assert.equal(args.options.env.PGPASSWORD,'synthetic-password');
  assert.deepEqual(args.args,['-X','-q','-A','-t','-f','-']);
  assert.ok(!Object.hasOwn(args.options,'timeout'));assert.equal(args.cancelOnLoss,true);
  return {code:0,stdout:'HR_FILE_BEGIN 0\nprivate fixture payload\nHR_FILE_END 0\nHR_EXPLICIT_ROLLBACK_COMPLETED\n',
   stderr:'psql:<stdin>:2: ERROR: 23505: confidential duplicate\nDETAIL: confidential detail\nCONTEXT: confidential query\n'};
 }};
 const result=await executePsqlWithinOwnedTurn({turn,psqlPath:'C:/Program Files/PostgreSQL/18/bin/psql.exe',
  env:{PGDATABASE:'cloudtms_test_clone',PGHOST:'synthetic-host',PGUSER:'synthetic-user',PGPASSWORD:'synthetic-password'},
  sql,sqlHash:digest(sql),marker,expectedContractHash:hash,diagnosticCodesHash:digest('[]'),lineMap:[{startLine:2,endLine:2,file:'supabase/verification/native.sql',kind:'verifier',statement:1,object:'private.source_owner'}]});
 assert.equal(called,1);assert.equal(result.errorCollectionComplete,true);assert.equal(result.explicitRollback,true);
 assert.equal(result.errors[0].sqlstate,'23505');assert.equal(result.errors[0].error,'DATABASE_DIAGNOSTIC_WITHHELD');
 assert.ok(!JSON.stringify(result).includes('confidential'));assert.ok(!JSON.stringify(result).includes('private fixture payload'));
});

test('root psql adapter refuses missing unit end markers and unmapped database diagnostics',async()=>{
 const sql='\\echo HR_FILE_BEGIN 0\nselect 1;\n\\echo HR_FILE_END 0\nrollback;\n\\echo HR_EXPLICIT_ROLLBACK_COMPLETED\n';
 const turn={identity,runOwnedChild:async()=>({code:0,stdout:'HR_FILE_BEGIN 0\nHR_EXPLICIT_ROLLBACK_COMPLETED\n',stderr:'ERROR: 23505: a server error with sensitive data\n'})};
 const result=await executePsqlWithinOwnedTurn({turn,psqlPath:'C:/Program Files/PostgreSQL/18/bin/psql.exe',
  env:{PGDATABASE:'cloudtms_test_clone',PGHOST:'synthetic-host',PGUSER:'synthetic-user',PGPASSWORD:'synthetic-password'},sql,sqlHash:digest(sql),marker,expectedContractHash:hash,diagnosticCodesHash:digest('[]'),lineMap:[]});
 assert.equal(result.errorCollectionComplete,false);assert.equal(result.unexpectedDiagnostic,'UNMAPPED_DATABASE_ERROR');
 assert.deepEqual(result.unmappedDiagnostics,[{sqlstate:'23505',error:'DATABASE_DIAGNOSTIC_LOCATION_UNPROVED'}]);
 assert.ok(!JSON.stringify(result).includes('sensitive'));
});

test('psql client errors are mapped and held even when all unit and rollback markers are present',async()=>{
 const sql='\\echo HR_FILE_BEGIN 0\n\\invalid_native_probe\n\\echo HR_FILE_END 0\nrollback;\n\\echo HR_EXPLICIT_ROLLBACK_COMPLETED\n';
 const turn={identity,runOwnedChild:async()=>({code:0,stdout:'HR_FILE_BEGIN 0\nHR_FILE_END 0\nHR_EXPLICIT_ROLLBACK_COMPLETED\n',stderr:'psql:<stdin>:2: error: raw private command diagnostic\n'})};
 const result=await executePsqlWithinOwnedTurn({turn,psqlPath:'C:/Program Files/PostgreSQL/18/bin/psql.exe',env:{PGDATABASE:'cloudtms_test_clone',PGHOST:'synthetic-host',PGUSER:'synthetic-user',PGPASSWORD:'synthetic-password'},sql,sqlHash:digest(sql),marker,expectedContractHash:hash,diagnosticCodesHash:digest('[]'),lineMap:[{startLine:2,endLine:2,file:'HARNESS_CLIENT_PROBE',kind:'control',statement:1,object:'PSQL_META'}]});
 assert.equal(result.errorCollectionComplete,true);assert.equal(result.explicitRollback,true);
 assert.deepEqual(result.errors,[{file:'HARNESS_CLIENT_PROBE',kind:'control',statement:1,object:'PSQL_META',line:2,sqlstate:null,error:'PSQL_CLIENT_ERROR'}]);
 assert.ok(!JSON.stringify(result).includes('raw private'));
});

const markerRelation=()=>({acl:['DELETE','INSERT','MAINTAIN','REFERENCES','SELECT','TRIGGER','TRUNCATE','UPDATE']
 .map(privilege=>({grantee:'postgres',grantable:false,privilege})),rls:false,kind:'r',name:marker,owner:'postgres',schema:'private',
 columns:[{name:'marker',type:'boolean',default:null,identity:'',not_null:true,generated:''}],indexes:[],options:[],force_rls:false,
 constraints:[],view_definition_sha256:null});
test('in-session contract strips only the exact checked marker while every security/catalogue section remains sealed',()=>{
 const contract={relations:[{schema:'private',name:'hr_install1_other',kind:'r'}],triggers:[],policies:[],routines:[],schemas:[]};
 const expectedContractHash=digest(JSON.stringify(contract)),actual={...contract,relations:[...contract.relations,markerRelation()]};
 assert.equal(assertInstalledRehearsalContract(actual,{expectedContractHash,marker}).sha256,expectedContractHash);
 for(const changed of [
  {...actual,relations:[...contract.relations,{...markerRelation(),rls:true}]},
  {...actual,relations:[...contract.relations,{...markerRelation(),columns:[]}]},
  {...actual,relations:[...contract.relations,{...markerRelation(),indexes:[{name:'unexpected'}]}]},
  {...actual,relations:[...contract.relations,{...markerRelation(),constraints:[{type:'c'}]}]},
  {...actual,relations:[...contract.relations,{...markerRelation(),acl:[...markerRelation().acl,{grantee:'service_role',privilege:'SELECT',grantable:false}]}]},
  {...actual,triggers:[{schema:'private',table:marker}]},
  {...actual,policies:[{schema:'private',table:marker}]},
  {...actual,relations:[markerRelation()]},
  {...actual,routines:[{identity:'unexpected()'}]},
 ])assert.throws(()=>assertInstalledRehearsalContract(changed,{expectedContractHash,marker}),/REHEARSAL_/);
});
test('closed exception codes are derived only from hash-bound literal source messages',()=>{
 const file='supabase/verification/13102026_1126_weekly_source_protected_pay_authority_verification.sql';
 const source=fs.readFileSync(new URL('../'+file,import.meta.url),'utf8').replaceAll('\r\n','\n');
 const codes=reviewedDiagnosticCodes({sourceFiles:[{file,sha256:digest(source)}]});
 assert.ok(codes.length>0);assert.ok(codes.every(code=>/^[A-Z][A-Z0-9_]{5,}$/.test(code)));
 assert.throws(()=>reviewedDiagnosticCodes({sourceFiles:[{file,sha256:'0'.repeat(64)}]}),/DRIFT/);
 const diag=closedRehearsalDiagnostic('psql:<stdin>:4: ERROR: P0001: '+codes[0]+': confidential row',[],new Set(codes));
 assert.equal(diag.error,codes[0]);assert.ok(!JSON.stringify(diag).includes('confidential'));
});
test('expected contract digest is derived by read-only PostgreSQL jsonb conversion of the exact source contract',()=>{
 const contract=JSON.parse(fs.readFileSync(new URL('../supabase/release/current-contract.json',import.meta.url),'utf8'));
 const sql=expectedContractDigestSql({expectedContractHash:digest(JSON.stringify(contract))});
 assert.equal(scanManagedSql(sql).statements.length,1);assert.equal(scanManagedSql(sql).statements[0].tokens[0],'SELECT');
 assert.ok(sql.includes('::jsonb::text'));assert.ok(sql.includes('customer_key is null'));
 assert.throws(()=>expectedContractDigestSql({expectedContractHash:'0'.repeat(64)}),/DRIFT/);
});

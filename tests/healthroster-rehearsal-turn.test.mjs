import test from 'node:test';
import assert from 'node:assert/strict';
import {withSharedRehearsalTurn,TURN_KEY,TURN_OWNERSHIP_SQL} from '../scripts/healthroster-rehearsal-turn.mjs';
const ownership={physicalDatabase:'cloudtms_test_clone',databaseOid:42,backendPid:71,testIdentity:true,legacyControl:true,held:true};

test('SEND7 two-int32 key checks its own namespace and refuses a one-int64 lookalike',async()=>{
 for(const namespace of [2,1]){
  const {deps,events}=fixture([['t'],['t'],['t']]);let worked=false;
  const open=deps.openTurn;
  deps.openTurn=()=>{const connection=open(),query=connection.query;
   connection.query=async sql=>{
    if(sql===TURN_OWNERSHIP_SQL){
     // PostgreSQL 17 SET_LOCKTAG_INT32 uses field4/objsubid 2; INT64 uses 1.
     const tag=Number(sql.match(/objsubid\s*=\s*(\d+)/)?.[1]);
     assert.match(sql,/classid=20261013 and objid=1/);
     return [JSON.stringify({...ownership,held:tag===namespace})];
    }
    return query(sql);
   };return connection;
  };
  const run=withSharedRehearsalTurn({task:'namespace',async work(){worked=true;return {status:'proved'};}},deps);
  if(namespace===2){assert.equal((await run).status,'proved');assert.equal(worked,true);}
  else{await assert.rejects(run,/TURN_OWNERSHIP_LOST/);assert.equal(worked,false);assert.ok(events.includes('close'));}
 }
});
function fixture(answers){const events=[],loss=new AbortController();return {events,loss,deps:{checkProtectedRelease(){events.push('releaseCheck');},openTurn(){events.push('open');return {signal:loss.signal,async query(sql){events.push(sql);return sql===TURN_OWNERSHIP_SQL?[JSON.stringify(ownership)]:answers.shift();},async close(){events.push('close');}};},log(row){events.push(row.status);}}};}
test('busy turn runs no identity, fingerprint or trial and closes only its own connection',async()=>{const {events,deps}=fixture([['f']]);const result=await withSharedRehearsalTurn({task:'test',work(){assert.fail('must not run');}},deps);assert.equal(result.status,'BUSY_NO_TRIAL');assert.deepEqual(events,['releaseCheck','open','select pg_try_advisory_lock(20261013, 1);','BUSY_NO_TRIAL','close']);});
test('separate turn survives before, trial, rollback and fresh after, then unlocks and closes',async()=>{const {events,deps}=fixture([['t'],['t'],['t']]);const result=await withSharedRehearsalTurn({task:'test',async work(){events.push('before','trial','rollback','freshAfter');return {status:'proved'};}},deps);assert.equal(result.status,'proved');const before=events.indexOf('before'),after=events.indexOf('freshAfter'),unlock=events.indexOf('select pg_advisory_unlock(20261013, 1);');assert.ok(events.indexOf('TAKEN')<before&&after<unlock&&unlock<events.indexOf('close'));assert.equal(events.at(-1),'RELEASED');assert.deepEqual(TURN_KEY,[20261013,1]);});
test('trial failure still unlocks and closes before propagating its error',async()=>{const {events,deps}=fixture([['t'],['t'],['t']]);await assert.rejects(withSharedRehearsalTurn({task:'test',work(){throw Error('source failure');}},deps),/source failure/);assert.ok(events.includes('select pg_advisory_unlock(20261013, 1);'));assert.equal(events.at(-2),'close');assert.equal(events.at(-1),'RELEASED');});
test('a successful trial cannot return proved when explicit unlock reports false',async()=>{
 const {events,deps}=fixture([['t'],['t'],['f']]);const records=[];
 deps.log=row=>{records.push(row);events.push(row.status);};
 await assert.rejects(withSharedRehearsalTurn({task:'test',work(){return {status:'proved'};}},deps),/TURN_UNLOCK_CONFIRMATION_FAILED/);
 assert.equal(events.at(-2),'close');assert.equal(events.at(-1),'RELEASED');
 assert.equal(records.at(-1).explicitUnlockConfirmed,false);assert.equal(records.at(-1).connectionClosed,true);
});
test('wrong physical or release identity refuses work and releases acquired turn',async()=>{const {events,deps}=fixture([['t'],['f'],['t']]);await assert.rejects(withSharedRehearsalTurn({task:'test',work(){assert.fail('must not run');}},deps),/TURN_TEST_IDENTITY_OR_RELEASE_REFUSED/);assert.equal(events.at(-2),'close');assert.equal(events.at(-1),'RELEASED');});
test('active protected release opens no database connection',async()=>{const {events,deps}=fixture([]);deps.checkProtectedRelease=()=>{throw Error('PROTECTED_RELEASE_RUNNING_HOLD');};await assert.rejects(withSharedRehearsalTurn({task:'test',work(){assert.fail('must not run');}},deps),/PROTECTED_RELEASE_RUNNING_HOLD/);assert.deepEqual(events,[]);});
test('persisted busy cooldown refuses an early retry before release check or connection',async()=>{
 const {events,deps}=fixture([]);deps.now=()=>1000000;deps.readLastBusy=()=>900000;
 const result=await withSharedRehearsalTurn({task:'test',work(){assert.fail('must not run');}},deps);
 assert.equal(result.status,'COOLDOWN_NO_CONNECTION');assert.equal(result.nextEligibleAt,new Date(1200000).toISOString());assert.deepEqual(events,[]);
});
test('exact five-minute boundary permits a fresh protected check and non-waiting lock attempt',async()=>{
 const {events,deps}=fixture([['f']]);deps.now=()=>1200000;deps.readLastBusy=()=>900000;
 const result=await withSharedRehearsalTurn({task:'test',work(){assert.fail('must not run');}},deps);
 assert.equal(result.status,'BUSY_NO_TRIAL');assert.equal(events[0],'releaseCheck');assert.equal(events.at(-1),'close');
});
test('actual PID and database OID are exposed as the only activity exemption',async()=>{
 const {deps}=fixture([['t'],['t'],['t']]);
 await withSharedRehearsalTurn({task:'test',async work(turn){assert.equal(turn.identity.backendPid,71);assert.equal(turn.identity.databaseOid,42);
  assert.equal(turn.activityExclusionSql,'pid <> pg_backend_pid() and not (pid = 71 and datid = 42)');await turn.assertHeld('before');}},deps);
});
test('loss while callback is active signals work immediately and cannot produce clean receipt',async()=>{
 const {deps,loss}=fixture([['t'],['t'],['t']]);const records=[];deps.log=record=>records.push(record);
 await assert.rejects(withSharedRehearsalTurn({task:'test',async work(turn){loss.abort(Error('own connection closed'));
  assert.equal(turn.signal.aborted,true);await assert.rejects(turn.assertHeld('publication'),/own connection closed/);return {status:'proved'};}},deps),/own connection closed/);
 assert.equal(records.some(row=>row.status==='LOST_INCOMPLETE'),true);assert.equal(records.at(-1).proved,false);
});
test('ownership drift in exact pg_locks probe refuses all later work',async()=>{
 const {deps}=fixture([['t'],['t'],['t']]);const open=deps.openTurn;let probes=0;
 deps.openTurn=()=>{const connection=open(),query=connection.query;connection.query=sql=>sql===TURN_OWNERSHIP_SQL
  ?Promise.resolve([JSON.stringify({...ownership,backendPid:++probes===1?71:72})]):query(sql);return connection;};
 await assert.rejects(withSharedRehearsalTurn({task:'test',work:turn=>turn.assertHeld('before-full-sql')},deps),/TURN_OWNERSHIP_LOST/);
});
test('source failure remains primary when explicit unlock also fails',async()=>{
 const {deps}=fixture([['t'],['t'],['f']]);
 await assert.rejects(withSharedRehearsalTurn({task:'test',work(){throw Error('original source failure');}},deps),error=>error.message==='original source failure'&&error.turnUnlockConfirmed===false);
});
for(const phase of ['before-fingerprint','full-rehearsal','rollback','fresh-proof','protected-wait','runtime','office']){
 test('connection loss at '+phase+' stops later phases and cannot return proved',async()=>{
  const {deps,loss}=fixture([['t'],['t'],['t']]),seen=[];
  await assert.rejects(withSharedRehearsalTurn({task:'test',async work(turn){
   for(const current of ['before-fingerprint','full-rehearsal','rollback','fresh-proof','protected-wait','runtime','office']){
    if(current===phase)loss.abort(Error('lost at '+phase));await turn.assertHeld(current);seen.push(current);
   }
   return {status:'proved'};
  }},deps),new RegExp('lost at '+phase));assert.equal(seen.includes(phase),false);
 });
}
test('periodic monitor signals an active callback when actual lock ownership disappears',async()=>{
 const {deps}=fixture([['t'],['t'],['t']]),open=deps.openTurn;let probes=0;
 deps.openTurn=()=>{const connection=open(),query=connection.query;connection.query=sql=>sql===TURN_OWNERSHIP_SQL
  ?Promise.resolve([JSON.stringify({...ownership,held:++probes<3})]):query(sql);return connection;};
 await assert.rejects(withSharedRehearsalTurn({task:'test',monitorMs:1,async work(turn){
  await new Promise(resolve=>turn.signal.addEventListener('abort',resolve,{once:true}));return {status:'proved'};
 }},deps),/TURN_OWNERSHIP_LOST/);assert.ok(probes>=3);
});
test('failed loss logging stays contained and preserves the primary loss while own cleanup completes',async()=>{
 const {deps,loss,events}=fixture([['t'],['t'],['t']]);
 deps.log=record=>{if(record.status!=='TAKEN')throw Error('private raw write failure');};
 await assert.rejects(withSharedRehearsalTurn({task:'test',async work(turn){
  assert.doesNotThrow(()=>loss.abort(Error('primary own connection loss')));
  await turn.assertHeld('must hold');
 }},deps),/primary own connection loss/);
 assert.ok(events.includes('close'));
});
test('failed initial journal prevents callback and still unlocks and closes only its own turn',async()=>{
 const {deps,events}=fixture([['t'],['t']]);deps.log=()=>{throw Error('private raw write failure');};
 await assert.rejects(withSharedRehearsalTurn({task:'test',work(){assert.fail('must not run');}},deps),/TURN_JOURNAL_WRITE_FAILED/);
 assert.ok(events.includes('select pg_advisory_unlock(20261013, 1);'));assert.ok(events.includes('close'));
});

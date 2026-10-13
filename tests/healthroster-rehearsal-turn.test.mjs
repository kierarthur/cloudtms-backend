import test from 'node:test';
import assert from 'node:assert/strict';
import {withSharedRehearsalTurn,TURN_KEY} from '../scripts/healthroster-rehearsal-turn.mjs';
function fixture(answers){const events=[];return {events,deps:{checkProtectedRelease(){events.push('releaseCheck');},openTurn(){events.push('open');return {async query(sql){events.push(sql);return answers.shift();},async close(){events.push('close');}};},log(row){events.push(row.status);}}};}
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

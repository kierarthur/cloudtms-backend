import test from 'node:test';
import assert from 'node:assert/strict';
import {EventEmitter} from 'node:events';
import {serveTurnControl,connectTurnControl,runOwnedTurnChild,ownTurnActivityExclusionSql,abortableDelay} from '../scripts/healthroster-turn-control.mjs';
const identity={physicalDatabase:'cloudtms_test_clone',databaseOid:42,backendPid:71,turnId:'hr_turn_'+'a'.repeat(32),key:[20261013,1]};

test('live capability-bound local control carries exact identity and loss while child work is active',async()=>{
 const controller=new AbortController(),phases=[];
 const host=await serveTurnControl({identity,signal:controller.signal,async assertHeld(phase){phases.push(phase);if(controller.signal.aborted)throw controller.signal.reason;}});
 let client;
 try {
  client=await connectTurnControl(host.environment);assert.deepEqual(client.identity,identity);
  await client.assertHeld('before-runtime');assert.deepEqual(phases,['before-runtime']);
  const stopped=abortableDelay(60000,client.signal);controller.abort(Error('test loss'));
  await assert.rejects(stopped,/SHARED_TURN_LOST/);await assert.rejects(client.assertHeld('office'),/SHARED_TURN_LOST/);
 }finally{client?.close();await host.close();}
});
test('incorrect capability cannot provide a trusted historical turn identity',async()=>{
 const controller=new AbortController(),host=await serveTurnControl({identity,signal:controller.signal,assertHeld:async()=>{}});
 try {await assert.rejects(connectTurnControl({...host.environment,CLOUDTMS_SHARED_TURN_CAPABILITY:'wrong'}),/SHARED_TURN_CONTROL_CLOSED/);}finally{await host.close();}
});
test('only the exact spawned local rehearsal child is cancelled on loss',async()=>{
 const controller=new AbortController(),owned=new EventEmitter(),other=new EventEmitter(),kills=[];
 owned.stdout=new EventEmitter();owned.stderr=new EventEmitter();owned.kill=signal=>{kills.push(['owned',signal]);setImmediate(()=>owned.emit('close',null,signal));};
 other.kill=()=>assert.fail('other session/process must not be cancelled');
 let checks=0;const task=runOwnedTurnChild({file:'fake',signal:controller.signal,assertHeld:async()=>{checks++;},spawnChild:()=>owned});
 await new Promise(resolve=>setImmediate(resolve));controller.abort(Error('lock lost'));
 await assert.rejects(task,/lock lost/);assert.deepEqual(kills,[['owned','SIGTERM']]);assert.equal(checks,1);
});
test('loss before child starts opens no child',async()=>{
 const controller=new AbortController();controller.abort(Error('already lost'));
 await assert.rejects(runOwnedTurnChild({file:'fake',signal:controller.signal,assertHeld:async()=>{},spawnChild:()=>assert.fail('must not spawn')}),/already lost/);
});
test('healthy long child has no harness deadline and checks ownership after completion',async()=>{
 const controller=new AbortController(),owned=new EventEmitter(),phases=[];
 owned.stdout=new EventEmitter();owned.stderr=new EventEmitter();owned.kill=()=>assert.fail('healthy child must not be stopped');
 const task=runOwnedTurnChild({file:'fake',signal:controller.signal,assertHeld:async phase=>phases.push(phase),spawnChild:()=>owned});
 await new Promise(resolve=>setImmediate(resolve));owned.emit('close',0,null);
 assert.equal((await task).code,0);assert.deepEqual(phases,['before-owned-child','after-owned-child']);
});
test('activity guard rejects fabricated identity and never masks another application name or PID',()=>{
 assert.equal(ownTurnActivityExclusionSql(identity),'pid <> pg_backend_pid() and not (pid = 71 and datid = 42)');
 for(const patch of [{physicalDatabase:'other'},{databaseOid:0},{backendPid:'71'},{turnId:'historical'}])assert.throws(()=>ownTurnActivityExclusionSql({...identity,...patch}),/IDENTITY_REFUSED/);
});

test('broken owned SQL stdin rejects even if that exact child subsequently exits zero',async()=>{
 const controller=new AbortController(),owned=new EventEmitter(),kills=[],phases=[];
 owned.stdout=new EventEmitter();owned.stderr=new EventEmitter();owned.stdin=new EventEmitter();
 owned.kill=signal=>kills.push(signal);
 const task=runOwnedTurnChild({file:'fake',signal:controller.signal,assertHeld:async phase=>phases.push(phase),spawnChild:()=>owned});
 await new Promise(resolve=>setImmediate(resolve));owned.stdin.emit('error',Error('synthetic EPIPE'));owned.emit('close',0,null);
 await assert.rejects(task,/OWNED_CHILD_INPUT_FAILED/);assert.deepEqual(kills,['SIGTERM']);assert.deepEqual(phases,['before-owned-child']);
});

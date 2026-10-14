import net from 'node:net';
import crypto from 'node:crypto';
import os from 'node:os';
import path from 'node:path';
import {spawn} from 'node:child_process';

export function closedTurnError(code='SHARED_TURN_LOST') { return Object.assign(Error(code),{code}); }
export function assertTurnIdentity(identity) {
 if(identity?.physicalDatabase!=='cloudtms_test_clone'||!Number.isSafeInteger(identity.databaseOid)||identity.databaseOid<1
  ||!Number.isSafeInteger(identity.backendPid)||identity.backendPid<1||!/^hr_turn_[a-f0-9]{32}$/.test(identity.turnId??'')
  ||JSON.stringify(identity.key)!=='[20261013,1]')throw closedTurnError('SHARED_TURN_IDENTITY_REFUSED');
 return identity;
}

// Exclude ONLY the proved same-database turn PID and the activity query itself.
// An application_name match or caller-supplied extra PID is never an exemption.
export function ownTurnActivityExclusionSql(identity) {
 assertTurnIdentity(identity);
 return `pid <> pg_backend_pid() and not (pid = ${identity.backendPid} and datid = ${identity.databaseOid})`;
}

export function abortableDelay(ms,signal) {
 if(signal?.aborted)return Promise.reject(signal.reason);
 return new Promise((resolve,reject)=>{
  const done=()=>{clearTimeout(timer);signal?.removeEventListener('abort',abort);resolve();};
  const abort=()=>{clearTimeout(timer);signal?.removeEventListener('abort',abort);reject(signal.reason);};
  const timer=setTimeout(done,ms);signal?.addEventListener('abort',abort,{once:true});
 });
}

// No wall-clock limit on full SQL. Cancellation is exclusively a loss signal
// applied to the exact child object created here; no PID search or remote cancel.
export async function runOwnedTurnChild({file,args=[],options={},input,signal,assertHeld,cancelOnLoss=true,spawnChild=spawn}) {
 await assertHeld('before-owned-child');
 if(signal.aborted)throw signal.reason;
 const child=spawnChild(file,args,{...options,windowsHide:true});
 return await new Promise((resolve,reject)=>{
  let stdout='',stderr='',killTimer,inputError,observerError;
  const observe=(callback,chunk)=>{
   if(observerError||!callback)return;
   try{callback(chunk);}catch{
    observerError=closedTurnError('OWNED_CHILD_OBSERVER_FAILED');
    // A failed rollback observer closes only its own child. A dispatched
    // durable release is allowed to finish, then held for reconciliation.
    if(cancelOnLoss){child.kill('SIGTERM');killTimer=setTimeout(()=>child.kill('SIGKILL'),5000);killTimer.unref?.();}
   }
  };
  const abort=()=>{if(cancelOnLoss){child.kill('SIGTERM');killTimer=setTimeout(()=>child.kill('SIGKILL'),5000);killTimer.unref?.();}};
  child.stdout?.on('data',chunk=>{stdout+=chunk.toString();observe(options.onStdout,chunk);});
  child.stderr?.on('data',chunk=>{stderr+=chunk.toString();observe(options.onStderr,chunk);});
  child.stdin?.on('error',()=>{
   inputError=closedTurnError('OWNED_CHILD_INPUT_FAILED');
   child.kill('SIGTERM');killTimer=setTimeout(()=>child.kill('SIGKILL'),5000);killTimer.unref?.();
  });
  signal.addEventListener('abort',abort,{once:true});
  child.once('error',()=>{signal.removeEventListener('abort',abort);clearTimeout(killTimer);reject(closedTurnError('OWNED_CHILD_START_FAILED'));});
  child.once('close',async(code,childSignal)=>{
   signal.removeEventListener('abort',abort);clearTimeout(killTimer);
   if(signal.aborted){reject(signal.reason);return;}
   if(inputError){reject(inputError);return;}
   if(observerError){reject(observerError);return;}
   try {await assertHeld('after-owned-child');resolve({code,signal:childSignal,stdout,stderr});}catch(error){reject(error);}
  });
  if(input!==undefined){if(typeof input!=='string'||!child.stdin){child.kill('SIGTERM');reject(closedTurnError('OWNED_CHILD_INPUT_REFUSED'));return;}child.stdin.end(input);}
  if(signal.aborted)abort();
 });
}

// A per-process capability and a local pipe bind the canonical npm coordinator
// to this still-open turn. Neither is a saved proof or database credential.
export async function serveTurnControl({identity,signal,assertHeld}) {
 assertTurnIdentity(identity);
 const capability=crypto.randomBytes(32).toString('hex');
 const pipe=process.platform==='win32'?`\\\\.\\pipe\\healthroster-turn-${process.pid}-${crypto.randomBytes(12).toString('hex')}`
  :path.join(os.tmpdir(),`hr-turn-${process.pid}-${crypto.randomBytes(12).toString('hex')}.sock`);
 const peers=new Set();
 const server=net.createServer(socket=>{
  let authenticated=false,buffer='';
  const send=value=>{if(!socket.destroyed)socket.write(JSON.stringify(value)+'\n');};
  socket.on('data',chunk=>{
   buffer+=chunk.toString();if(buffer.length>16384){socket.destroy();return;}
   let end;while((end=buffer.indexOf('\n'))>=0){const line=buffer.slice(0,end);buffer=buffer.slice(end+1);
    let request;try{request=JSON.parse(line);}catch{socket.destroy();return;}
    if(!authenticated){
     if(request.type!=='hello'||typeof request.capability!=='string'||request.capability!==capability){socket.destroy();return;}
     authenticated=true;peers.add(socket);send({type:'identity',identity});
     if(signal.aborted)send({type:'lost',code:'SHARED_TURN_LOST'});
    } else if(request.type==='check'&&Number.isSafeInteger(request.id)&&typeof request.phase==='string'){
     void assertHeld(request.phase).then(()=>send({type:'checked',id:request.id}),()=>send({type:'lost',code:'SHARED_TURN_LOST'}));
    } else socket.destroy();
   }
  });
  socket.on('error',()=>{});socket.once('close',()=>peers.delete(socket));
 });
 await new Promise((resolve,reject)=>{server.once('error',reject);server.listen(pipe,resolve);});
 const loss=()=>{for(const socket of peers)socket.write(JSON.stringify({type:'lost',code:'SHARED_TURN_LOST'})+'\n');};
 signal.addEventListener('abort',loss,{once:true});
 return {environment:{CLOUDTMS_SHARED_TURN_PIPE:pipe,CLOUDTMS_SHARED_TURN_CAPABILITY:capability},async close(){
  signal.removeEventListener('abort',loss);for(const socket of peers)socket.destroy();await new Promise(resolve=>server.close(resolve));
 }};
}

export async function connectTurnControl(env=process.env,{connect=net.createConnection,probeMs=10000}={}) {
 if(!env.CLOUDTMS_SHARED_TURN_PIPE||!env.CLOUDTMS_SHARED_TURN_CAPABILITY)throw closedTurnError('SHARED_TURN_CONTROL_REQUIRED');
 const controller=new AbortController(),pending=new Map();let identity,buffer='',sequence=0,closing=false;
 const socket=connect(env.CLOUDTMS_SHARED_TURN_PIPE);
 const lose=code=>{if(!closing&&!controller.signal.aborted)controller.abort(closedTurnError(code));for(const item of pending.values()){clearTimeout(item.timer);item.reject(controller.signal.reason??closedTurnError(code));}pending.clear();};
 const ready=new Promise((resolve,reject)=>{
  const timer=setTimeout(()=>{lose('SHARED_TURN_CONTROL_TIMEOUT');reject(controller.signal.reason);socket.destroy();},probeMs);
  socket.once('connect',()=>socket.write(JSON.stringify({type:'hello',capability:env.CLOUDTMS_SHARED_TURN_CAPABILITY})+'\n'));
  socket.on('data',chunk=>{
   buffer+=chunk.toString();if(buffer.length>16384){lose('SHARED_TURN_CONTROL_REFUSED');socket.destroy();return;}
   let end;while((end=buffer.indexOf('\n'))>=0){const line=buffer.slice(0,end);buffer=buffer.slice(end+1);
    let response;try{response=JSON.parse(line);}catch{lose('SHARED_TURN_CONTROL_REFUSED');return;}
    if(response.type==='identity'&&!identity){try{identity=assertTurnIdentity(response.identity);clearTimeout(timer);resolve();}catch(error){clearTimeout(timer);reject(error);lose(error.code);}}
    else if(response.type==='checked'&&pending.has(response.id)){const item=pending.get(response.id);pending.delete(response.id);clearTimeout(item.timer);item.resolve();}
    else if(response.type==='lost'){lose('SHARED_TURN_LOST');clearTimeout(timer);reject(controller.signal.reason);}
    else {lose('SHARED_TURN_CONTROL_REFUSED');clearTimeout(timer);reject(controller.signal.reason);}
   }
  });
  socket.on('error',()=>{clearTimeout(timer);lose('SHARED_TURN_CONTROL_CLOSED');reject(controller.signal.reason);});
  socket.once('close',()=>{clearTimeout(timer);lose('SHARED_TURN_CONTROL_CLOSED');reject(controller.signal.reason);});
 });
 await ready;
 return {identity,signal:controller.signal,async assertHeld(phase){
  if(controller.signal.aborted)throw controller.signal.reason;
  const id=++sequence;
  await new Promise((resolve,reject)=>{const timer=setTimeout(()=>{lose('SHARED_TURN_CONTROL_TIMEOUT');socket.destroy();},probeMs);pending.set(id,{resolve,reject,timer});socket.write(JSON.stringify({type:'check',id,phase})+'\n');});
  if(controller.signal.aborted)throw controller.signal.reason;
 },close(){closing=true;socket.destroy();}};
}

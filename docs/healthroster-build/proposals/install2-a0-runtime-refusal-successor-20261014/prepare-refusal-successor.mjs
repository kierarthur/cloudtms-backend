import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';import {execFileSync} from 'node:child_process';
const here=path.dirname(fileURLToPath(import.meta.url)),prior=path.resolve(here,'../install2-a0-runtime-route-wiring-20261014'),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const input=fs.readFileSync(path.join(prior,'forward/broker/src/index.js'));
const hunks=[{original:'return sbRpc(env, name, args, { timeoutMs: 20000 });',replacement:'return sbRpcRecordingGuardRefusal(env, name, args, { timeoutMs: 20000 });'},
 {original:"const result = await sbRpc(env, action === 'AUTHORISE' ? 'timesheet_authorise_bulk_atomic' : 'timesheet_unauthorise_bulk_atomic', {",
 replacement:"const result = await sbRpcRecordingGuardRefusal(env, action === 'AUTHORISE' ? 'timesheet_authorise_bulk_atomic' : 'timesheet_unauthorise_bulk_atomic', {"}];
let output=input.toString();for(const h of hunks){if(output.split(h.original).length!==2)throw Error('Nonunique actual A0 call hunk');output=output.replace(h.original,h.replacement);}
let reverse=output;for(const h of [...hunks].reverse()){if(reverse.split(h.replacement).length!==2)throw Error('Nonunique reverse');reverse=reverse.replace(h.replacement,h.original);}if(!Buffer.from(reverse).equals(input))throw Error('Entire19 reverse mismatch');
const destination=path.join(here,'forward/broker/src/index.js');fs.mkdirSync(path.dirname(destination),{recursive:true});fs.writeFileSync(destination,output);
const priorReverse=JSON.parse(fs.readFileSync(path.join(prior,'CURRENT_INDEX_FULL_REVERSE.json')));
let currentReverse=reverse;for(const h of [...priorReverse.hunks].reverse())currentReverse=currentReverse.replace(h.replacement,h.original);
const own=path.resolve(here,'../../../..');const current=execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','show',`${priorReverse.input_commit}:broker/src/index.js`],{cwd:own,maxBuffer:25000000});
if(!Buffer.from(currentReverse).equals(current))throw Error('Entire current owner reverse mismatch');
fs.writeFileSync(path.join(here,'CURRENT_REFUSAL_FULL_REVERSE.json'),JSON.stringify({input_commit:priorReverse.input_commit,prior19_manifest_sha256:sha(fs.readFileSync(path.join(prior,'REVIEW_MANIFEST.json'))),
 before_sha256:sha(input),after_sha256:sha(output),current_input_sha256:sha(current),hunks,prior_three_hunks_preserved:true,full_current_reverse:true},null,2)+'\n');
let test=fs.readFileSync(path.join(prior,'office-route-wiring.test.mjs'),'utf8');
test=test.replaceAll("'./reference/","'../install2-a0-runtime-route-wiring-20261014/reference/").replaceAll("'./forward/","'../install2-a0-runtime-route-wiring-20261014/forward/")
 .replace("'./unit-fixtures.mjs'","'../install2-a0-runtime-route-wiring-20261014/unit-fixtures.mjs'")
 .replace("new URL('./CURRENT_INDEX_FULL_REVERSE.json', import.meta.url)","new URL('../install2-a0-runtime-route-wiring-20261014/CURRENT_INDEX_FULL_REVERSE.json', import.meta.url)");
test=test.replace("const wiring = reverse.hunks[1].replacement.split('\\nasync function runTsfinWorkerOnce(')[0];",
 "const successor = JSON.parse(fs.readFileSync(new URL('./CURRENT_REFUSAL_FULL_REVERSE.json', import.meta.url)));\n"+
 "let wiring = reverse.hunks[1].replacement.split('\\nasync function runTsfinWorkerOnce(')[0];\n"+
 "for (const h of successor.hunks) { assert.equal(wiring.split(h.original).length, 2); wiring = wiring.replace(h.original, h.replacement); }");
test=test.replace('handleTimesheetCorrectionOfficeAction, requireUser, sbRpc,','handleTimesheetCorrectionOfficeAction, requireUser, sbRpc,\n'+
 '    sbRpcRecordingGuardRefusal: async (...args) => { calls.push([\'guardRefusalWrapper\', args[1], copy(args[2])]); return sbRpc(...args); },');
test += "\ntest('new correction and ordinary lifecycle mutations retain exact current guard-refusal wrapper admission', async () => {\n"+
 "  const h = harness(); const response = await h.actual.handleTimesheetCorrectionOfficeRoute(env, request('send', send), 'send');\n"+
 "  assert.equal(response.status, 200); const wrapped = h.calls.find(call => call[0] === 'guardRefusalWrapper');\n"+
 "  assert.equal(wrapped[1], 'timesheet_correction_send_v1'); assert.deepEqual(wrapped[2], { p_request: send, p_actor_user_id: I.actor });\n"+
 "  const p = harness({ rpc: async (name, args) => name === 'timesheet_correction_request_projection_v1' ? null : { ok: true, all_success: true, failure_count: 0, results: args.p_items.map(item => ({ success: true, current_timesheet_id: item.timesheet_id })) } });\n"+
 "  assert.equal((await p.actual.handleTimesheetCorrectionOfficeRoute(env, request('lifecycle', { action: 'AUTHORISE', items: [{ timesheet_id: I.original }] }), 'runLifecycle')).status, 200);\n"+
 "  assert.equal(p.calls.find(call => call[0] === 'guardRefusalWrapper')[1], 'timesheet_authorise_bulk_atomic');\n});\n";
fs.writeFileSync(path.join(here,'office-route-wiring-successor.test.mjs'),test);
const source=current.toString(),start=source.indexOf('async function sbRpcRecordingGuardRefusal('),end=source.indexOf('// The two QR owners',start);
if(start<0||end<start)throw Error('Current total wrapper missing');
fs.writeFileSync(path.join(here,'CURRENT_GUARD_REFUSAL_OWNER.json'),JSON.stringify({input_commit:priorReverse.input_commit,path:'broker/src/index.js',
 source:source.slice(start,end),sha256:sha(source.slice(start,end)),actor_key:'p_actor_user_id',unchanged:true},null,2)+'\n');
console.log(JSON.stringify({index_sha256:sha(output),hunks:hunks.length}));

import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)), prior=path.dirname(dir);
const i2=process.argv[2];if(!i2)throw Error('Explicit authorized I2 source directory required');
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const write=(p,b)=>{const f=path.join(dir,p);fs.mkdirSync(path.dirname(f),{recursive:true});fs.writeFileSync(f,b);};
const pins=[];
for(const p of ['broker/src/payment-actor-permission.js','broker/src/weekly-source/authorise-routing.mjs']) {
 const b=fs.readFileSync(path.join(i2,p));write(`reference/${p}`,b);if(p.endsWith('payment-actor-permission.js'))write(`forward/${p}`,b);
 pins.push({path:p,sha256:sha(b),unchanged_reference:true});
}
const idx=fs.readFileSync(path.join(i2,'broker/src/index.js'),'utf8');
if(sha(idx)!=='6dcaec638015d190ed2ff46b10540a6e8795c0f17892ef17ee7738ffa6c6ba8b')throw Error('Unexpected current root index');
const start=idx.indexOf('async function weeklySourceScopedLifecycleActor('),end=idx.indexOf('\nasync function handleTimesheetUnauthorise(',start);
if(start<0||end<0)throw Error('Actual scoped lifecycle owner not uniquely found');
const helper=idx.slice(start,end);write('CURRENT_NATIVE_SCOPED_ACTOR_OWNER.js.txt',helper);
pins.push({path:'broker/src/index.js',sha256:sha(idx),selected_owner:'weeklySourceScopedLifecycleActor',selected_owner_sha256:sha(helper)});
const orchestration=fs.readFileSync(path.join(i2,'broker/src/timesheet-correction-orchestrator.mjs'));
if(sha(orchestration)!=='bdb6db888eb4a9588f860503716edfa03a4a71f3629624130ddf88677eab5ced')throw Error('Unexpected current orchestration owner');
pins.push({path:'broker/src/timesheet-correction-orchestrator.mjs',sha256:sha(orchestration)});
for(const p of ['docs/healthroster-build/WP_PP_IMPLEMENTATION.md','supabase/repeatable/13102026_1127_healthroster_correction_requests/13102026_1127_context.sqlinc','supabase/repeatable/13102026_1113_weekly_source_protected_pay_authority_v1.sql','supabase/repeatable/05102026_1210_weekly_source_office_authorise_scope_v1.sql','supabase/repeatable/14102026_0519_healthroster_a0_current_native_composition/08_complete_current_request.sqlinc']) {
 if(!fs.existsSync(path.join(i2,p))){pins.push({path:p,status:'PATH_LOOKUP_REQUIRED'});continue;}const b=fs.readFileSync(path.join(i2,p));pins.push({path:p,sha256:sha(b)});
}
write('CURRENT_PERMISSION_SOURCE_PINS.json',JSON.stringify({schema:'SOURCE_ONLY_PERMISSION_PINS_V1',root_source_commit:'b5c1ea1b002b2d6b756707c020b6a170cc9fea80',root_saved_index_sha256:sha(idx),source_only:true,native_observations:0,pins},null,2)+'\n');
let t=fs.readFileSync(path.join(prior,'install2-a0-auth-tenant-successor-20261014/office-auth-tenant-successor.test.mjs'),'utf8');
t=t.replace("'../install2-a0-runtime-route-wiring-20261014/reference/broker/src/timesheet-correction-orchestrator.mjs'","'./forward/broker/src/timesheet-correction-orchestrator.mjs'");
const ts=t.indexOf('const reverse = JSON.parse'),te=t.indexOf('const actualAuth = ',ts);
if(ts<0||te<0)throw Error('Test source owner lookup failed');
t=t.slice(0,ts)+`import { weeklySourceAuthoriseRouting } from './reference/broker/src/weekly-source/authorise-routing.mjs';\nimport { isActivePaymentActor } from './reference/broker/src/payment-actor-permission.js';\nconst source = fs.readFileSync(new URL('./forward/broker/src/index.js', import.meta.url), 'utf8');\nconst wiring = source.slice(source.indexOf('// Uses the unchanged W14/W15 current native routing owner.'), source.indexOf('async function runTsfinWorkerOnce(', source.indexOf('// Uses the unchanged W14/W15 current native routing owner.')));\nconst scopedOwner = fs.readFileSync(new URL('./CURRENT_NATIVE_SCOPED_ACTOR_OWNER.js.txt', import.meta.url), 'utf8');\n`+t.slice(te);
t=t.replace("new URL('./CURRENT_AUTH_OWNER_SOURCE.json', import.meta.url)","new URL('../install2-a0-auth-tenant-successor-20261014/CURRENT_AUTH_OWNER_SOURCE.json', import.meta.url)");
t=t.replace("    if (options.rpc) return options.rpc(name, args);","    if (name === 'weekly_source_office_authorise_scope_v1') {\n      if (options.scopeError) throw options.scopeError;\n      return options.scope?.(args.p_request.timesheet_id) ?? { contract: 'WEEKLY_SOURCE_AUTHORISE_SCOPE_V1', applicable: true };\n    }\n    if (options.rpc) return options.rpc(name, args);");
t=t.replace("  const owners = { createTimesheetCorrectionLifecycleResolver,",`  const scoped = Function('owners', 'const { weeklySourceAuthoriseRouting, isActivePaymentActor, withCORS, unauthorized, JSON_HEADERS, firstString } = owners;\\n' + scopedOwner + '\\nreturn weeklySourceScopedLifecycleActor;')({\n    weeklySourceAuthoriseRouting: (rpc,id,actor) => weeklySourceAuthoriseRouting((name,args,opts) => sbRpc(env,name,args,opts),id,actor),\n    isActivePaymentActor, withCORS: (_,__,response) => response, unauthorized: () => new Response(JSON.stringify({ error: 'Unauthorized' }),{ status:401 }),\n    JSON_HEADERS: { 'content-type':'application/json' }, firstString: (...values) => values.find(x => typeof x === 'string' && x) || null\n  });\n  const owners = { weeklySourceScopedLifecycleActor: scoped, createTimesheetCorrectionLifecycleResolver,`);
t=t.replace("    rpc: async (name, args) => {\n      if (name === 'timesheet_correction_request_projection_v1')",`    rpc: async (name, args) => {\n      if (name === 'timesheet_correction_request_authority_v1') return { contract_version: 'TIMESHEET_CORRECTION_REQUEST_AUTHORITY_V1', request_id:I.request,state:'APPLIED',state_revision:'2',replacement_timesheet_id:I.p0,return_invoice_id:null };\n      if (name === 'timesheet_correction_request_projection_v1')`);
t=t.replace('withCORS, unauthorized, JSON_HEADERS, firstString } = owners;', 'withCORS, unauthorized, JSON_HEADERS, firstString, sbRpc } = owners;');
t=t.replace('weeklySourceAuthoriseRouting: (rpc,id,actor) => weeklySourceAuthoriseRouting((name,args,opts) => sbRpc(env,name,args,opts),id,actor),', 'weeklySourceAuthoriseRouting, sbRpc,');
t=t.replace('    if (options.rpc) return options.rpc(name, args);', "    if (name === 'timesheet_correction_request_projection_v1' && options.projection !== undefined) return options.projection;\n    if (options.rpc) return options.rpc(name, args);");
t=t.replace('buildReplacementSnapshot: async () => replacementFixture()', "buildReplacementSnapshot: async () => { calls.push(['producer']); return replacementFixture(); }");
t += '\n'+fs.readFileSync(path.join(dir,'scoped-pay-cases.inc.mjs'),'utf8');
write('scoped-pay-successor.test.mjs',t);
process.stdout.write(JSON.stringify({source_pins:pins.length,test_source_written:true})+'\n');

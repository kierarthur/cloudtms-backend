import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';import {execFileSync} from 'node:child_process';
const here=path.dirname(fileURLToPath(import.meta.url)),prior=path.resolve(here,'../install2-a0-runtime-route-wiring-20261014'),refusal=path.resolve(here,'../install2-a0-runtime-refusal-successor-20261014'),own=path.resolve(here,'../../../..'),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const current='b5c1ea1b002b2d6b756707c020b6a170cc9fea80',git=file=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','show',`${current}:${file}`],{cwd:own,maxBuffer:25000000});
const config=git('wrangler.toml'),text=config.toString(),match=text.match(/\[env\.test\.vars\]([\s\S]*?)(?=\n\[|$)/);if(!match)throw Error('Actual TEST vars absent');
const vars={};for(const key of ['CANDIDATE_APP_ENVIRONMENT','MYTMS_OFFICE_AGENCY_ID','BANKING_PAY_CORRECTION_TENANT_ID_V1']){
 const value=match[1].match(new RegExp('^'+key+'\\s*=\\s*"([^"\\r\\n]+)"','m'));if(!value)throw Error('Actual nonsecret key missing');vars[key]=value[1];}
if(vars.MYTMS_OFFICE_AGENCY_ID===vars.BANKING_PAY_CORRECTION_TENANT_ID_V1)throw Error('Actual distinct identity vector unexpectedly equal');
const input=fs.readFileSync(path.join(prior,'forward/broker/src/timesheet-correction-office-routes.mjs'));
const hunks=[{original:'// The deployment identity and current authenticated user are server values.\n// This is transport admission, never a certificate or a browser capability.',
 replacement:'// The MyTMS Office agency identifies Source deployment; the independently\n// configured auth tenant is the authority used by existing requireUser tokens.\n// Both are server values. This is admission, never a qualification capability.'},
 {original:"  required(object(user) && user.role === 'admin' && user.is_active === true && UUID.test(user.id || '') &&\n    same(user.tenant_id, env.MYTMS_OFFICE_AGENCY_ID), 'CORRECTION_ACTOR_NOT_ALLOWED', 403);",
 replacement:"  const authTenant = env?.BANKING_PAY_CORRECTION_TENANT_ID_V1;\n  required(typeof authTenant === 'string' && /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(authTenant),\n    'CORRECTION_DEPLOYMENT_UNAVAILABLE', 503);\n  required(object(user) && user.role === 'admin' && user.is_active === true && UUID.test(user.id || '') &&\n    same(user.tenant_id, authTenant), 'CORRECTION_ACTOR_NOT_ALLOWED', 403);"}];
let output=input.toString();for(const h of hunks){if(output.split(h.original).length!==2)throw Error('Nonunique full source hunk');output=output.replace(h.original,h.replacement);}
let reverse=output;for(const h of [...hunks].reverse()){if(reverse.split(h.replacement).length!==2)throw Error('Nonunique reverse');reverse=reverse.replace(h.replacement,h.original);}if(!Buffer.from(reverse).equals(input))throw Error('Full prior module mismatch');
const destination=path.join(here,'forward/broker/src/timesheet-correction-office-routes.mjs');fs.mkdirSync(path.dirname(destination),{recursive:true});fs.writeFileSync(destination,output);
const index=git('broker/src/index.js').toString(),authStart=index.indexOf('function bankingPayCorrectionTenantAuthority('),authEnd=index.indexOf('async function mintAccessToken(',authStart),userStart=index.indexOf('async function requireUser('),userEnd=index.indexOf('// KV session helpers',userStart);
if([authStart,authEnd,userStart,userEnd].some(n=>n<0))throw Error('Actual current auth owner absent');
fs.writeFileSync(path.join(here,'CURRENT_AUTH_OWNER_SOURCE.json'),JSON.stringify({source_commit:current,
 config:{path:'wrangler.toml',sha256:sha(config),nonsecret_test_vars:vars,no_config_mutation:true},
 index:{path:'broker/src/index.js',sha256:sha(git('broker/src/index.js')),tenant_owner_source:index.slice(authStart,authEnd),
 tenant_owner_sha256:sha(index.slice(authStart,authEnd)),require_user_source:index.slice(userStart,userEnd),require_user_sha256:sha(index.slice(userStart,userEnd))},
 identities_are_distinct:true,native_call:false},null,2)+'\n');
fs.writeFileSync(path.join(here,'AUTH_TENANT_FULL_REVERSE.json'),JSON.stringify({source_commit:current,owner:'broker/src/timesheet-correction-office-routes.mjs',
 before_sha256:sha(input),after_sha256:sha(output),hunks,prior19_manifest_sha256:sha(fs.readFileSync(path.join(prior,'REVIEW_MANIFEST.json'))),
 prior10_manifest_sha256:sha(fs.readFileSync(path.join(refusal,'REVIEW_MANIFEST.json'))),index10_preserved_sha256:sha(fs.readFileSync(path.join(refusal,'forward/broker/src/index.js')))},null,2)+'\n');
let tests=fs.readFileSync(path.join(refusal,'office-route-wiring-successor.test.mjs'),'utf8');
tests=tests.replace("'../install2-a0-runtime-route-wiring-20261014/forward/broker/src/timesheet-correction-office-routes.mjs'","'./forward/broker/src/timesheet-correction-office-routes.mjs'")
 .replace("new URL('./CURRENT_REFUSAL_FULL_REVERSE.json', import.meta.url)","new URL('../install2-a0-runtime-refusal-successor-20261014/CURRENT_REFUSAL_FULL_REVERSE.json', import.meta.url)")
 .replace("const env = { CANDIDATE_APP_ENVIRONMENT: 'TEST', MYTMS_OFFICE_AGENCY_ID: I.client, SUPABASE_URL: 'https://unit.invalid' };",
  "const actualAuth = JSON.parse(fs.readFileSync(new URL('./CURRENT_AUTH_OWNER_SOURCE.json', import.meta.url)));\nconst env = { ...actualAuth.config.nonsecret_test_vars, SUPABASE_URL: 'https://unit.invalid' };")
 .replace('tenant_id: I.client','tenant_id: env.BANKING_PAY_CORRECTION_TENANT_ID_V1');
tests += "\ntest('actual saved TEST auth tenant and MyTMS Office agency stay distinct with legitimate current session admission', () => {\n"+
 " assert.notEqual(env.BANKING_PAY_CORRECTION_TENANT_ID_V1, env.MYTMS_OFFICE_AGENCY_ID);\n"+
 " assert.deepEqual(assertTimesheetCorrectionOfficeActor(env,user), { actor_user_id:I.actor,environment:'TEST' });\n"+
 " assert.throws(() => assertTimesheetCorrectionOfficeActor(env,{ ...user,tenant_id:env.MYTMS_OFFICE_AGENCY_ID }), /CORRECTION_ACTOR_NOT_ALLOWED/);\n});\n"+
 "test('actual Office deployment validity and independent auth tenant have closed missing/foreign negatives', () => {\n"+
 " for (const bad of ['', undefined, 'unit-invalid']) assert.throws(() => assertTimesheetCorrectionOfficeActor({ ...env, MYTMS_OFFICE_AGENCY_ID:bad },user), /CORRECTION_DEPLOYMENT_UNAVAILABLE/);\n"+
 " for (const bad of ['', undefined, 'unit-invalid']) assert.throws(() => assertTimesheetCorrectionOfficeActor({ ...env, BANKING_PAY_CORRECTION_TENANT_ID_V1:bad },user), /CORRECTION_DEPLOYMENT_UNAVAILABLE/);\n"+
 " assert.throws(() => assertTimesheetCorrectionOfficeActor({ ...env, BANKING_PAY_CORRECTION_TENANT_ID_V1:id(94) },user), /CORRECTION_ACTOR_NOT_ALLOWED/);\n"+
 " assert.deepEqual(assertTimesheetCorrectionOfficeActor({ ...env, MYTMS_OFFICE_AGENCY_ID:id(93) },user), { actor_user_id:I.actor,environment:'TEST' });\n});\n";
fs.writeFileSync(path.join(here,'office-auth-tenant-successor.test.mjs'),tests);
console.log(JSON.stringify({module_sha256:sha(output),unique_hunks:hunks.length,actual_current_config_sha256:sha(config),config_changes:0}));

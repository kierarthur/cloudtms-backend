// Read canonical owners and write this isolated proposal only. No SQL execution.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url));
const root=path.resolve(process.argv[2]??'');
if(!root.endsWith('healthroster-install2-14102026'))throw Error('EXACT_READ_ONLY_INTEGRATOR_REQUIRED');
const sha=s=>crypto.createHash('sha256').update(s).digest('hex');
const inputs=[];
const read=p=>{const bytes=fs.readFileSync(path.join(root,p));inputs.push({path:p,sha256:sha(bytes)});return bytes.toString('utf8');};
const write=(p,s)=>{fs.mkdirSync(path.dirname(path.join(here,p)),{recursive:true});fs.writeFileSync(path.join(here,p),s);};
function once(s,a,b){if(s.split(a).length!==2)throw Error('EXACT_OWNER_ANCHOR_MISMATCH: '+a);return s.replace(a,b);}
const base='supabase/repeatable/14102026_0430_healthroster_money_native_interfaces/';
const hold='weekly_source_invoice_hold_release_atomic_v1';
let command=read(base+'06_OFF_RULE_COMMAND.sqlinc');
command=once(command,"'weekly_source_upload_seal_atomic_v1','weekly_source_projection_begin_atomic_v1'",`'weekly_source_upload_seal_atomic_v1','${hold}','weekly_source_projection_begin_atomic_v1'`);
command=once(command,"    when 'weekly_source_upload_seal_atomic_v1' then result:=public.weekly_source_upload_seal_atomic_v1(req);",`    when 'weekly_source_upload_seal_atomic_v1' then result:=public.weekly_source_upload_seal_atomic_v1(req);\n    when '${hold}' then result:=public.${hold}(req);`);
write('forward/06_OFF_RULE_COMMAND.sqlinc',command);
let shape=read(base+'21_SETUP_STATIC_SHAPE_ASSERT.sqlinc');
shape=once(shape,"'weekly_source_upload_stage_rows_atomic_v1','weekly_source_upload_seal_atomic_v1',",`'weekly_source_upload_stage_rows_atomic_v1','weekly_source_upload_seal_atomic_v1','${hold}',`);
write('forward/21_SETUP_STATIC_SHAPE_ASSERT.sqlinc',shape);
let proxy=read('broker/src/weekly-source/off-rule-native-proxy.mjs');
proxy=once(proxy,"'weekly_source_upload_stage_rows_atomic_v1','weekly_source_upload_seal_atomic_v1','weekly_source_projection_begin_atomic_v1'",`'weekly_source_upload_stage_rows_atomic_v1','weekly_source_upload_seal_atomic_v1','${hold}','weekly_source_projection_begin_atomic_v1'`);
write('forward/off-rule-native-proxy.mjs',proxy);
// These are source-pinned dependencies, not complete body replacements.
for(const p of ['01_OFF_RULE_AUTHORITY.sqlinc','02_HISTORICAL_FIXTURE_ASSIGNMENT.sqlinc','03_HISTORICAL_FIXTURE_SETUP_ASSERT.sqlinc','04_13_complete_nullable_physical_qualification_scope.sqlinc','05_OFF_RULE_NATIVE_PARENT_SCOPE.sqlinc','10_02_effective_rule_owner.sqlinc','22_POST_VERIFIED_SETUP_ASSERT.sqlinc','27_POST_VERIFIED_EVIDENCE_RECORD.sqlinc','29_11_complete_historical_fixture_native_assignment.sqlinc'])read(base+p);
for(const p of ['scripts/cloudtms-db-release.mjs','scripts/cloudtms-managed-release.mjs','scripts/cloudtms-auto-test-release.mjs','scripts/request-test-release.mjs','scripts/cloudtms-db-release-lib.mjs','.github/workflows/database-release.yml','supabase/release/current-release.json','supabase/release/current-contract.json','scripts/weekly-source-off-rule-setup-evidence.mjs','scripts/weekly-source-protected-release-qualification.mjs','scripts/weekly-source-protected-execution-tree.mjs','scripts/weekly-source-post-verified-evidence-controller.mjs','supabase/repeatable/13102026_1431_weekly_source_invoice_money_owners_v1.sql','supabase/migrations/13102026_1133_weekly_source_rule_sets_schema.sql','supabase/repeatable/13102026_1134_weekly_source_rule_sets_v1.sql','docs/healthroster-build/F5_PRODUCER_FIXTURE.json','broker/src/weekly-source/index.js','broker/src/weekly-source/upload-staging-adapter.mjs','broker/src/weekly-source/source-finalised-time.js','broker/src/weekly-source/healthroster-normalise.js'])read(p);
const f5path='supabase/verification/13102026_1440_weekly_source_invoice_money_staging_hold_verification.sql';
const original=read(f5path).replaceAll('\r\n','\n');
const lines=original.split('\n');
const declaration=lines.findIndex(s=>s.startsWith(' row_id uuid;'));
const start=lines.findIndex(s=>s.trim().startsWith('select * into strict latest'));
const stage=lines.findIndex(s=>s.trim().startsWith('begun:='));
if(declaration<0||start<0||stage<0||!(stage>start))throw Error('F5_SOURCE_LAYOUT_CHANGED');
lines[declaration]+=' auth jsonb; n integer; actual_client uuid; protected_ids uuid[]; before_economics jsonb;';
const helpers=fs.readFileSync(path.join(here,'fixture-helpers.sqlinc'),'utf8');
const setup=fs.readFileSync(path.join(here,'fixture-admission.sqlinc'),'utf8');
let journey=[...lines.slice(0,start),setup,...lines.slice(stage)].join('\n');
journey=once(journey,'create temporary table f5_owner_proof',helpers+'\ncreate temporary table f5_owner_proof');
journey=once(journey,"select pg_temp.f5_assert(date '2026-11-01'>current_date,'future fixture date');",`create temporary table f5_generated_input(value jsonb not null);
-- F5_GENERATED_DATA_INSERT_REQUIRED
select pg_temp.f5_assert((select count(*)=1 from f5_generated_input),'actual parser-generated synthetic file required');
select pg_temp.f5_assert(date '2026-11-01'>(clock_timestamp() at time zone 'Europe/London')::date,'future fixture date');`);
const nativeOps={begun:'weekly_source_upload_stage_begin_atomic_v1',staged:'weekly_source_upload_stage_rows_atomic_v1',sealed:'weekly_source_upload_seal_atomic_v1',released:hold};
journey=journey.split('\n').map(line=>{
 const match=line.match(/^\s+(begun|staged|sealed|released):=public\.([a-z0-9_]+)\((.*)\);$/);
 if(!match)return line;
 if(nativeOps[match[1]]!==match[2])throw Error('F5_NATIVE_OWNER_CHANGED');
 return `  ${match[1]}:=pg_temp.f5_native(auth,'${match[2]}',pg_temp.f5_native_request(${match[3]},auth));`;
}).join('\n');
journey=journey.replaceAll("where id='f5510000-0000-4000-8000-000000000080';","where id=(auth->>'source_cycle_id')::uuid;");
journey=once(journey,"request:=jsonb_build_object('actor_user_id','f5510000-0000-4000-8000-000000000001','source_cycle_id','f5510000-0000-4000-8000-000000000080','upload_row_id',row_id,", "request:=jsonb_build_object('actor_user_id',auth->>'journey_actor_user_id','source_cycle_id',auth->>'source_cycle_id','upload_row_id',row_id,");
journey=once(journey,`(public.${hold}(request)->>'idempotent')::boolean`,`(pg_temp.f5_native(auth,'${hold}',request)->>'idempotent')::boolean`);
journey=once(journey,`format('select public.${hold}(%L::jsonb)',request||jsonb_build_object('action','BILL_AS_FILE'))`,`format('select pg_temp.f5_native(%L::jsonb,%L,%L::jsonb)',auth,'${hold}',request||jsonb_build_object('action','BILL_AS_FILE'))`);
journey=once(journey," insert into f5_owner_proof values ('hold_release',released),('native_seal',sealed),('feature_gate',enabled);",fs.readFileSync(path.join(here,'fixture-postconditions.sqlinc'),'utf8'));
journey=journey.replace('-- F5 first-use configuration/staging/hold proof, root-only hosted TEST rollback.','-- F5 actual OFF_RULE setup, post-VERIFIED mandatory pre-publication rollback journey.\n-- SOURCE_ONLY / NATIVE_NOT_RUN; genuine registered authorisation required, features remain OFF.');
// The original future workbook is historical. The mandatory verifier now
// consumes fresh actual-parser data, never old JSON with relabelled dates.
journey=journey.split('\n').map(line=>{
 if(line.trim().startsWith('begun:='))return "  begun:=pg_temp.f5_native(auth,'weekly_source_upload_stage_begin_atomic_v1',pg_temp.f5_native_request((select value->'begin_request' from f5_generated_input),auth));";
 if(line.trim().startsWith('staged:='))return "  staged:=pg_temp.f5_native(auth,'weekly_source_upload_stage_rows_atomic_v1',pg_temp.f5_native_request((select value->'stage_request' from f5_generated_input)||jsonb_build_object('actor_user_id',auth->>'journey_actor_user_id','upload_id',begun->>'logical_upload_id'),auth));";
 return line.replace("finalised_at_utc='2026-10-28 23:59Z'::timestamptz","finalised_at_utc=(select (value#>>'{proof,finalised_at_utc}')::timestamptz from f5_generated_input)");
}).join('\n');
// Keep original rule-save/assignment-OFF configuration positives future dated
// from the REAL native clock. This temporary group is not the Source cycle.
const future="((clock_timestamp() at time zone 'Europe/London')::date+14+(7-extract(dow from (clock_timestamp() at time zone 'Europe/London')::date+14)::integer)%7)";
journey=journey.replaceAll("date '2026-11-01'",future)
 .replace("'effective_from_week_ending','2026-11-01'",`'effective_from_week_ending',${future}`)
 .replace('-- Fixed future week 2026-11-01: regenerate/review if no longer future before rehearsal.','-- Fresh synthetic Source file comes from the actual past open cycle/window; all native clocks remain genuine.');
write('fixtures/f5-post-verified.sql',journey);
const head=execFileSync('git',['rev-parse','HEAD'],{cwd:root,encoding:'utf8'}).trim();
// Pin the real parser/adapter's complete current relative dependency graph.
const visited=new Set();function dependencies(p){if(visited.has(p))return;visited.add(p);
 const source=inputs.some(i=>i.path===p)?fs.readFileSync(path.join(root,p),'utf8'):read(p);
 for(const m of source.matchAll(/(?:from\s*|import\s*)['"]([^'"]+)['"]/g))if(m[1].startsWith('.'))dependencies(path.posix.normalize(path.posix.join(path.posix.dirname(p),m[1])));
}dependencies('broker/src/weekly-source/index.js');dependencies('broker/src/weekly-source/upload-staging-adapter.mjs');
write('SOURCE_INPUTS.json',JSON.stringify({integrator_commit:head,inputs,native:'NOT_RUN',database_calls:false},null,2)+'\n');
console.log(JSON.stringify({inputs:inputs.length,integrator_commit:head,proposal_only:true}));

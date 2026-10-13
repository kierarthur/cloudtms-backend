import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),root=process.argv[2];assert.ok(root,'explicit read-only root required');
const lane=path.resolve(here,'../../../..'),workspace=path.dirname(path.dirname(lane));
const sha=v=>crypto.createHash('sha256').update(v).digest('hex');
const frozenPath=path.join(lane,'docs/healthroster-build/PROTECTION_SOURCE_MANIFEST.json');
const frozenBytes=fs.readFileSync(frozenPath);
assert.equal(sha(frozenBytes),'2d0947db59f875da1fd4aeed4b49c6566174b19cc244c41785670248c3fd1070');
const frozen=JSON.parse(frozenBytes);for(const file of frozen.files)assert.equal(sha(fs.readFileSync(path.join(lane,file.path))),file.sha256,'pinned Protection52 '+file.path);
const inputMap=new Map();
function input(absolute){const resolved=path.resolve(absolute);if(inputMap.has(resolved))return;const bytes=fs.readFileSync(resolved);
  inputMap.set(resolved,{path:resolved,sha256:sha(bytes),bytes:bytes.length});}
function guarded(guard){for(const item of guard.inputs||[guard.input]){const file=path.isAbsolute(item.path)?item.path:path.join(root,item.path);input(file);
  assert.equal(inputMap.get(path.resolve(file)).sha256,item.sha256,'input changed '+item.path);}}
for(const name of ['F4_SOURCE_GUARD.json','AI_WORKER_SOURCE_GUARD.json','AI_OFFICE_SOURCE_GUARD.json','OFFICE_HUNKS.json','NATIVE_REGRESSION_INPUT.json'])guarded(JSON.parse(fs.readFileSync(path.join(here,name))));
const cn=path.join(root,'broker/src/timesheet-correction-orchestrator.mjs');input(cn);
assert.equal(inputMap.get(path.resolve(cn)).sha256,'e51be6f1005c2a71487efe308ea01bfaba9eb3c3767b3c632ca72f7c1343e118','root CN successor must retain final guards');
assert.equal(sha(fs.readFileSync(path.join(here,'timesheet-correction-orchestrator.mjs'))),inputMap.get(path.resolve(cn)).sha256);
const direct=[
  'broker/src/index.js','broker/src/candidate-correction-v1.js','docs/healthroster-build/CORRECTION_CONTRACT.md',
  'supabase/migrations/13102026_1127_healthroster_timesheet_correction_requests_v1.sql',
  'supabase/repeatable/13102026_1649_healthroster_correction_request_read_authority_v1.sql',
  'supabase/repeatable/13102026_1111_healthroster_credit_runtime/13102026_1111_dispatcher.sqlinc',
  'supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1250_08a_private_invoice_batch_query_validate_v2.sql',
  'supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1806_private_invoice_batch_generate_classification_v2.sql',
  'package.json','package-lock.json'
];
for(const relative of direct)input(path.join(root,relative));
for(const dir of ['supabase/repeatable/13102026_1127_healthroster_correction_requests','supabase/repeatable/13102026_1143_healthroster_scoped_credit',
  'supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt'])
  for(const name of fs.readdirSync(path.join(root,dir)).filter(name=>name.endsWith('.sqlinc')))input(path.join(root,dir,name));
const my=path.join(workspace,'.codex-worktrees/healthroster-v2-mytms-13102026');
input(path.join(my,'scripts/build-candidate-contract.mjs'));
const plan=path.join(workspace,'cloudtms-backend/codex_outputs/claude-stage2-continuation-20260925/HEALTHROSTER_PLAN_V2_CONSOLIDATION_13102026/HEALTHROSTER_SELFBILL_IMPLEMENTATION_PLAN_V2');
for(const name of ['03_IMPLEMENTATION_PLAN.md','05_ACCEPTANCE.md','OWNER_ANSWERS_ROUND1_20261011.md','OWNER_ANSWERS_ROUND21_20261012.md'])input(path.join(plan,name));
// Pin every local relative module in the actual regression's source closure.
const traversed=new Set();function jsClosure(file){file=path.resolve(file);if(traversed.has(file))return;traversed.add(file);input(file);
  const code=fs.readFileSync(file,'utf8');for(const match of code.matchAll(/(?:from\s*|import\s*\()(['"])(\.[^'"]+)\1/g)){
    const next=path.resolve(path.dirname(file),match[2]);if(fs.existsSync(next)&&fs.statSync(next).isFile()){
      input(next);if(/\.[mc]?js$/.test(next))jsClosure(next);
    }
  }}
jsClosure(path.join(root,'tests/invoice_async_backend.test.js'));
const files=[];function walk(dir){for(const name of fs.readdirSync(dir).sort()){const absolute=path.join(dir,name);if(fs.statSync(absolute).isDirectory())walk(absolute);
  else if(name!=='F4_AI_SOURCE_MANIFEST.json'){const bytes=fs.readFileSync(absolute);files.push({path:path.relative(lane,absolute).replaceAll('\\','/'),sha256:sha(bytes),bytes:bytes.length});}}}
walk(here);
for(const name of fs.readdirSync(path.join(lane,'tests')).filter(name=>/^healthroster-f4-ai-.*\.test\.mjs$/.test(name))){
  const bytes=fs.readFileSync(path.join(lane,'tests',name));files.push({path:'tests/'+name,sha256:sha(bytes),bytes:bytes.length});}
const syntax=files.filter(file=>/\.(?:mjs|js)$/.test(file.path));for(const file of syntax){const checked=spawnSync(process.execPath,['--check',path.join(lane,file.path)],{encoding:'utf8',windowsHide:true});
  assert.equal(checked.status,0,checked.stderr);}
const result={contract:'HEALTHROSTER_F4_AI_SOURCE_PROPOSAL_V1',status:'SOURCE_ONLY_UNMOUNTED_NOT_INSTALLED_NOT_ACTIVATED',
  protected_handoff:{manifest_sha256:sha(frozenBytes),files_checked:frozen.files.length,unchanged:true},
  inputs:[...inputMap.values()].sort((a,b)=>a.path.localeCompare(b.path)),files:files.sort((a,b)=>a.path.localeCompare(b.path)),
  checks:{node_syntax_files:syntax.length,source_pure_mock_tests:146,sql_compiled:false,native_database_journeys:false,office_browser_journeys:false,document_phone_pages:false},
  new_sql_signatures:{private:['weekly_source_invoice_adjustment_authority_v1(uuid,uuid[])','weekly_source_invoice_adjustment_line_valid_v1(uuid,uuid)',
    'weekly_source_invoice_adjustment_payload_admit_v1(uuid,jsonb)','weekly_source_invoice_adjustments_apply_v1(uuid,jsonb,uuid)'],
    service:['timesheet_correction_credit_candidates_v1(jsonb)']},
  gates:['qualified existing activation/release capability','kept-as-paid linkage owner','missing-A0 native normalization','current central registry/NEW/UPGRADE compilation',
    'native Source/correction/credit/document/security/NEXT journeys','joined lock/runtime acceptance','Office browser and signed phone/PDF proof']};
fs.writeFileSync(path.join(here,'F4_AI_SOURCE_MANIFEST.json'),JSON.stringify(result,null,2)+'\n');
console.log(JSON.stringify({inputs:result.inputs.length,files:files.length,bytes:files.reduce((n,f)=>n+f.bytes,0),syntax:syntax.length,protection_pins:frozen.files.length}));

import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const out=path.dirname(fileURLToPath(import.meta.url)),root=process.argv[2];if(!root)throw Error('READ_ONLY_ROOT_REQUIRED');
const q='docs/healthroster-build/proposals/protected-source-qualification-operational-gate/';
const files=[
 'supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/04_complete_fin.sqlinc',
 'supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/10d_retained_report_and_date_boundaries.sqlinc',
 'supabase/repeatable/13102026_2110_healthroster_report_native_fin_worker/04_complete_fin_overload.sqlinc',
 'supabase/repeatable/13102026_2110_healthroster_report_native_fin_worker/02_pay_authority.sqlinc',
 'supabase/repeatable/13102026_2110_healthroster_report_native_fin_worker/03_pay_witness.sqlinc',
 'supabase/repeatable/13102026_1344_weekly_source_self_bill_report/13102026_1344_projection_rows.sqlinc',
 'supabase/repeatable/15092026_1534_weekly_source_finalisation_v1.sql',
 'supabase/repeatable/13102026_1134_weekly_source_rule_sets_v1.sql',
 'supabase/repeatable/13102026_1344_weekly_source_self_bill_report/13102026_1344_report_scope.sqlinc',
 'supabase/repeatable/13102026_2205_weekly_source_automatic_native_delta/12_complete_private_registry_union.sqlinc',
 'supabase/repeatable/13102026_2205_weekly_source_automatic_native_delta/13_complete_final_acl_union.sqlinc',
 'supabase/verification/13102026_2206_weekly_source_automatic_native_delta_v1.sql',
 'scripts/cloudtms-managed-release.mjs','scripts/cloudtms-managed-release-sql.mjs',
 'scripts/cloudtms-db-release.mjs','supabase/release/current-release.json','supabase/release/current-contract.json',
 ...['01_complete_qualification_scope.sqlinc','03_complete_operational_release_reader.sqlinc','04_complete_capability_receipt.sqlinc',
 '07_complete_existing_feature_setup.sqlinc','14_complete_managed_release.mjs','15_complete_current_release_caller.mjs',
 'weekly-source-protected-operational-evidence.mjs','weekly-source-protected-release-qualification.mjs','CLOSED_DTO.sqlinc'].map(f=>q+f),
 'docs/healthroster-build/proposals/strict-qualification-git-path-parser-forward/weekly-source-protected-execution-tree.mjs',
 'docs/healthroster-build/proposals/strict-qualification-first-use-inventory-forward/OPERATIONAL_CERTIFICATE.sqlinc',
 'docs/healthroster-build/proposals/strict-qualification-first-use-inventory-forward/TEST_JOURNEY_SCOPE.sqlinc'
];
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const inputs=files.map(file=>{const b=fs.readFileSync(path.join(root,file));return {path:file,sha256:sha(b),bytes:b.length};});
const pinFile=path.join(out,'CONTROLLING_INPUTS.json');
const prior=fs.existsSync(pinFile)?JSON.parse(fs.readFileSync(pinFile,'utf8')):null;
if(prior&&JSON.stringify(prior.inputs)!==JSON.stringify(inputs))throw Error('CONTROLLING_OWNER_CHANGED_REVIEW_REQUIRED');
const head=execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','rev-parse','HEAD'],{cwd:root,encoding:'utf8'}).trim();
fs.writeFileSync(pinFile,JSON.stringify({schema_version:'REPORT_AB_CONTROLLING_SOURCE_INPUTS_V1',root_commit:head,
 runtime_base:'cea15289e0283e3383e71f0acb8b18ce184647c2',status:'SOURCE_ONLY_NO_NATIVE_ACCEPTANCE',inputs,
 preserve:['Frozen Prepared30/array11/Report/Automatic/CN/A0 inputs','All root dirty evidence','All Banking worktree/source bytes'],
 native_admission:'Existing exact Source pay-query guard body/call retained; no downstream Banking definition changed or followed.',
 execution_equivalence:'Runtime byte equality is not whole-tree equivalence. Real qualification compares every tracked object at actual installed source and separate receipt-input commit using the final strict forward.'},null,2)+'\n');
console.log(JSON.stringify({root_commit:head,inputs:inputs.length}));

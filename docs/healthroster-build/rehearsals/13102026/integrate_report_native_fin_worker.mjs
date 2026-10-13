import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
const root='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-v2-backend-13102026';
const packet='docs/healthroster-build/proposals/report-native-pay-fin-delegate';
const out=path.dirname(fileURLToPath(import.meta.url));
const recovery=process.argv[2]==='--recover-receipt';
const bytes=p=>recovery?execFileSync('git',['show','8f711251725269b2aed5b0491731fab9cdaeeec5:'+p],{cwd:root,maxBuffer:3000000}):fs.readFileSync(path.join(root,p));
const read=p=>bytes(p).toString('utf8'),sha=x=>crypto.createHash('sha256').update(x).digest('hex');
const parse=p=>JSON.parse(read(p));
const sourceCommit=execFileSync('git',['rev-parse','HEAD'],{cwd:root,encoding:'utf8'}).trim();
if(sourceCommit!=='8f711251725269b2aed5b0491731fab9cdaeeec5')throw Error('SOURCE_CHECKPOINT_CHANGED');
const manifest=parse(packet+'/SOURCE_MANIFEST.json');
if(sha(read(packet+'/SOURCE_MANIFEST.json'))!=='68f37566589858c2b536a3755f74d9fd691a25693d3c50fb0d3ffab021e36af1')throw Error('FROZEN_PACKET_MANIFEST_CHANGED');
for(const a of manifest.artifacts){const b=bytes(a.path);if(b.length!==a.bytes||sha(b)!==a.sha256)throw Error('FROZEN_ARTIFACT_CHANGED '+a.path);}
const repair=parse('docs/healthroster-build/rehearsals/13102026/REPORT_M1_PROJECTION_LITERAL_REPAIR.json');
const allowed=new Map(repair.outputs.map(x=>[x.file,x]));
const pins=[...parse(packet+'/CONTROLLING_INPUTS.json').inputs,...parse(packet+'/WORKER_INPUTS.json').inputs];
const checked=[];
for(const pin of pins){const b=bytes(pin.path),current=sha(b),fixed=allowed.get(pin.path);
 if(current!==pin.sha256||b.length!==pin.bytes){
  if(!fixed||fixed.beforeSha256!==pin.sha256||fixed.afterSha256!==current)throw Error('CONTROLLING_INPUT_DRIFT '+pin.path);
  checked.push({path:pin.path,before:pin.sha256,current,acceptedOnlyRecordedLiteralRepair:true});
 }else checked.push({path:pin.path,current});
}
function reverse(body,hunks){for(const h of [...hunks].reverse()){const a=h.after??h.newText,b=h.before??h.oldText;if(body.split(a).length!==2)throw Error('REVERSE_HUNK_CARDINALITY');body=body.replace(a,()=>b);}return body;}
if(reverse(read(packet+'/04_complete_fin_overload.sqlinc'),parse(packet+'/FIN_HUNKS.json'))!==read(pins[0].path))throw Error('FIN_NATIVE_REVERSE_FAILED');
const wh=parse(packet+'/WORKER_HUNKS.json'),changes=[];
function add(file,text){const full=path.join(root,file);if(!recovery&&fs.existsSync(full))throw Error('NEW_DESTINATION_EXISTS '+file);changes.push({file,text,before:null});}
function replace(file,text){const before=read(file);if(text===before)throw Error('EMPTY_EDIT '+file);changes.push({file,text,before:sha(before)});}
for(const file of ['upload-staging-adapter.mjs','upload-publication-owner.mjs']){
 const proposal='worker/'+file,target='broker/src/weekly-source/'+file,body=read(packet+'/'+proposal);
 if(reverse(body,wh.filter(x=>x.file===proposal))!==read(target))throw Error('WORKER_NATIVE_REVERSE_FAILED '+target);
 replace(target,body);
}
add('broker/src/weekly-source/report-upload-provenance.mjs',read(packet+'/worker/report-upload-provenance.mjs'));
const contextPath='supabase/repeatable/13102026_1344_weekly_source_self_bill_report/13102026_1344_upload_context.sqlinc';
if(reverse(read(packet+'/08_complete_upload_context.sqlinc'),wh.filter(x=>x.file==='08_complete_upload_context.sqlinc'))!==read(contextPath))throw Error('CONTEXT_NATIVE_REVERSE_FAILED');
const leaf='supabase/repeatable/13102026_2110_healthroster_report_native_fin_worker';
for(const name of ['01_projection_receipt.sqlinc','02_pay_authority.sqlinc','03_pay_witness.sqlinc','04_complete_fin_overload.sqlinc','08_complete_upload_context.sqlinc','07_private_acl.sqlinc'])add(leaf+'/'+name,read(packet+'/'+name));
const reportPath='supabase/repeatable/13102026_1344_weekly_source_self_bill_report/13102026_1344_projection_rows.sqlinc';
if(read(packet+'/09_complete_report_projection_rows_forwardfix.sqlinc')!==read(reportPath))throw Error('REPORT_ALREADY_REPAIRED_OWNER_CHANGED');
const m1=read('supabase/repeatable/13102026_1561_weekly_source_magnit_contract_price_v1.sql');
const anchor='create or replace function public.weekly_source_projection_rows_apply_atomic_v1(';
const pos=m1.indexOf(anchor);if(pos<0||read(packet+'/10_complete_m1_projection_rows_forwardfix.sqlinc')!==m1.slice(pos,m1.indexOf('$function$;',pos)+11))throw Error('M1_ALREADY_REPAIRED_OWNER_CHANGED');
add(leaf+'_v1.sql',String.raw`-- Complete native Report reads, internal FIN overload and upload context.
-- Operational A/B dispatch remains held pending real release-bound proof.
\set ON_ERROR_STOP on
begin;
\ir 13102026_2110_healthroster_report_native_fin_worker/01_projection_receipt.sqlinc
\ir 13102026_2110_healthroster_report_native_fin_worker/02_pay_authority.sqlinc
\ir 13102026_2110_healthroster_report_native_fin_worker/03_pay_witness.sqlinc
\ir 13102026_2110_healthroster_report_native_fin_worker/04_complete_fin_overload.sqlinc
\ir 13102026_2110_healthroster_report_native_fin_worker/08_complete_upload_context.sqlinc
\ir 13102026_2110_healthroster_report_native_fin_worker/07_private_acl.sqlinc
notify pgrst, 'reload schema';
commit;
`);
const additions=['private.weekly_source_finalise_engine_v1(jsonb,uuid,boolean,text)','private.weekly_source_report_projection_receipt_assert_v1(public.weekly_source_ordinary_pay_projection_receipts)','private.weekly_source_self_bill_report_pay_authority_v1(uuid)'];
let privateRows=null;
for(const file of ['supabase/repeatable/15092026_1534_weekly_source_acl_contract_v1.sql','supabase/repeatable/13102026_1436_weekly_source_invoice_money_acl_inventory_v1.sql']){
 let body=read(file),m=body.match(/create or replace function private\._weekly_source_acl_private_helper_contract_v1\(\)[\s\S]*?\$function\$;/i);
 if(!m)throw Error('MISSING_NATIVE_PRIVATE_REGISTRY');
 const old=[...m[0].matchAll(/\('([^']+)'\)/g)].map(x=>x[1]);if(old.length!==218||new Set(old).size!==218)throw Error('BASE_PRIVATE_COUNT_CHANGED');
 const rows=[...new Set([...old,...additions])].sort();if(rows.length!==221)throw Error('PRIVATE_ADDITIONS_NOT_EXACT');
 if(privateRows&&JSON.stringify(privateRows)!==JSON.stringify(rows))throw Error('NATIVE_REGISTRY_DIVERGENCE');privateRows=rows;
 const next=m[0].replace(/  values[\s\S]*?\$function\$;/,()=> '  values\n'+rows.map(x=>"    ('"+x+"')").join(',\n')+';\n$function$;');
 replace(file,body.replace(m[0],()=>next));
}
{
 const file='supabase/verification/15092026_1534_weekly_source_acl_contract_v1.sql',body=read(file);
 const m=body.match(/v_expected_private_helpers text\[\]:=array\[([\s\S]*?)\]::text\[\];/);if(!m)throw Error('MISSING_INDEPENDENT_PRIVATE_CENSUS');
 const old=[...m[1].replace(/--[^\n]*/g,'').matchAll(/'([^']+)'/g)].map(x=>x[1]);
 const rows=[...new Set([...old,...additions])].sort();if(old.length!==218||JSON.stringify(rows)!==JSON.stringify(privateRows))throw Error('INDEPENDENT_CENSUS_DIVERGENCE');
 replace(file,body.replace(m[0],()=>'v_expected_private_helpers text[]:=array[\n'+rows.map(x=>"    '"+x+"'").join(',\n')+'\n  ]::text[];'));
}
{
 const file='tests/weekly-source/weekly-source-acl-contract.test.mjs',body=read(file);
 if(body.split('assert.equal(actual.length, 218);').length!==2)throw Error('TEST_PRIVATE_CENSUS_DRIFT');
 replace(file,body.replace('assert.equal(actual.length, 218);',()=> 'assert.equal(actual.length, 221);'));
}
add('tests/weekly-source/report-native-upload-protocol.test.mjs',read(packet+'/worker-protocol.test.mjs').replaceAll("from './worker/","from '../../broker/src/weekly-source/"));
const receipt={status:'SOURCE_INTEGRATED_NATIVE_FINANCIAL_API_RELEASE_GATES_PENDING',sourceCommit,frozenManifestSha256:sha(read(packet+'/SOURCE_MANIFEST.json')),inputChecks:checked,reverseProofs:{completeFIN:true,ordinaryNHSPBankHooksRetained:true,completeWorkers:true,completeUploadContext:true},alreadyAppliedLiteralRepairs:repair.outputs,signatureChanges:additions,privateCount:221,serviceCount:142,tableCount:105,noOperationalABDispatch:true,holds:['Separate real A/B financial, admission, render and pay isolation proof','Deterministic existing protected release binding','Fresh calendar/rules and actual native positive receipts','Prepared immutable output amount recovery','Protected reconcile-only publication and exact NEXT active/withheld money join','Browser/Office/app/phone acceptance'],outputs:changes.map(x=>({file:x.file,before:x.before,after:sha(x.text),bytes:Buffer.byteLength(x.text)}))};
for(const edit of changes){const full=path.join(root,edit.file);if(recovery){if(!fs.existsSync(full)||sha(fs.readFileSync(full))!==sha(edit.text))throw Error('RECOVERY_APPLIED_BYTES_DIFFER '+edit.file);}else if(edit.before!==null&&sha(fs.readFileSync(full))!==edit.before)throw Error('CONCURRENT_EDIT_REFUSED '+edit.file);}
if(!recovery)for(const edit of changes){const full=path.join(root,edit.file);fs.mkdirSync(path.dirname(full),{recursive:true});fs.writeFileSync(full,edit.text);}
if(recovery)receipt.receiptRecovery={reason:'Initial receipt write failed because output URL pathname retained percent-encoded spaces',sourceRead:'Exact pre-integration Git commit',allAppliedOutputsByteVerified:true,sourceWritesDuringRecovery:0};
const json=JSON.stringify(receipt,null,2)+'\n';
fs.writeFileSync(path.join(out,'REPORT_NATIVE_FIN_WORKER_ROOT_INTEGRATION.json'),json);
fs.writeFileSync(path.join(root,'docs/healthroster-build/rehearsals/13102026/REPORT_NATIVE_FIN_WORKER_ROOT_INTEGRATION.json'),json);
console.log(JSON.stringify({outputs:changes.length,private:221,service:142,tables:105,operationalFINDispatch:'HELD',runtimeProof:'PENDING'}));


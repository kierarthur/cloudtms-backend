import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {fileURLToPath} from 'node:url';
const root=path.dirname(fileURLToPath(import.meta.url));
const read=file=>fs.readFileSync(path.join(root,file),'utf8');
const sha=file=>crypto.createHash('sha256').update(fs.readFileSync(path.join(root,file))).digest('hex');
function apply(source,recipe){for(const patch of recipe.patches){if(source.split(patch.before).length!==2)throw new Error('Exact unique native anchor missing: '+patch.description);source=source.replace(patch.before,patch.after);}return source;}
fs.mkdirSync(path.join(root,'test-runtime'),{recursive:true});
const native=apply(read('reference/query-admission.sql'),JSON.parse(read('ROOT0226_PATCH.json')));
fs.writeFileSync(path.join(root,'test-runtime/root0226.sqlinc'),native.split('\n').filter(line=>!line.startsWith('\\')).join('\n'));
const drain=apply(read('reference/drainEmailOutboxOnce.jsinc'),JSON.parse(read('INDEX_DRAIN_PATCH.json')));
fs.writeFileSync(path.join(root,'test-runtime/drain-overlay.mjs'),drain);
for(const [reference,recipe,output] of [['pause.sql','ROOT_PAUSE_PATCH.json','pause-overlay.sqlinc'],['contact-pause-office.mjs','WORKER_PAUSE_PATCH.json','contact-pause-office.mjs'],['office-contact-pause-contract.js','OFFICE_CONTRACT_PATCH.json','office-contact-pause-contract.cjs'],['office-contact-pause-controls.js','OFFICE_CONTROLS_PATCH.json','office-contact-pause-controls.cjs']]) {
 fs.writeFileSync(path.join(root,'test-runtime',output),apply(read('reference/'+reference),JSON.parse(read(recipe))).split('\n').filter(line=>!line.startsWith('\\')).join('\n'));
}
const files=fs.readdirSync(path.join(root,'forward')).filter(file=>file.endsWith('.sqlinc')&&!['090_acl.sqlinc','091_claim_acl.sqlinc'].includes(file)).sort();
const functions=[];
for(const file of files){for(const match of read('forward/'+file).matchAll(/create or replace function\s+((?:private|public)\.[a-z0-9_]+)\s*\(([\s\S]*?)\)\s*returns/gi)){
 const args=match[2].split(',').map(arg=>arg.trim().split(/\s+/)[1]).join(',');functions.push({name:match[1],signature:match[1]+'('+args+')',payload:'forward/'+file});
}}
const service=new Set(['public.timesheet_query_email_enqueue_v1','public.timesheet_query_email_recompose_v1','public.timesheet_query_email_recompose_ready_v1','public.timesheet_query_email_contact_result_v1','public.email_outbox_claim_ready_batch','public._email_outbox_contact_claim_core_v1','public._email_outbox_contact_claim_deployment_v1','public.email_outbox_claim_ready_batch_contact_v1','public._email_outbox_contact_claim_stamp_v1','public._email_outbox_contact_claim_cleanup_v1']);
const acl=f=>`alter function ${f.signature} owner to postgres;\nrevoke all on function ${f.signature} from public,anon,authenticated,service_role;${service.has(f.name)?'\ngrant execute on function '+f.signature+' to service_role;':''}`;
fs.writeFileSync(path.join(root,'forward/090_acl.sqlinc'),functions.map(acl).join('\n')+'\n');
fs.writeFileSync(path.join(root,'forward/091_claim_acl.sqlinc'),functions.filter(f=>['forward/081_native_claim_guard.sqlinc','forward/082_native_claim_kernel.sqlinc','forward/083_native_claim_bridge.sqlinc'].includes(f.payload)).map(acl).join('\n')+'\n');
files.push('090_acl.sqlinc','091_claim_acl.sqlinc');
const manifest={version:'V6_NATIVE_QUERY_CONTACT_SOURCE_V1',native:'REQUIRED_NOT_RUN',database_execution:false,
 source_inputs:'SOURCE_INPUTS.json',ordered_sql:files.map(f=>'forward/'+f),functions,
 schema:{target:'New root-allocated protected migration only',payload:'forward/010_existing_delivery_lineage.sqlinc',atomic_claim_fence:['forward/010_existing_delivery_lineage.sqlinc','forward/081_native_claim_guard.sqlinc','forward/082_native_claim_kernel.sqlinc','forward/083_native_claim_bridge.sqlinc','forward/091_claim_acl.sqlinc'],note:'Snapshot and query lease/generic claim fence plus every created routine ACL must commit atomically. Existing delivery store only; root self identity and genuine revision1, no timestamp backfill.'},
 root0226:{target:JSON.parse(read('ROOT0226_PATCH.json')).target,recipe:'ROOT0226_PATCH.json',note:'Apply finite exact anchors to current owner; never replace wholeRoot0226 from the reference.'},
 original_owners:[{target:'public.timesheet_query_email_enqueue_v1(uuid,uuid,jsonb,uuid,integer,integer)',payload:'forward/050_native_entrypoints.sqlinc',core:'forward/030_native_enqueue_core.sqlinc',preserved:'Original two render CTEs/attachments/native issue and event owners'},
 {target:'public._timesheet_query_email_delivery_mark_core_v1(uuid,text,text,timestamptz,uuid)',payload:'forward/070_native_mark.sqlinc'},
 {target:'public.email_outbox_claim_ready_batch(integer,text,integer)',payload:'forward/083_native_claim_bridge.sqlinc',core:'forward/082_native_claim_kernel.sqlinc',preserved:'Original SECURITY INVOKER/RLS, queue, leases, order, bounds and every non-query predicate. Old generic entry excludes query work; only the same-kernel current contact bridge selects new managed queries.'}],
 compatibility:{database_first:true,jobs_remain_running:true,pause_recipe:'ROOT_PAUSE_PATCH.json',worker_pause_recipe:'WORKER_PAUSE_PATCH.json',office_contract_recipe:'OFFICE_CONTRACT_PATCH.json',office_controls_recipe:'OFFICE_CONTROLS_PATCH.json',pre_upgrade_claims:'Freeze actual original native lease identity and timestamps; unknown remains legacy ownership without fabricated START/permit or expiry resend.'},
 worker:{module_target:'broker/src/weekly-source/query-contact-drain.mjs',module:'forward/query-contact-drain.mjs',target:'broker/src/index.js',recipe:'INDEX_DRAIN_PATCH.json',scope:'drainEmailOutboxOnce only'},
 files:[...fs.readdirSync(path.join(root,'forward')).sort().map(f=>({file:'forward/'+f,sha256:sha('forward/'+f)})),
  ...['ROOT0226_PATCH.json','INDEX_DRAIN_PATCH.json','ROOT_PAUSE_PATCH.json','WORKER_PAUSE_PATCH.json','OFFICE_CONTRACT_PATCH.json','OFFICE_CONTROLS_PATCH.json','fixtures/native-query-contact-assertions.sqlinc'].map(file=>({file,sha256:sha(file)}))],
 focused_dependencies:['Actual current Root0226 common START/permit and Root contact primitives must be mounted with finite patches.',
 'A PDF key genuinely shared with excluded workers holds for native per-Timesheet evidence rebuild; no guessed document command or page filtering.',
 'Pre-existing legacy DELIVERY_UNCERTAIN with no genuine new permit remains held for original outcome review; no fabricated provenance.',
 'Mount exact same-kernel claim fence, typed legacy in-flight union and current drain bridge before claiming new managed query mail. Jobs remain running; generic old sender excludes query work and cannot fabricate managed SENT.',
 'Genuine schema/transaction/Office history/provider-result/runtime fixtures remain NOT_RUN.']};
fs.writeFileSync(path.join(root,'SOURCE_MANIFEST.json'),JSON.stringify(manifest,null,2)+'\n');
console.log(JSON.stringify({sql_files:files.length,functions:functions.length,manifest_sha256:sha('SOURCE_MANIFEST.json'),native:manifest.native}));

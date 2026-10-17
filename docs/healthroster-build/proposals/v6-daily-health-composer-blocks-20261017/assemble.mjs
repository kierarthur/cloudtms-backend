// Source-only owner-block assembly. No DB/network/SQL execution. Current
// metadata is deliberately unbound, so no runtime composer file can be emitted.
import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
export const here=path.dirname(fileURLToPath(import.meta.url));
const own=path.resolve(here,'../../../..');
const roots={integration:path.resolve(own,'../healthroster-install2-14102026'),checklist:path.resolve(own,'../healthroster-checklist-native-14102026'),policy:'D:/Development/Legacy-Work/C-tmp/healthroster-handover/HealthRoster_Builder_Pack_16102026/NHSP_AUTHORISATION_AUDIT_15102026_HANDOFF_V6'};
const read=p=>fs.readFileSync(p,'utf8').replaceAll('\r\n','\n');
const sha=s=>createHash('sha256').update(s).digest('hex');
export const blockFiles=['health/00_presentation.sqlinc','health/10_revision_apply.sqlinc','templates/01_review.sql.tmpl','templates/02_accept.sql.tmpl','templates/03_result.sql.tmpl'];
export function sources(){return Object.fromEntries(blockFiles.map(file=>[file,read(path.join(here,file))]));}
export function verifyFrozenSources(){
 for(const root of [own,...Object.values(roots)])assert(fs.realpathSync(root).replaceAll('\\','/').startsWith('D:/Development/'),'PHYSICAL_D_SOURCE_REQUIRED');
 const pins=JSON.parse(read(path.join(here,'SOURCE_PINS.json')));
 for(const row of pins.files){
  const root=roots[row.workspace],absolute=path.join(root,row.file),raw=fs.readFileSync(absolute);
  assert.equal(sha(raw),row.rawSha256,'saved raw source: '+row.file);
  assert.equal(sha(raw.toString('utf8').replaceAll('\r\n','\n')),row.canonicalSha256,'complete current owner: '+row.file);
  if(row.commit)assert.deepEqual(raw,execFileSync('git',['-C',root,'show',row.commit+':'+row.file]),'frozen committed source: '+row.file);
 }
 const registry=JSON.parse(read(path.join(roots.integration,'docs/bpay-next/final-definer-closures.json')));
 for(const row of pins.finalOwners){
  const final=registry.chains.find(c=>c.signature===row.signature);assert(final,'final owner: '+row.signature);
  assert.equal(final.finalRoot,row.finalRoot);assert.equal(final.finalPhysical,row.finalPhysical);
 }
 return {fullSourceFiles:pins.files.length,finalOwnerSignatures:pins.finalOwners.length,currentOwnerEdits:0};
}
function ordered(text,needles,label){let previous=-1;for(const needle of needles){const at=text.indexOf(needle,previous+1);assert(at>previous,label+': '+needle);previous=at;}}
export function validateHealthBlocks(all=sources()){
 const presentation=all[blockFiles[0]],apply=all[blockFiles[1]],review=all[blockFiles[2]],accept=all[blockFiles[3]],result=all[blockFiles[4]];
 const joined=Object.values(all).join('\n');
 assert.equal((joined.match(/create or replace function private\./g)||[]).length,7,'four Health helpers plus three held composer bodies');
 assert(!/create or replace function (?:public\.|private\.bpay_next_)/i.test(joined),'no new public/Banking authority');
 assert(!/\b(?:insert into public\.|update public\.|delete from public\.|alter table|create table)\b/i.test(joined),'native original owners alone write business rows');
 assert(!/bpay_next_contract_move_(?:cut|stage|publish)_v1\s*\(/.test(joined),'no Contract-only financial API substitution');
 for(const [text,token] of [[review,'@@BANK_REVIEW@@'],[accept,'@@BANK_ACCEPT@@'],[result,'@@BANK_RESULT@@']])assert.equal(text.split(token).length,2,'one exact unbound owner slot');
 assert(!/bank_(?:ready|accept_qualified|financial_visible)\s*:=\s*true|financial_visible\s*:=\s*true/i.test(joined),'no successful Bank/visibility default');
 ordered(review,['daily_rate_correction_preview_replay_v1(p_args)','daily_rate_correction_context_v1(',
  'daily_rate_correction_preview_replay_v1(p_args)','daily_rate_correction_health_basis_v1(p_args,true)',
  '@@BANK_REVIEW@@','invoice_contract_move_inspect_v1(physical,daily,actor,true)',
  "'timesheet-contract-move-request:'",'daily_rate_correction_request_hash_v1(p_args,\'PREVIEW\')',
  'insert into private.timesheet_contract_move_requests','insert into private.timesheet_contract_move_reviews',
  'invoice_contract_move_preflight_v1(review_id,actor,true)','ready:=bank_ready','receipt_json=result'], 'Review actual native/replay/seal order');
 assert(review.includes('bank_manifest_extension ?| array[\'daily\',\'physical\',\'invoice\',\'reason\']'),'Bank cannot overwrite Health manifest');
 assert(review.includes("bank_pay_effect is distinct from '{\"visibility\":\"REDACTED\"}'::jsonb"),'preview denied-by-shape');
 ordered(accept,['daily_rate_correction_request_hash_v1(p_args,\'ACCEPT\')','if q.request_id is not null then',
  'daily_rate_correction_health_actor_require_v1(actor,false)',"current_setting('transaction_isolation')",
  'daily_rate_correction_context_v1(o.from_timesheet_id,actor,true)','if q.request_id is not null then',
  'daily_rate_correction_health_actor_require_v1(actor,true)','daily_rate_correction_health_reason_note_v1(o.reason)',
  'is distinct from r.preview_request_hash','@@BANK_ACCEPT@@','daily_rate_correction_health_accept_basis_v1(p_args)',
  "'timesheet-contract-move-request:'",'daily_rate_correction_health_revision_apply_v1(p_args)',
  'insert into private.timesheet_contract_move_requests',"state='COMPLETE'",'daily_rate_correction_assert_v1(o.id,r.id)',
  'result:=private.daily_rate_correction_result_v1(o.id,actor)','receipt_json=result'], 'Accept replay/currentness/native-before-journal order');
 assert(!accept.includes('return q.receipt_json'),'accepted replay must project actual current reader');
 ordered(apply,['daily_rate_correction_health_accept_basis_v1(p_args)',"o.financial_state is distinct from 'QUALIFIED'",
  '(private.timesheet_contract_move_context_v1()).id is not null','update private.timesheet_contract_move_operations',
  'update private.timesheet_contract_move_members','insert into private.timesheet_contract_move_execution_contexts',
  'daily_rate_correction_revision_admit_v1(o.id,m.member_key)',
  'publication:=private.bpay_next_daily_rate_correction_publish_v1(o.id,financial)',
  'placement_receipt_json=receipt','timesheet_contract_move_capability_cleanup_v1()',
  'invoice_contract_move_prepare_v1(o.id,o.billing_revision,actor,clock_timestamp())',
  'timesheet_contract_move_capability_cleanup_v1()','daily_rate_correction_assert_v1(o.id,r.id)'], 'real native apply/publication/Invoice/proof order');
 assert.equal((apply.match(/publication:=private\.bpay_next_daily_rate_correction_publish_v1\(/g)||[]).length,1,'one native publication call');
 assert(apply.includes("now_utc+interval '120 seconds'")&&apply.includes('pg_backend_pid(),pg_current_xact_id()'),'real backend/xid/expiry context');
 assert(!/exception when/i.test(apply+accept+review),'no exception converts incomplete effects to success');
 assert(presentation.includes("p_financial->'command_id' is distinct from 'null'::jsonb")&&presentation.includes("p_financial->'cut_id' is distinct from 'null'::jsonb"),'denied financial identifiers NULL');
 assert(presentation.includes('count(distinct jsonb_build_array('),'native actions semantic dedupe');
 ordered(result,['daily_rate_correction_health_result_basis_v1(p_operation_id,p_actor_user_id)','@@BANK_RESULT@@',
  'daily_rate_correction_health_financial_projection_assert_v1(financial,financial_visible)',
  'daily_rate_correction_health_actions_assert_v1(bank_next_actions||bank_links)',
  "o.placement_receipt_json->>'placement_revision'", "'financial',financial,'billing',basis->'billing'",
  'daily_rate_correction_health_result_basis_v1(p_operation_id,p_actor_user_id) is distinct from basis',
  "daily_rate_correction_public_assert_v1(result,'RESULT')"], 'current-role result projection');
 assert(!result.includes('daily_rate_correction_health_actor_require_v1('),'GET must not inherit mutation admission');
 assert(result.includes('into reader_before from public.tms_users')&&result.includes('is distinct from reader_before'),'post-join current reader row must not change');
 assert(!result.includes("'placement_receipt',")&&!result.includes("'financial',o.receipt_json"),'no raw native/saved creator receipt');
 return {privateHelpers:4,heldComposerBodies:3,unboundOwnerBlocks:3,businessWritersImplemented:0};
}
export function requireOwnerBindings(manifest){
 if(manifest.status!=='PUBLISHED_REVIEWED_OWNER_BLOCKS'||manifest.runtimeComposerSymbolsMayBeInstalled!==true)
  throw Error('DAILY_V3_OWNER_BLOCKS_UNPROVIDED: assembly emits no runtime SQL');
 for(const boundary of manifest.boundaries){
  for(const key of ['publishedContractPath','publishedContractCommit','publishedContractSha256','ownerCallBlockPath','ownerCallBlockSha256']){
   if(typeof boundary[key]!=='string'||boundary[key]==='')throw Error('DAILY_V3_OWNER_BINDING_PIN_REQUIRED:'+boundary.token+':'+key);
  }
  if(boundary.status!=='PUBLISHED_REVIEWED')throw Error('DAILY_V3_OWNER_BLOCKS_UNPROVIDED:'+boundary.token);
 }
 if(manifest.boundaries.length!==3)throw Error('DAILY_V3_EXACT_OWNER_BOUNDARIES_REQUIRED');
 return manifest;
}
export function check(){return {...verifyFrozenSources(),...validateHealthBlocks(),assembly:'HELD_UNPROVIDED_GENUINE_BANK_INTERFACES',native:'NOT_RUN',runtimeSqlEmitted:false};}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 try{
  const result=check();
  if(process.argv.includes('--emit')){
   requireOwnerBindings(JSON.parse(read(path.join(here,'OWNER_BLOCKS.json'))));
   // Reaching a newly published owner binding requires a separately reviewed
   // assembly successor. This frozen package has no runtime-emission branch.
   throw Error('DAILY_V3_NEW_OWNER_BINDINGS_REQUIRE_REVIEWED_ASSEMBLY_SUCCESSOR');
  }
  console.log(JSON.stringify(result,null,2));
 }catch(error){console.error(error.message);process.exitCode=2;}
}

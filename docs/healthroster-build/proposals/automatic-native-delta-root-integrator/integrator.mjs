// Source-only root integration. Default is read-only planning. No DB, API,
// credential, environment, dependency installation or Git mutation APIs.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { fileURLToPath, pathToFileURL } from 'node:url';

export const here=path.dirname(fileURLToPath(import.meta.url));
export const SOURCE_BASE='dd06f9245f8a20615cd2edcea523a28dc5285f68';
export const OBSERVED_HEAD='9fc45b9a4cc3a8dce400887f1640cfaf233f23be';
export const ROOT_BRANCH='codex/healthroster-v2-backend-13102026';
export const REPEATABLE='supabase/repeatable/13102026_2205_weekly_source_automatic_native_delta_v1.sql';
export const SUPPORT='supabase/repeatable/13102026_2205_weekly_source_automatic_native_delta/';
export const VERIFIER='supabase/verification/13102026_2206_weekly_source_automatic_native_delta_v1.sql';
export const ACL_VERIFIER='supabase/verification/15092026_1534_weekly_source_acl_contract_v1.sql';
export const RELEASE='supabase/release/current-release.json';
export const TARGETS=[REPEATABLE,ACL_VERIFIER,VERIFIER,RELEASE,...[
 '01_staged_source_documents.sqlinc','02_staged_publication_context.sqlinc','03_approved_detail_evidence.sqlinc',
 '04_native_receipt_successors.sqlinc','05_staged_prior_wait_receipt.sqlinc','06_expense_evidence.sqlinc',
 '07_staged_entitlement_components.sqlinc','08_complete_automatic_reconcile_delta_forward.sqlinc',
 '09_complete_local_successor.sqlinc','12_complete_private_registry_union.sqlinc','13_complete_final_acl_union.sqlinc',
 '15_final_automatic_private_acl.sqlinc'].map(p=>SUPPORT+p)];
const PACKETS={automatic:'docs/healthroster-build/proposals/automatic-native-delta/',
 forward:'docs/healthroster-build/proposals/automatic-native-delta-forward-review/',
 review:'docs/healthroster-build/proposals/automatic-native-delta-root-b9e2-review/'};
const MANIFESTS={automatic:'8c640b781800532e562405dd8bcbf818b1ec9f8a07f7352dd68fefc95381d939',
 forward:'00cc030cff3a30c8582a9ab118d482e690010012856157e8dc873a541a7808b2',
 review:'e413b82d44e2b417f0b1a270150b17d4db2da418f3b645532c9ab8b9e64790fa'};
export const REBASE={
 'supabase/repeatable/13102026_1344_weekly_source_self_bill_report/13102026_1344_projection_rows.sqlinc':{
  before:'8218421d0732c10c3b2c2b3f49c2b5a982956d9d70d7920db846d7cdb1cd44e1',
  after:'3a3f3e2c7f8d165064d94453436ce63e833f0815b64d25ccebcd73df046db495',
  canonicalFile:'7c5c499d7c13d8974842fa932101c6c0fad71b1f746ea20cd96980ebaf219f2d',
  rootReportedCanonicalOwner:'2621a75d7a461da4ced5eda77faf5a581c3c1136ff3cc95b4e2ac2be137d6994'},
 'supabase/repeatable/13102026_1561_weekly_source_magnit_contract_price_v1.sql':{
  before:'d05872065c382d3d4d63330a91dfbf09ce0903ee58bc76c7590b1b3cc1be2c28',
  after:'517ec09509d02ffcbfbf3451162ce011fc5e2bacc411590d9db7166902a59b4d',
  canonicalFile:'517ec09509d02ffcbfbf3451162ce011fc5e2bacc411590d9db7166902a59b4d',
  rootReportedCanonicalOwner:'0cadd57420a32f77bf096e859659f9e4dba7592290226bd1596dd2ac9d125175'}
};
export const REBASE_ANCHORS=[
 "coalesce(v_economic->>'sign','')!~'^[-]?[0-9]+",
 "coalesce(v_economic->>'paid_minutes','')!~'^[0-9]+",
 "coalesce(v_economic->>'break_minutes','')!~'^[0-9]+",
 "coalesce(v_economic#>>array['bucket_minutes',bucket.name],'')!~'^[0-9]+",
 "coalesce(v_economic#>>array['hours',bucket.name],'')!~'^[+-]?[0-9]+([.][0-9]+)?",
 "coalesce(v_economic#>>array['pay_rates',bucket.name],'')!~'^[+]?[0-9]+([.][0-9]+)?",
 "coalesce(v_economic#>>array['charge_rates',bucket.name],'')!~'^[+]?[0-9]+([.][0-9]+)?"
];
export const hash=x=>crypto.createHash('sha256').update(x).digest('hex');
const canonical=x=>String(x).replaceAll('\r\n','\n');
const slash=x=>x.replaceAll('\\','/');
const fail=code=>{throw Error(code);};
function git(root,args) {
 const p=spawnSync('git',['-c','gc.auto=0','-c','maintenance.auto=false',...args],{cwd:root,encoding:'utf8',maxBuffer:8*1024*1024});
 if(p.status!==0)fail('SOURCE_GIT_METADATA_UNAVAILABLE');return p.stdout.trim();
}
export function bankingSource(p) {return /(?:\/banking-pay\/|banking_pay|newtablesbanking|\/bpay-next\/)/i.test(p);}
export function inside(root,p) {
 if(path.isAbsolute(p)||p.split(/[\\/]/).includes('..'))fail('INTEGRATOR_PATH_ESCAPE');
 const full=path.resolve(root,p),rel=path.relative(root,full);
 if(!rel||rel.startsWith('..')||path.isAbsolute(rel))fail('INTEGRATOR_PATH_ESCAPE');
 let parent=path.dirname(full);while(!fs.existsSync(parent)){const next=path.dirname(parent);if(next===parent)fail('INTEGRATOR_PARENT_UNAVAILABLE');parent=next;}
 const realRel=path.relative(fs.realpathSync(root),fs.realpathSync(parent));
 if(realRel.startsWith('..')||path.isAbsolute(realRel))fail('INTEGRATOR_SYMLINK_ESCAPE');
 if(fs.existsSync(full)&&fs.lstatSync(full).isSymbolicLink())fail('INTEGRATOR_SYMLINK_ESCAPE');
 return full;
}
function read(root,p) {if(bankingSource(p))fail('BANKING_SOURCE_READ_FORBIDDEN');return fs.readFileSync(inside(root,p));}
function assertHash(root,p,expected) {
 const b=read(root,p);if(hash(b)!==expected)fail('ROOT_INPUT_HASH_CHANGED:'+p);return b;
}
export function definition(sql,name,scan) {
 const parsed=scan(canonical(sql));const defs=parsed.statements.map(s=>parsed.source.slice(s.start,s.end))
  .filter(s=>s.startsWith('create or replace function '+name+'('));
 if(defs.length!==1)fail('EXACT_COMPLETE_OWNER_REQUIRED:'+name);return defs[0];
}
function changeOnce(text,from,to) {if(text.split(from).length!==2)fail('UNIQUE_REVERSE_ANCHOR_CHANGED');return text.replace(from,to);}
export function projectionRebase(p,bytes) {
 const expected=REBASE[p];if(!expected||hash(bytes)!==expected.after||hash(canonical(bytes))!==expected.canonicalFile)fail('PROJECTION_REBASE_INPUT_CHANGED');
 const current=bytes.toString('utf8');let reverse=current;
 for(const anchor of REBASE_ANCHORS){
  const exact=anchor+"$'";if(current.split(exact).length!==2)fail('PROJECTION_REBASE_ANCHOR_CHANGED');
  reverse=changeOnce(reverse,exact,anchor);
 }
 if(hash(reverse)!==expected.before)fail('PROJECTION_REBASE_UNRELATED_BYTES_CHANGED');
 const lines=current.split(/\r?\n/).filter(l=>l.includes("!~'"));
 if(lines.length!==9||lines.some(l=>! /!~'\^[^\r\n']+\$'/.test(l)))fail('PROJECTION_REBASE_NINE_NATIVE_GUARDS_REQUIRED');
 return {path:p,beforeSha256:expected.before,afterSha256:expected.after,canonicalFileSha256:expected.canonicalFile,
  rootReportedCanonicalOwnerSha256:expected.rootReportedCanonicalOwner,
  reverseSha256:hash(reverse),suffixInsertions:7,nativeRegexPredicates:9,unrelatedBytesRetained:true,
  controllingRepairCommit:'c82efd3f6ad2cb8afc4a80464fc7541df5f01ae4',rootWritesPerformed:false};
}
export function reverseLocal(proposed,original) {
 let p=changeOnce(proposed,'  v_components jsonb;\n  v_automatic_components jsonb;','  v_components jsonb;');
 p=changeOnce(p,`  -- A manually invoked native Local owner cannot bypass the sealed automatic\n`
  +`  -- request's per-component zero-delta proof. This is before PREPARING insert.\n`
  +`  if v_generation.complete_next_vector_json ? 'automatic_reconcile_proof' then\n`
  +`    perform private.weekly_source_automatic_reconcile_delta_assert_v1(v_request.id,v_actor);\n  end if;\n`,'');
 const start=original.indexOf("    select coalesce(jsonb_agg(jsonb_build_object('work_event_id',expense.work_event_id,");
 const end=original.indexOf('    -- Source expenses must be the same approved observations',start);
 if(start<0||end<0)fail('EXACT_ORDINARY_PRODUCER_MISSING');
 const ordinary=original.slice(start,end);
 p=changeOnce(p,`    if v_generation.complete_next_vector_json ? 'automatic_reconcile_proof' then\n`
  +`      -- Exactly the producer used by delta_assert; no separately guessed vector.\n`
  +`      v_automatic_components:=private.weekly_source_staged_entitlement_components_v1(v_request.id,v_actor);\n`
  +`      v_components:=v_automatic_components->'components';\n      v_expenses:=v_automatic_components->'expenses';\n`
  +`    else\n`+ordinary+`    end if;\n`,ordinary);
 if(p!==original)fail('LOCAL_COMPLETE_NATIVE_REVERSE_PROOF_FAILED');return hash(p);
}
export function appendVerifier(text,key,p) {
 const before=JSON.parse(text);if(!Array.isArray(before[key])||before[key].includes(p))fail('RELEASE_ARRAY_OR_REPLAY_CHANGED');
 const k=text.indexOf(JSON.stringify(key));let start=text.indexOf('[',k),end=start,depth=0,quoted=false,escaped=false;
 if(k<0||start<0)fail('RELEASE_ARRAY_MISSING');
 for(;end<text.length;end++){const c=text[end];if(quoted){if(escaped)escaped=false;else if(c==='\\')escaped=true;else if(c==='"')quoted=false;}
  else if(c==='"')quoted=true;else if(c==='[')depth++;else if(c===']'&&--depth===0)break;}
 if(end===text.length)fail('RELEASE_ARRAY_UNCLOSED');
 const inner=text.slice(start+1,end),trailing=inner.match(/\s*$/)[0],eol=text.includes('\r\n')?'\r\n':'\n';
 const addition=`,${eol}    ${JSON.stringify(p)}`;
 const result=text.slice(0,start+1)+inner.slice(0,inner.length-trailing.length)+addition+trailing+text.slice(end);
 const after=JSON.parse(result);const expected={...before,[key]:[...before[key],p]};
 const at=start+1+inner.length-trailing.length;
 if(JSON.stringify(after)!==JSON.stringify(expected)||result.slice(0,at)+result.slice(at+addition.length)!==text)fail('RELEASE_EXACT_REVERSE_FAILED');
 return result;
}
function body(sql) {const m=sql.match(/\bas\s+(\$[a-z_0-9]*\$)/i);if(!m)fail('DOLLAR_BODY_REQUIRED');return sql.slice(m.index+m[0].length,sql.lastIndexOf(m[1]));}
export function dedicatedVerifier(privateSignatures,definitions) {
 const rows=privateSignatures.map(s=>`    ('${s}','${hash(body(definitions.get(s.split('(')[0])))}')`).join(',\n');
 const retainedNames=['public.weekly_exceptional_pay_complete_local_v1',...['local','c1','next'].map(k=>`private.weekly_source_pay_query_${k}_receipt_v1`)];
 const retained=retainedNames.map(name=>{
  const sql=definitions.get(name);if(!sql)fail('RETAINED_AUTOMATIC_OWNER_MISSING');
  const sig=name==='public.weekly_exceptional_pay_complete_local_v1'?name+'(jsonb)':name+'(uuid,uuid,uuid)';
  return `    ('${sig}','${hash(body(sql))}')`;
 }).join(',\n');
 return `-- Data-free owner declarations, exact body hashes and early refusal only.\n`
 +`-- No positive component/CAS/transport/release qualification or activation.\n\\set ON_ERROR_STOP on\nbegin;\n`
 +`do $automatic_declarations$\ndeclare r record; p pg_catalog.pg_proc%rowtype;\nbegin\n`
 +`  for r in select * from (values\n${rows}\n  ) expected(signature,body_sha256) loop\n`
 +`    select * into p from pg_catalog.pg_proc where oid=pg_catalog.to_regprocedure(r.signature);\n`
 +`    if not found or p.proowner<>current_user::regrole::oid or not p.prosecdef or p.provolatile<>'s'\n`
 +`      or p.prokind<>'f' or p.pronargdefaults<>0 or p.prorettype<>'jsonb'::regtype::oid\n`
 +`      or p.proconfig is distinct from array['search_path=pg_catalog, pg_temp']::text[]\n`
 +`      or pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(p.prosrc,'UTF8')),'hex')<>r.body_sha256\n`
 +`      or pg_catalog.has_function_privilege('anon',p.oid,'EXECUTE')\n`
 +`      or pg_catalog.has_function_privilege('authenticated',p.oid,'EXECUTE')\n`
 +`      or pg_catalog.has_function_privilege('service_role',p.oid,'EXECUTE')\n`
 +`      or exists(select 1 from pg_catalog.aclexplode(coalesce(p.proacl,pg_catalog.acldefault('f',p.proowner))) a\n`
 +`        where a.grantee=0 and a.privilege_type='EXECUTE') then\n`
 +`      raise exception 'AUTOMATIC_NATIVE_PRIVATE_DECLARATION_FAILED' using errcode='55000',detail=r.signature;\n    end if;\n  end loop;\n`
 +`  -- Full ordinary receipt wrappers and Local retain their reviewed body bytes.\n`
 +`  for r in select * from (values\n${retained}\n  ) expected(signature,body_sha256) loop\n`
 +`    if not exists(select 1 from pg_catalog.pg_proc p where p.oid=pg_catalog.to_regprocedure(r.signature)\n`
 +`      and pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(p.prosrc,'UTF8')),'hex')=r.body_sha256) then\n`
 +`      raise exception 'AUTOMATIC_NATIVE_EXISTING_OWNER_BODY_CHANGED' using errcode='55000',detail=r.signature;\n    end if;\n  end loop;\nend;\n$automatic_declarations$;\n`
 +`do $automatic_early_refusals$\ndeclare refused boolean; missing_id constant uuid:='13102026-2206-4000-8000-000000000001';\nbegin\n`
 +`  refused:=false;\n  begin\n    perform private.weekly_source_staged_publication_context_v1(null,null);\n`
 +`  exception when sqlstate '22023' then\n    if sqlerrm<>'WEEKLY_SOURCE_STAGED_OWNERSHIP_UNAVAILABLE' then raise; end if; refused:=true;\n  end;\n`
 +`  if not refused then raise exception 'AUTOMATIC_NATIVE_NULL_STAGE_NOT_REFUSED'; end if;\n`
 +`  refused:=false;\n  begin\n    perform private.weekly_source_automatic_reconcile_delta_assert_v1(null,null);\n`
 +`  exception when sqlstate '22023' then\n    if sqlerrm<>'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_DELTA_UNAVAILABLE' then raise; end if; refused:=true;\n  end;\n`
 +`  if not refused then raise exception 'AUTOMATIC_NATIVE_NULL_DELTA_NOT_REFUSED'; end if;\n`
 +`  -- Prove the missing selector before calling SELECT STRICT; never touch a real request.\n`
 +`  if exists(select 1 from public.weekly_exceptional_c1_publication_requests where id=missing_id) then\n`
 +`    raise exception 'AUTOMATIC_NATIVE_VERIFIER_SENTINEL_COLLISION'; end if;\n`
 +`  refused:=false;\n  begin\n    perform private.weekly_source_staged_publication_context_v1(missing_id,missing_id);\n`
 +`  exception when no_data_found then refused:=true; end;\n`
 +`  if not refused then raise exception 'AUTOMATIC_NATIVE_MISSING_STAGE_NOT_REFUSED'; end if;\n`
 +`  refused:=false;\n  begin\n    perform private.weekly_source_automatic_reconcile_delta_assert_v1(missing_id,missing_id);\n`
 +`  exception when no_data_found then refused:=true; end;\n`
 +`  if not refused then raise exception 'AUTOMATIC_NATIVE_MISSING_DELTA_NOT_REFUSED'; end if;\n`
 +`  -- Presence is never readiness. Only exercise the proven missing-dependency arm.\n`
 +`  if pg_catalog.to_regprocedure('private.weekly_source_automatic_reconcile_producer_ready_v1(uuid)') is null then\n`
 +`    refused:=false;\n    begin\n      perform private.weekly_source_automatic_reconcile_capability_assert_v1(null);\n`
 +`    exception when sqlstate '55000' then\n      if sqlerrm<>'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_DELTA_UNAVAILABLE' then raise; end if; refused:=true;\n    end;\n`
 +`    if not refused then raise exception 'AUTOMATIC_NATIVE_MISSING_CAPABILITY_NOT_REFUSED'; end if;\n  end if;\n`
 +`  if pg_catalog.to_regprocedure('private.weekly_source_bulk_producer_ready_v1(uuid)') is null\n`
 +`    or pg_catalog.to_regprocedure('private.weekly_source_bulk_source_cas_v1(uuid,uuid[])') is null then\n`
 +`    refused:=false;\n    begin\n      perform private.weekly_source_bulk_source_cas_assert_v1(null,'{}'::uuid[]);\n`
 +`    exception when sqlstate '55000' then\n      if sqlerrm<>'WEEKLY_SOURCE_BULK_PRODUCER_UNAVAILABLE' then raise; end if; refused:=true;\n    end;\n`
 +`    if not refused then raise exception 'AUTOMATIC_NATIVE_MISSING_BULK_NOT_REFUSED'; end if;\n  end if;\nend;\n$automatic_early_refusals$;\nrollback;\n`;
}

export async function prepare(root,{expectedHead=OBSERVED_HEAD,packetRoot=root}={}) {
 root=fs.realpathSync(root);packetRoot=fs.realpathSync(packetRoot);
 if(git(root,['rev-parse','HEAD'])!==expectedHead||git(root,['branch','--show-current'])!==ROOT_BRANCH)fail('EXACT_ROOT_HEAD_OR_BRANCH_REQUIRED');
 if(slash(git(root,['rev-parse','--show-toplevel'])).toLowerCase()!==slash(root).toLowerCase())fail('EXACT_ROOT_WORKTREE_REQUIRED');
 git(root,['merge-base','--is-ancestor',SOURCE_BASE,expectedHead]);
 const controlled=['supabase','broker','scripts/cloudtms-managed-release-sql.mjs','scripts/cloudtms-db-release-lib.mjs'];
 const nativeChanges=git(root,['diff','--name-only',SOURCE_BASE,'--',...controlled]).split('\n').filter(Boolean);
 if(nativeChanges.some(p=>!Object.hasOwn(REBASE,p)))fail('NATIVE_SOURCE_DRIFT_REVIEW_REQUIRED');
 const untracked=git(root,['ls-files','--others','--exclude-standard','--','supabase','broker']);
 if(untracked)fail('UNREVIEWED_NATIVE_SOURCE_PRESENT');
 const metadata={observedHead:expectedHead,sourceBase:SOURCE_BASE,branch:ROOT_BRANCH,
  separatelyOwnedChanges:git(root,['diff','--name-only',SOURCE_BASE,expectedHead,'--','scripts','docs','tests']).split('\n').filter(Boolean),
  dirtyOutsideNative:git(root,['diff','--name-only','--','scripts','docs','tests']).split('\n').filter(Boolean)};
 const packets={},packetFacts=[];
 for(const [key,dir] of Object.entries(PACKETS)){
  const bytes=assertHash(packetRoot,dir+'SOURCE_MANIFEST.json',MANIFESTS[key]);const m=JSON.parse(bytes);
  packetFacts.push({path:dir+'SOURCE_MANIFEST.json',sha256:MANIFESTS[key]});
  for(const f of m.files??m.artifacts){const p=f.path.startsWith('docs/')||f.path.startsWith('tests/')?f.path:dir+f.path;
   assertHash(packetRoot,p,f.sha256);packetFacts.push({path:p,sha256:f.sha256});}
  packets[key]={dir,manifest:m};
 }
 const reviewInputs=JSON.parse(read(packetRoot,PACKETS.review+'CONTROLLING_INPUTS.json'));
 const projectionRebases=[];
 const inputFacts=reviewInputs.sourceInputs.filter(f=>!bankingSource(f.path)).map(f=>{
  if(Object.hasOwn(REBASE,f.path)){
   if(f.sha256!==REBASE[f.path].before)fail('PROJECTION_PREDECESSOR_PIN_CHANGED');
   const proof=projectionRebase(f.path,read(root,f.path));projectionRebases.push(proof);
   return {...f,sha256:REBASE[f.path].after,predecessorSha256:f.sha256};
  }
  assertHash(root,f.path,f.sha256);return f;
 });
 if(projectionRebases.length!==2)fail('COMPLETE_PROJECTION_REBASE_CENSUS_REQUIRED');
 // Deliberately no fresh Banking source reads. Metadata drift checks preserve
 // every unselected native byte; the old frozen review remains evidence only.
 metadata.unreadBankingInputPaths=reviewInputs.sourceInputs.filter(f=>bankingSource(f.path)).map(f=>f.path);
 const official=await import(pathToFileURL(inside(root,'scripts/cloudtms-managed-release-sql.mjs')));
 const files=new Map(),provenance=[];
 const copy=(target,key,name)=>{const p=PACKETS[key]+name,b=read(packetRoot,p);files.set(target,b);provenance.push({target,source:p,sha256:hash(b)});};
 for(const name of ['01_staged_source_documents.sqlinc','02_staged_publication_context.sqlinc','03_approved_detail_evidence.sqlinc',
  '04_native_receipt_successors.sqlinc','05_staged_prior_wait_receipt.sqlinc','06_expense_evidence.sqlinc','07_staged_entitlement_components.sqlinc','09_complete_local_successor.sqlinc'])copy(SUPPORT+name,'automatic',name);
 copy(SUPPORT+'08_complete_automatic_reconcile_delta_forward.sqlinc','forward','08_complete_automatic_reconcile_delta_forward.sqlinc');
 for(const name of ['12_complete_private_registry_union.sqlinc','13_complete_final_acl_union.sqlinc','15_final_automatic_private_acl.sqlinc'])copy(SUPPORT+name,'review',name);
 copy(REPEATABLE,'review','16_proposed_root.sqlinc');copy(ACL_VERIFIER,'review','14_complete_acl_verifier_union.sqlinc');
 const defs=new Map();for(const [p,b] of files)if(p.startsWith(SUPPORT)){
  const parsed=official.scanManagedSql(canonical(b));for(const s of parsed.statements){const sql=parsed.source.slice(s.start,s.end);
   const m=sql.match(/^create or replace function ((?:private|public)\.[a-z_0-9]+)\(/);if(m)defs.set(m[1],sql);}
 }
 const names=packets.automatic.manifest.newPrivateSignatures;
 const localName='public.weekly_exceptional_pay_complete_local_v1';
 const nativeLocal=definition(read(root,'supabase/repeatable/13102026_1113_weekly_source_protected_pay_authority_v1.sql'),localName,official.scanManagedSql);
 const proofs={localCompleteReverseSha256:reverseLocal(defs.get(localName),nativeLocal),ordinaryReceiptReverse:[]};
 for(const [kind,p] of [['local','05102026_0114_weekly_source_pay_query_evidence_v1.sql'],['c1','05102026_0245_weekly_source_pay_query_c1_receipt_v1.sql'],['next','05102026_0258_weekly_source_pay_query_next_receipt_v1.sql']]){
  const oldName=`private.weekly_source_pay_query_${kind}_receipt_v1`,newName=`private.weekly_source_pay_query_${kind}_receipt_event_v1`;
  const original=definition(read(root,'supabase/repeatable/'+p),oldName,official.scanManagedSql);
  let reversed=changeOnce(defs.get(newName),newName,oldName);reversed=changeOnce(reversed,',p_family_event_id uuid','');
  reversed=changeOnce(reversed,'\n      and (p_family_event_id is null or e.id=p_family_event_id)','');
  if(reversed!==original)fail('ORDINARY_RECEIPT_COMPLETE_REVERSE_FAILED:'+kind);
  const wrapper=defs.get(oldName);
  if(!wrapper.includes(`${newName}(p_root_timesheet_id,p_approval_id,p_generation_id,null)`))fail('ORDINARY_NULL_EVENT_DELEGATE_CHANGED');
  proofs.ordinaryReceiptReverse.push({kind,sha256:hash(reversed)});
 }
 const addition=names.map(s=>`    ('${s}'),`).join('\n')+'\n';proofs.ACLCompleteReverse=[];
 for(const [name,input] of [['12_complete_private_registry_union.sqlinc','supabase/repeatable/13102026_1436_weekly_source_invoice_money_acl_inventory_v1.sql'],
  ['13_complete_final_acl_union.sqlinc','supabase/repeatable/15092026_1534_weekly_source_acl_contract_v1.sql']]){
  const next=canonical(files.get(SUPPORT+name));const expected=official.exactAtomicBody(canonical(read(root,input))).trimEnd()+'\n';
  if(changeOnce(next,addition,'')!==expected)fail('ACL_COMPLETE_REVERSE_FAILED');proofs.ACLCompleteReverse.push({input,sha256:hash(expected)});
 }
 const independentAddition=names.map(s=>`    '${s}',`).join('\n')+'\n';
 if(changeOnce(canonical(files.get(ACL_VERIFIER)),independentAddition,'')!==canonical(read(root,ACL_VERIFIER)))fail('INDEPENDENT_VERIFIER_REVERSE_FAILED');
 proofs.independentVerifierReverseSha256=hash(canonical(read(root,ACL_VERIFIER)));
 const stableBefore=canonical(read(packetRoot,PACKETS.automatic+'08_automatic_reconcile_delta.sqlinc'));
 const stableAfter=canonical(files.get(SUPPORT+'08_complete_automatic_reconcile_delta_forward.sqlinc'));
 if(changeOnce(stableAfter,"'segment_id','segment_key','segment_stable_key',","'segment_id','segment_key',")!==stableBefore)fail('STABLE_KEY_ONLY_FORWARD_REVERSE_FAILED');
 const stageFacts=['20_stage_request.sqlinc','27_automatic_reconcile.sqlinc','25_bulk_stage_proof.sqlinc'].map(name=>{
  const p='supabase/repeatable/13102026_1438_weekly_source_protection_queries/'+name,b=read(root,p);return {path:p,sha256:hash(b)};});
 let release=read(root,RELEASE).toString('utf8');release=appendVerifier(release,'verificationFiles',VERIFIER);release=appendVerifier(release,'newVerificationFiles',VERIFIER);
 files.set(RELEASE,Buffer.from(release));files.set(VERIFIER,Buffer.from(dedicatedVerifier(names,defs)));
 const outputFacts=[...files].map(([target,b])=>({target,artifact:'generated/'+target+(target.endsWith('.sql')?'inc':''),bytes:b.length,sha256:hash(b),
  beforeSha256:fs.existsSync(inside(root,target))?hash(read(root,target)):null}));
 const rootSql=canonical(files.get(REPEATABLE));let expanded=rootSql;
 for(const m of [...rootSql.matchAll(/^\\ir\s+([^\r\n]+)$/gm)].reverse()){
  const target=path.posix.normalize(path.posix.join(path.posix.dirname(REPEATABLE),m[1]));
  if(!files.has(target)||!target.startsWith(SUPPORT))fail('ROOT_CLOSURE_UNDECLARED_INCLUDE');expanded=expanded.slice(0,m.index)+canonical(files.get(target))+expanded.slice(m.index+m[0].length);
 }
 official.exactAtomicBody(expanded);
 proofs.expandedSourceSha256=hash(expanded);proofs.expandedStatementCount=official.scanManagedSql(expanded).statements.length;
 return {root,packetRoot,packetFacts,metadata,files,outputFacts,inputFacts,proofs,stageFacts,projectionRebases,privateSignatures:names,provenance,
  status:'GUARDED_AUTOMATIC_SOURCE_PLAN_ONLY',counts:{tables:105,private:232,service:142},
  runtime:{compiled:false,qualified:false,exposed:false,capabilities:'ABSENT_AT_SOURCE_BASE_AND_FAIL_CLOSED',prerequisiteV2:'OPERATIONAL_ACTIVATION_REJECTED_NOT_IMPORTED'},
  official};
}
export function validatePrepared(plan) {
 // Complete all CAS/path checks again BEFORE the first permanent write.
 if(plan.outputFacts.length!==TARGETS.length||plan.files.size!==TARGETS.length
  ||new Set(plan.outputFacts.map(f=>f.target)).size!==TARGETS.length
  ||plan.outputFacts.some(f=>!TARGETS.includes(f.target)||!plan.files.has(f.target)
    ||hash(plan.files.get(f.target))!==f.sha256||plan.files.get(f.target).length!==f.bytes))fail('CLOSED_SOURCE_WRITE_SET_CHANGED');
 if(git(plan.root,['rev-parse','HEAD'])!==plan.metadata.observedHead||git(plan.root,['branch','--show-current'])!==ROOT_BRANCH)fail('ROOT_CHANGED_AFTER_PLAN');
 const controlled=['supabase','broker','scripts/cloudtms-managed-release-sql.mjs','scripts/cloudtms-db-release-lib.mjs'];
 const changes=git(plan.root,['diff','--name-only',SOURCE_BASE,'--',...controlled]).split('\n').filter(Boolean);
 if(changes.some(p=>!Object.hasOwn(REBASE,p)))fail('NATIVE_SOURCE_DRIFT_REVIEW_REQUIRED');
 if(git(plan.root,['ls-files','--others','--exclude-standard','--','supabase','broker']))fail('UNREVIEWED_NATIVE_SOURCE_PRESENT');
 for(const f of plan.packetFacts)assertHash(plan.packetRoot,f.path,f.sha256);
 for(const f of plan.inputFacts)assertHash(plan.root,f.path,f.sha256);
 for(const f of plan.outputFacts){const full=inside(plan.root,f.target),old=fs.existsSync(full)?hash(read(plan.root,f.target)):null;
  if(old!==f.beforeSha256)fail('TARGET_CHANGED_AFTER_PLAN:'+f.target);}
 return {status:'SOURCE_PREWRITE_GUARDS_MATCH_NO_WRITES',targets:plan.outputFacts.length};
}
export function applyPrepared(plan,{authorisedSourceWrite=false}={}) {
 if(!authorisedSourceWrite)fail('EXPLICIT_SOURCE_APPLY_FLAG_REQUIRED');
 validatePrepared(plan);
 const written=[];
 try {for(const f of plan.outputFacts){const full=inside(plan.root,f.target);
   const old=fs.existsSync(full)?hash(read(plan.root,f.target)):null;if(old!==f.beforeSha256)fail('TARGET_CHANGED_DURING_APPLY:'+f.target);
   fs.mkdirSync(path.dirname(full),{recursive:true});
   fs.writeFileSync(full,plan.files.get(f.target),{flag:f.beforeSha256===null?'wx':'w'});written.push(f.target);}
  for(const f of plan.outputFacts)if(hash(read(plan.root,f.target))!==f.sha256)fail('SOURCE_WRITE_POSTCONDITION_FAILED:'+f.target);
  for(const f of plan.stageFacts)assertHash(plan.root,f.path,f.sha256);
  for(const f of plan.inputFacts)if(!TARGETS.includes(f.path))assertHash(plan.root,f.path,f.sha256);
 }catch(error){error.completedSourceFiles=written;throw error;}
 return {status:'APPLIED_SOURCE_ONLY_NO_DATABASE_OR_QUALIFICATION',outputs:plan.outputFacts,unchangedStageOwners:plan.stageFacts};
}
export function saveProposal(plan,out=here) {
 for(const f of plan.outputFacts){const full=inside(out,f.artifact);fs.mkdirSync(path.dirname(full),{recursive:true});fs.writeFileSync(full,plan.files.get(f.target));}
 const value={schema_version:'AUTOMATIC_ROOT_INTEGRATOR_RECEIPT_PROPOSAL_V1',status:plan.status,metadata:plan.metadata,counts:plan.counts,
  inputs:plan.inputFacts,frozenPacketInputs:plan.packetFacts,outputs:plan.outputFacts,completeReverseProofs:plan.proofs,unchangedStageOwners:plan.stageFacts,projectionRebases:plan.projectionRebases,
  privateSignatures:plan.privateSignatures,sourceProvenance:plan.provenance,runtime:plan.runtime,
  rootWritesPerformed:false,DBCalls:false,networkCalls:false,bankingSourceRead:false,
  retainedOwnerRule:'All outputs have exact closed destinations; no FIN/Report/CN/A0/native/Bank owner is a target. Source drift requires adjacent review, never an overwrite.',
  resumeRule:'A partial filesystem failure reports completedSourceFiles; do not reset/restore or blindly rerun. Root reviews actual output hashes and reconstructs a new guarded source plan. No filesystem write is claimed transaction-atomic.'};
 fs.writeFileSync(inside(out,'RECEIPT_PROPOSAL.json'),JSON.stringify(value,null,2)+'\n');return value;
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 const args=process.argv.slice(2),get=name=>{const i=args.indexOf(name);return i<0?undefined:args[i+1];};
 const seen=new Set();for(let i=0;i<args.length;i++){
  const key=args[i];if(seen.has(key)||!['--root','--expected-head','--out','--apply'].includes(key))fail('UNKNOWN_OR_DUPLICATE_INTEGRATOR_ARGUMENT');seen.add(key);
  if(key!=='--apply'&&(!args[++i]||args[i].startsWith('--')))fail('INTEGRATOR_ARGUMENT_VALUE_REQUIRED');
 }
 const root=get('--root');if(!root)fail('ROOT_PATH_REQUIRED');
 const plan=await prepare(root,{expectedHead:get('--expected-head')??OBSERVED_HEAD});
 const out=get('--out');const receipt=out?saveProposal(plan,out):{status:plan.status,outputs:plan.outputFacts,counts:plan.counts};
 if(args.includes('--apply')){
  const result=applyPrepared(plan,{authorisedSourceWrite:true});console.log(JSON.stringify({status:result.status,outputs:result.outputs.length}));
 }else console.log(JSON.stringify({status:receipt.status,outputCount:receipt.outputs.length,counts:receipt.counts,rootWritesPerformed:false}));
}

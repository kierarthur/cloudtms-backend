// Finite Health-only source correction. No runtime emission/DB/Bank body.
import fs from'node:fs';import path from'node:path';import{fileURLToPath}from'node:url';import{execFileSync}from'node:child_process';import{createHash}from'node:crypto';import assert from'node:assert/strict';
const here=path.dirname(fileURLToPath(import.meta.url)),own='D:/Development/GitHub/.codex-worktrees/healthroster-checklist-native-14102026',root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026',money='D:/Development/GitHub/.codex-worktrees/healthroster-v2-money-build-13102026',parent='4b1067c859f921855989eb7f147636cebf9286c3',base='docs/healthroster-build/proposals/daily-health-composer-current0910-forward-20261017/',commit='bb6252f46597cf15a6450adfb2af27c1e615fb3d',peer='3dcee2696454c64f99d158a261d0ba5db44f12fa',sha=b=>createHash('sha256').update(b).digest('hex'),git=(at,...a)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',at,...a],{maxBuffer:24e6}),save=(f,b)=>{fs.mkdirSync(path.dirname(here+'/'+f),{recursive:true});fs.writeFileSync(here+'/'+f,b);};
assert(fs.realpathSync.native(here).replaceAll('\\','/').startsWith(own+'/'));assert(!fs.existsSync(here+'/MANIFEST.json'));const old=f=>{const b=git(own,'show',parent+':'+base+f);assert(b.equals(fs.readFileSync(own+'/'+base+f)),f);return b;};
const edits=[];function edit(f,mutate){const before=old(f).toString(),hunks=[];save('reference/parent/'+f,before);let current=before;const replace=(a,b)=>{assert.equal(current.split(a).length,2,f);current=current.replace(a,()=>b);hunks.push({before:a,after:b});};mutate(replace);let reversed=current;for(const h of [...hunks].reverse()){assert.equal(reversed.split(h.after).length,2);reversed=reversed.replace(h.after,()=>h.before);}assert.equal(reversed,before);save(f,current);edits.push({file:f,completeParent:'reference/parent/'+f,parentCommit:parent,parentPath:base+f,beforeSha256:sha(before),afterSha256:sha(current),hunks,fullRawReverse:true});}
const acceptedQuery=` -- Read the actual published revision ordinal after publication. Both IDs and
 -- the new approved financial row are independently bound; no preview+1,
 -- operation billing counter or added publisher return field is authority.
 select v.revision_no into strict accepted_revision_no
 from private.bpay_next_work_revision v join private.bpay_next_work w on w.id=v.work_id
 where v.id=native_apply.revision_id and w.id=native_apply.work_id and w.current_revision_id=v.id
  and w.current_revision_no=v.revision_no and v.financial_snapshot_id=native_apply.financial
  and v.physical_timesheet_id=o.from_timesheet_id and v.physical_timesheet_version=m.from_version
  and w.original_timesheet_id=o.from_timesheet_id and w.booking_id=m.original_booking_id
  and w.candidate_id=o.candidate_id and w.contract_id is null and w.origin_kind='DAILY'
  and w.work_kind='ORDINARY' and w.approval_state='APPROVED'
  and v.source_kind='ORDINARY' and v.approved_at_utc is not null and v.sealed_at_utc is not null;
 if accepted_revision_no is null or accepted_revision_no<=0 then
  raise exception 'DAILY_RATE_ACCEPTED_REVISION_ORDINAL_UNBOUND' using errcode='23514';end if;
`;
edit('health/10_revision_apply.sqlinc',replace=>{replace('as $function$\ndeclare basis jsonb;','as $function$\n<<native_apply>>\ndeclare basis jsonb;');replace(' command uuid; work_id uuid; revision_id uuid; now_utc timestamptz;',' command uuid; work_id uuid; revision_id uuid; accepted_revision_no bigint; now_utc timestamptz;');replace(" receipt:=jsonb_build_object('version','DAILY_RATE_REVISION_RECEIPT_V1'",acceptedQuery+" receipt:=jsonb_build_object('version','DAILY_RATE_REVISION_RECEIPT_V1'");replace("'placement_revision',o.billing_revision::text","'placement_revision',accepted_revision_no::text");});
const retainedQuery=` select jsonb_build_object('accepted_revision_id',v.id,'accepted_revision_no',v.revision_no::text,
  'accepted_financial_id',v.financial_snapshot_id,'work_id',w.id,'current_revision_id',w.current_revision_id,
  'current_revision_no',w.current_revision_no::text,'physical_timesheet_id',v.physical_timesheet_id,
  'physical_timesheet_version',v.physical_timesheet_version::text,'approved_at_utc',v.approved_at_utc,
  'sealed_at_utc',v.sealed_at_utc) into strict retained_binding
 from private.timesheet_contract_move_members m
 join private.bpay_next_work_revision v on v.id=(o.placement_receipt_json->>'revision_id')::uuid
  and v.financial_snapshot_id=m.to_financial_id and v.physical_timesheet_id=m.to_timesheet_id
  and v.physical_timesheet_version=m.to_version
 join private.bpay_next_work w on w.id=v.work_id and w.id=(o.placement_receipt_json->>'work_id')::uuid
 where m.operation_id=o.id and m.from_timesheet_id=o.from_timesheet_id
  and w.original_timesheet_id=o.from_timesheet_id and w.booking_id=m.original_booking_id
  and w.candidate_id=o.candidate_id and w.contract_id is null and w.origin_kind='DAILY' and w.work_kind='ORDINARY'
  and v.source_kind='ORDINARY' and v.revision_no>0 and v.approved_at_utc is not null and v.sealed_at_utc is not null;
`;
edit('templates/03_result.sql.tmpl',replace=>{replace(' result jsonb; placement_revision text; reader_before jsonb;',' result jsonb; placement_revision text; reader_before jsonb; retained_binding jsonb; retained_before jsonb;');replace(' @@BANK_RESULT@@',` -- Verify immutable accepted association by exact indexed native IDs. Old
 -- receipts need no added scalar and the accepted revision need not be current.
 -- No history scan, receipt rewrite or Admin-only mutation context is used.
 if basis->>'state' in('COMPLETE','SUPERSEDED') then
${retainedQuery}  retained_before:=retained_binding;
 end if;
 @@BANK_RESULT@@`);const begin=" if basis->>'state' in('COMPLETE','SUPERSEDED') then\n  placement_revision:=o.placement_receipt_json->>'placement_revision';",start=old('templates/03_result.sql.tmpl').toString(),at=start.indexOf(begin),end=start.indexOf(" else placement_revision:='0';end if;",at)+" else placement_revision:='0';end if;".length;assert(at>=0&&end>at);replace(start.slice(at,end),` if basis->>'state' in('COMPLETE','SUPERSEDED') then
${retainedQuery}  if retained_binding is distinct from retained_before then
   raise exception 'REVIEW_STALE' using errcode='40001';end if;
  -- The genuine current-versus-retained reader/call contract is unprovided.
  -- An accepted ordinal, same physical ID or current pointer alone does not
  -- establish the permitted current five-field Placement/receipt association.
  -- Do not substitute accepted history, a billing counter or zero for it.
  raise exception 'DAILY_RATE_CURRENT_PLACEMENT_READ_UNPROVIDED' using errcode='55000';
 else placement_revision:='0';end if;`);});
const schemas=JSON.parse(git(root,'show',commit+':supabase/release/current-contract.json')).relations.filter(r=>r.schema==='private'&&['bpay_next_work','bpay_next_work_revision'].includes(r.name));assert.equal(schemas.length,2);save('reference/actual_native_relations.json',JSON.stringify(schemas,null,2)+'\n');
const proof=JSON.parse(old('SOURCE_PROOF.json'));for(const f of['OWNER_BLOCKS.json','ASSEMBLY_STATE.json','CURRENT0910_BINDING.json','health/99_acl.sqlinc','verification/mandatory_security.sql','ADDITIVE_REVERSE.sqlinc'])save('reference/parent/'+f,old(f));for(const f of['reference/root/supabase/repeatable/16102026_0858_nhsp_v6_daily_native_authority/02_admission.sqlinc','reference/current0910/supabase/repeatable/17102026_0910_nhsp_v6_daily_role_identity/01_complete_context.sqlinc'])save(f,old(f));
for(const name of['TECHNICAL_SCALAR_DISPOSITION.md','SCALAR_MAPPING_EVIDENCE.json']){const file='docs/healthroster-build/proposals/daily-health-composer-independent-peer-20261017/'+name,b=git(money,'show',peer+':'+file);assert(b.equals(fs.readFileSync(money+'/'+file)));save('reference/peer/'+name,b);}
save('SOURCE_SELECTION.json',JSON.stringify({version:'DAILY_HEALTH_NATIVE_REVISION_MAPPING_FORWARD_V1',completeSourceParent:parent,parentFolder:base,parentSelectionSha256:sha(old('SOURCE_SELECTION.json')),parentManifestSha256:sha(old('MANIFEST.json')),overrides:edits.map(p=>({file:p.file,sha256:p.afterSha256,wholeParent:p.completeParent})),runtimeTargets:[],newSignatures:[],aclChanges:0,registryChanges:0,sourceOnly:true,bankJoins:'THREE_UNPROVIDED',currentPlacementProjection:'EXPLICIT_DEPENDENCY_REFUSAL_NOT_IMPLEMENTED',newOwnerPermissionRequired:false,native:'NOT_RUN'},null,2)+'\n');
save('FORWARD_EDITS.json',JSON.stringify({version:'DAILY_HEALTH_NATIVE_REVISION_MAPPING_EDITS_V1',completeSourceParent:parent,rootSourceParent:commit,independentTechnicalPeer:peer,edits,healthBodiesChanged:2,originalSixHelperBodiesChanged:0,publicShapeChanges:0,bankingBodyReads:0,native:'NOT_RUN'},null,2)+'\n');console.log(JSON.stringify({completeBodiesChanged:2,freshAcceptedOrdinal:'ACTUAL_INDEXED_NATIVE_REVISION',currentPlacement:'UNPROVIDED_TRUTHFUL_REFUSAL',native:'NOT_RUN'}));

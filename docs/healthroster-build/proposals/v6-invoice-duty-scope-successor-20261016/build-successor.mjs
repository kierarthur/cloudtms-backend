// Finite complete Invoice-duty successor. Reads but never edits Source's worktree.
import fs from 'node:fs';
import path from 'node:path';
import {spawnSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {fileURLToPath} from 'node:url';
import assert from 'node:assert/strict';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..');
assert(process.argv[2],'READ_ONLY_SOURCE_WORKTREE_REQUIRED');
const sourceRepo=path.resolve(process.argv[2]);
const sha=value=>createHash('sha256').update(value).digest('hex');
const lf=value=>value.toString('utf8').replaceAll('\r\n','\n');
const git=(cwd,args)=>{
 const r=spawnSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...args],{cwd,maxBuffer:3_000_000,windowsHide:true});
 assert.equal(r.status,0,r.stderr.toString());return r.stdout;
};
const originalCommit='c86a1dfcf0098a26b6f0eac085c95c9b08f81074';
const originalPath='docs/healthroster-build/proposals/v6-invoice-native-20261016/forward/021a_source_duty.sqlinc';
const originalBytes=git(repo,['show',`${originalCommit}:${originalPath}`]),original=lf(originalBytes);
const mapPath='docs/healthroster-build/proposals/v6-invoice-final-mount-20261016/FINAL_MOUNT_SELECTION.json';
const map=JSON.parse(fs.readFileSync(path.join(repo,mapPath)));
const selected=map.selected.find(row=>row.path===originalPath);
assert.equal(sha(originalBytes),selected.rawSha256);assert.equal(sha(original),selected.canonicalLfSha256);
assert.equal(sha(fs.readFileSync(path.join(repo,originalPath))),selected.currentSavedDiskRawSha256);
const before='v_scope:=private.weekly_source_pay_query_scope_v1(p_root_timesheet_id);';
const after='v_scope:=private.weekly_source_invoice_duty_scope_v1(p_root_timesheet_id);';
assert.equal(original.split(before).length,2,'EXACT_SINGLE_DUTY_SCOPE_CALL_REQUIRED');
assert.equal(original.includes(after),false);
const successor=original.replace(before,after);
assert.equal(successor.replace(after,before),original,'COMPLETE_BYTE_REVERSAL_REQUIRED');
const sourceCommit=git(sourceRepo,['rev-parse','HEAD']).toString().trim();
const sourceBase='docs/healthroster-build/proposals/source-pending-current-native-20261016/';
const receiptPath=sourceBase+'SOURCE_RECEIPT.json';
const receiptBytes=git(sourceRepo,['show',`${sourceCommit}:${receiptPath}`]),receipt=JSON.parse(receiptBytes);
const helperPath=sourceBase+'forward/06_invoice_duty_scope.sqlinc',aclPath=sourceBase+'forward/99_acl.sqlinc';
const helperBytes=git(sourceRepo,['show',`${sourceCommit}:${helperPath}`]),helper=lf(helperBytes);
const aclBytes=git(sourceRepo,['show',`${sourceCommit}:${aclPath}`]),acl=lf(aclBytes);
assert.equal(sha(fs.readFileSync(path.join(sourceRepo,helperPath))),sha(helperBytes),'SOURCE_HELPER_SAVED_DISK_MUST_MATCH_COMMIT');
assert.equal(sha(fs.readFileSync(path.join(sourceRepo,aclPath))),sha(aclBytes),'SOURCE_ACL_SAVED_DISK_MUST_MATCH_COMMIT');
assert.equal(sha(helperBytes),receipt.outputs.find(row=>row.path==='forward/06_invoice_duty_scope.sqlinc').sha256);
assert.match(helper,/create or replace function private\.weekly_source_invoice_duty_scope_v1\(p_timesheet_id uuid\)/i);
const returnStart=helper.indexOf("  return jsonb_build_object('root_timesheet_id',v_root.timesheet_id,");
const returnEnd=helper.indexOf('\nend;',returnStart);
assert(returnStart>=0&&returnEnd>returnStart,'EXACT_NATIVE_RETURN_SPAN_REQUIRED');
const nativeReturn=helper.slice(returnStart,returnEnd);
assert.equal(sha(nativeReturn),receipt.derived[0].exact_native_return_sha256);
assert.equal(receipt.derived[0].scope_keys.length,9,'NATIVE_SCOPE_HAS_NINE_KEYS');
assert.match(acl,/alter function private\.weekly_source_invoice_duty_scope_v1\(uuid\) owner to postgres;/i);
assert.match(acl,/revoke all on function private\.weekly_source_invoice_duty_scope_v1\(uuid\) from public,anon,authenticated,service_role;/i);
const header=text=>text.slice(0,text.indexOf('as $function$'));
assert.equal(header(successor),header(original));
const manifest={version:'HEALTH_INVOICE_DUTY_SCOPE_COMPLETE_SUCCESSOR_V1',qualification:'SOURCE_ONLY_NATIVE_NOT_RUN',
 signature:'private.weekly_source_invoice_duty_v1(uuid)',
 original:{commit:originalCommit,path:originalPath,rawSha256:sha(originalBytes),canonicalLfSha256:sha(original),completeStatementSha256:selected.functions[0].statementSha256,headerSha256:sha(header(original))},
 successor:{path:'COMPLETE_DUTY_SUCCESSOR.sqlinc',canonicalLfSha256:sha(successor),bytes:Buffer.byteLength(successor),completeStatementSha256:sha(successor.trim()),headerSha256:sha(header(successor))},
 exactOnlyDelta:{before,after,occurrences:1,completeReverseEqualsOriginal:true},
 sourceHelper:{commit:sourceCommit,path:helperPath,rawSha256:sha(helperBytes),canonicalLfSha256:sha(helper),
  receiptPath,receiptRawSha256:sha(receiptBytes),aclPath,aclRawSha256:sha(aclBytes),
  exactNativeReturnSha256:sha(nativeReturn),scopeKeys:receipt.derived[0].scope_keys},
 mountOrder:['Canonical Root01 schema, complete earlier Physical0713 core and Daily04b/04c/04d',
  'Complete base94 Invoice source assembly including018 (or skip only its superseded complete021a)',
  'Complete finalCandidate55/pending and exact effects/final ACL after Invoice94 per f566 recipe',
  'Complete Source d863 foundation plus Source pending/current successor, dedicated Invoice-duty helper and Source99 private ACL',
  'This complete Invoice-duty successor after the base021a; unchanged final099 owner/private ACL and final browser-isolation closure; complete first use only after all final definitions'],
 preserved:['Original-A Timesheet/Contract/Candidate/Client/week/version identity and genuine exceptional target family',
  'Entire Invoice duty census, discovery basis, movement/upload/operation membership and observation semantics',
  'Complete original duty SECURITY DEFINER/STABLE/search_path and function ABI',
  'Current-pay pending refusal for B with no synthetic current Timesheet or financial row',
  'Original-A invoice/certificate/document/evidence owners and Policy X'],
 native:{postgres17:'NOT_RUN',rollback:'NOT_RUN',api:'NOT_RUN',acl:'NOT_RUN',financial:'NOT_RUN',concurrency:'NOT_RUN'},
 rootIntegrationRequired:true,noDatabaseOrDeployment:true,noBankBodyOrAbiChange:true};
fs.writeFileSync(path.join(here,'COMPLETE_DUTY_SUCCESSOR.sqlinc'),successor);
fs.writeFileSync(path.join(here,'DUTY_SUCCESSOR_MANIFEST.json'),JSON.stringify(manifest,null,2)+'\n');
const files=['COMPLETE_DUTY_SUCCESSOR.sqlinc','DUTY_SUCCESSOR_MANIFEST.json','build-successor.mjs','verify-successor.test.mjs','README.md'];
fs.writeFileSync(path.join(here,'SOURCE_MANIFEST.json'),JSON.stringify({version:'HEALTH_SOURCE_QUALIFICATION_MANIFEST_V1',qualification:'SOURCE_ONLY_NOT_NATIVE_ACCEPTANCE',native:'NOT_RUN',files:Object.fromEntries(files.map(file=>[file,sha(fs.readFileSync(path.join(here,file)))]))},null,2)+'\n');
console.log(JSON.stringify({signature:manifest.signature,bytes:manifest.successor.bytes,sha256:sha(successor),sourceHelperCommit:sourceCommit,completeReverseEqualsOriginal:true}));

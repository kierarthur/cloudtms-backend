import fs from'node:fs';import path from'node:path';import assert from'node:assert/strict';import{fileURLToPath,pathToFileURL}from'node:url';import{createHash}from'node:crypto';
const here=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(here,'../../../../../healthroster-install2-14102026'),forward=path.join(here,'forward'),gatePath='scripts/bpay-next-source-gates.mjs',helperPath='scripts/healthroster-source-classifier-delegation-review.mjs',hash=x=>createHash('sha256').update(x).digest('hex');
const contribution=JSON.parse(fs.readFileSync(path.join(here,'GATE_CONTRIBUTION.json'))),classification=JSON.parse(fs.readFileSync(path.join(forward,contribution.classificationPath))),caller=classification.rows[0],reviewRoot=path.posix.dirname(contribution.classificationPath),readBase=p=>fs.readFileSync(path.join(p.startsWith(reviewRoot+'/')?forward:root,p)),listBase=dir=>fs.readdirSync(path.join(root,dir),{withFileTypes:true}).filter(e=>e.isFile()&&e.name.toLowerCase().endsWith('.sql')).map(e=>dir+'/'+e.name);
const imports=s=>s.replace(/from\s*(['"])(\.\/[^'"]+)\1/g,(_m,q,p)=>'from '+JSON.stringify(pathToFileURL(path.join(root,'scripts',p)).href));
const helper=await import('data:text/javascript;base64,'+Buffer.from(imports(fs.readFileSync(path.join(forward,helperPath),'utf8'))).toString('base64')),{executableParts}=await import(pathToFileURL(path.join(root,gatePath)));
const originalCaller=readBase(caller.physical).toString(),part=source=>executableParts(source,caller.physical).find(p=>p.signature===caller.signature),checks=[],check=(name,fn)=>{fn();checks.push(name);};
function trial({editFile,replaceFile,extraFile}={}){
 const read=p=>replaceFile?.[p]??(editFile?.[p]?Buffer.from(editFile[p](readBase(p).toString())):extraFile?.[p]??readBase(p));
 const list=p=>[...listBase(p),...Object.keys(extraFile??{}).filter(f=>path.posix.dirname(f)===p)];
 return helper.createReviewedSourceClassifierDelegation(root,{readFile:read,listFiles:list});
}
const success=trial();
check('actual-helper-exact-complete-caller-admits-exact-four',()=>assert.deepEqual(success(originalCaller,caller.physical,caller.signature,part(originalCaller)),classification.scope.delegates));
check('whole-statement-only-input-also-exact',()=>{const s=fs.readFileSync(path.join(forward,reviewRoot,'review',caller.bodyPath),'utf8');assert.deepEqual(success(s,caller.physical,caller.signature,part(s)),classification.scope.delegates);});
check('unknown-path-refuses',()=>assert.throws(()=>success(originalCaller,'supabase/repeatable/unknown.sql',caller.signature,part(originalCaller)),/CALLER_PATH_CHANGED/));
check('other-signature-never-gets-four-delegates',()=>assert.deepEqual(success(originalCaller,caller.physical,'private.renamed_source_classifier_v1(uuid)',part(originalCaller)),[]));
check('body-argument-drift-refuses',()=>assert.throws(()=>success(originalCaller,caller.physical,caller.signature,{...part(originalCaller),text:part(originalCaller).text+' perform unknown();'}),/COMPLETE_CALLER_CHANGED/));
for(const[name,edit]of[
 ['changed-caller-complete-header',s=>s.replace('SECURITY DEFINER','SECURITY INVOKER')],
 ['new-unreviewed-edge',s=>s.replace(/\r?\nBEGIN\r?\n/,'\nBEGIN\nPERFORM private.unreviewed_identity_reader_v1();\n')],
 ['renamed-direct-delegate',s=>s.replace('private.timesheet_contract_native_pending_for_v1','private.timesheet_contract_native_pending_other_v1')],
])check(name+'-refuses',()=>{const changed=edit(originalCaller);assert.notEqual(changed,originalCaller);assert.throws(()=>success(changed,caller.physical,caller.signature,part(changed)),/COMPLETE_CALLER_CHANGED/);});
for(const row of classification.rows)check('complete-dependency-file-drift-refuses:'+row.signature,()=>{
 const editFile={[row.physical]:s=>s.replace(fs.readFileSync(path.join(forward,reviewRoot,'review',row.bodyPath),'utf8').trim(),fs.readFileSync(path.join(forward,reviewRoot,'review',row.bodyPath),'utf8').trim()+'\n-- unexpected source drift')};
 assert.notEqual(editFile[row.physical](readBase(row.physical).toString()),readBase(row.physical).toString());assert.throws(()=>trial({editFile})(originalCaller,caller.physical,caller.signature,part(originalCaller)),/SOURCE_DEPENDENCY_CHANGED/);
});
for(const row of classification.rows.slice(1,5))check('actual-direct-delegate-predicate-drift-refuses:'+row.signature,()=>{
 const original=fs.readFileSync(path.join(forward,reviewRoot,'review',row.bodyPath),'utf8').trim(),changed=row.index===2?original.replace('return null;','return \'{}\'::jsonb;'):original.replace(/\band\b/i,'or');
 assert.notEqual(changed,original);const editFile={[row.physical]:s=>s.replace(original,changed)};
 assert.throws(()=>trial({editFile})(originalCaller,caller.physical,caller.signature,part(originalCaller)),/SOURCE_DEPENDENCY_CHANGED/);
});
check('actual-private-ACL-owner-change-refuses',()=>{
 const acl=classification.aclEvents.find(e=>e.statement==='alter function private.timesheet_contract_move_accepting_v1(uuid) owner to current_user;');assert(acl);
 const editFile={[acl.physical]:s=>s.replace(acl.statement,acl.statement.replace('current_user','service_role'))};
 assert.throws(()=>trial({editFile})(originalCaller,caller.physical,caller.signature,part(originalCaller)),/SOURCE_DEPENDENCY_CHANGED|ACTUAL_ACL_CHANGED/);
});
check('caller-final-registry-owner-drift-refuses',()=>assert.throws(()=>trial({editFile:{'docs/bpay-next/final-definer-closures.json':s=>{const r=JSON.parse(s);r.chains.find(r=>r.signature===caller.signature).finalPhysical='supabase/repeatable/unreviewed.sql';return JSON.stringify(r);}}})(originalCaller,caller.physical,caller.signature,part(originalCaller)),/FINAL_SOURCE_OWNER_CHANGED/));
check('delegate-final-registry-owner-drift-refuses',()=>assert.throws(()=>trial({editFile:{'docs/bpay-next/final-definer-closures.json':s=>{const r=JSON.parse(s);r.chains.find(r=>r.signature===classification.rows[1].signature).finalRoot='supabase/repeatable/unreviewed.sql';return JSON.stringify(r);}}})(originalCaller,caller.physical,caller.signature,part(originalCaller)),/FINAL_SOURCE_OWNER_CHANGED/));
for(const suffix of ['CLASSIFICATION.json','review/REVIEW.json','review/EXTRACTION.json','review/bodies/02.sqlinc','review/SOURCE_MANIFEST.json'])check('review-artifact-drift-refuses:'+suffix,()=>assert.throws(()=>trial({editFile:{[reviewRoot+'/'+suffix]:s=>s+' '}})(originalCaller,caller.physical,caller.signature,part(originalCaller)),/CLASSIFICATION_CHANGED|REVIEW_ARTIFACT_CHANGED|REVIEW_EVIDENCE_CHANGED/));
const extra='supabase/repeatable/16102026_2359_unreviewed_source_delegation.sql';
for(const[name,sql,expected]of[
 ['new-final-owner',fs.readFileSync(path.join(forward,reviewRoot,'review',caller.bodyPath),'utf8'),/ACTUAL_DEFINITION_OWNER_CHANGED/],
 ['new-private-grant','grant execute on function private.timesheet_contract_native_pending_for_v1(uuid) to service_role;',/ACTUAL_ACL_CHANGED/],
 ['comment-and-quoted-ACL','grant execute on function "private" /* identity */ . "timesheet_contract_native_pending_for_v1" (uuid) to authenticated;',/ACTUAL_ACL_CHANGED/],
 ['new-broad-ACL','grant execute on all functions in schema private to authenticated;',/ACTUAL_ACL_CHANGED/],
 ['new-broad-default-ACL','alter default privileges in schema private grant execute on functions to service_role;',/ACTUAL_ACL_CHANGED/],
])check(name+'-refuses',()=>assert.throws(()=>trial({extraFile:{[extra]:Buffer.from(sql)}})(originalCaller,caller.physical,caller.signature,part(originalCaller)),expected));
check('unrelated-registry-entry-may-evolve',()=>{const changed=trial({editFile:{'docs/bpay-next/final-definer-closures.json':s=>{const r=JSON.parse(s);r.chains.find(r=>!classification.rows.some(p=>p.signature===r.signature)).owner+=' unrelated metadata';return JSON.stringify(r);}}});assert.deepEqual(changed(originalCaller,caller.physical,caller.signature,part(originalCaller)),classification.scope.delegates);});
check('unrelated-new-root-is-not-generic-exemption',()=>assert.deepEqual(trial({extraFile:{[extra]:Buffer.from('select 1;')}})(originalCaller,caller.physical,caller.signature,part(originalCaller)),classification.scope.delegates));
// Import the actual complete proposed gate, replacing only module resolution with
// the real root imports and the actual helper factory bound to this read-only view.
globalThis.__sourceDelegationReviewReadOnlyFactory=success;
const shim='export const reviewedSourceClassifierDelegates=globalThis.__sourceDelegationReviewReadOnlyFactory;',shimUrl='data:text/javascript;base64,'+Buffer.from(shim).toString('base64');
const proposed=fs.readFileSync(path.join(forward,gatePath),'utf8'),gateModule=await import('data:text/javascript;base64,'+Buffer.from(imports(proposed).replace(JSON.stringify(pathToFileURL(path.join(root,helperPath)).href),JSON.stringify(shimUrl))).toString('base64'));
check('actual-proposed-gate-complete-caller-pass',()=>gateModule.assertRoutineHistoryPoints(originalCaller,caller.physical,caller.signature));
for(const[name,edit,error]of[
 ['history-reader',s=>s.replace(/\r?\nBEGIN\r?\n/,'\nBEGIN\nPERFORM private.bpay_next_payment_history_page_v1();\n'),/BPAY_NEXT_ROUTINE_HISTORY_READER/],
 ['dynamic-EXECUTE',s=>s.replace(/\r?\nBEGIN\r?\n/,"\nBEGIN\nEXECUTE 'select 1';\n"),/BPAY_NEXT_ROUTINE_HISTORY_DYNAMIC/],
 ['new-edge',s=>s.replace(/\r?\nBEGIN\r?\n/,'\nBEGIN\nPERFORM private.unreviewed_identity_reader_v1();\n'),/COMPLETE_CALLER_CHANGED/],
 ['renamed-caller',s=>s.replaceAll('_candidate_expense_source_family_v1','_candidate_expense_source_family_other_v1'),/BPAY_NEXT_ROUTINE_HISTORY_DELEGATION/],
])check('actual-proposed-gate-'+name+'-refuses',()=>{const source=edit(originalCaller),sig=name==='renamed-caller'?'private._candidate_expense_source_family_other_v1(uuid)':caller.signature;assert.throws(()=>gateModule.assertRoutineHistoryPoints(source,caller.physical,sig),error);});
check('constant-empty-map-preserved',()=>assert(proposed.includes('"private._candidate_expense_source_family_v1(uuid)": []')));
check('bounded-edits-full-reverse',()=>{let reverse=proposed;for(const e of[...contribution.edits].reverse()){assert.equal(reverse.split(e.after).length,2);reverse=reverse.replace(e.after,e.before);}assert.deepEqual(Buffer.from(reverse),fs.readFileSync(path.join(here,'baseline',path.basename(gatePath))));assert.equal(hash(reverse),contribution.old.sha256);});
check('root-gate-never-written',()=>assert.equal(hash(fs.readFileSync(path.join(root,gatePath))),contribution.old.sha256));
delete globalThis.__sourceDelegationReviewReadOnlyFactory;
const result={version:'SOURCE_CLASSIFIER_DELEGATION_ACTUAL_HELPER_TESTS_V1',sourceCheckpoint:contribution.sourceCheckpoint,passed:checks.length,failed:0,checks,helperSha256:hash(fs.readFileSync(path.join(forward,helperPath))),classificationSha256:hash(fs.readFileSync(path.join(forward,contribution.classificationPath))),rootEdits:0,sqlChanges:0,bankingEdits:0,dbConnections:0,native:'NOT_RUN'};
fs.writeFileSync(path.join(here,'TEST_RESULTS.json'),JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify({passed:checks.length,failed:0,native:'NOT_RUN'}));

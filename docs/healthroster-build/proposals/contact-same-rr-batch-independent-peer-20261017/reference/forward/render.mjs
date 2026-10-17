import'./batch-overlay.mjs';import assert from'node:assert/strict';import{pathToFileURL}from'node:url';import{ROOT,ADAPTER,put,json}from'./prepare.mjs';
const mod=await import(pathToFileURL(ROOT+'/'+ADAPTER+'/fixed-metadata-reader.mjs').href),identity={physicalDatabase:'cloudtms_test_clone',databaseOid:17,backendPid:101,turnId:'hr_turn_'+'1'.repeat(32),key:[20261013,1]},turn={identity,signal:new AbortController().signal,assertHeld:async()=>{throw Error('OFFLINE_NO_NATIVE');},runOwnedChild:()=>{throw Error('OFFLINE_NO_CHILD');}};
for(const caseId of ['CONTACT_CONSTRUCTORS_V1','QT_ADMIN_CONTROLS_V1']){
 const reader=await mod.createFixedBatchMetadataReader({root:ROOT,turn,caseId}),old=await mod.createFixedMetadataReader({root:ROOT,turn,caseId}),r=reader.inspectSource();assert.equal(r.canonicalSql,old.inspectSource().canonicalSql);assert.equal(r.batch.originalInput,old.inspectSource().input);assert.equal(reader.sourceDescriptor.batch.canonicalStatementExecuted,false);
 put('rendered/'+caseId+'.json',json({sourceIdentity:'SYNTHETIC_SOURCE_ONLY_NOT_NATIVE',...r.batch}));
}
console.log(JSON.stringify({cases:2,gsetBags:2,baseKeys:15,backgroundKeys:6,native:'NOT_RUN'}));

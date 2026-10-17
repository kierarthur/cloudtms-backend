// Fixed subset diagnostics are separate from the canonical snapshot lifecycle.
export async function createFixedComponentDiagnostic({root,turn,caseId}){
 const reader=await createFixedMetadataReader({root,turn,caseId}),original=reader.inspectSource(),managed=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs')).href),built=compileComponentInputs(original,managed,diagnosticMarker);
 const helpers=await import(pathToFileURL(path.join(root,'scripts/healthroster-rehearsal-turn.mjs')).href);let state='READY';
 const run=async mode=>{check(COMPONENTS.includes(mode),'LOCAL_METADATA_COMPONENT_MODE_REFUSED');check(state==='READY','LOCAL_METADATA_COMPONENT_ORDER_REFUSED');state='RUNNING';
  try{assertPins(root);await source(root,caseId);const env=helpers.agencyChildEnvironment('healthroster_contact_qt_fixed_metadata');env.PGCLIENTENCODING='UTF8';env.PGCONNECT_TIMEOUT=String(LIMITS.connectSeconds);env.PGOPTIONS+=' -c default_transaction_read_only=on -c statement_timeout='+LIMITS.statementMilliseconds+' -c lock_timeout='+LIMITS.lockMilliseconds;
   const result=await runFixedChild({input:built.inputs[mode],env,turn,phase:'BEFORE',diagnosticMode:mode}),p=result.peer;check(p.database==='cloudtms_test_clone'&&p.databaseOid===turn.identity.databaseOid&&p.holderPid===turn.identity.backendPid&&p.readerPid!==turn.identity.backendPid&&p.readOnly===true&&p.isolation==='repeatable read'&&p.noRoleSwitch===true,'LOCAL_METADATA_NATIVE_PEER_REFUSED');assertPins(root);state='DONE';return{status:'FIXED_COMPONENT_COMPLETED_NO_RESTORATION_VERDICT',component:result.component,peer:p,progress:result.progress,source:{...built.descriptor,limits:LIMITS},childClosed:result.childClosed,fullCanonicalSnapshotRead:false,restorationQualified:false};
  }catch(error){state='FAILED';throw error;}
 };
 return Object.freeze({component:run,sourceDescriptor:Object.freeze({...built.descriptor,limits:LIMITS}),inspectSource:()=>built});
}

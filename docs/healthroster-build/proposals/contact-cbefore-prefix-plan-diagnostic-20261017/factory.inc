// Separate diagnostic state. It can never supply a snapshot or restoration proof.
export async function createFixedMetadataDiagnostic({root,turn,caseId}){
 const reader=await createFixedMetadataReader({root,turn,caseId}),original=reader.inspectSource();
 const managed=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs')).href);
 const scanned=managed.scanManagedSql(original.input),statements=scanned.statements;
 check(scanned.meta.length===0&&statements.length===6&&statements.map(s=>s.tokens[0]).join('|')==='BEGIN|DO|SELECT|SET|WITH|ROLLBACK','LOCAL_METADATA_EXACT_DIAGNOSTIC_PARENT_REQUIRED');
 const canonical=managed.scanManagedSql(original.canonicalSql);check(canonical.meta.length===0&&canonical.statements.length===2&&canonical.statements[0].tokens[0]==='SET'&&canonical.statements[1].tokens[0]==='WITH','LOCAL_METADATA_EXACT_CANONICAL_PARENT_REQUIRED');
 const pieces=statements.map(s=>original.input.slice(s.start,s.end));
 const wholeQuery=original.canonicalSql.slice(canonical.statements[1].start,canonical.statements[1].end);
 check(pieces[4]===wholeQuery,'LOCAL_METADATA_EXACT_WHOLE_QUERY_REQUIRED');
 const inputs={};for(const mode of ['PREFIX','PLAN']){
  const selected=mode==='PREFIX'?[pieces[0],pieces[1],pieces[2],pieces[3],pieces[5]]:[pieces[0],pieces[1],pieces[2],pieces[3],'EXPLAIN (ANALYZE FALSE, VERBOSE FALSE, COSTS TRUE, SETTINGS FALSE, SUMMARY FALSE, FORMAT JSON) '+wholeQuery,pieces[5]];
  inputs[mode]=selected.map((s,i)=>s+'\n\\echo '+diagnosticMarker(mode,i)+'\n').join('');
 }
 const descriptor=Object.freeze({version:'CONTACT_CBEFORE_FIXED_DIAGNOSTIC_SOURCE_V1',scope:SCOPE,canonicalSqlSha256:sha(original.canonicalSql),wholeQuerySha256:sha(wholeQuery),originalInputSha256:sha(original.input),prefixInputSha256:sha(inputs.PREFIX),planInputSha256:sha(inputs.PLAN),prefixStatements:5,planStatements:6,canonicalRowsRead:false,analyze:false,limits:LIMITS});
 let state='PREFIX';
 const run=async mode=>{
  check(state===mode,'LOCAL_METADATA_DIAGNOSTIC_ORDER_REFUSED');state='RUNNING';assertPins(root);await source(root,caseId);
  const env=helpersEnvironment();
  try{const result=await runFixedChild({input:inputs[mode],env,turn,phase:'BEFORE',diagnosticMode:mode}),p=result.peer;
   check(p.database==='cloudtms_test_clone'&&p.databaseOid===turn.identity.databaseOid&&p.holderPid===turn.identity.backendPid&&p.readerPid!==turn.identity.backendPid&&p.readOnly===true&&p.isolation==='repeatable read'&&p.noRoleSwitch===true,'LOCAL_METADATA_NATIVE_PEER_REFUSED');
   assertPins(root);state=mode==='PREFIX'?'PLAN':'DONE';return{status:mode==='PREFIX'?'EXACT_PREFIX_COMPLETED_NO_SNAPSHOT':'EXACT_ESTIMATED_PLAN_COMPLETED_NO_SNAPSHOT',mode,peer:p,progress:result.progress,...result.plan?{plan:result.plan}:{},source:descriptor,childClosed:result.childClosed,restorationQualified:false};
  }catch(error){state='FAILED';throw error;}
 };
 // This is the same approved environment constructor and fixed limits as C.
 const helpers=await import(pathToFileURL(path.join(root,'scripts/healthroster-rehearsal-turn.mjs')).href);
 function helpersEnvironment(){const env=helpers.agencyChildEnvironment('healthroster_contact_qt_fixed_metadata');env.PGCLIENTENCODING='UTF8';env.PGCONNECT_TIMEOUT=String(LIMITS.connectSeconds);env.PGOPTIONS+=' -c default_transaction_read_only=on -c statement_timeout='+LIMITS.statementMilliseconds+' -c lock_timeout='+LIMITS.lockMilliseconds;return env;}
 return Object.freeze({prefix:()=>run('PREFIX'),plan:()=>run('PLAN'),sourceDescriptor:descriptor,inspectSource:()=>({prefixInput:inputs.PREFIX,planInput:inputs.PLAN,wholeQuery,originalInput:original.input,canonicalSql:original.canonicalSql})});
}


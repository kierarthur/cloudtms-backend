// Read-only source census. Candidate metadata is never written to shared registry.
import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {pathToFileURL,fileURLToPath}from 'node:url';import {execFileSync}from 'node:child_process';
const sha=s=>crypto.createHash('sha256').update(s).digest('hex');
export async function registryCensus(root){
 const load=f=>import(pathToFileURL(path.join(root,'scripts',f)).href);
 const {definerInventory}=await load('bpay-next-final-definers.mjs'),{readManagedClosure,scanManagedSql}=await load('cloudtms-managed-release-sql.mjs');
 const census=definerInventory(),registry=JSON.parse(fs.readFileSync(path.join(root,'docs/bpay-next/final-definer-closures.json'),'utf8'));
 const git=args=>execFileSync('git',['-C',root,...args],{encoding:'utf8',windowsHide:true,maxBuffer:96000000}).trim();
 const rootCommit=git(['rev-parse','HEAD']),baseline='f743b81fe537830d81b1de93ec4f9cb0331532a7';
 const changed=new Set(git(['diff','--name-only',baseline+'..'+rootCommit,'--','supabase']).split(/\r?\n/));
 const registered=new Map(registry.chains.map(e=>[e.signature,e])),rootCache=new Map(),evidence=new Map();
 function ownerEvidence(final){
  if(!final)return [];
  if(!rootCache.has(final.root)){const sql=readManagedClosure(final.root,root).expanded;rootCache.set(final.root,scanManagedSql(sql).statements.map(s=>({sql:sql.slice(s.start,s.end),tokens:s.tokens})));}
  const name=final.signature.split('(')[0],quoted=name.replaceAll('.','\\s*\\.\\s*'),match=new RegExp('\\b'+quoted+'\\s*\\(','i');
  const refs=[];for(const s of rootCache.get(final.root)){
   if(!['ALTER','GRANT','REVOKE','DO'].includes(s.tokens[0]))continue;
   if(!match.test(s.sql)&&!(s.tokens[0]==='DO'&&/has_function_privilege|\b(?:grant|revoke)\b.*\b(?:execute|function)\b/is.test(s.sql)))continue;
   const key=sha(s.sql);evidence.set(key,{sha256:key,root:final.root,sql:s.sql,qualification:match.test(s.sql)?'EXACT_ROUTINE_NAME_STATEMENT':'ROOT_ACL_GUARD_REVIEW_REQUIRED'});refs.push(key);
  }return refs;
 }
 function classify(definitions){const final=definitions.at(-1),changedPhysical=definitions.filter(x=>changed.has(x.physical));
  if(!changedPhysical.length&&!changed.has(final.root))return 'UNCHANGED_I1_OR_HISTORICAL_OWNER_ROOT_REVIEW';
  if(/banking_pay|bpay_/.test(final.physical)&&!/(?:13|14|15|16)102026/.test(final.physical))return 'BANK_OR_SHARED_BOUNDARY_REVIEW_NO_BODY_CHANGE';
  if(/weekly_exceptional_pay|workbench/.test(final.signature))return 'HEALTH_SOURCE_BANK_CONSUMER_BOUNDARY_ROOT_REVIEW';
  if(/invoice/.test(final.signature))return 'HEALTH_INVOICE_OWNER_REVIEW';
  return 'HEALTH_CURRENT_OWNER_REVIEW';
 }
 const mismatches=[],unregistered=[];
 for(const [signature,definitions]of census.routines){
  const expected=registered.get(signature),final=definitions.at(-1),unregisteredPhysical=[...new Set(definitions.filter(d=>!expected?.allowedPhysical.includes(d.physical)).map(x=>x.physical))];
  const common={signature,classification:classify(definitions),actualFinal:final,definitions,aclEvidence:[...new Set(definitions.flatMap(d=>ownerEvidence(d)))],finalRootAclEvidence:ownerEvidence(final),aclRule:'CREATE_OR_REPLACE_RETAINS_NATIVE_OWNER_ACL_UNLESS_EXACT_LATER_ALTER_GRANT_REVOKE_CHANGES_IT'};
  if(expected&&(final.root!==expected.finalRoot||final.physical!==expected.finalPhysical||unregisteredPhysical.length)){
   const proposed={...expected,finalRoot:final.root,finalPhysical:final.physical,
    allowedPhysical:[...new Set([...expected.allowedPhysical,...definitions.map(x=>x.physical)])],
    predecessors:[...new Set([...expected.predecessors,...definitions.map(x=>x.root).filter(x=>x!==final.root)])],finalClosures:[...new Set([...expected.finalClosures,final.root])]};
   mismatches.push({...common,expected,unregisteredPhysical,proposed,proposalStatus:'FINITE_CANDIDATE_ROOT_REVIEW_REQUIRED_NOT_APPLIED'});
  }else if(!expected&&new Set(definitions.map(x=>x.root)).size>1)unregistered.push({...common,proposed:{signature,finalRoot:final.root,finalPhysical:final.physical,
   allowedPhysical:[...new Set(definitions.map(x=>x.physical))],predecessors:[...new Set(definitions.map(x=>x.root).filter(x=>x!==final.root))],finalClosures:[final.root],
   owner:'Health V6 current source census; '+common.classification,baselineSource:baseline},proposalStatus:'EXPLICIT_SOURCE_ONLY_CHAIN_ROOT_REVIEW_REQUIRED_NOT_APPLIED'});
 }
 for(const row of [...mismatches,...unregistered]){row.nativeAclQualification='NOT_RUN';row.finalExactAclEvidence=row.finalRootAclEvidence.filter(h=>evidence.get(h).qualification==='EXACT_ROUTINE_NAME_STATEMENT');row.inheritedSourceAclEvidence=row.aclEvidence.filter(h=>evidence.get(h).qualification==='EXACT_ROUTINE_NAME_STATEMENT');row.aclCaveat='Source statements and CREATE_OR_REPLACE retention are not installed owner/ACL proof; root must check native current catalogue.';}
 const counts=rows=>Object.fromEntries([...new Set(rows.map(x=>x.classification))].sort().map(c=>[c,rows.filter(x=>x.classification===c).length]));
 return {version:'CONTACT_NATIVE_FINAL_REGISTRY_REVIEW_CENSUS_V1',rootCommit,baseline,registrySourceHash:sha(fs.readFileSync(path.join(root,'docs/bpay-next/final-definer-closures.json')).toString('utf8').replaceAll('\r\n','\n')),
  status:'SOURCE_CANDIDATES_REVIEW_REQUIRED_NATIVE_NOT_RUN',mismatches,unregisteredRepeatedSignatures:unregistered,
  counts:{mismatches:mismatches.length,unregisteredRepeatedSignatures:unregistered.length,mismatchClasses:counts(mismatches),unregisteredClasses:counts(unregistered)},
  aclStatements:[...evidence.values()],candidateEntries:[...mismatches,...unregistered].map(x=>({classification:x.classification,proposalStatus:x.proposalStatus,entry:x.proposed})),sharedChangesApplied:0};
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 const root=process.argv.find(x=>x.startsWith('--root='))?.slice(7);if(!root)throw Error('--root required');
 const result=await registryCensus(path.resolve(root));fs.writeFileSync(path.join(path.dirname(fileURLToPath(import.meta.url)),'REGISTRY_REVIEW_CENSUS.json'),JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify({rootCommit:result.rootCommit,...result.counts}));
}

// Complete, read-only three-root expansion review. Banking includes are read
// unchanged for provenance; this proposal authors no included function body.
import fs from 'node:fs';import path from 'node:path';import {execFileSync} from 'node:child_process';import {fileURLToPath,pathToFileURL} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url));
const root='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-install2-14102026';
const baseline='fae51b0e0bb33e3e3d62cce47bb6aa96da9b3fbe';
const api=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs')).href);
const effect="notify pgrst, 'reload schema';";
const specs=[['13102026_1705_weekly_source_invoice_adjustments_v1.sql','82a4edda0a81355c75a0a74d1b4494d991c6be8ea6d5ebf0e8f10c9a901bcd15','beforeCommit'],
 ['28092026_1234_stage2_hosted_final_authority_reassert_v1.sql','b7b5dc446ee62b11818362ce5880667acef4f1e4b02b1d3c32962ae637a10b3c','afterIncludes'],
 ['13102026_1706_healthroster_automatic_credit_candidates_v1.sql','aa77a2c396b0b7c5c26edb4b80159cd76b5182b2edbb7d6debe975a73f167c61','beforeCommit']];
const gitRead=file=>api.canonical(execFileSync('git',['show',baseline+':'+file],{cwd:root,encoding:'utf8'}));
function gitClosure(file,stack=[]){
 if(stack.includes(file))throw Error('SOURCE_INCLUDE_CYCLE');const text=gitRead(file),parsed=api.scanManagedSql(text),ordered=[{path:file,source:text}];let expanded=text;
 const replacements=[];for(const meta of parsed.meta.filter(m=>/^\\ir\s/i.test(m.text))){
  const m=meta.text.match(/^\\ir\s+(?:'([^']+)'|"([^"]+)"|([^\s;]+))\s*;?\s*$/i);if(!m)throw Error('EXACT_SOURCE_INCLUDE_INVALID');
  const child=gitClosure(path.posix.normalize(path.posix.join(path.posix.dirname(file),m[1]??m[2]??m[3])),[...stack,file]);ordered.push(...child.ordered);replacements.push({...meta,value:child.expanded});
 }for(const row of replacements.reverse())expanded=expanded.slice(0,row.start)+row.value+expanded.slice(row.end);
 return{ordered,expanded,closureHash:api.digest(Buffer.concat(ordered.flatMap(r=>[Buffer.from(r.path+'\0'),Buffer.from(r.source),Buffer.from('\0')])))};
}
const functions=text=>api.scanManagedSql(text).statements.filter(s=>s.tokens[0]==='CREATE'&&s.tokens.slice(0,5).includes('FUNCTION')).map(s=>api.digest(text.slice(s.start,s.end)));
const results=[];
for(const [name,expectedHash,placement]of specs){
 const file='supabase/repeatable/'+name,before=gitRead(file),saved=api.canonical(fs.readFileSync(path.join(root,file),'utf8'));
 if(api.digest(saved)!==expectedHash)throw Error('CURRENT_REVIEWED_ROOT_HASH_CHANGED');
 const expected=placement==='beforeCommit'?before.replace(/commit;\s*$/i,effect+'\ncommit;\n'):before+effect+'\n';
 if(saved!==expected)throw Error('ONLY_EXACT_FINAL_NOTIFY_REQUIRED');
 const beforeClosure=gitClosure(file),after=api.readManagedClosure(file,root);
 if(JSON.stringify(beforeClosure.ordered.map(r=>r.path))!==JSON.stringify(after.ordered.map(r=>r.path)))throw Error('INCLUDE_ORDER_CHANGED');
 const inputs=after.ordered.map((r,i)=>{const prior=beforeClosure.ordered[i];if(i&&prior.source!==r.source)throw Error('COMPLETE_INCLUDED_BYTES_CHANGED');return{file:r.path,beforeSha256:api.digest(prior.source),afterSha256:api.digest(r.source),unchanged:prior.source===r.source};});
 const position=after.expanded.lastIndexOf(effect);if(position<0||after.expanded.slice(0,position)+after.expanded.slice(position+effect.length+1)!==beforeClosure.expanded)throw Error('COMPLETE_EXPANDED_REVERSE_FAILED');
 const oldFunctions=functions(beforeClosure.expanded),newFunctions=functions(after.expanded);if(JSON.stringify(oldFunctions)!==JSON.stringify(newFunctions))throw Error('COMPLETE_FUNCTION_BYTES_CHANGED');
 for(const [which,text]of [['before',before],['after',saved]]){const dest=path.join(here,'roots',which,name);fs.mkdirSync(path.dirname(dest),{recursive:true});fs.writeFileSync(dest,text);}
 results.push({file,placement,beforeSha256:api.digest(before),afterSha256:api.digest(saved),beforeClosureSha256:beforeClosure.closureHash,afterClosureSha256:after.closureHash,
  rawRootReverseEqual:true,completeExpandedReverseEqual:true,completeFunctionHashesUnchanged:true,completeFunctionStatements:oldFunctions.length,functions:oldFunctions,
  includedOccurrences:after.ordered.length-1,uniqueIncludedFiles:new Set(after.ordered.slice(1).map(r=>r.path)).size,inputs});
}
const evidence={version:'THREE_ROOT_EXACT_POSTGREST_RELOAD_PEER_V1',baselineRootCommit:baseline,currentRootHead:execFileSync('git',['rev-parse','HEAD'],{cwd:root,encoding:'utf8'}).trim(),
 verdict:'NO_SOURCE_BLOCKER_THREE_NOTIFY_ONLY_SUCCESSORS',effect,results,sourceGate73RpcDiagnostic:'Root reported selector PASS0; no independent full-gate PASS substituted',
 I2Edits:0,BankingBodyEdits:0,databaseCalls:0,native:'NOT_RUN',PostgrestApiCache:'NOT_RUN',sourceGateWaiver:false};
fs.writeFileSync(path.join(here,'EVIDENCE.json'),JSON.stringify(evidence,null,2)+'\n');
fs.writeFileSync(path.join(here,'REVIEW.md'),`No source blocker was found in the three exact NOTIFY-only successors. The two transactional Health roots place the notification after their final grants/ACL includes and immediately before the original COMMIT. The dated shared final-authority reassert wrapper places it after all five unchanged direct includes; it adds no BEGIN/COMMIT and changes no Banking-owned included body. Included definitions have completed their original transactions before the final wrapper notification.\n\nFor every root I compared its complete saved bytes with fae51b0e, expanded the full committed predecessor include tree and current saved tree, and reversed only that final notification. All root/expanded byte reverses, include order/bytes and complete function statement hashes match. Root must retain the canonical managed closure successor hashes and rerun its required source/integrity/contract gates. The earlier227174 single-root peer review remains unchanged as historical evidence.\n\nNOTIFY requests PostgREST cache refresh after the definitions/grants are visible; it proves no native installation, actual cache refresh or API success. The reported73-RPC selector PASS is parent context, not an independent native/full-gate result. No I2 edit, database call, Banking body edit or gate waiver occurred in this lane.\n`);
console.log(JSON.stringify({verdict:evidence.verdict,results:results.map(r=>({file:r.file,functions:r.completeFunctionStatements,includes:r.includedOccurrences,afterRootSha256:r.afterSha256,afterClosureSha256:r.afterClosureSha256})),native:'NOT_RUN'}));

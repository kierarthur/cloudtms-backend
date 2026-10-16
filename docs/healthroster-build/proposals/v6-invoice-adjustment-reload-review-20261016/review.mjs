// Read-only root peer review; writes this Report evidence folder only.
import fs from 'node:fs';
import path from 'node:path';
import {execFileSync} from 'node:child_process';
import {fileURLToPath,pathToFileURL} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url));
const root='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-install2-14102026';
const baseline='fae51b0e0bb33e3e3d62cce47bb6aa96da9b3fbe';
const file='supabase/repeatable/13102026_1705_weekly_source_invoice_adjustments_v1.sql';
const sql=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs')).href);
const saved=sql.canonical(fs.readFileSync(path.join(root,file),'utf8'));
const before=sql.canonical(execFileSync('git',['show',baseline+':'+file],{cwd:root,encoding:'utf8'}));
if(before.split('commit;').length!==2||before.includes('reload schema')||saved!==before.replace('commit;',"notify pgrst, 'reload schema';\ncommit;"))throw Error('BOUNDED_NOTIFY_SUCCESSOR_MISMATCH');
const afterClosure=sql.readManagedClosure(file,root),rows=[];
for(const input of afterClosure.ordered){
 const predecessor=sql.canonical(execFileSync('git',['show',baseline+':'+input.path],{cwd:root,encoding:'utf8'}));
 if(input.path!==file&&predecessor!==input.source)throw Error('INCLUDE_SOURCE_DRIFT');
 for(const [which,text]of [['before',predecessor],['after',input.source]]){const out=path.join(here,'inputs',which,input.path);fs.mkdirSync(path.dirname(out),{recursive:true});fs.writeFileSync(out,text);}
 rows.push({file:input.path,beforeSha256:sql.digest(predecessor),afterSha256:sql.digest(input.source),unchanged:predecessor===input.source});
}
const beforeClosure=sql.readManagedClosure(file,path.join(here,'inputs/before'));
const effect="notify pgrst, 'reload schema';";
if(afterClosure.expanded.split(effect).length!==2||afterClosure.expanded.replace(effect+'\n','')!==beforeClosure.expanded)throw Error('COMPLETE_EXPANDED_CLOSURE_REVERSE_FAILED');
const functions=text=>sql.scanManagedSql(text).statements.filter(s=>s.tokens[0]==='CREATE'&&s.tokens.slice(0,5).includes('FUNCTION')).map(s=>sql.digest(text.slice(s.start,s.end)));
const beforeFns=functions(beforeClosure.expanded),afterFns=functions(afterClosure.expanded);
if(JSON.stringify(beforeFns)!==JSON.stringify(afterFns))throw Error('COMPLETE_FUNCTION_BYTES_CHANGED');
const evidence={version:'INVOICE_ADJUSTMENT_POSTGRST_RELOAD_PEER_V1',baselineRootCommit:baseline,currentRootHead:execFileSync('git',['rev-parse','HEAD'],{cwd:root,encoding:'utf8'}).trim(),file,
 beforeRootSha256:sql.digest(before),afterRootSha256:sql.digest(saved),beforeClosureSha256:beforeClosure.closureHash,afterClosureSha256:afterClosure.closureHash,
 exactInsertion:effect,position:'After06/07 final ACL includes, immediately before original COMMIT',fullRawRootReverseEqual:true,completeExpandedClosureReverseEqual:true,
 includesUnchanged:rows.filter(r=>r.file!==file).length,completeFunctionStatements:beforeFns.length,completeFunctionHashesUnchanged:true,functions:beforeFns,inputs:rows,
 verdict:'NO_SOURCE_BLOCKER_BOUNDED_NOTIFY_SUCCESSOR',native:'NOT_RUN',PostgrestApiCache:'NOT_RUN',I2Edits:0,sourceGateWaiver:false};
fs.writeFileSync(path.join(here,'EVIDENCE.json'),JSON.stringify(evidence,null,2)+'\n');
fs.writeFileSync(path.join(here,'REVIEW.md'),`The exact root successor adds only ${effect} after both final ACL includes and immediately before the existing COMMIT. This is the required transactional placement for the reported RPC reload source gate. The complete raw root and expanded closure reverse exactly after removing that one line; all ${rows.length-1} complete include sources and ${beforeFns.length} complete function statement hashes remain unchanged. No function, grant, transaction boundary, Banking body or business behavior is changed.\n\nThe notification is delivered at transaction commit, after the definitions/grants it describes are visible. It requests the existing PostgREST schema-cache reload; it does not prove cache refresh, API-path success or a deployed/native result. Root must retain its ordinary source/integrity/closure gates and later genuine PostgREST acceptance. No gate waiver or I2/database edit was performed by this peer lane.\n`);
console.log(JSON.stringify({verdict:evidence.verdict,includes:evidence.includesUnchanged,functions:evidence.completeFunctionStatements,beforeRootSha256:evidence.beforeRootSha256,afterRootSha256:evidence.afterRootSha256,afterClosureSha256:evidence.afterClosureSha256,native:'NOT_RUN'}));

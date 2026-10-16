// Source-only complete selected exception census. No execution or envelope removal.
import fs from 'node:fs';import path from 'node:path';import {pathToFileURL,fileURLToPath}from 'node:url';import {execFileSync}from 'node:child_process';
const root=path.resolve(process.argv.find(x=>x.startsWith('--root='))?.slice(7)??'');
const load=f=>import(pathToFileURL(path.join(root,'scripts',f)).href);
const {inventory}=await load('cloudtms-db-release-lib.mjs'),{pendingInventory}=await load('cloudtms-managed-release.mjs'),{closeFinalDefinerSelection}=await load('bpay-next-final-definers.mjs'),
 {readManagedClosure,scanManagedSql}=await load('cloudtms-managed-release-sql.mjs'),{MANAGED_EXCEPTIONS}=await load('cloudtms-managed-execution-policy.mjs'),
 {collectInstall2ReviewedInstalledSource}=await load('healthroster-install2-installed-source.mjs');
const installed=collectInstall2ReviewedInstalledSource({repositoryRoot:root,installedSourceCommit:'f743b81fe537830d81b1de93ec4f9cb0331532a7'}),current=inventory(),pending=pendingInventory(current,installed.inventory),closed=closeFinalDefinerSelection(pending,current),selected=new Set([...closed.migrations,...closed.repeatables].map(x=>x.path));
const rows=[];for(const e of MANAGED_EXCEPTIONS.filter(x=>selected.has(x.path))){
 const closure=readManagedClosure(e.path,root),parsed=scanManagedSql(closure.expanded);
 const dos=parsed.statements.filter(x=>['DO','CALL'].includes(x.tokens[0])).map(x=>{const s=parsed.source.slice(x.start,x.end);return{head:x.tokens[0],tag:s.match(/^do\s+(\$[a-z0-9_]*\$)/i)?.[1]??null,
  calls:[...new Set([...s.matchAll(/\b((?:private|public)\.[a-z_][a-z0-9_]*)\s*\(/gi)].map(x=>x[1]))],
  bodyEffectIndicators:{insert:/\binsert\s+into\b/i.test(s),update:/\bupdate\s+[a-z_]/i.test(s),delete:/\bdelete\s+from\b/i.test(s),truncate:/\btruncate\b/i.test(s),ddl:/\b(?:create|alter|drop)\b/i.test(s),dynamicExecute:/\bexecute\b/i.test(s)}};});
 rows.push({path:e.path,reason:e.reason,pinnedClosureHash:e.sha256,actualClosureHash:closure.closureHash,hashMatches:e.sha256===closure.closureHash,
  transactions:parsed.statements.filter(x=>['BEGIN','COMMIT','ROLLBACK','START','END','ABORT','PREPARE'].includes(x.tokens[0])).map(x=>x.tokens.join(' ')),
  transactionCount:parsed.statements.filter(x=>['BEGIN','COMMIT','ROLLBACK','START','END','ABORT','PREPARE'].includes(x.tokens[0])).length,
  statements:parsed.statements.length,metaCommands:[...new Set(parsed.meta.map(x=>x.text.split(/\s/)[0]))],
  auditedExistingRationale:e.rationale,topLevelDoCall:dos,executionQualification:'NOT_REVIEWED_FOR_FULL_SINGLE_TRANSACTION_HARNESS',
  effectCaveat:'Indicators are conservative lexical source facts, not effect proof; existing rationale plus every actual selected DO/CALL and complete callee body needs root review.'});
}
const result={version:'CONTACT_NATIVE_ALL_SELECTED_MANAGED_EXCEPTIONS_V1',rootCommit:execFileSync('git',['-C',root,'rev-parse','HEAD'],{encoding:'utf8'}).trim(),
 currentCounts:{migrations:current.migrations.length,repeatables:current.repeatables.length},pendingCounts:{migrations:pending.migrations.length,repeatables:pending.repeatables.length},
 selectedCounts:{migrations:closed.migrations.length,repeatables:closed.repeatables.length},rows,databaseCalls:0,removedEnvelopeCommands:0,native:'NOT_RUN'};
const file=path.join(path.dirname(fileURLToPath(import.meta.url)),'ALL_SELECTED_MANAGED_EXCEPTIONS.json');fs.writeFileSync(file,JSON.stringify(result,null,2)+'\n');
console.log(JSON.stringify({rootCommit:result.rootCommit,selected:result.selectedCounts,exceptions:rows.map(x=>({path:x.path,reason:x.reason,hash:x.actualClosureHash,transactionCount:x.transactionCount,meta:x.metaCommands,DO_CALL:x.topLevelDoCall.length}))}));

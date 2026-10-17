// Offline input derivation. Proposed B is only the Kier server-owned tool.
import {buildInput as originalBuild,source,proofSql,requireThat,sortWriters,literal,holderProofSql} from './canonical-input-builder.mjs';
import {sha} from '../v6-contact-six-lock-contention-20261016/prepare.mjs';
export {source,proofSql,requireThat,sortWriters,literal,holderProofSql,sha};
export const ROUTE='mcp__cloudtms_miget_operations_kier_codex__miget_db_transaction_rollback';
export async function buildInput({root,caseId,identity}){
 const original=await originalBuild({root,caseId,identity}),{rehearsal,sql,plan}=await source(root);
 const row=plan.cases.find(c=>c.id===caseId),scan=sql.scanManagedSql(original.input.sql);
 const removedIndex=original.receipt.indices.beforeSnapshot,span=scan.statements[removedIndex-1];
 requireThat(span.tokens[0]==='WITH','EXACT_SINGLE_CANONICAL_SNAPSHOT_REQUIRED');
 const removed=original.input.sql.slice(span.start,span.end),text=original.input.sql.slice(0,span.start)+original.input.sql.slice(span.end);
 requireThat(text.slice(0,span.start)+removed+text.slice(span.start)===original.input.sql,'WHOLE_ORIGINAL_B_RAW_REVERSE_REQUIRED');
 const next=sql.scanManagedSql(text);
 requireThat(next.statements.length===scan.statements.length-1&&text.trimEnd().endsWith(row.commands.at(-1)),'EXACT_FINAL_TARGET_REQUIRED');
 const kept=scan.statements.filter((_,i)=>i!==removedIndex-1).map(s=>original.input.sql.slice(s.start,s.end));
 requireThat(JSON.stringify(kept)===JSON.stringify(next.statements.map(s=>text.slice(s.start,s.end))),'EVERY_RETAINED_STATEMENT_BYTES_REQUIRED');
 const remap=i=>{requireThat(i!==removedIndex,'REMOVED_SNAPSHOT_INDEX_REFUSED');return i>removedIndex?i-1:i;};
 const indices=Object.fromEntries(Object.entries(original.receipt.indices).filter(([k])=>k!=='beforeSnapshot').map(([k,v])=>[k,Array.isArray(v)?v.map(remap):remap(v)]));
 const verification="select pg_catalog.set_config('search_path','pg_catalog, public',true);\n"+rehearsal.identityProbeSql(plan.marker)+'\n'+proofSql(row,identity,0);
 const oldFresh=sql.scanManagedSql(original.input.verification_sql),newFresh=sql.scanManagedSql(verification);
 requireThat(oldFresh.statements.length===3&&newFresh.statements.length===3,'FINITE_FRESH_REPLACEMENT_REQUIRED');
 for(const i of [0,2])requireThat(original.input.verification_sql.slice(oldFresh.statements[i].start,oldFresh.statements[i].end)
  ===verification.slice(newFresh.statements[i].start,newFresh.statements[i].end),'FRESH_CONTROL_PROOFS_UNCHANGED_REQUIRED');
 const input={...original.input,sql:text,verification_sql:verification};
 const receipt={...original.receipt,version:'CONTACT_SIX_FIXED_LOCAL_METADATA_MCP_INPUT_V1',sqlSha256:sha(text),verificationSqlSha256:sha(verification),
  statements:next.statements.length,indices,largeSnapshotsInMcp:false,metadataRoute:'OWNER_FIXED_READ_ONLY_LOCAL_C_BEFORE_AND_FRESH_C_AFTER',
  beforeCanonicalSqlSha256:sha(original.input.sql.slice(scan.statements[removedIndex-2].start,span.end)),
  original:{sqlSha256:original.receipt.sqlSha256,verificationSqlSha256:original.receipt.verificationSqlSha256,statements:scan.statements.length,
   removedStatement:removedIndex,removedStart:span.start,removedEnd:span.end,removedSha256:sha(removed),wholeRawReverse:true,everyOtherStatementUnchanged:true,
   freshOnlyStatement2Replaced:true,freshOtherStatementBytesUnchanged:true},
  native:'NOT_RUN'};
 return {input,receipt,reverse:{removedStatementSql:removed,originalSql:original.input.sql,originalVerificationSql:original.input.verification_sql}};
}

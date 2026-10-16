import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {buildInput as priorBuild,source,saveInput} from '../v6-contact-six-lock-mcp-successor-20261016/input-builder.mjs';
import {wrapCanonicalQuery,hash,MAX_PARTS,closedCliError} from './frame-transport.mjs';
export {source,sortWriters,holderProofSql,holderGuard,proofSql,requireThat,literal} from '../v6-contact-six-lock-mcp-successor-20261016/input-builder.mjs';
export {saveInput};
export async function buildInput(options){
 const prior=await priorBuild(options),{sql}=await source(options.root),index=prior.receipt.indices.beforeSnapshot-1;
 const statement=sql.scanManagedSql(prior.input.sql).statements[index],query=prior.input.sql.slice(statement.start,statement.end);
 const transport=wrapCanonicalQuery(query,'CANONICAL_SNAPSHOT');
 for(const key of ['sql','verification_sql']){
  if(prior.input[key].split(query).length!==2)throw Error('CANONICAL_SNAPSHOT_SINGLE_OCCURRENCE_REQUIRED');
  prior.input[key]=prior.input[key].replace(query,transport.sql);
 }
 prior.input.max_rows_per_statement=MAX_PARTS;
 prior.receipt={...prior.receipt,version:'CONTACT_SIX_LOCK_KIER_MCP_CHUNKED_INPUT_V1',
  historicalScalarInputSha256:prior.receipt.sqlSha256,sqlSha256:hash(prior.input.sql),verificationSqlSha256:hash(prior.input.verification_sql),
  transport,canonicalSnapshotQuery:query,canonicalSnapshotFullReverse:true};
 if(sql.scanManagedSql(prior.input.sql).statements.length!==prior.receipt.statements)throw Error('CHUNK_WRAPPER_STATEMENT_COUNT_CHANGED');
 return prior;
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 try{const [root,out,caseId]=process.argv.slice(2),identity=JSON.parse(fs.readFileSync(path.join(out,'TURN_IDENTITY.json'),'utf8'));saveInput(out,await buildInput({root,caseId,identity}));console.log('BOUNDED_CHUNKED_MCP_INPUT_BUILT_NOT_EXECUTED');}
 catch(error){console.error(JSON.stringify(closedCliError(error)));process.exitCode=1;}
}

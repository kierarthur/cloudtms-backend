import fs from 'node:fs';import path from 'node:path';import {fileURLToPath,pathToFileURL} from 'node:url';
const root='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-v2-backend-13102026',out=path.dirname(fileURLToPath(import.meta.url));
const {digest,scanManagedSql}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs')));
const {neutraliseReviewedVerifierEnvelope,renderFullRehearsalUnits}=await import(pathToFileURL(path.join(root,'scripts/healthroster-full-rehearsal-protocol.mjs')));
const file='supabase/verification/22092026_1052_weekly_source_operation_local_validation_v1.sql',sql=fs.readFileSync(path.join(root,file),'utf8'),unit={file,kind:'verifier',sql,sqlHash:digest(sql)};
const review={sourceSqlHash:unit.sqlHash,readOnlyBodyReviewed:true,removedCommands:['\\set ON_ERROR_STOP on','BEGIN READ ONLY','ROLLBACK']},neutral=neutraliseReviewedVerifierEnvelope(unit,review),rendered=renderFullRehearsalUnits([neutral]);
const native=scanManagedSql(sql).statements.find(s=>s.tokens[0]==='DO'),body=sql.slice(native.start,native.end);
if(!neutral.sql.includes(body)||!rendered.sql.includes(body)||neutral.envelope.length!==3)throw Error('NATIVE_ASSERTION_BODY_DRIFT');
const receipt={status:'SOURCE_ONLY_ENVELOPE_CHECK_PASS_NO_DATABASE_EXECUTION',file,sourceSqlHash:unit.sqlHash,nativeAssertionBodySha256:digest(body),nativeAssertionBodyRetainedByteExact:true,neutralSqlSha256:neutral.sqlHash,removedEnvelope:neutral.envelope,renderedSha256:rendered.sha256,scope:'Complete saved body reread: catalogue/helper/role/trigger bindings only. Removal review applies only to this exact source hash and explicit three-command envelope. No post-Banking plan, installed check, permission to execute, or native/full rehearsal proof.'};
for(const dir of [out,path.join(root,'docs/healthroster-build/checkpoint13102026')])fs.writeFileSync(path.join(dir,'READ_ONLY_VERIFIER_ENVELOPE_SOURCE_CHECK.json'),JSON.stringify(receipt,null,2)+'\n');console.log(JSON.stringify(receipt));


// Offline source evidence only. Reads no credential file and opens no connection.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {fileURLToPath} from 'node:url';
import {execFileSync} from 'node:child_process';
const here=path.dirname(fileURLToPath(import.meta.url));
const workspace='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub';
const root=path.join(workspace,'.codex-worktrees/healthroster-install2-14102026');
const backend=path.join(workspace,'cloudtms-backend');
const sha=v=>crypto.createHash('sha256').update(v).digest('hex');
const git=(cwd,...args)=>execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...args],{cwd,encoding:'utf8',windowsHide:true}).trim();
const row=(base,file)=>({file,bytes:fs.statSync(path.join(base,file)).size,sha256:sha(fs.readFileSync(path.join(base,file)))});
const rootFiles=['scripts/healthroster-install1-full-rehearsal.mjs','scripts/healthroster-live-test-restoration.mjs',
 'scripts/healthroster-rehearsal-turn.mjs','scripts/healthroster-turn-control.mjs','scripts/cloudtms-managed-release-sql.mjs',
 'supabase/release/export_contract.sql','supabase/release/current-contract.json'];
const ownerFiles=['codex_outputs/claude-stage2-continuation-20260925/SEND_5_TO_HEALTHROSTER_BUILD_STAGE_NONSTOP.md',
 'codex_outputs/claude-stage2-continuation-20260925/SEND_5B_TO_HEALTHROSTER_FULL_REHEARSAL.md',
 'codex_outputs/claude-stage2-continuation-20260925/SEND_7_TEST_REHEARSAL_TURN_TAKING.md','AGENTS.md'];
const sourceFiles=['infra/miget/cloudtms-miget-operations/src/index.ts','infra/miget/cloudtms-miget-operations/src/rollback-transaction.ts',
 'infra/miget/cloudtms-miget-operations/package.json','infra/miget/cloudtms-miget-operations/node_modules/agents/dist/mcp/index.js',
 'infra/miget/cloudtms-miget-operations/node_modules/@modelcontextprotocol/server/dist/index.mjs'];
const pins={version:'CONTACT_FIXED_CANONICAL_METADATA_SOURCE_PINS_V1',rootHead:git(root,'rev-parse','HEAD'),
 rootInputs:rootFiles.map(f=>row(root,f)),rootCallerEvidence:[
  'scripts/healthroster-install1-native-coordinator.mjs',
  'docs/healthroster-build/install1-rehearsal/LIVE_TEST_RESTORATION_RULING_14102026.md',
  'docs/healthroster-build/install1-source/BUILD_RELEASE_TURN_SOURCE_REVIEW_20261014.md'].map(f=>row(root,f)),
 ownerInstructions:[{base:'workspace',...row(workspace,'AGENTS.md')},...ownerFiles.map(f=>({base:'ordinary-backend',...row(backend,f)}))],
 savedRouteEvidence:sourceFiles.map(f=>({base:'ordinary-backend',...row(backend,f)})),
 routeAttestation:'SAVED_SOURCE_ONLY_NOT_DEPLOYED_BYTES; ordinary index.ts modified and rollback-transaction.ts untracked',
 historicalTransportCommit:'737b82cadea8d9e73b73421de1d4f36565e5b29e',
 physicalPayloadReviewCommit:'85b485e0e75342e2b2e20c42533a0570865c448e'};
fs.writeFileSync(path.join(here,'SOURCE_PINS.json'),JSON.stringify(pins,null,2)+'\n');
const attemptFile=path.join(root,'.codex-tmp/contact-six-mcp-20261016-contact20-lock1-attempt3/NATIVE_RESULT.json');
const raw=fs.readFileSync(attemptFile),result=JSON.parse(raw);
const summary=s=>{const r=s.rows[0];return {statement:s.statement,rowCount:s.rowCount,rowsReturned:s.rows.length,rowsTruncated:s.rowsTruncated,
 totalParts:r.total_parts,totalChars:r.total_chars,totalUtf8Bytes:r.total_utf8_bytes,fullSha256:r.sha256,querySha256:r.query_sha256,
 firstPart:r.part,lastPart:s.rows.at(-1).part,lastEof:s.rows.at(-1).eof,returnedChunkCodepoints:s.rows.reduce((n,v)=>n+Array.from(v.chunk).length,0)};};
const before=summary(result.statements.find(s=>s.rows?.[0]?.transport_version)),after=summary(result.rollbackVerification.statements.find(s=>s.rows?.[0]?.transport_version));
const incident={version:'CONTACT_CANONICAL_FRAME_ATTEMPT3_SAFE_METADATA_V1',resultBytes:raw.length,resultSha256:sha(raw),
 accepted:result.accepted,database:result.database,rolledBack:result.rolledBack,connectedFresh:result.rollbackVerification.connectedFresh,
 failed:result.statements.filter(s=>s.error).map(s=>({statement:s.statement,sqlstate:s.error.code})),before,after,
 outcome:'HELD_MCP_FRAME_TOTALS_REFUSED; parent reported own holder189510 rollback/unlock/close; no qualification PASS',
 rawMetadataCopied:false,nativeExecutedByThisLane:false,
 hypotheticalFortyColumnGrid:{chunkCodepoints:8000,columns:40,rowLimit:100,capacityCodepoints:32000000,
  rowsPerSnapshot:Math.ceil(before.totalParts/40),combinedCanonicalCodepoints:before.totalChars+after.totalChars,
  minimumDuplicateJsonPayloadAsciiBytes:2*(before.totalChars+after.totalChars),
  conclusion:'Capacity arithmetic only; not a memory, response delivery, database resource or clinical-data-exposure qualification'}};
fs.writeFileSync(path.join(here,'INCIDENT_AND_BUDGET.json'),JSON.stringify(incident,null,2)+'\n');
console.log(JSON.stringify({status:'SOURCE_METADATA_ONLY_PREPARED',rootHead:pins.rootHead,beforeChars:before.totalChars,afterChars:after.totalChars,rawFramesPrinted:false}));

import fs from 'node:fs';import assert from 'node:assert/strict';
import {withSharedRehearsalTurn,agencyChildEnvironment} from '../scripts/healthroster-rehearsal-turn.mjs';
import {backgroundSnapshotFieldsSql,backgroundRelations} from '../scripts/healthroster-live-test-restoration.mjs';
import {scanManagedSql,digest} from '../scripts/cloudtms-managed-release-sql.mjs';
const progressPath='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-v2-backend-13102026/docs/healthroster-build/PROGRESS.md';
const result=await withSharedRehearsalTurn({task:'I1 corrected live snapshot fields read-only first use',progressPath,async work(turn){
 const source=fs.readFileSync('supabase/release/export_contract.sql','utf8'),s=scanManagedSql(source).statements[1],query=source.slice(s.start,s.end).trim().replace(/;\s*$/,'');
 const env=agencyChildEnvironment('healthroster_install1_live_fields_first_use');
 const sql='begin isolation level repeatable read read only;\nwith catalogue as ('+query+') select jsonb_build_object('+backgroundSnapshotFieldsSql()+');\nrollback;';
 const r=await turn.runOwnedChild({file:'C:/Program Files/PostgreSQL/18/bin/psql.exe',args:['-X','-q','-A','-t','-v','ON_ERROR_STOP=on','-v','VERBOSITY=sqlstate','-f','-'],input:sql,
  options:{env:{...env,PGOPTIONS:env.PGOPTIONS+' -c statement_timeout=0'},stdio:['pipe','pipe','pipe']},cancelOnLoss:true});
 const state=r.stderr.match(/(?:ERROR|FATAL):\s*([A-Z0-9]{5})/)?.[1]??null;
 if(r.code!==0) return {status:'READ_ONLY_FIELDS_FAILED',sqlstate:state};
 const lines=r.stdout.split(/\r?\n/).filter(x=>x.startsWith('{'));assert.equal(lines.length,1);const snapshot=JSON.parse(lines[0]);
 const sort=rows=>[...rows].sort((a,b)=>(a.schema+'|'+a.identity).localeCompare(b.schema+'|'+b.identity));
 const expected=sort(JSON.parse(fs.readFileSync('supabase/release/current-contract.json')).routines.filter(r=>/^(candidate_daily_projection_claim_v1|_candidate_daily_json_sha256_v1|tsfin_dequeue_batch_ids|tsfin_work_fail)\(/.test(r.identity)));
 assert.equal(expected.length,4);assert.deepEqual(sort(snapshot.backgroundWriterRoutines),expected);assert.equal(snapshot.backgroundSnapshotEpochZero,true);
 return {status:'READ_ONLY_FIELDS_PASS',database:turn.identity.physicalDatabase,writerCount:expected.length,sqlSha256:digest(sql),
  relations:backgroundRelations.map(relation=>({relation,rowCount:snapshot.backgroundRows[relation].length})),
  nativeClaimRows:snapshot.backgroundRows[backgroundRelations[0]].filter(r=>r.nativeClaimProved).length,
  rowMapsRetained:false,backgroundJobsPaused:false,fullRehearsal:false,installation:false,checkedAt:new Date().toISOString()};
}});
fs.writeFileSync('docs/healthroster-build/install1-rehearsal/LIVE_TEST_FIELDS_FIRST_USE_14102026.json',JSON.stringify(result,null,2)+'\n');
console.log(JSON.stringify(result));

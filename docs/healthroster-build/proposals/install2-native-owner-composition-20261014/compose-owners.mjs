// Generate complete reviewed source candidates; never install or execute SQL.
import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { execFileSync, spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { scanManagedSql } from '../../../../scripts/cloudtms-managed-release-sql.mjs';
const folder=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(folder,'../../../..');
const inputs=JSON.parse(fs.readFileSync(path.join(folder,'INPUTS.json')));
const hash=bytes=>createHash('sha256').update(bytes).digest('hex');
const broadCommit='f7afa8f66133b5b58abadbe1c3654a1e80b475a0';
const stageSource='docs/healthroster-build/proposals/protected-source-qualification-operational-gate/12_complete_actor_20_stage_request.sqlinc';
const rawStage=execFileSync('git',['show',broadCommit+':'+stageSource],{cwd:root});
assert.equal(hash(Buffer.from(fs.readFileSync(path.join(root,stageSource),'utf8').replaceAll('\r\n','\n'))),hash(rawStage));
const parsed=scanManagedSql(rawStage.toString('utf8'));
const stageRows=parsed.statements.map(unit=>parsed.source.slice(unit.start,unit.end)).filter(body=>/^\s*create\s+or\s+replace\s+function\s+public\.weekly_exceptional_pay_stage_c1_request_v1\s*\(/i.test(body));
assert.equal(stageRows.length,1);
const bankAdmission="  -- SP-12: existing admission only; no producer/version evidence.\n  if (select active_owner from private.bpay_next_module_control where id=1)='NEXT' then\n    perform private.weekly_source_pay_query_admit_v2();\n  end if;\n\n";
const replayAnchor='  -- An exact staged request is the idempotency authority even after the family\n';
assert.equal(stageRows[0].split(replayAnchor).length-1,2);
assert.equal(stageRows[0].split('  -- SP-12:').length-1,0);
const admittedStage=stageRows[0].replace(replayAnchor,()=>bankAdmission+replayAnchor);
const rows=[];
for(const row of inputs.owners){
 const actual=fs.readFileSync(path.join(folder,row.slug+'.actual.sqlinc'),'utf8');
 assert.equal(hash(actual.slice(0,-1)),row.actual.sha256);
 let composed,decision;
 if(row.name==='public.weekly_source_manual_review_resolve_v1'){
  composed=actual;decision='RETAIN_COMPLETE_CURRENT_COMMAND_REPLAY_AND_ACCEPTED_EVIDENCE_OWNER; REJECT_STALE_BROAD_PRE_COMMAND_REPLACEMENT';
 }else if(row.name==='public.weekly_exceptional_pay_stage_c1_request_v1'){
  composed=admittedStage+'\n';decision='COMPLETE_Q45_ACTOR_EXACT_EVENT_OWNER_PLUS_CURRENT_NEXT_ADMISSION_BEFORE_FIRST_REPLAY';
 }else{
  composed=fs.readFileSync(path.join(folder,row.slug+'.draft.txt'),'utf8');assert.ok(!/^(?:<<<<<<<|=======|>>>>>>>|\|\|\|\|\|\|\|) /m.test(composed));
  decision='NONCONFLICTING_COMPLETE_CURRENT_AND_BROAD_THREE_WAY_SOURCE_UNION';
 }
 const check=scanManagedSql(composed);assert.equal(check.statements.filter(unit=>/^\s*create\s+(?:or\s+replace\s+)?function\b/i.test(check.source.slice(unit.start,unit.end))).length,1);
 const output=row.slug+'.complete.sqlinc';fs.writeFileSync(path.join(folder,output),composed);
 const diff=spawnSync('git',['diff','--no-index','--no-ext-diff','--',path.join(folder,row.slug+'.actual.sqlinc'),path.join(folder,output)],{encoding:'utf8',windowsHide:true,maxBuffer:8_000_000});assert.ok(diff.status===0||diff.status===1);fs.writeFileSync(path.join(folder,row.slug+'.complete.diff'),diff.stdout);
 rows.push({name:row.name,file:output,inputDefinitionSha256:row.actual.sha256,completeFileSha256:hash(composed),decision});
}
fs.writeFileSync(path.join(folder,'COMPLETE_OWNER_DECISIONS.json'),JSON.stringify({sourceOnly:true,status:'COMPLETE_SOURCE_CANDIDATES_NATIVE_REVIEW_HELD',inputs:'INPUTS.json',q45Stage:{commit:broadCommit,file:stageSource,rawFileSha256:hash(rawStage),inputDefinitionSha256:hash(stageRows[0]),nativeAdmissionBeforeFirstReplay:true},owners:rows,noNativeSqlOrReleaseAction:true},null,2)+'\n');
console.log(JSON.stringify(rows));

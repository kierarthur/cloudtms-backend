// Source-only proposal construction. No database/psql/EXPLAIN or root writes.
import fs from 'node:fs';import path from 'node:path';import cp from 'node:child_process';import crypto from 'node:crypto';import assert from 'node:assert/strict';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026',parent='42802228977b941325f0feb44c11918b0f42190f',prefix='docs/healthroster-build/proposals/contact-qt-positive-native-proposal-20261017',sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const git=(...args)=>{const r=cp.spawnSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',root,...args],{maxBuffer:25e6});assert.equal(r.status,0,r.stderr.toString());return r.stdout;};
const files=['scripts/healthroster-install1-full-rehearsal.mjs','scripts/healthroster-live-test-restoration.mjs','scripts/healthroster-turn-control.mjs','supabase/release/export_contract.sql',...['fixed-metadata-reader.mjs','metadata-diagnostic.mjs','control.mjs','timing.mjs','input-builder.mjs','source.mjs','SOURCE_PINS.json','native-boundary.mjs','result-validator.mjs','hold-case.mjs'].map(f=>prefix+'/'+f)],pins=[];
for(const file of files){const b=git('show',parent+':'+file);assert(b.equals(fs.readFileSync(root+'/'+file)));assert(b.equals(git('show','HEAD:'+file)));assert(b.equals(git('show',':'+file)));const capture='reference/'+file;fs.mkdirSync(path.dirname(here+'/'+capture),{recursive:true});fs.writeFileSync(here+'/'+capture,b);pins.push({file,capture,bytes:b.length,sha256:sha(b),savedIndexHeadParentEqual:true});}
fs.writeFileSync(here+'/SOURCE_PINS.json',JSON.stringify({root,parent,currentRootHead:git('rev-parse','HEAD').toString().trim(),files:pins,native:'NOT_RUN'},null,2)+'\n');
const edits=[];function change(file,transform){const before=fs.readFileSync(root+'/'+file,'utf8'),hunks=[];const replace=(old,value)=>{const n=before.includes('\r\n')?'\r\n':'\n';old=old.replace(/\r?\n/g,()=>n);value=value.replace(/\r?\n/g,()=>n);const index=state.indexOf(old);assert.equal(state.split(old).length-1,1,file);state=state.replace(old,()=>value);hunks.push({before:old,after:value});};let state=before;transform(replace);const payload='candidate/'+file;fs.mkdirSync(path.dirname(here+'/'+payload),{recursive:true});fs.writeFileSync(here+'/'+payload,state);edits.push({file,payload,beforeRaw:before,afterRaw:state,beforeSha256:sha(before),afterSha256:sha(state),hunks});}
change('scripts/healthroster-live-test-restoration.mjs',replace=>{
 replace('export function backgroundSnapshotFieldsSql(){',`export function backgroundSnapshotFieldsSql(){return renderBackgroundSnapshotFieldsSql(false);}
// The existing zero-argument renderer remains byte-identical for legacy callers.
// Only the owning snapshot query may bind this fixed one-row statement CTE.
export function boundBackgroundSnapshotFieldsSql(){return renderBackgroundSnapshotFieldsSql(true);}
function renderBackgroundSnapshotFieldsSql(bound){
 const activeSnapshot=bound?'(select snapshot_value from background_statement_snapshot)':'pg_current_snapshot()';`);
 // Five closed source occurrences; no caller SQL or dynamic rewrite is accepted.
 const old=fs.readFileSync(root+'/scripts/healthroster-live-test-restoration.mjs','utf8');assert.equal(old.split('pg_current_snapshot()').length-1,5);
 replace("'rowHash',${hash('to_jsonb(r)')},'xmin',r.xmin::text,'xactStatus',case when pg_snapshot_xmax(pg_current_snapshot())::text::numeric<4294967296 then pg_xact_status(r.xmin::text::xid8) end,", "'rowHash',${hash('to_jsonb(r)')},'xmin',r.xmin::text,'xactStatus',case when pg_snapshot_xmax(${activeSnapshot})::text::numeric<4294967296 then pg_xact_status(r.xmin::text::xid8) end,");
 replace("'xactStatus',case when pg_snapshot_xmax(pg_current_snapshot())::text::numeric<4294967296 then pg_xact_status(r.xmin::text::xid8) end,\n   'attemptCount'", "'xactStatus',case when pg_snapshot_xmax(${activeSnapshot})::text::numeric<4294967296 then pg_xact_status(r.xmin::text::xid8) end,\n   'attemptCount'");
 replace("'backgroundSnapshotXmax',pg_snapshot_xmax(pg_current_snapshot())::text,", "'backgroundSnapshotXmax',pg_snapshot_xmax(${activeSnapshot})::text,");
 replace("'backgroundSnapshotEpochZero',pg_snapshot_xmax(pg_current_snapshot())::text::numeric<4294967296,", "'backgroundSnapshotEpochZero',pg_snapshot_xmax(${activeSnapshot})::text::numeric<4294967296,");
 replace("'backgroundSnapshotXip',(select coalesce(jsonb_agg(x::text),'[]'::jsonb) from pg_snapshot_xip(pg_current_snapshot()) x),", "'backgroundSnapshotXip',(select coalesce(jsonb_agg(x::text),'[]'::jsonb) from pg_snapshot_xip(${activeSnapshot}) x),");
});
change('scripts/healthroster-install1-full-rehearsal.mjs',replace=>{
 replace('import {backgroundSnapshotFieldsSql,fixtureObservationSql', 'import {boundBackgroundSnapshotFieldsSql,fixtureObservationSql');
 replace(' catalogue as (${exportQuery})\n select (identity_proof', " catalogue as (${exportQuery})${observeLiveBusiness?', background_statement_snapshot as materialized (select pg_current_snapshot() snapshot_value)':''}\n select (identity_proof");
 replace("${observeLiveBusiness?backgroundSnapshotFieldsSql()+',':''}", "${observeLiveBusiness?boundBackgroundSnapshotFieldsSql()+',':''}");
});
for(const e of edits){let s=e.afterRaw;for(const h of [...e.hunks].reverse()){assert.equal(s.split(h.after).length-1,1);s=s.replace(h.after,()=>h.before);}assert.equal(s,e.beforeRaw);}
// Unchanged pinned exporter companion lets the complete proposal module find
// its own exact root-relative source guard during offline source rendering.
fs.mkdirSync(here+'/candidate/supabase/release',{recursive:true});fs.copyFileSync(here+'/reference/supabase/release/export_contract.sql',here+'/candidate/supabase/release/export_contract.sql');
fs.writeFileSync(here+'/FORWARD_EDITS.json',JSON.stringify({parent,edits,sourceOnly:true,readyForIndependentSourceReview:true,mountReady:false,reason:'Complete current Contact source-pin companion is separately selected in PAYLOAD_SELECTION.json. Root review, mount/commit and genuine current source admission are required before any retry. Cancelled statement and speedup remain unknown; no frozen evidence rewrite.',native:'NOT_RUN'},null,2)+'\n');console.log(JSON.stringify({captured:files.length,completeCandidateParents:edits.length,native:'NOT_RUN'}));

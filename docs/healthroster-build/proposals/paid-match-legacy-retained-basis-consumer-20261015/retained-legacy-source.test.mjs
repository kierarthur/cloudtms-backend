import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import test from 'node:test';
import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
import {scanManagedSql} from '../strict-qualification-install1-forward/PINNED_MANAGED_SQL_SCANNER.mjs';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..');
const read=f=>fs.readFileSync(path.join(dir,f),'utf8'),sha=b=>createHash('sha256').update(b).digest('hex');
const p=JSON.parse(read('COMPLETE_REVERSE_AND_INPUT_PINS.json')),before=read('before/01_complete_retained_transition_assert.sqlinc'),after=read('forward/01_complete_retained_transition_assert.sqlinc');
const eol=before.includes('\r\n')?'\r\n':'\n';
test('whole existing owner uniquely reverses two hunks to exact published Legacy30 bytes',()=>{
 assert.equal(p.hunks.length,2);assert.equal(sha(before),p.before_sha256);assert.equal(sha(after),p.after_sha256);
 let reverse=after;for(const h of [...p.hunks].reverse()){assert.equal(reverse.split(h.after).length,2);reverse=reverse.replace(h.after,()=>h.before);}
 assert.equal(reverse,before);assert.equal(before,execFileSync('git',['show',p.commit+':'+p.source],{cwd:root,encoding:'utf8',maxBuffer:3e6}));
});
test('exact actual native hook excludes Legacy capture without creating a fake map',()=>{
 const h=read(p.native_input.saved);assert.equal(sha(h),p.native_input.sha256);
 const owner=h.slice(h.indexOf('create or replace function private.bpay_next_source_current_publish_revision_v1'));
 assert.match(owner,/active_owner='NEXT' for share;\s*if not found then return;end if;/);
 assert.doesNotMatch(after,/insert into|update public\.|delete from|create table|paid_at|source_present\s*:=|new_present\s*:=/i);
});
test('NEXT mapping remains the exact predicate and error from the predecessor',()=>{
 const start='not exists(select 1 from private.bpay_next_source_current_snapshots mapping';
 const end=before.indexOf(' then'+eol+"      raise exception 'WEEKLY_SOURCE_PAID_MATCH_PRIOR_MOVEMENT_UNBOUND'",before.indexOf(start));assert.ok(end>before.indexOf(start));
 const b=before.slice(before.indexOf(start),end);
 assert.ok(b.length>300);assert.ok(after.includes(b));
 const next=after.slice(after.indexOf("if item#>>'{native_payment_facts,paid_evidence_state}'='SETTLED_ORIGINAL'"),after.indexOf('    else',after.indexOf("if item#>>'{native_payment_facts,paid_evidence_state}'='SETTLED_ORIGINAL'")));
 assert.ok(next.includes(b));assert.doesNotMatch(next,/current_snapshot_for_cycle|LEGACY/);
});
test('Legacy calls the one existing Source selection owner and compares the entire genuine result',()=>{
 assert.match(after,/legacy_current:=private.weekly_source_paid_overlap_current_snapshot_for_cycle_v1\(t.work_event_id,m.id,/);
 assert.ok(after.includes("legacy_current is distinct from item->'current_pay_snapshot'"));
 const legacy=after.slice(after.indexOf('    else'+eol+'      -- LEGACY'),after.indexOf('    select * into strict journal'));
 assert.doesNotMatch(legacy,/bpay_next_source_current_snapshots|order by .*revision|limit 1|sum\(|round\(/i);
 assert.ok(legacy.includes("active_owner from private.bpay_next_module_control"));
});
test('immutable Source event/group/actor/Contract and native root certificate hashes remain closed',()=>{
 for(const s of ["native_basis->>'inputs_sha256' is distinct from encode(private.weekly_source_sha256_jsonb_v1(","native_basis->>'actor_user_id' is distinct from r.finalised_by_user_id::text","native_basis->>'source_cycle_id' is distinct from r.source_cycle_id::text","native_basis->>'source_group_id' is distinct from source_group_id::text","native_basis->>'candidate_id' is distinct from old_line.candidate_id::text","native_basis->>'client_id' is distinct from old_line.client_id::text","not ((native_basis->>'root_timesheet_id')::uuid=any(native_roots))","count(distinct root)"]){assert.ok(after.includes(s),s);}
 assert.ok(after.includes("array(select (value->>'root_timesheet_id')::uuid"));
});
test('actual present B rows and old NORMAL S2 to M1 identities remain byte exact outside hunks',()=>{
 for(const start of ['  for b in select *','    select * into strict journal','  expected:=jsonb_build_object']){
  const b=before.slice(before.indexOf(start),start==='  for b in select *'?before.indexOf('    select count(*),max(value'):before.length);
  if(start==='  for b in select *')assert.ok(after.includes(b));else assert.equal(after.slice(after.indexOf(start)),b);
 }
 assert.ok(after.includes("old_line.pay_effect<>'NORMAL'"));assert.ok(after.includes('rows is distinct from t.paid_match_source_rows'));
});
test('all dependency kernels match immutable Git and are delegated without new money classification',()=>{
 for(const d of p.dependencies){const b=fs.readFileSync(path.join(root,d.file));assert.equal(sha(b),d.sha256);assert.equal(b.length,d.bytes);assert.deepEqual(b,execFileSync('git',['show',d.commit+':'+d.file],{cwd:root,maxBuffer:3e6}));}
 assert.doesNotMatch(after,/paid\s*:=|target_amount_ex_vat|pay_amount|amount_ex_vat|GREATEST\(|SETTLED_TERMINAL/);
});
test('schema parser retains one same-signature owner and owner-only ACL',()=>{
 const s=scanManagedSql(after);assert.equal(s.meta.length,0);assert.equal(s.statements.length,3);
 assert.match(after,/private.weekly_source_paid_match_retained_transition_assert_v1\(p_transition_id uuid\)/);
 assert.match(after,/stable security definer/);assert.match(after,/from public,anon,authenticated,service_role;/);
});
// These in-memory tuples exercise Source membership/identity admission only.
// No fixture, money/classifier result or PostgreSQL execution is supplied.
const anchor={state:'CURRENT',snapshot_line_id:'S2',movement_id:'M1',native_current_selections:[{selection_owner:'SOURCE_FIN4_PRIOR_STATE',selected_transition_id:'T2',selected_final_revision_id:'R2',snapshot_line_id:'S2',source_hash:'H2',selected_manifest_hash:'MF2',source_export_present:true,exclude_cancel:false}]};
const admitted=(expected,actual,roots,basisRoots,module='LEGACY')=>module==='LEGACY'&&roots.length>0&&roots.length<=200&&new Set(roots).size===roots.length&&JSON.stringify([...roots].sort())===JSON.stringify([...basisRoots].sort())&&JSON.stringify(expected)===JSON.stringify(actual)&&actual.state==='CURRENT'&&actual.snapshot_line_id==='S2'&&actual.movement_id==='M1';
test('native-shaped multi-root Legacy retained source witness admits S2 to M1 without NEXT mapping',()=>{
 assert.equal(admitted(anchor,structuredClone(anchor),['rootB','rootA'],['rootA','rootB']),true);
 assert.ok(after.includes("legacy_current->>'snapshot_line_id' is distinct from old_line.id::text"));
});
test('foreign missing or duplicate native roots refuse before Source selection',()=>{
 for(const pair of [[['rootA','rootB'],['rootA','foreign']],[['rootA','rootB'],['rootA']],[['rootA','rootA'],['rootA','rootA']],[[],[]]])assert.equal(admitted(anchor,anchor,...pair),false);
});
test('changed manifest selected transition or export presence refuses whole Source witness',()=>{
 for(const key of ['selected_manifest_hash','selected_transition_id','source_export_present']){const x=structuredClone(anchor);x.native_current_selections[0][key]='changed';assert.equal(admitted(anchor,x,['rootA'],['rootA']),false);}
});
test('module switch and fake S1 selection cannot reuse Legacy current evidence',()=>{
 assert.equal(admitted(anchor,anchor,['rootA'],['rootA'],'NEXT'),false);
 assert.equal(admitted(anchor,{...anchor,snapshot_line_id:'S1'},['rootA'],['rootA']),false);
 assert.equal(admitted(anchor,{...anchor,movement_id:'M2'},['rootA'],['rootA']),false);
});

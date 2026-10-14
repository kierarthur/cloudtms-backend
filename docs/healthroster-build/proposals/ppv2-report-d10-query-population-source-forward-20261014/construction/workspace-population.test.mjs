import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
import test from 'node:test';
import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),read=n=>fs.readFileSync(path.join(dir,n),'utf8');
const workspace=read('workspace.report-d10-population-forward.sqlinc'),scopes=read('scopes.report-d10-population-forward.sqlinc');
const digest=v=>crypto.createHash('sha256').update(typeof v==='string'?v:JSON.stringify(v)).digest('hex');
const normalized=s=>s.replaceAll('\r\n','\n');
const uniqueReverse=(source,hunks)=>{for(const h of [...hunks].reverse()){assert.equal(source.split(h.after).length,2);source=source.replace(h.after,()=>h.before);}return source;};
// Explicit UNIT_ONLY reference vectors for the bound SQL composition. They do
// not execute PostgreSQL, the native fact owner, real permissions or mutations.
const key=(kind,n)=>kind+'_'+n.toString(16).padStart(64,'0');
const row=(kind,n,candidate,keys=[])=>({row_key:key(kind,n),group_key:key(kind,n),candidate,client:'Client',issues:1,
  candidate_asked:null,manager_informed:null,status:{text:'Native caption'},children:[{row_key:'actual-decision-'+n}],
  actions:[{command:kind==='qg'?'ASK_CANDIDATE':'CHECK_ONLY_D10_REVIEW'}],issue_filter_keys:keys,
  first_seen_at_utc:'2026-10-09T00:00:00Z',source_outreach_eligible:kind==='qg'});
const frame=(rows=[],week='2026-10-09')=>({contract:'WEEKLY_SOURCE_CHECK_ONLY_QUERY_GROUPS_V1',actor_user_id:'actor',
  permission_view:'ADMIN',scope:{source_group_id:'group',source_cycle_id:'cycle',client_id:'client',week_ending:week},
  groups:rows,read_revision:'a'.repeat(64),source_read_revision:'b'.repeat(64),source_outreach_eligible:false,bulk_actions:{}});
function d10Rows(f,{actor='actor',view='ADMIN',week='2026-10-09',issue='ALL',status='UNRESOLVED',candidate='',target=null}={}){
  assert.equal(f.contract,'WEEKLY_SOURCE_CHECK_ONLY_QUERY_GROUPS_V1');assert.equal(f.actor_user_id,actor);assert.equal(f.permission_view,view);
  assert.deepEqual(f.scope,{source_group_id:'group',source_cycle_id:'cycle',client_id:'client',week_ending:week});
  assert.equal(f.source_outreach_eligible,false);assert.deepEqual(f.bulk_actions,{});assert.ok(Array.isArray(f.groups));
  assert.match(f.read_revision,/^[0-9a-f]{64}$/);assert.match(f.source_read_revision,/^[0-9a-f]{64}$/);
  if(view==='PAY_READONLY')assert.deepEqual(f.groups,[]);
  const keys=f.groups.map(r=>r.group_key);assert.equal(new Set(keys).size,keys.length);
  return f.groups.filter(r=>{assert.match(r.group_key,/^d10g_[0-9a-f]{64}$/);assert.equal(r.row_key,r.group_key);
    assert.ok(Array.isArray(r.issue_filter_keys));assert.ok(r.issue_filter_keys.length<=3);
    assert.equal(new Set(r.issue_filter_keys).size,r.issue_filter_keys.length);
    for(const k of r.issue_filter_keys)assert.ok(typeof k==='string'&&['TIMESHEET_MISSING','HOURS_DIFFER','MISSING_SHIFT'].includes(k));
    return (!target||r.group_key===target)&&['UNRESOLVED','ALL'].includes(status)&&r.candidate.toLowerCase().includes(candidate.toLowerCase())
      &&(issue==='ALL'||r.issue_filter_keys.includes(issue));});
}
function population(source,d10,{offset=0,limit=2,target=null,direction='asc',sort='candidate'}={}){
  const all=[...source,...d10];assert.equal(new Set(all.map(x=>x.group_key)).size,all.length);
  if(target)assert.equal(all.length,1);
  const value=r=>sort==='candidate'?r.candidate.toLowerCase():sort==='age'?r.first_seen_at_utc:r.issues;
  all.sort((a,b)=>{const av=value(a),bv=value(b),cmp=av<bv?-1:av>bv?1:0;return(direction==='desc'?-cmp:cmp)||(a.group_key<b.group_key?-1:1);});
  return{total:all.length,rows:all.slice(offset,offset+limit),bulk:target?[]:source.map(x=>x.group_key)};
}
const cursorBasis={actor_user_id:'actor',permission_view:'ADMIN',source_group_id:'group',source_cycle_id:'cycle',client_id:'client',
  source_workspace_version:'c'.repeat(64),d10_read_revisions:[{client_id:'client',week_ending:'2026-10-09',read_revision:'a'.repeat(64),source_read_revision:'b'.repeat(64)}],
  filters:{issue:'ALL'},sort_key:'candidate',sort_direction:'asc'};
test('whole Queries reverse reaches actual current0044 through retained Report and target layers',()=>{
  const r=JSON.parse(read('PPV2_D10_POPULATION_FULL_REVERSE.json'));assert.equal(digest(workspace),r.forward_sha256);
  let restored=uniqueReverse(workspace,r.hunks);assert.equal(restored,read('workspace.report-d10-target-forward.sqlinc'));
  const prior=JSON.parse(read('PPV2_REPORT_D10_TARGET_FULL_REVERSE.json'));restored=uniqueReverse(restored,prior.report_overlay_to_d10_hunks);
  assert.equal(restored,read('workspace.root-report-overlay.sqlinc'));
  let b=Buffer.from(restored);for(const h of [...prior.current_to_report_overlay_hunks].reverse()){
    const at=h.after_byte_offset,a=Buffer.from(h.after);assert.ok(b.subarray(at,at+a.length).equals(a));b=Buffer.concat([b.subarray(0,at),Buffer.from(h.before),b.subarray(at+a.length)]);}
  assert.ok(b.equals(fs.readFileSync(path.join(dir,'workspace.current-before.sqlinc'))));
});
test('whole scopes D10 reverse reaches the retained Report overlay',()=>{
  const r=JSON.parse(read('PPV2_D10_SCOPES_FULL_REVERSE.json'));assert.equal(digest(scopes),r.forward_sha256);
  assert.equal(uniqueReverse(scopes,r.hunks),read('scopes.root-report-overlay.sqlinc'));
});
test('PAY authority, reduction and permission tail remain byte-identical to current0044',()=>{
  const before=read('workspace.current-before.sqlinc');const marker='  -- Reduce the existing Hours-question projection';
  const reduction=s=>s.slice(s.indexOf(marker),s.indexOf('  v_result:=pg_catalog.jsonb_build_object(',s.indexOf(marker)));
  assert.equal(reduction(workspace),reduction(before));
  const tail=s=>s.slice(s.indexOf("    'lifecycle_policy',private.weekly_source_office_lifecycle_policy_v1()"));
  assert.equal(tail(workspace),tail(before));
  assert.ok(workspace.includes("v_actor,case when v_pay_view then 'VIEW_PAY_WORK' else 'VIEW_SOURCE_PROGRESS' end,"));
});
test('actual current Source query producer calls, filter and bulk proof domain are retained',()=>{
  const before=read('workspace.current-before.sqlinc');
  const calls=s=>[...s.matchAll(/private\.weekly_source_office_query_groups_v1\([^;]*?\)/g)].map(m=>m[0]);assert.deepEqual(calls(workspace),calls(before));
  for(const m of before.matchAll(/'WEEKLY_SOURCE_[A-Z_]*SELECTION[A-Z_]*'/g))assert.ok(workspace.includes(m[0]));
  const bulk=workspace.slice(workspace.indexOf('  if v_publication_id is not null and not v_d10_target then'),workspace.indexOf("  if v_tab='queries' and v_publication_id is not null and not v_d10_target then"));
  assert.ok(!bulk.includes('v_d10_rows'));assert.ok(!bulk.includes('d10g_'));
});
test('actual native own-week date and preferred Client cycle are used in both consumers',()=>{
  for(const s of [read('d10-census.fragment.sql.txt'),read('d10-scope-census.fragment.sql.txt')]){
    assert.ok(s.includes('t.week_ending_date'));assert.ok(s.includes('preferred.scope_client_id='));
    assert.ok(!s.includes("'week_ending',v_cycle.finalisation_week_ending"));
  }
});
test('native filter membership precedes one complete population count and page',()=>{
  assert.ok(workspace.indexOf("v_d10_row->'issue_filter_keys' ?")<workspace.indexOf('v_total:=jsonb_array_length(v_query_population)'));
  assert.ok(workspace.indexOf('v_total:=jsonb_array_length(v_query_population)')<workspace.indexOf('page.position>v_offset'));
  assert.ok(!read('d10-census.fragment.sql.txt').includes("like '%Hours differ%'"));
});
test('normal D10 population no Source publication dependency',()=>{
  const s=read('d10-census.fragment.sql.txt');assert.ok(s.includes("if v_tab='queries' and v_group.source_family='ROSTER' then"));
  assert.ok(!s.includes('v_publication_id is not null'));
});
test('check-only scope never claims Source completed/prepared or contributes missing report totals',()=>{
  assert.ok(scopes.includes("then null::boolean"));assert.ok(scopes.includes("filter(where scope_kind<>'CHECK_ONLY_D10'"));
  assert.ok(scopes.includes("p.document_mode='INVOICE_EVIDENCE_REQUIRED' and not p.self_bill_enabled"));
  assert.ok(scopes.includes("if v_admin then"));assert.ok(scopes.includes("if (v_d10_frame->>'group_count')::integer>0 then"));
});
for(const issue of ['TIMESHEET_MISSING','HOURS_DIFFER','MISSING_SHIFT'])test('selective '+issue+' uses literal keys and keeps matching native action/child',()=>{
  const native=row('d10g',1,'A',[issue]);const got=d10Rows(frame([native,row('d10g',2,'B',[])]),{issue});assert.deepEqual(got,[native]);assert.equal(got[0].actions[0].command,'CHECK_ONLY_D10_REVIEW');
});
test('unknown native fact stays visible under ALL but no fabricated selective membership',()=>{
  const native=row('d10g',1,'A',[]);native.status.text='Hours differ Missing';assert.deepEqual(d10Rows(frame([native])),[native]);assert.deepEqual(d10Rows(frame([native]),{issue:'HOURS_DIFFER'}),[]);
});
test('full mixed page sorts before slicing and total includes both owners',()=>{
  const source=[row('qg',1,'Z'),row('qg',2,'D')],native=d10Rows(frame([row('d10g',1,'A'),row('d10g',2,'M')]));
  const p=population(source,native);assert.equal(p.total,4);assert.deepEqual(p.rows.map(x=>x.candidate),['A','D']);assert.deepEqual(p.bulk,source.map(x=>x.group_key));
  assert.deepEqual(population(source,native,{offset:2}).rows.map(x=>x.candidate),['M','Z']);
});
test('D10 cannot enter Source ALL_FILTERED selection in either sort direction',()=>{
  for(const direction of ['asc','desc']){const p=population([row('qg',1,'B')],d10Rows(frame([row('d10g',1,'A')])),{direction});assert.deepEqual(p.bulk,[key('qg',1)]);}
});
test('literal exact D10 target suppresses bulk and preserves only native parent',()=>{
  const chosen=row('d10g',1,'A');const d=d10Rows(frame([chosen,row('d10g',2,'B')]),{target:chosen.group_key});
  const p=population([],d,{target:chosen.group_key});assert.equal(p.total,1);assert.deepEqual(p.rows,[chosen]);assert.deepEqual(p.bulk,[]);
});
test('filtered or absent exact target causes stale refusal, not another parent fallback',()=>{
  const chosen=row('d10g',1,'A',['HOURS_DIFFER']);assert.throws(()=>population([],d10Rows(frame([chosen]),{target:chosen.group_key,issue:'MISSING_SHIFT'}),{target:chosen.group_key}));
});
test('duplicate native parent is refused even if one copy would be filtered out',()=>{
  assert.throws(()=>d10Rows(frame([row('d10g',1,'A',['HOURS_DIFFER']),row('d10g',1,'B',[])]),{issue:'HOURS_DIFFER'}));
  assert.ok(workspace.includes('group by g having count(*)<>1'));
});
for(const [name,change] of [
  ['foreign actor',f=>{f.actor_user_id='foreign';}],['foreign Client',f=>{f.scope.client_id='foreign';}],
  ['wrong real week',f=>{f.scope.week_ending='2026-10-11';}],['outreach injection',f=>{f.source_outreach_eligible=true;}],
  ['absent revision',f=>{delete f.read_revision;}],['array revision',f=>{f.source_read_revision=['b'.repeat(64)];}],
  ['invalid filter scalar',f=>{f.groups[0].issue_filter_keys='HOURS_DIFFER';}],
  ['caller invented key',f=>{f.groups[0].issue_filter_keys=['WITHHELD'];}],
  ['duplicate subject key',f=>{f.groups[0].issue_filter_keys=['HOURS_DIFFER','HOURS_DIFFER'];}]
])test(name+' is refused in the bound compatibility model',()=>{const f=frame([row('d10g',1,'A')]);change(f);assert.throws(()=>d10Rows(f));});
test('PAY_READONLY accepts empty owner census and refuses private D10 parent',()=>{
  const f=frame();f.permission_view='PAY_READONLY';assert.deepEqual(d10Rows(f,{view:'PAY_READONLY'}),[]);f.groups=[row('d10g',1,'A')];assert.throws(()=>d10Rows(f,{view:'PAY_READONLY'}));
});
for(const field of ['actor_user_id','permission_view','source_group_id','source_cycle_id','client_id','filters','sort_key','sort_direction','source_workspace_version','d10_read_revisions'])test('cursor seal binds '+field,()=>{
  const c=structuredClone(cursorBasis);c[field]=field==='d10_read_revisions'?[]:field==='filters'?{issue:'HOURS_DIFFER'}:'other';assert.notEqual(digest(c),digest(cursorBasis));assert.ok(workspace.includes("'"+field+"'"));
});
test('cursor rejects overflow and malformed trailing bytes before integer conversion',()=>{
  const accept=c=>/^[0-9a-f]{64}:[0-9]{1,10}$/.test(c)&&Number(c.split(':')[1])<=2147483547;
  assert.ok(accept('a'.repeat(64)+':2147483547'));for(const c of ['a'.repeat(64)+':2147483647','a'.repeat(64)+':1:x','a'.repeat(64)+':-1','a'.repeat(64)+':00000000000'])assert.equal(accept(c),false);
  assert.ok(workspace.includes('::numeric>2147483547'));assert.ok(scopes.includes('v_offset>2147483547'));
});
test('query tuple raw provenance/filter internals do not escape public parent render',()=>{
  assert.ok(workspace.includes("g.value-array['_query_first_seen_at_utc','first_seen_at_utc','issue_filter_keys']"));
  assert.ok(!workspace.includes("'holds_pay',true"));assert.ok(!workspace.includes('withheld_pence'));
});
test('stable native read owners retain original search paths and signatures; no new write or grant',()=>{
  assert.match(workspace,/returns jsonb\s+language plpgsql\s+stable\s+security definer/);
  assert.match(scopes,/returns jsonb language plpgsql stable security definer/);
  for(const s of [read('d10-census.fragment.sql.txt'),read('d10-population-page.fragment.sql.txt'),read('d10-scope-census.fragment.sql.txt')])assert.equal(/\b(insert into|delete from|update public\.|grant |set_config|create table)\b/i.test(s),false);
});
console.log('UNIT_ONLY: source-bound compatibility and reversal vectors; no native execution or financial/permission proof.');

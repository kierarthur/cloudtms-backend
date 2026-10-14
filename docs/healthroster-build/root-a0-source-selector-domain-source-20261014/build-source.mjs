// Source construction only. No SQL execution or canonical owner mutation.
import fs from 'node:fs';
import path from 'node:path';
import cp from 'node:child_process';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
const repo=process.cwd(),base='docs/healthroster-build/proposals/a0-source-selector-domain-construction-20261014';
const prior='9755f656daa297a7e156111ae981bddb1f0e10aa';
const priorBase='docs/healthroster-build/proposals/a0-hidden-rr-owner-coverage-successor-20261014/sql/';
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const save=(p,b)=>{const f=path.join(repo,base,p);fs.mkdirSync(path.dirname(f),{recursive:true});fs.writeFileSync(f,b,{flag:'wx'});};
if(fs.existsSync(path.join(repo,base)))throw Error('Existing Source-domain construction must be inspected, not overwritten');
const objectRelations=['weekly_source_groups','weekly_source_cycles','weekly_source_uploads','weekly_source_upload_rows',
 'weekly_source_projection_publications','weekly_source_row_resolutions','weekly_source_row_economic_snapshots',
 'weekly_source_row_expense_policy_snapshots','weekly_expense_authority_generations','weekly_source_expense_cell_evidence',
 'weekly_source_charge_checks','weekly_source_charge_acceptances','weekly_source_invoice_hold_releases',
 'weekly_source_state_transitions','weekly_source_report_scopes','weekly_source_self_bill_reports',
 'weekly_source_self_bill_report_line_bindings','weekly_work_events','weekly_source_final_revisions',
 'weekly_source_final_snapshot_lines','weekly_source_row_timesheet_lineages','weekly_source_ordinary_pay_projection_receipts',
 'weekly_discrepancy_incidents','weekly_issue_comparison_revisions','weekly_source_billing_movements',
 'weekly_source_group_clients','weekly_source_client_policies','weekly_source_contract_policies',
 'weekly_source_group_rule_assignments','weekly_source_rule_set_versions','weekly_source_rule_sets',
 'weekly_source_format_profiles','clients','client_settings','contracts','timesheets','timesheets_financials'];
save('SOURCE_OBJECT_RELATIONS.json',JSON.stringify(objectRelations.map(n=>'public.'+n),null,2)+'\n');
const sqlList=objectRelations.map(n=>"'public."+n+"'").join(',');
const validator=`-- Closed grammar for actual Source dependencies in the SAME proposed A0 arbiter.
-- No caller SQL, fake family identity or monetary permission is introduced.
create or replace function private.timesheet_correction_a0_source_token_assert_v1(p_token jsonb)
returns void language plpgsql immutable security definer set search_path='pg_catalog','pg_temp'
as $source_token$
declare kind text:=p_token->>'kind';identity jsonb; i integer; expected jsonb; span daterange;
begin
 if kind not in('SOURCE_OBJECT','SOURCE_GROUP_CLIENT','SOURCE_CYCLE_CLIENT','SOURCE_GLOBAL_SETTINGS','SOURCE_CONTRACT_COVERAGE') then
  if p_token ? 'coverage' then raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
  return;
 end if;
 if kind='SOURCE_CONTRACT_COVERAGE' then
  if jsonb_typeof(p_token->'coverage') is distinct from 'array' or jsonb_array_length(p_token->'coverage')<>2
   or exists(select 1 from jsonb_array_elements(p_token->'coverage') x where jsonb_typeof(x) not in('string','null')
    or(jsonb_typeof(x)='string' and pg_input_is_valid(x#>>'{}','date') is distinct from true)) then
   raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
  span:=daterange((p_token->'coverage'->>0)::date,(p_token->'coverage'->>1)::date,'[]');
  if isempty(span) then raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
  return;
 end if;
 if p_token ? 'coverage' or jsonb_typeof(p_token->'json') is distinct from 'string' then
  raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
 if kind='SOURCE_GLOBAL_SETTINGS' then
  if p_token->>'json' is distinct from 'true' then raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
  return;
 end if;
 begin identity:=(p_token->>'json')::jsonb;
 exception when invalid_text_representation then raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end;
 if jsonb_typeof(identity) is distinct from 'array' or jsonb_array_length(identity)<>2 then
  raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
 if kind='SOURCE_OBJECT' then
  if jsonb_typeof(identity->0) is distinct from 'string' or identity->>0 not in(${sqlList})
   or jsonb_typeof(identity->1) is distinct from 'string' or pg_input_is_valid(identity->>1,'uuid') is distinct from true then
   raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
  expected:=jsonb_build_array(identity->>0,(identity->>1)::uuid);
 else
  for i in 0..1 loop
   if jsonb_typeof(identity->i) is distinct from 'string' or pg_input_is_valid(identity->>i,'uuid') is distinct from true then
    raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
  end loop;
  expected:=jsonb_build_array((identity->>0)::uuid,(identity->>1)::uuid);
 end if;
 if p_token->>'json' is distinct from expected::text then
  raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
end;
$source_token$;
alter function private.timesheet_correction_a0_source_token_assert_v1(jsonb) owner to postgres;
revoke all on function private.timesheet_correction_a0_source_token_assert_v1(jsonb) from public,anon,authenticated,service_role;
`;
save('forward/00_source_token_grammar.sqlinc',validator);
const reverses=[];
function transform(file,output,edit){const bytes=cp.execFileSync('git',['cat-file','blob',prior+':'+priorBase+file],{maxBuffer:4e6});
 const before=bytes.toString(),nl=before.includes('\r\n')?'\r\n':'\n';let s=before;const hunks=[];
 const once=(name,a,b)=>{a=a.replaceAll('\n',nl);b=b.replaceAll('\n',nl);assert.equal(s.split(a).length,2,name);s=s.replace(a,()=>b);hunks.push({name,before:a,after:b});};
 edit(once,()=>s,v=>{s=v;});let restored=s;for(const h of [...hunks].reverse()){assert.equal(restored.split(h.after).length,2,h.name);restored=restored.replace(h.after,()=>h.before);}assert.equal(restored,before);
 save('before/'+output,bytes);save('forward/'+output,s);reverses.push({source_commit:prior,source_path:priorBase+file,file:output,before_bytes:bytes.length,before_sha256:sha(bytes),after_bytes:Buffer.byteLength(s),after_sha256:sha(s),hunks});
}
transform('14102026_1216_a0_transaction_fence_schema.sql','01_complete_same_arbiter_schema.sqlinc',(once)=>{
 once('one nullable native date scope column','  intent_span numrange not null,','  intent_span numrange not null,\n  coverage_span daterange,');
 once('closed Source text identities and genuinely typed coverage',
 `    (token_kind in('FAMILY_UUID','PHYSICAL_UUID','WORKFLOW_UUID','CONTRACT_WEEK_UUID','SOURCE_CYCLE_UUID')
      and uuid_identity is not null and text_identity is null)
    or(token_kind in('BOOKING','KEY_NORM','ALIAS_JSON_STRING') and uuid_identity is null
      and text_identity is not null and octet_length(text_identity)<=1536)),`,
 `    (coverage_span is null and (
      (token_kind in('FAMILY_UUID','PHYSICAL_UUID','WORKFLOW_UUID','CONTRACT_WEEK_UUID','SOURCE_CYCLE_UUID')
        and uuid_identity is not null and text_identity is null)
      or(token_kind in('BOOKING','KEY_NORM','ALIAS_JSON_STRING','SOURCE_OBJECT','SOURCE_GROUP_CLIENT',
        'SOURCE_CYCLE_CLIENT','SOURCE_GLOBAL_SETTINGS') and uuid_identity is null
        and text_identity is not null and octet_length(text_identity)<=1536)))
    or(token_kind='SOURCE_CONTRACT_COVERAGE' and uuid_identity is not null and text_identity is null
      and coverage_span is not null and not isempty(coverage_span))),`);
 once('ordinary existing UUID constraint excludes the new coverage arm',
 '    where(uuid_identity is not null),','    where(uuid_identity is not null and coverage_span is null),');
 once('the one arbiter adds a two-class overlap constraint',
 '    where(text_identity is not null)\n);',
 `    where(text_identity is not null),
  constraint timesheet_correction_a0_fences_coverage_excl exclude using gist(
    token_kind public.gist_text_ops with =,uuid_identity public.gist_uuid_ops with =,
    coverage_span with &&,intent_span with &&,retained public.gist_bool_ops with <>)
    where(coverage_span is not null)
);
-- Retained scopes stay exact. Different overlapping scopes are not merged or
-- client-wide serialized; an ordinary range conflicts with either real scope.
create unique index timesheet_correction_a0_fences_coverage_retained_unique
  on private.timesheet_correction_a0_transaction_fences(token_kind,uuid_identity,coverage_span)
  where(retained and coverage_span is not null);`);
 once('require the actual boolean inequality opclass used by coverage',
 "  for r in select a.atttypid,c.collisdeterministic from pg_catalog.pg_attribute a",
 `  if not exists(select 1 from pg_catalog.pg_opclass c join pg_catalog.pg_namespace n on n.oid=c.opcnamespace
    join pg_catalog.pg_am a on a.oid=c.opcmethod where n.nspname='public' and c.opcname='gist_bool_ops'
      and c.opcintype='boolean'::regtype and a.amname='gist') then
    raise exception 'CORRECTION_A0_SOURCE_COVERAGE_OPCLASS_UNPROVED' using errcode='55000';end if;
  for r in select a.atttypid,c.collisdeterministic from pg_catalog.pg_attribute a`);
});
transform('14102026_1216_a0_transaction_fence_helpers.sqlinc','02_complete_scalar_helpers.sqlinc',(once,get)=>{
 const oldName='private.timesheet_correction_a0_fence_acquire_v1',newName='private.timesheet_correction_a0_fence_scalar_acquire_core_v1';
 const s=get(),begin=s.indexOf('create or replace function '+oldName+'('),end=s.indexOf('$function$;',begin)+'$function$;'.length;
 assert.ok(begin>=0&&end>begin);const before=s.slice(begin,end);
 const core=before.replace('function '+oldName+'(','function '+newName+'(')
  .replaceAll("'ALIAS_JSON_STRING','WORKFLOW_UUID','CONTRACT_WEEK_UUID','SOURCE_CYCLE_UUID'",
   "'ALIAS_JSON_STRING','WORKFLOW_UUID','CONTRACT_WEEK_UUID','SOURCE_CYCLE_UUID',\n        'SOURCE_OBJECT','SOURCE_GROUP_CLIENT','SOURCE_CYCLE_CLIENT','SOURCE_GLOBAL_SETTINGS'")
  .replaceAll("in('BOOKING','KEY_NORM','ALIAS_JSON_STRING')", "in('BOOKING','KEY_NORM','ALIAS_JSON_STRING','SOURCE_OBJECT','SOURCE_GROUP_CLIENT','SOURCE_CYCLE_CLIENT','SOURCE_GLOBAL_SETTINGS')")
  .replace("in('WORKFLOW_UUID','CONTRACT_WEEK_UUID','SOURCE_CYCLE_UUID') then", "in('WORKFLOW_UUID','CONTRACT_WEEK_UUID','SOURCE_CYCLE_UUID','SOURCE_OBJECT','SOURCE_GROUP_CLIENT','SOURCE_CYCLE_CLIENT','SOURCE_GLOBAL_SETTINGS') then");
 assert.notEqual(core,before);
 once('the full native scalar acquire body is extracted once; original eight domains unchanged',before,core);
 once('closed core ownership',`alter function ${oldName}(jsonb,boolean) owner to postgres;\nrevoke all on function ${oldName}(jsonb,boolean) from public,anon,authenticated,service_role;`,
 `alter function ${newName}(jsonb,boolean) owner to postgres;\nrevoke all on function ${newName}(jsonb,boolean) from public,anon,authenticated,service_role;`);
});
const coverage=`-- The same arbiter, native date overlap and opposite intent classes.
-- Engine contention/dirty visibility/old-RR and whole caller order remain UNRUN.
create or replace function private.timesheet_correction_a0_source_coverage_acquire_v1(p_client uuid,p_coverage daterange,p_retain boolean)
returns void language plpgsql volatile security definer set search_path='pg_catalog','pg_temp'
as $coverage$
declare old_timeout text:=current_setting('lock_timeout');span numrange;xid numeric;inserted integer;changed integer;
begin
 if p_client is null or p_coverage is null or isempty(p_coverage) or p_retain is null then
  raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
 if p_retain then span:=numrange(null,null,'()');else
  xid:=pg_current_xact_id()::text::numeric;span:=numrange(xid,xid+1,'[)');end if;
 begin
  perform set_config('lock_timeout','1ms',true);
  if p_retain then
   perform 1 from private.timesheet_correction_a0_transaction_fences f
    where f.retained and f.token_kind='SOURCE_CONTRACT_COVERAGE' and f.uuid_identity=p_client
      and f.coverage_span=p_coverage for update nowait;
   if not found then
    insert into private.timesheet_correction_a0_transaction_fences(token_kind,uuid_identity,retained,intent_span,coverage_span,touch_bit)
     values('SOURCE_CONTRACT_COVERAGE',p_client,true,span,p_coverage,true)
     on conflict(token_kind,uuid_identity,coverage_span) where retained and coverage_span is not null
      do nothing returning 1 into inserted;
    if inserted is distinct from 1 then
     perform 1 from private.timesheet_correction_a0_transaction_fences f
      where f.retained and f.token_kind='SOURCE_CONTRACT_COVERAGE' and f.uuid_identity=p_client
       and f.coverage_span=p_coverage for update nowait;
     if not found then raise exception 'CORRECTION_A0_TOKEN_CHANGED' using errcode='40001';end if;
    end if;
   end if;
   if inserted is distinct from 1 then
    update private.timesheet_correction_a0_transaction_fences f set touch_bit=not f.touch_bit
     where f.retained and f.token_kind='SOURCE_CONTRACT_COVERAGE' and f.uuid_identity=p_client and f.coverage_span=p_coverage;
    get diagnostics changed=row_count;
    if changed<>1 then raise exception 'CORRECTION_A0_TOKEN_CHANGED' using errcode='40001';end if;
   end if;
  else
   perform 1 from private.timesheet_correction_a0_transaction_fences f
    where f.retained and f.token_kind='SOURCE_CONTRACT_COVERAGE' and f.uuid_identity=p_client
     and f.coverage_span && p_coverage for share nowait;
   if not found then
    insert into private.timesheet_correction_a0_transaction_fences(token_kind,uuid_identity,retained,intent_span,coverage_span,touch_bit)
     values('SOURCE_CONTRACT_COVERAGE',p_client,false,span,p_coverage,false)
     on conflict on constraint timesheet_correction_a0_fences_coverage_excl do nothing returning 1 into inserted;
    if inserted=1 then
     delete from private.timesheet_correction_a0_transaction_fences f
      where not f.retained and f.token_kind='SOURCE_CONTRACT_COVERAGE' and f.uuid_identity=p_client
       and f.coverage_span=p_coverage and f.intent_span=span;
    else
     perform 1 from private.timesheet_correction_a0_transaction_fences f
      where f.retained and f.token_kind='SOURCE_CONTRACT_COVERAGE' and f.uuid_identity=p_client
       and f.coverage_span && p_coverage for share nowait;
     if not found then raise exception 'CORRECTION_A0_TOKEN_CHANGED' using errcode='40001';end if;
    end if;
   end if;
  end if;
  perform set_config('lock_timeout',old_timeout,true);
 exception when others then perform set_config('lock_timeout',old_timeout,true);raise;end;
end;
$coverage$;
alter function private.timesheet_correction_a0_source_coverage_acquire_v1(uuid,daterange,boolean) owner to postgres;
revoke all on function private.timesheet_correction_a0_source_coverage_acquire_v1(uuid,daterange,boolean) from public,anon,authenticated,service_role;

-- Preserve the exact original public-to-private caller signature. No Source
-- token means one unchanged native scalar call, preserving its old validation.
create or replace function private.timesheet_correction_a0_fence_acquire_v1(p_tokens jsonb,p_retain boolean)
returns void language plpgsql volatile security definer set search_path='pg_catalog','pg_temp'
as $source_acquire$
declare t jsonb;scalar_tokens jsonb;
begin
 if jsonb_typeof(p_tokens) is distinct from 'array' then
  perform private.timesheet_correction_a0_fence_scalar_acquire_core_v1(p_tokens,p_retain);return;end if;
 if not exists(select 1 from jsonb_array_elements(p_tokens) x where x->>'kind' in
  ('SOURCE_OBJECT','SOURCE_GROUP_CLIENT','SOURCE_CYCLE_CLIENT','SOURCE_GLOBAL_SETTINGS','SOURCE_CONTRACT_COVERAGE')) then
  perform private.timesheet_correction_a0_fence_scalar_acquire_core_v1(p_tokens,p_retain);return;end if;
 if p_retain is null then raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
 -- Validate the entire new envelope BEFORE an acquisition. Neither a malformed
 -- coverage token nor mixed native token may partially admit an owner.
 for t in select value from jsonb_array_elements(p_tokens) loop
  if jsonb_typeof(t) is distinct from 'object' or exists(select 1 from jsonb_object_keys(t) key
    where key not in('kind','uuid','json','coverage')) then
   raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
  perform private.timesheet_correction_a0_source_token_assert_v1(t);
  if t->>'kind'='SOURCE_CONTRACT_COVERAGE' and (t ? 'json' or jsonb_typeof(t->'uuid') is distinct from 'string'
    or pg_input_is_valid(t->>'uuid','uuid') is distinct from true) then
   raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
 end loop;
 select coalesce(jsonb_agg(value order by value),'[]'::jsonb) into scalar_tokens
  from jsonb_array_elements(p_tokens) where value->>'kind' is distinct from 'SOURCE_CONTRACT_COVERAGE';
 perform private.timesheet_correction_a0_fence_scalar_acquire_core_v1(scalar_tokens,p_retain);
 for t in select distinct value from jsonb_array_elements(p_tokens)
   where value->>'kind'='SOURCE_CONTRACT_COVERAGE' order by value loop
  perform private.timesheet_correction_a0_source_coverage_acquire_v1((t->>'uuid')::uuid,
   daterange((t->'coverage'->>0)::date,(t->'coverage'->>1)::date,'[]'),p_retain);
 end loop;
end;
$source_acquire$;
alter function private.timesheet_correction_a0_fence_acquire_v1(jsonb,boolean) owner to postgres;
revoke all on function private.timesheet_correction_a0_fence_acquire_v1(jsonb,boolean) from public,anon,authenticated,service_role;
`;
save('forward/03_coverage_and_compatibility_acquire.sqlinc',coverage);
save('COMPLETE_CURRENT_REVERSES.json',JSON.stringify({source_only:true,native:false,records:reverses},null,2)+'\n');
save('SOURCE_BOUNDARY.md',`# Same A0 arbiter: Source selector grammar and coverage construction\n\nUnreviewed source construction only. No native proof, canonical mount or caller admission. The former eight-domain paths use the preserved scalar body once. New Source text identities and native Contract dateranges remain explicit, closed, owner-only dependencies. One table is extended; no second authority, fake family/singleton UUID, business history touch or financial calculator is added.\n\nCoverage exclusion uses equality of Client and native date overlap, intent-span overlap and opposite retained/ordinary classes. Existing retained scopes are exact and can overlap without union, retirement or permanent failure of a future lawful scope. Exact retained ranges retain a partial unique key and native touch; ordinary spans use actual top-level XID and delete only their own inserted span. Unrelated Client/date scopes remain algebraically disjoint. These are source predicates, not observed GiST/dirty/RR semantics. [PG17 btree_gist](https://www.postgresql.org/docs/17/btree-gist.html) documents boolean support and inequality exclusion; actual installed extension/opclass/index shape and native both-order schedules remain held.\n\nActual Source footprint producer, OLD/NEW raw guards and their complete source/order census, full native business actor/Candidate/I1/family/Source/allocated phase CAS, between-RPC ACTIVE-request refusal/recovery, current security/registry/contract/verifier integration and all native positive/negative proof are still required. This package must not be mounted alone. No database/lock/fixture/release/external effect or cleanup occurred.\n`);
const manifest=fs.readdirSync(path.join(repo,base),{recursive:true}).filter(f=>fs.statSync(path.join(repo,base,f)).isFile()).sort().map(file=>{const b=fs.readFileSync(path.join(repo,base,file));return{file:file.replaceAll('\\','/'),bytes:b.length,sha256:sha(b)}});
save('SOURCE_MANIFEST.json',JSON.stringify({source_only:true,native:false,mount_ready:false,source_commit:prior,artifacts:manifest},null,2)+'\n');
console.log(JSON.stringify({source_only:true,native:false,artifacts:manifest.length,reverses:reverses.length}));

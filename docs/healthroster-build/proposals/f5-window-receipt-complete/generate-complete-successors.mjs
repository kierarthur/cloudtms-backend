import fs from 'node:fs';
import crypto from 'node:crypto';
import path from 'node:path';

const base='docs/healthroster-build/proposals/f5-window-receipt-complete';
const old='docs/healthroster-build/proposals/f5-window-rounding';
const out=base+'/generated';
fs.mkdirSync(out,{recursive:true});
const sha=data=>crypto.createHash('sha256').update(data).digest('hex');
const inputs=new Map();
const read=file=>{const data=fs.readFileSync(file);inputs.set(file,{file,bytes:data.length,sha256:sha(data)});return data.toString('utf8').replaceAll('\r\n','\n');};
const frozen=JSON.parse(read('docs/healthroster-build/F5_INTEGRATION_MANIFEST.json'));
for(const file of frozen.files){const data=fs.readFileSync(file.file);if(data.length!==file.bytes||sha(data)!==file.sha256)throw Error('FROZEN_CHANGED: '+file.file);}
const replace=(body,anchor,value,count=1)=>{if(body.split(anchor).length-1!==count)throw Error('ANCHOR_COUNT '+anchor.slice(0,100));return body.replaceAll(anchor,value);};
const functions=text=>[...text.matchAll(/create or replace function\s+([\w.]+)\s*\([\s\S]*?\bas\s+(\$[\w]*\$)[\s\S]*?\2\s*;/gi)].map(m=>({name:m[1],body:m[0]}));
const get=(text,name)=>{const rows=functions(text).filter(x=>x.name===name);if(rows.length!==1)throw Error('FUNCTION_COUNT '+name+' '+rows.length);return rows[0].body;};
const emit=(name,text)=>fs.writeFileSync(out+'/'+name,'-- Isolated complete source proposal. No installation/runtime acceptance.\n'+text+'\n');
const finFile='supabase/repeatable/13102026_1433_weekly_source_invoice_money_finalisation_v1.sql';
const admissionFile='supabase/repeatable/13102026_1434_weekly_source_invoice_money_admission_v1.sql';
const payFile='supabase/repeatable/13102026_1435_weekly_source_invoice_only_pay_isolation_v1.sql';
const batchFile='supabase/repeatable/15092026_1534_weekly_source_invoice_batch_integration_v1.sql';
const nativeAdmFile='supabase/repeatable/15092026_1534_weekly_source_invoice_admission_v1.sql';
const snapshotFile='supabase/repeatable/25072026_0002_private_invoice_presentation_snapshot_batch.sql';
const finSource=read(finFile),admSource=read(admissionFile),paySource=read(payFile),batchSource=read(batchFile),nativeAdm=read(nativeAdmFile);
emit('01_schema.sqlinc',read(base+'/SCHEMA.sqlinc'));

// Reuse the already reviewed complete money-origin/window/ROUND producers.
let money=read(old+'/generated/COMPLETE_MONEY_OWNER_PROPOSALS.sqlinc');
// Releases before FIN leave the same raw immutable source money. Their admitted
// choices still require the pre-manifest ROUND authority, including raw D0.
money=replace(money,"      and not exists(select 1 from public.weekly_source_invoice_hold_releases receipt where receipt.upload_row_id=s.id and receipt.hold_kind='VAT_FIT');",';');
emit('02_money_origins.sqlinc',money);
let privateOwners=read(old+'/PROPOSED_PRIVATE_OWNERS.sqlinc');
// D0/no hold still has no rounding authority. A non-SUM receipt owner may
// validate a manifest with no ROUND only when there are no receipt changes and
// its exact raw S09 delta is zero; this is qualified below after summation.
privateOwners=replace(privateOwners,"  if not found then raise exception 'WEEKLY_SOURCE_FROZEN_ROUNDING_AUTHORITY_REQUIRED' using errcode='55000'; end if;",'');
privateOwners=replace(privateOwners,'  if target is null or (raw_target-raw_vat)::bigint<>(rounding.vat_amount*100)::bigint then',`  if rounding.id is null then
    if target is null or raw_target<>raw_vat or target<>admitted_vat
      or exists(select 1 from jsonb_array_elements(witness) w where (w->>'original_vat_pence')::bigint<>(w->>'admitted_vat_pence')::bigint) then
      raise exception 'WEEKLY_SOURCE_FROZEN_ROUNDING_AUTHORITY_REQUIRED' using errcode='55000'; end if;
    return null;
  end if;
  if target is null or (raw_target-raw_vat)::bigint<>(rounding.vat_amount*100)::bigint then`);
privateOwners=replace(privateOwners,"      or member.actual_client_id<>manifest.client_id or member.movement_hash<>member.movement_economic_hash then",`      or member.actual_client_id<>manifest.client_id or member.movement_hash<>member.movement_economic_hash
      or private.weekly_source_invoice_movement_window_id_v1(member.id)<>coalesce(manifest.invoice_window_cycle_id,manifest.source_cycle_id) then`);
privateOwners=replace(privateOwners,"      or not(supplied ?& array['billing_movement_id','invoice_hold_release_id'])",`      or not(supplied ?& array['billing_movement_id','invoice_hold_release_id'])
      or jsonb_typeof(supplied->'billing_movement_id') is distinct from 'string'
      or not pg_input_is_valid(coalesce(supplied->>'billing_movement_id',''),'uuid')
      or (supplied->'invoice_hold_release_id'<>'null'::jsonb and (jsonb_typeof(supplied->'invoice_hold_release_id') is distinct from 'string'
        or not pg_input_is_valid(coalesce(supplied->>'invoice_hold_release_id',''),'uuid')))`);
privateOwners=replace(privateOwners,'    or presentation.total_pay_ex_vat<>0 or presentation.total_charge_ex_vat<>0 or presentation.vat_amount<>0 or presentation.total_inc_vat<>0',`    or presentation.total_pay_ex_vat<>0 or presentation.total_charge_ex_vat<>0 or presentation.vat_amount<>0 or presentation.total_inc_vat<>0
    or coalesce(line.hours_day,0)<>0 or coalesce(line.hours_night,0)<>0 or coalesce(line.hours_sat,0)<>0 or coalesce(line.hours_sun,0)<>0 or coalesce(line.hours_bh,0)<>0
    or coalesce(presentation.hours_day,0)<>0 or coalesce(presentation.hours_night,0)<>0 or coalesce(presentation.hours_sat,0)<>0 or coalesce(presentation.hours_sun,0)<>0 or coalesce(presentation.hours_bh,0)<>0`);
emit('03_receipt_and_window_owners.sqlinc',privateOwners+'\n\n'+read(base+'/PRIVATE_OWNERS.sqlinc').replace(/\brow\b/g,'v_record'));

let fin=get(finSource,'private.weekly_source_finalise_engine_v1');
fin=replace(fin,'  v_lock_discovery jsonb;',`  v_lock_discovery jsonb;
  v_window_discovery jsonb; v_window_observed jsonb; v_window_lock record;
  v_invoice_window_ids uuid[]; v_round_scope record; v_round_id uuid;`);
fin=replace(fin,'    select * into v_cycle from public.weekly_source_cycles where id=v_cycle_id for update;',`    -- Complete source + candidate invoice windows were discovered BEFORE I-1.
    -- Take every cycle in UUID order, then compare full original row witnesses.
    for v_window_lock in select cycle.* from public.weekly_source_cycles cycle
      where cycle.id=any(v_invoice_window_ids) order by cycle.id for update
    loop
      if to_jsonb(v_window_lock) is distinct from v_window_discovery->'cycles'->v_window_lock.id::text then
        raise exception 'WEEKLY_SOURCE_INVOICE_WINDOW_SET_CHANGED_DURING_LOCK' using errcode='40001'; end if;
    end loop;
    if (select count(*) from public.weekly_source_cycles cycle where cycle.id=any(v_invoice_window_ids))<>cardinality(v_invoice_window_ids) then
      raise exception 'WEEKLY_SOURCE_INVOICE_WINDOW_SET_CHANGED_DURING_LOCK' using errcode='40001'; end if;
    select * into v_cycle from public.weekly_source_cycles where id=v_cycle_id;`);
fin=replace(fin,'  v_lock_observed:=jsonb_build_object(',`  v_window_observed:=private.weekly_source_invoice_window_lock_inventory_v1(v_upload.id,v_generation,v_cycle.id,v_client_id);
  if v_lock_phase=1 then
    v_window_discovery:=v_window_observed;
    select array_agg(key::uuid order by key::uuid) into v_invoice_window_ids
      from jsonb_object_keys(v_window_discovery->'cycles') key;
  elsif v_window_observed is distinct from v_window_discovery then
    raise exception 'WEEKLY_SOURCE_INVOICE_WINDOW_SET_CHANGED_DURING_LOCK' using errcode='40001'; end if;
  v_lock_observed:=jsonb_build_object(`);
// Source chronology/currentness remains based on genuine original Source weeks.
// Adding invoice-only manifests must not multiply or redirect that authority.
for(const alias of ['later_manifest','prior_manifest'])fin=fin.replaceAll(`on ${alias}.final_revision_id=`, `on coalesce(${alias}.invoice_window_cycle_id,${alias}.source_cycle_id)=${alias}.source_cycle_id and ${alias}.final_revision_id=`);
const blockStart=fin.indexOf('  for v_manifest in\n');
const blockEnd=fin.indexOf('\n  if v_finalisation_lock_operation is not null then',blockStart);
if(blockStart<0||blockEnd<0)throw Error('FIN_MANIFEST_BOUNDARIES');
let partition=read(old+'/generated/FIN_MANIFEST_WINDOW_PARTITION.sqlinc').replace(/^--[^\n]*\n/,'');
// The ROUND authority is born before any immutable manifest hash/membership.
const round=`  for v_round_scope in select distinct movement.actual_client_id client_id,
      private.weekly_source_invoice_movement_window_id_v1(movement.id) invoice_window_cycle_id
    from public.weekly_source_billing_movements movement where movement.final_revision_id=v_revision_id
      and movement.source_line_kind<>'INVOICE_VAT_ROUNDING'
    order by client_id,invoice_window_cycle_id
  loop
    if not(v_round_scope.invoice_window_cycle_id=any(v_invoice_window_ids)) then
      raise exception 'WEEKLY_SOURCE_INVOICE_WINDOW_SET_GREW_AFTER_LOCK' using errcode='40001'; end if;
    v_round_id:=private.weekly_source_vat_rounding_movement_insert_v1(v_revision_id,v_round_scope.client_id,null,v_round_scope.invoice_window_cycle_id);
    if v_round_id is not null then v_movement_count:=v_movement_count+1; end if;
  end loop;
`;
fin=fin.slice(0,blockStart)+round+partition+fin.slice(blockEnd);
emit('04_complete_fin.sqlinc',fin);

let admission=get(admSource,'public.weekly_source_invoice_admit_atomic_v1');
admission=replace(admission,'  v_manifest public.weekly_source_client_manifests%rowtype;',`  v_manifest public.weekly_source_client_manifests%rowtype;
  v_invoice_window public.weekly_source_cycles%rowtype;
  v_receipt_map jsonb; v_round_authority jsonb; v_round_presentation jsonb;`);
// Native hold-release takes group→cycle. Admission must serialize with it using
// the same group + complete sorted cycle locks before manifest/member writes.
admission=replace(admission,'  select * into v_manifest from public.weekly_source_client_manifests\n  where id=v_manifest_id for update;',`  select * into v_manifest from public.weekly_source_client_manifests where id=v_manifest_id;
  if not found then raise exception 'WEEKLY_SOURCE_INVOICE_MANIFEST_CHANGED' using errcode='40001'; end if;
  perform 1 from public.weekly_source_groups where id=v_manifest.source_group_id for update;
  perform 1 from public.weekly_source_cycles where id in (v_manifest.source_cycle_id,coalesce(v_manifest.invoice_window_cycle_id,v_manifest.source_cycle_id)) order by id for update;
  select * into v_manifest from public.weekly_source_client_manifests where id=v_manifest_id for update;`);
admission=replace(admission,"  perform private.weekly_source_office_authority_v1(\n    v_actor,'ADMIT_SOURCE_INVOICE'",`  select * into strict v_invoice_window from public.weekly_source_cycles
    where id=coalesce(v_manifest.invoice_window_cycle_id,v_manifest.source_cycle_id);
  if v_invoice_window.source_group_id<>v_manifest.source_group_id
    or v_invoice_window.finalisation_week_ending<>v_manifest.finalisation_week_ending
    or (v_invoice_window.scope_client_id is not null and v_invoice_window.scope_client_id<>v_manifest.client_id) then
    raise exception 'WEEKLY_SOURCE_INVOICE_WINDOW_SCOPE_INVALID' using errcode='55000'; end if;
  perform private.weekly_source_office_authority_v1(
    v_actor,'ADMIT_SOURCE_INVOICE'`);
admission=replace(admission,'           or movement.actual_client_id is distinct from v_manifest.client_id)',`           or movement.actual_client_id is distinct from v_manifest.client_id
           or private.weekly_source_invoice_movement_window_id_v1(movement.id)<>v_invoice_window.id
           or (v_invoice_window.id<>v_cycle.id and (movement.source_line_kind not in ('SOURCE_INVOICE_CORRECTION','INVOICE_VAT_ROUNDING')
             or movement.total_pay_ex_vat<>0 or exists(select 1 from jsonb_each_text(movement.canonical_pay_vector_json->'minutes') v where v.value::numeric<>0)
             or exists(select 1 from jsonb_each_text(movement.canonical_pay_vector_json->'hours') v where v.value::numeric<>0))))`);
admission=replace(admission,'  select * into strict v_client from public.clients where id=v_manifest.client_id for share;',`  v_receipt_map:=private.weekly_source_invoice_admission_receipt_map_v1(v_manifest.id);
  v_round_authority:=private.weekly_source_invoice_rounding_receipt_set_v1(v_manifest.id,v_receipt_map);
  select * into strict v_client from public.clients where id=v_manifest.client_id for share;`);
admission=replace(admission,"      'automatic_consolidation','ONE_CLIENT_ONE_FINALISED_CYCLE'",`      'automatic_consolidation','ONE_CLIENT_ONE_FINALISED_CYCLE'
    )||case when v_manifest.invoice_window_cycle_id is null then '{}'::jsonb else jsonb_build_object(
      'invoice_window_cycle_id',v_invoice_window.id,'source_finalisation_week_ending',v_cycle.finalisation_week_ending`);
admission=replace(admission,"'source_finalisation_week_ending',v_cycle.finalisation_week_ending\n    )", "'source_finalisation_week_ending',v_cycle.finalisation_week_ending\n    ) end");
admission=replace(admission,'    v_is_rounding:=v_movement.source_line_kind=',"    v_round_presentation:=null;\n    v_is_rounding:=v_movement.source_line_kind=");
admission=replace(admission,'    if v_is_expense then\n      v_hours_day:=0;',`    if v_is_rounding and v_round_authority is not null then
      if v_ids<>array[(v_round_authority->>'rounding_movement_id')::uuid] then
        raise exception 'WEEKLY_SOURCE_ROUNDING_PRESENTATION_MEMBER_INVALID' using errcode='55000'; end if;
      v_round_presentation:=v_round_authority;
      v_vat:=(v_round_authority->>'admitted_rounding_vat_pence')::numeric/100;
      v_total:=v_vat;
      v_source_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_INVOICE_ROUNDING_PRESENTATION_V1',
        jsonb_build_object('original_movement_source_hash',encode(v_source_hash,'hex'),
          'rounding_authority_sha256',v_round_authority->>'authority_sha256'));
      -- New authority hash is reproducible from exact stored native typmods.
      -- Retained ordinary/NULL-rule presentation hash inputs remain untouched.
      v_hours_day:=v_hours_day::numeric(7,2);v_hours_night:=v_hours_night::numeric(7,2);
      v_hours_sat:=v_hours_sat::numeric(7,2);v_hours_sun:=v_hours_sun::numeric(7,2);v_hours_bh:=v_hours_bh::numeric(7,2);
      v_total_pay:=v_total_pay::numeric(12,2);v_total_charge:=v_total_charge::numeric(12,2);
      v_vat_rate:=v_vat_rate::numeric(5,2);v_vat:=v_vat::numeric(12,2);v_total:=v_total::numeric(12,2);
    end if;
    if v_is_expense then
      v_hours_day:=0;`);
admission=admission.replaceAll("end\n    );\n    insert into public.weekly_source_invoice_presentation_lines(","end||case when v_round_presentation is null then '{}'::jsonb else jsonb_build_object('weekly_source_rounding_authority',v_round_presentation) end\n    );\n    insert into public.weekly_source_invoice_presentation_lines(");
admission=replace(admission,'v_presentation_hash,v_release_id,','v_presentation_hash,v_release_id,v_round_presentation,');
admission=replace(admission,'amount_authority,presentation_hash,invoice_hold_release_id,companion_presentation_line_id','amount_authority,presentation_hash,invoice_hold_release_id,invoice_rounding_authority_json,companion_presentation_line_id');
admission=replace(admission,"end,'WEEKLY_SOURCE:'||v_presentation_id::text",`end||case when v_round_presentation is null then '{}'::jsonb else jsonb_build_object(
        'weekly_source_rounding_authority',v_round_presentation,
        'presentation_disposition',case when (v_round_presentation->>'suppress_visible_rounding_line')::boolean
          then 'SUPPRESS_ZERO_ROUNDING_AUTHORITY' else 'VISIBLE' end) end,'WEEKLY_SOURCE:'||v_presentation_id::text`);
admission=replace(admission,"'PLACED',true,v_manifest.source_cycle_id,v_manifest.source_cycle_id,","'PLACED',true,v_invoice_window.id,v_invoice_window.id,");
emit('05_complete_admission.sqlinc',admission);

let allocation=get(admSource,'private.weekly_source_invoice_allocation_assert_v1');
allocation=replace(allocation,'  if exists(\n    select 1\n    from public.weekly_source_invoice_line_bindings binding',`  perform private.weekly_source_invoice_receipt_projection_assert_v1(p_invoice_id);
  if exists(
    select 1
    from public.weekly_source_invoice_line_bindings binding`);
allocation=replace(allocation,'        or binding.client_id is distinct from v_invoice.client_id',`        or placement.original_automatic_cycle_id is distinct from coalesce(manifest.invoice_window_cycle_id,manifest.source_cycle_id)
        or private.weekly_source_invoice_movement_window_id_v1(movement.id) is distinct from coalesce(manifest.invoice_window_cycle_id,manifest.source_cycle_id)
        or binding.client_id is distinct from v_invoice.client_id`);
let issue=get(admSource,'private.weekly_source_invoice_issue_validate_v1');
issue=replace(issue,"    -- \"NHSP physical negative and positive independence\"",`    -- Immutable automatic invoice windows remain distinct from original Source
    -- cycle witnesses. The header must describe the actual complete bound set.
    if coalesce(v_invoice.header_snapshot_json#>'{meta,contained_invoice_window_cycle_ids}',
        v_invoice.header_snapshot_json#>'{meta,contained_source_cycle_ids}','[]'::jsonb)
      is distinct from coalesce((select jsonb_agg(window_id order by window_id) from (
        select distinct coalesce(manifest.invoice_window_cycle_id,manifest.source_cycle_id) window_id
          from public.weekly_source_invoice_line_bindings binding
          join public.weekly_source_manifest_movements mm on mm.billing_movement_id=binding.billing_movement_id
          join public.weekly_source_client_manifests manifest on manifest.id=mm.client_manifest_id
          where binding.invoice_id=p_invoice_id and binding.state='CURRENT') actual),'[]'::jsonb) then
      v_codes:=v_codes||'WEEKLY_SOURCE_ISSUE_WINDOW_SCOPE_INCONSISTENT'::text;
    end if;
    -- "NHSP physical negative and positive independence"`);
issue=replace(issue,'private.weekly_source_invoice_presentation_source_hash_v1(array_agg(movement.id order by movement.id),presentation.invoice_hold_release_id) rebuilt','private.weekly_source_invoice_presentation_expected_source_hash_v1(presentation.id) rebuilt');
issue=replace(issue,'pg_catalog.sum((private.weekly_source_invoice_movement_money_v1(movement.id)->>\'vat_pence\')::numeric/100)',"pg_catalog.sum((private.weekly_source_invoice_admitted_member_money_v1(presentation.id,movement.id)->>'vat_pence')::numeric/100)");
issue=replace(issue,'pg_catalog.sum((private.weekly_source_invoice_movement_money_v1(movement.id)->>\'gross_pence\')::numeric/100)',"pg_catalog.sum((private.weekly_source_invoice_admitted_member_money_v1(presentation.id,movement.id)->>'gross_pence')::numeric/100)");
// Allocation is already invoked by this full native issue owner. Its new exact
// authority assertion refuses malformed or stale receipt maps before sealed OK.
emit('06_complete_allocation_issue.sqlinc',allocation+'\n\n'+issue);

let header=get(nativeAdm,'private.weekly_source_invoice_header_refresh_v1');
header=replace(header,'  v_cycle_ids jsonb;','  v_cycle_ids jsonb; v_invoice_window_ids jsonb;');
header=replace(header,'manifest.id as manifest_id,manifest.source_cycle_id,','manifest.id as manifest_id,manifest.source_cycle_id,coalesce(manifest.invoice_window_cycle_id,manifest.source_cycle_id) invoice_window_cycle_id,');
header=replace(header,'  into v_manifest_ids,v_cycle_ids,v_week_endings,v_backing_reports;',`    ,coalesce((select jsonb_agg(w.invoice_window_cycle_id order by w.invoice_window_cycle_id)
      from (select distinct invoice_window_cycle_id from facts) w),'[]'::jsonb)
  into v_manifest_ids,v_cycle_ids,v_week_endings,v_backing_reports,v_invoice_window_ids;`);
header=replace(header,"            'backing_report_numbers',v_backing_reports","            'backing_report_numbers',v_backing_reports\n          )||case when v_invoice_window_ids=v_cycle_ids then '{}'::jsonb else jsonb_build_object(\n            'contained_invoice_window_cycle_ids',v_invoice_window_ids");
header=replace(header,"'contained_invoice_window_cycle_ids',v_invoice_window_ids\n          ),true", "'contained_invoice_window_cycle_ids',v_invoice_window_ids\n          ) end,true");
header=replace(header,"coalesce(invoice.header_snapshot_json->'meta','{}'::jsonb)","(coalesce(invoice.header_snapshot_json->'meta','{}'::jsonb)-'contained_invoice_window_cycle_ids')");
let move=get(admSource,'public.weekly_source_invoice_move_atomic_v1');
move=move.replaceAll('v_destination_manifest.source_cycle_id','coalesce(v_destination_manifest.invoice_window_cycle_id,v_destination_manifest.source_cycle_id)');
// Invoice evidence is genuine original Source evidence, not target-window data.
emit('07_complete_header_move.sqlinc',header+'\n\n'+move);

const batchOwners=functions(batchSource).map(({name,body})=>{
  if(name==='private.weekly_source_invoice_batch_snapshot_v1')body=replace(body,"        'members',coalesce((",`        'invoice_window_cycle_id',coalesce(manifest.invoice_window_cycle_id,manifest.source_cycle_id),
        'admission_receipts',private.weekly_source_invoice_admission_receipt_map_v1(manifest.id),
        'members',coalesce((`);
  if(name==='private.weekly_source_invoice_batch_rows_v1'){
    body=replace(body,'coalesce(pg_catalog.sum(movement.vat_amount),0) vat_amount,','(private.weekly_source_invoice_manifest_admission_money_v1(manifest.id)->>\'vat_pence\')::numeric/100 vat_amount,');
    body=replace(body,'coalesce(pg_catalog.sum(movement.total_inc_vat),0) total_inc_vat,','(private.weekly_source_invoice_manifest_admission_money_v1(manifest.id)->>\'gross_pence\')::numeric/100 total_inc_vat,');
    body=replace(body,"        and pg_catalog.bool_and(binding.id is null)",`        and pg_catalog.bool_and(binding.id is null)
        and not exists(select 1 from public.weekly_source_manifest_movements held
          join public.weekly_source_billing_movements hm on hm.id=held.billing_movement_id
          where held.client_manifest_id=manifest.id and hm.source_facts_json->>'invoice_hold'='VAT_FIT'
            and not exists(select 1 from public.weekly_source_invoice_hold_releases receipt
              where receipt.upload_row_id::text=hm.source_facts_json->>'upload_row_id' and receipt.hold_kind='VAT_FIT'))`);
  }
  if(name==='public.weekly_source_invoice_edit_context_v1')body=replace(body,"    'source_cycle_id',v_manifest.source_cycle_id,",`    'source_cycle_id',v_manifest.source_cycle_id,
    'invoice_window_cycle_id',coalesce(v_manifest.invoice_window_cycle_id,v_manifest.source_cycle_id),`);
  if(name==='public.weekly_source_invoice_batch_admit_atomic_v1'){
    body=replace(body,"  select pg_catalog.bool_and(manifest.invoice_state='ADMITTED'",`  -- Whole batch group/cycle inventory precedes every nested manifest lock.
  perform 1 from public.weekly_source_groups g where g.id in (
    select manifest.source_group_id from public.weekly_source_client_manifests manifest
    join jsonb_array_elements(v_refs) item on manifest.id=(item->>'client_manifest_id')::uuid)
    order by g.id for update;
  perform 1 from public.weekly_source_cycles c where c.id in (
    select manifest.source_cycle_id from public.weekly_source_client_manifests manifest
    join jsonb_array_elements(v_refs) item on manifest.id=(item->>'client_manifest_id')::uuid
    union select coalesce(manifest.invoice_window_cycle_id,manifest.source_cycle_id)
      from public.weekly_source_client_manifests manifest
      join jsonb_array_elements(v_refs) item on manifest.id=(item->>'client_manifest_id')::uuid)
    order by c.id for update;
  select pg_catalog.bool_and(manifest.invoice_state='ADMITTED'`);
    body=body.replaceAll("'source_cycle_id',v_manifest.source_cycle_id,", "'source_cycle_id',v_manifest.source_cycle_id,'invoice_window_cycle_id',coalesce(v_manifest.invoice_window_cycle_id,v_manifest.source_cycle_id),");
    body=body.replaceAll("left_result->>'source_cycle_id'<>right_result->>'source_cycle_id'", "left_result->>'invoice_window_cycle_id'<>right_result->>'invoice_window_cycle_id'");
  }
  return body;
});
emit('08_complete_batch_detail.sqlinc',batchOwners.join('\n\n'));

// Rewrite actual manifest selectors only; movement predicates remain intact.
// Full Source owners, never Bank writers. Canonical NULL/window selection is
// applied before any revision-level pay/Correction identity query.
const payOwners=functions(paySource).map(({body})=>{
  body=body.replace(/(public\.weekly_source_client_manifests\s+(?:cm|m))\s+where\s+(?:cm|m)\.source_profile_kind<>'SELF_BILL_REPORT' and (?:cm|m)\.source_line_kind not in \([^)]*\)\s+and/g,'$1 where');
  body=body.replace(/(from|join) public\.weekly_source_client_manifests\s+(manifest|client_manifest|cm|m|other_manifest)\b(?:\s+(on|where))?/g,(all,clause,alias,next)=>{
    if(!next)return all;
    return `${clause} public.weekly_source_client_manifests ${alias} ${next} coalesce(${alias}.invoice_window_cycle_id,${alias}.source_cycle_id)=${alias}.source_cycle_id and`;
  });
  // Unaliased first manifest used by pay contexts: declare canonical source
  // admission on exact revision selector; sibling invoice-only manifests no
  // longer invalidate the native one-client pay context.
  body=body.replaceAll('from public.weekly_source_client_manifests\n  where final_revision_id=', 'from public.weekly_source_client_manifests\n  where coalesce(invoice_window_cycle_id,source_cycle_id)=source_cycle_id and final_revision_id=');
  return body;
});
emit('09_complete_pay_consumers.sqlinc',payOwners.join('\n\n'));

const extraSourceFiles=['15092026_1534_weekly_source_correct_final_source_v1.sql','15092026_1534_weekly_source_read_projections_v1.sql','19092026_0100_weekly_source_correction_cancel_v1.sql'];
const payNames=new Set(functions(paySource).map(x=>x.name));
const extraOwners=[];
for(const file of extraSourceFiles){
  for(let {name,body} of functions(read('supabase/repeatable/'+file))){
    if(!body.includes('public.weekly_source_client_manifests')||payNames.has(name))continue;
    // CorrectFinal apply updates all partitions on invoice lifecycle together;
    // only context/root pay discovery needs canonical single-manifest selection.
    body=body.replace(/from public\.weekly_source_client_manifests\s+where final_revision_id=/g,'from public.weekly_source_client_manifests where coalesce(invoice_window_cycle_id,source_cycle_id)=source_cycle_id and final_revision_id=');
    body=body.replaceAll('where other_manifest.final_revision_id=', 'where coalesce(other_manifest.invoice_window_cycle_id,other_manifest.source_cycle_id)=other_manifest.source_cycle_id and other_manifest.final_revision_id=');
    extraOwners.push(body);
  }
}
// Full query/export/evidence readers retain original identities, expose actual
// immutable invoice week via manifest field. No rewrite of source evidence.
for(const file of ['17092026_1200_weekly_source_audit_and_export_v1.sql','24092026_1440_weekly_source_invoice_evidence_lookup_v1.sql']){
  for(const {body} of functions(read('supabase/repeatable/'+file)))if(body.includes('public.weekly_source_client_manifests'))extraOwners.push(body);
}
emit('10_complete_source_readers_correction.sqlinc',extraOwners.join('\n\n'));

// Complete adjacent remaining manifest consumers. Native completed source
// history is one genuine revision/client, while invoice membership keeps its
// actual partition identity in the existing invoice/detail readers above.
const canonicalSelectors=body=>body.replace(/(from|join) public\.weekly_source_client_manifests\s+(manifest|cm|m|later_manifest|prior_manifest)\b\s+(on|where)/g,
  (all,clause,alias,next)=>`${clause} public.weekly_source_client_manifests ${alias} ${next} coalesce(${alias}.invoice_window_cycle_id,${alias}.source_cycle_id)=${alias}.source_cycle_id and`);
const moreFiles=['01102026_1201_weekly_source_combined_workspace.sql','02102026_1601_weekly_source_manual_review_v1.sql',
  '04102026_2206_weekly_source_accepted_publication_evidence_v1.sql','05102026_0428_weekly_source_manual_review_commands_v2.sql',
  '05102026_0710_weekly_source_invoice_duty_v1.sql','09102026_1130_weekly_source_released_review_visibility_v1.sql'];
const latestMore=new Map();
for(const file of moreFiles)for(const {name,body} of functions(read('supabase/repeatable/'+file)))
  if(body.includes('public.weekly_source_client_manifests')&&!payNames.has(name))latestMore.set(name,canonicalSelectors(body));
emit('10b_complete_remaining_manifest_consumers.sqlinc',[...latestMore.values()].join('\n\n'));

// Preserve latest root Report wrappers/refusals and workspace/report semantics.
// These are separate successors, never writes into its active inputs. The
// canonical source selectors are applied inside the exact complete root body.
const root='../healthroster-v2-backend-13102026';
const rootReport=root+'/supabase/repeatable/13102026_1344_weekly_source_self_bill_report/';
const rootUnion=[];
for(const file of ['13102026_1344_correct_final_refusals.sqlinc','13102026_1344_invoice_duty.sqlinc','13102026_1344_workspace_scopes.sqlinc']){
  for(const {body} of functions(read(rootReport+file))){
    let union=canonicalSelectors(body).replace(/from public\.weekly_source_client_manifests\s+where final_revision_id=/g,'from public.weekly_source_client_manifests where coalesce(invoice_window_cycle_id,source_cycle_id)=source_cycle_id and final_revision_id=');
    union=union.replaceAll('where other_manifest.final_revision_id=', 'where coalesce(other_manifest.invoice_window_cycle_id,other_manifest.source_cycle_id)=other_manifest.source_cycle_id and other_manifest.final_revision_id=');
    rootUnion.push(union);
  }
}
emit('10c_complete_root_report_consumer_union.sqlinc',rootUnion.join('\n\n'));
const retained=[];
for(const file of [rootReport+'13102026_1344_finalise.sqlinc',root+'/supabase/repeatable/13102026_1359_weekly_source_timing_coverage_group_guards_v1.sql',root+'/supabase/repeatable/13102026_1404_weekly_source_client_group_move_guard_v1.sql'])
  for(const {body} of functions(read(file)))if(body.includes('public.weekly_source_client_manifests'))retained.push(body);
emit('10d_retained_report_and_date_boundaries.sqlinc',retained.join('\n\n'));

let snapshot=get(read(snapshotFile),'private._invoice_presentation_snapshot_batch');
// Materialize the qualification before any component split/filter. All claimed
// suppressions are checked, even if a later component would otherwise be empty.
snapshot=replace(snapshot,'  invoice_line_base as materialized (',`  source_rounding_suppression as materialized (
    select il.invoice_id,il.id invoice_line_id,
      private.weekly_source_invoice_rounding_suppression_witness_v1(il.id) witness
    from public.invoice_lines il where il.invoice_id in(select invoice_id from requested_invoices)
      and il.meta_json->>'presentation_disposition'='SUPPRESS_ZERO_ROUNDING_AUTHORITY'
  ),
  source_rounding_suppression_agg as materialized (
    select invoice_id,jsonb_agg(witness order by invoice_line_id) witnesses
    from source_rounding_suppression group by invoice_id
  ),
  source_receipt_support as materialized (
    select ri.invoice_id,private.weekly_source_invoice_receipt_support_v1(ri.invoice_id) witness from requested_invoices ri
  ),
  invoice_line_base as materialized (`);
snapshot=replace(snapshot,'    where il.invoice_id in(select invoice_id from requested_invoices)\n  ),\n  invoice_line_component_raw',`    where il.invoice_id in(select invoice_id from requested_invoices)
      and not exists(select 1 from source_rounding_suppression suppressed where suppressed.invoice_line_id=il.id and suppressed.witness is not null)
  ),
  invoice_line_component_raw`);
snapshot=replace(snapshot,"        'page_geometry',coalesce(i.header_snapshot_json->>'page_geometry',sd.invoice_document_presentation_json->>'page_geometry',sd.import_config_json#>>'{invoice_document_presentation,page_geometry}','A4_PORTRAIT_210X297MM')) presentation_model,",`        'page_geometry',coalesce(i.header_snapshot_json->>'page_geometry',sd.invoice_document_presentation_json->>'page_geometry',sd.import_config_json#>>'{invoice_document_presentation,page_geometry}','A4_PORTRAIT_210X297MM'))
        ||case when srs.witnesses is null then '{}'::jsonb else jsonb_build_object(
          'source_rounding_suppressions',srs.witnesses) end presentation_model,`);
snapshot=replace(snapshot,"      coalesce(sa.supporting_sources,'[]'::jsonb) supporting_sources,",`      coalesce(sa.supporting_sources,'[]'::jsonb)||case when srs.witnesses is null then '[]'::jsonb else
        jsonb_build_array(jsonb_build_object('source_system','WEEKLY_SOURCE_ZERO_ROUNDING_AUTHORITY',
          'qualified_suppressions',srs.witnesses)) end||case when srr.witness is null then '[]'::jsonb else
          jsonb_build_array(jsonb_build_object('source_system','WEEKLY_SOURCE_INVOICE_RECEIPT_SUPPORT','receipt_support',srr.witness)) end supporting_sources,`);
snapshot=replace(snapshot,'    left join supporting_manifest_agg sma on sma.invoice_id=i.id','    left join source_rounding_suppression_agg srs on srs.invoice_id=i.id\n    left join supporting_manifest_agg sma on sma.invoice_id=i.id');
snapshot=replace(snapshot,'    left join source_rounding_suppression_agg srs on srs.invoice_id=i.id','    left join source_rounding_suppression_agg srs on srs.invoice_id=i.id\n    left join source_receipt_support srr on srr.invoice_id=i.id');
const snapshotOriginal=read(snapshotFile);
const snapshotOriginalFunction=get(snapshotOriginal,'private._invoice_presentation_snapshot_batch');
emit('11_complete_native_snapshot_assembler.sqlinc',replace(snapshotOriginal,snapshotOriginalFunction,snapshot));

// FINAL_ISSUE transport reuses its stored frozen snapshot/model hash. It must
// remain exact, so emit the COMPLETE original owner as an explicit retained
// audited input, not a guessed post-issue requalification from current rows.
const transportFile='supabase/repeatable/24072026_1217_invoice_async_processor_contract_v4/24072026_1217_private_invoice_document_advance_batch_v6_downstream.sql';
emit('12_retained_final_issue_transport.sqlinc',read(transportFile));

const privateSignatures=[
  'weekly_source_invoice_vat_target_pence_v1(numeric,numeric,text,numeric)',
  'weekly_source_invoice_window_scope_v1(uuid,uuid,uuid,uuid)',
  'weekly_source_invoice_rounding_receipt_set_v1(uuid,jsonb)',
  'weekly_source_invoice_rounding_suppression_witness_v1(uuid)',
  'weekly_source_invoice_window_lock_inventory_v1(uuid,bigint,uuid,uuid)',
  'weekly_source_invoice_window_cycle_v1(uuid,uuid)',
  'weekly_source_invoice_admission_receipt_map_v1(uuid)',
  'weekly_source_invoice_manifest_admission_money_v1(uuid)',
  'weekly_source_invoice_rounding_authority_rebuild_v1(uuid)',
  'weekly_source_invoice_presentation_expected_source_hash_v1(uuid)',
  'weekly_source_invoice_admitted_member_money_v1(uuid,uuid)',
  'weekly_source_invoice_receipt_projection_assert_v1(uuid)',
  'weekly_source_invoice_receipt_support_v1(uuid)',
  'weekly_source_invoice_legacy_render_support_v1(uuid)',
  'weekly_source_invoice_movement_window_id_v1(uuid)',
  'weekly_source_vat_rounding_movement_insert_v1(uuid,uuid,uuid,uuid)'
];
emit('13_private_acl.sqlinc',privateSignatures.map(signature=>`alter function private.${signature} owner to postgres;\nrevoke all on function private.${signature} from public,anon,authenticated,service_role;`).join('\n'));
const aclInput=read(root+'/supabase/repeatable/13102026_1436_weekly_source_invoice_money_acl_inventory_v1.sql');
let helperContract=get(aclInput,'private._weekly_source_acl_private_helper_contract_v1');
const additions=privateSignatures.map(s=>'private.'+s).filter(s=>!helperContract.includes("('"+s+"')"));
helperContract=replace(helperContract,';\n$function$;',',\n'+additions.map(s=>"    ('"+s+"')").join(',\n')+';\n$function$;');
const serviceContract=get(aclInput,'private._weekly_source_acl_service_rpc_contract_v1');
if(!serviceContract.includes('private.weekly_source_pay_actor_allowed_v1(uuid)'))throw Error('PP_SERVICE_UNION_MISSING');
emit('13b_complete_acl_helper_service_union.sqlinc',helperContract+'\n\n'+serviceContract);

const contract=read('broker/src/invoice-presentation-contract.js');
const contractComplete=replace(contract,"  const lines = requireArray(model.lines, 'INVOICE_PRESENTATION_MODEL_INVALID');",`  validateZeroRoundingSuppressions(model);
  const lines = requireArray(model.lines, 'INVOICE_PRESENTATION_MODEL_INVALID');`);
fs.writeFileSync(out+'/invoice-presentation-contract.js',contractComplete+'\n'+read(base+'/zero-rounding-model-qualification.js'));
let templates=read('broker/src/invoice-document-templates.js');
templates=replace(templates,'export function renderInvoiceLineTable(model = {}) {',`export function renderInvoiceLineTable(model = {}) {
  validateZeroRoundingSuppressions(model);`);
templates=replace(templates,"import { validateFrozenPresentationModel } from './invoice-presentation-contract.js';", "import { validateFrozenPresentationModel, validateZeroRoundingSuppressions } from './invoice-presentation-contract.js';");
fs.writeFileSync(out+'/invoice-document-templates.js',templates);
// Runtime hashes the complete model before rendering; retains frozen identity
// checks. Its private import points to the same generated contract module.
fs.writeFileSync(out+'/invoice-queue-runtime.js',read('broker/src/invoice-queue-runtime.js'));

// Legacy /api/invoices/:id and render_bundle remain reachable in this repo.
// Keep internal native lines; filter only the visible JSON after qualification.
let legacy=get(read('supabase/repeatable/08012026_invoice_rpcs_issue_hold_paid_creditnote.sql'),'public.invoice_render_manifest');
legacy=replace(legacy,'  return v_manifest;',`  v_manifest:=v_manifest||private.weekly_source_invoice_legacy_render_support_v1(p_invoice_id);
  return v_manifest;`);
legacy=replace(legacy,"      from lines l\n    ), '[]'::jsonb),",`      from lines l where l.meta_json->>'presentation_disposition' is distinct from 'SUPPRESS_ZERO_ROUNDING_AUTHORITY'
        or private.weekly_source_invoice_rounding_suppression_witness_v1(l.id) is null
    ), '[]'::jsonb),`);
emit('14_complete_legacy_render_manifest.sqlinc',legacy);
const index=read('broker/src/index.js');
const indexGet=(name,next)=>{const start=index.indexOf('async function '+name+'('),end=index.indexOf(next,start);if(start<0||end<start)throw Error('JS_BOUNDARY '+name);return index.slice(start,end).trim();};
let legacyRenderer=indexGet('_renderInvoiceBundleAndStore','async function handleInvoiceBatchIssueCandidates(');
legacyRenderer=replace(legacyRenderer,'    const inv = manifest.invoice || manifest.invoice_row || null;',`    validateLegacyInvoiceManifestSuppressions(manifest);
    const inv = manifest.invoice || manifest.invoice_row || null;`);
let legacyDetail=indexGet('handleGetInvoice','// ============================================================================\n// NEW: ensureInvoicePdf');
legacyDetail=replace(legacyDetail,'    const lineRows = Array.isArray(manifest.lines) ? manifest.lines : [];',`    validateLegacyInvoiceManifestSuppressions(manifest);
    const lineRows = Array.isArray(manifest.lines) ? manifest.lines : [];`);
fs.writeFileSync(out+'/legacy-index-owner-successors.js',read(base+'/legacy-render-qualification.js')+'\n\n'+legacyDetail+'\n\n'+legacyRenderer+'\n');

// Reviewable exhaustive file/function census over CURRENT saved local source.
// Historical definitions remain marked; root's installed/latest union decides
// controlling owners. No filtered-away Bank sources or false closure claim.
const census=[];
const walk=dir=>fs.readdirSync(dir,{withFileTypes:true}).flatMap(e=>e.isDirectory()?walk(dir+'/'+e.name):[dir+'/'+e.name]);
for(const file of [...walk('supabase/repeatable'),...walk(root+'/supabase/repeatable')].filter(x=>/\.(sql|sqlinc)$/.test(x))){
 const text=fs.readFileSync(file,'utf8').replaceAll('\r\n','\n');
 for(const {name,body} of functions(text))if(body.includes('public.weekly_source_client_manifests'))census.push({file,function:name,sha256:sha(body),references:body.split('public.weekly_source_client_manifests').length-1,
   decision:/bpay|banking/.test(name)?'RETAIN_BANK_OWNER_ROOT_REVIEW':name.includes('invoice')?'ORIGINAL_SOURCE_AUTHORITY_ACTUAL_MANIFEST_INVOICE_WEEK':/pay|selected_source|proposal_revision|precondition/.test(name)?'CANONICAL_SOURCE_WINDOW_REQUIRED':'ALL_PARTITIONS_OR_ORIGINAL_SOURCE_READER_REVIEW',
   successor:fs.readdirSync(out).filter(x=>x.endsWith('.sqlinc')).reverse().find(x=>functions(fs.readFileSync(out+'/'+x,'utf8')).some(f=>f.name===name))||null});
}
fs.writeFileSync(out+'/READER_CENSUS.json',JSON.stringify({contract:'WEEKLY_SOURCE_WINDOW_READER_CENSUS_V1',includes_historical_definitions:true,root_latest_installed_census_pending:true,rows:census},null,2)+'\n');
const artifacts=fs.readdirSync(out).filter(x=>x!=='SOURCE_MANIFEST.json').sort().map(file=>{const data=fs.readFileSync(out+'/'+file);return {file:out+'/'+file,bytes:data.length,sha256:sha(data)};});
const signatures=artifacts.filter(x=>x.file.endsWith('.sqlinc')).flatMap(a=>functions(fs.readFileSync(a.file,'utf8')).map(f=>({file:a.file,name:f.name,definition_header:f.body.slice(0,f.body.search(/\bas\s+\$/i))})));
fs.writeFileSync(out+'/SOURCE_MANIFEST.json',JSON.stringify({contract:'HEALTHROSTER_COMPLETE_WINDOW_RECEIPT_PROPOSAL_V1',frozen_28_unchanged:true,source_only:true,db_executed:false,inputs:[...inputs.values()],artifacts,functions:signatures},null,2)+'\n');
process.stdout.write(JSON.stringify({generated:true,inputs:inputs.size,artifacts:artifacts.length,full_definitions:signatures.length,reader_rows:census.length})+'\n');

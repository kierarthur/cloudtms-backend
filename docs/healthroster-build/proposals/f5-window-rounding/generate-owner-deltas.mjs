// Produces separate review artifacts only, never writes a frozen Money input.
import fs from 'node:fs';
import crypto from 'node:crypto';
const out='docs/healthroster-build/proposals/f5-window-rounding/generated';
fs.mkdirSync(out,{recursive:true});
const manifest=JSON.parse(fs.readFileSync('docs/healthroster-build/F5_INTEGRATION_MANIFEST.json','utf8'));
for(const file of manifest.files) {
  const data=fs.readFileSync(file.file);
  if(data.length!==file.bytes||crypto.createHash('sha256').update(data).digest('hex')!==file.sha256) throw new Error('FROZEN_INPUT_CHANGED: '+file.file);
}
const source=fs.readFileSync('supabase/repeatable/13102026_1431_weekly_source_invoice_money_owners_v1.sql','utf8');
const get=(name)=>{const start=source.indexOf('create or replace function '+name+'(');if(start<0)throw new Error(name);return source.slice(start,source.indexOf('$function$;',start)+11);};
const replace=(body,old,value,count=1)=>{if(body.split(old).length-1!==count)throw new Error('ANCHOR_COUNT: '+old.slice(0,80));return body.replaceAll(old,value);};
let hash=get('private.weekly_source_billing_movement_hash_v1');
hash=replace(hash,"'vat_rounding_revision_id'","'vat_rounding_revision_id','invoice_window_cycle_id'");
let correction=get('private.weekly_source_invoice_correction_insert_v1');
correction=replace(correction,'  facts jsonb; pay_vector jsonb;', '  facts jsonb; v_invoice_window_scope jsonb; pay_vector jsonb;');
const refusalStart=correction.indexOf('  if window_cycle<>cycle_id then');
const refusalEnd=correction.indexOf('  amounts:=',refusalStart);
if(refusalStart<0||refusalEnd<0)throw new Error('WINDOW_REFUSAL_BOUNDARY_MISSING');
correction=correction.slice(0,refusalStart)+`  -- The movement retains the actual Source revision/cycle. Only its immutable
  -- invoice membership witness identifies another real OPEN client window.
  v_invoice_window_scope:=private.weekly_source_invoice_window_scope_v1(
    cycle_id,current_line.client_id,current_line.upload_row_id,window_cycle);
`+correction.slice(refusalEnd);
correction=replace(correction,"'invoice_window_cycle_id',window_cycle,","'invoice_window_cycle_id',window_cycle,'invoice_window_scope',v_invoice_window_scope,");
let rounding=get('private.weekly_source_vat_rounding_movement_insert_v1');
rounding=replace(rounding,'p_client_id uuid,p_identity_movement_id uuid)','p_client_id uuid,p_identity_movement_id uuid,p_invoice_window_cycle_id uuid)');
rounding=replace(rounding,'  id uuid; facts jsonb;', '  id uuid; pending_hold_rows uuid[]; facts jsonb;');
rounding=replace(rounding,"  if revision.state not in ('CURRENT','PREPARED')",`  if p_invoice_window_cycle_id is null or not exists(select 1 from public.weekly_source_cycles actual
    join public.weekly_source_cycles original on original.id=revision.source_cycle_id and actual.source_group_id=original.source_group_id
    where actual.id=p_invoice_window_cycle_id and (actual.scope_client_id is null or actual.scope_client_id=p_client_id)) then
    raise exception 'WEEKLY_SOURCE_VAT_ROUNDING_WINDOW_INVALID' using errcode='55000'; end if;
  if revision.state not in ('CURRENT','PREPARED')`);
rounding=replace(rounding,'m.vat_rounding_revision_id=revision.id and m.actual_client_id=p_client_id;',
  'm.vat_rounding_revision_id=revision.id and m.actual_client_id=p_client_id\n    and coalesce(m.invoice_window_cycle_id,m.finalisation_cycle_id)=p_invoice_window_cycle_id;');
rounding=replace(rounding,"and m.source_line_kind<>'INVOICE_VAT_ROUNDING';", "and m.source_line_kind<>'INVOICE_VAT_ROUNDING'\n      and private.weekly_source_invoice_movement_window_id_v1(m.id)=p_invoice_window_cycle_id;");
const targetStart=rounding.indexOf('  target:=case rules');
const targetEnd=rounding.indexOf('  if target is null',targetStart);
rounding=rounding.slice(0,targetStart)+"  target:=private.weekly_source_invoice_vat_target_pence_v1(net,vat,rules->>'invoice_vat_method',rate);\n"+rounding.slice(targetEnd);
rounding=replace(rounding,'  difference:=(target-vat)::bigint; if difference=0 then return null; end if;',`  select array_agg(distinct s.id order by s.id) into pending_hold_rows
    from public.weekly_source_billing_movements m join public.weekly_source_upload_rows s
      on s.id::text=m.source_facts_json->>'upload_row_id'
    where m.final_revision_id=revision.id and m.actual_client_id=p_client_id
      and private.weekly_source_invoice_movement_window_id_v1(m.id)=p_invoice_window_cycle_id
      and m.source_facts_json->>'invoice_hold'='VAT_FIT'
      and not exists(select 1 from public.weekly_source_invoice_hold_releases receipt where receipt.upload_row_id=s.id and receipt.hold_kind='VAT_FIT');
  difference:=(target-vat)::bigint;
  -- This proposed zero authority is restricted to unresolved actual VAT_FIT.
  -- D0 with no hold (including Report210825) retains no movement/visible line.
  if difference=0 and pending_hold_rows is null then return null; end if;`);
rounding=replace(rounding,"'invoice_vat_method',rules->>'invoice_vat_method','membership_net_pence',net,", "'invoice_vat_method',rules->>'invoice_vat_method','membership_net_pence',net,\n    'invoice_window_cycle_id',p_invoice_window_cycle_id,'pending_vat_fit_upload_row_ids',coalesce(to_jsonb(pending_hold_rows),'[]'::jsonb),");
rounding=replace(rounding,"'invoice_timesheet_id',identity_row.invoice_timesheet_id));", "'invoice_timesheet_id',identity_row.invoice_timesheet_id)\n    ||case when p_invoice_window_cycle_id=revision.source_cycle_id then '{}'::jsonb else jsonb_build_object('invoice_window_cycle_id',p_invoice_window_cycle_id) end);");
rounding=replace(rounding,'vat_rounding_revision_id,final_revision_id,finalisation_cycle_id,actual_client_id,','vat_rounding_revision_id,invoice_window_cycle_id,final_revision_id,finalisation_cycle_id,actual_client_id,');
rounding=replace(rounding,'values(revision.id,revision.id,revision.source_cycle_id,p_client_id,','values(revision.id,case when p_invoice_window_cycle_id=revision.source_cycle_id then null else p_invoice_window_cycle_id end,revision.id,revision.source_cycle_id,p_client_id,');
const movementWindow=`create or replace function private.weekly_source_invoice_movement_window_id_v1(p_movement_id uuid)
returns uuid language plpgsql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare m public.weekly_source_billing_movements%rowtype; target uuid;
begin
  select * into strict m from public.weekly_source_billing_movements where id=p_movement_id;
  target:=case when m.source_line_kind='INVOICE_VAT_ROUNDING' then coalesce(m.invoice_window_cycle_id,m.finalisation_cycle_id)
    when m.source_line_kind='SOURCE_INVOICE_CORRECTION' and m.movement_role in ('REVERSAL','REPLACEMENT')
      then nullif(m.source_facts_json->>'invoice_window_cycle_id','')::uuid else m.finalisation_cycle_id end;
  if target is null or not exists(select 1 from public.weekly_source_cycles actual join public.weekly_source_cycles original
    on original.id=m.finalisation_cycle_id and actual.source_group_id=original.source_group_id
    where actual.id=target and (actual.scope_client_id is null or actual.scope_client_id=m.actual_client_id)) then
    raise exception 'WEEKLY_SOURCE_INVOICE_MOVEMENT_WINDOW_INVALID' using errcode='55000'; end if;
  return target;
end;
$function$;`;
const wrapper=`create or replace function private.weekly_source_vat_rounding_movement_insert_v1(p_final_revision_id uuid,p_client_id uuid,p_identity_movement_id uuid)
returns uuid language sql volatile security definer set search_path to 'pg_catalog','pg_temp'
as $function$ select private.weekly_source_vat_rounding_movement_insert_v1(p_final_revision_id,p_client_id,p_identity_movement_id,
  (select source_cycle_id from public.weekly_source_final_revisions where id=p_final_revision_id)); $function$;`;
fs.writeFileSync(out+'/COMPLETE_MONEY_OWNER_PROPOSALS.sqlinc','-- REVIEW ONLY: requires proposed schema/helpers and root-composed consumers.\n'+[hash,movementWindow,correction,rounding,wrapper].join('\n\n')+'\n');
const fin=fs.readFileSync('supabase/repeatable/13102026_1433_weekly_source_invoice_money_finalisation_v1.sql','utf8');
const blockStart=fin.indexOf('  for v_manifest in\n');
const blockEnd=fin.indexOf('\n  if v_finalisation_lock_operation is not null then',blockStart);
if(blockStart<0||blockEnd<0)throw new Error('FIN_MANIFEST_BOUNDARY_MISSING');
let block=fin.slice(blockStart,blockEnd);
const selectEnd=block.indexOf('  loop\n')+7;
block=`  for v_manifest in
    select scope.client_id,scope.invoice_window_cycle_id,actual.finalisation_week_ending as invoice_week_ending
    from (select movement.actual_client_id as client_id,
      private.weekly_source_invoice_movement_window_id_v1(movement.id) as invoice_window_cycle_id
      from public.weekly_source_billing_movements movement where movement.final_revision_id=v_revision_id
      union select v_client_id,v_cycle.id) scope
    join public.weekly_source_cycles actual on actual.id=scope.invoice_window_cycle_id
    where scope.client_id is not null order by scope.client_id,scope.invoice_window_cycle_id
  loop
`+block.slice(selectEnd);
block=block.replaceAll("'finalisation_week_ending',v_cycle.finalisation_week_ending,", "'finalisation_week_ending',v_manifest.invoice_week_ending,");
block=replace(block,"'backing_report_number',v_backing_report_number,", "'backing_report_number',v_backing_report_number,");
// Only foreign window adds a new manifest JSON identity; original window/null
// keeps the exact existing field set and byte identity.
block=replace(block,"      )||case when v_rule_version is null", "      )||case when v_manifest.invoice_window_cycle_id=v_cycle.id then '{}'::jsonb else jsonb_build_object('invoice_window_cycle_id',v_manifest.invoice_window_cycle_id) end\n      ||case when v_rule_version is null");
block=block.replaceAll('and movement.actual_client_id=v_manifest.client_id;', 'and movement.actual_client_id=v_manifest.client_id\n      and private.weekly_source_invoice_movement_window_id_v1(movement.id)=v_manifest.invoice_window_cycle_id;');
block=block.replaceAll('and movement.actual_client_id=v_manifest.client_id),','and movement.actual_client_id=v_manifest.client_id\n         and private.weekly_source_invoice_movement_window_id_v1(movement.id)=v_manifest.invoice_window_cycle_id),');
block=block.replaceAll('expense.final_revision_id=v_revision_id and event.client_id=v_manifest.client_id',
  'expense.final_revision_id=v_revision_id and event.client_id=v_manifest.client_id\n            and v_manifest.invoice_window_cycle_id=v_cycle.id');
block=replace(block,'and snapshot_line.client_id=v_manifest.client_id),',
  'and snapshot_line.client_id=v_manifest.client_id\n         and v_manifest.invoice_window_cycle_id=v_cycle.id),');
block=replace(block,'and event.client_id=v_manifest.client_id),',
  'and event.client_id=v_manifest.client_id\n         and v_manifest.invoice_window_cycle_id=v_cycle.id),');
block=replace(block,'final_revision_id,source_group_id,source_cycle_id,client_id,','final_revision_id,source_group_id,source_cycle_id,invoice_window_cycle_id,client_id,');
block=replace(block,'v_revision_id,v_group.id,v_cycle.id,v_manifest.client_id,','v_revision_id,v_group.id,v_cycle.id,case when v_manifest.invoice_window_cycle_id=v_cycle.id then null else v_manifest.invoice_window_cycle_id end,v_manifest.client_id,');
block=replace(block,'v_cycle.finalisation_week_ending,v_backing_report_number,v_client_manifest_hash,','v_manifest.invoice_week_ending,v_backing_report_number,v_client_manifest_hash,');
// Source pay/expense statistics belong only to the one canonical source window.
block=replace(block,'    ) returning id into v_client_manifest_id;',`    ) returning id into v_client_manifest_id;`);
fs.writeFileSync(out+'/FIN_MANIFEST_WINDOW_PARTITION.sqlinc','-- REVIEW ONLY: insert ROUND once per actual scope before this block; foreign-window manifests are invoice-only in pay callers.\n'+block+'\n');
const artifacts=fs.readdirSync(out).filter(file=>file!=='ARTIFACT_HASHES.json').sort().map(file=>{const bytes=fs.readFileSync(out+'/'+file);return{file:out+'/'+file,bytes:bytes.length,sha256:crypto.createHash('sha256').update(bytes).digest('hex')}});
fs.writeFileSync(out+'/ARTIFACT_HASHES.json',JSON.stringify({contract:'HEALTHROSTER_F5_WINDOW_RECEIPT_SOURCE_PROPOSAL_V1',frozen_inputs_unchanged:true,artifacts},null,2)+'\n');
process.stdout.write(JSON.stringify({generated:true,frozen_inputs_unchanged:true,artifacts})+'\n');

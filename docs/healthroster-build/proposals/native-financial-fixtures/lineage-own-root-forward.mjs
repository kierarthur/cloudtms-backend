import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';
const root=path.resolve(process.argv[2]),out=path.dirname(fileURLToPath(import.meta.url));
const file='supabase/repeatable/13102026_1433_weekly_source_invoice_money_finalisation_v1.sql';
const bytes=fs.readFileSync(path.join(root,file)),source=bytes.toString('utf8');
const fn=source.match(/create or replace function public\.weekly_source_timesheet_lineage_ensure_atomic_v1\([\s\S]*?\$function\$;\r?\n/);
if(!fn)throw Error('NFX_LINEAGE_COMPLETE_OWNER_MISSING');
const original=fn[0],normalized=original.replaceAll('\r\n','\n');let body=normalized;
function once(anchor,value){if(body.split(anchor).length-1!==1)throw Error('NFX_LINEAGE_ANCHOR_CHANGED');body=body.replaceAll(anchor,()=>value);}
once('  v_finalisation_root_set text;','  v_finalisation_root_set text;\n  v_legacy_root_set_now text;\n  v_legacy_root_ids uuid[];');
const anchor="  v_family:=v_lock_result->'families'->0;\n\n  -- Review F5";
const delta=`  v_family:=v_lock_result->'families'->0;

  -- Only the native INSERT RETURNING above can extend this transaction's
  -- already complete LEGACY lock census. It owns this previously invisible
  -- root, and the unchanged native I-1 call has now succeeded. A lookup,
  -- replay, external root, rotation or nonempty unbound NEXT operation cannot
  -- enter this branch. Existing discovery/root-set growth 40001 guards remain.
  if v_created_timesheet and v_finalisation_lock_operation.id is null
     and v_finalisation_lock_operation_text is null
     and v_finalisation_root_set is not null
     and (select active_owner from private.bpay_next_module_control where id=1)='LEGACY' then
    v_legacy_root_set_now:=nullif(pg_catalog.btrim(coalesce(
      pg_catalog.current_setting('cloudtms.weekly_source_finalisation_root_set',true),''
    )),'');
    if v_legacy_root_set_now is distinct from v_finalisation_root_set
       or v_known_root_timesheet_id is not null
       or v_timesheet.timesheet_id is null
       or v_timesheet.booking_id is distinct from v_booking_id
       or v_timesheet.contract_id is distinct from v_resolution.contract_id
       or v_timesheet.week_ending_date is distinct from v_week_ending_date then
      raise exception 'WEEKLY_SOURCE_FINALISATION_ROOT_SET_CHANGED_DURING_LOCK' using errcode='40001';
    end if;
    -- EMPTY_LOCKED_SET is an explicit complete empty census, never a UUID.
    if v_legacy_root_set_now='EMPTY_LOCKED_SET' then
      v_legacy_root_ids:=array[]::uuid[];
    else
      begin
        v_legacy_root_ids:=pg_catalog.string_to_array(v_legacy_root_set_now,',')::uuid[];
      exception when invalid_text_representation then
        raise exception 'WEEKLY_SOURCE_FINALISATION_ROOT_SET_CHANGED_DURING_LOCK' using errcode='40001';
      end;
      if pg_catalog.array_position(v_legacy_root_ids,null::uuid) is not null then
        raise exception 'WEEKLY_SOURCE_FINALISATION_ROOT_SET_CHANGED_DURING_LOCK' using errcode='40001';
      end if;
    end if;
    select array_agg(own_root order by own_root) into v_legacy_root_ids
    from (select distinct own_root from unnest(
      v_legacy_root_ids||array[v_timesheet.timesheet_id]::uuid[]) own_root) own_roots;
    perform pg_catalog.set_config('cloudtms.weekly_source_finalisation_root_set',
      pg_catalog.array_to_string(v_legacy_root_ids,','),true);
  end if;

  -- Review F5`;
once(anchor,delta);
const reverse=body.replaceAll(delta,()=>anchor).replaceAll('  v_finalisation_root_set text;\n  v_legacy_root_set_now text;\n  v_legacy_root_ids uuid[];',()=> '  v_finalisation_root_set text;');
if(reverse!==normalized)throw Error('NFX_LINEAGE_REVERSE_HUNK_NOT_EXACT');
const nextBlock=x=>x.match(/    if v_finalisation_lock_operation\.id is not null then[\s\S]*?    end if;/)?.[0];
if(!nextBlock(original)||nextBlock(normalized)!==nextBlock(body))throw Error('NFX_NATIVE_NEXT_HOOK_CHANGED');
if((body.match(/\$function\$/g)||[]).length!==2||/\\(?:if|endif)\b|\$\&|\$`/.test(body))throw Error('NFX_LINEAGE_REPLACEMENT_CORRUPT');
const exactBody=original.includes('\r\n')?body.replaceAll('\n','\r\n'):body;
if(nextBlock(original)!==nextBlock(exactBody))throw Error('NFX_NATIVE_NEXT_HOOK_BYTES_CHANGED');
const output='-- Root-review complete forward Source owner; no frozen input edited.\n'+exactBody+
'\nalter function public.weekly_source_timesheet_lineage_ensure_atomic_v1(uuid,uuid) owner to postgres;\nrevoke all on function public.weekly_source_timesheet_lineage_ensure_atomic_v1(uuid,uuid) from public,anon,authenticated,service_role;\ngrant execute on function public.weekly_source_timesheet_lineage_ensure_atomic_v1(uuid,uuid) to service_role;\n';
fs.writeFileSync(path.join(out,'LINEAGE_OWN_NEW_ROOT_FORWARD.sqlinc'),output);
const sha=x=>crypto.createHash('sha256').update(x).digest('hex');
const receipt={contract:'NFX_LINEAGE_OWN_NEW_ROOT_FORWARD_V1',native_acceptance:false,signature:'public.weekly_source_timesheet_lineage_ensure_atomic_v1(uuid,uuid)',input:{file,source_file_sha256:sha(bytes),complete_definition_sha256:sha(original)},output:{file:'LINEAGE_OWN_NEW_ROOT_FORWARD.sqlinc',sha256:sha(output),bytes:Buffer.byteLength(output)},deltas:['two new local declarations','one bounded append after genuine native INSERT RETURNING and successful native I-1'],reverse_hunk_matches_entire_input_definition:true,native_next_hook_equal:true,unchanged:['service actor/admission','complete G1 two-phase discovery/locked CAS','all external/preexisting-root 40001 fences','native NEXT own new-member hook','native INSERT/immutable lineage/settings/audit writes','native family/current/rotation/root-authorization guards','original FIN context restoration on success/exception'],pending_hosted_cases:['two Source rows one newly created Contract/week LEGACY root','positive source-expense row sharing own created root','external root after EMPTY census before creation ->40001','external rotation/root change ->40001','changed entry GUC ->40001','malformed/sentinel parser','standalone no inherited context unchanged','published NEXT exact operation member/replay unchanged','whole native sorted-family concurrency']};
fs.writeFileSync(path.join(out,'LINEAGE_OWN_NEW_ROOT_FORWARD.json'),JSON.stringify(receipt,null,2)+'\n');console.log(JSON.stringify(receipt.output));

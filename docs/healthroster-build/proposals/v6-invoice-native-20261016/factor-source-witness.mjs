import assert from 'node:assert/strict';

// Split SQL select operands without evaluating or rewriting their expressions.
export function splitSql(s){
 const out=[];let depth=0,quoted=false,start=0;
 for(let i=0;i<s.length;i++){
  const c=s[i];
  if(c==="'"){if(quoted&&s[i+1]==="'"){i++;continue;}quoted=!quoted;}
  else if(!quoted){if(c==='(')depth++;else if(c===')')depth--;else if(c===','&&depth===0){out.push(s.slice(start,i).trim());start=i+1;}}
 }
 assert.equal(depth,0);assert.equal(quoted,false);out.push(s.slice(start).trim());return out;
}

export function factorWeeklySourceAdmission(source,replace){
 const start=source.indexOf('    insert into public.invoice_lines(');
 const end=source.indexOf('    ) returning id into v_invoice_line_id;',start)+'    ) returning id into v_invoice_line_id;'.length;
 assert.ok(start>0&&end>start);
 const original=source.slice(start,end),valuesStart=original.indexOf('    ) values (');
 const columns=original.slice(original.indexOf('invoice_lines(')+14,valuesStart).split(',').map(s=>s.trim());
 const values=splitSql(original.slice(valuesStart+'    ) values ('.length,original.lastIndexOf('    ) returning')));
 assert.equal(columns.length,values.length);assert.equal(columns[0],'invoice_id');assert.equal(columns.at(-1),'source_key');
 source=replace(source,'  v_is_report boolean:=false;', '  v_native_line jsonb;\n  v_is_report boolean:=false;');
 source=replace(source,original,`    select pg_catalog.jsonb_build_object(
      ${columns.map((col,i)=>`'${col}',${values[i]}`).join(',\n      ')}) into v_native_line;
    if private.timesheet_contract_source_original_eligible_v1(v_movement.invoice_timesheet_id) then
      perform private.invoice_source_admission_line_enter_v1(v_invoice_id,v_actor,v_manifest.id,v_native_line);
    end if;
    insert into public.invoice_lines(${columns.join(',')})
    select ${columns.map(col=>'planned.'+col).join(',')}
    from pg_catalog.jsonb_populate_record(null::public.invoice_lines,v_native_line) planned
    returning id into v_invoice_line_id;
    if private.timesheet_contract_source_original_eligible_v1(v_movement.invoice_timesheet_id) then
      perform private.invoice_source_admission_line_finish_v1(v_invoice_id,v_invoice_line_id);
    end if;`);
 source=replace(source,`  return pg_catalog.jsonb_build_object(
    'ok',true,'status','ADMITTED','idempotent',false,`,`  if exists(select 1 from public.invoices i where i.id=v_invoice_id and i.header_snapshot_json?'source_original_native_admission') then
    raise exception 'SOURCE_ORIGINAL_ADMISSION_STAMP_SURVIVED' using errcode='55000';end if;
  return pg_catalog.jsonb_build_object(
    'ok',true,'status','ADMITTED','idempotent',false,`);
 return source;
}

export function factorSourceWitness(generation,replace){
 const a=generation.indexOf('  inserted_lines as (');
 const b=generation.indexOf('  all_target_lines as materialized (',a);
 assert.ok(a>0&&b>a);
 const old=generation.slice(a,b);
 const columns=old.slice(old.indexOf('insert into public.invoice_lines(')+'insert into public.invoice_lines('.length,old.indexOf('\n    )')).trim().split(',').map(s=>s.trim());
 const selected=old.slice(old.indexOf('    select ')+11,old.indexOf('\n    from line_union l join target_headers'));
 const values=splitSql(selected);
 assert.equal(columns.length,values.length);
 assert.equal(columns[0],'invoice_id');assert.equal(columns.at(-1),'source_key');
 const planned=`  native_planned_lines as materialized (
    select l.chunk_id,
      ${values.map((v,i)=>`${v} as ${columns[i]}`).join(',\n      ')}
    from line_union l
  ),
  source_original_chunks as materialized (
    select distinct vc.id from write_eligible_chunks vc join members m on m.chunk_id=vc.id
    where private.timesheet_contract_source_original_eligible_v1(m.timesheet_id)
  ),
  source_original_plan_inputs as materialized (
    select s.id chunk_id,jsonb_agg(to_jsonb(l)-'chunk_id' order by l.timesheet_id,l.source_key) lines
    from source_original_chunks s join native_planned_lines l on l.chunk_id=s.id group by s.id
  ),
  source_original_missing_witness as materialized (
    select s.id chunk_id from source_original_chunks s join public.invoice_operation_chunks c on c.id=s.id
    where not(c.payload_json?'source_original_line_plan')
  ),
`;
 generation=replace(generation,'  line_totals as materialized (',planned+'  line_totals as materialized (');
 const projected=columns.map(c=>c==='meta_json'?`l.meta_json||case when private.timesheet_contract_source_original_eligible_v1(l.timesheet_id)
        then jsonb_build_object('source_original_generation_chunk_id',l.chunk_id,
          'source_original_line_plan_sha256',(select c.payload_json#>>'{source_original_line_plan,sha256}' from public.invoice_operation_chunks c where c.id=l.chunk_id))
        else '{}'::jsonb end`:`l.${c}`).join(',\n      ');
 const insert=`  inserted_lines as (
    insert into public.invoice_lines(${columns.join(',')})
    select ${projected}
    from native_planned_lines l join target_headers h on h.invoice_id=l.invoice_id
    on conflict(invoice_id,source_key) do nothing
    returning invoice_id,timesheet_id,total_charge_ex_vat,vat_amount,total_inc_vat
  ),
`;
 generation=replace(generation,old,insert);
 generation=replace(generation,`      and exists(
        select 1
        from public.invoices existing_header
        where existing_header.id=s.planned_invoice_id)`,`      and exists(
        select 1
        from public.invoices existing_header
        where existing_header.id=s.planned_invoice_id)
      and (not private.timesheet_contract_source_original_eligible_v1(s.timesheet_id)
        or exists(select 1 from public.invoice_operation_chunks native_owner
          where native_owner.id=s.chunk_id and native_owner.payload_json?'source_original_line_plan'))`);
 generation=replace(generation,`      where h.id=e.invoice_id)
  ),
  deferred_new_headers as (`,`      where h.id=e.invoice_id)
      and not exists(select 1 from source_original_missing_witness missing where missing.chunk_id=e.chunk_id)
  ),
  deferred_existing_source_headers as (
    update public.invoice_operation_chunks c set status='QUEUED',phase='COMMIT',
      payload_json=jsonb_set(c.payload_json,'{source_original_line_plan}',private.invoice_source_original_line_plan_v1(c.id,p.lines),true),
      progress_json=coalesce(c.progress_json,'{}'::jsonb)||jsonb_build_object('status_message','Original Source line plan recorded'),
      error_json=null,lease_owner=null,lease_token=null,lease_expires_at_utc=null,updated_at_utc=v_now
    from source_original_missing_witness missing join source_original_plan_inputs p on p.chunk_id=missing.chunk_id
    join existing_target_headers h on h.chunk_id=p.chunk_id
    where c.id=p.chunk_id returning c.id
  ),
  deferred_new_headers as (`);
 generation=replace(generation,`  deferred_new_headers as (
    update public.invoice_operation_chunks c
    set status='QUEUED',`,`  deferred_new_headers as (
    update public.invoice_operation_chunks c
    set payload_json=case when exists(select 1 from source_original_plan_inputs p where p.chunk_id=c.id)
        then jsonb_set(c.payload_json,'{source_original_line_plan}',private.invoice_source_original_line_plan_v1(c.id,
          (select p.lines from source_original_plan_inputs p where p.chunk_id=c.id)),true)
        else c.payload_json end,
      status='QUEUED',`);
 // I2 has this stray credit-only alias in the non-credit audits CTE.
 generation=replace(generation,`    left join public.tms_users u on u.id=o.actor_user_id
    where c.resume_invoice_id is null
    returning id`,`    left join public.tms_users u on u.id=o.actor_user_id
    returning id`);
 generation=replace(generation,'begin\n  -- The optional scoped arm',`begin
  perform private.invoice_source_original_claim_enter_v1(p_claims,v_now);
  -- The optional scoped arm`);
 generation=replace(generation,`  return coalesce(v_result,'[]'::jsonb);`,`  update public.invoice_operation_chunks c set payload_json=c.payload_json-'source_original_native_claim'
    where c.id in(select (x->>'chunk_id')::uuid from jsonb_array_elements(p_claims) x)
      and c.payload_json?'source_original_native_claim';
  return coalesce(v_result,'[]'::jsonb);`);
 return generation;
}

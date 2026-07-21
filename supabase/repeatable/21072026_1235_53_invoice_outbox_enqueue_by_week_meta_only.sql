-- CloudTMS reviewed direct replacement; review artifact only, not installed.
-- Exact TEST baseline body MD5 prefix: 1be76651d0c5.
-- Ordinary and non-import-authoritative branches remain on the installed implementation.
CREATE OR REPLACE FUNCTION public.invoice_outbox_enqueue_by_week(p_client_id uuid, p_invoice_week_start date, p_actor_user_id uuid, p_meta jsonb DEFAULT NULL::jsonb)
 RETURNS uuid
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_payload jsonb;
  v_existing uuid;
  v_new uuid;
  v_correction_scope_ids uuid[] := array[]::uuid[];
begin
  if p_client_id is null then
    raise exception 'client_id is required';
  end if;

  if p_invoice_week_start is null then
    raise exception 'invoice_week_start is required';
  end if;

  v_correction_scope_ids:=public._ctms_invoice_week_candidate_ids_v1(
    p_client_id,p_invoice_week_start,100
  );
  if cardinality(v_correction_scope_ids)>0 then
    v_correction_scope_ids:=public._ctms_expand_correction_member_ids_v1(v_correction_scope_ids,100);
    perform public._ctms_assert_correction_invoice_scope_v1(
      v_correction_scope_ids,null::uuid,p_actor_user_id,true,false,false,'INVOICE_OUTBOX_BY_WEEK'
    );
  end if;

  v_payload := jsonb_build_object(
    'client_id', p_client_id::text,
    'invoice_week_start', p_invoice_week_start::text
  );

  if p_actor_user_id is not null then
    v_payload := v_payload || jsonb_build_object('actor_user_id', p_actor_user_id::text);
  end if;

  if p_meta is not null then
    if jsonb_typeof(p_meta) = 'object' then
      v_payload := v_payload || p_meta;
    else
      v_payload := v_payload || jsonb_build_object('meta', p_meta);
    end if;
  end if;

  select o.id
  into v_existing
  from public.invoice_jobs_outbox o
  where o.kind = 'BY_WEEK'
    and (o.payload->>'client_id') = p_client_id::text
    and (o.payload->>'invoice_week_start') = p_invoice_week_start::text
  order by o.created_at desc
  limit 1;

  if v_existing is not null then
    return v_existing;
  end if;

  insert into public.invoice_jobs_outbox(kind, payload)
  values ('BY_WEEK'::text, v_payload)
  returning id into v_new;

  return v_new;
end;
$function$;

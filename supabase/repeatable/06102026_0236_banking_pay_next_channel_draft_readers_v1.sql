-- STAGED ONLY. Root must generate/register the canonical repeatable before adoption.
-- Frozen Draft addresses, not another run, entitlement ledger or channel lifecycle.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_channel_drafts_page_v1(p_run_id uuid)
returns jsonb language plpgsql stable security invoker
set search_path=pg_catalog,private
as $function$
declare
  v_run private.bpay_next_pay_run%rowtype;
  v_rows jsonb;
begin
  if p_run_id is null then
    raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
  end if;
  select r.* into strict v_run from private.bpay_next_pay_run r where r.id=p_run_id;
  if v_run.confirmed_at_utc is null or v_run.selection_state<>'SEALED'
     or v_run.status not in ('DRAFT','EXECUTING','COMPLETE','CANCELLED') then
    raise exception using errcode='55000',message='BPAY_NEXT_CHANNEL_DRAFT_NOT_AVAILABLE';
  end if;
  -- Exactly two indexed existence probes; zero money/cancelled workers remain members.
  select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
    'run_id',p_run_id,'pay_channel',c.pay_channel) order by c.ordinal),'[]'::jsonb)
    into v_rows
  from (values (1,'PAYE'::text),(2,'UMBRELLA'::text)) c(ordinal,pay_channel)
  where exists(select 1 from private.bpay_next_run_worker w
    where w.run_id=p_run_id and w.target_pay_channel=c.pay_channel);
  if pg_catalog.jsonb_array_length(v_rows) not between 1 and 2 then
    raise exception using errcode='55000',message='BPAY_NEXT_CHANNEL_DRAFT_NOT_AVAILABLE';
  end if;
  return pg_catalog.jsonb_build_object('rows',v_rows,'complete',true);
end
$function$;

create or replace function private.bpay_next_channel_worker_page_v1(
  p_run_id uuid,p_pay_channel text,p_after_candidate_id uuid,p_limit integer
) returns jsonb language plpgsql stable security invoker
set search_path=pg_catalog,private
as $function$
declare
  v_query text;v_row record;v_rows jsonb:='[]'::jsonb;
  v_bytes integer:=512;v_count integer:=0;v_more boolean:=false;
begin
  if p_run_id is null or p_pay_channel is null or p_pay_channel not in ('PAYE','UMBRELLA')
     or p_limit is null or p_limit not between 1 and 100 then
    raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
  end if;
  perform private.bpay_next_channel_drafts_page_v1(p_run_id);
  if not exists(select 1 from private.bpay_next_run_worker w
    where w.run_id=p_run_id and w.target_pay_channel=p_pay_channel) then
    raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_SCOPE_INVALID';
  end if;
  -- Equality on run/channel precedes keyset+LIMIT. Two fixed, bound-value query shapes.
  v_query:=$query$select pg_catalog.to_jsonb(page) as row_value from (
    select w.id as run_worker_id,w.candidate_id,w.target_pay_channel as pay_channel,w.status as worker_status,
      w.gross_ex_vat::text as frozen_ex_vat,w.gross_vat::text as frozen_vat,
      w.gross_inc_vat::text as frozen_inc_vat,w.review_issue_code
    from private.bpay_next_run_worker w where w.run_id=$1 and w.target_pay_channel=$2$query$;
  if p_after_candidate_id is not null then
    v_query:=v_query||' and w.candidate_id>$3';
  end if;
  v_query:=v_query||' order by w.candidate_id,w.id limit $4) page';
  for v_row in execute v_query using p_run_id,p_pay_channel,p_after_candidate_id,p_limit loop
    if v_bytes+pg_catalog.octet_length(v_row.row_value::text)+1>120000 then
      if v_count=0 then
        raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_ROW_TOO_LARGE';
      end if;
      v_more:=true;exit;
    end if;
    v_bytes:=v_bytes+pg_catalog.octet_length(v_row.row_value::text)+1;
    v_rows:=v_rows||pg_catalog.jsonb_build_array(v_row.row_value);v_count:=v_count+1;
  end loop;
  -- A full page is conservatively nonterminal; cursor comes from the last EMITTED row.
  return pg_catalog.jsonb_build_object('rows',v_rows,'complete',not(v_more or v_count=p_limit));
end
$function$;

create or replace function private.bpay_next_channel_status_page_v1(
  p_run_id uuid,p_pay_channel text,p_worker_id uuid,p_after_transfer_no integer,p_limit integer
) returns jsonb language plpgsql stable security invoker
set search_path=pg_catalog,private
as $function$
begin
  if p_run_id is null or p_worker_id is null or p_pay_channel is null
     or p_pay_channel not in ('PAYE','UMBRELLA') then
    raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
  end if;
  perform private.bpay_next_channel_drafts_page_v1(p_run_id);
  if not exists(select 1 from private.bpay_next_run_worker w where w.id=p_worker_id
    and w.run_id=p_run_id and w.target_pay_channel=p_pay_channel) then
    raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_SCOPE_INVALID';
  end if;
  return private.bpay_next_worker_status_page_v1(p_run_id,p_worker_id,p_after_transfer_no,p_limit);
end
$function$;

alter function private.bpay_next_channel_drafts_page_v1(uuid) owner to postgres;
alter function private.bpay_next_channel_worker_page_v1(uuid,text,uuid,integer) owner to postgres;
alter function private.bpay_next_channel_status_page_v1(uuid,text,uuid,integer,integer) owner to postgres;
revoke all on function private.bpay_next_channel_drafts_page_v1(uuid),
  private.bpay_next_channel_worker_page_v1(uuid,text,uuid,integer),
  private.bpay_next_channel_status_page_v1(uuid,text,uuid,integer,integer)
from public,anon,authenticated,service_role;
notify pgrst, 'reload schema';
commit;

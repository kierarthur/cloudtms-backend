create or replace function public.tsfin_work_success_bulk(p_ids uuid[])
returns integer
language plpgsql
as $$
declare
  v_count int := 0;
  v_id uuid;
begin
  if p_ids is null then
    return 0;
  end if;

  foreach v_id in array p_ids loop
    begin
      perform public.tsfin_work_success(v_id);
      v_count := v_count + 1;
    exception when others then
      -- swallow (caller already recorded primary success/fail; extras are best-effort)
      null;
    end;
  end loop;

  return v_count;
end;
$$;

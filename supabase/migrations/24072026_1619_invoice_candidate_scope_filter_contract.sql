do $migration$
begin
  if to_regprocedure(
    'public.invoice_batch_generate_candidates(boolean,integer,text[])'
  ) is null then
    raise exception using
      errcode='42883',
      message='INVOICE_CANDIDATE_SCOPE_FILTER_CONTRACT_MISSING';
  end if;

  if to_regprocedure(
    'public.invoice_batch_generate_candidates(boolean,integer)'
  ) is not null then
    raise exception using
      errcode='42725',
      message='INVOICE_CANDIDATE_LEGACY_OVERLOAD_STILL_PRESENT';
  end if;
end;
$migration$;

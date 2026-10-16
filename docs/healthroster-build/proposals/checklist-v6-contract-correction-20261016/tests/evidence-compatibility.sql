-- Root rollback rehearsal only; real owner function, no substitute definition
-- or business fixture. A genuinely unregistered identity must not enter H103.
do $test$
declare unregistered_id uuid:=gen_random_uuid();
begin
 if private.timesheet_contract_move_evidence_source_v1(null::uuid) is not null then
  raise exception 'NULL_INVOICE_SUBJECT_ENTERED_MOVE_RESOLVER';end if;
 while exists(select 1 from private.timesheet_contract_family_aliases a where a.timesheet_id=unregistered_id) loop
  unregistered_id:=gen_random_uuid();
 end loop;
 if private.timesheet_contract_move_evidence_source_v1(unregistered_id) is not null then
  raise exception 'UNREGISTERED_INVOICE_SUBJECT_ENTERED_MOVE_RESOLVER';end if;
end;
$test$;

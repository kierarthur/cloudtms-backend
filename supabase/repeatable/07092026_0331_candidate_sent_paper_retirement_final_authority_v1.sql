-- Final sent-PAPER retirement authority.
--
-- Managed TEST can carry an older installed definition of the private helper
-- even when the main Advanced Expense closure is already recorded in the
-- repeatable ledger.  Keep this deliberately last in chronological release
-- order so an incremental release and a clean replay both finish with the
-- locked cancellation/refusal behaviour.  Sent mail remains immutable audit
-- history; only the current QR/document generation is retired.

\set ON_ERROR_STOP on

begin;

do $candidate_sent_paper_retirement_final_authority$
declare
  v_definition text;
  v_old_guard text:=E'if v_mail.status=''SENT'' and not v_is_historical_replacement_retirement then\n      raise exception ''CANDIDATE_PAPER_OUTBOX_ALREADY_SENT''';
  v_new_guard text:=E'if v_mail.status=''SENT''\n       and not v_is_historical_replacement_retirement\n       and v_reason not in (\n         ''WORKFLOW_CANCELLED'',''WORKFLOW_SUPERSEDED'',''WORKFLOW_AMENDED'',\n         ''OFFICE_REJECTED'',''EXPENSE_CATEGORY_OFFICE_REJECTED''\n       )\n       and v_reason not like ''ROUTE_INTERVENTION_%'' then\n      raise exception ''CANDIDATE_PAPER_OUTBOX_ALREADY_SENT''';
  v_old_update text:=E'and (mail_row.status<>''SENT'' or v_is_historical_replacement_retirement)\n    and mail_row.payment_scope_json->>''candidate_workflow_id''=v_workflow.id::text';
  v_new_update text:=E'and (\n      mail_row.status<>''SENT''\n      or v_is_historical_replacement_retirement\n      or v_reason in (\n        ''WORKFLOW_CANCELLED'',''WORKFLOW_SUPERSEDED'',''WORKFLOW_AMENDED'',\n        ''OFFICE_REJECTED'',''EXPENSE_CATEGORY_OFFICE_REJECTED''\n      )\n      or v_reason like ''ROUTE_INTERVENTION_%''\n    )\n    and mail_row.payment_scope_json->>''candidate_workflow_id''=v_workflow.id::text';
  v_guard_offset integer;
  v_update_offset integer;
begin
  select pg_catalog.pg_get_functiondef(
    'private._candidate_paper_delivery_retire_v1(uuid,integer,text,timestamptz)'::regprocedure
  ) into v_definition;
  v_definition:=pg_catalog.replace(v_definition,E'\r\n',E'\n');
  if pg_catalog.strpos(v_definition,v_new_guard)>0
     and pg_catalog.strpos(v_definition,v_new_update)>0 then
    return;
  end if;
  v_guard_offset:=pg_catalog.strpos(v_definition,v_old_guard);
  v_update_offset:=pg_catalog.strpos(v_definition,v_old_update);
  if v_guard_offset=0 or v_update_offset=0
     or pg_catalog.strpos(
       pg_catalog.substr(v_definition,v_guard_offset+pg_catalog.length(v_old_guard)),v_old_guard
     )>0
     or pg_catalog.strpos(
       pg_catalog.substr(v_definition,v_update_offset+pg_catalog.length(v_old_update)),v_old_update
     )>0 then
    raise exception 'CANDIDATE_SENT_PAPER_RETIREMENT_FINAL_AUTHORITY_DRIFT'
      using errcode='55000';
  end if;
  v_definition:=pg_catalog.replace(v_definition,v_old_guard,v_new_guard);
  v_definition:=pg_catalog.replace(v_definition,v_old_update,v_new_update);
  execute v_definition;
end;
$candidate_sent_paper_retirement_final_authority$;

commit;

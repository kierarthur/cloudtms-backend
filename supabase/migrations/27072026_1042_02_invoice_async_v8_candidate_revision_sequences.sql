-- CloudTMS TEST Invoice Async V8/V2 candidate snapshot revisions.

create sequence if not exists private.invoice_generate_candidate_change_seq
  as bigint
  increment by 1
  minvalue 1
  start with 1
  cache 1;

create sequence if not exists private.invoice_issue_candidate_change_seq
  as bigint
  increment by 1
  minvalue 1
  start with 1
  cache 1;

alter sequence private.invoice_generate_candidate_change_seq owner to postgres;
alter sequence private.invoice_issue_candidate_change_seq owner to postgres;

revoke all on sequence private.invoice_generate_candidate_change_seq
  from public, anon, authenticated;
revoke all on sequence private.invoice_issue_candidate_change_seq
  from public, anon, authenticated;
grant usage, select, update on sequence private.invoice_generate_candidate_change_seq
  to service_role;
grant usage, select, update on sequence private.invoice_issue_candidate_change_seq
  to service_role;

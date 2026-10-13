-- Repeatable CloudTMS function/view authority: banking_pay_next_preparation_wait_owner_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- DR14: one exact current native owner, never a Candidate-directory guess.
create or replace function private.bpay_next_preparation_wait_owner_v1(p_run_id uuid,p_candidate_id uuid)
returns table(waiting_run_id uuid,waiting_actor_display_name text)
language sql stable security invoker set search_path=pg_catalog,private,public
as $function$
  select other_run.id,pg_catalog.left(nullif(pg_catalog.btrim(u.display_name),''),250)
  from private.bpay_next_selection_candidate sc
  join private.bpay_next_pay_run own_run on own_run.id=sc.run_id
    and own_run.selection_state='SEALED' and own_run.status='PREPARING'
    and own_run.confirmed_at_utc is null
  join private.bpay_next_module_control m on m.id=1 and m.active_owner='NEXT'
  join private.bpay_next_worker_control wc on wc.candidate_id=sc.candidate_id
  join private.bpay_next_run_command own_binding on own_binding.run_id=sc.run_id
  left join private.bpay_next_run_worker own_worker on own_worker.run_id=sc.run_id
    and own_worker.candidate_id=sc.candidate_id
  join private.bpay_next_job own_job on own_job.candidate_id=sc.candidate_id
    and own_job.job_kind='PREPARE'
    and (own_job.id=own_worker.active_prepare_job_id or (own_worker.id is null and own_job.command_id=own_binding.command_id))
    and own_job.module_epoch=m.owner_epoch and own_job.status in ('READY','LEASED')
    and not own_job.needs_attention and own_job.retry_count<5
    and (own_job.next_retry_at_utc is null or own_job.next_retry_at_utc<=pg_catalog.statement_timestamp())
  join private.bpay_next_command own_command on own_command.id=own_job.command_id
    and own_command.agency_sequence=own_job.command_sequence and own_command.module_epoch=m.owner_epoch
    and own_command.command_kind='PREPARE' and own_command.status='SEALED'
    and own_command.sealed_at_utc is not null and own_command.enrolled_member_count=own_command.expected_member_count
  join private.bpay_next_job other_job on other_job.candidate_id=sc.candidate_id
    and other_job.owner_epoch=wc.active_owner_epoch and other_job.module_epoch=m.owner_epoch
    and other_job.job_kind='PREPARE' and other_job.status='LEASED'
    and other_job.lease_nonce is not null and other_job.lease_until_utc>pg_catalog.statement_timestamp()
    and other_job.id<>own_job.id
  join private.bpay_next_pay_run other_run on other_run.id=private.bpay_next_prepare_job_run_v1(other_job.id)
    and other_run.id<>sc.run_id and other_run.selection_state='SEALED'
    and other_run.status in ('PREPARING','REVIEW') and other_run.confirmed_at_utc is null
  left join public.tms_users u on u.id=other_run.actor_user_id
  where sc.run_id=p_run_id and sc.candidate_id=p_candidate_id
    and (own_worker.id is null or (own_worker.status='PREPARING' and own_worker.active_prepare_job_id=own_job.id))
    and (own_worker.id is null or private.bpay_next_prepare_job_active_v1(own_job.id))
    and private.bpay_next_prepare_job_active_v1(other_job.id);
$function$;
alter function private.bpay_next_preparation_wait_owner_v1(uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_preparation_wait_owner_v1(uuid,uuid) from public,anon,authenticated,service_role;

notify pgrst,'reload schema';

commit;

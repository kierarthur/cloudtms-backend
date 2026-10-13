-- WB08 narrow three-way successor of installed1246. Replay remains pure.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_source_disposition_seed_v1(
  p_work_id uuid,p_component_key text,p_expected_applied_revision uuid,
  p_seed_effect_id uuid default null,p_seed_hold_id uuid default null)
returns table(qualification_state text,source_book_revision bigint)
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare v_candidate uuid;v_position private.bpay_next_position%rowtype;v_proof record;
begin
  if p_work_id is null or p_component_key is null or p_expected_applied_revision is null then
    raise exception using errcode='22023',message='BPAY_NEXT_SOURCE_BOOK_SEED_ID_REQUIRED';end if;
  perform 1 from private.bpay_next_module_control c where c.id=1 and c.active_owner='NEXT' for share;
  if not found then raise exception using errcode='55000',message='BPAY_NEXT_BINDING_NEXT_REQUIRED';end if;
  select w.candidate_id into strict v_candidate from private.bpay_next_work w where w.id=p_work_id;
  perform 1 from private.bpay_next_worker_control c where c.candidate_id=v_candidate for update;
  if not found then raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_CONTROL_REQUIRED';end if;
  perform 1 from private.bpay_next_work w where w.id=p_work_id for update;
  select p.* into v_position from private.bpay_next_position p where p.work_id=p_work_id and p.component_key=p_component_key for update;
  if not found then qualification_state:='POSITION_PENDING';source_book_revision:=0;return next;return;end if;
  if v_position.source_book_state='READY' then
    if v_position.source_book_seed_applied_revision_id is distinct from p_expected_applied_revision
       or v_position.source_book_seed_effect_id is distinct from p_seed_effect_id
       or v_position.source_book_seed_hold_id is distinct from p_seed_hold_id then
      raise exception using errcode='23514',message='BPAY_NEXT_SOURCE_BOOK_SEED_REPLAY_CONFLICT';end if;
    qualification_state:='READY';source_book_revision:=v_position.source_book_revision;return next;return;
  end if;
  select p.* into strict v_proof from private.bpay_next_source_book_seed_proof_v1(
    p_work_id,p_component_key,p_expected_applied_revision,p_seed_effect_id,p_seed_hold_id) p;
  update private.bpay_next_position p set source_book_state='READY',source_book_revision=1,
    native_realised_paye=case when v_proof.source_channel='PAYE' then v_proof.realised_source else 0 end,
    native_realised_umbrella=case when v_proof.source_channel='UMBRELLA' then v_proof.realised_source else 0 end,
    native_held_paye=case when v_proof.source_channel='PAYE' then v_proof.held_source else 0 end,
    native_held_umbrella=case when v_proof.source_channel='UMBRELLA' then v_proof.held_source else 0 end,
    source_book_seed_kind=v_proof.seed_kind,source_book_seed_applied_revision_id=p_expected_applied_revision,
    source_book_seed_revision_id=v_proof.basis_revision_id,source_book_seed_line_id=v_proof.basis_line_id,
    source_book_seed_effect_id=p_seed_effect_id,source_book_seed_hold_id=p_seed_hold_id,
    source_book_seed_channel=v_proof.source_channel,source_book_seed_digest=v_proof.seed_digest
    where p.work_id=p_work_id and p.component_key=p_component_key;
  perform private.bpay_next_workbench_refresh_work_v1(p_work_id);
  qualification_state:='READY';source_book_revision:=1;return next;
end
$function$;


alter function private.bpay_next_source_disposition_seed_v1(uuid,text,uuid,uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_source_disposition_seed_v1(uuid,text,uuid,uuid,uuid) from public,anon,authenticated,service_role;

notify pgrst, 'reload schema';
commit;

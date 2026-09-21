-- Repeatable CloudTMS authority: weekly_source_correction_cancel_v1
-- The exit from an open Correct-final-source correction session.
--
-- WHY THIS EXISTS.  weekly_final_source_correction_sessions_active_uq is a
-- partial unique index over
--   (source_cycle_id, authority_scope_kind, coalesce(report_scope_id, ...))
-- where state is one of DRAFT, STAGING, READY, REVIEWED, PREPARING, PREPARED,
-- COMMITTING.  DRAFT is inside that list, and before this owner existed nothing
-- in the catalogue ever wrote CANCELLED onto a correction session: eight
-- routines wrote session state and none of them wrote that value.  A session
-- opened and then abandoned therefore held the scope for ever -- the second
-- actor and a re-open were both refused WEEKLY_SOURCE_CORRECTION_DESCENDANT_EXISTS
-- by private.weekly_source_correct_final_preconditions_v1, and one person
-- walking away took that Trust and cutoff out of service with no recovery.
--
-- The index itself is correct: it is the scope lock the pack requires
-- ("Partial unique one active weekly_final_source_correction_sessions row per
-- cycle", 08 KEY-T11).  What was missing was a way to leave the set it
-- watches.  No safety decision in this file is taken by an index, a LIMIT or an
-- ORDER BY; every gate is an explicit test with its own typed refusal.
--
-- WHAT THE PACK SAYS.  05 CFS-015: "Correction session preview succeeds but is
-- abandoned/expired | Inspect | Staged upload and preview remain audit history
-- only; no later ordinary cycle or correction can promote them implicitly."
-- 05 SRC-038 and CFS-018 both speak of "cancelled, expired and reopened
-- Correct-final sessions", so a cancelled session is a state the pack expects
-- to exist.  03 section "weekly_final_source_correction_sessions" names
-- CANCELLED in the required state set.  24 section 18 requires every audited
-- event to retain "actor, reason, decision time".
--
-- WHAT THIS OWNER DELIBERATELY WILL NOT DO.  It never cancels from PREPARING,
-- PREPARED, COMMITTING, APPLIED, CANCELLED or FAILED, and it never touches the
-- prior authority.  A cancel is the absence of a correction, not a correction
-- back: expected_current_final_revision_id stays CURRENT, no movement is
-- voided, no Timesheet, TSFIN, invoice, Draft, payment or Banking Pay row is
-- read for a decision or written.

\set ON_ERROR_STOP on

begin;

create or replace function public.weekly_source_correct_final_cancel_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_expected_keys text[]:=array[
    'actor_user_id','correction_session_id','expected_session_version',
    'idempotency_key','reason','schema_version'
  ];
  v_actual_keys text[];
  v_actor uuid;
  v_session_id uuid;
  v_expected_session_version bigint;
  v_idempotency_key text;
  v_reason text;
  v_request_hash bytea;
  v_session public.weekly_final_source_correction_sessions%rowtype;
  v_locked public.weekly_final_source_correction_sessions%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_scope public.weekly_source_report_scopes%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_prior_revision public.weekly_source_final_revisions%rowtype;
  v_client_id uuid;
  v_manifest_count bigint;
  v_state_before text;
  v_upload record;
  v_publication record;
  v_rejected_uploads integer:=0;
  v_superseded_uploads integer:=0;
  v_staled_publications integer:=0;
  v_live_uploads_before bigint;
  v_live_publications_before bigint;
  v_live_uploads_after bigint;
  v_live_publications_after bigint;
  v_rows integer;
  v_result jsonb;
  v_result_hash bytea;
begin
  perform pg_catalog.set_config('lock_timeout','5s',true);
  if coalesce(current_setting('request.jwt.claim.role',true),
       nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;
  if pg_catalog.jsonb_typeof(p_request)<>'object' then
    raise exception 'WEEKLY_SOURCE_CORRECTION_CANCEL_REQUEST_INVALID' using errcode='22023';
  end if;
  select pg_catalog.array_agg(key order by key) into v_actual_keys
  from pg_catalog.jsonb_object_keys(p_request) key;
  select pg_catalog.array_agg(key order by key) into v_expected_keys
  from pg_catalog.unnest(v_expected_keys) key;
  if v_actual_keys is distinct from v_expected_keys
     or p_request->>'schema_version'<>'WEEKLY_SOURCE_CORRECT_FINAL_CANCEL_V1' then
    raise exception 'WEEKLY_SOURCE_CORRECTION_CANCEL_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_session_id:=(p_request->>'correction_session_id')::uuid;
    v_expected_session_version:=(p_request->>'expected_session_version')::bigint;
  exception when others then
    raise exception 'WEEKLY_SOURCE_CORRECTION_CANCEL_VALUE_INVALID' using errcode='22023';
  end;
  v_idempotency_key:=pg_catalog.btrim(coalesce(p_request->>'idempotency_key',''));
  v_reason:=pg_catalog.btrim(coalesce(p_request->>'reason',''));
  if v_actor is null or v_session_id is null
     or v_expected_session_version is null or v_expected_session_version<1
     or pg_catalog.char_length(v_idempotency_key) not between 1 and 200
     or pg_catalog.char_length(v_reason) not between 1 and 1000 then
    raise exception 'WEEKLY_SOURCE_CORRECTION_CANCEL_VALUE_INVALID' using errcode='22023';
  end if;
  v_request_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_CORRECT_FINAL_CANCEL_V1',p_request-'idempotency_key'
  );

  -- Unlocked pre-read.  Its ONLY uses are (a) to answer an exact idempotent
  -- replay, which writes nothing, and (b) to learn which cycle row to lock
  -- first.  Every value this owner gates on is read again under the locks
  -- below, and the session's own scope identity is re-proved after the lock.
  select * into v_session
  from public.weekly_final_source_correction_sessions where id=v_session_id;
  if not found then
    raise exception 'WEEKLY_SOURCE_CORRECTION_SESSION_NOT_FOUND' using errcode='22023';
  end if;
  if v_session.state='CANCELLED'
     and v_session.result_json->>'cancel_idempotency_key'=v_idempotency_key then
    if v_session.actor_user_id is distinct from v_actor
       or v_session.result_json->>'cancel_request_hash'
            is distinct from pg_catalog.encode(v_request_hash,'hex') then
      raise exception 'WEEKLY_SOURCE_CORRECTION_CANCEL_IDEMPOTENCY_COLLISION'
        using errcode='22023';
    end if;
    return v_session.result_json||pg_catalog.jsonb_build_object('idempotent_replay',true);
  end if;

  -- Lock order, taken BEFORE any state this owner gates on is read.  It is the
  -- order the four installed lifecycle owners already use -- cycle, then report
  -- scope, then correction session, then the revision -- so a cancel racing a
  -- review, a prepare or an apply queues behind the same first lock instead of
  -- deadlocking against it.  WP-54 section 6.2's precondition list named only
  -- the session lock; that is not enough to be deadlock-compatible with the
  -- owners it has to race, and this owner follows the installed order instead.
  select * into v_cycle from public.weekly_source_cycles
  where id=v_session.source_cycle_id for update;
  if not found then
    raise exception 'WEEKLY_SOURCE_CYCLE_NOT_FOUND' using errcode='22023';
  end if;
  select * into strict v_group from public.weekly_source_groups
  where id=v_cycle.source_group_id;
  if v_session.authority_scope_kind='NHSP_REPORT_SCOPE' then
    select * into v_scope from public.weekly_source_report_scopes
    where id=v_session.report_scope_id for update;
    if not found then
      raise exception 'WEEKLY_SOURCE_CORRECTION_SCOPE_INVALID' using errcode='22023';
    end if;
  end if;
  select * into v_locked from public.weekly_final_source_correction_sessions
  where id=v_session_id for update;
  if not found then
    raise exception 'WEEKLY_SOURCE_CORRECTION_SESSION_NOT_FOUND' using errcode='22023';
  end if;
  v_session:=v_locked;

  -- The scope identity the locks were chosen from must still be the session's
  -- own.  If it moved across the lock the locks are the wrong ones and nothing
  -- read after them can be trusted, so fail closed rather than continue.
  if v_session.source_cycle_id is distinct from v_cycle.id
     or v_session.report_scope_id is distinct from v_scope.id
     or (v_session.authority_scope_kind='NHSP_REPORT_SCOPE')
          is distinct from (v_session.report_scope_id is not null) then
    raise exception 'WEEKLY_SOURCE_CORRECTION_SESSION_STALE' using errcode='40001';
  end if;
  if v_session.version<>v_expected_session_version then
    raise exception 'WEEKLY_SOURCE_CORRECTION_SESSION_STALE' using errcode='40001';
  end if;

  -- Authority before disposition.  03 section 21 gives Correct final source the
  -- "same active administrator predicate" with "no invented high-risk
  -- permission", so cancel re-uses the CORRECT_FINAL_SOURCE verb through the
  -- installed evaluator and adds no new operation key.  An actor who fails here
  -- is told only that, and never learns the session's internal state.
  if v_session.authority_scope_kind='NHSP_REPORT_SCOPE' then
    v_client_id:=v_scope.client_id;
  else
    select pg_catalog.count(*) into v_manifest_count
    from public.weekly_source_client_manifests
    where final_revision_id=v_session.expected_current_final_revision_id;
    if v_manifest_count<>1 then
      raise exception 'WEEKLY_SOURCE_CORRECTION_CLIENT_SCOPE_INVALID' using errcode='55000';
    end if;
    select client_id into v_client_id
    from public.weekly_source_client_manifests
    where final_revision_id=v_session.expected_current_final_revision_id;
  end if;
  perform private.weekly_source_office_authority_v1(
    v_actor,'CORRECT_FINAL_SOURCE',v_group.id,v_client_id,v_cycle.finalisation_week_ending
  );

  -- WHO MAY CANCEL -- owner decision D-WP59-1, taken conservatively.
  -- WP-54 section 6.2 offered three candidate policies and recommended the
  -- third (the opening actor always; another holder of the authority only after
  -- a stated idle interval) while refusing to invent the interval, because the
  -- pack does not state one.  It is still not stated anywhere in the pack, so
  -- this owner ships the policy that cannot be wrong: the opening actor only.
  -- That is also the policy every other stage of this lifecycle already
  -- enforces -- review, prepare and apply each require
  -- v_session.actor_user_id = the calling actor.  Widening it to a second
  -- holder of the authority is an owner decision and an additive change to this
  -- one test; it is NOT taken here.
  if v_session.actor_user_id is distinct from v_actor then
    raise exception 'WEEKLY_SOURCE_CORRECTION_CANCEL_ACTOR_NOT_OPENER'
      using errcode='42501',
        detail='Only the actor who opened this correction session may cancel it. '
               'A second holder of CORRECT_FINAL_SOURCE may not, because the idle '
               'interval that would make that claim auditable is not stated in the '
               'pack and has not been ruled.';
  end if;

  -- WHAT THE SESSION HAS ALREADY COMMITTED.  Establish it before releasing
  -- anything, and refuse rather than guess.  These six limbs are exhaustive
  -- over the ten declared states, and the last one is the fail-closed branch
  -- for a state this owner does not recognise.
  v_state_before:=v_session.state;
  if v_state_before='APPLIED' or v_session.applied_final_revision_id is not null then
    raise exception 'WEEKLY_SOURCE_CORRECTION_ALREADY_APPLIED'
      using errcode='55000',
        detail='This correction has already replaced the final source authority. '
               'Cancelling it would claim to undo an applied correction.';
  end if;
  if v_state_before='COMMITTING' then
    raise exception 'WEEKLY_SOURCE_CORRECTION_COMMIT_IN_FLIGHT'
      using errcode='55000',
        detail='A correction transaction is in flight. Cancelling it is how a '
               'half-applied correction is produced; this is a case for the '
               'existing failure path, not for cancel.';
  end if;
  if v_state_before='PREPARING' then
    raise exception 'WEEKLY_SOURCE_CORRECTION_PREPARE_IN_FLIGHT'
      using errcode='55000',
        detail='PREPARE is in flight. Whether a replacement revision has been '
               'materialised cannot be established from this state, so cancel '
               'refuses rather than guess.';
  end if;
  if v_state_before='PREPARED' or v_session.prepared_final_revision_id is not null then
    -- Owner decision D-WP59-2, and a correction to WP-54 section 6.2.  That
    -- specification permits cancel from PREPARING and PREPARED and states that
    -- "then the active_uq index frees the scope and the next correction opens
    -- normally".  Executed, that is false.  PREPARE materialises an inactive
    -- replacement revision whose predecessor_revision_id is the prior CURRENT
    -- revision, and private.weekly_source_correct_final_preconditions_v1
    -- refuses any later correction while a descendant of that prior revision
    -- exists, whatever the descendant's own state and whatever happened to the
    -- session that made it.  Cancelling a PREPARED session would therefore
    -- release the session row and leave the Trust and cutoff blocked anyway --
    -- a cancel that promises a release it cannot deliver.
    -- Releasing it needs the descendant refusal to stop counting a cancelled
    -- session's superseded descendant, and the pack states that refusal without
    -- any such exception (03 section 17: "It refuses ... if a correction
    -- descendant exists"; 05 CFS-008 and CFS-021).  The pack outranks this
    -- package's judgement, so the exception is not invented here.
    raise exception 'WEEKLY_SOURCE_CORRECTION_PREPARED_REVISION_EXISTS'
      using errcode='55000',
        detail='A replacement final revision has already been materialised for '
               'this session. It is a correction descendant of the prior '
               'authority, so cancelling the session would not release the Trust '
               'and cutoff. Releasing a prepared correction is an unruled owner '
               'decision (WP-59 D-WP59-2).';
  end if;
  if v_state_before='CANCELLED' then
    raise exception 'WEEKLY_SOURCE_CORRECTION_ALREADY_CANCELLED'
      using errcode='55000',
        detail='This correction session is already cancelled. Re-send the exact '
               'cancel request with its original idempotency key for the '
               'idempotent replay.';
  end if;
  if v_state_before='FAILED' then
    raise exception 'WEEKLY_SOURCE_CORRECTION_ALREADY_TERMINAL'
      using errcode='55000',
        detail='This correction session is already terminal.';
  end if;
  if v_state_before not in ('DRAFT','STAGING','READY','REVIEWED') then
    raise exception 'WEEKLY_SOURCE_CORRECTION_CANCEL_STATE_UNKNOWN'
      using errcode='55000',detail='state='||v_state_before;
  end if;

  -- THE PRIOR AUTHORITY MUST NOT HAVE MOVED.  A cancel releases a scope on the
  -- understanding that the authority it was opened against is still the live
  -- one.  If it is not, the world changed under this session and the honest
  -- answer is a stale refusal, not a release.
  select * into v_prior_revision from public.weekly_source_final_revisions
  where id=v_session.expected_current_final_revision_id for share;
  if not found
     or v_prior_revision.state<>'CURRENT'
     or v_prior_revision.manifest_hash is distinct from v_session.expected_final_manifest_hash
     or v_prior_revision.source_cycle_id is distinct from v_cycle.id
     or v_prior_revision.authority_scope_kind is distinct from v_session.authority_scope_kind
     or v_prior_revision.report_scope_id is distinct from v_session.report_scope_id
     or coalesce(v_scope.current_final_revision_id,v_cycle.current_final_revision_id)
          is distinct from v_session.expected_current_final_revision_id then
    raise exception 'WEEKLY_SOURCE_CORRECTION_FINAL_REVISION_STALE' using errcode='40001';
  end if;

  -- A cancel must not promise a release it cannot deliver.  If a correction
  -- descendant of the prior authority already exists, the scope stays blocked
  -- by weekly_source_correct_final_preconditions_v1 whatever this owner does,
  -- so say so instead of releasing the session row and leaving the caller to
  -- discover it.  In a session this owner will cancel there is none, because
  -- the only routine that makes one is PREPARE and a prepared session is
  -- refused above.
  if exists(
    select 1 from public.weekly_source_final_revisions descendant
    where descendant.predecessor_revision_id=v_prior_revision.id
  ) then
    raise exception 'WEEKLY_SOURCE_CORRECTION_DESCENDANT_EXISTS'
      using errcode='55000',
        detail='A correction descendant of the prior final revision exists, so '
               'cancelling this session would not release the Trust and cutoff.';
  end if;

  -- The session's staged artefacts.  A session can carry more than one upload
  -- and more than one projection publication: the seal-failure and abort paths
  -- return the session to DRAFT and null the pointer while leaving the rejected
  -- upload row behind, and the rebuild path replaces
  -- replacement_projection_publication_id with a newer publication.  Every row
  -- bound to this session is therefore locked and disposed of, not only the two
  -- the session currently points at.  The ORDER BY exists solely to make lock
  -- acquisition deterministic; no decision below is taken by it.
  for v_upload in
    select id,state from public.weekly_source_uploads
    where correction_session_id=v_session.id
    order by id
    for update
  loop
    if v_upload.state not in ('STAGING','CORRECTION_READY','REJECTED','SUPERSEDED') then
      raise exception 'WEEKLY_SOURCE_CORRECTION_REPLACEMENT_STALE'
        using errcode='40001',
          detail='correction upload '||v_upload.id::text||' is '||v_upload.state
                 ||'; it has been promoted beyond this session and cancel will not '
                 'discard accepted work.';
    end if;
  end loop;
  for v_publication in
    select id,state from public.weekly_source_projection_publications
    where correction_session_id=v_session.id
    order by id
    for update
  loop
    if v_publication.state not in ('BUILDING','CORRECTION_READY','STALE','FAILED') then
      raise exception 'WEEKLY_SOURCE_CORRECTION_REPLACEMENT_STALE'
        using errcode='40001',
          detail='correction publication '||v_publication.id::text||' is '
                 ||v_publication.state||'; it has been promoted beyond this session '
                 'and cancel will not discard accepted work.';
    end if;
  end loop;

  -- A cancel must never SILENTLY succeed.  Removing nothing where something was
  -- expected is a refusal, not a success -- which is exactly the shape of the
  -- defect next door, `public.invoice_remove_nhsp_shifts`, a routine that
  -- returns cleanly and does nothing (WP-54 handoff N3).  Two guards make that
  -- impossible here.
  --
  -- FIRST: a pointer the session still holds must resolve to a row that is
  -- actually bound back to this session.  The disposal loops key on
  -- correction_session_id, so a pointer at a row that is NOT bound would be
  -- left live while the cancel reported success.
  if v_session.replacement_correction_upload_id is not null
     and not exists(
       select 1 from public.weekly_source_uploads bound_upload
       where bound_upload.id=v_session.replacement_correction_upload_id
         and bound_upload.correction_session_id=v_session.id
     ) then
    raise exception 'WEEKLY_SOURCE_CORRECTION_REPLACEMENT_STALE'
      using errcode='40001',
        detail='replacement_correction_upload_id '
               ||v_session.replacement_correction_upload_id::text
               ||' is not bound back to this correction session; cancel will not '
               'report success over work it cannot account for.';
  end if;
  if v_session.replacement_projection_publication_id is not null
     and not exists(
       select 1 from public.weekly_source_projection_publications bound_publication
       where bound_publication.id=v_session.replacement_projection_publication_id
         and bound_publication.correction_session_id=v_session.id
     ) then
    raise exception 'WEEKLY_SOURCE_CORRECTION_REPLACEMENT_STALE'
      using errcode='40001',
        detail='replacement_projection_publication_id '
               ||v_session.replacement_projection_publication_id::text
               ||' is not bound back to this correction session; cancel will not '
               'report success over work it cannot account for.';
  end if;

  -- SECOND: count what is live before the effects, so the effects can be
  -- required to account for exactly that much afterwards.
  select pg_catalog.count(*) into v_live_uploads_before
  from public.weekly_source_uploads
  where correction_session_id=v_session.id
    and state in ('STAGING','CORRECTION_READY');
  select pg_catalog.count(*) into v_live_publications_before
  from public.weekly_source_projection_publications
  where correction_session_id=v_session.id
    and state in ('BUILDING','CORRECTION_READY');

  -- Effects.  One transaction, prior authority untouched.
  --
  -- WP-54 section 6.2 said to "move that upload to REJECTED".  Executed, that
  -- is only possible for an upload that is still STAGING.  The table's
  -- weekly_source_uploads_check3 is
  --   (state in ('SEALED','CURRENT','SUPERSEDED','CORRECTION_READY'))
  --     = (row_manifest_hash is not null)
  -- so moving a sealed CORRECTION_READY upload to REJECTED raises 23514 unless
  -- its row_manifest_hash is erased first -- which would destroy the very
  -- evidence 05 CFS-015 says must remain ("Staged upload and preview remain
  -- audit history only").  A sealed replacement therefore goes to SUPERSEDED,
  -- the state the pack already uses for an upload that stays audit-only and
  -- "cannot be promoted by the correction route" (05 CFS-011).  Both carry the
  -- same typed attempt, so the Blocked/attempt history reads the same either way.
  for v_upload in
    select id from public.weekly_source_uploads
    where correction_session_id=v_session.id
      and state='STAGING'
    order by id
  loop
    update public.weekly_source_uploads set state='REJECTED' where id=v_upload.id;
    get diagnostics v_rows=row_count;
    if v_rows<>1 then
      raise exception 'WEEKLY_SOURCE_CORRECTION_CANCEL_INCOMPLETE'
        using errcode='55000',
          detail='correction upload '||v_upload.id::text||' did not move to REJECTED '
                 '(rows='||v_rows::text||').';
    end if;
    perform private.weekly_source_upload_attempt_append_v1(
      v_upload.id,v_actor,'REJECTED','CORRECTION_SESSION_CANCELLED'
    );
    v_rejected_uploads:=v_rejected_uploads+1;
  end loop;
  for v_upload in
    select id from public.weekly_source_uploads
    where correction_session_id=v_session.id
      and state='CORRECTION_READY'
    order by id
  loop
    update public.weekly_source_uploads set state='SUPERSEDED' where id=v_upload.id;
    get diagnostics v_rows=row_count;
    if v_rows<>1 then
      raise exception 'WEEKLY_SOURCE_CORRECTION_CANCEL_INCOMPLETE'
        using errcode='55000',
          detail='correction upload '||v_upload.id::text||' did not move to SUPERSEDED '
                 '(rows='||v_rows::text||').';
    end if;
    perform private.weekly_source_upload_attempt_append_v1(
      v_upload.id,v_actor,'REJECTED','CORRECTION_SESSION_CANCELLED'
    );
    v_superseded_uploads:=v_superseded_uploads+1;
  end loop;
  for v_publication in
    select id from public.weekly_source_projection_publications
    where correction_session_id=v_session.id
      and state in ('BUILDING','CORRECTION_READY')
    order by id
  loop
    update public.weekly_source_projection_publications
    set state='STALE',failure_code='CORRECTION_SESSION_CANCELLED'
    where id=v_publication.id;
    get diagnostics v_rows=row_count;
    if v_rows<>1 then
      raise exception 'WEEKLY_SOURCE_CORRECTION_CANCEL_INCOMPLETE'
        using errcode='55000',
          detail='correction publication '||v_publication.id::text||' did not move to '
                 'STALE (rows='||v_rows::text||').';
    end if;
    v_staled_publications:=v_staled_publications+1;
  end loop;

  -- The effects must account for exactly what was live before them, and
  -- nothing bound to this session may still be live after them.  Either
  -- mismatch means the cancel did less than it is about to report, and a cancel
  -- that removed nothing where something was expected is a refusal, not a
  -- success.
  if (v_rejected_uploads+v_superseded_uploads)<>v_live_uploads_before
     or v_staled_publications<>v_live_publications_before then
    raise exception 'WEEKLY_SOURCE_CORRECTION_CANCEL_INCOMPLETE'
      using errcode='55000',
        detail='expected to dispose '||v_live_uploads_before::text||' upload(s) and '
               ||v_live_publications_before::text||' publication(s); disposed '
               ||(v_rejected_uploads+v_superseded_uploads)::text||' and '
               ||v_staled_publications::text||'.';
  end if;
  select pg_catalog.count(*) into v_live_uploads_after
  from public.weekly_source_uploads
  where correction_session_id=v_session.id
    and state in ('STAGING','CORRECTION_READY');
  select pg_catalog.count(*) into v_live_publications_after
  from public.weekly_source_projection_publications
  where correction_session_id=v_session.id
    and state in ('BUILDING','CORRECTION_READY');
  if v_live_uploads_after<>0 or v_live_publications_after<>0 then
    raise exception 'WEEKLY_SOURCE_CORRECTION_CANCEL_INCOMPLETE'
      using errcode='55000',
        detail=v_live_uploads_after::text||' upload(s) and '
               ||v_live_publications_after::text||' publication(s) bound to this '
               'correction session are still live after the cancel.';
  end if;

  v_result:=pg_catalog.jsonb_build_object(
    'ok',true,'status','CANCELLED',
    'correction_session_id',v_session.id,
    'cancelled_from_state',v_state_before,
    'version',v_expected_session_version+1,
    'prior_final_revision_id',v_prior_revision.id,
    'rejected_upload_count',v_rejected_uploads,
    'superseded_upload_count',v_superseded_uploads,
    'staled_publication_count',v_staled_publications,
    'cancel_reason',v_reason,
    'cancel_idempotency_key',v_idempotency_key,
    'cancel_request_hash',pg_catalog.encode(v_request_hash,'hex')
  );
  v_result_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_CORRECT_FINAL_CANCEL_RESULT_V1',v_result
  );

  -- state and completed_at_utc move in the SAME statement: the table's own
  -- check ((state in ('APPLIED','CANCELLED','FAILED')) = (completed_at_utc is
  -- not null)) refuses anything else, including a bare UPDATE.
  -- The session's `reason` column is deliberately NOT overwritten: it holds the
  -- reason the correction was opened for, which is audit history.  The cancel's
  -- own reason is carried in result_json and in the audit event, which is what
  -- 24 section 18 asks for ("actor, reason, decision time").
  update public.weekly_final_source_correction_sessions
  set state='CANCELLED',completed_at_utc=pg_catalog.transaction_timestamp(),
      result_json=v_result,result_hash=v_result_hash,
      version=version+1,updated_at_utc=pg_catalog.transaction_timestamp()
  where id=v_session.id and state=v_state_before
    and version=v_expected_session_version;
  if not found then
    raise exception 'WEEKLY_SOURCE_CORRECTION_SESSION_CAS_LOST' using errcode='40001';
  end if;

  insert into public.audit_events(
    ts_utc,actor_user_id,actor_display,actor_role_at_time,object_type,
    object_id_text,action,before_json,after_json,reason
  )
  select pg_catalog.statement_timestamp(),v_actor,actor.display_name,actor.role,
         'weekly_final_source_correction_sessions',v_session.id::text,
         'WEEKLY_SOURCE_CORRECT_FINAL_CANCELLED',
         pg_catalog.jsonb_build_object(
           'state',v_state_before,'version',v_expected_session_version,
           'expected_current_final_revision_id',v_prior_revision.id,
           'replacement_correction_upload_id',v_session.replacement_correction_upload_id,
           'replacement_projection_publication_id',
             v_session.replacement_projection_publication_id
         ),
         pg_catalog.jsonb_build_object(
           'state','CANCELLED','version',v_expected_session_version+1,
           'rejected_upload_count',v_rejected_uploads,
           'superseded_upload_count',v_superseded_uploads,
           'staled_publication_count',v_staled_publications,
           'prior_final_revision_state',v_prior_revision.state,
           'cancel_result_hash',pg_catalog.encode(v_result_hash,'hex')
         ),v_reason
  from public.tms_users actor where actor.id=v_actor;
  if not found then
    raise exception 'WEEKLY_SOURCE_ACTOR_NOT_FOUND' using errcode='55000';
  end if;

  return v_result||pg_catalog.jsonb_build_object('idempotent_replay',false);
end;
$function$;

alter function public.weekly_source_correct_final_cancel_atomic_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_correct_final_cancel_atomic_v1(jsonb)
  from public,anon,authenticated;
grant execute on function public.weekly_source_correct_final_cancel_atomic_v1(jsonb)
  to service_role;

comment on function public.weekly_source_correct_final_cancel_atomic_v1(jsonb) is
  'Cancels an open same-cycle Correct-final-source correction session from DRAFT, STAGING, READY or REVIEWED, releasing the Trust and cutoff held by weekly_final_source_correction_sessions_active_uq. It refuses from PREPARING, PREPARED, COMMITTING, APPLIED, CANCELLED and FAILED, each with its own typed reason, and never touches the prior final source authority, its movements, an invoice, a Timesheet, a TSFIN, a Draft or any Banking Pay row.';

notify pgrst, 'reload schema';

commit;

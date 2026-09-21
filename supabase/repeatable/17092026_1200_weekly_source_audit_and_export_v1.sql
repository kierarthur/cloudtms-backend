-- Repeatable CloudTMS authority: weekly_source_audit_and_export_v1
--
-- Gate 11 (Plan 6.2 contract section 15; gap rows XSG-028 and XSG-029;
-- pack `24 section 18`, `16 section 7`, `04A_MANAGER_EMAIL_POLICY.md`).
--
-- Four things live here and nothing else:
--
--   1. AUDIT.  Plain-English Timesheet Audit events for the Plan 6.2 lifecycle:
--      first authorisation, head publication (immediate and deferred), pending
--      saved, frozen, released, superseded, manual review, and the guard
--      refusals that are RETURNED rather than raised.  Every one of them is
--      written through the EXISTING audit owner `public._audit_insert`; this
--      file creates no audit store, no audit relation and no second audit
--      writer.  Withdrawal (`WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWN`),
--      withdrawal refusal and the Office reopen are already written by their
--      own owners (`17092026_0600_…:2161`, `:1941`, `:1969`, `:1995`;
--      `17092026_0700_…:1298`) and are deliberately NOT re-written here: the
--      chronology reader below renders those rows instead.  Duplicating them
--      would break `UNA-001` and `G5-6`, which count them exactly.
--
--      The events are driven by the database state the real owners produce, not
--      by a call the owners would have to make: each is an AFTER trigger on the
--      Plan 6.2 lifecycle relation the owner writes.  That is why this package
--      needs no edit in any other package's file.
--
--      Every event this file writes is keyed `object_type='timesheets'`,
--      `object_id_text=<canonical root Timesheet id>`, because `24 section 18`
--      is about the TIMESHEET Audit tab.  It is also what keeps WP-08b's
--      `G5-6` audit cardinality assertion (which counts EVERY action on the
--      pending-bundle id) true.
--
--   2. EXPORT.  Four separated hour facts and the invoice movements, so an
--      export can never present one as another (`24 section 18`: "Office and
--      export/report owners must be able to distinguish submitted, source,
--      approved and settled facts").  `Hours paid` comes ONLY from the Gate 9
--      settlement-allocation reader `private.weekly_source_settlement_allocation_v1`
--      (XSG-029).  This file contains no currency-to-hours arithmetic and reads
--      neither `public.timesheet_pay_state.last_settled_pay_batch_id` nor
--      `last_settled_signature`; its verifier asserts both by inspecting the
--      installed definitions.
--
--   3. CANDIDATE PUSH.  An hours-only Candidate push when the approved hours
--      change, made through the EXISTING push boundary
--      `private._candidate_notification_insert_v1`.  The complete serialised
--      payload is scanned before the boundary is called: any forbidden field or
--      any of the words source, protected, exceptional, reconciliation fails
--      closed and nothing is pushed.
--
--   4. NOTIFICATION ROUTE CONTRACT.  A read-only assert that the grouped
--      manager email and its secure response exist only on the source-authority
--      route, and that Office Weekly source notices never enter the Banking
--      alert store.
--
-- Nothing here writes, locks or reads-for-update any Banking Pay, Draft,
-- execution, cancellation, provider, settlement, recovery or remittance
-- relation.  The settlement evidence is read through the Gate 9 reader only.
-- No Workbench session setting is set, read as authority, or pre-seeded.

\set ON_ERROR_STOP on

begin;

-- ===========================================================================
-- 1. Vocabulary and the payload safety scanner
-- ===========================================================================

-- The four words MyTMS must never see (`24 section 17`, `16 section 3.3`,
-- Gate 9 `NAI-MYT-002`).  Held as data so the scanner and its verifier read the
-- same list.
create or replace function private.weekly_source_candidate_forbidden_words_v1()
returns text[]
language sql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  select array['source','protected','exceptional','reconciliation']::text[];
$function$;

-- The field families a Candidate payload may never carry: money, payment and
-- recovery history, and remittance links.  Matching is on the KEY TEXT, at any
-- depth, as a substring, so `total_pay_ex_vat`, `net_amount` and
-- `remittance_url` are all caught without enumerating them.
create or replace function private.weekly_source_candidate_forbidden_key_parts_v1()
returns text[]
language sql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  select array[
    'pay','paid','rate','amount','money','currency','net','gross','vat',
    'charge','margin','invoice','banking','bank','batch','remittance',
    'recovery','advance','settle','settled','settlement','transfer',
    'reservation','residual','headroom','umbrella','payslip','deduction'
  ]::text[];
$function$;

-- Every key and every string scalar of a jsonb value, at any depth, as rows.
-- The scanner below works on THIS, not on the fields anyone expects to be
-- there: a payload that grows a member later is scanned too.
create or replace function private.weekly_source_jsonb_atoms_v1(p_value jsonb)
returns table(atom_kind text, atom_text text)
language plpgsql
immutable
set search_path to 'pg_catalog','private','pg_temp'
as $function$
declare
  v_type text;
  v_key text;
  v_element jsonb;
begin
  if p_value is null then
    return;
  end if;
  v_type:=pg_catalog.jsonb_typeof(p_value);
  if v_type='object' then
    for v_key,v_element in
      select object_row.key,object_row.value
      from pg_catalog.jsonb_each(p_value) as object_row
    loop
      atom_kind:='KEY'; atom_text:=v_key; return next;
      return query select * from private.weekly_source_jsonb_atoms_v1(v_element);
    end loop;
  elsif v_type='array' then
    for v_element in
      select array_row.value from pg_catalog.jsonb_array_elements(p_value) as array_row
    loop
      return query select * from private.weekly_source_jsonb_atoms_v1(v_element);
    end loop;
  elsif v_type='string' then
    atom_kind:='STRING'; atom_text:=p_value #>> '{}'; return next;
  end if;
  return;
end;
$function$;

-- Fail-closed verdict over a COMPLETE serialised Candidate payload.
-- `ok=false` names the first offending atom; the caller then pushes nothing.
create or replace function private.weekly_source_candidate_payload_safe_v1(
  p_payload jsonb
) returns jsonb
language plpgsql
immutable
set search_path to 'pg_catalog','private','pg_temp'
as $function$
declare
  v_words text[]:=private.weekly_source_candidate_forbidden_words_v1();
  v_keys text[]:=private.weekly_source_candidate_forbidden_key_parts_v1();
  v_offence jsonb;
begin
  if p_payload is null or pg_catalog.jsonb_typeof(p_payload)<>'object' then
    return pg_catalog.jsonb_build_object(
      'ok',false,'reason','PAYLOAD_NOT_AN_OBJECT','offending_atom',null);
  end if;

  -- One pass over every atom.  `min` over the offence text makes the answer
  -- deterministic without an ORDER BY … LIMIT: a safety decision must never
  -- rest on sort order or a row cap, and `min` over the WHOLE offending set is
  -- null if and only if the set is empty.
  select pg_catalog.min(offence.description)::jsonb
    into v_offence
  from (
    select pg_catalog.jsonb_build_object(
             'atom_kind',atom.atom_kind,
             'atom_text',atom.atom_text,
             'rule',rule.rule_name)::text as description
    from private.weekly_source_jsonb_atoms_v1(p_payload) as atom
    cross join lateral (
      select 'FORBIDDEN_WORD'::text as rule_name
      where exists (
        select 1 from pg_catalog.unnest(v_words) as word(value)
        where pg_catalog.strpos(pg_catalog.lower(atom.atom_text),word.value)>0)
      union all
      select 'FORBIDDEN_FIELD'::text
      where atom.atom_kind='KEY'
        and exists (
          select 1 from pg_catalog.unnest(v_keys) as key_part(value)
          where pg_catalog.strpos(pg_catalog.lower(atom.atom_text),key_part.value)>0)
    ) as rule
  ) as offence(description);

  if v_offence is not null then
    return pg_catalog.jsonb_build_object(
      'ok',false,'reason',v_offence->>'rule','offending_atom',v_offence);
  end if;
  return pg_catalog.jsonb_build_object('ok',true,'reason',null,'offending_atom',null);
end;
$function$;

-- ===========================================================================
-- 2. Plain English
-- ===========================================================================

-- The old and the new source reference and cycle for one event, taken from the
-- before and after images the owners already record.  `24 section 18`: "Each
-- event retains the source revision, old and new complete schedule, actor,
-- reason, decision time and publication receipt where applicable."
-- WP-14b F10: an entitlement head id is NOT a source reference.  A head is the
-- APPROVED HOURS RECORD for the week; the source reference is the final source
-- revision (or the reference the writing owner stored under that name).  They
-- were coalesced into one member, so a chronology sentence told Office "the
-- source reference is <head id>", naming an object that does not exist in any
-- source report.  The two are now separate members, both always present, so
-- nothing a reader already binds to disappears and neither is ever narrated as
-- the other.
create or replace function private.weekly_source_audit_references_v1(
  p_before jsonb,
  p_after jsonb
) returns jsonb
language sql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  select pg_catalog.jsonb_build_object(
    'old_source_reference',coalesce(
      p_before->>'source_reference',p_before->>'final_revision_id'),
    'new_source_reference',coalesce(
      p_after->>'source_reference',p_after->>'final_revision_id'),
    'old_approved_hours_record',coalesce(
      p_before->>'prior_head_id',p_before->>'head_id',
      p_before->>'current_entitlement_head_id'),
    'new_approved_hours_record',coalesce(
      p_after->>'head_id',p_after->>'current_entitlement_head_id'),
    'old_source_cycle_id',coalesce(
      p_before->>'source_cycle_id',p_before->>'finalisation_cycle_id'),
    'new_source_cycle_id',coalesce(
      p_after->>'source_cycle_id',p_after->>'finalisation_cycle_id'),
    'office_decision',coalesce(
      p_after->>'office_decision',p_after->>'decision',p_after->>'decision_kind'),
    'office_decision_reason',coalesce(
      p_after->>'office_decision_reason',p_after->>'decision_reason',
      p_after->>'manual_review_reason'));
$function$;

-- One plain-English sentence per Weekly Source action code.  Office reads THIS,
-- never the action code and never the raw JSON (`24 section 18`).  An action
-- this file does not know is described as an unexplained Weekly Source event
-- rather than silently dropped, so nothing ever disappears from the chronology.
-- A stored reason is written for operators and sometimes carries a machine
-- detail blob after it (WP-08b's manual-review reason does).  `24 section 18`
-- forbids raw JSON as the primary Office explanation, so the sentence keeps the
-- human half only: everything from the first `detail=` or the first embedded
-- JSON object onward is dropped, and the remainder is capped.  The complete
-- reason is still on the audit row for anyone who wants it.
create or replace function private.weekly_source_audit_human_reason_v1(
  p_reason text
) returns text
language plpgsql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_raw text:=pg_catalog.btrim(coalesce(p_reason,''));
  v_json jsonb;
  v_value text;
begin
  if v_raw='' then
    return null;
  end if;

  -- WP-14b F10, second half.  WP-08b's manual-review reason is a STRUCTURED
  -- value whose `message` member is the operator sentence and whose other
  -- members are machine detail (`…pending_release_review_reason_v1`).  Stripping
  -- from the first `{` threw the sentence away together with the detail, so
  -- Office read "The held decision needs Office review before it can publish."
  -- and was never told WHY, while the reason column held the explanation the
  -- whole time.  The human half is now taken OUT of the structure.  Raw JSON is
  -- still never shown, which is the thing `24 section 18` forbids.
  if pg_catalog.left(v_raw,1)='{' then
    begin
      v_json:=v_raw::jsonb;
    exception when others then
      v_json:=null;
    end;
    if v_json is not null and pg_catalog.jsonb_typeof(v_json)='object' then
      v_value:=pg_catalog.btrim(coalesce(v_json->>'message',''));
      if v_value='' then
        v_value:=pg_catalog.btrim(coalesce(v_json->>'code',''));
      end if;
    end if;
  end if;
  if v_value is null then
    v_value:=v_raw;
  end if;

  -- One common tail strip, whichever shape the reason arrived in.  Everything
  -- from the first `detail=`, the first `{` or the first `[` onward is machine
  -- detail and is dropped.  WP-14b F10, first half: stripping only from `{`
  -- left Office reading a stray "[." at the end of the sentence, and the
  -- message INSIDE a structured reason carries the same tail, so the strip must
  -- run after the extraction and not only instead of it.  `[[{]` is a bracket
  -- expression containing `[` and `{`.
  v_value:=pg_catalog.btrim(pg_catalog.regexp_replace(
    pg_catalog.regexp_replace(
      pg_catalog.split_part(v_value,'detail=',1),
      '[[{].*$',''),
    '[[:space:];,:]+$',''));
  return nullif(pg_catalog.btrim(
    case when pg_catalog.char_length(v_value)>300
      -- A non-ASCII LITERAL is double-encoded when the loading client is not
      -- UTF-8, and the release runner does not guarantee one (WP-11b 1.2).
      then pg_catalog.left(v_value,300)||pg_catalog.chr(8230)
      else v_value end),'');
end;
$function$;

-- ---------------------------------------------------------------------------
-- HANDOVER 2 round-5 ruling A2 / contract decision D13 (WP-14c).
--
-- A guard refusal happens BEFORE any mutation and the refusing transaction
-- writes nothing; that is the money-safety rule and it is not weakened here.
-- Where a DURABLE caller -- a process outside the database that survives the
-- rollback -- receives the refusal, it may record the structured refusal
-- afterwards, in a SEPARATE transaction, carrying the same correlation
-- identity.  The two helpers below are the vocabulary that record uses.  They
-- are held as data so the recorder, the narrative and the verifier all read
-- the same list (the same pattern as the forbidden-word list above).
--
-- The five bases are the ones WP-09b's 34 installed refusal sites emit in the
-- `refusal_basis` member of the refusal DETAIL.  The verifier asserts that no
-- installed site can emit a sixth.
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_guard_refusal_bases_v1()
returns text[]
language sql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  select array[
    'WEEKLY_SOURCE_MANAGED_ROOT',
    'WEEKLY_SOURCE_BOUND_OR_PROTECTED_ROOT_UNRESOLVABLE',
    'BOOKING_REFERENCE_CANONICAL_COLLISION',
    'AUTHORISATION_RECORD_WITHOUT_AUTHORISED_TIMESHEET',
    'PROTECTED_ROOT_AUTHORITY_MISSING'
  ]::text[];
$function$;

-- The plain-English half of a refusal basis.  `24 section 18` forbids showing
-- Office a raw code, so every basis the guard can emit has a sentence here and
-- an unknown one still produces a true, if less specific, sentence rather than
-- printing the token.
create or replace function private.weekly_source_guard_refusal_basis_clause_v1(
  p_basis text
) returns text
language sql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  select case pg_catalog.btrim(coalesce(p_basis,''))
    when 'WEEKLY_SOURCE_MANAGED_ROOT'
      then 'because the week is already authorised for pay.'
    when 'BOOKING_REFERENCE_CANONICAL_COLLISION'
      then 'because two Timesheets share the same booking reference, so the '
        ||'correct family for this week could not be established.'
    when 'WEEKLY_SOURCE_BOUND_OR_PROTECTED_ROOT_UNRESOLVABLE'
      then 'because this week is bound to Weekly source or to protected pay '
        ||'and its family could not be established.'
    when 'AUTHORISATION_RECORD_WITHOUT_AUTHORISED_TIMESHEET'
      then 'because this week has an authorisation record that does not match '
        ||'an authorised Timesheet, which Office must review.'
    when 'PROTECTED_ROOT_AUTHORITY_MISSING'
      then 'because this week carries protected pay evidence but no '
        ||'authorisation record.'
    else 'because a Weekly source check refused it.'
  end;
$function$;

create or replace function private.weekly_source_audit_sentence_v1(
  p_action text,
  p_before jsonb,
  p_after jsonb,
  p_reason text
) returns text
language plpgsql
immutable
set search_path to 'pg_catalog','private','pg_temp'
as $function$
declare
  v_refs jsonb:=private.weekly_source_audit_references_v1(p_before,p_after);
  v_sentence text;
  v_decision text:=v_refs->>'office_decision';
  v_decision_reason text:=v_refs->>'office_decision_reason';
  v_old text:=v_refs->>'old_source_reference';
  v_new text:=v_refs->>'new_source_reference';
  v_old_record text:=v_refs->>'old_approved_hours_record';
  v_new_record text:=v_refs->>'new_approved_hours_record';
  v_old_cycle text:=v_refs->>'old_source_cycle_id';
  v_new_cycle text:=v_refs->>'new_source_cycle_id';
  v_attempt text:=p_after->>'technical_failure_count';
begin
  v_sentence:=case p_action
    when 'WEEKLY_SOURCE_FIRST_AUTHORISATION_RECORDED'
      then 'Office authorised this week for pay for the first time.'
    when 'WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWN'
      then 'Office withdrew the first authorisation, so the week went back to awaiting authorisation.'
    when 'WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWAL_REFUSED'
      then 'Office asked to withdraw the first authorisation and the request was refused.'
    when 'WEEKLY_SOURCE_ENTITLEMENT_PUBLISHED_IMMEDIATELY'
      then 'The approved hours for this week were published straight away.'
    when 'WEEKLY_SOURCE_ENTITLEMENT_PUBLISHED_AFTER_DEFERRAL'
      then 'The approved hours held while payment was frozen were published.'
    when 'WEEKLY_SOURCE_ENTITLEMENT_STAGED'
      then 'A new set of approved hours was prepared for this week.'
    when 'WEEKLY_SOURCE_ENTITLEMENT_SUPERSEDED'
      then 'The previously approved hours for this week were replaced by a later set.'
    when 'WEEKLY_SOURCE_PENDING_ENTITLEMENT_SAVED'
      then 'The decision was saved and is waiting to publish.'
    -- WP-14b F2.  A frozen entitlement and a failed release attempt are
    -- different facts and Office must be able to tell them apart.  FROZEN is
    -- now written ONLY when the freeze census itself said FROZEN on this
    -- attempt; the two sentences below carry the other two real outcomes of the
    -- same PENDING->RELEASING->PENDING transition.
    when 'WEEKLY_SOURCE_ENTITLEMENT_PUBLICATION_FROZEN'
      then 'Publication is held because this week is inside a payment that is frozen.'
    when 'WEEKLY_SOURCE_PENDING_RELEASE_ATTEMPT_STARTED'
      then 'A release attempt for the held decision was started by the release worker.'
    when 'WEEKLY_SOURCE_PENDING_RELEASE_ATTEMPT_FAILED'
      then 'A release attempt for this week did not finish because of a technical '
        ||'failure, so the held decision is still waiting. The approved hours '
        ||'are unchanged'
        ||case when v_attempt is not null
             then ' (attempt '||v_attempt||' of 10).' else '.' end
    when 'WEEKLY_SOURCE_PENDING_RELEASE_ATTEMPT_DEFERRED'
      then 'A release attempt for this week was not made on this pass because '
        ||'another job for this candidate was already running, so the held '
        ||'decision is still waiting. The approved hours are unchanged.'
    when 'WEEKLY_SOURCE_PENDING_ENTITLEMENT_RELEASED'
      then 'The held decision was released and the approved hours were published.'
    when 'WEEKLY_SOURCE_PENDING_ENTITLEMENT_SUPERSEDED'
      then 'The held decision was replaced by a later decision and will not publish.'
    when 'WEEKLY_SOURCE_PENDING_ENTITLEMENT_MANUAL_REVIEW'
      then 'The held decision needs Office review before it can publish.'
    when 'WEEKLY_SOURCE_PENDING_ENTITLEMENT_BUNDLE_REOPENED'
      then 'Office reopened the held decision and asked for another attempt.'
    -- WP-14b F9 and WP-14c (ruling A2).  There are now TWO ways a refusal
    -- reaches this vocabulary and they know different things, so they say
    -- different things.
    --
    --   * CAUGHT_REFUSAL_POST_ROLLBACK -- a durable caller actually made the
    --     request, the guard actually raised, and the caller handed the
    --     refusal back after its transaction had rolled back.  Here a request
    --     WAS made and WAS refused, and the basis the guard itself gave is
    --     rendered in plain English.
    --   * anything else -- the WP-14 recorder, which reads the guard decision
    --     server-side and is not bound to an attempted rotation.  It can only
    --     truthfully say the check was MADE and what it returned (WP-14b F9).
    when 'WEEKLY_SOURCE_ROTATION_REFUSED'
      then case
        when p_after->>'record_source'='CAUGHT_REFUSAL_POST_ROLLBACK'
          then 'A request to replace this Timesheet was refused before anything '
            ||'was changed, '
            ||private.weekly_source_guard_refusal_basis_clause_v1(
                 p_after->>'refusal_basis')
        else 'Office checked whether this Timesheet could be replaced; it could not, '
          ||'because the week is already authorised for pay.'
      end
    when 'WEEKLY_SOURCE_CANDIDATE_HOURS_PUSH_WITHHELD'
      then 'The candidate was not told about the updated hours.'
    when 'WEEKLY_SOURCE_LATER_CHANGE_DECIDED'
      then 'Office made a decision about a later change to this week.'
    when 'WEEKLY_SOURCE_BASE_TIMESHEET_CREATED'
      then 'A Timesheet was created for this week from the source.'
    when 'WEEKLY_SOURCE_TIMESHEET_LINEAGE_CREATED'
      then 'A source row was linked to this week.'
    when 'WEEKLY_SOURCE_CORRECT_FINAL_OPENED'
      then 'Office started correcting the final source for this week.'
    when 'WEEKLY_SOURCE_CORRECT_FINAL_PREPARED'
      then 'A correction to the final source for this week was prepared.'
    when 'WEEKLY_SOURCE_CORRECT_FINAL_REVIEWED'
      then 'A correction to the final source for this week was reviewed.'
    when 'WEEKLY_SOURCE_CORRECT_FINAL_APPLIED'
      then 'A correction to the final source for this week was applied.'
    when 'WEEKLY_SOURCE_INVOICE_ADMITTED'
      then 'This week was admitted to a draft invoice.'
    when 'WEEKLY_SOURCE_PRESENTATION_LINE_MOVED_BETWEEN_DRAFT_INVOICES'
      then 'An invoice line for this week was moved to a different draft invoice.'
    when 'WEEKLY_SOURCE_SYSTEM_HOURS_ACCEPTED'
      then 'The system hours for a queried shift were accepted.'
    when 'WEEKLY_SOURCE_CANDIDATE_ASKED'
      then 'The candidate was asked to check their hours.'
    when 'WEEKLY_SOURCE_MANAGER_SEND_STAGED'
      then 'A grouped review email to the client manager was prepared.'
    when 'WEEKLY_TIMESHEET_SUBMISSION_REQUESTED'
      then 'The candidate was asked to submit a Timesheet for this week.'
    when 'WEEKLY_TIMESHEET_SUBMISSION_COMPLETED'
      then 'The candidate submitted a Timesheet for this week.'
    else null
  end;

  if v_sentence is null then
    v_sentence:='A Weekly source event was recorded for this week ('
      ||coalesce(pg_catalog.btrim(p_action),'unnamed')||').';
  end if;

  if v_old is not null and v_new is not null and v_old is distinct from v_new then
    v_sentence:=v_sentence||' The source reference changed from '||v_old||' to '||v_new||'.';
  elsif v_new is not null then
    v_sentence:=v_sentence||' The source reference is '||v_new||'.';
  elsif v_old is not null then
    v_sentence:=v_sentence||' The source reference was '||v_old||'.';
  end if;

  -- WP-14b F10: the head is narrated as what it is.
  if v_old_record is not null and v_new_record is not null
     and v_old_record is distinct from v_new_record then
    v_sentence:=v_sentence||' The approved hours record changed from '
      ||v_old_record||' to '||v_new_record||'.';
  elsif v_new_record is not null then
    v_sentence:=v_sentence||' The approved hours record is '||v_new_record||'.';
  elsif v_old_record is not null then
    v_sentence:=v_sentence||' The approved hours record was '||v_old_record||'.';
  end if;

  if v_old_cycle is not null and v_new_cycle is not null
     and v_old_cycle is distinct from v_new_cycle then
    v_sentence:=v_sentence||' The source cycle changed from '||v_old_cycle
      ||' to '||v_new_cycle||'.';
  elsif v_new_cycle is not null then
    v_sentence:=v_sentence||' The source cycle is '||v_new_cycle||'.';
  elsif v_old_cycle is not null then
    v_sentence:=v_sentence||' The source cycle was '||v_old_cycle||'.';
  end if;

  if v_decision is not null then
    v_sentence:=v_sentence||' The Office decision was '||v_decision||'.';
  end if;
  v_decision_reason:=coalesce(
    private.weekly_source_audit_human_reason_v1(v_decision_reason),
    case when coalesce(p_reason,'') like 'WEEKLY!_SOURCE!_%' escape '!'
      then null
      else private.weekly_source_audit_human_reason_v1(p_reason) end);
  if v_decision_reason is not null then
    v_sentence:=v_sentence||' The reason given was: '||v_decision_reason;
    if pg_catalog.right(v_decision_reason,1) not in ('.','!','?',pg_catalog.chr(8230)) then
      v_sentence:=v_sentence||'.';
    end if;
  end if;

  -- 24 section 18: raw JSON is never the primary Office explanation.
  if pg_catalog.strpos(v_sentence,'{')>0 or pg_catalog.strpos(v_sentence,'":')>0 then
    v_sentence:=pg_catalog.btrim(
      pg_catalog.regexp_replace(v_sentence,'\{.*$',''));
  end if;

  return v_sentence;
end;
$function$;

-- ===========================================================================
-- 3. Family resolution for a Weekly Source root
--
-- Rotation is resolved through the INSTALLED resolver, which is call-only
-- (contract section 2).  A family that cannot be resolved returns the physical
-- id alone rather than guessing, so a chronology or an export is never widened
-- by a failed resolution.
-- ===========================================================================
create or replace function private.weekly_source_audit_family_v1(
  p_timesheet_id uuid
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_members uuid[];
  v_canonical uuid;
  v_booking text;
begin
  if p_timesheet_id is null then
    return pg_catalog.jsonb_build_object(
      'ok',false,'reason','ROOT_NOT_SUPPLIED',
      'canonical_timesheet_id',null,'family_booking_id',null,
      'member_timesheet_ids','[]'::jsonb);
  end if;

  select pg_catalog.array_agg(distinct scope_row.family_timesheet_id),
         pg_catalog.min(scope_row.canonical_timesheet_id::text)::uuid,
         pg_catalog.min(scope_row.booking_id)
    into v_members,v_canonical,v_booking
  from public._pay_timesheet_rotation_scope(array[p_timesheet_id]) as scope_row;

  if v_members is null or pg_catalog.cardinality(v_members)=0 then
    return pg_catalog.jsonb_build_object(
      'ok',false,'reason','FAMILY_UNRESOLVED',
      'canonical_timesheet_id',p_timesheet_id,'family_booking_id',null,
      'member_timesheet_ids',pg_catalog.to_jsonb(array[p_timesheet_id]));
  end if;

  return pg_catalog.jsonb_build_object(
    'ok',true,'reason',null,
    'canonical_timesheet_id',coalesce(v_canonical,p_timesheet_id),
    'family_booking_id',v_booking,
    'member_timesheet_ids',pg_catalog.to_jsonb(v_members));
end;
$function$;

-- The single audit key for a Weekly Source event: the canonical root Timesheet.
create or replace function private.weekly_source_audit_key_v1(
  p_timesheet_id uuid
) returns uuid
language sql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
  select coalesce(
    (private.weekly_source_audit_family_v1(p_timesheet_id)->>'canonical_timesheet_id')::uuid,
    p_timesheet_id);
$function$;

-- ===========================================================================
-- 4. The event writers
--
-- Every one of them calls `public._audit_insert`, the installed audit owner.
-- None of them writes anything else anywhere.
-- ===========================================================================

-- 4.1 First authorisation.  Fires when the first-authorisation core (I-6)
-- inserts the per-root authorisation record decision D8 put in
-- `public.weekly_source_root_authorisations`.  The withdrawal marks are an
-- UPDATE of that same row and are deliberately not covered here: the withdrawal
-- owner writes `WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWN` itself, and
-- `UNA-001` counts it exactly once.
create or replace function private.weekly_source_audit_first_authorisation_v1()
returns trigger
language plpgsql
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_after jsonb;
begin
  v_after:=pg_catalog.jsonb_build_object(
    'root_authorisation_id',new.id,
    'root_timesheet_id',new.root_timesheet_id,
    'family_booking_id',new.family_booking_id,
    'timesheet_version',new.timesheet_version,
    'authorisation_generation',new.authorisation_generation,
    'authorised_at_utc',new.authorised_at_utc,
    'current_entitlement_head_id',new.current_entitlement_head_id);
  v_after:=v_after||pg_catalog.jsonb_build_object(
    'narrative',private.weekly_source_audit_sentence_v1(
      'WEEKLY_SOURCE_FIRST_AUTHORISATION_RECORDED',null,v_after,null));

  perform public._audit_insert(
    'timesheets',new.root_timesheet_id::text,
    'WEEKLY_SOURCE_FIRST_AUTHORISATION_RECORDED',
    null,v_after,'WEEKLY_SOURCE_FIRST_AUTHORISATION',new.authorised_by_user_id);
  return null;
end;
$function$;

drop trigger if exists weekly_source_audit_first_authorisation
  on public.weekly_source_root_authorisations;
create trigger weekly_source_audit_first_authorisation
after insert on public.weekly_source_root_authorisations
for each row execute function private.weekly_source_audit_first_authorisation_v1();

-- 4.2 Head publication, immediate and deferred, and supersession.
--
-- A head reaches `COMMITTED_CURRENT` in exactly two ways: the coordinator
-- publishes it in the Office transaction (IMMEDIATE), or the release worker
-- publishes it out of a saved pending bundle (DEFERRED).  The distinction is
-- read from the state of the pending bundle for the same decision bundle, which
-- is a fact in the database rather than a flag a caller supplies.
create or replace function private.weekly_source_audit_entitlement_head_v1()
returns trigger
language plpgsql
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_deferred boolean;
  v_action text;
  v_before jsonb;
  v_after jsonb;
  v_became_current boolean;
  v_became_superseded boolean;
  v_actor uuid;
  v_superseding_decider uuid;
  v_performed_by text;
begin
  v_became_current:=new.state='COMMITTED_CURRENT'
    and (tg_op='INSERT' or old.state is distinct from 'COMMITTED_CURRENT');
  v_became_superseded:=new.state='SUPERSEDED'
    and (tg_op='INSERT' or old.state is distinct from 'SUPERSEDED');

  if not v_became_current and not v_became_superseded
     and not (tg_op='INSERT' and new.state='STAGED') then
    return null;
  end if;

  v_before:=case when tg_op='UPDATE' then pg_catalog.jsonb_build_object(
      'head_id',old.id,'state',old.state,
      'head_revision',old.head_revision,
      'prior_head_id',old.prior_head_id,
      'component_count',old.component_count,
      'certified_zero',old.certified_zero)
    else null end;

  if v_became_current then
    -- Immediate or deferred is a FACT about the decision, not a caller's flag:
    -- a pending bundle exists for a decision bundle if and only if the freeze
    -- census refused an immediate publication and the decision was parked
    -- (`24 section 4.4`; `proof/32 section 8`).  The test is therefore the
    -- EXISTENCE of the bundle, not its state: the release owner commits the
    -- head through the same coordinator BEFORE it marks the bundle RELEASED,
    -- so a state test would label a genuinely deferred publication immediate.
    -- `exists` is already two-valued, so no null can read as "not deferred".
    v_deferred:=exists(
      select 1 from public.weekly_source_pending_entitlement_bundles bundle
      where bundle.decision_bundle_id=new.decision_bundle_id
        and bundle.bundle_revision=new.bundle_revision);
    v_action:=case when v_deferred
      then 'WEEKLY_SOURCE_ENTITLEMENT_PUBLISHED_AFTER_DEFERRAL'
      else 'WEEKLY_SOURCE_ENTITLEMENT_PUBLISHED_IMMEDIATELY' end;
  elsif v_became_superseded then
    v_action:='WEEKLY_SOURCE_ENTITLEMENT_SUPERSEDED';
  else
    v_action:='WEEKLY_SOURCE_ENTITLEMENT_STAGED';
  end if;

  v_after:=pg_catalog.jsonb_build_object(
    'head_id',new.id,'state',new.state,
    'authority_kind',new.authority_kind,
    'head_revision',new.head_revision,
    'prior_head_id',new.prior_head_id,
    'superseded_by_head_id',new.superseded_by_head_id,
    'component_count',new.component_count,
    'certified_zero',new.certified_zero,
    'root_timesheet_id',new.root_timesheet_id,
    'root_family_booking_id',new.root_family_booking_id,
    'root_timesheet_version',new.root_timesheet_version,
    'week_ending_date',new.week_ending_date,
    'decision_bundle_id',new.decision_bundle_id,
    'bundle_revision',new.bundle_revision,
    'decision_id',new.decision_id,
    'publication_mode',case
      when v_action='WEEKLY_SOURCE_ENTITLEMENT_PUBLISHED_AFTER_DEFERRAL' then 'DEFERRED'
      when v_action='WEEKLY_SOURCE_ENTITLEMENT_PUBLISHED_IMMEDIATELY' then 'IMMEDIATE'
      else null end,
    'publication_receipt_digest',case when new.publication_receipt_digest is not null
      then pg_catalog.encode(new.publication_receipt_digest,'hex') else null end,
    'committed_at_utc',new.committed_at_utc,
    'superseded_at_utc',new.superseded_at_utc);

  -- WP-14b F5.  Who really performed this transition:
  --   * STAGED and PUBLISHED_IMMEDIATELY happen inside the Office transaction,
  --     so the decision's owner IS the actor;
  --   * PUBLISHED_AFTER_DEFERRAL is performed by the release worker out of a
  --     saved bundle, long after the Office user went home, so it carries no
  --     actor and names the worker instead;
  --   * SUPERSEDED is caused by the head that REPLACED this one, so the actor
  --     is that head's decider, not this head's.  Recording the old head's
  --     decider made the audit name the wrong person on every supersession.
  -- `decided_by_user_id` stays on every row either way.
  if v_action='WEEKLY_SOURCE_ENTITLEMENT_SUPERSEDED' then
    select superseding.decided_by_user_id into v_superseding_decider
    from public.weekly_source_entitlement_heads superseding
    where superseding.id=new.superseded_by_head_id;
    v_actor:=v_superseding_decider;
    v_performed_by:=case when v_superseding_decider is null
      then 'WEEKLY_SOURCE_PUBLICATION_OWNER' else 'OFFICE_USER' end;
  elsif v_action='WEEKLY_SOURCE_ENTITLEMENT_PUBLISHED_AFTER_DEFERRAL' then
    v_actor:=null;
    v_performed_by:='WEEKLY_SOURCE_RELEASE_WORKER';
  else
    v_actor:=new.decided_by_user_id;
    v_performed_by:='OFFICE_USER';
  end if;

  v_after:=v_after||pg_catalog.jsonb_build_object(
    'decided_by_user_id',new.decided_by_user_id,
    'superseded_by_decided_by_user_id',v_superseding_decider,
    'performed_by',v_performed_by);
  v_after:=v_after||pg_catalog.jsonb_build_object(
    'narrative',private.weekly_source_audit_sentence_v1(
      v_action,v_before,v_after,null));

  perform public._audit_insert(
    'timesheets',new.root_timesheet_id::text,v_action,
    v_before,v_after,'WEEKLY_SOURCE_ENTITLEMENT_PUBLICATION',v_actor);
  return null;
end;
$function$;

drop trigger if exists weekly_source_audit_entitlement_head
  on public.weekly_source_entitlement_heads;
create trigger weekly_source_audit_entitlement_head
after insert or update on public.weekly_source_entitlement_heads
for each row execute function private.weekly_source_audit_entitlement_head_v1();

-- 4.3 Pending saved, frozen, released, superseded and manual review.
--
-- MANUAL_REVIEW to PENDING is the audited Office reopen, which WP-08b's owner
-- writes itself (`G5-6` counts it exactly once); it is skipped here.
create or replace function private.weekly_source_audit_pending_bundle_v1()
returns trigger
language plpgsql
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_root uuid;
  v_actions text[]:=array[]::text[];
  v_action text;
  v_before jsonb;
  v_after jsonb;
  v_census_state text;
  v_returned_to_pending boolean;
  v_failures_rose boolean;
  v_census_refreshed boolean;
  v_actor uuid;
begin
  -- WP-14b F8: the census and the save owner store the verdict under `result`.
  -- Reading `census_result` made this member null on every event in
  -- production, so the "frozen fact with the census result as its evidence"
  -- carried no evidence at all.  `census_result` stays the AUDIT member name
  -- (it is what the row means); only the key read from the census changes.
  v_census_state:=new.last_census_json->>'result';

  if tg_op='INSERT' then
    -- A bundle exists at all only because the freeze census refused an
    -- immediate publication (`24 section 4.4`), and the save owner REFUSES
    -- unless the census it was handed says FROZEN.  The frozen fact is
    -- therefore recorded alongside the save, with the census result as its
    -- evidence - and only when that evidence actually says FROZEN.
    --
    -- Every literal is explicitly cast: `text[] || <untyped literal>` is
    -- ambiguous and raises at run time, which is the trap WP-08b found in the
    -- coordinator (handoff N4).
    v_actions:=array['WEEKLY_SOURCE_PENDING_ENTITLEMENT_SAVED'::text];
    if v_census_state='FROZEN' then
      v_actions:=v_actions||array['WEEKLY_SOURCE_ENTITLEMENT_PUBLICATION_FROZEN'::text];
    end if;
  else
    -- WP-14b F2.  `PENDING -> RELEASING -> PENDING` with a moved
    -- `pending_revision` is made by the REAL release owner for three different
    -- reasons, and only one of them is a frozen payment:
    --
    --   * a technical failure           -> `technical_failure_count` rises
    --                                      (`…release_v1` technical-failure
    --                                      owner is the only writer that
    --                                      increments it);
    --   * the census said FROZEN        -> `last_census_json` is REWRITTEN on
    --                                      this attempt and says `FROZEN`;
    --   * the serial gate said BUSY     -> neither the counter nor the census
    --                                      is touched, because the attempt
    --                                      never reached the census.
    --
    -- Auditing all three as "payment frozen" told Office a false reason for ten
    -- of twenty-three events on the reviewer's run, and then contradicted the
    -- manual-review row that followed them.  The census test requires the value
    -- to have CHANGED, so a stale FROZEN census left on the row by an earlier
    -- attempt can never make a busy skip look frozen.
    v_returned_to_pending:=new.state='PENDING' and old.state='RELEASING'
      and new.pending_revision>old.pending_revision;
    v_failures_rose:=coalesce(new.technical_failure_count,0)
                     >coalesce(old.technical_failure_count,0);
    v_census_refreshed:=new.last_census_json is distinct from old.last_census_json;

    if new.state='RELEASING'
       and (old.state is distinct from 'RELEASING'
            or new.pending_revision>old.pending_revision) then
      -- WP-14b, orchestrator decision on the silent transitions.  The lease
      -- claim `PENDING -> RELEASING` wrote nothing, and neither did a RE-claim
      -- after a lease expired.  That made one case invisible that matters: a
      -- worker that claims the bundle and then dies reports NO outcome at all,
      -- so the chronology showed a held decision simply sitting there.  The
      -- claim is recorded from the state change the claim owner already makes;
      -- the owner itself is untouched.
      v_actions:=array['WEEKLY_SOURCE_PENDING_RELEASE_ATTEMPT_STARTED'::text];
    elsif v_returned_to_pending then
      if v_failures_rose then
        v_actions:=array['WEEKLY_SOURCE_PENDING_RELEASE_ATTEMPT_FAILED'::text];
      elsif v_census_refreshed and v_census_state='FROZEN' then
        v_actions:=array['WEEKLY_SOURCE_ENTITLEMENT_PUBLICATION_FROZEN'::text];
      else
        -- The serial-gate BUSY return, and any other landing that neither
        -- failed nor re-proved the freeze.  This is the record for the
        -- otherwise silent BUSY transition (WP-14b, orchestrator decision on
        -- the silent transitions): it is written from the state change the
        -- owner already makes, so no other owner's behaviour changes.
        v_actions:=array['WEEKLY_SOURCE_PENDING_RELEASE_ATTEMPT_DEFERRED'::text];
      end if;
    elsif new.state='RELEASED' and old.state is distinct from 'RELEASED' then
      v_actions:=array['WEEKLY_SOURCE_PENDING_ENTITLEMENT_RELEASED'::text];
    elsif new.state='SUPERSEDED' and old.state is distinct from 'SUPERSEDED' then
      v_actions:=array['WEEKLY_SOURCE_PENDING_ENTITLEMENT_SUPERSEDED'::text];
    elsif new.state='MANUAL_REVIEW' and old.state is distinct from 'MANUAL_REVIEW' then
      -- The tenth consecutive technical failure is BOTH a failed attempt and
      -- the move to manual review.  Recording both keeps the attempt count in
      -- the chronology equal to the count on the row.
      if v_failures_rose then
        v_actions:=array['WEEKLY_SOURCE_PENDING_RELEASE_ATTEMPT_FAILED'::text,
                         'WEEKLY_SOURCE_PENDING_ENTITLEMENT_MANUAL_REVIEW'::text];
      else
        v_actions:=array['WEEKLY_SOURCE_PENDING_ENTITLEMENT_MANUAL_REVIEW'::text];
      end if;
    else
      return null;
    end if;
  end if;

  v_before:=case when tg_op='UPDATE' then pg_catalog.jsonb_build_object(
      'pending_bundle_id',old.id,'state',old.state,
      'pending_revision',old.pending_revision,
      'technical_failure_count',old.technical_failure_count,
      'manual_review_reason',old.manual_review_reason)
    else null end;

  -- A cross-Contract atomic amendment carries more than one root
  -- (`24 section 4.5`); every one of them gets the event on its own Timesheet
  -- Audit, so no root's chronology is silently short of an event.
  foreach v_action in array v_actions loop
  -- WP-14b F5.  Only the SAVE is the Office user's act.  The release, the
  -- freeze re-proof, a failed or deferred attempt, a supersession and the move
  -- to manual review are performed by the release worker or by the census, and
  -- attributing them to the person who took the decision made the audit say an
  -- administrator released a held publication when a background worker did.
  -- Those events carry NO actor; who really performed them - the lease owner
  -- and the worker id the owner recorded - is in `after_json`, and the
  -- decision's owner stays there too as `decided_by_user_id`.
  v_actor:=case when v_action='WEEKLY_SOURCE_PENDING_ENTITLEMENT_SAVED'
    then new.decided_by_user_id else null end;
  foreach v_root in array new.member_root_ids loop
    v_after:=pg_catalog.jsonb_build_object(
      'pending_bundle_id',new.id,'state',new.state,
      'pending_revision',new.pending_revision,
      'decision_bundle_id',new.decision_bundle_id,
      'bundle_revision',new.bundle_revision,
      'decision_id',new.decision_id,
      'candidate_id',new.candidate_id,
      'member_root_ids',pg_catalog.to_jsonb(new.member_root_ids),
      'member_family_booking_ids',pg_catalog.to_jsonb(new.member_family_booking_ids),
      'technical_failure_count',new.technical_failure_count,
      'manual_review_reason',new.manual_review_reason,
      'census_result',v_census_state,
      'census_reason',new.last_census_json->>'reason',
      'census_evaluated_at_utc',new.last_census_json->>'evaluated_at_utc',
      'decided_by_user_id',new.decided_by_user_id,
      'performed_by',case
        when v_action='WEEKLY_SOURCE_PENDING_ENTITLEMENT_SAVED' then 'OFFICE_USER'
        else 'WEEKLY_SOURCE_RELEASE_WORKER' end,
      'released_by_worker_id',new.released_by_worker_id,
      'lease_owner',new.lease_owner,
      'released_at_utc',new.released_at_utc,
      'released_receipt_id',new.released_receipt_id,
      'publication_mode',case when v_action='WEEKLY_SOURCE_PENDING_ENTITLEMENT_RELEASED'
        then 'DEFERRED' else null end);
    v_after:=v_after||pg_catalog.jsonb_build_object(
      'narrative',private.weekly_source_audit_sentence_v1(
        v_action,v_before,v_after,new.manual_review_reason));

    perform public._audit_insert(
      'timesheets',v_root::text,v_action,v_before,v_after,
      'WEEKLY_SOURCE_PENDING_ENTITLEMENT',v_actor);
  end loop;
  end loop;
  return null;
end;
$function$;

drop trigger if exists weekly_source_audit_pending_bundle
  on public.weekly_source_pending_entitlement_bundles;
create trigger weekly_source_audit_pending_bundle
after insert or update on public.weekly_source_pending_entitlement_bundles
for each row execute function private.weekly_source_audit_pending_bundle_v1();

-- 4.4 Guard refusals.
--
-- The managed-root rotation guard REFUSES BY RAISING (`55000
-- WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED`), which rolls the whole
-- transaction back; an audit row written inside it would be rolled back with
-- it, so the refusal cannot be audited from inside the refusing transaction and
-- this file does not pretend otherwise.  What CAN be audited, and is, is a
-- refusal decision a caller obtained WITHOUT raising, through WP-03's read-only
-- decision shim.  The recorder is service-only, takes no actor or timestamp
-- from the caller beyond the acting user, and writes exactly one audit row.
create or replace function private.weekly_source_audit_guard_refusal_v1(
  p_timesheet_id uuid,
  p_decision jsonb,
  p_entry_point text,
  p_actor_user_id uuid
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_root uuid;
  v_after jsonb;
begin
  if p_timesheet_id is null or p_decision is null
     or pg_catalog.jsonb_typeof(p_decision)<>'object' then
    raise exception 'WEEKLY_SOURCE_AUDIT_GUARD_REFUSAL_REQUEST_INVALID' using errcode='22023';
  end if;
  if coalesce((p_decision->>'managed')::boolean,false) is not true then
    return pg_catalog.jsonb_build_object('ok',true,'recorded',false,
      'reason','NOT_A_REFUSAL');
  end if;

  v_root:=private.weekly_source_audit_key_v1(p_timesheet_id);
  v_after:=pg_catalog.jsonb_build_object(
    'requested_timesheet_id',p_timesheet_id,
    'canonical_timesheet_id',v_root,
    'entry_point',nullif(pg_catalog.btrim(coalesce(p_entry_point,'')),''),
    'refusal_code',p_decision->>'refusal_code',
    'reason',p_decision->>'reason');
  v_after:=v_after||pg_catalog.jsonb_build_object(
    'narrative',private.weekly_source_audit_sentence_v1(
      'WEEKLY_SOURCE_ROTATION_REFUSED',null,v_after,p_decision->>'reason'));

  perform public._audit_insert(
    'timesheets',v_root::text,'WEEKLY_SOURCE_ROTATION_REFUSED',
    null,v_after,'WEEKLY_SOURCE_MANAGED_ROOT_GUARD',p_actor_user_id);
  return pg_catalog.jsonb_build_object('ok',true,'recorded',true,
    'canonical_timesheet_id',v_root);
end;
$function$;

create or replace function public.weekly_source_audit_guard_refusal_record_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_timesheet uuid;
  v_actor uuid;
  v_entry text;
  v_decision jsonb;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
               where key not in ('timesheet_id','entry_point','actor_user_id')) then
    raise exception 'WEEKLY_SOURCE_AUDIT_GUARD_REFUSAL_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_timesheet:=(p_request->>'timesheet_id')::uuid;
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_entry:=p_request->>'entry_point';
  exception when others then
    raise exception 'WEEKLY_SOURCE_AUDIT_GUARD_REFUSAL_REQUEST_INVALID' using errcode='22023';
  end;
  -- The decision is read server-side from WP-03's read-only shim.  A caller may
  -- not supply a refusal.
  v_decision:=private.weekly_source_managed_root_guard_decision_v1(v_timesheet);
  return private.weekly_source_audit_guard_refusal_v1(
    v_timesheet,v_decision,v_entry,v_actor);
end;
$function$;


-- 4.5 The POST-ROLLBACK record of a guard refusal that RAISED.
--
-- HANDOVER 2 round-5 ruling A2, contract decision D13:
--
--   "A guard refusal must occur before any mutation.  Do not insert an audit
--    row in the transaction and then pretend the rolled-back row is durable.
--    Where a caller receives the refusal, it may record the structured refusal
--    in a separate post-rollback transaction using the same correlation
--    identity.  Where no durable caller exists, return the structured refusal
--    to the Office screen and retain ordinary operational logs; do not weaken
--    the zero-write proof merely to manufacture an audit row."
--
-- Nothing in this section is reachable from inside a refused transaction, and
-- nothing in it is called by any guarded owner.  It is called by the DURABLE
-- CALLER -- the process outside the database that issued the refused request
-- and is still running after PostgreSQL rolled that transaction back -- on a
-- NEW transaction, with the refusal exactly as it caught it.
--
-- Three properties make this safe rather than merely later:
--
--   1. The refusal's STRUCTURE is taken from what the guard already returned
--      (its SQLSTATE, its message and the DETAIL object WP-09b's sites build).
--      It is never re-derived from the current database state, so the record
--      says what actually happened and not what would happen now.
--   2. The record is refused unless this transaction has written nothing at
--      all.  That is read from the server twice over: no transaction id has
--      been assigned (any tuple write assigns one, in a subtransaction too),
--      and pg_stat_xact_user_tables -- the same counter the zero-write
--      measurement reads -- reports zero tuples written.  A caller that tried
--      to bolt the record onto the transaction that made the attempt, or onto
--      a savepoint inside it, is refused instead of obeyed.
--   3. The correlation identity the durable caller carries is stored on the
--      record, so the refusal can be tied to the attempt it came from.  It is
--      required; a record without one is refused.
--
-- What this deliberately does NOT do: it does not let a caller assert a
-- refusal that no guard made.  The supplied refusal must carry SQLSTATE
-- 55000, the exact guard message, an entry point that is present in an
-- INSTALLED routine which raises that message, and one of the installed
-- refusal bases.  The current guard decision is read server-side as well and
-- stored as corroboration, so a record made against a root that no longer
-- refuses is visible as such rather than silently equal to one that does.  A
-- caller that fabricates a well-formed refusal for a root that really is
-- refusing cannot be detected from inside the database; that residual is
-- stated in the package report rather than papered over.
create or replace function private.weekly_source_guard_refusal_entry_point_installed_v1(
  p_entry_point text
) returns boolean
language sql
stable
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
  select nullif(pg_catalog.btrim(coalesce(p_entry_point,'')),'') is not null
     and exists(
       select 1
       from pg_catalog.pg_proc installed
       where pg_catalog.strpos(
               installed.prosrc,'WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED')>0
         and pg_catalog.strpos(
               installed.prosrc,
               pg_catalog.format('%L,%L','entry_point',p_entry_point))>0);
$function$;

create or replace function private.weekly_source_guard_refusal_detail_v1(
  p_refusal jsonb
) returns jsonb
language plpgsql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_detail jsonb;
begin
  if p_refusal is null or pg_catalog.jsonb_typeof(p_refusal)<>'object' then
    return null;
  end if;
  v_detail:=p_refusal->'detail';
  if v_detail is null then
    return null;
  end if;
  -- PostgreSQL delivers DETAIL to a client as TEXT, so a durable caller that
  -- passes the caught error through verbatim sends a JSON string here and one
  -- that parses it first sends an object.  Both are accepted; nothing else is.
  if pg_catalog.jsonb_typeof(v_detail)='string' then
    begin
      v_detail:=(p_refusal->>'detail')::jsonb;
    exception when others then
      return null;
    end;
  end if;
  if pg_catalog.jsonb_typeof(coalesce(v_detail,'null'::jsonb))<>'object' then
    return null;
  end if;
  return v_detail;
end;
$function$;

create or replace function private.weekly_source_guard_refusal_record_v1(
  p_correlation_id text,
  p_refusal jsonb,
  p_caller text,
  p_actor_user_id uuid
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_detail jsonb;
  v_entry text;
  v_basis text;
  v_timesheet uuid;
  v_root uuid;
  v_decision jsonb;
  v_corroboration text;
  v_after jsonb;
begin
  if p_refusal is null or pg_catalog.jsonb_typeof(p_refusal)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_refusal) key
               where key not in ('sqlstate','message','detail')) then
    raise exception 'WEEKLY_SOURCE_GUARD_REFUSAL_RECORD_REQUEST_INVALID'
      using errcode='22023';
  end if;

  v_detail:=private.weekly_source_guard_refusal_detail_v1(p_refusal);
  -- The refusal must be the guard's own refusal, not a caller's description of
  -- one.  Absent, null and non-text all take the unsafe value here.
  if nullif(pg_catalog.btrim(coalesce(p_refusal->>'sqlstate','')),'')
       is distinct from '55000'
     or nullif(pg_catalog.btrim(coalesce(p_refusal->>'message','')),'')
       is distinct from 'WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED'
     or v_detail is null
     or nullif(pg_catalog.btrim(coalesce(v_detail->>'code','')),'')
       is distinct from 'WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED' then
    raise exception 'WEEKLY_SOURCE_GUARD_REFUSAL_NOT_A_GUARD_REFUSAL'
      using errcode='22023';
  end if;

  v_entry:=nullif(pg_catalog.btrim(coalesce(v_detail->>'entry_point','')),'');
  if not private.weekly_source_guard_refusal_entry_point_installed_v1(v_entry) then
    raise exception 'WEEKLY_SOURCE_GUARD_REFUSAL_ENTRY_POINT_UNKNOWN'
      using errcode='22023';
  end if;

  v_basis:=nullif(pg_catalog.btrim(coalesce(v_detail->>'refusal_basis','')),'');
  if v_basis is null
     or not (v_basis = any(private.weekly_source_guard_refusal_bases_v1())) then
    raise exception 'WEEKLY_SOURCE_GUARD_REFUSAL_BASIS_UNKNOWN'
      using errcode='22023';
  end if;

  begin
    v_timesheet:=nullif(pg_catalog.btrim(coalesce(v_detail->>'timesheet_id','')),'')::uuid;
  exception when others then
    v_timesheet:=null;
  end;
  if v_timesheet is null then
    raise exception 'WEEKLY_SOURCE_GUARD_REFUSAL_TIMESHEET_REQUIRED'
      using errcode='22023';
  end if;

  if nullif(pg_catalog.btrim(coalesce(p_correlation_id,'')),'') is null
     or pg_catalog.char_length(p_correlation_id)>200 then
    raise exception 'WEEKLY_SOURCE_GUARD_REFUSAL_CORRELATION_REQUIRED'
      using errcode='22023';
  end if;

  -- Read server-side, stored, never used to decide whether to record: the
  -- caller's caught refusal is the evidence, this is only its corroboration.
  v_decision:=private.weekly_source_managed_root_guard_decision_v1(v_timesheet);
  v_corroboration:=case
    when v_decision is null or pg_catalog.jsonb_typeof(v_decision)<>'object'
      then 'UNAVAILABLE'
    when coalesce((v_decision->>'managed')::boolean,false)
      or coalesce((v_decision->>'authorisation_record_without_authorised_timesheet')::boolean,false)
      or (v_decision->>'protected_target_ownership_state') is not null
      then 'STILL_REFUSES'
    else 'NO_LONGER_REFUSES'
  end;

  v_root:=private.weekly_source_audit_key_v1(v_timesheet);
  v_after:=pg_catalog.jsonb_build_object(
    'record_source','CAUGHT_REFUSAL_POST_ROLLBACK',
    'correlation_id',pg_catalog.btrim(p_correlation_id),
    'durable_caller',nullif(pg_catalog.btrim(coalesce(p_caller,'')),''),
    'requested_timesheet_id',v_timesheet,
    'canonical_timesheet_id',v_root,
    'entry_point',v_entry,
    'refusal_code','WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED',
    'refusal_sqlstate','55000',
    'refusal_basis',v_basis,
    'block_reason',v_detail->>'block_reason',
    'integrity_failure',v_detail->'integrity_failure',
    'reason',v_detail->>'reason',
    'corroboration',v_corroboration,
    'recorded_in_transaction_id',pg_catalog.pg_current_xact_id()::text,
    'performed_by',case when p_actor_user_id is null then 'SYSTEM' else 'OFFICE_USER' end);
  v_after:=v_after||pg_catalog.jsonb_build_object(
    'narrative',private.weekly_source_audit_sentence_v1(
      'WEEKLY_SOURCE_ROTATION_REFUSED',null,v_after,v_detail->>'reason'));

  perform public._audit_insert(
    'timesheets',v_root::text,'WEEKLY_SOURCE_ROTATION_REFUSED',
    null,v_after,'WEEKLY_SOURCE_MANAGED_ROOT_GUARD',p_actor_user_id);

  return pg_catalog.jsonb_build_object(
    'ok',true,'recorded',true,
    'correlation_id',pg_catalog.btrim(p_correlation_id),
    'entry_point',v_entry,
    'refusal_basis',v_basis,
    'requested_timesheet_id',v_timesheet,
    'canonical_timesheet_id',v_root,
    'corroboration',v_corroboration);
end;
$function$;

create or replace function public.weekly_source_guard_refusal_record_after_rollback_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_correlation text;
  v_caller text;
  v_actor uuid;
  v_refusals jsonb;
  v_element jsonb;
  v_records jsonb:='[]'::jsonb;
  v_count integer;
  v_written bigint;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
               where key not in ('correlation_id','caller','actor_user_id','refusals')) then
    raise exception 'WEEKLY_SOURCE_GUARD_REFUSAL_RECORD_REQUEST_INVALID'
      using errcode='22023';
  end if;

  -- Ruling A2: a SEPARATE post-rollback transaction, proved from the server
  -- before anything is written.  Any tuple write in this transaction -- in an
  -- enclosing statement, an earlier statement or an aborted subtransaction --
  -- assigns a transaction id, so a null here means this transaction has
  -- written nothing at all.  This is the gate, and it is exact.
  --
  -- `pg_stat_xact_user_tables` is deliberately NOT the gate, although it is
  -- the counter the zero-write measurement reads.  Measured here: its absolute
  -- reading is per SESSION until the statistics are flushed, not per
  -- transaction, so a connection that wrote in an earlier transaction still
  -- reads non-zero at the start of the next one.  As a gate it would refuse a
  -- legitimate record on a pooled connection; as a DELTA -- which is how the
  -- zero-write measurement uses it, before the call and again inside the
  -- handler -- it is exact.  So it is used here as a delta too, below, to
  -- prove this transaction writes the refusal records and nothing else.
  if pg_catalog.pg_current_xact_id_if_assigned() is not null then
    raise exception 'WEEKLY_SOURCE_GUARD_REFUSAL_RECORD_NOT_A_SEPARATE_TRANSACTION'
      using errcode='55000',
            detail='This transaction has already written. A guard refusal is '
              ||'recorded only in its own transaction, after the refused '
              ||'transaction has rolled back.';
  end if;

  v_correlation:=p_request->>'correlation_id';
  v_caller:=p_request->>'caller';
  begin
    v_actor:=nullif(pg_catalog.btrim(coalesce(p_request->>'actor_user_id','')),'')::uuid;
  exception when others then
    raise exception 'WEEKLY_SOURCE_GUARD_REFUSAL_RECORD_REQUEST_INVALID'
      using errcode='22023';
  end;

  v_refusals:=p_request->'refusals';
  if v_refusals is null or pg_catalog.jsonb_typeof(v_refusals)<>'array' then
    raise exception 'WEEKLY_SOURCE_GUARD_REFUSAL_RECORD_REQUEST_INVALID'
      using errcode='22023';
  end if;
  -- An explicit cardinality decision, never a limit: one call carries the
  -- refusals of ONE attempt, and a per-row owner that returns many (E26) can
  -- carry all of them without a second round trip.
  v_count:=pg_catalog.jsonb_array_length(v_refusals);
  if v_count<1 or v_count>50 then
    raise exception 'WEEKLY_SOURCE_GUARD_REFUSAL_RECORD_CARDINALITY_INVALID'
      using errcode='22023';
  end if;

  select coalesce(pg_catalog.sum(stat.n_tup_ins+stat.n_tup_upd+stat.n_tup_del),0)
    into v_written
  from pg_catalog.pg_stat_xact_user_tables stat;

  for v_element in
    select element.value
    from pg_catalog.jsonb_array_elements(v_refusals) as element
  loop
    v_records:=v_records||pg_catalog.jsonb_build_array(
      private.weekly_source_guard_refusal_record_v1(
        v_correlation,v_element,v_caller,v_actor));
  end loop;

  -- The recording transaction writes the refusal records and nothing else.
  -- Measured as a delta over the same counter the zero-write proof reads.
  select coalesce(pg_catalog.sum(stat.n_tup_ins+stat.n_tup_upd+stat.n_tup_del),0)
       - v_written
    into v_written
  from pg_catalog.pg_stat_xact_user_tables stat;
  if v_written is distinct from v_count::bigint then
    raise exception 'WEEKLY_SOURCE_GUARD_REFUSAL_RECORD_WROTE_MORE_THAN_THE_RECORD'
      using errcode='55000',
            detail='Expected '||v_count::text||' tuples, measured '
              ||coalesce(v_written::text,'<null>')||'.';
  end if;

  return pg_catalog.jsonb_build_object(
    'ok',true,
    'correlation_id',nullif(pg_catalog.btrim(coalesce(v_correlation,'')),''),
    'recorded',pg_catalog.jsonb_array_length(v_records),
    'recording_transaction_id',pg_catalog.pg_current_xact_id()::text,
    'tuples_written_by_this_call',v_written,
    'records',v_records);
end;
$function$;

-- ===========================================================================
-- 5. The plain-English chronology reader
--
-- This is what closes the Gate 11 exit: Office can explain what was submitted,
-- approved, invoiced, paid and later changed WITHOUT reading raw JSON.  Every
-- row carries its own sentence; rows written by the other Weekly Source owners
-- (which carry no narrative of their own) are given one here from the same
-- vocabulary, so the chronology is complete rather than partial.
-- ===========================================================================
-- STEP 6 / hostile-review F-02.  `audit_events.event_sequence` is now the
-- durable ordering authority.  Unlike `ts_utc` (a transaction timestamp) and
-- `id` (a random UUID), it records the database insertion order even when one
-- transaction writes several lifecycle facts at the same instant.  Rows that
-- predate the migration are retained but explicitly report
-- `order_is_authoritative=false`; the reader never pretends that their lost
-- historical order can be reconstructed.
--
-- The lifecycle-rank helper is retained only as a stable compatibility helper
-- for existing callers.  It is not used to manufacture the chronology.
create or replace function private.weekly_source_audit_lifecycle_rank_v1(
  p_action text
) returns integer
language sql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  select case p_action
    when 'WEEKLY_SOURCE_BASE_TIMESHEET_CREATED' then 10
    when 'WEEKLY_SOURCE_TIMESHEET_LINEAGE_CREATED' then 15
    when 'WEEKLY_TIMESHEET_SUBMISSION_REQUESTED' then 20
    when 'WEEKLY_TIMESHEET_SUBMISSION_COMPLETED' then 25
    when 'WEEKLY_SOURCE_CANDIDATE_ASKED' then 28
    when 'WEEKLY_SOURCE_MANAGER_SEND_STAGED' then 29
    when 'WEEKLY_SOURCE_SYSTEM_HOURS_ACCEPTED' then 30
    when 'WEEKLY_SOURCE_CORRECT_FINAL_OPENED' then 32
    when 'WEEKLY_SOURCE_CORRECT_FINAL_PREPARED' then 33
    when 'WEEKLY_SOURCE_CORRECT_FINAL_REVIEWED' then 34
    when 'WEEKLY_SOURCE_CORRECT_FINAL_APPLIED' then 35
    when 'WEEKLY_SOURCE_FIRST_AUTHORISATION_RECORDED' then 40
    when 'WEEKLY_SOURCE_ROTATION_REFUSED' then 42
    when 'WEEKLY_SOURCE_LATER_CHANGE_DECIDED' then 45
    when 'WEEKLY_SOURCE_ENTITLEMENT_STAGED' then 50
    when 'WEEKLY_SOURCE_PENDING_ENTITLEMENT_SAVED' then 55
    when 'WEEKLY_SOURCE_PENDING_RELEASE_ATTEMPT_STARTED' then 58
    when 'WEEKLY_SOURCE_ENTITLEMENT_PUBLICATION_FROZEN' then 60
    when 'WEEKLY_SOURCE_PENDING_RELEASE_ATTEMPT_DEFERRED' then 62
    when 'WEEKLY_SOURCE_PENDING_RELEASE_ATTEMPT_FAILED' then 64
    when 'WEEKLY_SOURCE_PENDING_ENTITLEMENT_MANUAL_REVIEW' then 66
    when 'WEEKLY_SOURCE_PENDING_ENTITLEMENT_BUNDLE_REOPENED' then 68
    when 'WEEKLY_SOURCE_PENDING_ENTITLEMENT_RELEASED' then 70
    when 'WEEKLY_SOURCE_ENTITLEMENT_PUBLISHED_IMMEDIATELY' then 72
    when 'WEEKLY_SOURCE_ENTITLEMENT_PUBLISHED_AFTER_DEFERRAL' then 72
    when 'WEEKLY_SOURCE_CANDIDATE_HOURS_PUSH_WITHHELD' then 75
    when 'WEEKLY_SOURCE_ENTITLEMENT_SUPERSEDED' then 80
    when 'WEEKLY_SOURCE_PENDING_ENTITLEMENT_SUPERSEDED' then 82
    when 'WEEKLY_SOURCE_INVOICE_ADMITTED' then 85
    when 'WEEKLY_SOURCE_PRESENTATION_LINE_MOVED_BETWEEN_DRAFT_INVOICES' then 87
    when 'WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWAL_REFUSED' then 88
    when 'WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWN' then 90
    else 500
  end;
$function$;

create or replace function private.weekly_source_audit_chronology_v1(
  p_timesheet_id uuid
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_family jsonb;
  v_members uuid[];
  v_rows jsonb;
  v_all_order_authoritative boolean;
begin
  v_family:=private.weekly_source_audit_family_v1(p_timesheet_id);
  select pg_catalog.array_agg(member.value::uuid)
    into v_members
  from pg_catalog.jsonb_array_elements_text(v_family->'member_timesheet_ids')
    as member(value);
  v_members:=coalesce(v_members,array[p_timesheet_id]);

  -- WP-14b F3.  Several REQUIRED events are written by other Weekly Source
  -- owners against the object they own rather than against the Timesheet, so a
  -- reader that looked only at `object_type='timesheets'` could never show
  -- them: the Office decision (`…decision_bundles`), the Office reopen
  -- (`…pending_entitlement_bundles`), the source lineage, the invoice
  -- admission and line move, and the final-source correction.  `24 section 18`
  -- requires all of them, and the Gate 11 exit is not met without them.
  --
  -- Nothing new is WRITTEN here: each class is JOINED back to a member root
  -- through the identity its own row already carries, so `G5-6` (the reopen is
  -- counted exactly once) stays true and no owner's behaviour changes.  A class
  -- whose audit row carries no Timesheet identity at all is NOT guessed at; it
  -- is listed as a stated gap in the package report.
  select coalesce(pg_catalog.jsonb_agg(entry order by sort_sequence),'[]'::jsonb)
    into v_rows
  from (
    select pg_catalog.jsonb_build_object(
      'audit_event_id',keyed.id,
      'event_sequence',keyed.event_sequence,
      'order_is_authoritative',keyed.event_sequence_is_authoritative,
      'at_utc',keyed.ts_utc,
      'actor_display',keyed.actor_display,
      'actor_role_at_time',keyed.actor_role_at_time,
      'performed_by',coalesce(keyed.after_json->>'performed_by',
        case when keyed.actor_display is null then 'SYSTEM' else 'OFFICE_USER' end),
      'timesheet_id',keyed.member_id,
      'audited_object_type',keyed.object_type,
      'audited_object_id',keyed.object_id_text,
      'event',keyed.action,
      'narrative',coalesce(
        keyed.after_json->>'narrative',
        private.weekly_source_audit_sentence_v1(
          keyed.action,keyed.before_json,keyed.after_json,
          keyed.reason)),
      'references',private.weekly_source_audit_references_v1(
        keyed.before_json,keyed.after_json),
      'publication_mode',keyed.after_json->>'publication_mode',
      'publication_receipt_digest',keyed.after_json->>'publication_receipt_digest'
    ) as entry,
    keyed.event_sequence as sort_sequence
    from (
      -- (1) Events this and the other owners key to the Timesheet itself.
      select audit_row.*,audit_row.object_id_text as member_id
        from public.audit_events audit_row
       where audit_row.object_type='timesheets'
         and exists(
           select 1 from pg_catalog.unnest(v_members) as member(member_id)
           where member.member_id::text=audit_row.object_id_text)

      -- (2) The Office reopen and anything else keyed to the pending bundle.
      --     The bundle carries ONE audit row for all its member roots, so it
      --     is emitted ONCE and attributed to the lowest member of this family
      --     that the bundle covers.  `min` over the whole matching set is a
      --     deterministic presentational choice, not a safety decision, and is
      --     the same technique the family reader already uses.
      union all
      select audit_row.*,
             (select pg_catalog.min(member.member_id::text)
                from pg_catalog.unnest(v_members) as member(member_id)
               where member.member_id=any(bundle.member_root_ids))
        from public.audit_events audit_row
        join public.weekly_source_pending_entitlement_bundles bundle
          on bundle.id::text=audit_row.object_id_text
       where audit_row.object_type='weekly_source_pending_entitlement_bundles'
         and exists(
           select 1 from pg_catalog.unnest(v_members) as member(member_id)
            where member.member_id=any(bundle.member_root_ids))

      -- (3) The Office decision, keyed to the decision bundle.  A member is
      --     reached through the recorded root, the bundle's own source root or
      --     the root of any head the bundle proposed.  One row per event.
      --     A decision bundle id spans several revisions, so the bundle table is
      --     reached only through `exists` and never joined: one audit row in,
      --     one chronology entry out, whatever the revision history.
      union all
      select audit_row.*,
             (select pg_catalog.min(member.member_id::text)
                from pg_catalog.unnest(v_members) as member(member_id)
               where member.member_id::text=audit_row.after_json->>'root_timesheet_id'
                  or exists(
                    select 1
                      from public.weekly_source_entitlement_decision_bundles decision
                     where decision.decision_bundle_id::text=audit_row.object_id_text
                       and (decision.source_root_timesheet_id=member.member_id
                            or exists(
                              select 1
                                from public.weekly_source_entitlement_heads proposed
                               where proposed.id=any(decision.proposed_head_ids)
                                 and proposed.root_timesheet_id=member.member_id))))
        from public.audit_events audit_row
       where audit_row.object_type='weekly_source_entitlement_decision_bundles'
         and exists(
           select 1 from pg_catalog.unnest(v_members) as member(member_id)
            where member.member_id::text=audit_row.after_json->>'root_timesheet_id'
               or exists(
                 select 1
                   from public.weekly_source_entitlement_decision_bundles decision
                  where decision.decision_bundle_id::text=audit_row.object_id_text
                    and (decision.source_root_timesheet_id=member.member_id
                         or exists(
                           select 1
                             from public.weekly_source_entitlement_heads proposed
                            where proposed.id=any(decision.proposed_head_ids)
                              and proposed.root_timesheet_id=member.member_id))))

      -- (4) The source lineage row, which carries its Timesheet directly.
      union all
      select audit_row.*,lineage.timesheet_id::text
        from public.audit_events audit_row
        join public.weekly_source_row_timesheet_lineages lineage
          on lineage.id::text=audit_row.object_id_text
       where audit_row.object_type='weekly_source_row_timesheet_lineages'
         and lineage.timesheet_id=any(v_members)

      -- (5) An invoice presentation line, through the billing movement that
      --     names the Timesheet the line presents.
      union all
      select audit_row.*,movement.invoice_timesheet_id::text
        from public.audit_events audit_row
        join public.weekly_source_invoice_presentation_lines line
          on line.id::text=audit_row.object_id_text
        join public.weekly_source_billing_movements movement
          on movement.id=line.billing_movement_id
       where audit_row.object_type='weekly_source_invoice_presentation_lines'
         and movement.invoice_timesheet_id=any(v_members)

      -- (6) Admission of a draft invoice, through the client manifest whose
      --     presentation lines present a member Timesheet.  One entry per
      --     admission, not one per line.
      union all
      select audit_row.*,
             (select pg_catalog.min(movement.invoice_timesheet_id::text)
                from public.weekly_source_invoice_presentation_lines line
                join public.weekly_source_billing_movements movement
                  on movement.id=line.billing_movement_id
               where line.client_manifest_id::text
                       =audit_row.after_json->>'client_manifest_id'
                 and movement.invoice_timesheet_id=any(v_members))
        from public.audit_events audit_row
       where audit_row.object_type='invoices'
         and audit_row.action='WEEKLY_SOURCE_INVOICE_ADMITTED'
         and exists(
           select 1
             from public.weekly_source_invoice_presentation_lines line
             join public.weekly_source_billing_movements movement
               on movement.id=line.billing_movement_id
            where line.client_manifest_id::text
                    =audit_row.after_json->>'client_manifest_id'
              and movement.invoice_timesheet_id=any(v_members))

      -- (7) A final-source correction, which is made against a source CYCLE and
      --     therefore concerns every member Timesheet drawn from that cycle.
      --     One entry per correction event, not one per lineage row.
      union all
      select audit_row.*,
             (select pg_catalog.min(lineage.timesheet_id::text)
                from public.weekly_final_source_correction_sessions session
                join public.weekly_source_row_timesheet_lineages lineage
                  on lineage.source_cycle_id=session.source_cycle_id
               where session.id::text=audit_row.object_id_text
                 and lineage.timesheet_id=any(v_members))
        from public.audit_events audit_row
       where audit_row.object_type='weekly_final_source_correction_sessions'
         and exists(
           select 1
             from public.weekly_final_source_correction_sessions session
             join public.weekly_source_row_timesheet_lineages lineage
               on lineage.source_cycle_id=session.source_cycle_id
            where session.id::text=audit_row.object_id_text
              and lineage.timesheet_id=any(v_members))
    ) as keyed
    where keyed.action like 'WEEKLY!_SOURCE!_%' escape '!'
       or keyed.action like 'WEEKLY!_TIMESHEET!_%' escape '!'
       or keyed.action like 'WEEKLY!_MANAGER!_%' escape '!'
       or keyed.action like 'WEEKLY!_CANDIDATE!_%' escape '!'
  ) as chronology;

  select coalesce(pg_catalog.bool_and(
           coalesce((event.value->>'order_is_authoritative')::boolean,false)),true)
    into v_all_order_authoritative
  from pg_catalog.jsonb_array_elements(v_rows) as event(value);

  return pg_catalog.jsonb_build_object(
    'ok',true,
    'root_timesheet_id',p_timesheet_id,
    'canonical_timesheet_id',v_family->'canonical_timesheet_id',
    'family_booking_id',v_family->'family_booking_id',
    'member_timesheet_ids',v_family->'member_timesheet_ids',
    'family_resolved',v_family->'ok',
    'event_order_authority','AUDIT_EVENT_SEQUENCE',
    'event_order_fully_authoritative',v_all_order_authoritative,
    'event_count',pg_catalog.jsonb_array_length(v_rows),
    'events',v_rows);
end;
$function$;

create or replace function public.weekly_source_timesheet_audit_chronology_v1(
  p_request jsonb
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_timesheet uuid;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
               where key not in ('timesheet_id')) then
    raise exception 'WEEKLY_SOURCE_AUDIT_CHRONOLOGY_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_timesheet:=(p_request->>'timesheet_id')::uuid;
  exception when others then
    raise exception 'WEEKLY_SOURCE_AUDIT_CHRONOLOGY_REQUEST_INVALID' using errcode='22023';
  end;
  if v_timesheet is null then
    raise exception 'WEEKLY_SOURCE_AUDIT_CHRONOLOGY_REQUEST_INVALID' using errcode='22023';
  end if;
  return private.weekly_source_audit_chronology_v1(v_timesheet);
end;
$function$;

-- ===========================================================================
-- 6. Export: four separated hour facts and the invoice movements
-- ===========================================================================

-- 6.1 SUBMITTED.  The Candidate's own evidence, and nothing else.  Where no
-- Candidate submission exists the submitted fact stays EMPTY and is never
-- filled from source (`24 section 18`, `UI-021`).
create or replace function private.weekly_source_export_submitted_hours_v1(
  p_timesheet_id uuid
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_timesheet public.timesheets%rowtype;
  v_submitted boolean;
  v_segments jsonb;
  v_hours numeric;
  v_rows integer;
begin
  select * into v_timesheet from public.timesheets
  where timesheet_id=p_timesheet_id;
  if not found then
    return pg_catalog.jsonb_build_object(
      'state','UNAVAILABLE','reason','TIMESHEET_NOT_FOUND',
      'submitted',false,'total_hours',null);
  end if;

  v_submitted:=v_timesheet.r2_nurse_key is not null
    and v_timesheet.img_sha256_nurse is not null;
  if not v_submitted then
    return pg_catalog.jsonb_build_object(
      'state','NO_SUBMISSION','reason',null,
      'submitted',false,'total_hours',null);
  end if;

  -- WP-14b F6.  A submitted schedule is stored in THREE shapes by the installed
  -- writers - `{date,start,end}` (Office and the calculators),
  -- `{worked_start_iso,worked_end_iso}` and `{start_utc,end_utc}` (the brokers).
  -- Summing only the second shape reported "AVAILABLE, 0 hours" for the other
  -- two: an available figure of zero for a Candidate who submitted a full week,
  -- which is exactly what `24 section 18` says a report must never do with the
  -- submitted fact.  The segments are now derived through the INSTALLED
  -- Candidate-app schedule reader, which accepts all three shapes and RAISES on
  -- anything it cannot read, so an unreadable segment becomes UNAVAILABLE with
  -- a reason and no figure - never a zero.
  begin
    v_segments:=private.weekly_source_candidate_app_schedule_v1(
      v_timesheet.actual_schedule_json,'{}'::jsonb);
  exception when others then
    return pg_catalog.jsonb_build_object(
      'state','UNAVAILABLE','reason','SUBMITTED_SCHEDULE_NOT_DERIVABLE',
      'submitted',true,'total_hours',null);
  end;

  -- A stored schedule that is a non-empty array must produce a segment for
  -- every element; the reader raises otherwise, so a short read cannot pass
  -- silently.  An empty stored schedule is a real (and rare) zero and is
  -- reported as such, distinguished by `segment_count`.
  select pg_catalog.count(*)::integer,
         pg_catalog.sum(
           pg_catalog.date_part('epoch',
             -- An end at or before the start is the next calendar day.
             ((segment.value->>'date')::date
               +(segment.value->>'end')::time
               +case when (segment.value->>'end')::time
                          <=(segment.value->>'start')::time
                     then pg_catalog.make_interval(days=>1)
                     else pg_catalog.make_interval() end)
             -((segment.value->>'date')::date+(segment.value->>'start')::time))/3600.0
           -coalesce((segment.value#>>'{break_entry,break_minutes}')::numeric,0)/60.0)
    into v_rows,v_hours
  from pg_catalog.jsonb_array_elements(v_segments) as segment(value);

  return pg_catalog.jsonb_build_object(
    'state','AVAILABLE','reason',null,
    'submitted',true,
    'segment_count',coalesce(v_rows,0),
    'total_hours',coalesce(v_hours,0));
exception when others then
  return pg_catalog.jsonb_build_object(
    'state','UNAVAILABLE','reason','SUBMITTED_SCHEDULE_NOT_DERIVABLE',
    'submitted',true,'total_hours',null);
end;
$function$;

-- 6.2 SOURCE.  The latest accepted source statement for the week, from the
-- current projection publication's resolved rows.  It is a source fact and is
-- never presented as submitted, approved or paid.
create or replace function private.weekly_source_export_source_hours_v1(
  p_timesheet_id uuid
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_hours numeric;
  v_rows integer;
  v_cycles jsonb;
  v_lineage_rows integer;
  v_upload uuid;
  v_upload_count integer;
  v_family uuid[];
begin
  -- WP-30 (WP-27 sweep finding N3), standing rule 3.  The lineage binding is a
  -- fact about the FAMILY, written against whichever physical version was
  -- current at the time.  Keyed on the bare physical id every one of the three
  -- lineage reads below went blind after a rotation and the week reported
  -- `NO_SOURCE` — EXECUTED.  WP-27 named two of the three; the aggregate below
  -- is the third and was not on its list.
  --
  -- One family resolution, through the one installed adapter, read by all
  -- three.  The lineage relation carries at most one row per source-row
  -- resolution (`on conflict (row_resolution_id) do nothing`), so widening to
  -- the family cannot double-count a resolution.
  v_family:=private.weekly_source_invoice_family_timesheet_ids_v1(p_timesheet_id);
  -- Standing rule 3's fail-closed branch: an explicit cardinality test, never a
  -- `limit`.  An unresolvable family is reported, never treated as "no source".
  if v_family is null or pg_catalog.cardinality(v_family)=0 then
    return pg_catalog.jsonb_build_object(
      'state','UNAVAILABLE','reason','SOURCE_FAMILY_UNRESOLVED',
      'row_count',null,'total_hours',null,'source_cycle_ids','[]'::jsonb);
  end if;
  -- WP-14b F7.  The lineage owner writes ONE ROW PER RESOLUTION
  -- (`…timesheet_lineage_v1`, `on conflict (row_resolution_id) do nothing`), so
  -- a later accepted upload of the same shifts - an ordinary later source
  -- change, `24 section 4.2` - adds a SECOND lineage row per shift and the
  -- "source hours" figure doubled on every re-export: 33 hours reported for a
  -- 16.5 hour week.  The source fact for a week is the LATEST ACCEPTED source
  -- statement, so the rows are restricted to the one upload the current
  -- projection publication was made from, exactly as WP-11a's Candidate
  -- producer resolves it.
  --
  -- Cardinality is checked EXPLICITLY rather than resolved by `order by … limit`
  -- (Part 1 rule 5): more than one current publication for one Timesheet is
  -- contradictory evidence and is reported, never silently picked from.
  select pg_catalog.count(*)::integer,
         pg_catalog.min(publication.upload_id::text)::uuid
    into v_upload_count,v_upload
  from public.weekly_source_projection_publications publication
  where publication.state='CURRENT'
    and exists(
      select 1 from public.weekly_source_row_timesheet_lineages lineage
      join public.weekly_source_row_resolutions resolution
        on resolution.id=lineage.row_resolution_id
      join public.weekly_source_upload_rows source_row
        on source_row.id=resolution.upload_row_id
      where lineage.timesheet_id=any(v_family)
        and source_row.upload_id=publication.upload_id);

  select pg_catalog.count(*)::integer into v_lineage_rows
  from public.weekly_source_row_timesheet_lineages lineage
  where lineage.timesheet_id=any(v_family);

  if coalesce(v_lineage_rows,0)=0 then
    return pg_catalog.jsonb_build_object(
      'state','NO_SOURCE','reason',null,'row_count',0,
      'total_hours',null,'source_cycle_ids','[]'::jsonb);
  end if;
  if v_upload_count=0 then
    -- Source rows are bound to this week but none of them belongs to a current
    -- publication.  A figure would be a statement about a source statement that
    -- is not the current one, so none is given.
    return pg_catalog.jsonb_build_object(
      'state','UNAVAILABLE','reason','NO_CURRENT_PROJECTION_PUBLICATION',
      'row_count',null,'total_hours',null,'source_cycle_ids','[]'::jsonb);
  end if;
  if v_upload_count>1 then
    return pg_catalog.jsonb_build_object(
      'state','UNAVAILABLE','reason','MULTIPLE_CURRENT_PROJECTION_PUBLICATIONS',
      'row_count',null,'total_hours',null,'source_cycle_ids','[]'::jsonb,
      'publication_count',v_upload_count);
  end if;

  select pg_catalog.sum(
           pg_catalog.date_part('epoch',
             source_row.end_at_local-source_row.start_at_local)/3600.0
           -coalesce(source_row.break_minutes,0)/60.0)::numeric,
         pg_catalog.count(*)::integer,
         coalesce(pg_catalog.jsonb_agg(distinct cycle_row.id),'[]'::jsonb)
    into v_hours,v_rows,v_cycles
  from public.weekly_source_row_timesheet_lineages lineage
  join public.weekly_source_row_resolutions resolution
    on resolution.id=lineage.row_resolution_id
  join public.weekly_source_upload_rows source_row
    on source_row.id=resolution.upload_row_id
  join public.weekly_source_cycles cycle_row
    on cycle_row.id=lineage.source_cycle_id
  where lineage.timesheet_id=any(v_family)
    and source_row.upload_id=v_upload
    and source_row.start_at_local is not null
    and source_row.end_at_local is not null
    and source_row.row_finalisation_state in ('NOT_APPLICABLE','SOURCE_WORKED');

  if coalesce(v_rows,0)=0 then
    return pg_catalog.jsonb_build_object(
      'state','NO_SOURCE','reason',null,'row_count',0,
      'total_hours',null,'source_cycle_ids','[]'::jsonb);
  end if;

  return pg_catalog.jsonb_build_object(
    'state','AVAILABLE','reason',null,'row_count',v_rows,
    'total_hours',coalesce(v_hours,0),'source_cycle_ids',v_cycles,
    'source_upload_id',v_upload,
    'lineage_row_count',v_lineage_rows);
end;
$function$;

-- 6.3 APPROVED.  The complete current entitlement, from the one committed head
-- for the family.  With no head, the export says so; it never substitutes the
-- source or the submission for an approved fact.
create or replace function private.weekly_source_export_approved_hours_v1(
  p_timesheet_id uuid
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_family jsonb;
  v_members uuid[];
  v_head public.weekly_source_entitlement_heads%rowtype;
  v_head_count integer;
  v_hours numeric;
  v_day numeric; v_night numeric; v_sat numeric; v_sun numeric; v_bh numeric;
begin
  v_family:=private.weekly_source_audit_family_v1(p_timesheet_id);
  select pg_catalog.array_agg(member.value::uuid)
    into v_members
  from pg_catalog.jsonb_array_elements_text(v_family->'member_timesheet_ids')
    as member(value);
  v_members:=coalesce(v_members,array[p_timesheet_id]);

  select pg_catalog.count(*)::integer into v_head_count
  from public.weekly_source_entitlement_heads head_row
  where head_row.state='COMMITTED_CURRENT'
    and head_row.root_timesheet_id=any(v_members);

  if v_head_count=0 then
    return pg_catalog.jsonb_build_object(
      'state','NO_APPROVED_ENTITLEMENT','reason',null,
      'head_id',null,'total_hours',null);
  end if;
  -- More than one committed head for one family is contradictory evidence.
  -- It is reported, never resolved by picking one.
  if v_head_count>1 then
    return pg_catalog.jsonb_build_object(
      'state','UNAVAILABLE','reason','MULTIPLE_COMMITTED_HEADS_FOR_FAMILY',
      'head_id',null,'total_hours',null,'head_count',v_head_count);
  end if;

  select * into v_head
  from public.weekly_source_entitlement_heads head_row
  where head_row.state='COMMITTED_CURRENT'
    and head_row.root_timesheet_id=any(v_members);

  if v_head.certified_zero then
    return pg_catalog.jsonb_build_object(
      'state','AVAILABLE','reason',null,
      'head_id',v_head.id,'head_revision',v_head.head_revision,
      'authority_kind',v_head.authority_kind,
      'certified_zero',true,'component_count',0,
      'total_hours',0::numeric,
      'hours_by_bucket',pg_catalog.jsonb_build_object(
        'day',0::numeric,'night',0::numeric,'sat',0::numeric,
        'sun',0::numeric,'bh',0::numeric));
  end if;

  select coalesce(pg_catalog.sum(
           coalesce(component.hours_day,0)+coalesce(component.hours_night,0)
           +coalesce(component.hours_sat,0)+coalesce(component.hours_sun,0)
           +coalesce(component.hours_bh,0)),0),
         coalesce(pg_catalog.sum(coalesce(component.hours_day,0)),0),
         coalesce(pg_catalog.sum(coalesce(component.hours_night,0)),0),
         coalesce(pg_catalog.sum(coalesce(component.hours_sat,0)),0),
         coalesce(pg_catalog.sum(coalesce(component.hours_sun,0)),0),
         coalesce(pg_catalog.sum(coalesce(component.hours_bh,0)),0)
    into v_hours,v_day,v_night,v_sat,v_sun,v_bh
  from public.weekly_source_entitlement_head_components component
  where component.head_id=v_head.id
    and component.exclude_from_pay is not true;

  return pg_catalog.jsonb_build_object(
    'state','AVAILABLE','reason',null,
    'head_id',v_head.id,'head_revision',v_head.head_revision,
    'authority_kind',v_head.authority_kind,
    'certified_zero',false,'component_count',v_head.component_count,
    'committed_at_utc',v_head.committed_at_utc,
    'total_hours',v_hours,
    'hours_by_bucket',pg_catalog.jsonb_build_object(
      'day',v_day,'night',v_night,'sat',v_sat,'sun',v_sun,'bh',v_bh));
end;
$function$;

-- 6.4 INVOICE MOVEMENTS.  Separate from every hour fact.  A movement is a
-- charge presentation event; it is never a paid-hours source (XSG-029).
create or replace function private.weekly_source_export_invoice_movements_v1(
  p_timesheet_id uuid
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_family jsonb;
  v_members uuid[];
  v_rows jsonb;
begin
  v_family:=private.weekly_source_audit_family_v1(p_timesheet_id);
  select pg_catalog.array_agg(member.value::uuid)
    into v_members
  from pg_catalog.jsonb_array_elements_text(v_family->'member_timesheet_ids')
    as member(value);
  v_members:=coalesce(v_members,array[p_timesheet_id]);

  select coalesce(pg_catalog.jsonb_agg(movement order by
           movement->>'created_at_utc',movement->>'movement_id'),'[]'::jsonb)
    into v_rows
  from (
    select pg_catalog.jsonb_build_object(
      'movement_id',movement_row.id,
      'movement_role',movement_row.movement_role,
      'placement_state',movement_row.placement_state,
      'source_line_kind',movement_row.source_line_kind,
      'final_revision_id',movement_row.final_revision_id,
      'source_cycle_id',movement_row.finalisation_cycle_id,
      'invoice_timesheet_id',movement_row.invoice_timesheet_id,
      'invoice_presentation_charge_pence',movement_row.invoice_presentation_charge_pence,
      'created_at_utc',movement_row.created_at_utc) as movement
    from public.weekly_source_billing_movements movement_row
    where movement_row.invoice_timesheet_id=any(v_members)
  ) as movements;

  return pg_catalog.jsonb_build_object(
    'state','AVAILABLE','reason',null,
    'movement_count',pg_catalog.jsonb_array_length(v_rows),
    'movements',v_rows);
end;
$function$;

-- 6.5 The one export composer.
--
-- `paid` comes ONLY from the Gate 9 settlement-allocation reader.  This routine
-- performs no currency-to-hours arithmetic and does not read the last-settled
-- cache; when the reader says UNAVAILABLE the export carries that state and its
-- reason and shows no figure.
create or replace function private.weekly_source_export_hours_v1(
  p_timesheet_id uuid
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_settlement jsonb;
  v_paid jsonb;
  v_family uuid[];
begin
  if p_timesheet_id is null then
    return '{}'::jsonb;
  end if;
  -- WP-30 (WP-27 sweep finding N3), standing rule 3.  After schema change S8 a
  -- physical root id is not a family identity, and these four facts are written
  -- against whichever PHYSICAL version was current when they were written.  A
  -- week that rotates keeps every one of them on the demoted sibling, so all
  -- four `not exists` were true for the new current root and the function
  -- returned `{}` — EXECUTED: a Weekly Source week exported as an ordinary week
  -- while the demoted id still exported as Weekly Source.
  --
  -- One family resolution, through the one installed adapter, read by all four
  -- limbs.  No second resolver and no inline re-derivation.
  v_family:=private.weekly_source_invoice_family_timesheet_ids_v1(p_timesheet_id);
  -- Standing rule 3's fail-closed branch, as an explicit cardinality test and
  -- never a `limit`.  The adapter falls back to the physical id for a Timesheet
  -- with no booking identity, so this cannot fire against the installed
  -- resolver; it is here so a later change to the adapter cannot turn an
  -- unresolvable family into a silent ordinary export.
  if v_family is null or pg_catalog.cardinality(v_family)=0 then
    raise exception 'WEEKLY_SOURCE_EXPORT_FAMILY_UNRESOLVED'
      using errcode='55000';
  end if;
  -- A Timesheet is a Weekly Source week when Weekly Source holds a fact about
  -- it: a source-row lineage, an authorisation record, an entitlement head or
  -- an invoice movement.  Four independent facts, so a week is not hidden from
  -- the export merely because one of them has not been written yet.  Each fact
  -- is a fact about the FAMILY, so each is tested over the family.
  if not exists(
       select 1 from public.weekly_source_row_timesheet_lineages lineage
       where lineage.timesheet_id=any(v_family))
     and not exists(
       select 1 from public.weekly_source_root_authorisations authorisation_row
       where authorisation_row.root_timesheet_id=any(v_family))
     and not exists(
       select 1 from public.weekly_source_entitlement_heads head_row
       where head_row.root_timesheet_id=any(v_family))
     and not exists(
       select 1 from public.weekly_source_billing_movements movement_row
       where movement_row.invoice_timesheet_id=any(v_family)) then
    -- Not a Weekly Source week.  An ordinary Timesheet's export row is
    -- byte-identical to today because the additive member is an empty object.
    return '{}'::jsonb;
  end if;

  v_settlement:=private.weekly_source_settlement_allocation_v1(p_timesheet_id);
  v_paid:=case
    when coalesce((v_settlement->>'ok')::boolean,false) is not true
      then pg_catalog.jsonb_build_object(
        'state',coalesce(v_settlement->>'state','UNAVAILABLE'),
        'reason',v_settlement->>'reason',
        'total_hours',null)
    else pg_catalog.jsonb_build_object(
        'state',v_settlement->>'state',
        'reason',v_settlement->>'reason',
        'total_hours',v_settlement->'total_hours',
        'hours_by_bucket',v_settlement->'hours_by_bucket',
        'batch_count',v_settlement->'batch_count',
        'settlement_count',v_settlement->'settlement_count',
        'first_settled_at_utc',v_settlement->'first_settled_at_utc',
        'last_settled_at_utc',v_settlement->'last_settled_at_utc')
  end;

  return pg_catalog.jsonb_build_object(
    'weekly_source',true,
    'submitted_hours',private.weekly_source_export_submitted_hours_v1(p_timesheet_id),
    'source_hours',private.weekly_source_export_source_hours_v1(p_timesheet_id),
    'approved_hours',private.weekly_source_export_approved_hours_v1(p_timesheet_id),
    'paid_hours',v_paid,
    'paid_hours_authority','SETTLEMENT_ALLOCATION',
    'invoice_movements',private.weekly_source_export_invoice_movements_v1(p_timesheet_id));
end;
$function$;

create or replace function public.weekly_source_timesheet_hours_export_v1(
  p_request jsonb
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_timesheet uuid;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
               where key not in ('timesheet_id')) then
    raise exception 'WEEKLY_SOURCE_HOURS_EXPORT_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_timesheet:=(p_request->>'timesheet_id')::uuid;
  exception when others then
    raise exception 'WEEKLY_SOURCE_HOURS_EXPORT_REQUEST_INVALID' using errcode='22023';
  end;
  if v_timesheet is null then
    raise exception 'WEEKLY_SOURCE_HOURS_EXPORT_REQUEST_INVALID' using errcode='22023';
  end if;
  return pg_catalog.jsonb_build_object(
    'ok',true,'timesheet_id',v_timesheet,
    'weekly_source_hours',private.weekly_source_export_hours_v1(v_timesheet));
end;
$function$;

-- 6.6 Invoice-report source truth.  The ordinary invoice report remains
-- unchanged; a source invoice receives one additive object whose values come
-- from the immutable movement/binding authority, never from a Candidate
-- submission and never by reverse-calculating money into hours.
create or replace function public.weekly_source_invoice_report_rows_v1(
  p_request jsonb
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_invoice_ids uuid[];
  v_rows jsonb;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
               where key not in ('invoice_ids'))
     or pg_catalog.jsonb_typeof(p_request->'invoice_ids') is distinct from 'array'
     or pg_catalog.jsonb_array_length(p_request->'invoice_ids') not between 1 and 500 then
    raise exception 'WEEKLY_SOURCE_INVOICE_REPORT_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    select pg_catalog.array_agg(value::uuid order by ordinality)
      into v_invoice_ids
    from pg_catalog.jsonb_array_elements_text(p_request->'invoice_ids')
      with ordinality item(value,ordinality);
  exception when others then
    raise exception 'WEEKLY_SOURCE_INVOICE_REPORT_REQUEST_INVALID' using errcode='22023';
  end;
  if v_invoice_ids is null
     or pg_catalog.cardinality(v_invoice_ids)<>pg_catalog.jsonb_array_length(p_request->'invoice_ids')
     or pg_catalog.cardinality(v_invoice_ids)<>pg_catalog.cardinality(
       (select pg_catalog.array_agg(distinct invoice_id) from pg_catalog.unnest(v_invoice_ids) invoice_id)) then
    raise exception 'WEEKLY_SOURCE_INVOICE_REPORT_REQUEST_INVALID' using errcode='22023';
  end if;

  with movement_rows as materialized (
    select binding.invoice_id,
           movement.id movement_id,
           movement.movement_role,
           movement.invoice_timesheet_id,
           movement.finalisation_cycle_id source_cycle_id,
           movement.invoice_presentation_charge_pence,
           manifest.backing_report_number
    from public.weekly_source_invoice_line_bindings binding
    join public.weekly_source_billing_movements movement
      on movement.id=binding.billing_movement_id
    join public.weekly_source_invoice_presentation_lines presentation
      on presentation.id=binding.presentation_line_id
    join public.weekly_source_client_manifests manifest
      on manifest.id=presentation.client_manifest_id
    where binding.state='CURRENT'
      and binding.invoice_id=any(v_invoice_ids)
  ), per_invoice as (
    select invoice_id,
           pg_catalog.count(*)::integer movement_count,
           (pg_catalog.sum(invoice_presentation_charge_pence)::numeric/100.0)::numeric(12,2)
             source_movement_ex_vat,
           pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
             'movement_id',movement_id,
             'movement_role',movement_role,
             'timesheet_id',invoice_timesheet_id,
             'source_cycle_id',source_cycle_id,
             'charge_ex_vat',(invoice_presentation_charge_pence::numeric/100.0)::numeric(12,2)
           ) order by movement_id) movements,
           coalesce((select pg_catalog.jsonb_agg(cycle_id order by cycle_id)
                     from (select distinct source_cycle_id cycle_id
                           from movement_rows member
                           where member.invoice_id=movement_rows.invoice_id) cycles),'[]'::jsonb)
             source_cycle_ids,
           coalesce((select pg_catalog.jsonb_agg(report_number order by report_number)
                     from (select distinct backing_report_number report_number
                           from movement_rows member
                           where member.invoice_id=movement_rows.invoice_id
                             and backing_report_number is not null) reports),'[]'::jsonb)
             backing_report_numbers
    from movement_rows
    group by invoice_id
  )
  select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
           'invoice_id',invoice_id,
           'weekly_source',true,
           'movement_count',movement_count,
           'source_movement_ex_vat',source_movement_ex_vat,
           'source_cycle_ids',source_cycle_ids,
           'backing_report_numbers',backing_report_numbers,
           'movements',movements
         ) order by invoice_id),'[]'::jsonb)
    into v_rows
  from per_invoice;

  return pg_catalog.jsonb_build_object(
    'ok',true,
    'invoice_count',pg_catalog.jsonb_array_length(v_rows),
    'rows',v_rows);
end;
$function$;

-- ===========================================================================
-- 7. The Candidate hours-only push
-- ===========================================================================

-- 7.1 The payload.  Hours only: the approved hours for the week, and nothing
-- else.  Every member is built here, so the scanner in section 1 is scanning
-- the real thing rather than a summary.
--
-- WP-14b F1.  This used to build the payload from
-- `private.weekly_source_candidate_approved_hours_v1`, which derives the rows
-- from the CURRENT projection publication's SOURCE rows.  The push fires when a
-- head becomes `COMMITTED_CURRENT`, which is the moment the approved hours are
-- DECIDED, and Office's decision routinely differs from the source: on the
-- reviewer's run a certified-zero head pushed TWO SHIFTS to a real Candidate,
-- and a later four-hour head pushed NOTHING, because the dedupe digest was
-- taken over the unchanged source payload.
--
-- The approved hours of a week are the committed entitlement head and its
-- components - the same fact `private.weekly_source_export_approved_hours_v1`
-- reports - so the payload is now built from the head whenever a head exists.
-- The source producer is used only where no head exists at all (a Timesheet
-- authority week reached through the service entry point), and never to
-- describe a decided entitlement.  The producer's own defect is WP-11d's to
-- fix, and this owner does not depend on that fix: it does not read the
-- producer on any path where a committed head exists.
--
-- The rows themselves come from WP-11d's published entitlement reader,
-- `private.weekly_source_candidate_approved_entitlement_v1`, which is the
-- producer half of the same finding: it resolves the committed head, recovers
-- the presentation times the head's components name, reconciles them against
-- the head's own hours and returns an explicit UNAVAILABLE rather than ever
-- falling back to a source figure.  Using it means MyTMS and this push describe
-- the same entitlement in the same shape, from one owner.  If that reader is
-- not installed, this owner still does not read the source: it falls back to
-- the head's own components, which carry `work_date` and the five hour buckets
-- but no clock times, and states a date and a number of hours.
--
-- Either way the total is computed HERE from the head's components and checked
-- against the approved export before the boundary is called, so the payload
-- cannot drift from the entitlement it claims to describe.
create or replace function private.weekly_source_candidate_hours_push_payload_v1(
  p_timesheet_id uuid
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_context jsonb;
  v_approved jsonb;
  v_timesheet public.timesheets%rowtype;
  v_family jsonb;
  v_members uuid[];
  v_head public.weekly_source_entitlement_heads%rowtype;
  v_head_count integer;
  v_total numeric;
  v_source text;
  v_entitlement jsonb;
begin
  v_context:=private.weekly_source_candidate_week_context_v1(p_timesheet_id);
  if v_context is null then
    return pg_catalog.jsonb_build_object(
      'ok',false,'reason','NOT_A_WEEKLY_SOURCE_WEEK');
  end if;
  if coalesce((v_context->>'authorised_for_pay')::boolean,false) is not true then
    return pg_catalog.jsonb_build_object(
      'ok',false,'reason','NOT_AUTHORISED_FOR_PAY');
  end if;

  select * into v_timesheet from public.timesheets where timesheet_id=p_timesheet_id;
  if not found then
    return pg_catalog.jsonb_build_object('ok',false,'reason','TIMESHEET_NOT_FOUND');
  end if;

  v_family:=private.weekly_source_audit_family_v1(p_timesheet_id);
  select pg_catalog.array_agg(member.value::uuid)
    into v_members
  from pg_catalog.jsonb_array_elements_text(v_family->'member_timesheet_ids')
    as member(value);
  v_members:=coalesce(v_members,array[p_timesheet_id]);

  select pg_catalog.count(*)::integer into v_head_count
  from public.weekly_source_entitlement_heads head_row
  where head_row.state='COMMITTED_CURRENT'
    and head_row.root_timesheet_id=any(v_members);

  -- Two committed heads for one family is contradictory evidence about what was
  -- approved.  Nothing is told to the Candidate on a guess (Part 1 rule 5).
  if v_head_count>1 then
    return pg_catalog.jsonb_build_object(
      'ok',false,'reason','APPROVED_ENTITLEMENT_AMBIGUOUS');
  end if;

  if v_head_count=1 then
    select * into v_head
    from public.weekly_source_entitlement_heads head_row
    where head_row.state='COMMITTED_CURRENT'
      and head_row.root_timesheet_id=any(v_members);

    -- The head's own total, computed here and used to check whatever rows are
    -- produced below.
    select coalesce(pg_catalog.sum(
             coalesce(component.hours_day,0)+coalesce(component.hours_night,0)
             +coalesce(component.hours_sat,0)+coalesce(component.hours_sun,0)
             +coalesce(component.hours_bh,0)),0)
      into v_total
    from public.weekly_source_entitlement_head_components component
    where component.head_id=v_head.id
      and component.exclude_from_pay is not true;

    -- WP-11d's published entitlement reader is the preferred source of the
    -- rows.  It is called defensively so this owner still works, head-based,
    -- on a build where that package is not installed.
    begin
      v_entitlement:=private.weekly_source_candidate_approved_entitlement_v1(v_context);
    exception when others then
      v_entitlement:=null;
    end;

    if v_entitlement is not null then
      if v_entitlement->>'state'='AVAILABLE' then
        v_approved:=coalesce(v_entitlement->'rows','[]'::jsonb);
      elsif v_entitlement->>'state'='NO_APPROVED_ENTITLEMENT' then
        return pg_catalog.jsonb_build_object(
          'ok',false,'reason','NO_APPROVED_ENTITLEMENT');
      else
        -- The entitlement cannot be established.  Nothing is told to the
        -- Candidate on a guess.
        return pg_catalog.jsonb_build_object(
          'ok',false,'reason','APPROVED_ENTITLEMENT_UNAVAILABLE');
      end if;
    else
      v_approved:=private.weekly_source_candidate_hours_push_head_rows_v1(v_head.id);
    end if;

    v_source:='COMMITTED_ENTITLEMENT_HEAD';
    if coalesce(v_head.certified_zero,false) is true then
      -- A certified-zero head approves nothing.  `24 section 17`: the Candidate
      -- is told when the approved hours change, and a change to zero is a
      -- change, so the push is made with an explicit zero rather than withheld.
      -- The old behaviour told the Candidate two source shifts in exactly this
      -- case.
      v_approved:='[]'::jsonb;
      v_total:=0;
    end if;
  else
    -- No committed head for this family.  Nothing has been approved through the
    -- entitlement route, so there is nothing to tell the Candidate.  The
    -- source-derived producer is NOT consulted: reading it here is precisely
    -- what produced the defect.
    return pg_catalog.jsonb_build_object(
      'ok',false,'reason','NO_APPROVED_ENTITLEMENT');
  end if;

  return pg_catalog.jsonb_build_object(
    'ok',true,'reason',null,
    'candidate_id',(v_context->>'candidate_id')::uuid,
    'timesheet_id',p_timesheet_id,
    'approved_hours_source',v_source,
    'approved_hours_record_id',v_head.id,
    'template_params',pg_catalog.jsonb_build_object(
      'week_ending_date',v_context->>'week_ending_date',
      'approved_hours_total',v_total,
      'approved_hours',v_approved),
    'deep_link',pg_catalog.jsonb_build_object(
      'destination','TIMESHEET_DETAIL',
      'timesheet_id',p_timesheet_id));
end;
$function$;

-- The head's own rows, used only where WP-11d's entitlement reader is not
-- installed.  Date and hours, because that is what a head component carries.
create or replace function private.weekly_source_candidate_hours_push_head_rows_v1(
  p_head_id uuid
) returns jsonb
language sql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
    select coalesce(pg_catalog.jsonb_agg(row_value order by ordering),'[]'::jsonb)
    from (
      select component.component_ordinal as ordering,
             (coalesce(component.hours_day,0)+coalesce(component.hours_night,0)
              +coalesce(component.hours_sat,0)+coalesce(component.hours_sun,0)
              +coalesce(component.hours_bh,0)) as hours_value,
             pg_catalog.jsonb_build_object(
               'row_key','approved-'||component.component_id::text,
               'date',pg_catalog.to_char(component.work_date,'YYYY-MM-DD'),
               'hours',(coalesce(component.hours_day,0)+coalesce(component.hours_night,0)
                        +coalesce(component.hours_sat,0)+coalesce(component.hours_sun,0)
                        +coalesce(component.hours_bh,0))) as row_value
      from public.weekly_source_entitlement_head_components component
      where component.head_id=p_head_id
        and component.exclude_from_pay is not true
        and component.work_date is not null
        and (coalesce(component.hours_day,0)+coalesce(component.hours_night,0)
             +coalesce(component.hours_sat,0)+coalesce(component.hours_sun,0)
             +coalesce(component.hours_bh,0))>0
    ) as approved_rows;
$function$;

-- 7.2 The push itself, through the EXISTING boundary.
--
-- Idempotency is the boundary's own `dedupe_key`, keyed on the Timesheet and a
-- digest of `template_params`.  WP-14b F1: `template_params` is now the
-- HEAD-BASED payload, so the key is a digest of WHAT THE CANDIDATE IS ACTUALLY
-- BEING TOLD rather than of the source behind it.  Two consequences, both
-- wanted: the same approved hours still never push twice (the digest is equal),
-- and an approved-hours change ALWAYS makes a new key even when the source did
-- not move - which is exactly the case where the old key went silent.  That is
-- why this owner needs no store of its own.
create or replace function private.weekly_source_candidate_hours_push_v1(
  p_timesheet_id uuid
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_payload jsonb;
  v_safe jsonb;
  v_account uuid;
  v_link_count integer;
  v_told jsonb;
  v_digest text;
  v_dedupe text;
  v_result jsonb;
  v_complete jsonb;
  v_approved_export jsonb;
begin
  v_payload:=private.weekly_source_candidate_hours_push_payload_v1(p_timesheet_id);
  if coalesce((v_payload->>'ok')::boolean,false) is not true then
    return pg_catalog.jsonb_build_object(
      'ok',true,'pushed',false,'reason',v_payload->>'reason');
  end if;

  -- WP-14b F1, the reconciliation the reviewer asked for: before the boundary
  -- is called, the hours the Candidate would be told are checked against the
  -- committed head as the EXPORT reads it - two independently written readers
  -- of the same entitlement.  If they disagree, nothing is told to anybody: the
  -- refusal is raised so the caller's existing withhold-and-audit path records
  -- it, and the entitlement publication is untouched.
  if v_payload->>'approved_hours_source'='COMMITTED_ENTITLEMENT_HEAD' then
    v_approved_export:=private.weekly_source_export_approved_hours_v1(p_timesheet_id);
    if coalesce(v_approved_export->>'state','')<>'AVAILABLE'
       or coalesce(v_approved_export->>'head_id','')
            is distinct from coalesce(v_payload->>'approved_hours_record_id','')
       or coalesce((v_approved_export->>'total_hours')::numeric,-1)
            is distinct from coalesce(
              (v_payload#>>'{template_params,approved_hours_total}')::numeric,-2) then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_PUSH_HOURS_NOT_RECONCILED'
        using errcode='55000',
              detail='the payload does not equal the committed entitlement head';
    end if;
  end if;

  -- One active Candidate App account, or no push.  The same rule the installed
  -- Weekly Source notification boundary applies.
  select pg_catalog.count(*)::integer into v_link_count
  from public.candidate_app_global_membership_links link
  join public.candidate_app_accounts account on account.id=link.account_id
  where link.candidate_id=(v_payload->>'candidate_id')::uuid
    and link.state='ACTIVE' and account.status='ACTIVE';
  if v_link_count<>1 then
    return pg_catalog.jsonb_build_object(
      'ok',true,'pushed',false,'reason','NO_SINGLE_ACTIVE_ACCOUNT');
  end if;
  select account.id into v_account
  from public.candidate_app_global_membership_links link
  join public.candidate_app_accounts account on account.id=link.account_id
  where link.candidate_id=(v_payload->>'candidate_id')::uuid
    and link.state='ACTIVE' and account.status='ACTIVE';

  -- WP-44 F3.  The key must be a digest of WHAT THE CANDIDATE IS TOLD, and
  -- `row_key` is not told: it is the internal component identity, carried so the
  -- app can key a list, and a re-decision mints a new component id for hours
  -- that have not changed.  Digesting it made an IDENTICAL republication read
  -- as "your approved hours have changed" and produced a second notice for the
  -- same week (executed: 3 -> 4 notices on an unchanged republish).
  --
  -- Only that one key is removed, and it is removed by SUBTRACTION from the
  -- real payload rather than by rebuilding a list of expected members: a member
  -- added to `template_params` later is digested automatically, so a genuine
  -- change can never be swallowed by this projection.
  v_told:=v_payload->'template_params';
  if pg_catalog.jsonb_typeof(v_told->'approved_hours')='array' then
    v_told:=pg_catalog.jsonb_set(
      v_told,'{approved_hours}',
      coalesce((
        select pg_catalog.jsonb_agg(told_row.value-'row_key' order by told_row.ordinality)
        from pg_catalog.jsonb_array_elements(v_told->'approved_hours')
          with ordinality as told_row(value,ordinality)
      ),'[]'::jsonb));
  end if;
  v_digest:=pg_catalog.encode(
    private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_APPROVED_HOURS_PUSH_V1',v_told),'hex');
  v_dedupe:='approved-hours-push:'||p_timesheet_id::text||':'||v_digest;

  -- The COMPLETE serialised payload the boundary will receive, scanned as one
  -- value.  Nothing is exempted because it was expected to be safe.
  v_complete:=pg_catalog.jsonb_build_object(
    'event_type','TIMESHEET_HOURS_UPDATED',
    'preference_category','timesheet_expense_attention',
    'template_key','approved-hours-updated-v1',
    'template_params',v_payload->'template_params',
    'deep_link',v_payload->'deep_link',
    'dedupe_key',v_dedupe);
  v_safe:=private.weekly_source_candidate_payload_safe_v1(v_complete);
  if coalesce((v_safe->>'ok')::boolean,false) is not true then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_PUSH_CONTENT_FORBIDDEN'
      using errcode='55000',
            detail=coalesce(v_safe->>'reason','')||' '||coalesce(v_safe->>'offending_atom','');
  end if;

  v_result:=private._candidate_notification_insert_v1(
    v_account,
    (v_payload->>'candidate_id')::uuid,
    null,
    p_timesheet_id,
    v_complete->>'event_type',
    v_complete->>'preference_category',
    v_complete->>'template_key',
    v_complete->'template_params',
    v_complete->'deep_link',
    v_dedupe);

  return pg_catalog.jsonb_build_object(
    'ok',true,'pushed',true,'reason',null,
    'notification_id',v_result->'notification_id',
    'dedupe_key',v_dedupe,
    'payload_scan',v_safe);
end;
$function$;

-- 7.3 When approved hours change.
--
-- The approved hours of a Weekly Source week change exactly when a head becomes
-- the committed current one.  That is the trigger, so the push follows the real
-- publication rather than a caller's opinion that something changed.
--
-- A notification must never be able to roll back an entitlement publication.
-- The push is therefore made inside its own block: a refusal, a forbidden
-- payload or any other failure withholds the notification and records WHY
-- through the existing audit owner, and the publication stands.  That is the
-- correct direction of fail-closed here: no message is sent, and no money
-- decision is undone by a message.
create or replace function private.weekly_source_candidate_hours_push_head_v1()
returns trigger
language plpgsql
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_error text;
  v_after jsonb;
  v_result jsonb;
begin
  if new.state<>'COMMITTED_CURRENT' then
    return null;
  end if;
  if tg_op='UPDATE' and old.state is not distinct from 'COMMITTED_CURRENT' then
    return null;
  end if;
  begin
    v_result:=private.weekly_source_candidate_hours_push_v1(new.root_timesheet_id);
    -- WP-14b.  A push that is REFUSED rather than raised used to leave no trace
    -- at all: the entitlement was published, the worker was not told, and
    -- nothing said why.  The two reasons that mean "there IS an approved
    -- entitlement here and it could not be described" are recorded, so the gap
    -- is visible.  The ordinary reasons - no app account, nothing approved, not
    -- a Weekly Source week - are not, because they are not withholdings and
    -- would put a row on every publication for every Candidate without the app.
    if coalesce((v_result->>'pushed')::boolean,false) is not true
       and v_result->>'reason' in ('APPROVED_ENTITLEMENT_UNAVAILABLE',
                                   'APPROVED_ENTITLEMENT_AMBIGUOUS') then
      v_after:=pg_catalog.jsonb_build_object(
        'head_id',new.id,
        'root_timesheet_id',new.root_timesheet_id,
        'sqlstate',null,
        'withheld_reason',v_result->>'reason',
        'narrative','The candidate was not told about the updated hours, and this '
          ||'week''s approved hours are unchanged by that.');
      perform public._audit_insert(
        'timesheets',new.root_timesheet_id::text,
        'WEEKLY_SOURCE_CANDIDATE_HOURS_PUSH_WITHHELD',
        null,v_after,'WEEKLY_SOURCE_CANDIDATE_PUSH',null);
    end if;
  exception when others then
    get stacked diagnostics v_error=returned_sqlstate;
    v_after:=pg_catalog.jsonb_build_object(
      'head_id',new.id,
      'root_timesheet_id',new.root_timesheet_id,
      'sqlstate',v_error,
      'withheld_reason','PUSH_BOUNDARY_FAILED',
      'narrative','The candidate was not told about the updated hours, and this '
        ||'week''s approved hours are unchanged by that.');
    perform public._audit_insert(
      'timesheets',new.root_timesheet_id::text,
      'WEEKLY_SOURCE_CANDIDATE_HOURS_PUSH_WITHHELD',
      null,v_after,'WEEKLY_SOURCE_CANDIDATE_PUSH',null);
  end;
  return null;
end;
$function$;

drop trigger if exists weekly_source_candidate_hours_push_head
  on public.weekly_source_entitlement_heads;
create trigger weekly_source_candidate_hours_push_head
after insert or update on public.weekly_source_entitlement_heads
for each row execute function private.weekly_source_candidate_hours_push_head_v1();

create or replace function public.weekly_source_candidate_hours_push_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_timesheet uuid;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
               where key not in ('timesheet_id')) then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_PUSH_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_timesheet:=(p_request->>'timesheet_id')::uuid;
  exception when others then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_PUSH_REQUEST_INVALID' using errcode='22023';
  end;
  if v_timesheet is null then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_PUSH_REQUEST_INVALID' using errcode='22023';
  end if;
  return private.weekly_source_candidate_hours_push_v1(v_timesheet);
end;
$function$;

-- ===========================================================================
-- 8. The notification route contract
--
-- Read-only.  It inspects the INSTALLED definitions, not this file's source,
-- and returns a verdict rather than raising, so a verifier can report which
-- clause failed.
-- ===========================================================================
create or replace function private.weekly_source_notification_route_contract_v1()
returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_cohort_gate boolean;
  v_review_gate boolean;
  v_office_store_separate boolean;
  v_banking_store_separate boolean;
  v_failures text[]:=array[]::text[];
begin
  -- 1. Every manager cohort, route, generation and grouped render begins at the
  --    cohort owner, and that owner refuses anything but the source-authority
  --    CHECK_ONLY route.
  select pg_catalog.pg_get_functiondef(p.oid) like '%SOURCE\_AUTHORITY%'
     and pg_catalog.pg_get_functiondef(p.oid) like '%WEEKLY\_SOURCE\_SECURE\_QUERY\_NOT\_APPLICABLE%'
    into v_cohort_gate
  from pg_catalog.pg_proc p
  join pg_catalog.pg_namespace n on n.oid=p.pronamespace
  where n.nspname='private' and p.proname='weekly_source_query_cohort_ensure_v1';
  if coalesce(v_cohort_gate,false) is not true then
    v_failures:=v_failures||'MANAGER_COHORT_ROUTE_GATE_ABSENT'::text;
  end if;

  -- 2. The secure response owner refuses a batch that is not on that route.
  select pg_catalog.count(*)>0 into v_review_gate
  from pg_catalog.pg_proc p
  join pg_catalog.pg_namespace n on n.oid=p.pronamespace
  where n.nspname='public'
    and p.proname in ('weekly_source_manager_review_get_v1',
                      'weekly_source_manager_review_respond_atomic_v1');
  if coalesce(v_review_gate,false) is not true then
    v_failures:=v_failures||'SECURE_RESPONSE_OWNER_ABSENT'::text;
  end if;

  -- 3. Office Weekly source notices use their own store and never a Banking
  --    alert relation.
  select not exists(
    select 1
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on n.oid=p.pronamespace
    where p.proname like 'weekly\_source%'
      and n.nspname in ('public','private')
      and pg_catalog.pg_get_functiondef(p.oid) like '%banking\_alert%')
    into v_office_store_separate;
  if coalesce(v_office_store_separate,false) is not true then
    v_failures:=v_failures||'WEEKLY_SOURCE_TOUCHES_BANKING_ALERTS'::text;
  end if;

  -- 4. And no Banking alert owner reads the Weekly source notice store.
  select not exists(
    select 1
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on n.oid=p.pronamespace
    where p.proname like 'banking\_alert%'
      and n.nspname in ('public','private')
      and pg_catalog.pg_get_functiondef(p.oid) like '%office\_action\_notifications%')
    into v_banking_store_separate;
  if coalesce(v_banking_store_separate,false) is not true then
    v_failures:=v_failures||'BANKING_ALERTS_READ_WEEKLY_SOURCE_NOTICES'::text;
  end if;

  return pg_catalog.jsonb_build_object(
    'ok',pg_catalog.cardinality(v_failures)=0,
    'failures',pg_catalog.to_jsonb(v_failures));
end;
$function$;

-- ===========================================================================
-- 9. Ownership, privileges and comments
-- ===========================================================================
alter function private.weekly_source_candidate_forbidden_words_v1() owner to postgres;
alter function private.weekly_source_candidate_forbidden_key_parts_v1() owner to postgres;
alter function private.weekly_source_jsonb_atoms_v1(jsonb) owner to postgres;
alter function private.weekly_source_candidate_payload_safe_v1(jsonb) owner to postgres;
alter function private.weekly_source_audit_references_v1(jsonb,jsonb) owner to postgres;
alter function private.weekly_source_audit_human_reason_v1(text) owner to postgres;
alter function private.weekly_source_audit_sentence_v1(text,jsonb,jsonb,text) owner to postgres;
alter function private.weekly_source_audit_family_v1(uuid) owner to postgres;
alter function private.weekly_source_audit_key_v1(uuid) owner to postgres;
alter function private.weekly_source_audit_first_authorisation_v1() owner to postgres;
alter function private.weekly_source_audit_entitlement_head_v1() owner to postgres;
alter function private.weekly_source_audit_pending_bundle_v1() owner to postgres;
alter function private.weekly_source_audit_guard_refusal_v1(uuid,jsonb,text,uuid) owner to postgres;
alter function private.weekly_source_guard_refusal_bases_v1() owner to postgres;
alter function private.weekly_source_guard_refusal_basis_clause_v1(text) owner to postgres;
alter function private.weekly_source_guard_refusal_entry_point_installed_v1(text) owner to postgres;
alter function private.weekly_source_guard_refusal_detail_v1(jsonb) owner to postgres;
alter function private.weekly_source_guard_refusal_record_v1(text,jsonb,text,uuid) owner to postgres;
alter function private.weekly_source_audit_lifecycle_rank_v1(text) owner to postgres;
alter function private.weekly_source_audit_chronology_v1(uuid) owner to postgres;
alter function private.weekly_source_export_submitted_hours_v1(uuid) owner to postgres;
alter function private.weekly_source_export_source_hours_v1(uuid) owner to postgres;
alter function private.weekly_source_export_approved_hours_v1(uuid) owner to postgres;
alter function private.weekly_source_export_invoice_movements_v1(uuid) owner to postgres;
alter function private.weekly_source_export_hours_v1(uuid) owner to postgres;
alter function private.weekly_source_candidate_hours_push_payload_v1(uuid) owner to postgres;
alter function private.weekly_source_candidate_hours_push_head_rows_v1(uuid) owner to postgres;
alter function private.weekly_source_candidate_hours_push_v1(uuid) owner to postgres;
alter function private.weekly_source_candidate_hours_push_head_v1() owner to postgres;
alter function private.weekly_source_notification_route_contract_v1() owner to postgres;
alter function public.weekly_source_audit_guard_refusal_record_v1(jsonb) owner to postgres;
alter function public.weekly_source_guard_refusal_record_after_rollback_v1(jsonb) owner to postgres;
alter function public.weekly_source_timesheet_audit_chronology_v1(jsonb) owner to postgres;
alter function public.weekly_source_timesheet_hours_export_v1(jsonb) owner to postgres;
alter function public.weekly_source_invoice_report_rows_v1(jsonb) owner to postgres;
alter function public.weekly_source_candidate_hours_push_v1(jsonb) owner to postgres;

revoke all on function private.weekly_source_candidate_forbidden_words_v1()
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_candidate_forbidden_key_parts_v1()
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_jsonb_atoms_v1(jsonb)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_candidate_payload_safe_v1(jsonb)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_audit_references_v1(jsonb,jsonb)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_audit_human_reason_v1(text)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_audit_sentence_v1(text,jsonb,jsonb,text)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_audit_family_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_audit_key_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_audit_first_authorisation_v1()
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_audit_entitlement_head_v1()
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_audit_pending_bundle_v1()
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_audit_guard_refusal_v1(uuid,jsonb,text,uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_guard_refusal_bases_v1()
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_guard_refusal_basis_clause_v1(text)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_guard_refusal_entry_point_installed_v1(text)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_guard_refusal_detail_v1(jsonb)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_guard_refusal_record_v1(text,jsonb,text,uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_audit_lifecycle_rank_v1(text)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_audit_chronology_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_export_submitted_hours_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_export_source_hours_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_export_approved_hours_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_export_invoice_movements_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_export_hours_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_candidate_hours_push_payload_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_candidate_hours_push_head_rows_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_candidate_hours_push_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_candidate_hours_push_head_v1()
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_notification_route_contract_v1()
  from public,anon,authenticated,service_role;

revoke all on function public.weekly_source_audit_guard_refusal_record_v1(jsonb)
  from public,anon,authenticated;
grant execute on function public.weekly_source_audit_guard_refusal_record_v1(jsonb) to service_role;
revoke all on function public.weekly_source_guard_refusal_record_after_rollback_v1(jsonb)
  from public,anon,authenticated;
grant execute on function public.weekly_source_guard_refusal_record_after_rollback_v1(jsonb) to service_role;
revoke all on function public.weekly_source_timesheet_audit_chronology_v1(jsonb)
  from public,anon,authenticated;
grant execute on function public.weekly_source_timesheet_audit_chronology_v1(jsonb) to service_role;
revoke all on function public.weekly_source_timesheet_hours_export_v1(jsonb)
  from public,anon,authenticated;
grant execute on function public.weekly_source_timesheet_hours_export_v1(jsonb) to service_role;
revoke all on function public.weekly_source_invoice_report_rows_v1(jsonb)
  from public,anon,authenticated;
grant execute on function public.weekly_source_invoice_report_rows_v1(jsonb) to service_role;
revoke all on function public.weekly_source_candidate_hours_push_v1(jsonb)
  from public,anon,authenticated;
grant execute on function public.weekly_source_candidate_hours_push_v1(jsonb) to service_role;

comment on function public.weekly_source_timesheet_audit_chronology_v1(jsonb) is
  'Gate 11 (24 section 18). The plain-English, chronological Timesheet Audit for a Weekly Source root and every physical member of its family. Each event carries its own sentence, the old and new source reference and cycle, and the Office decision and reason where one applies, so Office never reads raw JSON or an internal action code. Read-only; service-only.';
comment on function public.weekly_source_timesheet_hours_export_v1(jsonb) is
  'Gate 11 (24 section 18; XSG-029). The four separated export facts for a Weekly Source week: submitted (the Candidate evidence only, empty when there is no submission), source, approved (the one committed entitlement head) and paid, plus the invoice movements kept apart from all of them. Paid hours come only from private.weekly_source_settlement_allocation_v1; there is no currency-to-hours calculation anywhere here and the timesheet_pay_state last-settled cache is never read. Read-only; service-only.';
comment on function public.weekly_source_invoice_report_rows_v1(jsonb) is
  'Gate 11 / FTI-024. Additive invoice-report truth for Weekly Source invoices. It returns immutable signed source movements, their original cycle identities and backing-report numbers for the requested invoice ids. It never reads Candidate-submitted hours and never derives hours from money. Read-only; service-only.';
comment on function public.weekly_source_candidate_hours_push_v1(jsonb) is
  'Gate 11. The hours-only Candidate push for a Weekly Source week, made through the existing push boundary private._candidate_notification_insert_v1. The complete serialised payload is scanned before the boundary is called and any forbidden field or word fails closed with WEEKLY_SOURCE_CANDIDATE_PUSH_CONTENT_FORBIDDEN. Service-only.';
comment on function private.weekly_source_audit_entitlement_head_v1() is
  'Gate 11. Writes the head publication events through the existing audit owner public._audit_insert. Immediate and deferred are distinguished from the released pending bundle for the same decision bundle, a fact in the database, never a caller flag.';
comment on function public.weekly_source_guard_refusal_record_after_rollback_v1(jsonb) is
  'Gate 11 / HANDOVER 2 round-5 ruling A2, decision D13. Records a managed-root guard refusal that RAISED, in a SEPARATE transaction, after the refused transaction has rolled back, carrying the correlation identity of the attempt. Called only by a durable caller outside the database that caught the refusal; it refuses with WEEKLY_SOURCE_GUARD_REFUSAL_RECORD_NOT_A_SEPARATE_TRANSACTION if the calling transaction has written anything, so it can never add a write to a refused transaction. The refusal structure is taken from the guard DETAIL, never re-derived. Service-only.';
comment on function private.weekly_source_candidate_payload_safe_v1(jsonb) is
  'Gate 11 / NAI-MYT-002. Fail-closed scan of a COMPLETE serialised Candidate payload: every key and every string scalar at every depth, against the four forbidden words and the forbidden money, payment, recovery and remittance field families.';

notify pgrst, 'reload schema';

commit;

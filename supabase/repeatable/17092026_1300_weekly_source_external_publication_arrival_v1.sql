-- Repeatable CloudTMS authority: weekly_source_external_publication_arrival_v1
--
-- Package WP-06d.  What happens when an EXTERNAL publication arrives after a
-- CloudTMS entitlement head already exists.
--
-- Authority, read word for word before this file was written:
--   ..\plan6-pack-audit-20260916\HANDOVER2_IMPLEMENTATION_RULINGS_RESPONSE_R5.md
--   Part E, "External C1 publication after a CloudTMS head exists":
--
--     "The current CloudTMS head remains authoritative until a successor is
--      positively accepted.  An external publication with the exact same source
--      identity, generation and digest is idempotent evidence/readback.  A
--      different publication becomes a pending successor input and cannot
--      replace, mutate or release the current head automatically.  It must pass
--      the normal proposal/authorisation/publication path.  Unknown or
--      contradictory identity fails closed."
--
--   Part D, ruling OR-1:
--
--     "It is a CloudTMS-owned relation/adapter that consumes the sealed C1
--      source identity and receipt contract.  It is not a modification of C1 and
--      must not become a second source authority.  CloudTMS owns current-head
--      publication and Workbench consumption; C1 owns the source facts and
--      handoff evidence."
--
--   P:\24_CROSS_SYSTEM_SOURCE_PAY_INVOICE_AMENDMENT_AUTHORITY.md section 4.3
--   ("one root has at most one committed current head across both authority
--   kinds"; "the Workbench reads the common current-head interface").
--   IMPL\reports\WP-06_DESIGN.md section 4.2, the precise question this file
--   answers, and IMPL\handoffs\WP-06_NEEDS.md.
--
-- ===========================================================================
-- THE DEFINITION OF "IDENTICAL", which is the whole of the rule
-- ===========================================================================
-- An arrival declares TEN facts in three named groups, matching the ruling's
-- three words.  All ten are mandatory; none has a default; none is inferred.
--
--   source identity : declared_root_timesheet_id
--                     declared_root_family_booking_id
--                     declared_final_revision_id
--                     declared_source_cycle_id
--   generation      : declared_root_timesheet_version
--                     declared_source_revision_number
--                     declared_head_revision
--   digest          : declared_source_generation_sha256
--                     declared_entitlement_sha256
--                     declared_publication_receipt_sha256
--
-- The arrival is IDENTICAL if and only if, against the root's ONE committed
-- current head, every one of the following SEVEN comparisons holds at once:
--
--   1. declared_root_timesheet_id            = head.root_timesheet_id
--   2. btrim(declared_root_family_booking_id)= btrim(head.root_family_booking_id)
--   3. declared_root_timesheet_version       = head.root_timesheet_version
--   4. the source-generation digest REBUILT from CloudTMS's own
--      weekly_source_final_revisions row for the declared final revision
--                                            = head.source_generation_digest
--   5. declared_entitlement_sha256           = head.entitlement_digest
--   6. declared_publication_receipt_sha256   = head.publication_receipt_digest
--   7. declared_head_revision                = head.head_revision
--
-- ANY PARTIAL MATCH IS NOT IDENTICAL.  There is no tolerance, no "near enough",
-- no most-recent-wins and no field that may be skipped when absent.
--
-- Comparison 4 is the reason this owner cannot be fooled by an echo.  The
-- arrival does not get to assert its own source-generation digest and have it
-- believed: the owner looks the declared final revision up in CloudTMS's own
-- source relation, rebuilds {final_revision_id, source_cycle_id,
-- revision_number, manifest_hash, policy_fingerprint} through the SAME encoder
-- the publication coordinator used (H2-032), and refuses as CONTRADICTORY if
-- the arrival's declared digest disagrees with CloudTMS's own source facts.
-- The digest is therefore independently verified, not merely compared.
--
-- ===========================================================================
-- THE THREE OUTCOMES, distinguishable by NAME in what the owner returns
-- ===========================================================================
--   EXTERNAL_PUBLICATION_IDENTICAL          ok=true.  Idempotent readback.
--                                           Writes NOTHING.  Returns the
--                                           EXISTING publication receipt.
--   EXTERNAL_PUBLICATION_PENDING_SUCCESSOR  ok=true.  Appends one append-only
--                                           pending successor INPUT row.  The
--                                           current head is not replaced, not
--                                           mutated and not released.  The
--                                           return says so in three explicit
--                                           fields and names the ordinary path
--                                           the successor must take.
--   EXTERNAL_PUBLICATION_REFUSED            ok=false.  Fails closed with a
--                                           named reason.  Writes NOTHING.
--
-- The refusal reasons are:
--   WEEKLY_SOURCE_EXTERNAL_PUBLICATION_IDENTITY_UNKNOWN
--       the arrival cannot be bound to exactly one root with exactly one
--       committed current head and one publication receipt.
--   WEEKLY_SOURCE_EXTERNAL_PUBLICATION_IDENTITY_CONTRADICTORY
--       the arrival's own declared facts disagree with each other, with
--       CloudTMS's immutable source facts, or with the generation it claims.
--   WEEKLY_SOURCE_EXTERNAL_PUBLICATION_GENERATION_NOT_ADVANCING
--       a stale readback: the arrival declares a generation older than the
--       current head.  Round 5 Part E, freeze-census readings: "a stale
--       revision is superseded only by a positively identified newer
--       authoritative revision, otherwise it blocks".
--
-- Why "same generation, different content" is CONTRADICTORY and not a pending
-- successor: an arrival that declares the CURRENT head revision is asserting
-- that it IS the current head.  If its content digests differ, the two
-- assertions cannot both be true, and the ruling sends contradictory identity
-- to the fail-closed branch.  Treating it as a successor would let an external
-- system supply a second entitlement for a generation that already exists,
-- which is precisely the second source authority OR-1 forbids.
--
-- ===========================================================================
-- WHAT THIS OWNER NEVER DOES
-- ===========================================================================
-- It never inserts, updates or deletes a row in
-- public.weekly_source_entitlement_heads or
-- public.weekly_source_entitlement_head_components; never writes a decision
-- bundle, a pending bundle, a publication receipt, a root authorisation or a
-- scope-change token; never calls the publication coordinator, the Workbench
-- invalidator, the enqueue or the serial gate; never writes a pay, Draft,
-- reservation, provider, execution, cancellation, settlement, recovery or
-- remittance row; and never reads a C1 staging or checkpoint relation as
-- authority.  Its ONLY write, on its ONLY writing branch, is one append-only
-- row in private.weekly_source_external_publication_arrivals, a relation that
-- holds no figure of any kind.
--
-- LOCKS.  Exactly one: `for share` on the single committed current head row,
-- taken after the head has been resolved by an explicit cardinality check and
-- held only so the classification and the appended row cannot disagree with the
-- head.  `for share` cannot mutate.  No second lock is taken anywhere, so this
-- owner cannot deadlock against the coordinator's canonical lock order, and it
-- holds no lock on any Banking Pay relation.

\set ON_ERROR_STOP on

begin;

-- ---------------------------------------------------------------------------
-- 1. The declared-arrival reader.
--
-- Normalises the ten declared facts through the ONE installed scalar owner the
-- publication coordinator uses, so an arrival and a publication request are
-- validated by the same rules and canonicalised the same way.  That owner
-- raises on a bad field; this wrapper converts the raise into a returned
-- refusal, because the ruling's fail-closed branch must be a named answer the
-- caller can read and not an exception that loses the reason.
--
-- An absent key, a JSON null, a wrong JSON type and a wrong shape are all the
-- unsafe value and all refuse (Part 1 review rule 4).  Unknown keys refuse:
-- an identity comparison that silently ignores a field it does not recognise
-- is not an identity comparison.
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_external_publication_declared_v1(
  p_request jsonb
) returns jsonb
language plpgsql
stable
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_top_keys constant text[]:=array[
    'schema_version','external_system','source_identity','generation','digest']::text[];
  v_identity_keys constant text[]:=array[
    'root_timesheet_id','root_family_booking_id','final_revision_id','source_cycle_id']::text[];
  v_generation_keys constant text[]:=array[
    'root_timesheet_version','source_revision_number','head_revision']::text[];
  v_digest_keys constant text[]:=array[
    'source_generation_sha256','entitlement_sha256','publication_receipt_sha256']::text[];
  v_identity jsonb;
  v_generation jsonb;
  v_digest jsonb;
  v_declared jsonb;
  v_scalar_detail text;
begin
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object' then
    return pg_catalog.jsonb_build_object(
      'ok',false,'field','request','reason','EXPECTED_OBJECT');
  end if;
  if (select pg_catalog.array_agg(key order by key)
        from pg_catalog.jsonb_object_keys(p_request) key)
     is distinct from (select pg_catalog.array_agg(key order by key)
                         from pg_catalog.unnest(v_top_keys) key) then
    return pg_catalog.jsonb_build_object(
      'ok',false,'field','request','reason','KEY_SET_NOT_EXACT');
  end if;
  if p_request->>'schema_version'
     is distinct from 'WEEKLY_SOURCE_EXTERNAL_PUBLICATION_ARRIVAL_V1' then
    return pg_catalog.jsonb_build_object(
      'ok',false,'field','schema_version','reason','UNKNOWN_SCHEMA_VERSION');
  end if;

  v_identity:=p_request->'source_identity';
  v_generation:=p_request->'generation';
  v_digest:=p_request->'digest';
  if v_identity is null or pg_catalog.jsonb_typeof(v_identity)<>'object'
     or (select pg_catalog.array_agg(key order by key)
           from pg_catalog.jsonb_object_keys(v_identity) key)
        is distinct from (select pg_catalog.array_agg(key order by key)
                            from pg_catalog.unnest(v_identity_keys) key) then
    return pg_catalog.jsonb_build_object(
      'ok',false,'field','source_identity','reason','KEY_SET_NOT_EXACT');
  end if;
  if v_generation is null or pg_catalog.jsonb_typeof(v_generation)<>'object'
     or (select pg_catalog.array_agg(key order by key)
           from pg_catalog.jsonb_object_keys(v_generation) key)
        is distinct from (select pg_catalog.array_agg(key order by key)
                            from pg_catalog.unnest(v_generation_keys) key) then
    return pg_catalog.jsonb_build_object(
      'ok',false,'field','generation','reason','KEY_SET_NOT_EXACT');
  end if;
  if v_digest is null or pg_catalog.jsonb_typeof(v_digest)<>'object'
     or (select pg_catalog.array_agg(key order by key)
           from pg_catalog.jsonb_object_keys(v_digest) key)
        is distinct from (select pg_catalog.array_agg(key order by key)
                            from pg_catalog.unnest(v_digest_keys) key) then
    return pg_catalog.jsonb_build_object(
      'ok',false,'field','digest','reason','KEY_SET_NOT_EXACT');
  end if;

  begin
    v_declared:=pg_catalog.jsonb_build_object(
      'external_system',
        private.weekly_source_publication_scalar_v1(
          p_request->'external_system','external_system','TEXT'),
      'root_timesheet_id',
        private.weekly_source_publication_scalar_v1(
          v_identity->'root_timesheet_id','source_identity.root_timesheet_id','UUID'),
      'root_family_booking_id',
        private.weekly_source_publication_scalar_v1(
          v_identity->'root_family_booking_id','source_identity.root_family_booking_id','RAWTEXT'),
      'final_revision_id',
        private.weekly_source_publication_scalar_v1(
          v_identity->'final_revision_id','source_identity.final_revision_id','UUID'),
      'source_cycle_id',
        private.weekly_source_publication_scalar_v1(
          v_identity->'source_cycle_id','source_identity.source_cycle_id','UUID'),
      'root_timesheet_version',
        private.weekly_source_publication_scalar_v1(
          v_generation->'root_timesheet_version','generation.root_timesheet_version','INT'),
      'source_revision_number',
        private.weekly_source_publication_scalar_v1(
          v_generation->'source_revision_number','generation.source_revision_number','INT'),
      'head_revision',
        private.weekly_source_publication_scalar_v1(
          v_generation->'head_revision','generation.head_revision','INT'),
      'source_generation_sha256',
        private.weekly_source_publication_scalar_v1(
          v_digest->'source_generation_sha256','digest.source_generation_sha256','HEX32'),
      'entitlement_sha256',
        private.weekly_source_publication_scalar_v1(
          v_digest->'entitlement_sha256','digest.entitlement_sha256','HEX32'),
      'publication_receipt_sha256',
        private.weekly_source_publication_scalar_v1(
          v_digest->'publication_receipt_sha256','digest.publication_receipt_sha256','HEX32'));
  exception when sqlstate '22023' then
    get stacked diagnostics v_scalar_detail=pg_exception_detail;
    return pg_catalog.jsonb_build_object(
      'ok',false,'field','declared_fact','reason','FIELD_INVALID',
      'detail',nullif(pg_catalog.btrim(coalesce(v_scalar_detail,'')),''));
  end;

  -- Bounds that the scalar owner cannot express.  A non-positive version,
  -- revision number or head revision is not a generation.
  if (v_declared->>'root_timesheet_version')::numeric<1
     or (v_declared->>'source_revision_number')::numeric<1
     or (v_declared->>'head_revision')::numeric<1 then
    return pg_catalog.jsonb_build_object(
      'ok',false,'field','generation','reason','NON_POSITIVE_GENERATION');
  end if;
  if pg_catalog.char_length(v_declared->>'external_system') not between 1 and 64 then
    return pg_catalog.jsonb_build_object(
      'ok',false,'field','external_system','reason','LENGTH_OUT_OF_RANGE');
  end if;
  if pg_catalog.char_length(v_declared->>'root_family_booking_id') not between 1 and 200 then
    return pg_catalog.jsonb_build_object(
      'ok',false,'field','source_identity.root_family_booking_id','reason','LENGTH_OUT_OF_RANGE');
  end if;

  return pg_catalog.jsonb_build_object('ok',true,'declared',v_declared);
end;
$function$;
alter function private.weekly_source_external_publication_declared_v1(jsonb) owner to postgres;
revoke all on function private.weekly_source_external_publication_declared_v1(jsonb)
  from public,anon,authenticated,service_role;

-- ---------------------------------------------------------------------------
-- 2. The owner.
-- ---------------------------------------------------------------------------
create or replace function public.weekly_source_external_publication_arrival_v1(
  p_request jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_read jsonb;
  v_declared jsonb;
  v_root_timesheet_id uuid;
  v_family_booking_id text;
  v_final_revision_id uuid;
  v_source_cycle_id uuid;
  v_root_version integer;
  v_source_revision_number integer;
  v_head_revision bigint;
  v_declared_source_generation bytea;
  v_declared_entitlement bytea;
  v_declared_receipt bytea;
  v_timesheet_count integer;
  v_live_booking_id text;
  v_live_version integer;
  v_head_count integer;
  v_head public.weekly_source_entitlement_heads%rowtype;
  v_revision public.weekly_source_final_revisions%rowtype;
  v_revision_count integer;
  v_rebuilt_source_generation bytea;
  v_same_root boolean;
  v_same_family boolean;
  v_same_root_version boolean;
  v_same_source_generation boolean;
  v_same_entitlement boolean;
  v_same_receipt boolean;
  v_same_head_revision boolean;
  v_identical boolean;
  v_comparison jsonb;
  v_receipt private.weekly_source_entitlement_publication_receipts%rowtype;
  v_receipt_count integer;
  v_arrival_canonical jsonb;
  v_arrival_digest bytea;
  v_arrival_id uuid;
  v_arrival_count integer;
  v_created boolean:=false;
  v_refuse constant jsonb:=pg_catalog.jsonb_build_object(
    'ok',false,'outcome','EXTERNAL_PUBLICATION_REFUSED',
    'wrote_nothing',true,'head_replaced',false,'head_mutated',false,
    'head_released',false);
begin
  if coalesce(
       pg_catalog.current_setting('request.jwt.claim.role',true),
       nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role',''
     )<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;

  -- ---- 1. the ten declared facts -----------------------------------------
  v_read:=private.weekly_source_external_publication_declared_v1(p_request);
  if coalesce((v_read->>'ok')::boolean,false) is not true then
    return v_refuse
      ||pg_catalog.jsonb_build_object(
          'reason_code','WEEKLY_SOURCE_EXTERNAL_PUBLICATION_IDENTITY_UNKNOWN',
          'detail',v_read-'ok');
  end if;
  v_declared:=v_read->'declared';
  v_root_timesheet_id:=(v_declared->>'root_timesheet_id')::uuid;
  v_family_booking_id:=v_declared->>'root_family_booking_id';
  v_final_revision_id:=(v_declared->>'final_revision_id')::uuid;
  v_source_cycle_id:=(v_declared->>'source_cycle_id')::uuid;
  v_root_version:=(v_declared->>'root_timesheet_version')::integer;
  v_source_revision_number:=(v_declared->>'source_revision_number')::integer;
  v_head_revision:=(v_declared->>'head_revision')::bigint;
  v_declared_source_generation:=pg_catalog.decode(v_declared->>'source_generation_sha256','hex');
  v_declared_entitlement:=pg_catalog.decode(v_declared->>'entitlement_sha256','hex');
  v_declared_receipt:=pg_catalog.decode(v_declared->>'publication_receipt_sha256','hex');

  -- ---- 2. bind the arrival to exactly one physical root -------------------
  select pg_catalog.count(*)::integer into v_timesheet_count
    from public.timesheets as timesheet_row
   where timesheet_row.timesheet_id=v_root_timesheet_id;
  if v_timesheet_count<>1 then
    return v_refuse
      ||pg_catalog.jsonb_build_object(
          'reason_code','WEEKLY_SOURCE_EXTERNAL_PUBLICATION_IDENTITY_UNKNOWN',
          'detail',pg_catalog.jsonb_build_object(
            'reason','ROOT_TIMESHEET_NOT_FOUND',
            'root_timesheet_id',v_root_timesheet_id,
            'matches',v_timesheet_count));
  end if;
  select timesheet_row.booking_id,timesheet_row.version
    into v_live_booking_id,v_live_version
    from public.timesheets as timesheet_row
   where timesheet_row.timesheet_id=v_root_timesheet_id;
  -- The two halves of the declared source identity must name the SAME root.
  -- If they do not, the arrival is self-contradictory and no comparison of its
  -- content could mean anything.
  if pg_catalog.btrim(coalesce(v_live_booking_id,''))
     is distinct from pg_catalog.btrim(v_family_booking_id) then
    return v_refuse
      ||pg_catalog.jsonb_build_object(
          'reason_code','WEEKLY_SOURCE_EXTERNAL_PUBLICATION_IDENTITY_CONTRADICTORY',
          'detail',pg_catalog.jsonb_build_object(
            'reason','SOURCE_IDENTITY_NAMES_TWO_DIFFERENT_ROOTS',
            'root_timesheet_id',v_root_timesheet_id,
            'declared_root_family_booking_id',v_family_booking_id,
            'physical_root_family_booking_id',v_live_booking_id));
  end if;

  -- ---- 3. exactly one committed current head, by cardinality --------------
  -- Part 1 review rule 5: safety is never expressed through `limit`, `order by`
  -- or "the unique index makes this impossible".
  select pg_catalog.count(*)::integer into v_head_count
    from public.weekly_source_entitlement_heads as head_row
   where pg_catalog.btrim(head_row.root_family_booking_id)
         =pg_catalog.btrim(v_family_booking_id)
     and head_row.state='COMMITTED_CURRENT';
  if v_head_count=0 then
    return v_refuse
      ||pg_catalog.jsonb_build_object(
          'reason_code','WEEKLY_SOURCE_EXTERNAL_PUBLICATION_IDENTITY_UNKNOWN',
          'detail',pg_catalog.jsonb_build_object(
            'reason','NO_COMMITTED_CURRENT_HEAD',
            'root_family_booking_id',v_family_booking_id));
  end if;
  if v_head_count>1 then
    return v_refuse
      ||pg_catalog.jsonb_build_object(
          'reason_code','WEEKLY_SOURCE_EXTERNAL_PUBLICATION_IDENTITY_CONTRADICTORY',
          'detail',pg_catalog.jsonb_build_object(
            'reason','MULTIPLE_COMMITTED_CURRENT_HEADS',
            'root_family_booking_id',v_family_booking_id,
            'committed_current_heads',v_head_count));
  end if;
  -- The single lock this owner takes.  `for share` pins the head so the
  -- classification and any appended evidence cannot disagree with it; it can
  -- never mutate the row.
  select head_row.* into v_head
    from public.weekly_source_entitlement_heads as head_row
   where pg_catalog.btrim(head_row.root_family_booking_id)
         =pg_catalog.btrim(v_family_booking_id)
     and head_row.state='COMMITTED_CURRENT'
     for share;
  if v_head.root_timesheet_id is distinct from v_root_timesheet_id then
    return v_refuse
      ||pg_catalog.jsonb_build_object(
          'reason_code','WEEKLY_SOURCE_EXTERNAL_PUBLICATION_IDENTITY_CONTRADICTORY',
          'detail',pg_catalog.jsonb_build_object(
            'reason','CURRENT_HEAD_NAMES_A_DIFFERENT_PHYSICAL_ROOT',
            'declared_root_timesheet_id',v_root_timesheet_id,
            'current_head_root_timesheet_id',v_head.root_timesheet_id));
  end if;
  if v_head.publication_receipt_digest is null then
    return v_refuse
      ||pg_catalog.jsonb_build_object(
          'reason_code','WEEKLY_SOURCE_EXTERNAL_PUBLICATION_IDENTITY_UNKNOWN',
          'detail',pg_catalog.jsonb_build_object(
            'reason','CURRENT_HEAD_CARRIES_NO_PUBLICATION_RECEIPT',
            'current_head_id',v_head.id));
  end if;

  -- ---- 4. verify the declared source generation against CloudTMS's own ----
  --         source facts, rather than believing the arrival's own digest.
  select pg_catalog.count(*)::integer into v_revision_count
    from public.weekly_source_final_revisions as revision_row
   where revision_row.id=v_final_revision_id;
  if v_revision_count<>1 then
    return v_refuse
      ||pg_catalog.jsonb_build_object(
          'reason_code','WEEKLY_SOURCE_EXTERNAL_PUBLICATION_IDENTITY_UNKNOWN',
          'detail',pg_catalog.jsonb_build_object(
            'reason','FINAL_REVISION_NOT_FOUND',
            'final_revision_id',v_final_revision_id,
            'matches',v_revision_count));
  end if;
  select revision_row.* into v_revision
    from public.weekly_source_final_revisions as revision_row
   where revision_row.id=v_final_revision_id;
  if v_revision.source_cycle_id is distinct from v_source_cycle_id
     or v_revision.revision_number is distinct from v_source_revision_number then
    return v_refuse
      ||pg_catalog.jsonb_build_object(
          'reason_code','WEEKLY_SOURCE_EXTERNAL_PUBLICATION_IDENTITY_CONTRADICTORY',
          'detail',pg_catalog.jsonb_build_object(
            'reason','SOURCE_GENERATION_DISAGREES_WITH_SOURCE_FACTS',
            'final_revision_id',v_final_revision_id,
            'declared_source_cycle_id',v_source_cycle_id,
            'actual_source_cycle_id',v_revision.source_cycle_id,
            'declared_source_revision_number',v_source_revision_number,
            'actual_source_revision_number',v_revision.revision_number));
  end if;
  -- The SAME object, in the SAME key order, through the SAME encoder the
  -- publication coordinator used to fill head.source_generation_digest
  -- (17092026_0300_weekly_source_entitlement_publication_v1.sql, step 7).
  v_rebuilt_source_generation:=private.weekly_source_publication_request_digest_v1(
    pg_catalog.jsonb_build_object(
      'final_revision_id',pg_catalog.to_jsonb(v_revision.id::text),
      'source_cycle_id',pg_catalog.to_jsonb(v_revision.source_cycle_id::text),
      'revision_number',pg_catalog.to_jsonb(v_revision.revision_number),
      'manifest_hash',pg_catalog.to_jsonb(pg_catalog.encode(v_revision.manifest_hash,'hex')),
      'policy_fingerprint',pg_catalog.to_jsonb(
        pg_catalog.encode(v_revision.policy_fingerprint,'hex'))));
  if v_declared_source_generation is distinct from v_rebuilt_source_generation then
    return v_refuse
      ||pg_catalog.jsonb_build_object(
          'reason_code','WEEKLY_SOURCE_EXTERNAL_PUBLICATION_IDENTITY_CONTRADICTORY',
          'detail',pg_catalog.jsonb_build_object(
            'reason','DECLARED_SOURCE_GENERATION_DIGEST_DISAGREES_WITH_SOURCE_FACTS',
            'final_revision_id',v_final_revision_id,
            'declared_source_generation_sha256',
              pg_catalog.encode(v_declared_source_generation,'hex'),
            'rebuilt_source_generation_sha256',
              pg_catalog.encode(v_rebuilt_source_generation,'hex')));
  end if;

  -- ---- 5. the seven comparisons, all of them, together --------------------
  v_same_root:=(v_head.root_timesheet_id=v_root_timesheet_id);
  v_same_family:=(pg_catalog.btrim(v_head.root_family_booking_id)
                  =pg_catalog.btrim(v_family_booking_id));
  v_same_root_version:=(v_head.root_timesheet_version=v_root_version);
  v_same_source_generation:=(v_head.source_generation_digest=v_rebuilt_source_generation);
  v_same_entitlement:=(v_head.entitlement_digest=v_declared_entitlement);
  v_same_receipt:=(v_head.publication_receipt_digest=v_declared_receipt);
  v_same_head_revision:=(v_head.head_revision=v_head_revision);
  v_identical:=v_same_root and v_same_family and v_same_root_version
               and v_same_source_generation and v_same_entitlement
               and v_same_receipt and v_same_head_revision;

  v_comparison:=pg_catalog.jsonb_build_object(
    'source_identity',pg_catalog.jsonb_build_object(
      'root_timesheet_id_matches',v_same_root,
      'root_family_booking_id_matches',v_same_family),
    'generation',pg_catalog.jsonb_build_object(
      'root_timesheet_version_matches',v_same_root_version,
      'source_generation_matches',v_same_source_generation,
      'head_revision_matches',v_same_head_revision,
      'declared_head_revision',v_head_revision::text,
      'current_head_revision',v_head.head_revision::text),
    'digest',pg_catalog.jsonb_build_object(
      'source_generation_sha256_matches',v_same_source_generation,
      'entitlement_sha256_matches',v_same_entitlement,
      'publication_receipt_sha256_matches',v_same_receipt),
    'identical',v_identical);

  -- ---- 6a. IDENTICAL: idempotent evidence / readback.  Writes nothing. ----
  if v_identical then
    select pg_catalog.count(*)::integer into v_receipt_count
      from private.weekly_source_entitlement_publication_receipts as receipt_row
     where receipt_row.request_digest=v_head.publication_receipt_digest
       and receipt_row.decision_bundle_id=v_head.decision_bundle_id
       and receipt_row.bundle_revision=v_head.bundle_revision;
    if v_receipt_count<>1 then
      return v_refuse
        ||pg_catalog.jsonb_build_object(
            'reason_code','WEEKLY_SOURCE_EXTERNAL_PUBLICATION_IDENTITY_UNKNOWN',
            'detail',pg_catalog.jsonb_build_object(
              'reason','CURRENT_HEAD_PUBLICATION_RECEIPT_NOT_RESOLVABLE',
              'current_head_id',v_head.id,'matches',v_receipt_count));
    end if;
    select receipt_row.* into v_receipt
      from private.weekly_source_entitlement_publication_receipts as receipt_row
     where receipt_row.request_digest=v_head.publication_receipt_digest
       and receipt_row.decision_bundle_id=v_head.decision_bundle_id
       and receipt_row.bundle_revision=v_head.bundle_revision;
    return pg_catalog.jsonb_build_object(
      'ok',true,
      'outcome','EXTERNAL_PUBLICATION_IDENTICAL',
      'idempotent_readback',true,
      'wrote_nothing',true,
      'head_replaced',false,'head_mutated',false,'head_released',false,
      'current_head_id',v_head.id,
      'current_head_revision',v_head.head_revision::text,
      'current_head_authority_kind',v_head.authority_kind,
      'receipt',pg_catalog.jsonb_build_object(
        'publication_receipt_id',v_receipt.id,
        'request_digest',pg_catalog.encode(v_receipt.request_digest,'hex'),
        'publication_mode',v_receipt.publication_mode,
        'decision_bundle_id',v_receipt.decision_bundle_id,
        'bundle_revision',v_receipt.bundle_revision::text,
        'decision_id',v_receipt.decision_id,
        'scope_change_tx_token',v_receipt.scope_change_tx_token,
        'created_at_utc',v_receipt.created_at_utc),
      'comparison',v_comparison);
  end if;

  -- ---- 6b. NOT identical, and claiming the CURRENT generation ------------
  if v_same_head_revision then
    return v_refuse
      ||pg_catalog.jsonb_build_object(
          'reason_code','WEEKLY_SOURCE_EXTERNAL_PUBLICATION_IDENTITY_CONTRADICTORY',
          'detail',pg_catalog.jsonb_build_object(
            'reason','SAME_GENERATION_DIFFERENT_CONTENT',
            'current_head_id',v_head.id,
            'head_revision',v_head.head_revision::text),
          'comparison',v_comparison);
  end if;

  -- ---- 6c. NOT identical, and OLDER than the current generation ----------
  if v_head_revision<v_head.head_revision then
    return v_refuse
      ||pg_catalog.jsonb_build_object(
          'reason_code','WEEKLY_SOURCE_EXTERNAL_PUBLICATION_GENERATION_NOT_ADVANCING',
          'detail',pg_catalog.jsonb_build_object(
            'reason','STALE_EXTERNAL_GENERATION',
            'current_head_id',v_head.id,
            'declared_head_revision',v_head_revision::text,
            'current_head_revision',v_head.head_revision::text),
          'comparison',v_comparison);
  end if;

  -- ---- 6d. A later generation that reuses the current receipt digest -----
  -- A genuinely new publication cannot carry the receipt of the publication it
  -- claims to supersede.  This is a replay with a bumped generation number.
  if v_same_receipt then
    return v_refuse
      ||pg_catalog.jsonb_build_object(
          'reason_code','WEEKLY_SOURCE_EXTERNAL_PUBLICATION_IDENTITY_CONTRADICTORY',
          'detail',pg_catalog.jsonb_build_object(
            'reason','PUBLICATION_RECEIPT_DIGEST_REUSED_ACROSS_GENERATIONS',
            'current_head_id',v_head.id,
            'declared_head_revision',v_head_revision::text,
            'current_head_revision',v_head.head_revision::text),
          'comparison',v_comparison);
  end if;

  -- ---- 7. DIFFERENT: one append-only pending successor INPUT --------------
  -- It does not replace, mutate or release the head.  It carries no figure.
  -- It is not a proposal and not a decision: the successor must be composed,
  -- authorised and published by the ordinary path, which rebuilds the
  -- entitlement from CloudTMS's own facts and never reads this relation.
  v_arrival_canonical:=pg_catalog.jsonb_build_object(
    'schema_version','WEEKLY_SOURCE_EXTERNAL_PUBLICATION_ARRIVAL_V1',
    'declared',v_declared,
    'current_head',pg_catalog.jsonb_build_object(
      'head_id',pg_catalog.to_jsonb(v_head.id::text),
      'head_revision',pg_catalog.to_jsonb(v_head.head_revision),
      'entitlement_digest',pg_catalog.to_jsonb(
        pg_catalog.encode(v_head.entitlement_digest,'hex')),
      'publication_receipt_digest',pg_catalog.to_jsonb(
        pg_catalog.encode(v_head.publication_receipt_digest,'hex'))));
  v_arrival_digest:=private.weekly_source_publication_request_digest_v1(v_arrival_canonical);

  select pg_catalog.count(*)::integer into v_arrival_count
    from private.weekly_source_external_publication_arrivals as arrival_row
   where arrival_row.arrival_digest=v_arrival_digest;
  if v_arrival_count>1 then
    return v_refuse
      ||pg_catalog.jsonb_build_object(
          'reason_code','WEEKLY_SOURCE_EXTERNAL_PUBLICATION_IDENTITY_CONTRADICTORY',
          'detail',pg_catalog.jsonb_build_object(
            'reason','PENDING_SUCCESSOR_INPUT_NOT_SINGLE',
            'arrival_digest',pg_catalog.encode(v_arrival_digest,'hex'),
            'matches',v_arrival_count));
  end if;
  if v_arrival_count=1 then
    select arrival_row.id into v_arrival_id
      from private.weekly_source_external_publication_arrivals as arrival_row
     where arrival_row.arrival_digest=v_arrival_digest;
  else
    begin
      insert into private.weekly_source_external_publication_arrivals(
        arrival_outcome,external_system,
        declared_root_timesheet_id,declared_root_family_booking_id,
        declared_final_revision_id,declared_source_cycle_id,
        declared_root_timesheet_version,declared_source_revision_number,
        declared_head_revision,
        declared_source_generation_digest,declared_entitlement_digest,
        declared_publication_receipt_digest,
        current_head_id,current_head_revision,current_entitlement_digest,
        current_source_generation_digest,current_publication_receipt_digest,
        arrival_digest,comparison_json)
      values (
        'PENDING_SUCCESSOR_INPUT',v_declared->>'external_system',
        v_root_timesheet_id,v_family_booking_id,
        v_final_revision_id,v_source_cycle_id,
        v_root_version,v_source_revision_number,
        v_head_revision,
        v_declared_source_generation,v_declared_entitlement,
        v_declared_receipt,
        v_head.id,v_head.head_revision,v_head.entitlement_digest,
        v_head.source_generation_digest,v_head.publication_receipt_digest,
        v_arrival_digest,v_comparison)
      returning id into v_arrival_id;
      v_created:=true;
    exception when unique_violation then
      select arrival_row.id into v_arrival_id
        from private.weekly_source_external_publication_arrivals as arrival_row
       where arrival_row.arrival_digest=v_arrival_digest;
      v_created:=false;
    end;
  end if;

  return pg_catalog.jsonb_build_object(
    'ok',true,
    'outcome','EXTERNAL_PUBLICATION_PENDING_SUCCESSOR',
    'pending_successor_input_id',v_arrival_id,
    'created',v_created,
    'arrival_digest',pg_catalog.encode(v_arrival_digest,'hex'),
    'head_replaced',false,'head_mutated',false,'head_released',false,
    'current_head_authoritative',true,
    'current_head_id',v_head.id,
    'current_head_revision',v_head.head_revision::text,
    'current_head_authority_kind',v_head.authority_kind,
    'requires_ordinary_path',
      'PROPOSAL_THEN_OFFICE_AUTHORISATION_THEN_PUBLICATION_COORDINATOR',
    'comparison',v_comparison);
end;
$function$;
alter function public.weekly_source_external_publication_arrival_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_external_publication_arrival_v1(jsonb)
  from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_external_publication_arrival_v1(jsonb)
  to service_role;

-- ---------------------------------------------------------------------------
-- 3. The Office reader.
--
-- "Outstanding" is DERIVED, never stored: an appended input is still
-- outstanding while the root's current committed head has not reached the
-- generation the input declared.  Once the ordinary path publishes a successor
-- at or beyond that generation the input stops being outstanding without any
-- row being edited, so there is no state a writer could flip and no second
-- place where "accepted" could be asserted.
--
-- It returns identity, generation and digests only.  There is no figure in the
-- relation to return.
-- ---------------------------------------------------------------------------
create or replace function public.weekly_source_external_publication_pending_inputs_v1(
  p_request jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_keys constant text[]:=array['schema_version','root_timesheet_id']::text[];
  v_root_timesheet_id uuid;
  v_family_booking_id text;
  v_head_count integer;
  v_current_head_revision bigint;
  v_items jsonb;
  v_outstanding integer;
begin
  if coalesce(
       pg_catalog.current_setting('request.jwt.claim.role',true),
       nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role',''
     )<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or (select pg_catalog.array_agg(key order by key)
           from pg_catalog.jsonb_object_keys(p_request) key)
        is distinct from (select pg_catalog.array_agg(key order by key)
                            from pg_catalog.unnest(v_keys) key)
     or p_request->>'schema_version'
        is distinct from 'WEEKLY_SOURCE_EXTERNAL_PUBLICATION_PENDING_INPUTS_V1' then
    raise exception 'WEEKLY_SOURCE_EXTERNAL_PUBLICATION_PENDING_INPUTS_INVALID'
      using errcode='22023';
  end if;
  begin
    v_root_timesheet_id:=(p_request->>'root_timesheet_id')::uuid;
  exception when others then
    raise exception 'WEEKLY_SOURCE_EXTERNAL_PUBLICATION_PENDING_INPUTS_INVALID'
      using errcode='22023';
  end;
  if v_root_timesheet_id is null then
    raise exception 'WEEKLY_SOURCE_EXTERNAL_PUBLICATION_PENDING_INPUTS_INVALID'
      using errcode='22023';
  end if;

  select timesheet_row.booking_id into v_family_booking_id
    from public.timesheets as timesheet_row
   where timesheet_row.timesheet_id=v_root_timesheet_id;
  select pg_catalog.count(*)::integer into v_head_count
    from public.weekly_source_entitlement_heads as head_row
   where pg_catalog.btrim(head_row.root_family_booking_id)
         =pg_catalog.btrim(coalesce(v_family_booking_id,''))
     and head_row.state='COMMITTED_CURRENT';
  if v_head_count=1 then
    select head_row.head_revision into v_current_head_revision
      from public.weekly_source_entitlement_heads as head_row
     where pg_catalog.btrim(head_row.root_family_booking_id)
           =pg_catalog.btrim(coalesce(v_family_booking_id,''))
       and head_row.state='COMMITTED_CURRENT';
  end if;

  select coalesce(pg_catalog.jsonb_agg(
           pg_catalog.jsonb_build_object(
             'pending_successor_input_id',arrival_row.id,
             'external_system',arrival_row.external_system,
             'declared_head_revision',arrival_row.declared_head_revision::text,
             'declared_final_revision_id',arrival_row.declared_final_revision_id,
             'declared_source_revision_number',arrival_row.declared_source_revision_number,
             'declared_entitlement_sha256',
               pg_catalog.encode(arrival_row.declared_entitlement_digest,'hex'),
             'head_at_arrival_id',arrival_row.current_head_id,
             'head_at_arrival_revision',arrival_row.current_head_revision::text,
             'outstanding',
               (v_current_head_revision is null
                or arrival_row.declared_head_revision>v_current_head_revision),
             'created_at_utc',arrival_row.created_at_utc)
           order by arrival_row.declared_head_revision,arrival_row.created_at_utc,
                    arrival_row.id),'[]'::jsonb),
         pg_catalog.count(*) filter (
           where v_current_head_revision is null
              or arrival_row.declared_head_revision>v_current_head_revision)::integer
    into v_items,v_outstanding
    from private.weekly_source_external_publication_arrivals as arrival_row
   where arrival_row.declared_root_timesheet_id=v_root_timesheet_id;

  return pg_catalog.jsonb_build_object(
    'ok',true,
    'root_timesheet_id',v_root_timesheet_id,
    'committed_current_heads',v_head_count,
    'current_head_revision',v_current_head_revision::text,
    'outstanding_pending_successor_inputs',v_outstanding,
    'items',v_items);
end;
$function$;
alter function public.weekly_source_external_publication_pending_inputs_v1(jsonb)
  owner to postgres;
revoke all on function public.weekly_source_external_publication_pending_inputs_v1(jsonb)
  from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_external_publication_pending_inputs_v1(jsonb)
  to service_role;

comment on function private.weekly_source_external_publication_declared_v1(jsonb) is
  'Reads the ten declared facts of an external publication arrival - four source-identity, three generation, three digest - through the one installed publication scalar owner, and returns a named refusal instead of raising. An absent key, a JSON null, a wrong type, an unknown key and an out-of-range bound are all the unsafe value.';
comment on function public.weekly_source_external_publication_arrival_v1(jsonb) is
  'HANDOVER 2 round-5, Part E: the CloudTMS-owned adapter for an external publication that arrives after a CloudTMS entitlement head already exists. Three named outcomes: EXTERNAL_PUBLICATION_IDENTICAL (same source identity, generation and digest - idempotent readback, writes nothing, returns the existing publication receipt), EXTERNAL_PUBLICATION_PENDING_SUCCESSOR (different - one append-only pending successor input carrying no figure; the current head is not replaced, mutated or released and the successor must take the ordinary proposal, authorisation and publication path), EXTERNAL_PUBLICATION_REFUSED (unknown or contradictory identity, or a stale generation - fails closed and writes nothing). It never writes an entitlement head, a decision, a receipt or any payment fact.';
comment on function public.weekly_source_external_publication_pending_inputs_v1(jsonb) is
  'Service-only Office reader over the pending successor inputs recorded for one root. Outstanding is derived by comparing each input declared generation with the root current committed head, never stored, so no writer can mark an external input accepted. It returns identity, generation and digests only.';

notify pgrst,'reload schema';

commit;

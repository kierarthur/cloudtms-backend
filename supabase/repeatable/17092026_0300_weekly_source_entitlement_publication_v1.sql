-- Repeatable CloudTMS authority: weekly_source_entitlement_publication_v1
--
-- The Weekly Source entitlement-head publication coordinator (Gates 3 and 5):
-- the one place where a complete entitlement becomes current, the payment
-- Workbench is woken exactly once, and exactly one receipt is written, all or
-- nothing.
--
-- Pack authority, read word for word before this file was written:
--   P:\proof\32_PENDING_PUBLICATION_OWNER_SPECIFICATION_20260917.md
--     sections 6 (locks and ordering), 7 (revalidation), 8 (release
--     transaction), 9 (receipt relation and the canonical request digest) and
--     11 (forbidden).
--   P:\24_CROSS_SYSTEM_SOURCE_PAY_INVOICE_AMENDMENT_AUTHORITY.md
--     sections 4.3, 4.4, 4.5 and 5.1.
--   P:\26_PLAN_6_2_IMPLEMENTATION_AND_PROOF_LEDGER.md Gate 5, steps 1 to 10.
--   P:\27_WORKBENCH_COMPATIBILITY_AND_BANKING_PAY_HANDOFF.md sections 2, 4, 8.
--   P:\proof\34_… sections 4, 5, 6 and 7.
--   H2-024, H2-031, H2-032, H2-035, H2-036, H2-038.
--
-- The request shape is the fixed interface I-3, published by this package as
-- `plan6-2-implementation\interfaces\PUBLICATION_REQUEST_SHAPE.md` before any
-- code was written.  WP-06, WP-07 and WP-08b conform to that file.
--
-- What this coordinator never does (proof/32 section 11, 24 section 4.5):
-- it calculates no residual; it writes no pay, recovery, Draft, Case, provider,
-- settlement, reservation or remittance row; it reads no C1 staging or
-- checkpoint table as authority; it never mutates, rotates, replaces,
-- unauthorises, reauthorises or rebuilds a public Timesheet or a current TSFIN;
-- it never trusts a stored timesheet_id without the family resolution carried in
-- the I-1 lock result; it holds no lock on any Banking Pay table; it accepts no
-- actor and no timestamp from the caller (the actor is read from the immutable
-- accepted decision bundle, the time is pg_catalog.clock_timestamp()); and it
-- never sets, pre-seeds or reads a Workbench session setting.
--
-- Late binding.  plpgsql resolves called functions at run time, so the calls to
-- interfaces I-1 (WP-03), I-2 (WP-08a), I-5 (WP-08b) and I-6 (WP-07) compile
-- before those owners exist.
--
-- No function in this file is PostgREST-callable: every one is in `private`,
-- owned by postgres, with all privileges revoked from PUBLIC, anon,
-- authenticated and service_role.  There is therefore no
-- `notify pgrst, 'reload schema';` in this file.

\set ON_ERROR_STOP on

begin;

-- ---------------------------------------------------------------------------
-- 1. The canonical JSON text encoder
-- ---------------------------------------------------------------------------
-- proof/32 section 9, last paragraph: "SHA-256 over the UTF-8 bytes of a
-- canonical JSON object - keys sorted, no insignificant whitespace, integers as
-- base-10 strings, UUIDs lower-case".
--
-- PostgreSQL's own jsonb key order is length-first and then bytewise, which is
-- NOT sorted order, so the keys are sorted explicitly under the C collation
-- (byte order).  A JSON number must be an exact integer: every non-integral
-- quantity travels through this encoder as a fixed-scale decimal STRING, so a
-- caller can never change a money digest by writing a rate differently.
--
-- An integer is emitted as a bare base-10 numeral rather than a quoted string,
-- so that an integer and its decimal string remain distinguishable.  That
-- reading of "integers as base-10 strings" is recorded as an open question in
-- IMPL\reports\WP-02_REPORT.md; flipping it is a one-line change here.
create or replace function private.weekly_source_canonical_json_text_v1(
  p_value jsonb
) returns text
language plpgsql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_type text;
  v_num numeric;
  v_parts text[];
begin
  if p_value is null then
    return 'null';
  end if;
  v_type:=pg_catalog.jsonb_typeof(p_value);

  if v_type='null' then
    return 'null';

  elsif v_type='boolean' then
    return case when p_value::boolean then 'true' else 'false' end;

  elsif v_type='number' then
    v_num:=pg_catalog.trim_scale(p_value::numeric);
    if pg_catalog.scale(v_num)<>0 then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_DIGEST_NON_INTEGER'
        using errcode='22023',
              detail=pg_catalog.jsonb_build_object(
                'code','WEEKLY_SOURCE_PUBLICATION_DIGEST_NON_INTEGER',
                'value',p_value)::text;
    end if;
    return v_num::text;

  elsif v_type='string' then
    -- PostgreSQL's own JSON string escaping: " -> \", \ -> \\, the five short
    -- escapes \b \f \n \r \t, every other control character below 0x20 as
    -- \u00xx with lower-case hex, / unescaped, non-ASCII emitted literally.
    return pg_catalog.to_jsonb(p_value #>> '{}')::text;

  elsif v_type='array' then
    select coalesce(
             pg_catalog.array_agg(
               private.weekly_source_canonical_json_text_v1(array_element.value)
               order by array_element.ordinality),
             array[]::text[])
      into v_parts
      from pg_catalog.jsonb_array_elements(p_value)
           with ordinality as array_element(value,ordinality);
    return '['||pg_catalog.array_to_string(v_parts,',')||']';

  elsif v_type='object' then
    select coalesce(
             pg_catalog.array_agg(
               pg_catalog.to_jsonb(object_entry.key)::text
               ||':'
               ||private.weekly_source_canonical_json_text_v1(object_entry.value)
               order by object_entry.key collate "C"),
             array[]::text[])
      into v_parts
      from pg_catalog.jsonb_each(p_value) as object_entry(key,value);
    return '{'||pg_catalog.array_to_string(v_parts,',')||'}';
  end if;

  raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'
    using errcode='22023',
          detail=pg_catalog.jsonb_build_object(
            'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
            'reason','UNKNOWN_JSON_TYPE','jsonb_typeof',v_type)::text;
end;
$function$;

-- The single canonical request-digest encoder, shared by immediate
-- publication, deferred release and replay verification (proof/32 section 9;
-- H2-032).  There is exactly one implementation and every caller uses it.
create or replace function private.weekly_source_publication_request_digest_v1(
  p_canonical jsonb
) returns bytea
language sql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  select pg_catalog.sha256(
    pg_catalog.convert_to(
      private.weekly_source_canonical_json_text_v1(p_canonical),'UTF8'));
$function$;

-- ---------------------------------------------------------------------------
-- 2. Request validation helpers
-- ---------------------------------------------------------------------------
-- One scalar coercion owner for the whole request, so every field is validated
-- and normalised in exactly one place (I-3 section 6.2).
create or replace function private.weekly_source_publication_scalar_v1(
  p_value jsonb,
  p_field text,
  p_kind text,
  p_scale integer default null,
  p_nullable boolean default false
) returns jsonb
language plpgsql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_type text:=case when p_value is null then 'null'
                    else pg_catalog.jsonb_typeof(p_value) end;
  v_text text;
  v_num numeric;
  v_format text;
begin
  if v_type='null' then
    if p_nullable then
      return 'null'::jsonb;
    end if;
    raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'
      using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
              'field',p_field,'reason','NULL_NOT_ALLOWED')::text;
  end if;

  if p_kind='BOOL' then
    if v_type<>'boolean' then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'
        using errcode='22023',
              detail=pg_catalog.jsonb_build_object(
                'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
                'field',p_field,'reason','EXPECTED_BOOLEAN',
                'jsonb_typeof',v_type)::text;
    end if;
    return p_value;
  end if;

  if p_kind='INT' then
    if v_type<>'number' then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'
        using errcode='22023',
              detail=pg_catalog.jsonb_build_object(
                'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
                'field',p_field,'reason','EXPECTED_INTEGER',
                'jsonb_typeof',v_type)::text;
    end if;
    v_num:=pg_catalog.trim_scale(p_value::numeric);
    if pg_catalog.scale(v_num)<>0 then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_DIGEST_NON_INTEGER'
        using errcode='22023',
              detail=pg_catalog.jsonb_build_object(
                'code','WEEKLY_SOURCE_PUBLICATION_DIGEST_NON_INTEGER',
                'field',p_field,'value',p_value)::text;
    end if;
    return pg_catalog.to_jsonb(v_num);
  end if;

  if v_type<>'string' then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'
      using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
              'field',p_field,'reason','EXPECTED_STRING',
              'jsonb_typeof',v_type)::text;
  end if;
  v_text:=p_value #>> '{}';

  if p_kind='TEXT' then
    if pg_catalog.length(v_text)=0 then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'
        using errcode='22023',
              detail=pg_catalog.jsonb_build_object(
                'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
                'field',p_field,'reason','EMPTY_TEXT')::text;
    end if;
    return pg_catalog.to_jsonb(v_text);

  elsif p_kind='RAWTEXT' then
    -- The raw booking_id travels byte for byte; it is never trimmed, because
    -- the trimmed form is a lock and index key, not an identity (H2-035).
    return pg_catalog.to_jsonb(v_text);

  elsif p_kind='UUID' then
    if v_text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'
        using errcode='22023',
              detail=pg_catalog.jsonb_build_object(
                'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
                'field',p_field,'reason','EXPECTED_UUID')::text;
    end if;
    return pg_catalog.to_jsonb(v_text::uuid::text);

  elsif p_kind='HEX32' then
    if v_text !~* '^[0-9a-f]{64}$' then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'
        using errcode='22023',
              detail=pg_catalog.jsonb_build_object(
                'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
                'field',p_field,'reason','EXPECTED_32_BYTE_HEX')::text;
    end if;
    return pg_catalog.to_jsonb(pg_catalog.lower(v_text));

  elsif p_kind='DATE' then
    -- The wire form is validated as ISO by pattern, parsed to prove it is a
    -- real date, and rendered back by PATTERN, never by DateStyle.  `date::text`
    -- honours the session's DateStyle, so under `German, DMY` the same request
    -- would canonicalise `2026-03-02` as `02.03.2026` and produce a different
    -- money digest from the same decision (review finding, lower severity 2).
    if v_text !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'
        using errcode='22023',
              detail=pg_catalog.jsonb_build_object(
                'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
                'field',p_field,'reason','EXPECTED_ISO_DATE')::text;
    end if;
    return pg_catalog.to_jsonb(pg_catalog.to_char(v_text::date,'YYYY-MM-DD'));

  elsif p_kind='DEC' then
    if v_text !~ '^-?[0-9]+(\.[0-9]+)?$' then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'
        using errcode='22023',
              detail=pg_catalog.jsonb_build_object(
                'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
                'field',p_field,'reason','EXPECTED_DECIMAL_STRING')::text;
    end if;
    v_num:=pg_catalog.trim_scale(v_text::numeric);
    -- More precision than the stored column can hold is refused, never rounded:
    -- silently rounding money is how a penny goes missing.
    if pg_catalog.scale(v_num)>p_scale then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'
        using errcode='22023',
              detail=pg_catalog.jsonb_build_object(
                'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
                'field',p_field,'reason','DECIMAL_SCALE_EXCEEDED',
                'max_scale',p_scale,'actual_scale',pg_catalog.scale(v_num))::text;
    end if;
    -- And more MAGNITUDE than the column can hold is refused too.  The relation
    -- is numeric(18,s), so the integer part may carry at most 18-s digits.  The
    -- first version rendered through a fixed to_char picture, which prints `#`
    -- on overflow: `1000000000000000000.00` and `2000000000000000000.00` both
    -- became `##################.##` and DIGESTED THE SAME (review finding,
    -- lower severity 3).  A money digest may never be ambiguous, so this
    -- refuses instead of printing.
    if pg_catalog.length((pg_catalog.trunc(pg_catalog.abs(v_num)))::text)>18-p_scale then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'
        using errcode='22023',
              detail=pg_catalog.jsonb_build_object(
                'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
                'field',p_field,'reason','DECIMAL_MAGNITUDE_EXCEEDED',
                'max_integer_digits',18-p_scale,
                'actual_integer_digits',
                  pg_catalog.length((pg_catalog.trunc(pg_catalog.abs(v_num)))::text))::text;
    end if;
    -- Rendered by arithmetic, not by to_char: to_char's decimal separator
    -- follows lc_numeric, while `numeric::text` is always '.' with no grouping.
    v_format:=v_num::text;
    if p_scale>0 then
      if pg_catalog.strpos(v_format,'.')=0 then
        v_format:=v_format||'.'||pg_catalog.repeat('0',p_scale);
      else
        v_format:=v_format||pg_catalog.repeat('0',p_scale-pg_catalog.scale(v_num));
      end if;
    end if;
    return pg_catalog.to_jsonb(v_format);
  end if;

  raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'
    using errcode='22023',
          detail=pg_catalog.jsonb_build_object(
            'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
            'field',p_field,'reason','UNKNOWN_KIND','kind',p_kind)::text;
end;
$function$;

-- A money digest that silently drops a field it does not recognise is worse
-- than no digest, so an unknown key inside the digest scope is a refusal.
create or replace function private.weekly_source_publication_require_keys_v1(
  p_object jsonb,
  p_expected text[],
  p_field text
) returns void
language plpgsql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_unknown text[];
  v_missing text[];
begin
  if p_object is null or pg_catalog.jsonb_typeof(p_object)<>'object' then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'
      using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
              'field',p_field,'reason','EXPECTED_OBJECT')::text;
  end if;

  select pg_catalog.array_agg(present_key order by present_key)
    into v_unknown
    from pg_catalog.jsonb_object_keys(p_object) as present_key
   where not (present_key=any(p_expected));
  if v_unknown is not null then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_UNKNOWN_FIELD'
      using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_UNKNOWN_FIELD',
              'field',p_field,'unknown_keys',pg_catalog.to_jsonb(v_unknown))::text;
  end if;

  select pg_catalog.array_agg(expected_key order by expected_key)
    into v_missing
    from pg_catalog.unnest(p_expected) as expected_key
   where not p_object ? expected_key;
  if v_missing is not null then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'
      using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
              'field',p_field,'reason','MISSING_KEYS',
              'missing_keys',pg_catalog.to_jsonb(v_missing))::text;
  end if;
end;
$function$;

-- One entitlement component, normalised to the complete WB-005 allowlist of
-- public.weekly_source_entitlement_head_components minus the columns the
-- coordinator owns.  Every key is required and present; a missing key is a
-- refusal, not a default.  `adjustment_id` does not exist here and never will:
-- adjustments are never copied into a head (24 section 5; WB-007, WB-013).
create or replace function private.weekly_source_publication_component_canonical_v1(
  p_component jsonb,
  p_field text
) returns jsonb
language plpgsql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_out jsonb;
begin
  perform private.weekly_source_publication_require_keys_v1(
    p_component,
    array[
      'component_ordinal','component_id','component_kind','economic_key_type',
      'economic_key_value','component_member_identity','segment_id','segment_key',
      'segment_stable_key','work_date','reference_number','hours_day','hours_night',
      'hours_sat','hours_sun','hours_bh','additional_code_raw','unit_count',
      'unit_pay_rate','unit_charge_rate','expense_code','pay_ex_vat','charge_ex_vat',
      'exclude_from_pay','origin','movement_id','movement_group_id'
    ],
    p_field);

  v_out:=pg_catalog.jsonb_build_object(
    'component_ordinal',private.weekly_source_publication_scalar_v1(p_component->'component_ordinal',p_field||'.component_ordinal','INT'),
    'component_id',private.weekly_source_publication_scalar_v1(p_component->'component_id',p_field||'.component_id','UUID'),
    'component_kind',private.weekly_source_publication_scalar_v1(p_component->'component_kind',p_field||'.component_kind','TEXT'),
    'economic_key_type',private.weekly_source_publication_scalar_v1(p_component->'economic_key_type',p_field||'.economic_key_type','TEXT'),
    'economic_key_value',private.weekly_source_publication_scalar_v1(p_component->'economic_key_value',p_field||'.economic_key_value','TEXT'),
    'component_member_identity',private.weekly_source_publication_scalar_v1(p_component->'component_member_identity',p_field||'.component_member_identity','TEXT'),
    'segment_id',private.weekly_source_publication_scalar_v1(p_component->'segment_id',p_field||'.segment_id','TEXT',null,true),
    'segment_key',private.weekly_source_publication_scalar_v1(p_component->'segment_key',p_field||'.segment_key','TEXT',null,true),
    'segment_stable_key',private.weekly_source_publication_scalar_v1(p_component->'segment_stable_key',p_field||'.segment_stable_key','TEXT',null,true),
    'work_date',private.weekly_source_publication_scalar_v1(p_component->'work_date',p_field||'.work_date','DATE',null,true),
    'reference_number',private.weekly_source_publication_scalar_v1(p_component->'reference_number',p_field||'.reference_number','TEXT',null,true),
    'hours_day',private.weekly_source_publication_scalar_v1(p_component->'hours_day',p_field||'.hours_day','DEC',6,true),
    'hours_night',private.weekly_source_publication_scalar_v1(p_component->'hours_night',p_field||'.hours_night','DEC',6,true),
    'hours_sat',private.weekly_source_publication_scalar_v1(p_component->'hours_sat',p_field||'.hours_sat','DEC',6,true),
    'hours_sun',private.weekly_source_publication_scalar_v1(p_component->'hours_sun',p_field||'.hours_sun','DEC',6,true),
    'hours_bh',private.weekly_source_publication_scalar_v1(p_component->'hours_bh',p_field||'.hours_bh','DEC',6,true),
    'additional_code_raw',private.weekly_source_publication_scalar_v1(p_component->'additional_code_raw',p_field||'.additional_code_raw','TEXT',null,true),
    'unit_count',private.weekly_source_publication_scalar_v1(p_component->'unit_count',p_field||'.unit_count','DEC',6,true),
    'unit_pay_rate',private.weekly_source_publication_scalar_v1(p_component->'unit_pay_rate',p_field||'.unit_pay_rate','DEC',6,true),
    'unit_charge_rate',private.weekly_source_publication_scalar_v1(p_component->'unit_charge_rate',p_field||'.unit_charge_rate','DEC',6,true),
    'expense_code',private.weekly_source_publication_scalar_v1(p_component->'expense_code',p_field||'.expense_code','TEXT',null,true),
    'pay_ex_vat',private.weekly_source_publication_scalar_v1(p_component->'pay_ex_vat',p_field||'.pay_ex_vat','DEC',2),
    'charge_ex_vat',private.weekly_source_publication_scalar_v1(p_component->'charge_ex_vat',p_field||'.charge_ex_vat','DEC',2,true),
    'exclude_from_pay',private.weekly_source_publication_scalar_v1(p_component->'exclude_from_pay',p_field||'.exclude_from_pay','BOOL'),
    'origin',private.weekly_source_publication_scalar_v1(p_component->'origin',p_field||'.origin','TEXT'),
    'movement_id',private.weekly_source_publication_scalar_v1(p_component->'movement_id',p_field||'.movement_id','UUID',null,true),
    'movement_group_id',private.weekly_source_publication_scalar_v1(p_component->'movement_group_id',p_field||'.movement_group_id','UUID',null,true)
  );

  -- 24 section 4.5 step 3: movement_group_id groups components that MOVE
  -- together, so it cannot exist without a movement identity.  The head
  -- component relation carries the same rule as a CHECK.
  if v_out->'movement_group_id'<>'null'::jsonb and v_out->'movement_id'='null'::jsonb then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_MOVEMENT_IDENTITY_INVALID'
      using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_PUBLICATION_MOVEMENT_IDENTITY_INVALID',
              'field',p_field,'reason','GROUP_WITHOUT_MOVEMENT')::text;
  end if;

  return v_out;
end;
$function$;

-- ---------------------------------------------------------------------------
-- 3. The canonicaliser: project exactly the eleven proof/32 section 9 fields
-- ---------------------------------------------------------------------------
-- "Nothing outside that object influences the digest, and the same object is
-- what exact replay compares field by field" (proof/32 section 9).
--
-- publication_mode and pending_bundle_id are supplied by the coordinator, not
-- by the caller: they are digest fields, so the same economic decision has a
-- different digest when it is published immediately and when it is released
-- from a pending bundle.  That is what makes the pending bundle's stored digest
-- (section 2, section 7) and the deferred receipt's digest (section 9) one
-- value.
create or replace function private.weekly_source_publication_request_canonical_v1(
  p_request jsonb,
  p_publication_mode text,
  p_pending_bundle_id uuid
) returns jsonb
language plpgsql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_mode text:=pg_catalog.upper(pg_catalog.btrim(coalesce(p_publication_mode,'')));
  v_financial jsonb;
  v_source_revision jsonb;
  v_contract_choices jsonb;
  v_member_entitlements jsonb;
  v_member_count integer;
  v_entry jsonb;
  v_components jsonb;
  v_component_count integer;
  v_certified_zero boolean;
  v_ordinal integer;
  v_index integer;
  v_seen integer;
begin
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object' then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'
      using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
              'field','request','reason','EXPECTED_OBJECT')::text;
  end if;
  if v_mode not in ('IMMEDIATE','DEFERRED') then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'
      using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
              'field','publication_mode','reason','EXPECTED_IMMEDIATE_OR_DEFERRED')::text;
  end if;
  if (v_mode='DEFERRED')<>(p_pending_bundle_id is not null) then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'
      using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
              'field','pending_bundle_id',
              'reason','DEFERRED_REQUIRES_PENDING_BUNDLE')::text;
  end if;

  -- The four aligned arrays (H2-032).
  for v_index in 1..4 loop
    if pg_catalog.jsonb_typeof(p_request->(array['member_root_ids','member_family_booking_ids',
                                                 'member_root_versions','head_ids'])[v_index])<>'array' then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'
        using errcode='22023',
              detail=pg_catalog.jsonb_build_object(
                'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
                'field',(array['member_root_ids','member_family_booking_ids',
                               'member_root_versions','head_ids'])[v_index],
                'reason','EXPECTED_ARRAY')::text;
    end if;
  end loop;

  v_member_count:=pg_catalog.jsonb_array_length(p_request->'member_root_ids');
  if v_member_count<1
     or v_member_count<>pg_catalog.jsonb_array_length(p_request->'member_family_booking_ids')
     or v_member_count<>pg_catalog.jsonb_array_length(p_request->'member_root_versions')
     or v_member_count<>pg_catalog.jsonb_array_length(p_request->'head_ids') then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'
      using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
              'field','member_root_ids','reason','ARRAYS_NOT_ALIGNED',
              'member_root_ids',v_member_count)::text;
  end if;

  -- 24 section 4.5: this approval is the BOUNDED old-Contract/new-Contract A/B
  -- amendment.  "A larger member set requires staged keyset work and a
  -- constant-size bundle activation pointer", which does not exist, so three
  -- roots are refused rather than silently attempted.
  if v_member_count>2 then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_BUNDLE_UNBOUNDED'
      using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_PUBLICATION_BUNDLE_UNBOUNDED',
              'member_count',v_member_count)::text;
  end if;

  -- financial_request (I-3 section 4).
  v_financial:=p_request->'financial_request';
  perform private.weekly_source_publication_require_keys_v1(
    v_financial,
    array['source_revision','contract_choices','member_entitlements'],
    'financial_request');

  perform private.weekly_source_publication_require_keys_v1(
    v_financial->'source_revision',
    array['final_revision_id','source_cycle_id','revision_number','manifest_hash',
          'policy_fingerprint'],
    'financial_request.source_revision');
  v_source_revision:=pg_catalog.jsonb_build_object(
    'final_revision_id',private.weekly_source_publication_scalar_v1(v_financial->'source_revision'->'final_revision_id','financial_request.source_revision.final_revision_id','UUID'),
    'source_cycle_id',private.weekly_source_publication_scalar_v1(v_financial->'source_revision'->'source_cycle_id','financial_request.source_revision.source_cycle_id','UUID'),
    'revision_number',private.weekly_source_publication_scalar_v1(v_financial->'source_revision'->'revision_number','financial_request.source_revision.revision_number','INT'),
    'manifest_hash',private.weekly_source_publication_scalar_v1(v_financial->'source_revision'->'manifest_hash','financial_request.source_revision.manifest_hash','HEX32'),
    'policy_fingerprint',private.weekly_source_publication_scalar_v1(v_financial->'source_revision'->'policy_fingerprint','financial_request.source_revision.policy_fingerprint','HEX32'));

  if pg_catalog.jsonb_typeof(v_financial->'contract_choices')<>'array'
     or pg_catalog.jsonb_array_length(v_financial->'contract_choices')<>v_member_count then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'
      using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
              'field','financial_request.contract_choices',
              'reason','NOT_ALIGNED_WITH_MEMBERS')::text;
  end if;
  if pg_catalog.jsonb_typeof(v_financial->'member_entitlements')<>'array'
     or pg_catalog.jsonb_array_length(v_financial->'member_entitlements')<>v_member_count then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'
      using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
              'field','financial_request.member_entitlements',
              'reason','NOT_ALIGNED_WITH_MEMBERS')::text;
  end if;

  -- Contract choices, re-ordered by root_ordinal so caller order cannot change
  -- the digest.  Each ordinal must appear exactly once.
  v_contract_choices:='[]'::jsonb;
  for v_index in 1..v_member_count loop
    select choice_element.value into v_entry
      from pg_catalog.jsonb_array_elements(v_financial->'contract_choices') as choice_element(value)
     where (choice_element.value->>'root_ordinal')::text=v_index::text;
    if v_entry is null then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'
        using errcode='22023',
              detail=pg_catalog.jsonb_build_object(
                'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
                'field','financial_request.contract_choices',
                'reason','MISSING_ROOT_ORDINAL','root_ordinal',v_index)::text;
    end if;
    perform private.weekly_source_publication_require_keys_v1(
      v_entry,
      array['root_ordinal','contract_id','week_ending_date','selection_method'],
      'financial_request.contract_choices['||v_index::text||']');
    if (v_entry->>'selection_method') not in
       ('AUTO_UNIQUE','OFFICE_SELECTED','DURABLE_LINEAGE','UNCHANGED') then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'
        using errcode='22023',
              detail=pg_catalog.jsonb_build_object(
                'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
                'field','financial_request.contract_choices['||v_index::text||'].selection_method',
                'reason','UNKNOWN_SELECTION_METHOD',
                'selection_method',v_entry->'selection_method')::text;
    end if;
    v_contract_choices:=v_contract_choices||pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object(
        'root_ordinal',private.weekly_source_publication_scalar_v1(v_entry->'root_ordinal','financial_request.contract_choices.root_ordinal','INT'),
        'contract_id',private.weekly_source_publication_scalar_v1(v_entry->'contract_id','financial_request.contract_choices.contract_id','UUID'),
        'week_ending_date',private.weekly_source_publication_scalar_v1(v_entry->'week_ending_date','financial_request.contract_choices.week_ending_date','DATE'),
        'selection_method',private.weekly_source_publication_scalar_v1(v_entry->'selection_method','financial_request.contract_choices.selection_method','TEXT')));
    v_entry:=null;
  end loop;

  -- Every root's complete entitlement vector.
  v_member_entitlements:='[]'::jsonb;
  for v_index in 1..v_member_count loop
    select entitlement_element.value into v_entry
      from pg_catalog.jsonb_array_elements(v_financial->'member_entitlements') as entitlement_element(value)
     where (entitlement_element.value->>'root_ordinal')::text=v_index::text;
    if v_entry is null then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'
        using errcode='22023',
              detail=pg_catalog.jsonb_build_object(
                'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
                'field','financial_request.member_entitlements',
                'reason','MISSING_ROOT_ORDINAL','root_ordinal',v_index)::text;
    end if;
    perform private.weekly_source_publication_require_keys_v1(
      v_entry,
      array['root_ordinal','authority_kind','certified_zero','component_count','components'],
      'financial_request.member_entitlements['||v_index::text||']');

    -- 24 section 4.3: one common interface for both authority kinds.
    if (v_entry->>'authority_kind') not in ('PROTECTED','LOCKED_FINAL_SOURCE') then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'
        using errcode='22023',
              detail=pg_catalog.jsonb_build_object(
                'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
                'field','financial_request.member_entitlements['||v_index::text||'].authority_kind',
                'reason','EXPECTED_PROTECTED_OR_LOCKED_FINAL_SOURCE')::text;
    end if;
    if pg_catalog.jsonb_typeof(v_entry->'components')<>'array' then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'
        using errcode='22023',
              detail=pg_catalog.jsonb_build_object(
                'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
                'field','financial_request.member_entitlements['||v_index::text||'].components',
                'reason','EXPECTED_ARRAY')::text;
    end if;
    v_component_count:=pg_catalog.jsonb_array_length(v_entry->'components');
    if (v_entry->'component_count')::text<>v_component_count::text then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'
        using errcode='22023',
              detail=pg_catalog.jsonb_build_object(
                'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
                'field','financial_request.member_entitlements['||v_index::text||'].component_count',
                'reason','COMPONENT_COUNT_MISMATCH',
                'declared',v_entry->'component_count','actual',v_component_count)::text;
    end if;
    -- WB-009 and 24 section 4.5 step 3: certified zero is an EXPLICIT head and
    -- exists only when no component remains.
    v_certified_zero:=(private.weekly_source_publication_scalar_v1(
      v_entry->'certified_zero',
      'financial_request.member_entitlements['||v_index::text||'].certified_zero','BOOL'))::boolean;
    if v_certified_zero<>(v_component_count=0) then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_CERTIFIED_ZERO_INVALID'
        using errcode='22023',
              detail=pg_catalog.jsonb_build_object(
                'code','WEEKLY_SOURCE_PUBLICATION_CERTIFIED_ZERO_INVALID',
                'root_ordinal',v_index,'certified_zero',v_certified_zero,
                'component_count',v_component_count)::text;
    end if;

    -- Components re-ordered by component_ordinal; ordinals are 1..n exactly
    -- once each, so the inventory is complete and has no gap or repeat.
    v_components:='[]'::jsonb;
    for v_ordinal in 1..v_component_count loop
      select pg_catalog.count(*)::integer into v_seen
        from pg_catalog.jsonb_array_elements(v_entry->'components') as component_element(value)
       where (component_element.value->>'component_ordinal')::text=v_ordinal::text;
      if v_seen<>1 then
        raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'
          using errcode='22023',
                detail=pg_catalog.jsonb_build_object(
                  'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
                  'field','financial_request.member_entitlements['||v_index::text||'].components',
                  'reason','COMPONENT_ORDINAL_NOT_UNIQUE',
                  'component_ordinal',v_ordinal,'occurrences',v_seen)::text;
      end if;
      v_components:=v_components||pg_catalog.jsonb_build_array(
        private.weekly_source_publication_component_canonical_v1(
          (select component_element.value
             from pg_catalog.jsonb_array_elements(v_entry->'components') as component_element(value)
            where (component_element.value->>'component_ordinal')::text=v_ordinal::text),
          'financial_request.member_entitlements['||v_index::text||'].components['||v_ordinal::text||']'));
    end loop;

    -- One component_id may not appear twice inside one head.
    select pg_catalog.count(distinct canonical_component.value->>'component_id')::integer
      into v_seen
      from pg_catalog.jsonb_array_elements(v_components) as canonical_component(value);
    if v_seen<>v_component_count then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'
        using errcode='22023',
              detail=pg_catalog.jsonb_build_object(
                'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
                'field','financial_request.member_entitlements['||v_index::text||'].components',
                'reason','DUPLICATE_COMPONENT_ID')::text;
    end if;

    v_member_entitlements:=v_member_entitlements||pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object(
        'root_ordinal',private.weekly_source_publication_scalar_v1(v_entry->'root_ordinal','financial_request.member_entitlements.root_ordinal','INT'),
        'authority_kind',private.weekly_source_publication_scalar_v1(v_entry->'authority_kind','financial_request.member_entitlements.authority_kind','TEXT'),
        'certified_zero',pg_catalog.to_jsonb(v_certified_zero),
        'component_count',pg_catalog.to_jsonb(v_component_count),
        'components',v_components));
    v_entry:=null;
  end loop;

  return pg_catalog.jsonb_build_object(
    'decision_bundle_id',private.weekly_source_publication_scalar_v1(p_request->'decision_bundle_id','decision_bundle_id','UUID'),
    'pending_bundle_id',case when p_pending_bundle_id is null then 'null'::jsonb
                             else pg_catalog.to_jsonb(p_pending_bundle_id::text) end,
    'bundle_revision',private.weekly_source_publication_scalar_v1(p_request->'bundle_revision','bundle_revision','INT'),
    'candidate_id',private.weekly_source_publication_scalar_v1(p_request->'candidate_id','candidate_id','UUID'),
    'member_root_ids',(
      select coalesce(pg_catalog.jsonb_agg(
               private.weekly_source_publication_scalar_v1(root_element.value,'member_root_ids','UUID')
               order by root_element.ordinality),'[]'::jsonb)
        from pg_catalog.jsonb_array_elements(p_request->'member_root_ids')
             with ordinality as root_element(value,ordinality)),
    'member_family_booking_ids',(
      select coalesce(pg_catalog.jsonb_agg(
               private.weekly_source_publication_scalar_v1(booking_element.value,'member_family_booking_ids','RAWTEXT')
               order by booking_element.ordinality),'[]'::jsonb)
        from pg_catalog.jsonb_array_elements(p_request->'member_family_booking_ids')
             with ordinality as booking_element(value,ordinality)),
    'member_root_versions',(
      select coalesce(pg_catalog.jsonb_agg(
               private.weekly_source_publication_scalar_v1(version_element.value,'member_root_versions','INT')
               order by version_element.ordinality),'[]'::jsonb)
        from pg_catalog.jsonb_array_elements(p_request->'member_root_versions')
             with ordinality as version_element(value,ordinality)),
    'head_ids',(
      select coalesce(pg_catalog.jsonb_agg(
               private.weekly_source_publication_scalar_v1(head_element.value,'head_ids','UUID')
               order by head_element.ordinality),'[]'::jsonb)
        from pg_catalog.jsonb_array_elements(p_request->'head_ids')
             with ordinality as head_element(value,ordinality)),
    'decision_id',private.weekly_source_publication_scalar_v1(p_request->'decision_id','decision_id','UUID'),
    'publication_mode',pg_catalog.to_jsonb(v_mode),
    'financial_request',pg_catalog.jsonb_build_object(
      'source_revision',v_source_revision,
      'contract_choices',v_contract_choices,
      'member_entitlements',v_member_entitlements));
end;
$function$;

-- ---------------------------------------------------------------------------
-- 3a. Set algebra for the H2-024 post-decision head proofs
-- ---------------------------------------------------------------------------
-- 24 section 4.5 step 3 requires the coordinator to prove, in the same
-- transaction, "A-before = A-after union moved", "A-after intersect moved is
-- empty", "every moved component in exactly one destination head" and "every
-- unrelated B component retained".  These four owners make those proofs
-- readable instead of four nested array expressions.
create or replace function private.weekly_source_uuid_set_union_v1(
  p_left uuid[],p_right uuid[]
) returns uuid[]
language sql immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  select coalesce((
    select pg_catalog.array_agg(distinct set_value order by set_value)
    from pg_catalog.unnest(coalesce(p_left,array[]::uuid[])
                           ||coalesce(p_right,array[]::uuid[])) as set_value
  ),array[]::uuid[]);
$function$;

create or replace function private.weekly_source_uuid_set_intersect_v1(
  p_left uuid[],p_right uuid[]
) returns uuid[]
language sql immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  select coalesce((
    select pg_catalog.array_agg(distinct left_value order by left_value)
    from pg_catalog.unnest(coalesce(p_left,array[]::uuid[])) as left_value
    where left_value=any(coalesce(p_right,array[]::uuid[]))
  ),array[]::uuid[]);
$function$;

create or replace function private.weekly_source_uuid_set_difference_v1(
  p_left uuid[],p_right uuid[]
) returns uuid[]
language sql immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  select coalesce((
    select pg_catalog.array_agg(distinct left_value order by left_value)
    from pg_catalog.unnest(coalesce(p_left,array[]::uuid[])) as left_value
    where not (left_value=any(coalesce(p_right,array[]::uuid[])))
  ),array[]::uuid[]);
$function$;

create or replace function private.weekly_source_uuid_set_equals_v1(
  p_left uuid[],p_right uuid[]
) returns boolean
language sql immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  select private.weekly_source_uuid_set_union_v1(p_left,array[]::uuid[])
         =private.weekly_source_uuid_set_union_v1(p_right,array[]::uuid[]);
$function$;

-- A component's CONTENT identity: what the component is, with the three facts
-- that describe where it sits removed.
--
--   component_ordinal   - its position inside one head.  A component retained
--                         while another moves out from in front of it changes
--                         position without changing.
--   movement_id,
--   movement_group_id   - bundle-scoped provenance.  A component that moved in
--                         bundle N carries a movement identity there and none
--                         when it is merely retained by bundle N+1.
--
-- Everything else - the economic keys, the hours, the rates, the money, the
-- expense code, exclude_from_pay and origin - is content, and `component_sha256`
-- is the digest of exactly that.  It is what lets the coordinator prove that a
-- RETAINED component is byte-identical (review finding U3(a)) without refusing
-- every legal move.  Position is still covered by `inventory_digest` and
-- `entitlement_digest`, and the movement rules by their own explicit checks and
-- unique indexes.
create or replace function private.weekly_source_publication_component_content_v1(
  p_component jsonb
) returns jsonb
language sql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  select p_component-'component_ordinal'-'movement_id'-'movement_group_id';
$function$;

-- Is the target root PROVABLY BLANK, not merely unknown to Weekly Source?
-- Review finding U2: a B Timesheet that already existed, was never touched by
-- Weekly Source and carries its own unrelated shifts passed the old
-- "no head and no lineage row" test and was authorised silently, which
-- 24 section 4.5 step 4, file 26 Gate 5 step 6, 27 section 8 step 6 and H2-024
-- all forbid.  Every clause below is a fact this coordinator can read; the
-- clean case is a root created inside this very transaction.
create or replace function private.weekly_source_publication_target_root_blank_v1(
  p_root_timesheet_id uuid,
  p_family_booking_id text
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_root record;
  v_reasons jsonb:='[]'::jsonb;
  v_created_in_this_transaction boolean:=false;
  v_snapshot_inventory jsonb;
  v_family uuid[];
begin
  select timesheet_id,status::text as status_text,actual_schedule_json,created_at,
         authorised_at_server
    into v_root
    from public.timesheets
   where timesheet_id=p_root_timesheet_id;
  if not found then
    return pg_catalog.jsonb_build_object('blank',false,
      'reasons',pg_catalog.jsonb_build_array('ROOT_TIMESHEET_NOT_FOUND'));
  end if;

  v_created_in_this_transaction:=v_root.created_at>=pg_catalog.transaction_timestamp();

  -- WP-30 (WP-27 sweep finding N2), standing rule 3.  Three consecutive
  -- eligibility reasons, and they disagreed with each other: the head limb below
  -- was already keyed on the family booking id while the authorisation limb here
  -- and the lineage limb after it were keyed on the physical root id.  A reason
  -- that is not added makes the root look PROVABLY BLANK, which is the
  -- fail-OPEN direction in the one test that stands between an Office decision
  -- and authorising a root silently.  EXECUTED on a rotated family whose
  -- authorisation and lineage rows sit on the demoted sibling: the current root
  -- returned `blank=true, reasons=[]`, while the demoted id returned both
  -- reasons.
  --
  -- One family resolution, through the one installed adapter, read by both
  -- limbs.  The adapter is used rather than the family booking string the two
  -- relations happen to carry, because a trimmed-booking join is a SECOND
  -- identity mechanism and disagrees with the installed resolver on a
  -- whitespace-padded booking id.
  v_family:=private.weekly_source_invoice_family_timesheet_ids_v1(p_root_timesheet_id);
  -- Standing rule 3's fail-closed branch, as an explicit cardinality test and
  -- never a `limit`: an unresolvable family is a REASON, so the root is not
  -- reported blank.
  if v_family is null or pg_catalog.cardinality(v_family)=0 then
    v_reasons:=v_reasons||pg_catalog.to_jsonb('FAMILY_IDENTITY_UNRESOLVED'::text);
    v_family:=array[p_root_timesheet_id]::uuid[];
  end if;

  -- Never authorised, by Weekly Source or by anyone.
  if exists (select 1 from public.weekly_source_root_authorisations as authorisation_row
              where authorisation_row.root_timesheet_id=any(v_family)) then
    v_reasons:=v_reasons||pg_catalog.to_jsonb('HAS_A_ROOT_AUTHORISATION_GENERATION'::text);
  end if;
  if exists (select 1 from public.weekly_source_entitlement_heads as head_row
              where pg_catalog.btrim(head_row.root_family_booking_id)
                    =pg_catalog.btrim(p_family_booking_id)) then
    v_reasons:=v_reasons||pg_catalog.to_jsonb('FAMILY_ALREADY_HAS_AN_ENTITLEMENT_HEAD'::text);
  end if;
  if exists (select 1 from public.weekly_source_row_timesheet_lineages as lineage_row
              where lineage_row.timesheet_id=any(v_family)) then
    v_reasons:=v_reasons||pg_catalog.to_jsonb('ALREADY_BOUND_TO_A_SOURCE_ROW'::text);
  end if;

  -- The ordinary Authorise owner that interface I-6 calls REQUIRES a current
  -- TSFIN row (`TARGET_NOT_FOUND`/`NO_TSFIN`), so a genuinely new B root always
  -- has one by the time it is authorised: it is prepared by the same source
  -- pipeline.  Refusing on its mere existence would make a new B root
  -- impossible, so the question is what the snapshot CARRIES.
  --
  -- WP-06c review observation O2, reproduced by WP-02b and fixed here.  This
  -- test used to be `created_at < transaction_timestamp()`: a snapshot that
  -- predated the transaction disqualified the root whatever it held.  That made
  -- a genuinely new B root provably blank ONLY inside the transaction that
  -- created it, so the two publication paths the design actually has - a
  -- retryable `WEEKLY_SOURCE_CANDIDATE_BUSY` (I-3 section 7.1 calls it common,
  -- not exceptional) and the `FROZEN` -> pending -> `DEFERRED` release that runs
  -- minutes or days later (proof/32 section 2) - both arrived at publication
  -- demanding a whole-root Office review that the accepted decision could not
  -- carry, because at proposal time there was nothing to review.  Executed
  -- before the fix: the same request, the same untouched root, composed and
  -- recorded in one transaction, refused in the next with
  -- `THE_ACCEPTED_DECISION_CARRIES_NO_WHOLE_ROOT_OFFICE_REVIEW`, and the builder
  -- could not re-compose it either.
  --
  -- `24 section 4.5` step 4 is about CONTENT - "an existing unauthorised B with
  -- unrelated content" - not about when a row was written.  So the test is now:
  -- does the snapshot hold any entitlement content?  The answer comes from the
  -- ONE named owner of "what a root effectively holds", interface I-7, so this
  -- introduces no second path to a figure, and every other disqualifier below
  -- and above (a lineage binding, an authorisation generation, a head on the
  -- family, the root's own schedule, an ordinary authorisation, a status that is
  -- not a new root's) is untouched.  I-7 absent, refusing, reporting a HEAD, or
  -- reporting any component at all all disqualify the root: fail closed.
  if exists (select 1 from public.timesheets_financials as financial_row
              where financial_row.timesheet_id=p_root_timesheet_id
                and financial_row.is_current) then
    if pg_catalog.to_regprocedure(
         'private.weekly_source_effective_inventory_v1(uuid)') is null then
      v_reasons:=v_reasons||pg_catalog.to_jsonb('HAS_A_CURRENT_FINANCIAL_SNAPSHOT'::text);
    else
      v_snapshot_inventory:=private.weekly_source_effective_inventory_v1(p_root_timesheet_id);
      if coalesce(pg_catalog.jsonb_typeof(v_snapshot_inventory),'')<>'object'
         or coalesce((v_snapshot_inventory->>'ok')::boolean,false) is not true
         or v_snapshot_inventory->>'authority' is distinct from 'TSFIN'
         or coalesce((v_snapshot_inventory->>'component_count')::integer,1)<>0
         or pg_catalog.jsonb_array_length(
              coalesce(v_snapshot_inventory->'components','["unknown"]'::jsonb))<>0 then
        v_reasons:=v_reasons||pg_catalog.to_jsonb('HAS_A_CURRENT_FINANCIAL_SNAPSHOT'::text);
      end if;
    end if;
  end if;
  if pg_catalog.jsonb_typeof(coalesce(v_root.actual_schedule_json,'[]'::jsonb))='array' then
    if pg_catalog.jsonb_array_length(coalesce(v_root.actual_schedule_json,'[]'::jsonb))>0 then
      v_reasons:=v_reasons||pg_catalog.to_jsonb('CARRIES_ITS_OWN_SCHEDULE'::text);
    end if;
  elsif coalesce(v_root.actual_schedule_json,'{}'::jsonb)<>'{}'::jsonb then
    v_reasons:=v_reasons||pg_catalog.to_jsonb('CARRIES_ITS_OWN_SCHEDULE'::text);
  end if;
  -- Authorisation is `timesheets.authorised_at_server`, NOT a status label: the
  -- installed `timesheet_status_enum` is RECEIVED, STORED, SHEETS_PENDING,
  -- SHEETS_PARTIAL, SHEETS_SYNCED, ERROR, REVOKED, so a status test for the
  -- word "authorised" can never fire and would be a silent no-op.
  if v_root.authorised_at_server is not null then
    v_reasons:=v_reasons||pg_catalog.to_jsonb('ALREADY_AUTHORISED_BY_THE_ORDINARY_OWNER'::text);
  end if;
  if v_root.status_text in ('SHEETS_PARTIAL','SHEETS_SYNCED','ERROR','REVOKED') then
    v_reasons:=v_reasons||pg_catalog.to_jsonb('STATUS_IS_NOT_A_NEW_ROOT_STATUS'::text);
  end if;

  return pg_catalog.jsonb_build_object(
    'blank',pg_catalog.jsonb_array_length(v_reasons)=0,
    'created_in_this_transaction',v_created_in_this_transaction,
    'root_timesheet_id',p_root_timesheet_id,
    'status',v_root.status_text,
    'snapshot_inventory',case when v_snapshot_inventory is null then 'null'::jsonb
      else pg_catalog.jsonb_build_object(
             'ok',v_snapshot_inventory->'ok',
             'authority',v_snapshot_inventory->'authority',
             'component_count',v_snapshot_inventory->'component_count') end,
    'reasons',v_reasons);
end;
$function$;

-- The canonical before-inventory of a whole request: one entry per member root,
-- ordered by root_ordinal, each carrying its component identities lower-cased
-- and sorted.  Its digest is what the accepted decision bundle stores as
-- `before_inventory_digest` (24 section 4.5 step 2, "approval digest"), so the
-- proposal composer and the coordinator compute the same value from the same
-- declared positions.  The caller's own per-root `inventory_digest` is
-- deliberately NOT included: it is a caller assertion, and the component
-- identities are the fact that binds.
create or replace function private.weekly_source_publication_before_inventory_v1(
  p_control jsonb,
  p_member_count integer
) returns jsonb
language plpgsql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_out jsonb:='[]'::jsonb;
  v_entry jsonb;
  v_i integer;
begin
  for v_i in 1..coalesce(p_member_count,0) loop
    select position_element.value into v_entry
      from pg_catalog.jsonb_array_elements(
             coalesce(p_control->'before_positions','[]'::jsonb)) as position_element(value)
     where (position_element.value->>'root_ordinal')::integer=v_i;
    if v_entry is null then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'
        using errcode='22023',
              detail=pg_catalog.jsonb_build_object(
                'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
                'field','control.before_positions','reason','MISSING_ROOT_ORDINAL',
                'root_ordinal',v_i)::text;
    end if;
    v_out:=v_out||pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object(
        'root_ordinal',pg_catalog.to_jsonb(v_i),
        'component_ids',coalesce((
          select pg_catalog.jsonb_agg(pg_catalog.to_jsonb(component_element.value::uuid::text)
                                      order by component_element.value::uuid::text)
            from pg_catalog.jsonb_array_elements_text(
                   coalesce(v_entry->'component_ids','[]'::jsonb)) as component_element(value)
        ),'[]'::jsonb)));
    v_entry:=null;
  end loop;
  return v_out;
end;
$function$;

-- The receipt as the coordinator returns it: every immutable field, with the
-- digest rendered as lower-case hex so a caller never has to decode bytea.
create or replace function private.weekly_source_publication_receipt_json_v1(
  p_receipt_id uuid
) returns jsonb
language sql stable
set search_path to 'pg_catalog','pg_temp'
as $function$
  select pg_catalog.jsonb_build_object(
    'id',receipt_row.id,
    'decision_bundle_id',receipt_row.decision_bundle_id,
    'pending_bundle_id',receipt_row.pending_bundle_id,
    'bundle_revision',receipt_row.bundle_revision,
    'request_digest',pg_catalog.encode(receipt_row.request_digest,'hex'),
    'publication_mode',receipt_row.publication_mode,
    'candidate_id',receipt_row.candidate_id,
    'member_root_ids',pg_catalog.to_jsonb(receipt_row.member_root_ids),
    'member_family_booking_ids',pg_catalog.to_jsonb(receipt_row.member_family_booking_ids),
    'member_root_versions',pg_catalog.to_jsonb(receipt_row.member_root_versions),
    'head_ids',pg_catalog.to_jsonb(receipt_row.head_ids),
    'scope_change_tx_token',receipt_row.scope_change_tx_token,
    'decision_id',receipt_row.decision_id,
    'decided_by_user_id',receipt_row.decided_by_user_id,
    'released_by_worker_id',receipt_row.released_by_worker_id,
    'released_by_worker_run_id',receipt_row.released_by_worker_run_id,
    'census_json',receipt_row.census_json,
    'proof_json',receipt_row.proof_json,
    'created_at_utc',receipt_row.created_at_utc)
  from private.weekly_source_entitlement_publication_receipts as receipt_row
  where receipt_row.id=p_receipt_id;
$function$;

-- ---------------------------------------------------------------------------
-- 4. Interface I-4 — the atomic head-publication coordinator (core)
-- ---------------------------------------------------------------------------
-- Assumes the I-1 rotation locks are ALREADY HELD in this transaction and
-- receives the helper result as the proof of it (proof/32 section 6; H2-035).
--
-- Order, exactly file 26 Gate 5 steps 4 to 10 and proof/32 section 8:
--   1  exact receipt replay, before anything else (section 8 step 1);
--   2  the immutable accepted decision bundle, locked;
--   3  member identity revalidated against the lock result (section 4.0);
--   4  compare-and-swap on each member's expected current head, under lock;
--   5  the complete post-decision head proofs of H2-024;
--   6  the target root's authorisation state;
--   7  stage every member head and its complete component inventory;
--   8  interface I-6 for a genuinely new B root, after staging;
--   9  activate every head together, superseding the old ones;
--  10  current_entitlement_head_id on every live lineage generation;
--  11  ONE call to private.pay_workbench_scope_invalidate_v1, success required;
--  12  exactly one immutable receipt;
--  13  return it.
--
-- Return discipline: a failure before the first write is RETURNED as
-- {ok:false,code,retryable}; a failure after the first write RAISES and rolls
-- the whole transaction back (R11; R25 point 7).  proof/32 section 6 step 1
-- states the rule for the gate refusals - "No write has happened at this point,
-- so these are returned, not raised" - and this coordinator applies it
-- consistently.  A caller that ignores ok:false is the one way to get a double
-- publication out of this design.
create or replace function private.weekly_source_entitlement_publish_core_v1(
  p_request jsonb,
  p_publication_mode text,
  p_lock_result jsonb,
  p_pending_bundle_id uuid,
  p_worker_id text,
  p_worker_run_id uuid,
  p_census jsonb,
  p_proof jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_mode text;
  v_canonical jsonb;
  v_digest bytea;
  v_now timestamptz;
  v_err_message text;
  v_err_detail text;
  v_err_state text;
  v_bundle record;
  v_pending record;
  v_receipt record;
  v_decision_bundle_id uuid;
  v_bundle_revision bigint;
  v_candidate_id uuid;
  v_decision_id uuid;
  v_member_root_ids uuid[];
  v_member_family_booking_ids text[];
  v_member_root_versions integer[];
  v_head_ids uuid[];
  v_expected_head_ids uuid[]:=array[]::uuid[];
  v_current_head_ids uuid[]:=array[]::uuid[];
  v_prior_head_ids uuid[]:=array[]::uuid[];
  v_head_revisions bigint[]:=array[]::bigint[];
  v_n integer;
  v_i integer;
  v_control jsonb;
  v_family jsonb;
  v_member_timesheet_ids uuid[]:=array[]::uuid[];
  v_entitlement jsonb;
  v_choice jsonb;
  v_component jsonb;
  v_component_sha bytea;
  v_component_ids uuid[];
  v_before_ids uuid[];
  v_after_1 uuid[]:=array[]::uuid[];
  v_after_2 uuid[]:=array[]::uuid[];
  v_before_1 uuid[]:=array[]::uuid[];
  v_before_2 uuid[]:=array[]::uuid[];
  v_moved uuid[]:=array[]::uuid[];
  -- WP-06c review F1: the CONTENT hash the SOURCE authority holds for every
  -- component of member 1's before-position, keyed by component_id.  It is what
  -- a MOVED component must still equal on the destination side.
  v_source_component_hashes jsonb:='{}'::jsonb;
  v_moved_found integer;
  v_inventory_pairs jsonb;
  v_component_hashes jsonb;
  v_head_revision bigint;
  v_token uuid;
  v_invalidation jsonb;
  v_rows integer;
  v_live_generations integer;
  v_family_live_generations integer;
  v_family_members uuid[];
  v_any_generations integer;
  v_any_heads integer;
  v_target_auth jsonb;
  v_target_authorise_result jsonb;
  v_review jsonb;
  v_source_generation_digest bytea;
  v_contract_choice_digest bytea;
  v_before_inventory jsonb;
  v_before_inventory_digest bytea;
  v_acceptance_digest bytea;
  v_before_source text[]:=array[]::text[];
  v_effective jsonb;
  v_blank jsonb;
  v_jobs_before uuid[]:=array[]::uuid[];
  v_scope_tx_before uuid[]:=array[]::uuid[];
  v_scope_state_before jsonb:='{}'::jsonb;
  v_token_recheck uuid;
  v_declared_scope uuid[];
  v_normalised jsonb;
  v_offending jsonb;
  v_result_heads jsonb:='[]'::jsonb;
  v_lineage_rows integer:=0;
begin
  -- Service-only, exactly as public.weekly_source_timesheet_lineage_ensure_atomic_v1.
  if coalesce(
       pg_catalog.current_setting('request.jwt.claim.role',true),
       nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role',
       ''
     )<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;

  -- Shape validation and the one canonical digest.  Nothing has been written,
  -- so a malformed request is returned rather than raised.
  begin
    v_mode:=pg_catalog.upper(pg_catalog.btrim(coalesce(p_publication_mode,'')));
    if v_mode not in ('IMMEDIATE','DEFERRED') then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'
        using errcode='22023',
              detail=pg_catalog.jsonb_build_object(
                'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
                'field','p_publication_mode','reason','EXPECTED_IMMEDIATE_OR_DEFERRED')::text;
    end if;
    -- proof/32 section 9 and H2-038: both Worker fields are required when the
    -- mode is DEFERRED and null when it is IMMEDIATE.
    if (v_mode='DEFERRED')<>(p_worker_id is not null and p_worker_run_id is not null)
       or (v_mode='IMMEDIATE' and (p_worker_id is not null or p_worker_run_id is not null)) then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'
        using errcode='22023',
              detail=pg_catalog.jsonb_build_object(
                'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
                'field','p_worker_id','reason','WORKER_FIELDS_MUST_MATCH_MODE')::text;
    end if;
    if pg_catalog.jsonb_typeof(coalesce(p_census,'{}'::jsonb))<>'object'
       or pg_catalog.jsonb_typeof(coalesce(p_proof,'{}'::jsonb))<>'object' then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'
        using errcode='22023',
              detail=pg_catalog.jsonb_build_object(
                'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
                'field','p_census','reason','EXPECTED_OBJECT')::text;
    end if;
    v_canonical:=private.weekly_source_publication_request_canonical_v1(
      p_request,v_mode,p_pending_bundle_id);
    v_digest:=private.weekly_source_publication_request_digest_v1(v_canonical);
  -- Review finding F10: catching only `invalid_parameter_value` let a malformed
  -- request RAISE instead of returning - an impossible calendar date (22008),
  -- an integer outside the range (22003) and a non-uuid in the control scope
  -- (22P02) all escaped.  Nothing has been written at this point, so every one
  -- of them is a returned refusal, and WP-08b no longer counts a caller's typo
  -- as a technical failure on its way to MANUAL_REVIEW.
  exception when invalid_parameter_value
              or datetime_field_overflow
              or invalid_datetime_format
              or numeric_value_out_of_range
              or invalid_text_representation then
    get stacked diagnostics v_err_message=message_text, v_err_detail=pg_exception_detail,
                            v_err_state=returned_sqlstate;
    return pg_catalog.jsonb_build_object(
      'ok',false,'published',false,'replayed',false,
      'code',case when v_err_state='22023' then v_err_message
                  else 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID' end,
      'retryable',false,
      'detail',case when coalesce(v_err_detail,'') ~ '^\{' then v_err_detail::jsonb
                    else pg_catalog.jsonb_build_object(
                           'reason','REQUEST_VALUE_COULD_NOT_BE_PARSED',
                           'sqlstate',v_err_state,'message',v_err_message) end);
  end;

  v_now:=pg_catalog.clock_timestamp();
  v_control:=coalesce(p_request->'control','{}'::jsonb);
  v_decision_bundle_id:=(v_canonical->>'decision_bundle_id')::uuid;
  v_bundle_revision:=(v_canonical->>'bundle_revision')::bigint;
  v_candidate_id:=(v_canonical->>'candidate_id')::uuid;
  v_decision_id:=(v_canonical->>'decision_id')::uuid;

  select pg_catalog.array_agg(root_element.value::uuid order by root_element.ordinality)
    into v_member_root_ids
    from pg_catalog.jsonb_array_elements_text(v_canonical->'member_root_ids')
         with ordinality as root_element(value,ordinality);
  select pg_catalog.array_agg(booking_element.value order by booking_element.ordinality)
    into v_member_family_booking_ids
    from pg_catalog.jsonb_array_elements_text(v_canonical->'member_family_booking_ids')
         with ordinality as booking_element(value,ordinality);
  select pg_catalog.array_agg(version_element.value::integer order by version_element.ordinality)
    into v_member_root_versions
    from pg_catalog.jsonb_array_elements_text(v_canonical->'member_root_versions')
         with ordinality as version_element(value,ordinality);
  select pg_catalog.array_agg(head_element.value::uuid order by head_element.ordinality)
    into v_head_ids
    from pg_catalog.jsonb_array_elements_text(v_canonical->'head_ids')
         with ordinality as head_element(value,ordinality);
  v_n:=pg_catalog.cardinality(v_member_root_ids);

  -- H2-032: no duplicate root and no duplicate head id.  The receipt relation
  -- repeats both rules; refusing here keeps the failure a refusal rather than a
  -- rolled-back transaction.
  if not private.weekly_source_uuid_array_is_distinct_v1(v_member_root_ids)
     or not private.weekly_source_uuid_array_is_distinct_v1(v_head_ids) then
    return pg_catalog.jsonb_build_object(
      'ok',false,'published',false,'replayed',false,
      'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID','retryable',false,
      'detail',pg_catalog.jsonb_build_object('reason','DUPLICATE_ROOT_OR_HEAD_ID'));
  end if;
  if v_n=2 and pg_catalog.btrim(v_member_family_booking_ids[1])
              =pg_catalog.btrim(v_member_family_booking_ids[2]) then
    return pg_catalog.jsonb_build_object(
      'ok',false,'published',false,'replayed',false,
      'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID','retryable',false,
      'detail',pg_catalog.jsonb_build_object('reason','MEMBERS_SHARE_ONE_FAMILY'));
  end if;

  -- ---- 1. exact receipt replay, before any mutation (proof/32 section 8.1) --
  -- The digest is only an index lookup: every immutable receipt field is
  -- compared before a replay may be returned (H2-032).
  select * into v_receipt
    from private.weekly_source_entitlement_publication_receipts as receipt_row
   where receipt_row.request_digest=v_digest;
  if found then
    if v_receipt.decision_bundle_id<>v_decision_bundle_id
       or v_receipt.bundle_revision<>v_bundle_revision
       or v_receipt.candidate_id<>v_candidate_id
       or v_receipt.member_root_ids is distinct from v_member_root_ids
       or v_receipt.member_family_booking_ids is distinct from v_member_family_booking_ids
       or v_receipt.member_root_versions is distinct from v_member_root_versions
       or v_receipt.head_ids is distinct from v_head_ids
       or v_receipt.decision_id<>v_decision_id
       or v_receipt.publication_mode<>v_mode
       or v_receipt.pending_bundle_id is distinct from p_pending_bundle_id then
      -- Round-5 ruling A1 control 5: "conflicting replay and a tampered row are
      -- PERMANENT integrity failures and go DIRECTLY to manual review; they are
      -- not retried ten times".  The same digest over different immutable
      -- receipt fields means two different economic decisions share one money
      -- identity, which no amount of retrying can resolve.  `retryable:false`
      -- alone only stops the immediate retry; the disposition below is what
      -- tells the deferred release owner to route the bundle to MANUAL_REVIEW
      -- at once instead of counting it as one of its ten technical failures.
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_PUBLICATION_REPLAY_CONFLICT','retryable',false,
        'detail',pg_catalog.jsonb_build_object(
          'integrity_failure',true,'disposition','MANUAL_REVIEW',
          'reason','A_COMMITTED_RECEIPT_CARRIES_THIS_DIGEST_WITH_DIFFERENT_IMMUTABLE_FIELDS',
          'receipt_id',v_receipt.id,
          'request_digest',pg_catalog.encode(v_digest,'hex')));
    end if;
    return pg_catalog.jsonb_build_object(
      'ok',true,'published',true,'replayed',true,
      'receipt',private.weekly_source_publication_receipt_json_v1(v_receipt.id));
  end if;

  -- ---- 2. the immutable accepted decision bundle -------------------------
  select * into v_bundle
    from public.weekly_source_entitlement_decision_bundles as bundle_row
   where bundle_row.decision_bundle_id=v_decision_bundle_id
     and bundle_row.bundle_revision=v_bundle_revision
   for update;
  if not found then
    return pg_catalog.jsonb_build_object(
      'ok',false,'published',false,'replayed',false,
      'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID','retryable',false,
      'detail',pg_catalog.jsonb_build_object('reason','DECISION_BUNDLE_NOT_FOUND',
        'decision_bundle_id',v_decision_bundle_id,'bundle_revision',v_bundle_revision));
  end if;
  if v_bundle.candidate_id<>v_candidate_id
     or v_bundle.decision_id<>v_decision_id
     or v_bundle.proposed_head_ids is distinct from v_head_ids
     or (v_bundle.bundle_kind='SINGLE_ROOT')<>(v_n=1)
     or (v_bundle.bundle_kind='CROSS_CONTRACT_A_B')<>(v_n=2) then
    return pg_catalog.jsonb_build_object(
      'ok',false,'published',false,'replayed',false,
      'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID','retryable',false,
      'detail',pg_catalog.jsonb_build_object('reason','DECISION_BUNDLE_DISAGREES_WITH_REQUEST',
        'bundle_kind',v_bundle.bundle_kind,'member_count',v_n));
  end if;
  if v_bundle.state not in ('PROPOSED','COMMITTED') then
    return pg_catalog.jsonb_build_object(
      'ok',false,'published',false,'replayed',false,
      'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID','retryable',false,
      'detail',pg_catalog.jsonb_build_object('reason','DECISION_BUNDLE_NOT_LIVE',
        'state',v_bundle.state));
  end if;
  -- Review finding F7.  The replay check above runs BEFORE the bundle lock and
  -- matches on the digest, so a SECOND, DIFFERENT request under an already
  -- COMMITTED bundle revision used to pass every pre-write check and be stopped
  -- only by the head primary key, as a raised 23505 that WP-08b would count as
  -- a technical failure.  A committed revision has had its one publication:
  -- anything that is not that exact request is refused here, with a code.
  if v_bundle.state='COMMITTED'
     and not exists (
       select 1
         from private.weekly_source_entitlement_publication_receipts as receipt_row
        where receipt_row.decision_bundle_id=v_decision_bundle_id
          and receipt_row.bundle_revision=v_bundle_revision
          and receipt_row.request_digest=v_digest) then
    return pg_catalog.jsonb_build_object(
      'ok',false,'published',false,'replayed',false,
      'code','WEEKLY_SOURCE_PUBLICATION_BUNDLE_REVISION_ALREADY_PUBLISHED','retryable',false,
      'detail',pg_catalog.jsonb_build_object(
        'reason','THIS_BUNDLE_REVISION_HAS_ALREADY_PUBLISHED_A_DIFFERENT_REQUEST',
        'decision_bundle_id',v_decision_bundle_id,'bundle_revision',v_bundle_revision));
  end if;

  -- ---- 2a0. DEFERRED mode is not trusted on the census (review U5) ---------
  -- The core's contract is "the caller ran the census", but a frozen root must
  -- never publish (24 section 4.4; proof/32 section 7, last bullet), and the
  -- reviewer executed a DEFERRED release that published with
  -- p_census = {"result":"FROZEN"} and a p_pending_bundle_id naming no row at
  -- all.  So the one step where the answer decides whether money moves is
  -- checked here as well as by the caller.
  if v_mode='DEFERRED' then
    if coalesce(p_census->>'result','')<>'RELEASABLE' then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_PUBLICATION_CENSUS_NOT_RELEASABLE','retryable',false,
        'detail',pg_catalog.jsonb_build_object(
          'reason','A_DEFERRED_RELEASE_MAY_ONLY_PUBLISH_ON_A_RELEASABLE_CENSUS',
          'census_result',coalesce(p_census->'result','null'::jsonb)));
    end if;
    select * into v_pending
      from public.weekly_source_pending_entitlement_bundles as pending_row
     where pending_row.id=p_pending_bundle_id
       and pending_row.decision_bundle_id=v_decision_bundle_id
       and pending_row.bundle_revision=v_bundle_revision
       and pending_row.candidate_id=v_candidate_id
     for update;
    if not found then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_PUBLICATION_PENDING_BUNDLE_INVALID','retryable',false,
        'detail',pg_catalog.jsonb_build_object(
          'integrity_failure',true,'disposition','MANUAL_REVIEW',
          'reason','NO_PENDING_BUNDLE_ROW_FOR_THIS_DECISION_BUNDLE_AND_REVISION',
          'pending_bundle_id',p_pending_bundle_id,
          'decision_bundle_id',v_decision_bundle_id,
          'bundle_revision',v_bundle_revision));
    end if;
    -- Decision D10.  The migration cannot bind the pending bundle's stored
    -- request to its stored digest, because such a trigger would have to call
    -- the canonical encoder, which lives in a repeatable, and migrations are
    -- applied first: a rebuild from empty would fail.  The safety therefore
    -- belongs here, where the decision to move money is actually made.  Under
    -- the lock already held, the stored request and the stored digest are
    -- verified TOGETHER, and against the request being released.
    --
    -- Round-5 ruling A1 control 3 is exactly this: "release must lock the row,
    -- recalculate the digest from the stored canonical request and refuse
    -- BEFORE EFFECTS on any mismatch".  The row is locked `for update` above,
    -- the digest is recomputed by this coordinator from the stored request, and
    -- both refusals below happen before the first write.  Control 5 adds that a
    -- tampered row is a PERMANENT integrity failure that goes DIRECTLY to
    -- manual review, so each one carries the disposition the deferred release
    -- owner must honour instead of counting a technical failure.
    if private.weekly_source_publication_request_digest_v1(
         private.weekly_source_publication_request_canonical_v1(
           v_pending.request_json,'DEFERRED',p_pending_bundle_id))
       is distinct from v_pending.request_digest then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_PUBLICATION_PENDING_BUNDLE_INVALID','retryable',false,
        'detail',pg_catalog.jsonb_build_object(
          'integrity_failure',true,'disposition','MANUAL_REVIEW',
          'reason','THE_STORED_REQUEST_DOES_NOT_MATCH_ITS_STORED_DIGEST',
          'pending_bundle_id',p_pending_bundle_id,
          'stored_digest',pg_catalog.encode(v_pending.request_digest,'hex'),
          'recomputed_digest',pg_catalog.encode(
            private.weekly_source_publication_request_digest_v1(
              private.weekly_source_publication_request_canonical_v1(
                v_pending.request_json,'DEFERRED',p_pending_bundle_id)),'hex')));
    end if;
    if v_pending.request_digest is distinct from v_digest then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_PUBLICATION_PENDING_BUNDLE_INVALID','retryable',false,
        'detail',pg_catalog.jsonb_build_object(
          'integrity_failure',true,'disposition','MANUAL_REVIEW',
          'reason','THE_REQUEST_BEING_RELEASED_IS_NOT_THE_ONE_THAT_WAS_SAVED',
          'pending_bundle_id',p_pending_bundle_id,
          'stored_digest',pg_catalog.encode(v_pending.request_digest,'hex'),
          'release_digest',pg_catalog.encode(v_digest,'hex')));
    end if;
  end if;

  -- ---- 2b. the accepted decision BINDS what is published (review U1) -------
  -- 24 section 4.5 step 2 and file 26 Gate 5 step 4 require the coordinator to
  -- revalidate "the current source revision, Contract choices, Candidate,
  -- Client, week, approval digest and the immutable identity of every moved
  -- component".  Quoting the right bundle id, decision id and head ids is not
  -- enough: the request must equal the accepted decision in every identity the
  -- bundle row carries, or a caller could publish a different root, Contract,
  -- week and amount of money under the Office decision's identity and actor.
  v_choice:=(
    select choice_element.value
      from pg_catalog.jsonb_array_elements(
             v_canonical->'financial_request'->'contract_choices') as choice_element(value)
     where (choice_element.value->>'root_ordinal')::integer=1);
  if v_member_root_ids[1] is distinct from v_bundle.source_root_timesheet_id
     or v_member_family_booking_ids[1] is distinct from v_bundle.source_root_family_booking_id
     or (v_choice->>'contract_id')::uuid is distinct from v_bundle.source_contract_id then
    return pg_catalog.jsonb_build_object(
      'ok',false,'published',false,'replayed',false,
      'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID','retryable',false,
      'detail',pg_catalog.jsonb_build_object('reason','SOURCE_ROOT_DISAGREES_WITH_ACCEPTED_DECISION',
        'accepted_root',v_bundle.source_root_timesheet_id,'requested_root',v_member_root_ids[1],
        'accepted_family',v_bundle.source_root_family_booking_id,
        'requested_family',v_member_family_booking_ids[1],
        'accepted_contract',v_bundle.source_contract_id,
        'requested_contract',v_choice->'contract_id'));
  end if;
  v_choice:=null;
  if v_n=2 then
    v_choice:=(
      select choice_element.value
        from pg_catalog.jsonb_array_elements(
               v_canonical->'financial_request'->'contract_choices') as choice_element(value)
       where (choice_element.value->>'root_ordinal')::integer=2);
    if v_member_root_ids[2] is distinct from v_bundle.target_root_timesheet_id
       or v_member_family_booking_ids[2] is distinct from v_bundle.target_root_family_booking_id
       or (v_choice->>'contract_id')::uuid is distinct from v_bundle.target_contract_id then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID','retryable',false,
        'detail',pg_catalog.jsonb_build_object('reason','TARGET_ROOT_DISAGREES_WITH_ACCEPTED_DECISION',
          'accepted_root',v_bundle.target_root_timesheet_id,'requested_root',v_member_root_ids[2],
          'accepted_family',v_bundle.target_root_family_booking_id,
          'requested_family',v_member_family_booking_ids[2],
          'accepted_contract',v_bundle.target_contract_id,
          'requested_contract',v_choice->'contract_id'));
    end if;
    v_choice:=null;
  elsif v_bundle.target_root_timesheet_id is not null
     or v_bundle.target_root_family_booking_id is not null
     or v_bundle.target_contract_id is not null then
    return pg_catalog.jsonb_build_object(
      'ok',false,'published',false,'replayed',false,
      'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID','retryable',false,
      'detail',pg_catalog.jsonb_build_object(
        'reason','SINGLE_ROOT_BUNDLE_CARRIES_A_TARGET_ROOT'));
  end if;

  -- The week is the accepted decision's, and it is also each root Timesheet's
  -- own week and Contract.  One ordinary root per Candidate, Contract and week
  -- (contract section 5) only means anything if the week is checked.
  for v_i in 1..v_n loop
    v_choice:=(
      select choice_element.value
        from pg_catalog.jsonb_array_elements(
               v_canonical->'financial_request'->'contract_choices') as choice_element(value)
       where (choice_element.value->>'root_ordinal')::integer=v_i);
    if (v_choice->>'week_ending_date')::date is distinct from v_bundle.week_ending_date
       or not exists (
         select 1
           from public.timesheets as root_row
          where root_row.timesheet_id=v_member_root_ids[v_i]
            and root_row.week_ending_date=v_bundle.week_ending_date
            and root_row.contract_id=(v_choice->>'contract_id')::uuid) then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID','retryable',false,
        'detail',pg_catalog.jsonb_build_object('reason','WEEK_OR_CONTRACT_DISAGREES_WITH_ACCEPTED_DECISION',
          'root_ordinal',v_i,'accepted_week',v_bundle.week_ending_date,
          'requested_week',v_choice->'week_ending_date',
          'requested_contract',v_choice->'contract_id'));
    end if;
    -- The Candidate must actually OWN the root.  The reviewer published a head,
    -- a receipt and an invalidation under a second Candidate for a root it did
    -- not own: the installed invalidator's ownership rule fires only when a
    -- current TSFIN row exists, so the coordinator checks the Contract itself
    -- rather than leaving it to I-1 or to Banking Pay.
    if not exists (
      select 1
        from public.contracts as contract_row
       where contract_row.id=(v_choice->>'contract_id')::uuid
         and contract_row.candidate_id=v_candidate_id) then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID','retryable',false,
        'detail',pg_catalog.jsonb_build_object('reason','CANDIDATE_DOES_NOT_OWN_THE_ROOT',
          'root_ordinal',v_i,'candidate_id',v_candidate_id,
          'contract_id',v_choice->'contract_id'));
    end if;
    v_choice:=null;
  end loop;

  -- The three approval digests the bundle row carries, recomputed here with the
  -- one canonical encoder.  These are mode-independent by construction, so the
  -- same values bind an immediate publication and a deferred release.
  v_source_generation_digest:=private.weekly_source_publication_request_digest_v1(
    v_canonical->'financial_request'->'source_revision');
  v_contract_choice_digest:=private.weekly_source_publication_request_digest_v1(
    v_canonical->'financial_request'->'contract_choices');
  v_before_inventory:=private.weekly_source_publication_before_inventory_v1(v_control,v_n);
  v_before_inventory_digest:=private.weekly_source_publication_request_digest_v1(v_before_inventory);
  -- The acceptance digest: the canonical request in IMMEDIATE mode with no
  -- pending bundle.  publication_mode and pending_bundle_id are digest fields
  -- (proof/32 section 9), so the digest the RECEIPT carries changes with the
  -- mode and cannot be the one stored on the accepted decision.  The bundle
  -- row's request_digest is therefore always taken in IMMEDIATE/null, which is
  -- what the proposal composer can compute at proposal time, before any pending
  -- bundle exists.  Written into interfaces\PUBLICATION_REQUEST_SHAPE.md.
  v_acceptance_digest:=private.weekly_source_publication_request_digest_v1(
    private.weekly_source_publication_request_canonical_v1(p_request,'IMMEDIATE',null::uuid));
  if v_bundle.source_revision_digest is distinct from v_source_generation_digest
     or v_bundle.contract_choice_digest is distinct from v_contract_choice_digest
     or v_bundle.before_inventory_digest is distinct from v_before_inventory_digest
     or v_bundle.request_digest is distinct from v_acceptance_digest then
    return pg_catalog.jsonb_build_object(
      'ok',false,'published',false,'replayed',false,
      'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID','retryable',false,
      'detail',pg_catalog.jsonb_build_object('reason','APPROVAL_DIGESTS_DISAGREE_WITH_ACCEPTED_DECISION',
        'source_revision_digest_matches',
          v_bundle.source_revision_digest is not distinct from v_source_generation_digest,
        'contract_choice_digest_matches',
          v_bundle.contract_choice_digest is not distinct from v_contract_choice_digest,
        'before_inventory_digest_matches',
          v_bundle.before_inventory_digest is not distinct from v_before_inventory_digest,
        'request_digest_matches',
          v_bundle.request_digest is not distinct from v_acceptance_digest));
  end if;

  -- ---- 2a. the current source revision (24 section 4.5 step 2; R7) --------
  -- "revalidate the current source revision".  A head built from a superseded
  -- or merely prepared final revision must never become current.
  if not exists (
    select 1
      from public.weekly_source_final_revisions as revision_row
     where revision_row.id=(v_canonical->'financial_request'->'source_revision'->>'final_revision_id')::uuid
       and revision_row.source_cycle_id=(v_canonical->'financial_request'->'source_revision'->>'source_cycle_id')::uuid
       and revision_row.revision_number=(v_canonical->'financial_request'->'source_revision'->>'revision_number')::integer
       and revision_row.state='CURRENT'
       and pg_catalog.encode(revision_row.manifest_hash,'hex')
           =(v_canonical->'financial_request'->'source_revision'->>'manifest_hash')
       and pg_catalog.encode(revision_row.policy_fingerprint,'hex')
           =(v_canonical->'financial_request'->'source_revision'->>'policy_fingerprint')
  ) then
    return pg_catalog.jsonb_build_object(
      'ok',false,'published',false,'replayed',false,
      'code','WEEKLY_SOURCE_PUBLICATION_SOURCE_REVISION_STALE','retryable',false,
      'detail',pg_catalog.jsonb_build_object(
        'source_revision',v_canonical->'financial_request'->'source_revision'));
  end if;

  -- ---- 3. member identity against the I-1 lock result ---------------------
  -- proof/32 section 4.0 integrity gate; proof/34 section 6: after first
  -- authorisation an unexpected rotation is an INTEGRITY FAILURE, never a stale
  -- rebuild.  This coordinator never rebuilds anything.
  if p_lock_result is null
     or pg_catalog.jsonb_typeof(p_lock_result)<>'object'
     or coalesce((p_lock_result->>'ok')::boolean,false) is not true
     or coalesce(p_lock_result->>'gate','')<>'GRANTED' then
    return pg_catalog.jsonb_build_object(
      'ok',false,'published',false,'replayed',false,
      'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID','retryable',false,
      'detail',pg_catalog.jsonb_build_object('reason','LOCK_RESULT_NOT_GRANTED'));
  end if;

  for v_i in 1..v_n loop
    select family_element.value into v_family
      from pg_catalog.jsonb_array_elements(p_lock_result->'families') as family_element(value)
     where family_element.value->>'requested_timesheet_id'=v_member_root_ids[v_i]::text;
    if v_family is null
       or coalesce((v_family->>'requested_is_canonical')::boolean,false) is not true
       or coalesce((v_family->>'family_is_current')::boolean,false) is not true
       or (v_family->>'canonical_timesheet_id')::uuid is distinct from v_member_root_ids[v_i]
       or (v_family->>'canonical_version')::integer is distinct from v_member_root_versions[v_i]
       or pg_catalog.btrim(coalesce(v_family->>'family_booking_id',''))
          is distinct from pg_catalog.btrim(v_member_family_booking_ids[v_i]) then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE','retryable',false,
        'detail',pg_catalog.jsonb_build_object(
          'root_ordinal',v_i,'requested_timesheet_id',v_member_root_ids[v_i],
          'lock_result_family',v_family));
    end if;
    select v_member_timesheet_ids||coalesce(
             pg_catalog.array_agg(member_element.value::uuid),array[]::uuid[])
      into v_member_timesheet_ids
      from pg_catalog.jsonb_array_elements_text(
             coalesce(v_family->'member_timesheet_ids','[]'::jsonb)) as member_element(value);

    -- WP-01a review U1: `root_family_booking_id` is free text on the head row and
    -- nothing in the schema ties it to the root's real booking_id, so the
    -- coordinator validates it itself.  The raw value must equal
    -- public.timesheets.booking_id of the canonical root BYTE FOR BYTE: the
    -- trimmed form is the lock and index key (proof/32 section 6 step 2), not an
    -- identity, and a differently cased or unrelated string would take a
    -- different advisory lock from the one protecting the uniqueness index.
    if not exists (
      select 1
        from public.timesheets as root_row
       where root_row.timesheet_id=v_member_root_ids[v_i]
         and root_row.booking_id=v_member_family_booking_ids[v_i]
         and root_row.version=v_member_root_versions[v_i]
         and root_row.is_current) then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE','retryable',false,
        'detail',pg_catalog.jsonb_build_object(
          'reason','FAMILY_BOOKING_ID_DOES_NOT_MATCH_THE_ROOT',
          'root_ordinal',v_i,'root_timesheet_id',v_member_root_ids[v_i],
          'declared_family_booking_id',v_member_family_booking_ids[v_i],
          'declared_version',v_member_root_versions[v_i]));
    end if;
    v_family:=null;
  end loop;

  -- ---- 4. compare-and-swap on each member's expected current head ---------
  -- jsonb_array_elements_text turns a JSON null into a SQL NULL, which is
  -- exactly "the caller believes this root has no committed head yet".
  select pg_catalog.array_agg(expected_element.value::uuid
                              order by expected_element.ordinality)
    into v_expected_head_ids
    from pg_catalog.jsonb_array_elements_text(
           coalesce(v_control->'expected_current_head_ids','[]'::jsonb))
         with ordinality as expected_element(value,ordinality);
  if coalesce(pg_catalog.cardinality(v_expected_head_ids),0)<>v_n then
    return pg_catalog.jsonb_build_object(
      'ok',false,'published',false,'replayed',false,
      'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID','retryable',false,
      'detail',pg_catalog.jsonb_build_object(
        'reason','EXPECTED_CURRENT_HEAD_IDS_NOT_ALIGNED'));
  end if;

  for v_i in 1..v_n loop
    -- One committed current head per root across both authority kinds
    -- (24 section 4.3), read under lock (H2-024).
    --
    -- WP-01a review U1: "at most one committed current head" is a partial unique
    -- index on the TRIMMED family string only, so it is not a schema guarantee
    -- for a physical root.  The coordinator therefore counts the rows itself,
    -- refuses more than one as an integrity failure rather than choosing one,
    -- and asserts that the head it found belongs to the canonical root I-1
    -- resolved.  It never falls back to stale TSFIN (27 section 5.1).
    select pg_catalog.count(*)::integer into v_rows
      from public.weekly_source_entitlement_heads as head_row
     where pg_catalog.btrim(head_row.root_family_booking_id)
           =pg_catalog.btrim(v_member_family_booking_ids[v_i])
       and head_row.state='COMMITTED_CURRENT';
    if v_rows>1 then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE','retryable',false,
        'detail',pg_catalog.jsonb_build_object(
          'reason','MORE_THAN_ONE_COMMITTED_CURRENT_HEAD',
          'root_ordinal',v_i,'committed_current_heads',v_rows));
    end if;
    v_current_head_ids:=v_current_head_ids||(
      select head_row.id
        from public.weekly_source_entitlement_heads as head_row
       where pg_catalog.btrim(head_row.root_family_booking_id)
             =pg_catalog.btrim(v_member_family_booking_ids[v_i])
         and head_row.state='COMMITTED_CURRENT'
         for update);
    if v_current_head_ids[v_i] is not null
       and not exists (
         select 1
           from public.weekly_source_entitlement_heads as head_row
          where head_row.id=v_current_head_ids[v_i]
            and head_row.root_timesheet_id=v_member_root_ids[v_i]) then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE','retryable',false,
        'detail',pg_catalog.jsonb_build_object(
          'reason','COMMITTED_HEAD_BELONGS_TO_ANOTHER_PHYSICAL_ROOT',
          'root_ordinal',v_i,'current_head_id',v_current_head_ids[v_i]));
    end if;
    -- The MIRROR of the guard above, and the same defect class as WP-06 review
    -- finding F2: the family key found no head, but the PHYSICAL root carries
    -- one.  Since schema change S8 removed unique(root_timesheet_id) from the
    -- family relation, nothing keyed on the physical id is unique any more, so
    -- the two identities really can disagree.  Falling through here would take a
    -- TSFIN — or an unproved — before-position for a root that demonstrably HAS
    -- a committed head, understating the effective entitlement, which is exactly
    -- how a residual turns into an overpayment.  It costs one indexed lookup
    -- (weekly_source_entitlement_heads_committed_root_uq), so it is checked
    -- rather than assumed, and it is checked HERE so it covers the I-7 branch
    -- and the no-I-7 branch alike.
    if v_current_head_ids[v_i] is null
       and exists (
         select 1
           from public.weekly_source_entitlement_heads as head_row
          where head_row.root_timesheet_id=v_member_root_ids[v_i]
            and head_row.state='COMMITTED_CURRENT') then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE','retryable',false,
        'detail',pg_catalog.jsonb_build_object(
          'reason','COMMITTED_HEAD_EXISTS_FOR_THE_PHYSICAL_ROOT_BUT_NOT_THE_DECLARED_FAMILY',
          'root_ordinal',v_i,'root_timesheet_id',v_member_root_ids[v_i],
          'declared_family_booking_id',v_member_family_booking_ids[v_i],
          'physical_root_head_id',(
            select head_row.id
              from public.weekly_source_entitlement_heads as head_row
             where head_row.root_timesheet_id=v_member_root_ids[v_i]
               and head_row.state='COMMITTED_CURRENT')));
    end if;
    if v_current_head_ids[v_i] is distinct from v_expected_head_ids[v_i] then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_PUBLICATION_HEAD_CAS_CONFLICT','retryable',false,
        'detail',pg_catalog.jsonb_build_object(
          'root_ordinal',v_i,
          'expected_current_head_id',v_expected_head_ids[v_i],
          'actual_current_head_id',v_current_head_ids[v_i]));
    end if;
    select coalesce(pg_catalog.max(head_row.head_revision),0)+1
      into v_head_revision
      from public.weekly_source_entitlement_heads as head_row
     where pg_catalog.btrim(head_row.root_family_booking_id)
           =pg_catalog.btrim(v_member_family_booking_ids[v_i]);
    v_head_revisions:=v_head_revisions||v_head_revision;
    v_prior_head_ids:=v_prior_head_ids||(
      select head_row.id
        from public.weekly_source_entitlement_heads as head_row
       where pg_catalog.btrim(head_row.root_family_booking_id)
             =pg_catalog.btrim(v_member_family_booking_ids[v_i])
       order by head_row.head_revision desc
       limit 1);
  end loop;

  -- ---- 5. the complete post-decision head proofs (H2-024) -----------------
  for v_i in 1..v_n loop
    v_entitlement:=(
      select entitlement_element.value
        from pg_catalog.jsonb_array_elements(
               v_canonical->'financial_request'->'member_entitlements') as entitlement_element(value)
       where (entitlement_element.value->>'root_ordinal')::integer=v_i);
    select coalesce(pg_catalog.array_agg(
             (component_element.value->>'component_id')::uuid
             order by (component_element.value->>'component_ordinal')::integer),
             array[]::uuid[])
      into v_component_ids
      from pg_catalog.jsonb_array_elements(v_entitlement->'components') as component_element(value);

    select coalesce(pg_catalog.array_agg(before_element.value::uuid),array[]::uuid[])
      into v_before_ids
      from pg_catalog.jsonb_array_elements_text(
             coalesce((select position_element.value->'component_ids'
                         from pg_catalog.jsonb_array_elements(
                                coalesce(v_control->'before_positions','[]'::jsonb)) as position_element(value)
                        where (position_element.value->>'root_ordinal')::integer=v_i),
                      'null'::jsonb)) as before_element(value);
    if (select position_element.value
          from pg_catalog.jsonb_array_elements(
                 coalesce(v_control->'before_positions','[]'::jsonb)) as position_element(value)
         where (position_element.value->>'root_ordinal')::integer=v_i) is null then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID','retryable',false,
        'detail',pg_catalog.jsonb_build_object(
          'reason','BEFORE_POSITION_MISSING','root_ordinal',v_i));
    end if;

    -- A head may never contain an adjustment, whatever key it arrives under
    -- (24 section 5; WB-007, WB-013; review U3 case H6c).  component_kind,
    -- economic_key_type and origin are free text on the relation, so the
    -- coordinator allowlists the kind and refuses the adjustment vocabulary
    -- outright rather than trusting the absence of an `adjustment_id` key.
    v_offending:=(
      select component_element.value
        from pg_catalog.jsonb_array_elements(v_entitlement->'components') as component_element(value)
       where (component_element.value->>'component_kind') not in
               ('WORKED_TIME','ADDITIONAL_UNIT','SOURCE_FIXED_EXPENSE')
          or component_element.value->>'component_kind' ~* 'ADJUST|ADVANCE'
          or component_element.value->>'economic_key_type' ~* 'ADJUST|ADVANCE'
          or component_element.value->>'origin' ~* 'ADJUST|ADVANCE'
       limit 1);
    if v_offending is not null then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_PUBLICATION_COMPONENT_KIND_FORBIDDEN','retryable',false,
        'detail',pg_catalog.jsonb_build_object(
          'root_ordinal',v_i,'component',v_offending,
          'permitted_kinds',pg_catalog.jsonb_build_array(
            'WORKED_TIME','ADDITIONAL_UNIT','SOURCE_FIXED_EXPENSE')));
    end if;
    v_offending:=null;

    -- H2-024: "each before-position is read from the single committed effective
    -- authority under lock".  Three cases, and only the third is ever accepted
    -- without a server-side read (review U3).
    if v_current_head_ids[v_i] is not null then
      -- (1) A committed head exists: the declared position must equal its
      -- component set EXACTLY, and every RETAINED component must be
      -- byte-identical - the committed row stores component_sha256, so
      -- re-pricing a component that is merely "kept by id" is detectable and
      -- is refused (review U3 cases H10 and H4c).
      if exists (
        select 1
          from (select component_row.component_id
                  from public.weekly_source_entitlement_head_components as component_row
                 where component_row.head_id=v_current_head_ids[v_i]
                except
                select pg_catalog.unnest(v_before_ids)) as missing_from_declared
        union all
        select 1
          from (select pg_catalog.unnest(v_before_ids)
                except
                select component_row.component_id
                  from public.weekly_source_entitlement_head_components as component_row
                 where component_row.head_id=v_current_head_ids[v_i]) as extra_in_declared
      ) then
        return pg_catalog.jsonb_build_object(
          'ok',false,'published',false,'replayed',false,
          'code','WEEKLY_SOURCE_PUBLICATION_BEFORE_POSITION_MISMATCH','retryable',false,
          'detail',pg_catalog.jsonb_build_object(
            'root_ordinal',v_i,'current_head_id',v_current_head_ids[v_i]));
      end if;
      -- `component_sha256` is a CONTENT identity and deliberately excludes
      -- `component_ordinal`: the ordinal is the component's position inside a
      -- head, and a component that is retained while another moves out from in
      -- front of it legitimately changes position.  Position is still covered,
      -- by `inventory_digest` (the ordered ordinal/id pairs) and by
      -- `entitlement_digest` (the content hashes in ordinal order).
      v_offending:=(
        select pg_catalog.jsonb_build_object(
                 'component_id',component_element.value->'component_id',
                 'committed_sha256',pg_catalog.encode(committed_row.component_sha256,'hex'),
                 'requested_sha256',pg_catalog.encode(
                   private.weekly_source_publication_request_digest_v1(
                     private.weekly_source_publication_component_content_v1(component_element.value)),'hex'))
          from pg_catalog.jsonb_array_elements(v_entitlement->'components') as component_element(value)
          join public.weekly_source_entitlement_head_components as committed_row
            on committed_row.head_id=v_current_head_ids[v_i]
           and committed_row.component_id=(component_element.value->>'component_id')::uuid
         where committed_row.component_sha256
               is distinct from private.weekly_source_publication_request_digest_v1(
                     private.weekly_source_publication_component_content_v1(component_element.value))
         limit 1);
      if v_offending is not null then
        return pg_catalog.jsonb_build_object(
          'ok',false,'published',false,'replayed',false,
          'code','WEEKLY_SOURCE_PUBLICATION_RETAINED_COMPONENT_CHANGED','retryable',false,
          'detail',pg_catalog.jsonb_build_object('root_ordinal',v_i,'component',v_offending));
      end if;
      v_offending:=null;
      -- WP-06c review F1: member 1 is the SOURCE of a cross-Contract move, so
      -- its committed head is the authority for what every MOVED component
      -- actually held.  The content hashes are taken here, under the same lock
      -- and from the same rows the retained check just used, and are compared
      -- against the destination side after the set proofs.
      if v_i=1 then
        select coalesce(pg_catalog.jsonb_object_agg(
                 committed_row.component_id::text,
                 pg_catalog.encode(committed_row.component_sha256,'hex')),'{}'::jsonb)
          into v_source_component_hashes
          from public.weekly_source_entitlement_head_components as committed_row
         where committed_row.head_id=v_current_head_ids[v_i];
      end if;
      v_before_source:=v_before_source||'HEAD'::text;

    elsif pg_catalog.to_regprocedure(
            'private.weekly_source_effective_inventory_v1(uuid)') is not null then
      -- (2) No head yet, but interface I-7 exists (decision D9, built by WP-06):
      -- the before-position is derived SERVER-SIDE from the single committed
      -- effective authority - the ordinary financial snapshot - under the same
      -- locks, and the declared position must equal it.
      v_effective:=private.weekly_source_effective_inventory_v1(v_member_root_ids[v_i]);
      if coalesce(pg_catalog.jsonb_typeof(v_effective),'')<>'object' then
        raise exception 'WEEKLY_SOURCE_PUBLICATION_EFFECTIVE_INVENTORY_INVALID'
          using errcode='55000',
                detail=pg_catalog.jsonb_build_object(
                  'code','WEEKLY_SOURCE_PUBLICATION_EFFECTIVE_INVENTORY_INVALID',
                  'root_ordinal',v_i,'result',v_effective)::text;
      end if;
      -- I-7 answers `ok:false` with an EMPTY components array when it cannot
      -- resolve the root.  Reading that as "the root holds nothing" would turn
      -- an integrity failure into a silent certified-zero publication, so the
      -- refusal is honoured before the sets are compared at all.
      if coalesce((v_effective->>'ok')::boolean,false) is not true then
        return pg_catalog.jsonb_build_object(
          'ok',false,'published',false,'replayed',false,
          'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE','retryable',false,
          'detail',pg_catalog.jsonb_build_object(
            'reason','INTERFACE_I7_REFUSED_THE_ROOT',
            'root_ordinal',v_i,'root_timesheet_id',v_member_root_ids[v_i],
            'effective_inventory',v_effective));
      end if;
      -- I-7 is reached ONLY when the family key above found no committed head,
      -- so the only authority it can legitimately report here is the ordinary
      -- financial snapshot.  If it reports a HEAD, the two keyings disagree
      -- about the same root — the shape of WP-06 review finding F2, in whichever
      -- direction the disagreement runs — and the coordinator must not pick a
      -- winner between them.  It refuses, cheaply, on a field it already holds.
      if v_effective->>'authority' is distinct from 'TSFIN' then
        return pg_catalog.jsonb_build_object(
          'ok',false,'published',false,'replayed',false,
          'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE','retryable',false,
          'detail',pg_catalog.jsonb_build_object(
            'reason','INTERFACE_I7_REPORTS_AN_AUTHORITY_THE_FAMILY_KEY_DOES_NOT',
            'root_ordinal',v_i,'root_timesheet_id',v_member_root_ids[v_i],
            'declared_family_booking_id',v_member_family_booking_ids[v_i],
            'i7_authority',v_effective->'authority','i7_head_id',v_effective->'head_id'));
      end if;
      if exists (
        select 1
          from (select (effective_element.value->>'component_id')::uuid as component_id
                  from pg_catalog.jsonb_array_elements(
                         coalesce(v_effective->'components','[]'::jsonb)) as effective_element(value)
                except
                select pg_catalog.unnest(v_before_ids)) as missing_from_declared
        union all
        select 1
          from (select pg_catalog.unnest(v_before_ids)
                except
                select (effective_element.value->>'component_id')::uuid
                  from pg_catalog.jsonb_array_elements(
                         coalesce(v_effective->'components','[]'::jsonb)) as effective_element(value)) as extra_in_declared
      ) then
        return pg_catalog.jsonb_build_object(
          'ok',false,'published',false,'replayed',false,
          'code','WEEKLY_SOURCE_PUBLICATION_BEFORE_POSITION_MISMATCH','retryable',false,
          'detail',pg_catalog.jsonb_build_object(
            'root_ordinal',v_i,'authority',v_effective->'authority',
            'reason','DECLARED_POSITION_DIFFERS_FROM_THE_EFFECTIVE_INVENTORY'));
      end if;
      -- I-7 also carries each component's content hash, so a component RETAINED
      -- from a head-less root gets the same byte-identity guarantee as one
      -- retained from a committed head (review finding U3(a), extended).
      v_offending:=(
        select pg_catalog.jsonb_build_object(
                 'component_id',component_element.value->'component_id',
                 'effective_sha256',effective_element.value->'component_sha256',
                 'requested_sha256',pg_catalog.encode(
                   private.weekly_source_publication_request_digest_v1(
                     private.weekly_source_publication_component_content_v1(
                       component_element.value)),'hex'))
          from pg_catalog.jsonb_array_elements(v_entitlement->'components') as component_element(value)
          join pg_catalog.jsonb_array_elements(
                 coalesce(v_effective->'components','[]'::jsonb)) as effective_element(value)
            on effective_element.value->>'component_id'=component_element.value->>'component_id'
         where effective_element.value->>'component_sha256'
               is distinct from pg_catalog.encode(
                 private.weekly_source_publication_request_digest_v1(
                   private.weekly_source_publication_component_content_v1(
                     component_element.value)),'hex')
         limit 1);
      if v_offending is not null then
        return pg_catalog.jsonb_build_object(
          'ok',false,'published',false,'replayed',false,
          'code','WEEKLY_SOURCE_PUBLICATION_RETAINED_COMPONENT_CHANGED','retryable',false,
          'detail',pg_catalog.jsonb_build_object(
            'root_ordinal',v_i,'authority',v_effective->'authority',
            'component',v_offending));
      end if;
      v_offending:=null;
      -- WP-06c review F1, the head-less source: I-7 carries each component's
      -- own content hash, computed through this file's canonicaliser, content
      -- projection and encoder, so it is directly comparable with a committed
      -- head row's `component_sha256` and with the destination side.
      if v_i=1 then
        select coalesce(pg_catalog.jsonb_object_agg(
                 effective_element.value->>'component_id',
                 effective_element.value->'component_sha256'),'{}'::jsonb)
          into v_source_component_hashes
          from pg_catalog.jsonb_array_elements(
                 coalesce(v_effective->'components','[]'::jsonb)) as effective_element(value);
      end if;
      v_before_source:=v_before_source||'I7'::text;

    else
      -- (3) No head and no I-7.  The declared position cannot be proved, so it
      -- is never recorded as if it were.  An A-to-B bundle is REFUSED rather
      -- than published on an unverified before-position, because that is where
      -- "minus only the moved components" and "keeps every existing component"
      -- would otherwise rest on the caller's word (review U3 case G1-a).
      if v_n=2 then
        return pg_catalog.jsonb_build_object(
          'ok',false,'published',false,'replayed',false,
          'code','WEEKLY_SOURCE_PUBLICATION_BEFORE_POSITION_UNPROVABLE','retryable',false,
          'detail',pg_catalog.jsonb_build_object(
            'root_ordinal',v_i,'root_timesheet_id',v_member_root_ids[v_i],
            'reason','NO_COMMITTED_HEAD_AND_INTERFACE_I7_IS_NOT_INSTALLED',
            'required_owner','private.weekly_source_effective_inventory_v1(uuid)'));
      end if;
      v_before_source:=v_before_source||'DECLARED_UNPROVED'::text;
    end if;

    if v_i=1 then
      v_after_1:=v_component_ids; v_before_1:=v_before_ids;
    else
      v_after_2:=v_component_ids; v_before_2:=v_before_ids;
    end if;
    v_entitlement:=null;
  end loop;

  select coalesce(pg_catalog.array_agg(moved_element.value::uuid),array[]::uuid[])
    into v_moved
    from pg_catalog.jsonb_array_elements_text(
           coalesce(v_control->'moved_component_ids','[]'::jsonb)) as moved_element(value);

  if v_n=1 then
    if pg_catalog.cardinality(v_moved)>0 then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_PUBLICATION_MOVE_SET_INVALID','retryable',false,
        'detail',pg_catalog.jsonb_build_object('reason','MOVED_SET_ON_SINGLE_ROOT_BUNDLE'));
    end if;
  else
    -- "an already-authorised B keeps EVERY existing component" (24 section 4.5
    -- step 3).  This is tested FIRST so that a B which loses one gets the exact
    -- code the interface documents.  Placed after the combined set proof it was
    -- unreachable, because `B-after = B-before union moved` already implies
    -- `B-before is a subset of B-after` (review finding F12).
    if pg_catalog.cardinality(
         private.weekly_source_uuid_set_difference_v1(v_before_2,v_after_2))>0 then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_PUBLICATION_TARGET_COMPONENT_DROPPED','retryable',false,
        'detail',pg_catalog.jsonb_build_object(
          'dropped',pg_catalog.to_jsonb(
            private.weekly_source_uuid_set_difference_v1(v_before_2,v_after_2))));
    end if;
    -- 24 section 4.5 step 3 and H2-024, proved in the same transaction.
    if pg_catalog.cardinality(v_moved)=0
       or not private.weekly_source_uuid_array_is_distinct_v1(v_moved)
       or private.weekly_source_uuid_set_equals_v1(
            v_before_1,private.weekly_source_uuid_set_union_v1(v_after_1,v_moved)) is not true
       or pg_catalog.cardinality(private.weekly_source_uuid_set_intersect_v1(v_after_1,v_moved))>0
       or pg_catalog.cardinality(private.weekly_source_uuid_set_intersect_v1(v_before_2,v_moved))>0
       or private.weekly_source_uuid_set_equals_v1(
            v_after_2,private.weekly_source_uuid_set_union_v1(v_before_2,v_moved)) is not true then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_PUBLICATION_MOVE_SET_INVALID','retryable',false,
        'detail',pg_catalog.jsonb_build_object(
          'a_before',pg_catalog.to_jsonb(v_before_1),'a_after',pg_catalog.to_jsonb(v_after_1),
          'b_before',pg_catalog.to_jsonb(v_before_2),'b_after',pg_catalog.to_jsonb(v_after_2),
          'moved',pg_catalog.to_jsonb(v_moved)));
    end if;

    -- Round-5 ruling, Part E, "Partial Contract-to-Contract move":
    --
    --   "Not in scope for this release.  The supported operation is the
    --    whole-entitlement move.  A requested partial move must take the named
    --    fail-closed path and explain that partial movement is unsupported; it
    --    must not approximate the move."
    --
    -- THE DEFINITION IS THE WHOLE OF THE RULE.  A move is WHOLE when the source
    -- member retains nothing after it, and PARTIAL otherwise.  Three statements
    -- of that are available here and they are the SAME statement, because the
    -- set proofs immediately above have already established
    -- `A_after = A_before \ moved`:
    --
    --   (i)   A-after is empty                     -> cardinality(v_after_1)=0
    --   (ii)  every component of A-before moved    -> moved = A_before
    --   (iii) member 1 is certified zero           -> the declared flag, which
    --         the coordinator re-checks against `component_count` below and
    --         which the head relation's own CHECK enforces
    --
    -- (i) is the one tested, because it is read from what the request actually
    -- carries rather than from a flag the caller sets.  (ii) is asserted
    -- alongside it: if the two ever disagreed, the set proofs above would have
    -- been wrong, so the disagreement is itself refused rather than resolved.
    --
    -- This is a SCOPE gate, so it is tested before the content proof below: a
    -- partial move is refused for being partial, in plain English, not for a
    -- hash mismatch it may not even have.  It is a returned refusal before the
    -- first write, exactly like every other check in this section.
    if pg_catalog.cardinality(v_after_1)>0
       or private.weekly_source_uuid_set_equals_v1(v_moved,v_before_1) is not true then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_PUBLICATION_PARTIAL_MOVE_UNSUPPORTED','retryable',false,
        'detail',pg_catalog.jsonb_build_object(
          'reason','ONLY_A_WHOLE_ENTITLEMENT_MOVE_IS_SUPPORTED_IN_THIS_RELEASE',
          'message','This bundle moves only part of the entitlement from one Contract '
                  ||'to the other and leaves the rest behind. Moving part of an '
                  ||'entitlement is not supported in this release: a Contract-to-Contract '
                  ||'amendment must move the whole entitlement, so that the old Contract '
                  ||'is left holding nothing. Nothing has been published. Either move '
                  ||'every component of the old Contract''s entitlement, or leave the '
                  ||'entitlement where it is.',
          'definition','A move is WHOLE when the source root retains nothing after it.',
          'a_before',pg_catalog.to_jsonb(v_before_1),
          'a_after',pg_catalog.to_jsonb(v_after_1),
          'a_after_component_count',pg_catalog.cardinality(v_after_1),
          'moved',pg_catalog.to_jsonb(v_moved),
          'moved_is_the_whole_source_position',
            private.weekly_source_uuid_set_equals_v1(v_moved,v_before_1)));
    end if;

    -- WP-06c review finding F1 (HIGH).  Everything above proves the move by
    -- IDENTITY.  A moved component's CONTENT was never compared with anything:
    -- it is absent from A-after and from B-before, so neither retained-component
    -- check above joins it, and a request composed outside the builder could
    -- re-price a shift on its way from A to B and be published with a receipt
    -- (executed: 85.50 -> 850.50 and 4.25 -> 40 hours).  The coordinator is the
    -- last gate before an entitlement is published, so it verifies rather than
    -- trusts: every moved component must still equal, byte for byte, what the
    -- SOURCE authority actually held - A's committed head row's
    -- `component_sha256` where A has a head, else interface I-7's own content
    -- hash for the same component.  `24 section 4.5` step 3 is "B-after is B's
    -- previously effective complete entitlement plus only those component(s)" -
    -- THOSE components, not components with those identities.
    --
    -- The comparison uses the same content projection as the retained checks, so
    -- `component_ordinal`, `movement_id` and `movement_group_id` - the three
    -- things a move legitimately changes - are excluded, and nothing else is.
    -- A moved component the source authority does not carry at all has a null
    -- hash here and is refused by the same test (`is distinct from`).
    v_entitlement:=(
      select entitlement_element.value
        from pg_catalog.jsonb_array_elements(
               v_canonical->'financial_request'->'member_entitlements') as entitlement_element(value)
       where (entitlement_element.value->>'root_ordinal')::integer=2);
    select pg_catalog.count(*)::integer into v_moved_found
      from pg_catalog.jsonb_array_elements(v_entitlement->'components') as component_element(value)
     where (component_element.value->>'component_id')::uuid=any(v_moved);
    if v_moved_found<>pg_catalog.cardinality(v_moved) then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_PUBLICATION_MOVE_SET_INVALID','retryable',false,
        'detail',pg_catalog.jsonb_build_object(
          'reason','THE_DESTINATION_DOES_NOT_CARRY_EACH_MOVED_COMPONENT_EXACTLY_ONCE',
          'moved',pg_catalog.to_jsonb(v_moved),
          'moved_components_in_b_after',v_moved_found));
    end if;
    v_offending:=(
      select pg_catalog.jsonb_build_object(
               'component_id',component_element.value->'component_id',
               'source_authority',v_before_source[1],
               'source_sha256',pg_catalog.to_jsonb(
                 v_source_component_hashes->>(component_element.value->>'component_id')),
               'requested_sha256',pg_catalog.encode(
                 private.weekly_source_publication_request_digest_v1(
                   private.weekly_source_publication_component_content_v1(
                     component_element.value)),'hex'))
        from pg_catalog.jsonb_array_elements(v_entitlement->'components') as component_element(value)
       where (component_element.value->>'component_id')::uuid=any(v_moved)
         and (v_source_component_hashes->>(component_element.value->>'component_id'))
             is distinct from pg_catalog.encode(
               private.weekly_source_publication_request_digest_v1(
                 private.weekly_source_publication_component_content_v1(
                   component_element.value)),'hex')
       limit 1);
    if v_offending is not null then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_PUBLICATION_MOVED_COMPONENT_CHANGED','retryable',false,
        'detail',pg_catalog.jsonb_build_object(
          'root_ordinal',2,'component',v_offending));
    end if;
    v_offending:=null;
    v_entitlement:=null;
  end if;

  -- Movement identity: every moved component carries movement_id in its
  -- destination head and no retained component carries one.
  for v_i in 1..v_n loop
    v_entitlement:=(
      select entitlement_element.value
        from pg_catalog.jsonb_array_elements(
               v_canonical->'financial_request'->'member_entitlements') as entitlement_element(value)
       where (entitlement_element.value->>'root_ordinal')::integer=v_i);
    if exists (
      select 1
        from pg_catalog.jsonb_array_elements(v_entitlement->'components') as component_element(value)
       where ((component_element.value->>'component_id')::uuid=any(v_moved)
              and v_i=2
              and component_element.value->'movement_id'='null'::jsonb)
          or (not ((component_element.value->>'component_id')::uuid=any(v_moved) and v_i=2)
              and component_element.value->'movement_id'<>'null'::jsonb)
    ) then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_PUBLICATION_MOVEMENT_IDENTITY_INVALID','retryable',false,
        'detail',pg_catalog.jsonb_build_object('root_ordinal',v_i));
    end if;
    v_entitlement:=null;
  end loop;

  -- ---- 6. the target root's authorisation state ---------------------------
  v_target_auth:=case when pg_catalog.jsonb_typeof(coalesce(v_control->'target_root_authorisation','null'::jsonb))='object'
                      then v_control->'target_root_authorisation' end;
  v_review:=case when pg_catalog.jsonb_typeof(coalesce(v_control->'whole_root_office_review','null'::jsonb))='object'
                 then v_control->'whole_root_office_review' end;

  for v_i in 1..v_n loop
    -- Decision D8: the authorisation record is per ROOT and lives in
    -- public.weekly_source_root_authorisations, keyed on the physical
    -- root_timesheet_id, with at most one live generation per root.  It is no
    -- longer the per-source-row lineage binding.
    select pg_catalog.count(*)::integer into v_live_generations
      from public.weekly_source_root_authorisations as authorisation_row
     where authorisation_row.root_timesheet_id=v_member_root_ids[v_i]
       and authorisation_row.withdrawn_at_utc is null;

    -- WP-07 review finding F2, answered from THIS side of the seam.  The
    -- authorisation record is keyed on the PHYSICAL root_timesheet_id, and since
    -- schema change S8 removed the physical uniqueness a single family can hold
    -- several of them.  A test that counts only the canonical root's own
    -- generations lets a ROTATED family whose NON-CANONICAL member is already
    -- authorised be authorised a second time; the root is then payable and
    -- carries two live generations, which every withdrawal path treats as an
    -- impossible state, so it can never be withdrawn again.
    --
    -- The "is it already authorised?" test is therefore over the whole FAMILY:
    -- every member Timesheet the lock result carries for this root, and the
    -- family booking id the authorisation row itself records.  The coordinator
    -- does not rely on interface I-6 having been widened to match — that is
    -- precisely the assumption this finding punished.
    select coalesce(pg_catalog.array_agg(member_element.value::uuid),array[]::uuid[])
      into v_family_members
      from pg_catalog.jsonb_array_elements(p_lock_result->'families') as family_element(value)
     cross join pg_catalog.jsonb_array_elements_text(
                  coalesce(family_element.value->'member_timesheet_ids','[]'::jsonb))
                as member_element(value)
     where family_element.value->>'requested_timesheet_id'=v_member_root_ids[v_i]::text;
    select pg_catalog.count(*)::integer into v_family_live_generations
      from public.weekly_source_root_authorisations as authorisation_row
     where authorisation_row.withdrawn_at_utc is null
       and (authorisation_row.root_timesheet_id=v_member_root_ids[v_i]
            or authorisation_row.root_timesheet_id=any(v_family_members)
            or pg_catalog.btrim(authorisation_row.family_booking_id)
               =pg_catalog.btrim(v_member_family_booking_ids[v_i]));

    if v_i=2 and v_target_auth is not null then
      -- 24 section 4.5 step 4: only a genuinely new, never-authorised B root is
      -- created and authorised here, and it is never authorised silently.
      if v_family_live_generations>0 then
        return pg_catalog.jsonb_build_object(
          'ok',false,'published',false,'replayed',false,
          'code','WEEKLY_SOURCE_PUBLICATION_TARGET_ALREADY_AUTHORISED','retryable',false,
          'detail',pg_catalog.jsonb_build_object(
            'live_generations',v_family_live_generations,
            'live_generations_on_this_physical_root',v_live_generations,
            'root_timesheet_id',v_member_root_ids[v_i],
            'family_booking_id',v_member_family_booking_ids[v_i],
            'family_member_timesheet_ids',pg_catalog.to_jsonb(v_family_members)));
      end if;
      if (v_target_auth->>'timesheet_id')::uuid is distinct from v_member_root_ids[2] then
        return pg_catalog.jsonb_build_object(
          'ok',false,'published',false,'replayed',false,
          'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID','retryable',false,
          'detail',pg_catalog.jsonb_build_object(
            'reason','TARGET_AUTHORISATION_TIMESHEET_MISMATCH'));
      end if;

      -- Review U2.  "Unknown to Weekly Source" is NOT the same as "blank": a B
      -- Timesheet that already existed, was never touched by Weekly Source and
      -- carries its own unrelated shifts satisfied the old test and was
      -- authorised silently, which 24 section 4.5 step 4, file 26 Gate 5 step 6,
      -- 27 section 8 step 6 and H2-024 all forbid.  B is now treated as
      -- genuinely new only when it is provably blank as well as unknown.
      v_blank:=private.weekly_source_publication_target_root_blank_v1(
        v_member_root_ids[2],v_member_family_booking_ids[2]);
      if coalesce((v_blank->>'blank')::boolean,false) is not true then
        -- The whole-root Office review is the only way past this, and it is a
        -- real control or it is nothing: a genuine boolean true, an existing
        -- reviewer, a time, bound to the accepted decision, and PERSISTED on
        -- the accepted decision so the act can be shown afterwards.  The three
        -- identity columns now exist on the bundle relation, so the check is
        -- the real one: the ACCEPTED DECISION must carry the review, and the
        -- request must quote exactly what it carries.
        if v_bundle.whole_root_review_required is not true
           or v_bundle.whole_root_reviewed_by_user_id is null
           or v_bundle.whole_root_reviewed_at_utc is null then
          return pg_catalog.jsonb_build_object(
            'ok',false,'published',false,'replayed',false,
            'code','WEEKLY_SOURCE_PUBLICATION_TARGET_ROOT_REVIEW_REQUIRED','retryable',false,
            'detail',pg_catalog.jsonb_build_object(
              'reason','THE_ACCEPTED_DECISION_CARRIES_NO_WHOLE_ROOT_OFFICE_REVIEW',
              'blank_check',v_blank,
              'decision_bundle_id',v_decision_bundle_id,
              'bundle_revision',v_bundle_revision));
        end if;
        -- The request's own review object: a real boolean, a real reviewer, a
        -- real time, and bound to this accepted decision.
        if v_review is null
           or pg_catalog.jsonb_typeof(v_review->'reviewed')<>'boolean'
           or (v_review->'reviewed')::boolean is not true
           or (v_review->>'reviewed_by_user_id') is null
           or (v_review->>'reviewed_at_utc') is null
           or (v_review->>'decision_id')::uuid is distinct from v_decision_id then
          return pg_catalog.jsonb_build_object(
            'ok',false,'published',false,'replayed',false,
            'code','WEEKLY_SOURCE_PUBLICATION_TARGET_ROOT_REVIEW_REQUIRED','retryable',false,
            'detail',pg_catalog.jsonb_build_object(
              'reason','WHOLE_ROOT_OFFICE_REVIEW_MISSING_OR_MALFORMED',
              'blank_check',v_blank,'review',v_review));
        end if;
        -- And it must be the review the ACCEPTED DECISION carries, not one the
        -- caller invented for this request.  The reviewer's existence is the
        -- bundle column's own foreign key, so it is proved by the row, not
        -- re-checked here.
        if (v_review->>'reviewed_by_user_id')::uuid
             is distinct from v_bundle.whole_root_reviewed_by_user_id
           or (v_review->>'reviewed_at_utc')::timestamptz
                is distinct from v_bundle.whole_root_reviewed_at_utc then
          return pg_catalog.jsonb_build_object(
            'ok',false,'published',false,'replayed',false,
            'code','WEEKLY_SOURCE_PUBLICATION_TARGET_ROOT_REVIEW_REQUIRED','retryable',false,
            'detail',pg_catalog.jsonb_build_object(
              'reason','REVIEW_IS_NOT_THE_ONE_ON_THE_ACCEPTED_DECISION',
              'accepted_reviewer',v_bundle.whole_root_reviewed_by_user_id,
              'requested_reviewer',v_review->'reviewed_by_user_id'));
        end if;
      end if;
    elsif v_live_generations=0 then
      -- A head over a root Weekly Source has not authorised would never reach
      -- payroll, so it is refused rather than written (decision D8: the live
      -- authorisation row is the test).
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_PUBLICATION_TARGET_NOT_AUTHORISED','retryable',false,
        'detail',pg_catalog.jsonb_build_object(
          'root_ordinal',v_i,'root_timesheet_id',v_member_root_ids[v_i],
          'family_booking_id',v_member_family_booking_ids[v_i]));
    end if;
  end loop;

  -- =========================================================================
  -- Everything from here writes.  Every failure below RAISES and rolls the
  -- whole transaction back: no head, pointer, invalidation, dirty job, B
  -- authorisation or receipt may survive (R11; 26 Gate 5 pass condition).
  -- =========================================================================

  -- HANDOVER 2 round-4 ruling 6 needs to know which invalidations and dirty
  -- jobs this transaction created, so the state is snapshotted before the first
  -- write.  Reading the Workbench's own tables is allowed; nothing of theirs is
  -- edited, wrapped or re-created.
  select coalesce(pg_catalog.array_agg(job_row.id),array[]::uuid[]) into v_jobs_before
    from public.banking_pay_workbench_jobs as job_row;
  select coalesce(pg_catalog.array_agg(scope_tx_row.tx_token),array[]::uuid[]) into v_scope_tx_before
    from public.banking_pay_scope_change_transactions as scope_tx_row;
  select coalesce(pg_catalog.jsonb_object_agg(
           scope_state_row.timesheet_id::text,
           coalesce(scope_state_row.last_scope_change_tx_token::text,'')),'{}'::jsonb)
    into v_scope_state_before
    from private.banking_pay_workbench_timesheet_scope_state as scope_state_row
   where scope_state_row.candidate_id=v_candidate_id;

  -- ---- 7. stage every member head and its complete component inventory ----
  v_source_generation_digest:=private.weekly_source_publication_request_digest_v1(
    v_canonical->'financial_request'->'source_revision');

  for v_i in 1..v_n loop
    v_entitlement:=(
      select entitlement_element.value
        from pg_catalog.jsonb_array_elements(
               v_canonical->'financial_request'->'member_entitlements') as entitlement_element(value)
       where (entitlement_element.value->>'root_ordinal')::integer=v_i);
    v_choice:=(
      select choice_element.value
        from pg_catalog.jsonb_array_elements(
               v_canonical->'financial_request'->'contract_choices') as choice_element(value)
       where (choice_element.value->>'root_ordinal')::integer=v_i);

    select coalesce(pg_catalog.jsonb_agg(
             pg_catalog.jsonb_build_object(
               'component_ordinal',component_element.value->'component_ordinal',
               'component_id',component_element.value->'component_id')
             order by (component_element.value->>'component_ordinal')::integer),'[]'::jsonb),
           coalesce(pg_catalog.jsonb_agg(
             pg_catalog.to_jsonb(pg_catalog.encode(
               private.weekly_source_publication_request_digest_v1(
                     private.weekly_source_publication_component_content_v1(component_element.value)),'hex'))
             order by (component_element.value->>'component_ordinal')::integer),'[]'::jsonb)
      into v_inventory_pairs,v_component_hashes
      from pg_catalog.jsonb_array_elements(v_entitlement->'components') as component_element(value);

    insert into public.weekly_source_entitlement_heads(
      id,authority_kind,agency_id,candidate_id,contract_id,week_ending_date,
      root_timesheet_id,root_family_booking_id,root_timesheet_version,head_revision,
      prior_head_id,state,certified_zero,component_count,entitlement_digest,inventory_digest,
      source_generation_digest,decision_bundle_id,bundle_revision,decision_id,
      decided_by_user_id,staged_at_utc,created_at_utc
    ) values (
      v_head_ids[v_i],
      v_entitlement->>'authority_kind',
      v_bundle.agency_id,
      v_candidate_id,
      (v_choice->>'contract_id')::uuid,
      (v_choice->>'week_ending_date')::date,
      v_member_root_ids[v_i],
      v_member_family_booking_ids[v_i],
      v_member_root_versions[v_i],
      v_head_revisions[v_i],
      v_prior_head_ids[v_i],
      'STAGED',
      (v_entitlement->>'certified_zero')::boolean,
      (v_entitlement->>'component_count')::integer,
      -- entitlement_digest: the complete economic content of the head.
      private.weekly_source_publication_request_digest_v1(
        pg_catalog.jsonb_build_object(
          'authority_kind',v_entitlement->'authority_kind',
          'certified_zero',v_entitlement->'certified_zero',
          'component_count',v_entitlement->'component_count',
          'components',v_component_hashes)),
      -- inventory_digest: which components the head contains, identity only.
      private.weekly_source_publication_request_digest_v1(
        pg_catalog.jsonb_build_object('components',v_inventory_pairs)),
      v_source_generation_digest,
      v_decision_bundle_id,v_bundle_revision,v_decision_id,
      v_bundle.decided_by_user_id,v_now,v_now);

    insert into public.weekly_source_entitlement_head_components(
      head_id,component_ordinal,component_id,component_kind,economic_key_type,
      economic_key_value,component_member_identity,segment_id,segment_key,
      segment_stable_key,work_date,reference_number,hours_day,hours_night,hours_sat,
      hours_sun,hours_bh,additional_code_raw,unit_count,unit_pay_rate,unit_charge_rate,
      expense_code,pay_ex_vat,charge_ex_vat,exclude_from_pay,origin,
      decision_bundle_id,bundle_revision,movement_id,movement_group_id,
      component_sha256,created_at_utc)
    select
      v_head_ids[v_i],
      (component_element.value->>'component_ordinal')::integer,
      (component_element.value->>'component_id')::uuid,
      component_element.value->>'component_kind',
      component_element.value->>'economic_key_type',
      component_element.value->>'economic_key_value',
      component_element.value->>'component_member_identity',
      component_element.value->>'segment_id',
      component_element.value->>'segment_key',
      component_element.value->>'segment_stable_key',
      (component_element.value->>'work_date')::date,
      component_element.value->>'reference_number',
      (component_element.value->>'hours_day')::numeric,
      (component_element.value->>'hours_night')::numeric,
      (component_element.value->>'hours_sat')::numeric,
      (component_element.value->>'hours_sun')::numeric,
      (component_element.value->>'hours_bh')::numeric,
      component_element.value->>'additional_code_raw',
      (component_element.value->>'unit_count')::numeric,
      (component_element.value->>'unit_pay_rate')::numeric,
      (component_element.value->>'unit_charge_rate')::numeric,
      component_element.value->>'expense_code',
      (component_element.value->>'pay_ex_vat')::numeric,
      (component_element.value->>'charge_ex_vat')::numeric,
      (component_element.value->>'exclude_from_pay')::boolean,
      component_element.value->>'origin',
      v_decision_bundle_id,v_bundle_revision,
      (component_element.value->>'movement_id')::uuid,
      (component_element.value->>'movement_group_id')::uuid,
      private.weekly_source_publication_request_digest_v1(
                     private.weekly_source_publication_component_content_v1(component_element.value)),
      v_now
    from pg_catalog.jsonb_array_elements(v_entitlement->'components') as component_element(value)
    order by (component_element.value->>'component_ordinal')::integer;

    v_entitlement:=null; v_choice:=null;
  end loop;

  -- ---- 8. interface I-6 for a genuinely new B root ------------------------
  -- After staging and before the single invalidation.  I-6 calls the UNCHANGED
  -- ordinary Authorise owner exactly once and inserts the lineage generation of
  -- proof/34 section 4.  It is late-bound: WP-07 owns it.
  if v_target_auth is not null then
    v_target_authorise_result:=private.weekly_source_first_authorise_core_v1(
      v_member_root_ids[2],
      v_target_auth->>'expected_row_signature',
      (v_target_auth->>'actor_user_id')::uuid,
      p_lock_result);
    if coalesce((v_target_authorise_result->>'ok')::boolean,false) is not true then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_TARGET_AUTHORISATION_FAILED'
        using errcode='55000',
              detail=pg_catalog.jsonb_build_object(
                'code','WEEKLY_SOURCE_PUBLICATION_TARGET_AUTHORISATION_FAILED',
                'first_authorise_result',v_target_authorise_result)::text;
    end if;
  end if;

  -- ---- 9. activate every head together ------------------------------------
  -- 24 section 5.1: pass the same transaction token and scalar head/currentness
  -- evidence used by the publication receipt.  The token is taken before
  -- activation so the head, the invalidation and the receipt all carry it.
  v_token:=public.pay_workbench_scope_change_tx_token_v1();
  if v_token is null then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_SCOPE_TOKEN_UNAVAILABLE'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_PUBLICATION_SCOPE_TOKEN_UNAVAILABLE')::text;
  end if;

  for v_i in 1..v_n loop
    if v_current_head_ids[v_i] is not null then
      update public.weekly_source_entitlement_heads
         set state='SUPERSEDED',
             superseded_at_utc=v_now,
             superseded_by_head_id=v_head_ids[v_i]
       where id=v_current_head_ids[v_i];
    end if;
  end loop;
  for v_i in 1..v_n loop
    update public.weekly_source_entitlement_heads
       set state='COMMITTED_CURRENT',
           committed_at_utc=v_now,
           publication_receipt_digest=v_digest,
           scope_change_tx_token=v_token
     where id=v_head_ids[v_i];
    get diagnostics v_rows=row_count;
    if v_rows<>1 then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_HEAD_ACTIVATION_FAILED'
        using errcode='55000',
              detail=pg_catalog.jsonb_build_object(
                'code','WEEKLY_SOURCE_PUBLICATION_HEAD_ACTIVATION_FAILED',
                'head_id',v_head_ids[v_i],'rows',v_rows)::text;
    end if;
  end loop;

  -- ---- 10. current_entitlement_head_id on the live ROOT AUTHORISATION ------
  -- Decision D8 (WP-01c): the authorisation record, and therefore this pointer,
  -- is per ROOT in public.weekly_source_root_authorisations, not on the
  -- per-source-row lineage binding.  proof/34 section 4: written by the
  -- head-publication coordinator only, and nothing else on the generation row
  -- is touched.  The relation's own partial unique index gives at most one live
  -- generation per root, so exactly one row must move.
  for v_i in 1..v_n loop
    update public.weekly_source_root_authorisations
       set current_entitlement_head_id=v_head_ids[v_i]
     where root_timesheet_id=v_member_root_ids[v_i]
       and withdrawn_at_utc is null;
    get diagnostics v_rows=row_count;
    if v_rows<>1 then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_ROOT_AUTHORISATION_POINTER_FAILED'
        using errcode='55000',
              detail=pg_catalog.jsonb_build_object(
                'code','WEEKLY_SOURCE_PUBLICATION_ROOT_AUTHORISATION_POINTER_FAILED',
                'root_ordinal',v_i,
                'root_timesheet_id',v_member_root_ids[v_i],
                'live_authorisation_rows_updated',v_rows)::text;
    end if;
    v_lineage_rows:=v_lineage_rows+v_rows;
  end loop;

  -- ---- 11. ONE bounded Workbench invalidation -----------------------------
  -- proof/32 section 8 step 3 and H2-036: exactly once, after every head and
  -- any B creation/Authorise, with the complete aligned Candidate/root pairs
  -- and the transaction token, success required before the receipt.  The
  -- ordinary Authorise trigger may queue a strict-subset job for the same
  -- Candidate under the same token; that is the accepted option 1 outcome and
  -- Weekly Source neither suppresses it nor folds it.
  v_invalidation:=private.pay_workbench_scope_invalidate_v1(
    p_candidate_ids=>pg_catalog.array_fill(v_candidate_id,array[v_n]),
    p_timesheet_ids=>v_member_root_ids,
    p_reason=>'WEEKLY_SOURCE_ENTITLEMENT_HEAD_PUBLICATION',
    p_scope_change_tx_token=>v_token,
    p_payload_json=>pg_catalog.jsonb_build_object(
      'weekly_source_publication',pg_catalog.jsonb_build_object(
        'decision_bundle_id',v_decision_bundle_id,
        'bundle_revision',v_bundle_revision,
        'publication_mode',v_mode,
        'request_digest',pg_catalog.encode(v_digest,'hex'),
        'head_ids',pg_catalog.to_jsonb(v_head_ids),
        'head_revisions',pg_catalog.to_jsonb(v_head_revisions))));
  -- `<>` on a missing key yields NULL, the IF does not fire, and the receipt is
  -- written anyway.  An invalidation result that does not say it covered
  -- exactly one Candidate must FAIL, so this is `is distinct from` (review
  -- finding, lower severity 1: executed with a wrapper that stripped the key,
  -- and the publication went through).
  if coalesce((v_invalidation->>'ok')::boolean,false) is not true
     or (v_invalidation->>'candidate_count')::integer is distinct from 1
     or (v_invalidation->>'scope_change_tx_token')::uuid is distinct from v_token
     or coalesce((v_invalidation->>'job_inserted_count')::integer,0)
        +coalesce((v_invalidation->>'job_coalesced_count')::integer,0)<1 then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_INVALIDATION_FAILED'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_PUBLICATION_INVALIDATION_FAILED',
              'invalidation',v_invalidation)::text;
  end if;

  -- ---- 11a. the R25 invalidation contract (HANDOVER 2 round 4, ruling 6) ---
  -- Restated R25: EVERY invalidation and dirty job created for this Candidate
  -- in this transaction - the explicit call, the ordinary Authorise route and
  -- every registered DIRTY_TRIGGER:<table>:<op> path - must carry the same
  -- token UUID and the same generation, and every non-complete job's scope,
  -- after the installed rotation normaliser, must be an exact subset of the
  -- declared aligned scope for that Candidate.  A different Candidate, an
  -- outside root or a token disagreement is a CONTRACT FAILURE, not extra
  -- harmless work, and rolls the whole transaction back.  Checked here, after
  -- the one explicit invalidation and BEFORE the receipt.
  --
  -- Ruling 6 point 1: calling the token owner again counts as the same token
  -- only if the returned UUID equals the controlling transaction token.
  v_token_recheck:=public.pay_workbench_scope_change_tx_token_v1();
  if v_token_recheck is distinct from v_token then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_INVALIDATION_CONTRACT_FAILURE'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_PUBLICATION_INVALIDATION_CONTRACT_FAILURE',
              'reason','CONTROLLING_TOKEN_CHANGED_INSIDE_THE_TRANSACTION',
              'controlling_token',v_token,'recheck_token',v_token_recheck)::text;
  end if;

  -- The declared complete aligned scope for this Candidate, expanded through
  -- the INSTALLED rotation normaliser (call-only, contract section 2).
  v_normalised:=public._pay_workbench_normalise_timesheet_rotation_scope_payload(
    v_member_root_ids,array[]::uuid[]);
  select coalesce(pg_catalog.array_agg(distinct scope_element.value::uuid),array[]::uuid[])
    into v_declared_scope
    from pg_catalog.jsonb_array_elements_text(
           coalesce(v_normalised->'family_timesheet_ids',
                    coalesce(v_normalised->'canonical_timesheet_ids',
                             pg_catalog.to_jsonb(v_member_root_ids)))) as scope_element(value);
  v_declared_scope:=private.weekly_source_uuid_set_union_v1(v_declared_scope,v_member_root_ids);

  -- At most one new scope-change transaction, and it is the controlling token.
  if exists (
    select 1 from public.banking_pay_scope_change_transactions as scope_tx_row
     where not (scope_tx_row.tx_token=any(v_scope_tx_before))
       and scope_tx_row.tx_token is distinct from v_token) then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_INVALIDATION_CONTRACT_FAILURE'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_PUBLICATION_INVALIDATION_CONTRACT_FAILURE',
              'reason','A_SECOND_SCOPE_CHANGE_TOKEN_WAS_CREATED_IN_THIS_TRANSACTION')::text;
  end if;

  -- Every job this transaction queued: same token, same (absent) generation,
  -- this Candidate, and a scope inside the declared aligned scope.
  v_offending:=(
    select pg_catalog.jsonb_build_object(
             'job_id',job_row.id,'job_type',job_row.job_type,
             'candidate_id',job_row.candidate_id,
             'scope_change_tx_token',job_row.scope_change_tx_token,
             'scope_change_generation',job_row.scope_change_generation,
             'targeted_timesheet_ids',job_row.payload_json->'targeted_timesheet_ids',
             'reason',job_row.payload_json->'reason')
      from public.banking_pay_workbench_jobs as job_row
     where not (job_row.id=any(v_jobs_before))
       and (
         job_row.scope_change_tx_token is distinct from v_token
         or job_row.scope_change_generation is not null
         or (job_row.candidate_id is not null and job_row.candidate_id<>v_candidate_id)
         or exists (
           select 1
             from pg_catalog.jsonb_array_elements_text(
                    coalesce(job_row.payload_json->'targeted_timesheet_ids','[]'::jsonb)) as target(value)
            where not (target.value::uuid=any(v_declared_scope)))
         or exists (
           select 1
             from pg_catalog.jsonb_array_elements_text(
                    coalesce(job_row.payload_json->'linked_timesheet_ids','[]'::jsonb)) as linked(value)
            where not (linked.value::uuid=any(v_declared_scope))))
     limit 1);
  if v_offending is not null then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_INVALIDATION_CONTRACT_FAILURE'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_PUBLICATION_INVALIDATION_CONTRACT_FAILURE',
              'reason','A_JOB_QUEUED_IN_THIS_TRANSACTION_BREAKS_THE_R25_CONTRACT',
              'controlling_token',v_token,
              'declared_scope',pg_catalog.to_jsonb(v_declared_scope),
              'job',v_offending)::text;
  end if;

  -- Every scope-state row THIS PUBLICATION dirtied for this Candidate.  The
  -- snapshot taken before the first write holds each row's token as it then
  -- was, so a row that already carried the controlling token before the
  -- publication began is not attributed to the publication.
  v_offending:=(
    select pg_catalog.jsonb_build_object(
             'timesheet_id',scope_state_row.timesheet_id,
             'candidate_id',scope_state_row.candidate_id,
             'last_scope_change_tx_token',scope_state_row.last_scope_change_tx_token,
             'last_dirty_reason',scope_state_row.last_dirty_reason)
      from private.banking_pay_workbench_timesheet_scope_state as scope_state_row
     where scope_state_row.candidate_id=v_candidate_id
       and scope_state_row.last_scope_change_tx_token=v_token
       and coalesce(v_scope_state_before->>scope_state_row.timesheet_id::text,'')
           is distinct from v_token::text
       and not (scope_state_row.timesheet_id=any(v_declared_scope))
     limit 1);
  if v_offending is not null then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_INVALIDATION_CONTRACT_FAILURE'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_PUBLICATION_INVALIDATION_CONTRACT_FAILURE',
              'reason','A_SCOPE_STATE_ROW_OUTSIDE_THE_DECLARED_ALIGNED_SCOPE_WAS_DIRTIED',
              'declared_scope',pg_catalog.to_jsonb(v_declared_scope),
              'scope_state',v_offending)::text;
  end if;

  -- Exactly one effective complete-scope dirty result per Candidate and token
  -- (ruling 6 point 6).
  if (select pg_catalog.count(*)
        from public.banking_pay_workbench_jobs as job_row
       where job_row.candidate_id=v_candidate_id
         and job_row.scope_change_tx_token=v_token
         and job_row.job_type='WORKBENCH_CANDIDATE_DIRTY_APPLY'
         and job_row.status in ('QUEUED','RUNNING')
         and private.weekly_source_uuid_set_equals_v1(
               (select coalesce(pg_catalog.array_agg(distinct target.value::uuid),array[]::uuid[])
                  from pg_catalog.jsonb_array_elements_text(
                         coalesce(job_row.payload_json->'targeted_timesheet_ids','[]'::jsonb)) as target(value)),
               v_member_root_ids))<>1 then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_INVALIDATION_CONTRACT_FAILURE'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_PUBLICATION_INVALIDATION_CONTRACT_FAILURE',
              'reason','NOT_EXACTLY_ONE_COMPLETE_SCOPE_JOB_FOR_THE_CANDIDATE_UNDER_THIS_TOKEN')::text;
  end if;

  -- ---- 12. exactly one immutable receipt ----------------------------------
  insert into private.weekly_source_entitlement_publication_receipts(
    decision_bundle_id,pending_bundle_id,bundle_revision,request_digest,publication_mode,
    candidate_id,member_root_ids,member_family_booking_ids,member_root_versions,head_ids,
    scope_change_tx_token,decision_id,decided_by_user_id,released_by_worker_id,
    released_by_worker_run_id,census_json,proof_json,created_at_utc
  ) values (
    v_decision_bundle_id,p_pending_bundle_id,v_bundle_revision,v_digest,v_mode,
    v_candidate_id,v_member_root_ids,v_member_family_booking_ids,v_member_root_versions,
    v_head_ids,v_token,v_decision_id,v_bundle.decided_by_user_id,p_worker_id,
    p_worker_run_id,coalesce(p_census,'{}'::jsonb),coalesce(p_proof,'{}'::jsonb),v_now)
  returning * into v_receipt;

  if v_bundle.state='PROPOSED' then
    update public.weekly_source_entitlement_decision_bundles
       set state='COMMITTED',committed_at_utc=v_now
     where decision_bundle_id=v_decision_bundle_id
       and bundle_revision=v_bundle_revision;
  end if;

  for v_i in 1..v_n loop
    v_result_heads:=v_result_heads||pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object(
        'root_ordinal',v_i,
        'head_id',v_head_ids[v_i],
        'head_revision',v_head_revisions[v_i],
        'prior_head_id',v_prior_head_ids[v_i],
        'superseded_head_id',v_current_head_ids[v_i],
        'root_timesheet_id',v_member_root_ids[v_i],
        'family_booking_id',v_member_family_booking_ids[v_i],
        'root_timesheet_version',v_member_root_versions[v_i]));
  end loop;

  return pg_catalog.jsonb_build_object(
    'ok',true,'published',true,'replayed',false,
    'receipt',private.weekly_source_publication_receipt_json_v1(v_receipt.id),
    'heads',v_result_heads,
    'scope_change_tx_token',v_token,
    'invalidation',v_invalidation,
    'root_authorisations_pointed',v_lineage_rows,
    'before_position_source',pg_catalog.to_jsonb(v_before_source),
    'declared_aligned_scope',pg_catalog.to_jsonb(v_declared_scope),
    'member_timesheet_ids',pg_catalog.to_jsonb(v_member_timesheet_ids));
end;
$function$;

-- ---------------------------------------------------------------------------
-- 5. Interface I-4 — the immediate publication entry point
-- ---------------------------------------------------------------------------
-- Takes the I-1 locks with job type WORKBENCH_CANDIDATE_ENTITLEMENT_PUBLICATION
-- (proof/32 section 6 step 1), runs the I-2 census over EVERY physical member of
-- EVERY family, and either calls the core (IMMEDIATE) or, when the census is
-- FROZEN, calls I-5 and publishes nothing (24 section 4.4).  CENSUS_ERROR
-- refuses with no write.
create or replace function private.weekly_source_entitlement_publish_immediate_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_candidate_id uuid;
  v_member_root_ids uuid[];
  v_member_timesheet_ids uuid[]:=array[]::uuid[];
  v_lock_result jsonb;
  v_census jsonb;
  v_core jsonb;
  v_pending jsonb;
  v_replay_canonical jsonb;
  v_replay_receipt record;
  v_saved record;
  v_saved_rows integer;
  v_err_message text;
  v_err_detail text;
begin
  if coalesce(
       pg_catalog.current_setting('request.jwt.claim.role',true),
       nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role',
       ''
     )<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;

  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or pg_catalog.jsonb_typeof(coalesce(p_request->'member_root_ids','null'::jsonb))<>'array' then
    return pg_catalog.jsonb_build_object(
      'ok',false,'published',false,'replayed',false,
      'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID','retryable',false,
      'detail',pg_catalog.jsonb_build_object('reason','EXPECTED_OBJECT_WITH_MEMBER_ROOT_IDS'));
  end if;
  v_candidate_id:=(p_request->>'candidate_id')::uuid;
  select pg_catalog.array_agg(root_element.value::uuid order by root_element.ordinality)
    into v_member_root_ids
    from pg_catalog.jsonb_array_elements_text(p_request->'member_root_ids')
         with ordinality as root_element(value,ordinality);

  -- ---- exact receipt replay, FIRST (review U4) ----------------------------
  -- proof/32 section 8 step 1 and section 2: "Exact receipt replay ... is
  -- evaluated FIRST"; 24 section 5.1: "An exact idempotent replay must return
  -- the already committed receipt before calling the invalidator".
  --
  -- The core has this check, but the core is reached only on a RELEASABLE
  -- census, and the normal state after a publication is that the Workbench
  -- drafts the new entitlement and FREEZES the root.  A retry whose first
  -- response was lost therefore used to fall into the FROZEN branch and save a
  -- PENDING bundle for a decision that was already published, sending the
  -- stale warning to the very Draft paying that entitlement.  The replay is
  -- answered here, before the serial gate, the locks, the census and I-5.
  begin
    v_replay_canonical:=private.weekly_source_publication_request_canonical_v1(
      p_request,'IMMEDIATE',null::uuid);
  exception when invalid_parameter_value then
    get stacked diagnostics v_err_message=message_text, v_err_detail=pg_exception_detail;
    return pg_catalog.jsonb_build_object(
      'ok',false,'published',false,'replayed',false,
      'code',v_err_message,'retryable',false,
      'detail',case when coalesce(v_err_detail,'') ~ '^\{' then v_err_detail::jsonb
                    else pg_catalog.to_jsonb(v_err_detail) end);
  end;
  select * into v_replay_receipt
    from private.weekly_source_entitlement_publication_receipts as receipt_row
   where receipt_row.request_digest
         =private.weekly_source_publication_request_digest_v1(v_replay_canonical);
  if found then
    -- A digest hit is only an index lookup: every immutable receipt field is
    -- compared before a replay may be returned (H2-032).
    if v_replay_receipt.decision_bundle_id
         is distinct from (v_replay_canonical->>'decision_bundle_id')::uuid
       or v_replay_receipt.bundle_revision
            is distinct from (v_replay_canonical->>'bundle_revision')::bigint
       or v_replay_receipt.candidate_id is distinct from v_candidate_id
       or v_replay_receipt.member_root_ids is distinct from v_member_root_ids
       -- Every immutable receipt field, exactly as the core compares them
       -- (H2-032): the digest is only an index lookup.
       or pg_catalog.to_jsonb(v_replay_receipt.member_family_booking_ids)
            is distinct from v_replay_canonical->'member_family_booking_ids'
       or pg_catalog.to_jsonb(v_replay_receipt.member_root_versions)
            is distinct from v_replay_canonical->'member_root_versions'
       or pg_catalog.to_jsonb(v_replay_receipt.head_ids)
            is distinct from v_replay_canonical->'head_ids'
       or v_replay_receipt.decision_id
            is distinct from (v_replay_canonical->>'decision_id')::uuid
       or v_replay_receipt.publication_mode<>'IMMEDIATE'
       or v_replay_receipt.pending_bundle_id is not null then
      -- Round-5 ruling A1 control 5, at the entry point as well as in the core:
      -- a conflicting replay is a PERMANENT integrity failure that goes DIRECTLY
      -- to manual review, never one of ten retries.
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_PUBLICATION_REPLAY_CONFLICT','retryable',false,
        'detail',pg_catalog.jsonb_build_object(
          'integrity_failure',true,'disposition','MANUAL_REVIEW',
          'reason','A_COMMITTED_RECEIPT_CARRIES_THIS_DIGEST_WITH_DIFFERENT_IMMUTABLE_FIELDS',
          'receipt_id',v_replay_receipt.id));
    end if;
    return pg_catalog.jsonb_build_object(
      'ok',true,'published',true,'replayed',true,
      'receipt',private.weekly_source_publication_receipt_json_v1(v_replay_receipt.id));
  end if;

  -- Interface I-1 (WP-03), late-bound.
  v_lock_result:=private.weekly_source_lock_and_resolve_families_v1(
    v_candidate_id,
    v_member_root_ids,
    'WORKBENCH_CANDIDATE_ENTITLEMENT_PUBLICATION',
    pg_catalog.gen_random_uuid(),
    'WEEKLY_SOURCE_ENTITLEMENT_PUBLICATION');
  if coalesce((v_lock_result->>'ok')::boolean,false) is not true then
    return v_lock_result||pg_catalog.jsonb_build_object('published',false,'replayed',false);
  end if;

  -- proof/34 section 8 and interface I-2: EVERY physical member of EVERY
  -- family, never only the canonical rows.
  select coalesce(pg_catalog.array_agg(distinct member_element.value::uuid),array[]::uuid[])
    into v_member_timesheet_ids
    from pg_catalog.jsonb_array_elements(v_lock_result->'families') as family_element(value)
   cross join lateral pg_catalog.jsonb_array_elements_text(
           coalesce(family_element.value->'member_timesheet_ids','[]'::jsonb)) as member_element(value);

  v_census:=private.weekly_source_freeze_census_v1(v_candidate_id,v_member_timesheet_ids);

  if coalesce(v_census->>'result','')='CENSUS_ERROR' then
    return pg_catalog.jsonb_build_object(
      'ok',false,'published',false,'replayed',false,
      'code','WEEKLY_SOURCE_CENSUS_ERROR','retryable',false,
      'census',v_census);
  end if;

  if coalesce(v_census->>'result','')='FROZEN' then
    -- 24 section 4.4: the decision is saved as pending, the previous effective
    -- entitlement remains current, and the existing Draft and all frozen
    -- evidence remain unchanged.  Interface I-5 (WP-08b), late-bound.
    v_pending:=private.weekly_source_pending_entitlement_bundle_save_v1(
      p_request,v_lock_result,v_census);

    -- Decision D10, the SAVE half.  A pending bundle is a promise to move money
    -- later, so before this transaction commits the coordinator re-reads what
    -- was actually stored, under the locks it already holds, and proves the
    -- stored request and the stored digest agree with each other and with the
    -- request it was given.  A disagreement raises and rolls the save back;
    -- it is never merely recorded.
    if coalesce((v_pending->>'ok')::boolean,false) is true then
      -- Round-5 ruling A1 control 2: "verify request plus digest UNDER THE SAME
      -- LOCK when saving".  The row is therefore re-read `for update`, and its
      -- cardinality is an explicit check rather than a `select ... into` that
      -- would silently take an arbitrary row if a second live pending bundle
      -- ever existed for one decision revision (Part 1 review rule 5).
      select pg_catalog.count(*)::integer into v_saved_rows
        from public.weekly_source_pending_entitlement_bundles as pending_row
       where pending_row.decision_bundle_id=(p_request->>'decision_bundle_id')::uuid
         and pending_row.bundle_revision=(p_request->>'bundle_revision')::bigint
         and pending_row.state in ('PENDING','RELEASING');
      if v_saved_rows>1 then
        raise exception 'WEEKLY_SOURCE_PUBLICATION_PENDING_BUNDLE_INVALID'
          using errcode='55000',
                detail=pg_catalog.jsonb_build_object(
                  'code','WEEKLY_SOURCE_PUBLICATION_PENDING_BUNDLE_INVALID',
                  'reason','MORE_THAN_ONE_LIVE_PENDING_BUNDLE_FOR_THIS_DECISION_REVISION',
                  'integrity_failure',true,'disposition','MANUAL_REVIEW',
                  'decision_bundle_id',p_request->'decision_bundle_id',
                  'bundle_revision',p_request->'bundle_revision',
                  'pending_bundle_rows',v_saved_rows)::text;
      end if;
      select pending_row.id,pending_row.request_json,pending_row.request_digest
        into v_saved
        from public.weekly_source_pending_entitlement_bundles as pending_row
       where pending_row.decision_bundle_id=(p_request->>'decision_bundle_id')::uuid
         and pending_row.bundle_revision=(p_request->>'bundle_revision')::bigint
         and pending_row.state in ('PENDING','RELEASING')
         for update;
      if not found then
        raise exception 'WEEKLY_SOURCE_PUBLICATION_PENDING_BUNDLE_NOT_SAVED'
          using errcode='55000',
                detail=pg_catalog.jsonb_build_object(
                  'code','WEEKLY_SOURCE_PUBLICATION_PENDING_BUNDLE_NOT_SAVED',
                  'save_result',v_pending)::text;
      end if;
      if private.weekly_source_publication_request_digest_v1(
           private.weekly_source_publication_request_canonical_v1(
             v_saved.request_json,'DEFERRED',v_saved.id))
         is distinct from v_saved.request_digest
         or private.weekly_source_publication_request_digest_v1(
              private.weekly_source_publication_request_canonical_v1(
                p_request,'DEFERRED',v_saved.id))
            is distinct from v_saved.request_digest then
        raise exception 'WEEKLY_SOURCE_PUBLICATION_PENDING_BUNDLE_INVALID'
          using errcode='55000',
                detail=pg_catalog.jsonb_build_object(
                  'code','WEEKLY_SOURCE_PUBLICATION_PENDING_BUNDLE_INVALID',
                  'integrity_failure',true,'disposition','MANUAL_REVIEW',
                  'reason','THE_SAVED_REQUEST_AND_ITS_SAVED_DIGEST_DO_NOT_AGREE',
                  'pending_bundle_id',v_saved.id,
                  'stored_digest',pg_catalog.encode(v_saved.request_digest,'hex'))::text;
      end if;
    end if;

    return pg_catalog.jsonb_build_object(
      'ok',coalesce((v_pending->>'ok')::boolean,false),
      'published',false,'replayed',false,
      'code','WEEKLY_SOURCE_PUBLICATION_DEFERRED_PENDING_FREEZE',
      'retryable',false,
      'pending',v_pending,'census',v_census,'lock_result',v_lock_result,
      'pending_bundle_verified',v_saved.id);
  end if;

  if coalesce(v_census->>'result','')<>'RELEASABLE' then
    return pg_catalog.jsonb_build_object(
      'ok',false,'published',false,'replayed',false,
      'code','WEEKLY_SOURCE_CENSUS_ERROR','retryable',false,
      'census',v_census);
  end if;

  -- proof/32 section 9: census_json and proof_json are "an empty object for
  -- immediate".  The census that was actually run is returned to the caller,
  -- not written to the frozen receipt relation.
  v_core:=private.weekly_source_entitlement_publish_core_v1(
    p_request=>p_request,
    p_publication_mode=>'IMMEDIATE',
    p_lock_result=>v_lock_result,
    p_pending_bundle_id=>null::uuid,
    p_worker_id=>null::text,
    p_worker_run_id=>null::uuid,
    p_census=>'{}'::jsonb,
    p_proof=>'{}'::jsonb);

  return v_core||pg_catalog.jsonb_build_object('census',v_census,'lock_result',v_lock_result);
end;
$function$;

-- ---------------------------------------------------------------------------
-- 6. Ownership and privileges
-- ---------------------------------------------------------------------------
alter function private.weekly_source_canonical_json_text_v1(jsonb) owner to postgres;
alter function private.weekly_source_publication_request_digest_v1(jsonb) owner to postgres;
alter function private.weekly_source_publication_scalar_v1(jsonb,text,text,integer,boolean) owner to postgres;
alter function private.weekly_source_publication_require_keys_v1(jsonb,text[],text) owner to postgres;
alter function private.weekly_source_publication_component_canonical_v1(jsonb,text) owner to postgres;
alter function private.weekly_source_publication_request_canonical_v1(jsonb,text,uuid) owner to postgres;
alter function private.weekly_source_uuid_set_union_v1(uuid[],uuid[]) owner to postgres;
alter function private.weekly_source_uuid_set_intersect_v1(uuid[],uuid[]) owner to postgres;
alter function private.weekly_source_uuid_set_difference_v1(uuid[],uuid[]) owner to postgres;
alter function private.weekly_source_uuid_set_equals_v1(uuid[],uuid[]) owner to postgres;
alter function private.weekly_source_publication_component_content_v1(jsonb) owner to postgres;
alter function private.weekly_source_publication_target_root_blank_v1(uuid,text) owner to postgres;
alter function private.weekly_source_publication_before_inventory_v1(jsonb,integer) owner to postgres;
alter function private.weekly_source_publication_receipt_json_v1(uuid) owner to postgres;
alter function private.weekly_source_entitlement_publish_core_v1(jsonb,text,jsonb,uuid,text,uuid,jsonb,jsonb) owner to postgres;
alter function private.weekly_source_entitlement_publish_immediate_v1(jsonb) owner to postgres;

revoke all on function private.weekly_source_canonical_json_text_v1(jsonb) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_publication_request_digest_v1(jsonb) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_publication_scalar_v1(jsonb,text,text,integer,boolean) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_publication_require_keys_v1(jsonb,text[],text) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_publication_component_canonical_v1(jsonb,text) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_publication_request_canonical_v1(jsonb,text,uuid) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_uuid_set_union_v1(uuid[],uuid[]) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_uuid_set_intersect_v1(uuid[],uuid[]) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_uuid_set_difference_v1(uuid[],uuid[]) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_uuid_set_equals_v1(uuid[],uuid[]) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_publication_component_content_v1(jsonb) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_publication_target_root_blank_v1(uuid,text) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_publication_before_inventory_v1(jsonb,integer) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_publication_receipt_json_v1(uuid) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_entitlement_publish_core_v1(jsonb,text,jsonb,uuid,text,uuid,jsonb,jsonb) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_entitlement_publish_immediate_v1(jsonb) from public,anon,authenticated,service_role;

comment on function private.weekly_source_publication_request_digest_v1(jsonb) is
  'The one canonical Weekly Source publication request-digest encoder, shared by immediate publication, deferred release and replay verification (proof/32 section 9; H2-032).';
comment on function private.weekly_source_entitlement_publish_core_v1(jsonb,text,jsonb,uuid,text,uuid,jsonb,jsonb) is
  'Atomic Weekly Source entitlement head-publication coordinator (interface I-4). Assumes the I-1 rotation locks are already held. One invalidation, one receipt, all or nothing (proof/32 sections 6 to 9; 24 section 4.5; file 26 Gate 5).';
comment on function private.weekly_source_entitlement_publish_immediate_v1(jsonb) is
  'Immediate Weekly Source entitlement publication (interface I-4): I-1 locks, I-2 census, then the core, or I-5 when the census is FROZEN.';

commit;

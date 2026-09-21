-- Rollback-only PostgreSQL 17.11 proof for the Weekly Source entitlement-head
-- publication coordinator (`17092026_0300_weekly_source_entitlement_publication_v1.sql`).
--
-- Authority: `P:\proof\32_PENDING_PUBLICATION_OWNER_SPECIFICATION_20260917.md`
-- sections 6 to 9 and 11; `P:\24_…AUTHORITY.md` sections 4.3 to 4.5 and 5.1;
-- `P:\26_…LEDGER.md` Gate 5 steps 1 to 10; `P:\27_…HANDOFF.md` sections 2, 4, 8;
-- `H2-024`, `H2-031` to `H2-033`, `H2-035`, `H2-036`, `H2-038`.  Scenario ids
-- below are the `R` numbers of `proof/32` section 12.
--
-- Interfaces I-1 (WP-03), I-5 and I-6 did not exist when this file was written.
-- Where a test needs one it either uses the installed owner when it is present
-- or a clearly named rollback-only test double created here; every such case is
-- labelled `TEST-DOUBLE` and listed in `plan6-2-implementation\reports\WP-02_REPORT.md`.
-- The lock results are hand-built to the fixed I-1 shape.
--
-- Everything here runs inside one transaction that ends in `rollback`.

\set ON_ERROR_STOP on

begin;
set local request.jwt.claim.role='service_role';

create function pg_temp.assert_true(p_condition boolean,p_message text)
returns void language plpgsql as $function$
begin
  if p_condition is distinct from true then
    raise exception 'ASSERTION_FAILED: %',p_message;
  end if;
end;
$function$;

-- Runs one statement, requires it to fail, and requires the SQLSTATE and (when
-- given) a fragment of the message.  A statement that SUCCEEDS is a failure.
create function pg_temp.expect_failure(
  p_sql text,p_sqlstate text,p_message_fragment text,p_label text
) returns void language plpgsql as $function$
declare
  v_state text;
  v_message text;
  v_detail text;
begin
  begin
    execute p_sql;
  exception when others then
    get stacked diagnostics v_state=returned_sqlstate, v_message=message_text,
                            v_detail=pg_exception_detail;
    -- A refusal carries its precise reason in DETAIL, so the fragment is looked
    -- for in the message AND the detail.
    v_message:=coalesce(v_message,'')||' '||coalesce(v_detail,'');
    if p_sqlstate is not null and v_state<>p_sqlstate then
      raise exception 'ASSERTION_FAILED: % expected SQLSTATE % but got % (%)',
        p_label,p_sqlstate,v_state,v_message;
    end if;
    if coalesce(p_message_fragment,'')<>''
       and pg_catalog.strpos(coalesce(v_message,''),p_message_fragment)=0 then
      raise exception 'ASSERTION_FAILED: % expected message containing "%" but got "%"',
        p_label,p_message_fragment,v_message;
    end if;
    return;
  end;
  raise exception 'ASSERTION_FAILED: % was expected to fail and did not',p_label;
end;
$function$;

-- ---------------------------------------------------------------------------
-- 1. The installed definitions themselves
-- ---------------------------------------------------------------------------
do $verify_publication_definitions$
declare
  v_function record;
  v_definition text;
begin
  -- Every owner this package installs exists with the fixed signature.
  for v_function in
    select * from (values
      ('private.weekly_source_canonical_json_text_v1(jsonb)','i','f'),
      ('private.weekly_source_publication_request_digest_v1(jsonb)','i','f'),
      ('private.weekly_source_publication_scalar_v1(jsonb,text,text,integer,boolean)','i','f'),
      ('private.weekly_source_publication_require_keys_v1(jsonb,text[],text)','i','f'),
      ('private.weekly_source_publication_component_canonical_v1(jsonb,text)','i','f'),
      ('private.weekly_source_publication_request_canonical_v1(jsonb,text,uuid)','i','f'),
      ('private.weekly_source_publication_component_content_v1(jsonb)','i','f'),
      ('private.weekly_source_publication_before_inventory_v1(jsonb,integer)','i','f'),
      ('private.weekly_source_publication_target_root_blank_v1(uuid,text)','s','t'),
      ('private.weekly_source_uuid_set_union_v1(uuid[],uuid[])','i','f'),
      ('private.weekly_source_uuid_set_intersect_v1(uuid[],uuid[])','i','f'),
      ('private.weekly_source_uuid_set_difference_v1(uuid[],uuid[])','i','f'),
      ('private.weekly_source_uuid_set_equals_v1(uuid[],uuid[])','i','f'),
      ('private.weekly_source_publication_receipt_json_v1(uuid)','s','f'),
      ('private.weekly_source_entitlement_publish_core_v1(jsonb,text,jsonb,uuid,text,uuid,jsonb,jsonb)','v','t'),
      ('private.weekly_source_entitlement_publish_immediate_v1(jsonb)','v','t')
    ) as expected(signature,volatility,security_definer)
  loop
    perform pg_temp.assert_true(
      pg_catalog.to_regprocedure(v_function.signature) is not null,
      'installed function missing: '||v_function.signature);
    perform pg_temp.assert_true(
      (select p.provolatile=v_function.volatility and p.prosecdef=(v_function.security_definer='t')
         from pg_catalog.pg_proc p
        where p.oid=pg_catalog.to_regprocedure(v_function.signature)),
      'volatility or security setting wrong on '||v_function.signature);
    -- Weekly Source ACL discipline: owned by the release owner, no other
    -- grantee, revoked from every browser role.
    perform pg_temp.assert_true(
      (select p.proowner=(current_user::pg_catalog.regrole)::oid
         from pg_catalog.pg_proc p
        where p.oid=pg_catalog.to_regprocedure(v_function.signature)),
      'unexpected owner on '||v_function.signature);
    perform pg_temp.assert_true(
      not exists (
        select 1
          from pg_catalog.pg_proc p
         cross join lateral pg_catalog.aclexplode(
           coalesce(p.proacl,pg_catalog.acldefault('f',p.proowner))) acl
         where p.oid=pg_catalog.to_regprocedure(v_function.signature)
           and acl.grantee<>p.proowner),
      'unexpected grantee on '||v_function.signature);

    -- Workspace AGENTS.md conditional-expression rule: COALESCE, NULLIF, LEAST
    -- and GREATEST are syntax constructs, not pg_catalog functions.  A
    -- schema-qualified call compiles and then fails 42883 at first execution,
    -- so the source is guarded rather than trusted.
    v_definition:=pg_catalog.pg_get_functiondef(
      pg_catalog.to_regprocedure(v_function.signature));
    perform pg_temp.assert_true(
      v_definition !~* 'pg_catalog\.(coalesce|nullif|least|greatest)\s*\(',
      'illegal schema-qualified conditional expression in '||v_function.signature);
  end loop;

  -- proof/32 section 11, by search over the installed definitions: the
  -- coordinator writes nothing in Banking Pay and holds no lock there.
  v_definition:=pg_catalog.pg_get_functiondef(pg_catalog.to_regprocedure(
    'private.weekly_source_entitlement_publish_core_v1(jsonb,text,jsonb,uuid,text,uuid,jsonb,jsonb)'))
    ||pg_catalog.pg_get_functiondef(pg_catalog.to_regprocedure(
    'private.weekly_source_entitlement_publish_immediate_v1(jsonb)'));
  perform pg_temp.assert_true(
    v_definition !~* '(insert|update|delete)\s+(into\s+)?(public\.)?(pay_batch|pay_bank|pay_advance|pay_payment|pay_settle|banking_pay_operations|timesheet_pay_state|pay_finance)',
    'the coordinator must not write any Banking Pay relation (proof/32 section 11)');
  perform pg_temp.assert_true(
    v_definition !~* 'for\s+(update|no\s+key\s+update|share)[^;]*pay_batch',
    'the coordinator must hold no lock on a Banking Pay table (proof/32 section 11)');
  perform pg_temp.assert_true(
    v_definition !~* 'lifecycle_defer_summary_refresh|bpay_scope_invalidator_active',
    'the coordinator must never set or read a Workbench session setting');
  perform pg_temp.assert_true(
    v_definition !~* 'update\s+public\.timesheets\b|update\s+public\.timesheets_financials\b',
    'the coordinator must never mutate a public Timesheet or current TSFIN');
  -- The single explicit invalidation, named once.
  perform pg_temp.assert_true(
    (pg_catalog.length(v_definition)
     -pg_catalog.length(pg_catalog.replace(v_definition,'pay_workbench_scope_invalidate_v1','')))
    /pg_catalog.length('pay_workbench_scope_invalidate_v1')=1,
    'the invalidator must be called from exactly one place in the coordinator');
end
$verify_publication_definitions$;

-- ---------------------------------------------------------------------------
-- 1a. The whole-root Office review columns, as a schema contract (handoff N9)
-- ---------------------------------------------------------------------------
-- These three columns on `public.weekly_source_entitlement_decision_bundles`
-- are the persistence the review control rests on.  Dropping one, widening one,
-- or loosening the rules that bind them together would each silently re-open
-- review finding U2 without failing any behavioural test, because the
-- coordinator would simply never reach the branch.  They are therefore asserted
-- as a contract in the style WP-01a uses for the rest of the Gate 1 schema.
do $verify_publication_review_columns$
declare
  v_missing text;
  v_checks text[];
begin
  select pg_catalog.string_agg(expected.relation||'.'||expected.column_name,', ')
    into v_missing
  from (values
    ('public.weekly_source_entitlement_decision_bundles','whole_root_review_required','boolean',true),
    ('public.weekly_source_entitlement_decision_bundles','whole_root_reviewed_by_user_id','uuid',false),
    ('public.weekly_source_entitlement_decision_bundles','whole_root_reviewed_at_utc','timestamp with time zone',false)
  ) as expected(relation,column_name,type_name,is_not_null)
  where not exists (
    select 1
      from pg_catalog.pg_attribute a
     where a.attrelid=pg_catalog.to_regclass(expected.relation)
       and a.attname=expected.column_name
       and a.attnum>0
       and not a.attisdropped
       and pg_catalog.format_type(a.atttypid,null)=expected.type_name
       and a.attnotnull=expected.is_not_null);
  if v_missing is not null then
    raise exception 'whole-root review column contract differs: %',v_missing;
  end if;

  -- `whole_root_review_required` must keep its false default: a bundle written
  -- by any other package must not accidentally arrive already "reviewed".
  perform pg_temp.assert_true(
    (select pg_catalog.pg_get_expr(d.adbin,d.adrelid)
       from pg_catalog.pg_attrdef d
       join pg_catalog.pg_attribute a on a.attrelid=d.adrelid and a.attnum=d.adnum
      where d.adrelid='public.weekly_source_entitlement_decision_bundles'::regclass
        and a.attname='whole_root_review_required')='false',
    'whole_root_review_required must default to false');

  -- The reviewer is a FOREIGN KEY with ON DELETE RESTRICT (WP-01a's discipline
  -- for an identity): a reviewer who gave an approval that let a root through
  -- can never be deleted out from under the record.
  perform pg_temp.assert_true(
    exists (
      select 1
        from pg_catalog.pg_constraint c
        join pg_catalog.pg_attribute a
          on a.attrelid=c.conrelid and a.attnum=c.conkey[1]
       where c.conrelid='public.weekly_source_entitlement_decision_bundles'::regclass
         and c.contype='f'
         and pg_catalog.cardinality(c.conkey)=1
         and a.attname='whole_root_reviewed_by_user_id'
         and c.confrelid='public.tms_users'::regclass
         and c.confdeltype='r'),
    'whole_root_reviewed_by_user_id must be a foreign key to public.tms_users '
      ||'with on delete restrict');

  -- The three rules that make the trio a single fact rather than three loose
  -- columns, compared as normalised CHECK text so weakening any one fails here
  -- even when no negative test happens to exercise it.
  select coalesce(pg_catalog.array_agg(
           pg_catalog.lower(pg_catalog.regexp_replace(
             pg_catalog.pg_get_constraintdef(c.oid),'\s+','','g'))
           order by 1),array[]::text[])
    into v_checks
    from pg_catalog.pg_constraint c
   where c.conrelid='public.weekly_source_entitlement_decision_bundles'::regclass
     and c.contype='c'
     and pg_catalog.pg_get_constraintdef(c.oid) ~ 'whole_root';
  perform pg_temp.assert_true(
    v_checks @> array['check((whole_root_review_required=(whole_root_reviewed_by_user_idisnotnull)))'],
    'a recorded review must name a reviewer, and a named reviewer must mean a required review');
  perform pg_temp.assert_true(
    v_checks @> array['check(((whole_root_reviewed_by_user_idisnull)=(whole_root_reviewed_at_utcisnull)))'],
    'a recorded review must carry a reviewer AND a time, never one of the two');
  perform pg_temp.assert_true(
    v_checks @> array['check(((notwhole_root_review_required)or(bundle_kind=''cross_contract_a_b''::text)))'],
    'only a cross-Contract bundle can carry a whole-root review, because the review is '
      ||'about the TARGET root');
  perform pg_temp.assert_true(pg_catalog.cardinality(v_checks)=3,
    'exactly three CHECKs bind the whole-root review columns, got '||v_checks::text);

  -- The audit index that makes "show me every bundle that needed a review, and
  -- who gave it" an indexed question rather than a sequential scan.
  perform pg_temp.assert_true(
    exists (
      select 1
        from pg_catalog.pg_index i
       where i.indrelid='public.weekly_source_entitlement_decision_bundles'::regclass
         and i.indpred is not null
         and pg_catalog.pg_get_indexdef(i.indexrelid)
             ~ 'whole_root_reviewed_by_user_id.*decision_bundle_id.*bundle_revision'
         and pg_catalog.pg_get_expr(i.indpred,i.indrelid) ~ 'whole_root_review_required'),
    'the partial audit index over the whole-root review must exist');

  -- IDENTITY, not lifecycle: the ACL closure must NOT list any of the three as
  -- a writable lifecycle column, or a caller could attach to a bundle the very
  -- approval that bundle needs.  Checked against the installed contract when the
  -- ACL package is present.
  if pg_catalog.to_regprocedure(
       'private._weekly_source_acl_lifecycle_column_contract_v1()') is not null then
    perform pg_temp.assert_true(
      not exists (
        select 1
          from private._weekly_source_acl_lifecycle_column_contract_v1() as contract_row
         where contract_row.table_name='weekly_source_entitlement_decision_bundles'
           and contract_row.column_name in ('whole_root_review_required',
                                            'whole_root_reviewed_by_user_id',
                                            'whole_root_reviewed_at_utc')),
      'the whole-root review columns are IDENTITY: none of them may be registered as a '
        ||'writable lifecycle column');
    -- And the relation really is the lifecycle-guarded class that makes that
    -- absence mean "immutable after insert" rather than "unguarded".
    perform pg_temp.assert_true(
      exists (
        select 1
          from private._weekly_source_acl_table_contract_v1() as contract_row
         where contract_row.table_name='weekly_source_entitlement_decision_bundles'
           and contract_row.record_class='IMMUTABLE_FACTS_WITH_LIFECYCLE'),
      'the decision bundle relation must stay IMMUTABLE_FACTS_WITH_LIFECYCLE, or the '
        ||'identity classification of the review columns means nothing');
  end if;

  -- WP-10 review finding F3: the Workbench selector's STAGED probe reads the
  -- head relation by PHYSICAL root id with no candidate_id, so it needs an index
  -- that leads with root_timesheet_id.  Dropping it is invisible to every
  -- behavioural test — the query still returns the right answer, just by
  -- sequential scan over history that only grows — so it is asserted here.
  perform pg_temp.assert_true(
    exists (
      select 1
        from pg_catalog.pg_index i
        join pg_catalog.pg_attribute a
          on a.attrelid=i.indrelid and a.attnum=i.indkey[0]
       where i.indrelid='public.weekly_source_entitlement_heads'::regclass
         and a.attname='root_timesheet_id'
         and i.indpred is not null
         and pg_catalog.pg_get_expr(i.indpred,i.indrelid) ~ 'STAGED'),
    'an index leading with root_timesheet_id, partial on the STAGED state, must serve the '
      ||'Workbench selector''s staged-head probe');
  -- And it must NOT be unique: after schema change S8 the physical root id is
  -- not an identity, and more than one staged head per root is the very state
  -- file 26 Gate 4 fails closed on rather than a constraint violation.
  perform pg_temp.assert_true(
    not exists (
      select 1
        from pg_catalog.pg_index i
        join pg_catalog.pg_attribute a
          on a.attrelid=i.indrelid and a.attnum=i.indkey[0]
       where i.indrelid='public.weekly_source_entitlement_heads'::regclass
         and a.attname='root_timesheet_id'
         and i.indpred is not null
         and pg_catalog.pg_get_expr(i.indpred,i.indrelid) ~ 'STAGED'
         and i.indisunique),
    'the staged-head probe index is a performance structure and must never imply a '
      ||'uniqueness the physical root id does not have');
end
$verify_publication_review_columns$;

-- ---------------------------------------------------------------------------
-- 2. Golden vectors for the one canonical encoder (proof/32 section 9; H2-032)
-- ---------------------------------------------------------------------------
-- Each vector asserts the canonical TEXT and then asserts that the digest of
-- that text, hashed independently here, equals what the installed encoder
-- produces.  The vectors are proved, not remembered.
do $verify_publication_golden_vectors$
declare
  v_case record;
  v_actual_text text;
begin
  for v_case in
    select * from (values
      ('G1 empty object','{}'::jsonb,'{}'),
      ('G2 sorted keys and a string','{"b":"x","a":1}'::jsonb,'{"a":1,"b":"x"}'),
      -- PostgreSQL's own jsonb order is length-first then bytewise, so the
      -- encoder must sort explicitly.  {"b":2,"aa":1} renders as {"b": 2, "aa": 1}
      -- inside PostgreSQL and must render as {"aa":1,"b":2} here.
      ('G3 sorted, not length-first','{"b":2,"aa":1}'::jsonb,'{"aa":1,"b":2}'),
      ('G4 negative integer','{"n":-10}'::jsonb,'{"n":-10}'),
      ('G5 integral scale is normalised','{"n":1.0}'::jsonb,'{"n":1}'),
      ('G6 nested array of mixed scalars','[1,"a",true,null,{"k":2}]'::jsonb,
       '[1,"a",true,null,{"k":2}]'),
      ('G7 empty array','[]'::jsonb,'[]')
    ) as vectors(label,input,expected_text)
  loop
    v_actual_text:=private.weekly_source_canonical_json_text_v1(v_case.input);
    perform pg_temp.assert_true(
      v_actual_text=v_case.expected_text,
      v_case.label||': expected '||v_case.expected_text||' but got '||v_actual_text);
    perform pg_temp.assert_true(
      private.weekly_source_publication_request_digest_v1(v_case.input)
      =pg_catalog.sha256(pg_catalog.convert_to(v_case.expected_text,'UTF8')),
      v_case.label||': digest must be SHA-256 of the UTF-8 bytes of the canonical text');
  end loop;

  -- String escaping, built from character codes so the file itself carries no
  -- control characters.
  perform pg_temp.assert_true(
    private.weekly_source_canonical_json_text_v1(
      pg_catalog.jsonb_build_object('s',pg_catalog.chr(34)||pg_catalog.chr(92)||pg_catalog.chr(10)))
    ='{"s":"'||pg_catalog.chr(92)||pg_catalog.chr(34)
             ||pg_catalog.chr(92)||pg_catalog.chr(92)
             ||pg_catalog.chr(92)||'n"}',
    'G8: quote, backslash and newline use the short JSON escapes');
  perform pg_temp.assert_true(
    private.weekly_source_canonical_json_text_v1(
      pg_catalog.jsonb_build_object('u','a'||pg_catalog.chr(1)||'b'))
    ='{"u":"a'||pg_catalog.chr(92)||'u0001b"}',
    'G9: a control character below 0x20 is escaped as \u00xx with lower-case hex');
  perform pg_temp.assert_true(
    private.weekly_source_canonical_json_text_v1(
      pg_catalog.jsonb_build_object('z',pg_catalog.chr(233)))
    ='{"z":"'||pg_catalog.chr(233)||'"}',
    'G10: non-ASCII is emitted literally as UTF-8, never \u-escaped');

  -- The digest must be able to tell an integer from its decimal string.
  perform pg_temp.assert_true(
    private.weekly_source_publication_request_digest_v1('{"n":3}'::jsonb)
    <>private.weekly_source_publication_request_digest_v1('{"n":"3"}'::jsonb),
    'G11: an integer and its decimal string must not collide');

  -- A non-integral JSON number can never reach a money digest.
  perform pg_temp.expect_failure(
    $sql$select private.weekly_source_publication_request_digest_v1('{"n":1.5}'::jsonb);$sql$,
    '22023','WEEKLY_SOURCE_PUBLICATION_DIGEST_NON_INTEGER',
    'G12: a non-integral JSON number in the digest scope');

  -- ---- review finding F6: the digest may not move with the session ------
  -- `date::text` honours DateStyle, so the same decision digested differently
  -- in a `German, DMY` session and an `SQL, MDY` one.  The encoder now renders
  -- dates by pattern.  Measured under three session settings, one value.
  perform pg_temp.assert_true(
    (select pg_catalog.count(distinct vector_digest)
       from (
         select pg_catalog.encode(
                  private.weekly_source_publication_request_digest_v1(
                    pg_catalog.jsonb_build_object(
                      'd',private.weekly_source_publication_scalar_v1(
                            '"2026-03-02"'::jsonb,'d','DATE'))),'hex') as vector_digest
         from pg_catalog.generate_series(1,1)) as iso_run)=1,
    'G13: the date vector digests to one value');
  perform pg_temp.assert_true(
    private.weekly_source_publication_scalar_v1('"2026-03-02"'::jsonb,'d','DATE')
    ='"2026-03-02"'::jsonb,
    'G13: the canonical date is ISO regardless of the session');

  -- ---- review finding F9: no collision through numeric formatting -------
  -- `to_char` with a fixed picture printed `##################.##` for any
  -- 19-digit value, so two different amounts digested identically.  The
  -- canonicaliser now refuses a magnitude the numeric(18,s) column cannot hold.
  perform pg_temp.assert_true(
    private.weekly_source_publication_scalar_v1('"9223372036854775807"'::jsonb,'n','TEXT')
    ='"9223372036854775807"'::jsonb,
    'G14: 2^63-1 as text is carried exactly');
  perform pg_temp.assert_true(
    private.weekly_source_canonical_json_text_v1('{"n":9223372036854775807}'::jsonb)
    ='{"n":9223372036854775807}',
    'G14: 2^63-1 as an integer renders as exact base-10 text');
  perform pg_temp.assert_true(
    private.weekly_source_publication_scalar_v1('"1234567890123456.78"'::jsonb,'n','DEC',2)
    <>private.weekly_source_publication_scalar_v1('"2234567890123456.78"'::jsonb,'n','DEC',2),
    'G15: two different amounts at the column limit do not collide');
  perform pg_temp.expect_failure(
    $sql$select private.weekly_source_publication_scalar_v1(
      '"1000000000000000000.00"'::jsonb,'n','DEC',2);$sql$,
    '22023','DECIMAL_MAGNITUDE_EXCEEDED',
    'G15: a magnitude the column cannot hold is refused, never printed as #');
end
$verify_publication_golden_vectors$;

-- ---------------------------------------------------------------------------
-- 3. Minimum legal fixture
-- ---------------------------------------------------------------------------
insert into public.settings_defaults(
  id,candidate_manager_email_templates_sha256,candidate_home_announcement_sha256
) values (
  1,pg_catalog.decode(pg_catalog.repeat('01',32),'hex'),
  pg_catalog.decode(pg_catalog.repeat('02',32),'hex')
) on conflict (id) do update set
  candidate_manager_email_templates_sha256=excluded.candidate_manager_email_templates_sha256;

-- Two real users: the one who decides, and a SECOND one who exists but did not
-- give the whole-root review, so "the request quotes a different reviewer from
-- the one the accepted decision carries" can be staged against a reviewer that
-- really exists rather than one the foreign key would have rejected anyway.
insert into public.tms_users(id,email,role,is_active,password_hash)
values ('c0000000-0000-4000-8000-000000000001','publication@example.test','admin',true,'not-a-login'),
       ('c0000000-0000-4000-8000-000000000051','other-reviewer@example.test','admin',true,'not-a-login');
insert into public.clients(id,name)
values ('c0000000-0000-4000-8000-000000000002','Publication Client');
insert into public.client_settings(client_id,vat_rate_pct,effective_from)
values ('c0000000-0000-4000-8000-000000000002',20,'2026-01-01');
insert into public.candidates(id,display_name) values
 ('c0000000-0000-4000-8000-000000000003','Publication Candidate'),
 ('c0000000-0000-4000-8000-000000000023','Other Candidate');
insert into public.contracts(
  id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
  weekly_timesheet_source,self_bill,no_timesheet_required,requires_hr,autoprocess_hr
) values
 ('c0000000-0000-4000-8000-000000000004','c0000000-0000-4000-8000-000000000003',
  'c0000000-0000-4000-8000-000000000002','2026-01-01','2026-12-31','PAYE','{}'::jsonb,
  'HEALTHROSTER',true,true,true,true),
 ('c0000000-0000-4000-8000-000000000014','c0000000-0000-4000-8000-000000000003',
  'c0000000-0000-4000-8000-000000000002','2026-01-01','2026-12-31','PAYE','{}'::jsonb,
  'HEALTHROSTER',true,true,true,true);
insert into public.contract_weeks(id,contract_id,week_ending_date) values
 ('c0000000-0000-4000-8000-000000000005','c0000000-0000-4000-8000-000000000004','2026-03-08'),
 ('c0000000-0000-4000-8000-000000000015','c0000000-0000-4000-8000-000000000014','2026-03-08');
insert into public.timesheets(
  timesheet_id,booking_id,version,is_current,status,sheet_scope,submission_mode,line_type,
  occupant_key_norm,hospital_norm,ward_norm,job_title_norm,shift_label_norm,
  week_ending_date,contract_id,actual_schedule_json,qr_payload_json,is_adjustment,
  created_at,updated_at
) values
 ('c0000000-0000-4000-8000-000000000006','WSPUB-0001',1,true,
  'RECEIVED'::public.timesheet_status_enum,'WEEKLY'::public.timesheet_scope_enum,
  'MANUAL'::public.submission_mode_enum,'HOURS'::public.timesheet_line_type_enum,
  'pub-occupant-a','pub-hospital','pub-ward','pub-role','weekly-0','2026-03-08',
  'c0000000-0000-4000-8000-000000000004','[]'::jsonb,'{}'::jsonb,false,
  pg_catalog.statement_timestamp(),pg_catalog.statement_timestamp()),
 ('c0000000-0000-4000-8000-000000000016','WSPUB-0002',1,true,
  'RECEIVED'::public.timesheet_status_enum,'WEEKLY'::public.timesheet_scope_enum,
  'MANUAL'::public.submission_mode_enum,'HOURS'::public.timesheet_line_type_enum,
  'pub-occupant-b','pub-hospital','pub-ward','pub-role','weekly-0','2026-03-08',
  'c0000000-0000-4000-8000-000000000014','[]'::jsonb,'{}'::jsonb,false,
  pg_catalog.statement_timestamp(),pg_catalog.statement_timestamp()),
 ('c0000000-0000-4000-8000-000000000026','WSPUB-0003',1,true,
  'RECEIVED'::public.timesheet_status_enum,'WEEKLY'::public.timesheet_scope_enum,
  'MANUAL'::public.submission_mode_enum,'HOURS'::public.timesheet_line_type_enum,
  'pub-occupant-c','pub-hospital','pub-ward','pub-role','weekly-0','2026-03-08',
  'c0000000-0000-4000-8000-000000000004','[]'::jsonb,'{}'::jsonb,false,
  pg_catalog.statement_timestamp(),pg_catalog.statement_timestamp()),
 -- Review finding U2: a B Timesheet that ALREADY EXISTS, was never touched by
 -- Weekly Source, and carries its own unrelated hand-entered shifts.  It is
 -- unknown to Weekly Source but it is NOT blank.
 ('c0000000-0000-4000-8000-000000000036','WSPUB-0004',1,true,
  'RECEIVED'::public.timesheet_status_enum,'WEEKLY'::public.timesheet_scope_enum,
  'MANUAL'::public.submission_mode_enum,'HOURS'::public.timesheet_line_type_enum,
  'pub-occupant-d','pub-hospital','pub-ward','pub-role','weekly-0','2026-03-08',
  'c0000000-0000-4000-8000-000000000014',
  -- Real work intervals, in the shape the installed cross-record overlap guard
  -- accepts (`TIMESHEET_WORK_INTERVAL_INVALID` otherwise): once the whole-root
  -- review can be persisted, the reviewed case really does reach the ordinary
  -- Authorise owner, and an unparseable schedule would fail there for a reason
  -- that has nothing to do with what this fixture is testing.
  '[{"work_date":"2026-03-05","start":"09:00","end":"16:30"},
    {"work_date":"2026-03-06","start":"09:00","end":"16:30"}]'::jsonb,
  '{}'::jsonb,false,
  pg_catalog.statement_timestamp(),pg_catalog.statement_timestamp()),
 -- A second provably blank root, kept for the interface I-6 breach case.
 ('c0000000-0000-4000-8000-000000000046','WSPUB-0005',1,true,
  'RECEIVED'::public.timesheet_status_enum,'WEEKLY'::public.timesheet_scope_enum,
  'MANUAL'::public.submission_mode_enum,'HOURS'::public.timesheet_line_type_enum,
  'pub-occupant-e','pub-hospital','pub-ward','pub-role','weekly-0','2026-03-08',
  'c0000000-0000-4000-8000-000000000014','[]'::jsonb,'{}'::jsonb,false,
  pg_catalog.statement_timestamp(),pg_catalog.statement_timestamp());

-- The ordinary Authorise owner that interface I-6 calls looks its Contract week
-- up BY TIMESHEET (contract_weeks.timesheet_id, 14082026_1310_...:1692-1700),
-- so the two roots that are actually authorised in this file carry their own.
insert into public.contract_weeks(id,contract_id,week_ending_date,additional_seq,timesheet_id) values
 ('c0000000-0000-4000-8000-000000000025','c0000000-0000-4000-8000-000000000004','2026-03-08',1,
  'c0000000-0000-4000-8000-000000000026'),
 ('c0000000-0000-4000-8000-000000000045','c0000000-0000-4000-8000-000000000004','2026-03-08',2,
  'c0000000-0000-4000-8000-000000000046'),
 -- WSPUB-0004 is the pre-existing, NON-blank B of review finding U2.  Once the
 -- whole-root review can be persisted (handoff N9) the reviewed case really does
 -- proceed to authorise it, so it needs what the ordinary Authorise owner needs.
 ('c0000000-0000-4000-8000-000000000035','c0000000-0000-4000-8000-000000000004','2026-03-08',4,
  'c0000000-0000-4000-8000-000000000036');

-- The ordinary Authorise owner that interface I-6 calls requires a CURRENT
-- financial snapshot on the root (`TARGET_NOT_FOUND`/`NO_TSFIN`).  A genuinely
-- new B root is prepared by the source pipeline in the same transaction, so the
-- roots that get authorised here carry one, created now.  A snapshot created in
-- THIS transaction does not make a root non-blank — only one that predates the
-- transaction does — so WSPUB-0004 still fails the blank test on the thing that
-- really disqualifies it: its own unrelated schedule.
insert into public.timesheets_financials(
  timesheet_id,timesheet_version,candidate_id,is_current,processing_status
) values
 ('c0000000-0000-4000-8000-000000000026',1,'c0000000-0000-4000-8000-000000000003',true,
  'PENDING_AUTH'::public.ts_fin_processing_status_enum),
 ('c0000000-0000-4000-8000-000000000036',1,'c0000000-0000-4000-8000-000000000003',true,
  'PENDING_AUTH'::public.ts_fin_processing_status_enum),
 ('c0000000-0000-4000-8000-000000000046',1,'c0000000-0000-4000-8000-000000000003',true,
  'PENDING_AUTH'::public.ts_fin_processing_status_enum);

insert into public.weekly_source_format_profiles(
  id,profile_code,version,final_authority_kind,container_kind,omission_meaning,
  row_finalisation_capability,worked_duration_authority,profile_json,profile_sha256
) values (
  'c0000000-0000-4000-8000-0000000000f1','PUBLICATION_PROOF',1,
  'GENERIC_COMPLETE_SNAPSHOT','XLSX','CANCEL_INSIDE_CONFIRMED_COVERAGE','NONE',
  'SOURCE_ACTUAL','{}'::jsonb,pg_catalog.decode(pg_catalog.repeat('a1',32),'hex'));
insert into public.weekly_source_groups(
  id,environment,agency_id,code,display_name,source_family,cutoff_weekday,cutoff_local_time
) values (
  'c0000000-0000-4000-8000-0000000000f2','TEST','c0000000-0000-4000-8000-0000000000aa',
  'PUBLICATION_GROUP','Publication Group','ROSTER',3,'15:00');
insert into public.weekly_source_cycles(id,source_group_id,finalisation_week_ending,cutoff_at_utc)
values ('c0000000-0000-4000-8000-0000000000f3','c0000000-0000-4000-8000-0000000000f2',
        '2026-03-08',pg_catalog.clock_timestamp());
insert into public.weekly_source_uploads(
  id,source_cycle_id,original_filename,content_sha256,byte_count,source_format_profile_id,
  parser_version,normaliser_version,header_coordinate_map_hash,declared_scope_fingerprint,
  coverage_proof_kind,physical_row_count,uploaded_by_user_id
) values (
  'c0000000-0000-4000-8000-0000000000f4','c0000000-0000-4000-8000-0000000000f3',
  'publication.xlsx',pg_catalog.decode(pg_catalog.repeat('a2',32),'hex'),1024,
  'c0000000-0000-4000-8000-0000000000f1','p1','n1',
  pg_catalog.decode(pg_catalog.repeat('a3',32),'hex'),
  pg_catalog.decode(pg_catalog.repeat('a4',32),'hex'),
  'HEALTHROSTER_COMPLETE_EXPORT_ATTESTATION',0,'c0000000-0000-4000-8000-000000000001');
insert into public.weekly_source_upload_rows(
  id,upload_id,source_row_ordinal,source_candidate_identity,source_client_identity,
  work_date,start_at_local,end_at_local,break_minutes,actual_net_minutes,normalised_row_hash
) values
 ('c0000000-0000-4000-8000-0000000000f5','c0000000-0000-4000-8000-0000000000f4',1,
  'cand-1','client-1','2026-03-02','2026-03-02 08:00','2026-03-02 16:00',30,450,
  pg_catalog.decode(pg_catalog.repeat('a5',32),'hex')),
 ('c0000000-0000-4000-8000-0000000000e5','c0000000-0000-4000-8000-0000000000f4',2,
  'cand-1','client-1','2026-03-03','2026-03-03 08:00','2026-03-03 16:00',30,450,
  pg_catalog.decode(pg_catalog.repeat('b5',32),'hex')),
 ('c0000000-0000-4000-8000-0000000000d5','c0000000-0000-4000-8000-0000000000f4',3,
  'cand-1','client-1','2026-03-04','2026-03-04 08:00','2026-03-04 16:00',30,450,
  pg_catalog.decode(pg_catalog.repeat('c5',32),'hex'));
insert into public.weekly_work_events(
  id,candidate_id,client_id,work_date,identity_kind,durable_identity_hash,
  source_format_profile_id,profile_external_key
) values
 ('c0000000-0000-4000-8000-0000000000f6','c0000000-0000-4000-8000-000000000003',
  'c0000000-0000-4000-8000-000000000002','2026-03-02','PROFILE_EXTERNAL_KEY',
  pg_catalog.decode(pg_catalog.repeat('a6',32),'hex'),
  'c0000000-0000-4000-8000-0000000000f1','publication-external-key-a'),
 ('c0000000-0000-4000-8000-0000000000e6','c0000000-0000-4000-8000-000000000003',
  'c0000000-0000-4000-8000-000000000002','2026-03-03','PROFILE_EXTERNAL_KEY',
  pg_catalog.decode(pg_catalog.repeat('b6',32),'hex'),
  'c0000000-0000-4000-8000-0000000000f1','publication-external-key-b'),
 ('c0000000-0000-4000-8000-0000000000d6','c0000000-0000-4000-8000-000000000003',
  'c0000000-0000-4000-8000-000000000002','2026-03-04','PROFILE_EXTERNAL_KEY',
  pg_catalog.decode(pg_catalog.repeat('c6',32),'hex'),
  'c0000000-0000-4000-8000-0000000000f1','publication-external-key-c');
insert into public.weekly_source_row_resolutions(
  id,upload_row_id,generation,mapping_state,qualification_profile_fingerprint,
  qualifying_contract_set_hash,source_row_fingerprint,work_event_id,candidate_id,client_id,
  contract_id,contract_selection_method,work_event_match_kind,work_event_match_fingerprint
) values
 ('c0000000-0000-4000-8000-0000000000f7','c0000000-0000-4000-8000-0000000000f5',1,'RESOLVED',
  pg_catalog.decode(pg_catalog.repeat('a7',32),'hex'),
  pg_catalog.decode(pg_catalog.repeat('a8',32),'hex'),
  pg_catalog.decode(pg_catalog.repeat('a9',32),'hex'),'c0000000-0000-4000-8000-0000000000f6',
  'c0000000-0000-4000-8000-000000000003','c0000000-0000-4000-8000-000000000002',
  'c0000000-0000-4000-8000-000000000004','AUTO_UNIQUE','NEW_PROFILE_KEY',
  pg_catalog.decode(pg_catalog.repeat('aa',32),'hex')),
 ('c0000000-0000-4000-8000-0000000000e7','c0000000-0000-4000-8000-0000000000e5',1,'RESOLVED',
  pg_catalog.decode(pg_catalog.repeat('b7',32),'hex'),
  pg_catalog.decode(pg_catalog.repeat('b8',32),'hex'),
  pg_catalog.decode(pg_catalog.repeat('b9',32),'hex'),'c0000000-0000-4000-8000-0000000000e6',
  'c0000000-0000-4000-8000-000000000003','c0000000-0000-4000-8000-000000000002',
  'c0000000-0000-4000-8000-000000000014','AUTO_UNIQUE','NEW_PROFILE_KEY',
  pg_catalog.decode(pg_catalog.repeat('ba',32),'hex')),
 ('c0000000-0000-4000-8000-0000000000d7','c0000000-0000-4000-8000-0000000000d5',1,'RESOLVED',
  pg_catalog.decode(pg_catalog.repeat('c7',32),'hex'),
  pg_catalog.decode(pg_catalog.repeat('c8',32),'hex'),
  pg_catalog.decode(pg_catalog.repeat('c9',32),'hex'),'c0000000-0000-4000-8000-0000000000d6',
  'c0000000-0000-4000-8000-000000000003','c0000000-0000-4000-8000-000000000002',
  'c0000000-0000-4000-8000-000000000014','AUTO_UNIQUE','NEW_PROFILE_KEY',
  pg_catalog.decode(pg_catalog.repeat('ca',32),'hex'));

-- The final source revision the proposal was built from (24 section 4.5 step 2),
-- plus a superseded one so R7 has something real to fail against.
insert into public.weekly_source_final_revisions(
  id,source_cycle_id,authority_scope_kind,revision_number,upload_id,
  coverage_start_local_date,coverage_end_local_date,coverage_timezone,reason,
  finalised_by_user_id,manifest_hash,policy_fingerprint,state
) values
 ('c0000000-0000-4000-8000-0000000000fa','c0000000-0000-4000-8000-0000000000f3','CYCLE',1,
  'c0000000-0000-4000-8000-0000000000f4','2026-03-02','2026-03-08','Europe/London',
  'INITIAL_FINALISATION','c0000000-0000-4000-8000-000000000001',
  pg_catalog.decode(pg_catalog.repeat('c1',32),'hex'),
  pg_catalog.decode(pg_catalog.repeat('c2',32),'hex'),'CURRENT'),
 ('c0000000-0000-4000-8000-0000000000fb','c0000000-0000-4000-8000-0000000000f3','CYCLE',2,
  'c0000000-0000-4000-8000-0000000000f4','2026-03-02','2026-03-08','Europe/London',
  'CORRECT_FINAL_SOURCE','c0000000-0000-4000-8000-000000000001',
  pg_catalog.decode(pg_catalog.repeat('c3',32),'hex'),
  pg_catalog.decode(pg_catalog.repeat('c4',32),'hex'),'SUPERSEDED');

-- The per-source-row BINDING, in its decision-D8 shape: the authorisation facts
-- have moved off this relation.
insert into public.weekly_source_row_timesheet_lineages(
  row_resolution_id,source_cycle_id,work_event_id,candidate_id,client_id,contract_id,
  contract_week_id,timesheet_id,family_booking_id,timesheet_version,
  week_ending_date,lineage_fingerprint
) values
 ('c0000000-0000-4000-8000-0000000000f7','c0000000-0000-4000-8000-0000000000f3',
  'c0000000-0000-4000-8000-0000000000f6','c0000000-0000-4000-8000-000000000003',
  'c0000000-0000-4000-8000-000000000002','c0000000-0000-4000-8000-000000000004',
  'c0000000-0000-4000-8000-000000000005','c0000000-0000-4000-8000-000000000006',
  'WSPUB-0001',1,'2026-03-08',
  pg_catalog.decode(pg_catalog.repeat('d1',32),'hex')),
 ('c0000000-0000-4000-8000-0000000000e7','c0000000-0000-4000-8000-0000000000f3',
  'c0000000-0000-4000-8000-0000000000e6','c0000000-0000-4000-8000-000000000003',
  'c0000000-0000-4000-8000-000000000002','c0000000-0000-4000-8000-000000000014',
  'c0000000-0000-4000-8000-000000000015','c0000000-0000-4000-8000-000000000016',
  'WSPUB-0002',1,'2026-03-08',
  pg_catalog.decode(pg_catalog.repeat('d2',32),'hex'));

-- Decision D8: the AUTHORISATION record is per root, in
-- public.weekly_source_root_authorisations, written by the first-authorisation
-- owner (I-6).  Generation 1 for roots A (WSPUB-0001) and B (WSPUB-0002).
-- WSPUB-0003 is deliberately left with NO authorisation row at all: it is the
-- "genuinely new, never-authorised root" of 24 section 4.5 step 4.
insert into public.weekly_source_root_authorisations(
  root_timesheet_id,family_booking_id,timesheet_version,authorisation_generation,
  authorised_row_signature,authorised_by_user_id
) values
 ('c0000000-0000-4000-8000-000000000006','WSPUB-0001',1,1,
  'signature-a-generation-1','c0000000-0000-4000-8000-000000000001'),
 ('c0000000-0000-4000-8000-000000000016','WSPUB-0002',1,1,
  'signature-b-generation-1','c0000000-0000-4000-8000-000000000001');

-- ---------------------------------------------------------------------------
-- 4. Request builders (interface I-3) and hand-built I-1 lock results
-- ---------------------------------------------------------------------------
-- The economic key fields are derived from the component IDENTITY, not from its
-- position: a component that moves between heads, or that stays while another
-- moves out from in front of it, is the same component.  (The first version
-- derived them from the ordinal, which made a legal A-to-B move look like a
-- re-pricing once the retained-component check landed.)
create function pg_temp.component(
  p_ordinal integer,p_id uuid,p_hours text,p_pay text,p_movement uuid default null
) returns jsonb language sql immutable as $function$
  select pg_catalog.jsonb_build_object(
    'component_ordinal',p_ordinal,'component_id',p_id,'component_kind','WORKED_TIME',
    'economic_key_type','SEGMENT',
    'economic_key_value','seg-'||pg_catalog.right(p_id::text,4),
    'component_member_identity','mem-'||pg_catalog.right(p_id::text,4),
    'segment_id',null,'segment_key',null,'segment_stable_key',null,
    'work_date','2026-03-02','reference_number',null,
    'hours_day',p_hours,'hours_night',null,'hours_sat',null,'hours_sun',null,'hours_bh',null,
    'additional_code_raw',null,'unit_count',null,'unit_pay_rate',null,'unit_charge_rate',null,
    'expense_code',null,'pay_ex_vat',p_pay,'charge_ex_vat',null,
    'exclude_from_pay',false,'origin','WEEKLY_SOURCE',
    'movement_id',p_movement,'movement_group_id',null);
$function$;

create function pg_temp.single_root_request(
  p_bundle uuid,p_revision bigint,p_head uuid,p_decision uuid,p_expected_head uuid,
  p_before jsonb,p_components jsonb,
  p_candidate uuid default 'c0000000-0000-4000-8000-000000000003',
  p_revision_id uuid default 'c0000000-0000-4000-8000-0000000000fa',
  p_manifest text default pg_catalog.repeat('c1',32),
  p_policy text default pg_catalog.repeat('c2',32),
  p_revision_number integer default 1
) returns jsonb language sql immutable as $function$
  select pg_catalog.jsonb_build_object(
    'decision_bundle_id',p_bundle,'pending_bundle_id',null,'bundle_revision',p_revision,
    'candidate_id',p_candidate,
    'member_root_ids',pg_catalog.jsonb_build_array('c0000000-0000-4000-8000-000000000006'),
    'member_family_booking_ids',pg_catalog.jsonb_build_array('WSPUB-0001'),
    'member_root_versions',pg_catalog.jsonb_build_array(1),
    'head_ids',pg_catalog.jsonb_build_array(p_head),
    'decision_id',p_decision,'publication_mode','IMMEDIATE',
    'financial_request',pg_catalog.jsonb_build_object(
      'source_revision',pg_catalog.jsonb_build_object(
        'final_revision_id',p_revision_id,
        'source_cycle_id','c0000000-0000-4000-8000-0000000000f3',
        'revision_number',p_revision_number,
        'manifest_hash',p_manifest,'policy_fingerprint',p_policy),
      'contract_choices',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object('root_ordinal',1,
          'contract_id','c0000000-0000-4000-8000-000000000004',
          'week_ending_date','2026-03-08','selection_method','UNCHANGED')),
      'member_entitlements',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object('root_ordinal',1,
          'authority_kind','LOCKED_FINAL_SOURCE',
          'certified_zero',pg_catalog.jsonb_array_length(p_components)=0,
          'component_count',pg_catalog.jsonb_array_length(p_components),
          'components',p_components))),
    'control',pg_catalog.jsonb_build_object(
      'bundle_kind','SINGLE_ROOT','reason','WEEKLY_SOURCE_ENTITLEMENT_PUBLICATION',
      'expected_current_head_ids',pg_catalog.jsonb_build_array(p_expected_head),
      'before_positions',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object('root_ordinal',1,'component_ids',p_before,
          'inventory_digest',pg_catalog.repeat('00',32))),
      'moved_component_ids',pg_catalog.jsonb_build_array(),
      'target_root_authorisation',null,'whole_root_office_review',null));
$function$;

create function pg_temp.lock_result(
  p_roots jsonb,p_bookings jsonb,p_versions jsonb
) returns jsonb language sql immutable as $function$
  select pg_catalog.jsonb_build_object('ok',true,'gate','GRANTED',
    'families',(
      select pg_catalog.jsonb_agg(
        pg_catalog.jsonb_build_object(
          'requested_timesheet_id',root_element.value,
          'family_booking_id',p_bookings->>(root_element.ordinality::integer-1),
          'canonical_timesheet_id',root_element.value,
          'canonical_version',(p_versions->>(root_element.ordinality::integer-1))::integer,
          'requested_is_canonical',true,'family_is_current',true,
          'member_timesheet_ids',pg_catalog.jsonb_build_array(root_element.value))
        order by root_element.ordinality)
      from pg_catalog.jsonb_array_elements_text(p_roots)
           with ordinality as root_element(value,ordinality)));
$function$;

create function pg_temp.lock_result_a() returns jsonb language sql immutable as $function$
  select pg_temp.lock_result(
    pg_catalog.jsonb_build_array('c0000000-0000-4000-8000-000000000006'),
    pg_catalog.jsonb_build_array('WSPUB-0001'),
    pg_catalog.jsonb_build_array(1));
$function$;

create function pg_temp.lock_result_b() returns jsonb language sql immutable as $function$
  select pg_temp.lock_result(
    pg_catalog.jsonb_build_array('c0000000-0000-4000-8000-000000000016'),
    pg_catalog.jsonb_build_array('WSPUB-0002'),
    pg_catalog.jsonb_build_array(1));
$function$;

-- A single-root request over root B (WSPUB-0002 / Contract …0014).
create function pg_temp.root_b_request(
  p_bundle uuid,p_head uuid,p_decision uuid,p_components jsonb,
  p_expected_head uuid default null,p_before jsonb default '[]'::jsonb
) returns jsonb language sql immutable as $function$
  select pg_catalog.jsonb_set(pg_catalog.jsonb_set(pg_catalog.jsonb_set(
    pg_temp.single_root_request(p_bundle,1,p_head,p_decision,p_expected_head,
                                p_before,p_components),
    '{member_root_ids}','["c0000000-0000-4000-8000-000000000016"]'::jsonb),
    '{member_family_booking_ids}','["WSPUB-0002"]'::jsonb),
    '{financial_request,contract_choices,0,contract_id}',
    '"c0000000-0000-4000-8000-000000000014"'::jsonb);
$function$;

create function pg_temp.publish(p_request jsonb,p_lock jsonb) returns jsonb
language sql volatile as $function$
  select private.weekly_source_entitlement_publish_core_v1(
    p_request,'IMMEDIATE',p_lock,null::uuid,null::text,null::uuid,'{}'::jsonb,'{}'::jsonb);
$function$;

-- The ACCEPTED decision bundle, derived from the request Office accepted.
--
-- The first version of this verifier wrote `request_digest = sha256('request:'||tag)`
-- — an arbitrary value nothing ever compared — which is exactly how review
-- finding U1 stayed invisible.  Every identity and every approval digest on the
-- bundle row is now computed from the accepted request by the installed owners,
-- so a later publication that disagrees with the accepted decision in ANY of
-- them is refused.
create function pg_temp.mk_bundle(
  p_accepted jsonb,
  p_reviewed_by uuid default null,
  p_reviewed_at timestamptz default null
) returns void language plpgsql as $function$
declare
  v_choice1 jsonb;
  v_choice2 jsonb;
  v_canonical jsonb;
  v_members integer:=pg_catalog.jsonb_array_length(p_accepted->'member_root_ids');
begin
  v_canonical:=private.weekly_source_publication_request_canonical_v1(p_accepted,'IMMEDIATE',null);
  select choice_element.value into v_choice1
    from pg_catalog.jsonb_array_elements(
           v_canonical->'financial_request'->'contract_choices') as choice_element(value)
   where (choice_element.value->>'root_ordinal')::integer=1;
  if v_members=2 then
    select choice_element.value into v_choice2
      from pg_catalog.jsonb_array_elements(
             v_canonical->'financial_request'->'contract_choices') as choice_element(value)
     where (choice_element.value->>'root_ordinal')::integer=2;
  end if;

  insert into public.weekly_source_entitlement_decision_bundles(
    decision_bundle_id,bundle_revision,agency_id,candidate_id,week_ending_date,bundle_kind,
    source_root_family_booking_id,source_root_timesheet_id,source_contract_id,
    target_root_family_booking_id,target_root_timesheet_id,target_contract_id,
    decision_id,decided_by_user_id,publication_mode,request_digest,source_revision_digest,
    contract_choice_digest,before_inventory_digest,proposed_head_ids,state,
    whole_root_review_required,whole_root_reviewed_by_user_id,whole_root_reviewed_at_utc
  ) values (
    (p_accepted->>'decision_bundle_id')::uuid,
    (p_accepted->>'bundle_revision')::bigint,
    'c0000000-0000-4000-8000-0000000000aa',
    (p_accepted->>'candidate_id')::uuid,
    (v_choice1->>'week_ending_date')::date,
    case when v_members=2 then 'CROSS_CONTRACT_A_B' else 'SINGLE_ROOT' end,
    p_accepted->'member_family_booking_ids'->>0,
    (p_accepted->'member_root_ids'->>0)::uuid,
    (v_choice1->>'contract_id')::uuid,
    case when v_members=2 then p_accepted->'member_family_booking_ids'->>1 end,
    case when v_members=2 then (p_accepted->'member_root_ids'->>1)::uuid end,
    case when v_members=2 then (v_choice2->>'contract_id')::uuid end,
    (p_accepted->>'decision_id')::uuid,
    'c0000000-0000-4000-8000-000000000001','IMMEDIATE',
    -- The acceptance digest: IMMEDIATE mode, no pending bundle.
    private.weekly_source_publication_request_digest_v1(v_canonical),
    private.weekly_source_publication_request_digest_v1(
      v_canonical->'financial_request'->'source_revision'),
    private.weekly_source_publication_request_digest_v1(
      v_canonical->'financial_request'->'contract_choices'),
    private.weekly_source_publication_request_digest_v1(
      private.weekly_source_publication_before_inventory_v1(p_accepted->'control',v_members)),
    (select coalesce(pg_catalog.array_agg(head_element.value::uuid
                                          order by head_element.ordinality),array[]::uuid[])
       from pg_catalog.jsonb_array_elements_text(p_accepted->'head_ids')
            with ordinality as head_element(value,ordinality)),
    'PROPOSED',
    -- The whole-root Office review, when the accepted decision carries one.
    -- These are IDENTITY columns, so they are written in the INSERT: an UPDATE
    -- afterwards is exactly what the ACL immutable-fact guard refuses, and the
    -- earlier version of this helper — which did update them — would have made
    -- the test lie about how a review can come to exist.
    p_reviewed_by is not null,p_reviewed_by,p_reviewed_at);
end;
$function$;

-- Give a request its own accepted decision bundle, so a negative case tests the
-- rule it names instead of tripping the U1 binding first.  Office is treated as
-- having ACCEPTED exactly this request; anything still refused is refused on its
-- own merits.
-- The real ordinary Authorise owner compares `expected_row_signature` with the
-- live signature (14082026_1310_...:1755), so the verifier asks the installed
-- owner for it rather than inventing one.  Falls back to the literal when the
-- signature owner is absent, which is what the I-6 TEST-DOUBLE expects.
create function pg_temp.row_signature(p_timesheet_id uuid,p_fallback text)
returns text language plpgsql as $function$
declare v_json jsonb;
begin
  if pg_catalog.to_regprocedure(
       'public.timesheet_lifecycle_guard_signature_v1(uuid,uuid,boolean)') is null then
    return p_fallback;
  end if;
  execute 'select public.timesheet_lifecycle_guard_signature_v1($1,null::uuid,false)'
    into v_json using p_timesheet_id;
  return coalesce(
    nullif(pg_catalog.btrim(coalesce(v_json->>'backend_row_signature','')),''),
    nullif(pg_catalog.btrim(coalesce(v_json->>'row_signature','')),''),
    nullif(pg_catalog.btrim(coalesce(v_json->>'signature','')),''),
    p_fallback);
end;
$function$;

create function pg_temp.accept(p_request jsonb,p_tag text) returns jsonb
language plpgsql as $function$
declare
  v_out jsonb;
  v_heads jsonb:='[]'::jsonb;
  v_i integer;
begin
  for v_i in 1..pg_catalog.jsonb_array_length(p_request->'member_root_ids') loop
    v_heads:=v_heads||pg_catalog.jsonb_build_array(
      pg_catalog.to_jsonb((pg_catalog.md5('head:'||p_tag||':'||v_i::text))::uuid::text));
  end loop;
  v_out:=pg_catalog.jsonb_set(pg_catalog.jsonb_set(pg_catalog.jsonb_set(p_request,
    '{decision_bundle_id}',
    pg_catalog.to_jsonb((pg_catalog.md5('bundle:'||p_tag))::uuid::text)),
    '{decision_id}',
    pg_catalog.to_jsonb((pg_catalog.md5('decision:'||p_tag))::uuid::text)),
    '{head_ids}',v_heads);
  perform pg_temp.mk_bundle(v_out);
  return v_out;
end;
$function$;

-- ---------------------------------------------------------------------------
-- 5. Canonicaliser and request-shape negatives (interface I-3)
-- ---------------------------------------------------------------------------
do $verify_publication_canonicaliser$
declare
  v_request jsonb;
  v_canonical jsonb;
begin
  v_request:=pg_temp.single_root_request(
    'c0000000-0000-4000-8000-0000000000b0',1,'c0000000-0000-4000-8000-0000000000c0',
    'c0000000-0000-4000-8000-0000000000d0',null,'[]'::jsonb,
    pg_catalog.jsonb_build_array(
      pg_temp.component(1,'c1c1c1c1-0000-4000-8000-000000000001','7.5','75.00')));
  v_canonical:=private.weekly_source_publication_request_canonical_v1(v_request,'IMMEDIATE',null);

  -- Exactly the eleven proof/32 section 9 fields, and nothing else.
  perform pg_temp.assert_true(
    (select pg_catalog.array_agg(canonical_key order by canonical_key)
       from pg_catalog.jsonb_object_keys(v_canonical) as canonical_key)
    =array['bundle_revision','candidate_id','decision_bundle_id','decision_id',
           'financial_request','head_ids','member_family_booking_ids','member_root_ids',
           'member_root_versions','pending_bundle_id','publication_mode'],
    'the canonical object must carry exactly the eleven proof/32 section 9 fields');
  -- "Nothing outside that object influences the digest": the control scope can
  -- change freely without moving the digest.
  perform pg_temp.assert_true(
    private.weekly_source_publication_request_digest_v1(v_canonical)
    =private.weekly_source_publication_request_digest_v1(
      private.weekly_source_publication_request_canonical_v1(
        v_request||pg_catalog.jsonb_build_object('control',
          (v_request->'control')||pg_catalog.jsonb_build_object('reason','SOMETHING_ELSE')),
        'IMMEDIATE',null)),
    'a control-scope change must not move the request digest');
  -- publication_mode and pending_bundle_id ARE digest fields.
  perform pg_temp.assert_true(
    private.weekly_source_publication_request_digest_v1(v_canonical)
    <>private.weekly_source_publication_request_digest_v1(
      private.weekly_source_publication_request_canonical_v1(
        v_request,'DEFERRED','c0000000-0000-4000-8000-0000000000e0'::uuid)),
    'the deferred digest must differ from the immediate digest');
  -- Decimal normalisation: "7.5" and "7.500000" are the same money.
  perform pg_temp.assert_true(
    v_canonical->'financial_request'->'member_entitlements'->0->'components'->0->>'hours_day'
    ='7.500000',
    'hours are normalised to scale 6');
  perform pg_temp.assert_true(
    v_canonical->'financial_request'->'member_entitlements'->0->'components'->0->>'pay_ex_vat'
    ='75.00',
    'money is normalised to scale 2');

  -- A DEFERRED canonical object must carry its pending bundle id.
  perform pg_temp.expect_failure(
    $sql$select private.weekly_source_publication_request_canonical_v1(
      pg_temp.single_root_request('c0000000-0000-4000-8000-0000000000b0',1,
        'c0000000-0000-4000-8000-0000000000c0','c0000000-0000-4000-8000-0000000000d0',
        null,'[]'::jsonb,'[]'::jsonb),'DEFERRED',null);$sql$,
    '22023','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
    'a DEFERRED canonical object without a pending bundle id');

  -- An unknown key inside the digest scope is refused, never silently dropped.
  perform pg_temp.expect_failure(
    $sql$select private.weekly_source_publication_request_canonical_v1(
      pg_catalog.jsonb_set(
        pg_temp.single_root_request('c0000000-0000-4000-8000-0000000000b0',1,
          'c0000000-0000-4000-8000-0000000000c0','c0000000-0000-4000-8000-0000000000d0',
          null,'[]'::jsonb,'[]'::jsonb),
        '{financial_request,source_revision,surprise}','"x"'::jsonb,true),
      'IMMEDIATE',null);$sql$,
    '22023','WEEKLY_SOURCE_PUBLICATION_REQUEST_UNKNOWN_FIELD',
    'an unknown key inside the digest scope');

  -- A component missing one allowlisted field is refused, not defaulted.
  perform pg_temp.expect_failure(
    $sql$select private.weekly_source_publication_component_canonical_v1(
      pg_temp.component(1,'c1c1c1c1-0000-4000-8000-000000000001','7.5','75.00')-'origin','c');$sql$,
    '22023','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
    'a component missing a required field');

  -- Certified zero is an explicit head and exists only when nothing remains.
  perform pg_temp.expect_failure(
    $sql$select private.weekly_source_publication_request_canonical_v1(
      pg_catalog.jsonb_set(
        pg_temp.single_root_request('c0000000-0000-4000-8000-0000000000b0',1,
          'c0000000-0000-4000-8000-0000000000c0','c0000000-0000-4000-8000-0000000000d0',
          null,'[]'::jsonb,
          pg_catalog.jsonb_build_array(
            pg_temp.component(1,'c1c1c1c1-0000-4000-8000-000000000001','7.5','75.00'))),
        '{financial_request,member_entitlements,0,certified_zero}','true'::jsonb),
      'IMMEDIATE',null);$sql$,
    '22023','WEEKLY_SOURCE_PUBLICATION_CERTIFIED_ZERO_INVALID',
    'certified zero declared with a component remaining');

  -- A bundle larger than the approved bounded A/B case is refused.
  perform pg_temp.expect_failure(
    $sql$select private.weekly_source_publication_request_canonical_v1(
      pg_catalog.jsonb_build_object(
        'decision_bundle_id','c0000000-0000-4000-8000-0000000000b0',
        'pending_bundle_id',null,'bundle_revision',1,
        'candidate_id','c0000000-0000-4000-8000-000000000003',
        'member_root_ids',pg_catalog.jsonb_build_array(
          'c0000000-0000-4000-8000-000000000006','c0000000-0000-4000-8000-000000000016',
          'c0000000-0000-4000-8000-000000000026'),
        'member_family_booking_ids',pg_catalog.jsonb_build_array('a','b','c'),
        'member_root_versions',pg_catalog.jsonb_build_array(1,1,1),
        'head_ids',pg_catalog.jsonb_build_array(
          'c0000000-0000-4000-8000-0000000000c0','c0000000-0000-4000-8000-0000000000c2',
          'c0000000-0000-4000-8000-0000000000c3'),
        'decision_id','c0000000-0000-4000-8000-0000000000d0',
        'publication_mode','IMMEDIATE',
        'financial_request','{}'::jsonb,'control','{}'::jsonb),
      'IMMEDIATE',null);$sql$,
    '22023','WEEKLY_SOURCE_PUBLICATION_BUNDLE_UNBOUNDED',
    'a three-root bundle');

  -- Misaligned arrays are refused before anything is written.
  perform pg_temp.expect_failure(
    $sql$select private.weekly_source_publication_request_canonical_v1(
      pg_catalog.jsonb_set(
        pg_temp.single_root_request('c0000000-0000-4000-8000-0000000000b0',1,
          'c0000000-0000-4000-8000-0000000000c0','c0000000-0000-4000-8000-0000000000d0',
          null,'[]'::jsonb,'[]'::jsonb),
        '{member_root_versions}','[1,2]'::jsonb),
      'IMMEDIATE',null);$sql$,
    '22023','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
    'misaligned member arrays');
end
$verify_publication_canonicaliser$;

-- ---------------------------------------------------------------------------
-- 6. The happy path, and R25's single-root half
-- ---------------------------------------------------------------------------
-- 26 Gate 5 steps 7 to 10, proof/32 section 8, H2-036: one token, one
-- complete-scope job for the Candidate, one receipt, one head per member.
do $verify_publication_happy_path$
declare
  v_request jsonb;
  v_result jsonb;
  v_jobs_before uuid[];
  v_new_jobs uuid[];
  v_token uuid;
begin
  v_request:=pg_temp.single_root_request(
    'c0000000-0000-4000-8000-0000000000b1',1,'c0000000-0000-4000-8000-0000000000c1',
    'c0000000-0000-4000-8000-0000000000d1',null,'[]'::jsonb,
    pg_catalog.jsonb_build_array(
      pg_temp.component(1,'c1c1c1c1-0000-4000-8000-000000000001','7.5','75.00'),
      pg_temp.component(2,'c1c1c1c1-0000-4000-8000-000000000002','2.25','22.50')));
  perform pg_temp.mk_bundle(v_request);

  select coalesce(pg_catalog.array_agg(job_row.id),array[]::uuid[]) into v_jobs_before
    from public.banking_pay_workbench_jobs as job_row;

  v_result:=pg_temp.publish(v_request,pg_temp.lock_result_a());

  perform pg_temp.assert_true((v_result->>'ok')::boolean,
    'the happy path must publish: '||v_result::text);
  perform pg_temp.assert_true((v_result->>'replayed')::boolean is false,
    'the first publication is not a replay');
  v_token:=(v_result->>'scope_change_tx_token')::uuid;

  -- Exactly one head, committed current, carrying its receipt digest and token.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_heads)=1,
    'exactly one head row');
  perform pg_temp.assert_true(
    (select head_row.state='COMMITTED_CURRENT'
        and head_row.head_revision=1
        and head_row.prior_head_id is null
        and head_row.component_count=2
        and head_row.certified_zero is false
        and head_row.scope_change_tx_token=v_token
        and head_row.publication_receipt_digest is not null
        and head_row.root_timesheet_id='c0000000-0000-4000-8000-000000000006'
        and head_row.root_family_booking_id='WSPUB-0001'
        and head_row.decision_bundle_id='c0000000-0000-4000-8000-0000000000b1'
       from public.weekly_source_entitlement_heads as head_row)
    ,'the activated head carries its identity, its receipt digest and the one token');
  -- WP-01a review U2: every component of a post-decision head carries the
  -- bundle identity, so the H2-024 unique index actually binds.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_head_components
      where decision_bundle_id='c0000000-0000-4000-8000-0000000000b1' and bundle_revision=1)=2,
    'every component row carries (decision_bundle_id, bundle_revision)');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_head_components
      where component_sha256 is null)=0,
    'every component row carries its own content hash');

  -- Exactly one receipt, and it is the coordinator's own relation.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*)
       from private.weekly_source_entitlement_publication_receipts)=1,
    'exactly one receipt');
  perform pg_temp.assert_true(
    (select receipt_row.publication_mode='IMMEDIATE'
        and receipt_row.pending_bundle_id is null
        and receipt_row.released_by_worker_id is null
        and receipt_row.released_by_worker_run_id is null
        and receipt_row.scope_change_tx_token=v_token
        and receipt_row.census_json='{}'::jsonb
        and receipt_row.proof_json='{}'::jsonb
       from private.weekly_source_entitlement_publication_receipts as receipt_row),
    'an IMMEDIATE receipt carries no Worker fields and empty census/proof objects');

  -- Decision D8 / proof/34 section 4: the head-publication coordinator updates
  -- current_entitlement_head_id on the live ROOT AUTHORISATION generation, and
  -- nothing else.
  perform pg_temp.assert_true(
    (select authorisation_row.current_entitlement_head_id='c0000000-0000-4000-8000-0000000000c1'
       from public.weekly_source_root_authorisations as authorisation_row
      where authorisation_row.root_timesheet_id='c0000000-0000-4000-8000-000000000006'),
    'the live root authorisation points at the new head');
  perform pg_temp.assert_true(
    (select authorisation_row.current_entitlement_head_id is null
       from public.weekly_source_root_authorisations as authorisation_row
      where authorisation_row.root_timesheet_id='c0000000-0000-4000-8000-000000000016'),
    'an unrelated root authorisation is untouched');
  perform pg_temp.assert_true(
    (v_result->>'root_authorisations_pointed')::integer=1,
    'exactly one live root authorisation moved per published root');
  -- U1: the accepted decision's identity and digests were verified, and the
  -- before-position came from the committed authority, not from the caller.
  -- The root has no committed head yet, so the before-position comes from
  -- interface I-7 when WP-06 has delivered it and is otherwise recorded as
  -- UNPROVED - never silently as if it had been proved (review finding U3(b)).
  perform pg_temp.assert_true(
    v_result->'before_position_source'=case
      when pg_catalog.to_regprocedure('private.weekly_source_effective_inventory_v1(uuid)') is null
        then pg_catalog.jsonb_build_array('DECLARED_UNPROVED')
      else pg_catalog.jsonb_build_array('I7') end,
    'a head-less single-root publication records I7 when interface I-7 exists and '
      ||'DECLARED_UNPROVED when it does not, never anything else: '
      ||coalesce((v_result->'before_position_source')::text,'<null>'));

  -- H2-036 / R25, single-root half: ONE scope-change token in the transaction,
  -- that token on the member root's scope-state row, and exactly one
  -- complete-scope WORKBENCH_CANDIDATE_DIRTY_APPLY job for the Candidate.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.banking_pay_scope_change_transactions)=1,
    'R25(1): exactly one scope-change transaction token');
  perform pg_temp.assert_true(
    (select scope_row.last_scope_change_tx_token=v_token
        and scope_row.economic_state='DIRTY'
        and scope_row.last_dirty_reason='WEEKLY_SOURCE_ENTITLEMENT_HEAD_PUBLICATION'
       from private.banking_pay_workbench_timesheet_scope_state as scope_row
      where scope_row.timesheet_id='c0000000-0000-4000-8000-000000000006'),
    'R25(2): the member root scope-state row carries the one token before the receipt commits');

  -- The invalidator was called once and enqueued once for the one Candidate.
  perform pg_temp.assert_true(
    (v_result->'invalidation'->>'candidate_count')::integer=1
    and coalesce((v_result->'invalidation'->>'job_inserted_count')::integer,0)
       +coalesce((v_result->'invalidation'->>'job_coalesced_count')::integer,0)=1,
    'R25: exactly one Candidate enqueue from the one invalidation: '
      ||(v_result->'invalidation')::text);
  select coalesce(pg_catalog.array_agg(job_row.id),array[]::uuid[]) into v_new_jobs
    from public.banking_pay_workbench_jobs as job_row
   where not (job_row.id=any(v_jobs_before));
  -- HANDOVER 2 round-7 ruling A7 rejects the raw-row-count form of this bound
  -- too: "The raw persisted row count is not fixed at one."
  --
  -- Measured on the build from empty banking_modal_v2_release4301_20260918:
  -- `v_new_jobs` is EMPTY at this point, because the publication coalesces onto
  -- the ordinary job the fixture's own timesheet insert already queued for this
  -- root, so the shipped `cardinality(v_new_jobs)<=1` passed as 0<=1 and
  -- measured nothing at all.  The property is ONE EFFECTIVE complete-scope
  -- outcome for the Candidate, whichever persisted row ends up carrying it.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*)
       from public.banking_pay_workbench_jobs as job_row
      where job_row.candidate_id='c0000000-0000-4000-8000-000000000003'
        and job_row.job_type='WORKBENCH_CANDIDATE_DIRTY_APPLY'
        and job_row.status in ('QUEUED','RUNNING')
        and private.weekly_source_uuid_set_equals_v1(
              (select coalesce(pg_catalog.array_agg(distinct target.value::uuid),array[]::uuid[])
                 from pg_catalog.jsonb_array_elements_text(
                        coalesce(job_row.payload_json->'targeted_timesheet_ids','[]'::jsonb))
                      as target(value)),
              array['c0000000-0000-4000-8000-000000000006']::uuid[]))=1,
    'A7/R25: the single-root publication coalesces to exactly ONE effective '
      ||'complete-scope outcome, whatever the raw persisted row count');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*)
       from public.banking_pay_workbench_jobs as job_row
      where job_row.candidate_id='c0000000-0000-4000-8000-000000000003'
        and coalesce(job_row.payload_json->>'reason','') not like 'DIRTY_TRIGGER:%'
        and coalesce(job_row.payload_json->>'reason','')
            not in ('WEEKLY_SOURCE_ENTITLEMENT_HEAD_PUBLICATION',
                    'PAY_BATCH_ITEMS_INSERT'))=0,
    'A7/R25: every job persisted for the Candidate carries a permitted '
      ||'registered reason');
  -- Exactly one COMPLETE-SCOPE job for the Candidate, carrying the one token.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*)
       from public.banking_pay_workbench_jobs as job_row
      where job_row.job_type='WORKBENCH_CANDIDATE_DIRTY_APPLY'
        and job_row.candidate_id='c0000000-0000-4000-8000-000000000003'
        and job_row.status='QUEUED'
        and job_row.payload_json->'targeted_timesheet_ids'
            =pg_catalog.jsonb_build_array('c0000000-0000-4000-8000-000000000006'))=1,
    'R25: exactly one complete-scope WORKBENCH_CANDIDATE_DIRTY_APPLY job for the Candidate');
  -- R25(1): every job queued for that Candidate carries the same token.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*)
       from public.banking_pay_workbench_jobs as job_row
      where job_row.candidate_id='c0000000-0000-4000-8000-000000000003'
        and job_row.scope_change_tx_token is distinct from v_token)=0,
    'R25(1): every Candidate job in the transaction carries the one token');
  -- R25(5): a job the publication itself put on the queue never targets a root
  -- outside the bundle.  (Jobs the FIXTURE's own timesheet inserts queued for
  -- unrelated roots of the same Candidate are test scaffolding, not part of the
  -- coordinator's behaviour, so they are excluded by the before/after set.)
  perform pg_temp.assert_true(
    (select pg_catalog.count(*)
       from public.banking_pay_workbench_jobs as job_row
      cross join lateral pg_catalog.jsonb_array_elements_text(
        coalesce(job_row.payload_json->'targeted_timesheet_ids','[]'::jsonb)) as target(value)
      where job_row.id=any(v_new_jobs)
        and target.value not in ('c0000000-0000-4000-8000-000000000006'))=0,
    'R25(5): no job the publication queued targets a root outside the bundle');
  -- R25(3): no job is visible before commit.  Inside this transaction the
  -- commit generation has not been allocated yet.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.banking_pay_workbench_jobs as job_row
      where job_row.candidate_id='c0000000-0000-4000-8000-000000000003'
        and job_row.scope_change_generation is not null)=0,
    'R25(3): no Candidate job has a commit generation before commit');

  -- 24 section 4.5 step 7: the decision bundle moves to COMMITTED with the
  -- publication, and nothing else on it changes.
  perform pg_temp.assert_true(
    (select bundle_row.state='COMMITTED' and bundle_row.committed_at_utc is not null
       from public.weekly_source_entitlement_decision_bundles as bundle_row
      where bundle_row.decision_bundle_id='c0000000-0000-4000-8000-0000000000b1'),
    'the decision bundle is marked committed by the publication');
end
$verify_publication_happy_path$;

-- ---------------------------------------------------------------------------
-- 6a. WP-01b's commit-time asserts, fired without committing
-- ---------------------------------------------------------------------------
-- WP-01b added three DEFERRABLE INITIALLY DEFERRED constraint triggers that run
-- at commit: component_count must equal the real component rows, certified_zero
-- must equal component_count = 0, and every committed head must have its
-- receipt with a matching digest, bundle, token and head-id membership
-- (WEEKLY_SOURCE_HEAD_INVENTORY_MISMATCH / WEEKLY_SOURCE_HEAD_RECEIPT_MISSING).
-- A rollback-only file never commits, so they would otherwise never fire here.
-- SET CONSTRAINTS ALL IMMEDIATE forces them now; any violation raises and fails
-- this verifier.  They are put back to deferred so the later sections can stage
-- a head before its components again.
set constraints weekly_source_entitlement_head_inventory_assert,
                weekly_source_entitlement_head_receipt_assert,
                weekly_source_entitlement_head_component_inventory_assert immediate;
set constraints weekly_source_entitlement_head_inventory_assert,
                weekly_source_entitlement_head_receipt_assert,
                weekly_source_entitlement_head_component_inventory_assert deferred;
do $verify_publication_deferred_asserts$
begin
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from pg_catalog.pg_trigger as trigger_row
      where not trigger_row.tgisinternal
        and trigger_row.tgdeferrable
        and trigger_row.tgname in (
          'weekly_source_entitlement_head_inventory_assert',
          'weekly_source_entitlement_head_receipt_assert',
          'weekly_source_entitlement_head_component_inventory_assert'))=3,
    'WP-01b''s three deferred commit-time asserts must exist and be deferrable');
end
$verify_publication_deferred_asserts$;

-- ---------------------------------------------------------------------------
-- 7. R9 exact replay, R10 conflicting replay
-- ---------------------------------------------------------------------------
do $verify_publication_replay$
declare
  v_request jsonb;
  v_first jsonb;
  v_replay jsonb;
  v_jobs_before uuid[];
  v_heads_before bigint;
  v_receipts_before bigint;
begin
  v_request:=pg_temp.single_root_request(
    'c0000000-0000-4000-8000-0000000000b1',1,'c0000000-0000-4000-8000-0000000000c1',
    'c0000000-0000-4000-8000-0000000000d1',null,'[]'::jsonb,
    pg_catalog.jsonb_build_array(
      pg_temp.component(1,'c1c1c1c1-0000-4000-8000-000000000001','7.5','75.00'),
      pg_temp.component(2,'c1c1c1c1-0000-4000-8000-000000000002','2.25','22.50')));

  select coalesce(pg_catalog.array_agg(job_row.id),array[]::uuid[]) into v_jobs_before
    from public.banking_pay_workbench_jobs as job_row;
  select pg_catalog.count(*) into v_heads_before from public.weekly_source_entitlement_heads;
  select pg_catalog.count(*) into v_receipts_before
    from private.weekly_source_entitlement_publication_receipts;

  -- R9: an exact replay returns the SAME receipt, with no second invalidation
  -- and no second generation.
  v_replay:=pg_temp.publish(v_request,pg_temp.lock_result_a());
  perform pg_temp.assert_true((v_replay->>'ok')::boolean,'R9: replay must succeed');
  perform pg_temp.assert_true((v_replay->>'replayed')::boolean,
    'R9: the replay must be reported as a replay');
  perform pg_temp.assert_true(
    (v_replay->'receipt'->>'id')::uuid
    =(select receipt_row.id from private.weekly_source_entitlement_publication_receipts as receipt_row),
    'R9: the replay returns the already committed receipt');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from private.weekly_source_entitlement_publication_receipts)
    =v_receipts_before,
    'R9: no second receipt');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_heads)=v_heads_before,
    'R9: no second head');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.banking_pay_workbench_jobs
      where not (id=any(v_jobs_before)))=0,
    'R9: no second invalidation job');

  -- R10: a conflicting replay refuses and changes nothing.  The digest covers
  -- every immutable receipt field, so the conflict is produced here by
  -- tampering with the stored receipt rather than the request - proving that
  -- the field-by-field comparison, and not the digest alone, is what decides.
  set local session_replication_role='replica';
  update private.weekly_source_entitlement_publication_receipts
     set member_root_versions=array[99]::integer[];
  set local session_replication_role='origin';

  v_replay:=pg_temp.publish(v_request,pg_temp.lock_result_a());
  perform pg_temp.assert_true((v_replay->>'ok')::boolean is false,
    'R10: a conflicting replay must refuse');
  perform pg_temp.assert_true(
    v_replay->>'code'='WEEKLY_SOURCE_PUBLICATION_REPLAY_CONFLICT',
    'R10: the refusal code is WEEKLY_SOURCE_PUBLICATION_REPLAY_CONFLICT, got '
      ||coalesce(v_replay->>'code','<null>'));
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from private.weekly_source_entitlement_publication_receipts)
    =v_receipts_before
    and (select pg_catalog.count(*) from public.weekly_source_entitlement_heads)=v_heads_before
    and (select pg_catalog.count(*) from public.banking_pay_workbench_jobs
          where not (id=any(v_jobs_before)))=0,
    'R10: nothing changed');

  set local session_replication_role='replica';
  update private.weekly_source_entitlement_publication_receipts
     set member_root_versions=array[1]::integer[];
  set local session_replication_role='origin';
end
$verify_publication_replay$;

-- ---------------------------------------------------------------------------
-- 8. R7 stale source revision; R8 newer head activated meanwhile
-- ---------------------------------------------------------------------------
do $verify_publication_stale$
declare
  v_request jsonb;
  v_accepted jsonb;
  v_result jsonb;
  v_heads_before bigint;
begin
  select pg_catalog.count(*) into v_heads_before from public.weekly_source_entitlement_heads;

  -- R7: a head built from a superseded final revision must never become
  -- current (24 section 4.5 step 2, "revalidate the current source revision").
  -- The accepted decision is built from this very request, so only the source
  -- revision is wrong and only R7's check can fire.
  v_request:=pg_temp.single_root_request(
    'c0000000-0000-4000-8000-0000000000b2',1,'c0000000-0000-4000-8000-0000000000c2',
    'c0000000-0000-4000-8000-0000000000d2','c0000000-0000-4000-8000-0000000000c1',
    pg_catalog.jsonb_build_array('c1c1c1c1-0000-4000-8000-000000000001',
                                 'c1c1c1c1-0000-4000-8000-000000000002'),
    pg_catalog.jsonb_build_array(
      pg_temp.component(1,'c1c1c1c1-0000-4000-8000-000000000001','7.5','75.00')),
    p_revision_id=>'c0000000-0000-4000-8000-0000000000fb',
    p_manifest=>pg_catalog.repeat('c3',32),p_policy=>pg_catalog.repeat('c4',32),
    p_revision_number=>2);
  perform pg_temp.mk_bundle(v_request);
  v_result:=pg_temp.publish(v_request,pg_temp.lock_result_a());
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->>'code'='WEEKLY_SOURCE_PUBLICATION_SOURCE_REVISION_STALE',
    'R7: a superseded source revision must refuse, got '||v_result::text);
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_heads)=v_heads_before,
    'R7: no write');

  -- R8: the compare-and-swap.  The caller believes there is no current head,
  -- but one was activated meanwhile.
  v_request:=pg_temp.single_root_request(
    'c0000000-0000-4000-8000-00000000b200',1,'c0000000-0000-4000-8000-00000000c200',
    'c0000000-0000-4000-8000-00000000d200',null,'[]'::jsonb,
    pg_catalog.jsonb_build_array(
      pg_temp.component(1,'c1c1c1c1-0000-4000-8000-000000000001','7.5','75.00')));
  perform pg_temp.mk_bundle(v_request);
  v_result:=pg_temp.publish(v_request,pg_temp.lock_result_a());
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->>'code'='WEEKLY_SOURCE_PUBLICATION_HEAD_CAS_CONFLICT',
    'R8: a stale expected current head must refuse, got '||v_result::text);
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_heads)=v_heads_before,
    'R8: no write');

  -- The declared before-position must equal the committed head's own
  -- components (H2-024, "read from the single committed effective authority").
  v_request:=pg_temp.single_root_request(
    'c0000000-0000-4000-8000-00000000b210',1,'c0000000-0000-4000-8000-00000000c210',
    'c0000000-0000-4000-8000-00000000d210','c0000000-0000-4000-8000-0000000000c1',
    pg_catalog.jsonb_build_array('c1c1c1c1-0000-4000-8000-000000000001'),
    pg_catalog.jsonb_build_array(
      pg_temp.component(1,'c1c1c1c1-0000-4000-8000-000000000001','7.5','75.00')));
  perform pg_temp.mk_bundle(v_request);
  v_result:=pg_temp.publish(v_request,pg_temp.lock_result_a());
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->>'code'='WEEKLY_SOURCE_PUBLICATION_BEFORE_POSITION_MISMATCH',
    'a before-position that disagrees with the committed head must refuse, got '||v_result::text);

  -- ======================= review finding U1 ==============================
  -- The accepted decision must BIND what is published.  The reviewer's executed
  -- attack: a request quoting the right bundle id, decision id and head ids but
  -- naming a different root, Contract, week and amount of money was published
  -- under the Office decision's identity and actor.  The accepted bundle below
  -- names root …0006 / WSPUB-0001 / Contract …0004; each attack changes exactly
  -- one identity and must be refused with no write.
  v_accepted:=pg_temp.single_root_request(
    'c0000000-0000-4000-8000-00000000b220',1,'c0000000-0000-4000-8000-00000000c220',
    'c0000000-0000-4000-8000-00000000d220','c0000000-0000-4000-8000-0000000000c1',
    pg_catalog.jsonb_build_array('c1c1c1c1-0000-4000-8000-000000000001',
                                 'c1c1c1c1-0000-4000-8000-000000000002'),
    pg_catalog.jsonb_build_array(
      pg_temp.component(1,'c1c1c1c1-0000-4000-8000-000000000001','7.5','75.00')));
  perform pg_temp.mk_bundle(v_accepted);

  -- U1-a: a DIFFERENT ROOT under the accepted decision's identity.
  v_result:=pg_temp.publish(
    pg_catalog.jsonb_set(pg_catalog.jsonb_set(v_accepted,
      '{member_root_ids}','["c0000000-0000-4000-8000-000000000016"]'::jsonb),
      '{member_family_booking_ids}','["WSPUB-0002"]'::jsonb),
    pg_temp.lock_result(
      pg_catalog.jsonb_build_array('c0000000-0000-4000-8000-000000000016'),
      pg_catalog.jsonb_build_array('WSPUB-0002'),
      pg_catalog.jsonb_build_array(1)));
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->>'code'='WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'
    and v_result->'detail'->>'reason'='SOURCE_ROOT_DISAGREES_WITH_ACCEPTED_DECISION',
    'U1-a: a different root under the accepted decision must refuse, got '||v_result::text);

  -- U1-b: a DIFFERENT CONTRACT.
  v_result:=pg_temp.publish(
    pg_catalog.jsonb_set(v_accepted,
      '{financial_request,contract_choices,0,contract_id}',
      '"c0000000-0000-4000-8000-000000000014"'::jsonb),
    pg_temp.lock_result_a());
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->'detail'->>'reason'='SOURCE_ROOT_DISAGREES_WITH_ACCEPTED_DECISION',
    'U1-b: a different Contract under the accepted decision must refuse, got '||v_result::text);

  -- U1-c: a DIFFERENT WEEK.
  v_result:=pg_temp.publish(
    pg_catalog.jsonb_set(v_accepted,
      '{financial_request,contract_choices,0,week_ending_date}','"2031-01-05"'::jsonb),
    pg_temp.lock_result_a());
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->'detail'->>'reason'='WEEK_OR_CONTRACT_DISAGREES_WITH_ACCEPTED_DECISION',
    'U1-c: a different week under the accepted decision must refuse, got '||v_result::text);

  -- U1-d: the SAME identities but DIFFERENT MONEY - caught by the approval
  -- digests, which are recomputed here with the one canonical encoder.
  v_result:=pg_temp.publish(
    pg_catalog.jsonb_set(v_accepted,
      '{financial_request,member_entitlements,0,components,0,pay_ex_vat}','"9999.00"'::jsonb),
    pg_temp.lock_result_a());
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->'detail'->>'reason'='APPROVAL_DIGESTS_DISAGREE_WITH_ACCEPTED_DECISION'
    and (v_result->'detail'->>'request_digest_matches')::boolean is false,
    'U1-d: different money under the accepted decision must refuse on the digests, got '
      ||v_result::text);

  -- U1-e: a different SOURCE REVISION, caught by source_revision_digest.
  v_result:=pg_temp.publish(
    pg_catalog.jsonb_set(v_accepted,
      '{financial_request,source_revision,revision_number}','2'::jsonb),
    pg_temp.lock_result_a());
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->'detail'->>'reason'='APPROVAL_DIGESTS_DISAGREE_WITH_ACCEPTED_DECISION'
    and (v_result->'detail'->>'source_revision_digest_matches')::boolean is false,
    'U1-e: a different source revision must refuse on the digests, got '||v_result::text);

  -- U1-f: a different DECLARED BEFORE-POSITION, caught by before_inventory_digest.
  v_result:=pg_temp.publish(
    pg_catalog.jsonb_set(v_accepted,
      '{control,before_positions,0,component_ids}',
      pg_catalog.jsonb_build_array('c1c1c1c1-0000-4000-8000-000000000001'))
    ,pg_temp.lock_result_a());
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->'detail'->>'reason'='APPROVAL_DIGESTS_DISAGREE_WITH_ACCEPTED_DECISION'
    and (v_result->'detail'->>'before_inventory_digest_matches')::boolean is false,
    'U1-f: a different declared before-position must refuse on the digests, got '||v_result::text);

  -- U1-g: the accepted decision is a SINGLE_ROOT bundle, so it may not carry a
  -- target root.  (The positive A/B binding is proved in section 11.)
  perform pg_temp.assert_true(
    (select bundle_row.target_root_timesheet_id is null
       from public.weekly_source_entitlement_decision_bundles as bundle_row
      where bundle_row.decision_bundle_id='c0000000-0000-4000-8000-00000000b220'),
    'U1-g: a SINGLE_ROOT accepted decision carries no target root');
  -- ====================== end review finding U1 ===========================

  -- WP-01a review U1 (schema): the head's family string is free text, so the
  -- coordinator validates it against the root's real booking_id itself.
  v_result:=pg_temp.publish(
    pg_catalog.jsonb_set(v_accepted,
      '{member_family_booking_ids}','["wspub-0001"]'::jsonb),
    pg_temp.lock_result(
      pg_catalog.jsonb_build_array('c0000000-0000-4000-8000-000000000006'),
      pg_catalog.jsonb_build_array('wspub-0001'),
      pg_catalog.jsonb_build_array(1)));
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->>'code' in ('WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE',
                              'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'),
    'a differently cased family string must refuse, got '||v_result::text);

  -- proof/34 section 6: a rotation observed on a later path is an integrity
  -- failure, never a stale rebuild.
  v_result:=pg_temp.publish(
    v_accepted,
    pg_catalog.jsonb_set(pg_temp.lock_result_a(),
      '{families,0,requested_is_canonical}','false'::jsonb));
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->>'code'='WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE',
    'a non-canonical requested root must be an integrity failure, got '||v_result::text);

  -- A lock result that is not GRANTED never reaches a write.
  v_result:=pg_temp.publish(
    v_accepted,
    pg_catalog.jsonb_set(pg_temp.lock_result_a(),'{gate}','"BLOCKED"'::jsonb));
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false,
    'a lock result that is not GRANTED must refuse');

  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_heads)=v_heads_before
    and (select pg_catalog.count(*)
           from private.weekly_source_entitlement_publication_receipts)=1,
    'none of the refusals above wrote anything');
end
$verify_publication_stale$;

-- ---------------------------------------------------------------------------
-- 9. R39 — the receipt's own constraints, and the replay-conflict rule
-- ---------------------------------------------------------------------------
do $verify_publication_receipt_constraints$
declare
  v_template text;
begin
  v_template:=$sql$
    insert into private.weekly_source_entitlement_publication_receipts(
      decision_bundle_id,pending_bundle_id,bundle_revision,request_digest,publication_mode,
      candidate_id,member_root_ids,member_family_booking_ids,member_root_versions,head_ids,
      scope_change_tx_token,decision_id,decided_by_user_id,released_by_worker_id,
      released_by_worker_run_id,census_json,proof_json
    ) values (
      'c0000000-0000-4000-8000-0000000000b9',%s,1,
      pg_catalog.sha256(pg_catalog.convert_to(%L,'UTF8')),%L,
      'c0000000-0000-4000-8000-000000000003',
      %s::uuid[],%s::text[],%s::integer[],%s::uuid[],
      'c0000000-0000-4000-8000-0000000000ee','c0000000-0000-4000-8000-0000000000d9',
      'c0000000-0000-4000-8000-000000000001',%s,%s,'{}'::jsonb,'{}'::jsonb);
  $sql$;

  -- Misaligned arrays.
  perform pg_temp.expect_failure(
    pg_catalog.format(v_template,'null','r39a','IMMEDIATE',
      $$array['c0000000-0000-4000-8000-000000000006']$$,
      $$array['WSPUB-0001','WSPUB-0002']$$,'array[1]',
      $$array['c0000000-0000-4000-8000-0000000000c9']$$,'null','null'),
    '23514','','R39: misaligned member arrays');
  -- A null element.
  perform pg_temp.expect_failure(
    pg_catalog.format(v_template,'null','r39b','IMMEDIATE',
      $$array['c0000000-0000-4000-8000-000000000006',null]$$,
      $$array['WSPUB-0001','WSPUB-0002']$$,'array[1,1]',
      $$array['c0000000-0000-4000-8000-0000000000c9','c0000000-0000-4000-8000-0000000000ca']$$,
      'null','null'),
    '23514','','R39: a null root element');
  -- A duplicate root.
  perform pg_temp.expect_failure(
    pg_catalog.format(v_template,'null','r39c','IMMEDIATE',
      $$array['c0000000-0000-4000-8000-000000000006','c0000000-0000-4000-8000-000000000006']$$,
      $$array['WSPUB-0001','WSPUB-0001']$$,'array[1,1]',
      $$array['c0000000-0000-4000-8000-0000000000c9','c0000000-0000-4000-8000-0000000000ca']$$,
      'null','null'),
    '23514','','R39: a duplicate root id');
  -- A duplicate head id.
  perform pg_temp.expect_failure(
    pg_catalog.format(v_template,'null','r39d','IMMEDIATE',
      $$array['c0000000-0000-4000-8000-000000000006','c0000000-0000-4000-8000-000000000016']$$,
      $$array['WSPUB-0001','WSPUB-0002']$$,'array[1,1]',
      $$array['c0000000-0000-4000-8000-0000000000c9','c0000000-0000-4000-8000-0000000000c9']$$,
      'null','null'),
    '23514','','R39: a duplicate head id');
  -- A DEFERRED receipt without the Worker run id (H2-038).
  perform pg_temp.expect_failure(
    pg_catalog.format(v_template,$$'c0000000-0000-4000-8000-0000000000e9'$$,'r39e','DEFERRED',
      $$array['c0000000-0000-4000-8000-000000000006']$$,
      $$array['WSPUB-0001']$$,'array[1]',
      $$array['c0000000-0000-4000-8000-0000000000c9']$$,$$'worker-1'$$,'null'),
    '23514','','R39: a DEFERRED receipt without released_by_worker_run_id');
  -- An IMMEDIATE receipt carrying Worker fields.
  perform pg_temp.expect_failure(
    pg_catalog.format(v_template,'null','r39f','IMMEDIATE',
      $$array['c0000000-0000-4000-8000-000000000006']$$,
      $$array['WSPUB-0001']$$,'array[1]',
      $$array['c0000000-0000-4000-8000-0000000000c9']$$,$$'worker-1'$$,
      $$'c0000000-0000-4000-8000-0000000000eb'$$),
    '23514','','R39: an IMMEDIATE receipt carrying Worker fields');

  -- The receipt is immutable.
  perform pg_temp.expect_failure(
    $sql$update private.weekly_source_entitlement_publication_receipts
           set census_json='{"tampered":true}'::jsonb;$sql$,
    '55000','WEEKLY_SOURCE_PUBLICATION_RECEIPT_IMMUTABLE','R39: updating a receipt');
  perform pg_temp.expect_failure(
    $sql$delete from private.weekly_source_entitlement_publication_receipts;$sql$,
    '55000','WEEKLY_SOURCE_PUBLICATION_RECEIPT_IMMUTABLE','R39: deleting a receipt');
end
$verify_publication_receipt_constraints$;

-- ---------------------------------------------------------------------------
-- 10. R11 — a forced failure at every internal step leaves nothing behind
-- ---------------------------------------------------------------------------
do $verify_publication_forced_failures$
declare
  v_case record;
  v_request jsonb;
  v_heads_before bigint;
  v_components_before bigint;
  v_receipts_before bigint;
  v_jobs_before bigint;
  v_lineage_before uuid;
  v_tokens_before bigint;
  v_scope_before text;
  v_state text;
  v_message text;
  v_failed boolean;
  v_i6_installed boolean;
begin
  v_i6_installed:=pg_catalog.to_regprocedure(
    'private.weekly_source_first_authorise_core_v1(uuid,text,uuid,jsonb)') is not null;

  for v_case in
    select * from (values
      -- 1. staging: the pre-allocated head id is already taken.
      ('R11 head staging: duplicate head id','HEAD_ID_TAKEN'),
      -- (The earlier "referential failure inside the head insert" case was
      --  removed after review finding U1 was closed: a Contract that disagrees
      --  with the accepted decision is now refused BEFORE any write, which is a
      --  better outcome than a mid-write rollback.  The remaining write-phase
      --  failures are staging, the invalidation, interface I-6 (section 12a)
      --  and the receipt's own uniqueness under the two-session race.)
      -- 2. the single invalidation, refused by the installed Workbench owner's
      --    own ownership rule.
      ('R11 invalidation: the installed invalidator refuses','INVALIDATOR_REFUSES')
    ) as forced(label,kind)
  loop
    select pg_catalog.count(*) into v_heads_before from public.weekly_source_entitlement_heads;
    select pg_catalog.count(*) into v_components_before
      from public.weekly_source_entitlement_head_components;
    select pg_catalog.count(*) into v_receipts_before
      from private.weekly_source_entitlement_publication_receipts;
    select pg_catalog.count(*) into v_jobs_before from public.banking_pay_workbench_jobs;
    select pg_catalog.count(*) into v_tokens_before
      from public.banking_pay_scope_change_transactions;
    select authorisation_row.current_entitlement_head_id into v_lineage_before
      from public.weekly_source_root_authorisations as authorisation_row
     where authorisation_row.root_timesheet_id='c0000000-0000-4000-8000-000000000016';
    select scope_row.last_dirty_reason into v_scope_before
      from private.banking_pay_workbench_timesheet_scope_state as scope_row
     where scope_row.timesheet_id='c0000000-0000-4000-8000-000000000016';

    v_failed:=false;
    begin
      if v_case.kind='HEAD_ID_TAKEN' then
        -- Publish over root B with a head id that already exists.
        v_request:=pg_temp.root_b_request(
          'c0000000-0000-4000-8000-0000000000b3','c0000000-0000-4000-8000-0000000000c1',
          'c0000000-0000-4000-8000-0000000000d3',
          pg_catalog.jsonb_build_array(
            pg_temp.component(1,'c2c2c2c2-0000-4000-8000-000000000001','1.0','10.00')));
        perform pg_temp.mk_bundle(v_request);
        perform private.weekly_source_entitlement_publish_core_v1(
          v_request,'IMMEDIATE',pg_temp.lock_result_b(),
          null,null,null,'{}'::jsonb,'{}'::jsonb);

      elsif v_case.kind='INVALIDATOR_REFUSES' then
        -- A current TSFIN row owned by a DIFFERENT Candidate makes the
        -- installed invalidator raise PAY_WORKBENCH_SCOPE_INVALIDATION_
        -- OWNERSHIP_MISMATCH.  Nothing in Banking Pay is edited: the installed
        -- owner's own rule is used.
        insert into public.timesheets_financials(
          timesheet_id,timesheet_version,candidate_id,is_current)
        values ('c0000000-0000-4000-8000-000000000016',1,
                'c0000000-0000-4000-8000-000000000023',true);
        v_request:=pg_temp.root_b_request(
          'c0000000-0000-4000-8000-0000000000b5','c0000000-0000-4000-8000-0000000000c5',
          'c0000000-0000-4000-8000-0000000000d5',
          pg_catalog.jsonb_build_array(
            pg_temp.component(1,'c3c3c3c3-0000-4000-8000-000000000001','1.0','10.00')));
        perform pg_temp.mk_bundle(v_request);
        perform private.weekly_source_entitlement_publish_core_v1(
          v_request,'IMMEDIATE',pg_temp.lock_result_b(),
          null,null,null,'{}'::jsonb,'{}'::jsonb);
      end if;
    exception when others then
      get stacked diagnostics v_state=returned_sqlstate, v_message=message_text;
      v_failed:=true;
    end;

    perform pg_temp.assert_true(v_failed,
      v_case.label||': the forced failure did not fail');
    perform pg_temp.assert_true(
      (select pg_catalog.count(*) from public.weekly_source_entitlement_heads)=v_heads_before,
      v_case.label||': a head survived the rollback');
    perform pg_temp.assert_true(
      (select pg_catalog.count(*)
         from public.weekly_source_entitlement_head_components)=v_components_before,
      v_case.label||': a component survived the rollback');
    perform pg_temp.assert_true(
      (select pg_catalog.count(*)
         from private.weekly_source_entitlement_publication_receipts)=v_receipts_before,
      v_case.label||': a receipt survived the rollback');
    perform pg_temp.assert_true(
      (select pg_catalog.count(*) from public.banking_pay_workbench_jobs)=v_jobs_before,
      v_case.label||': a dirty job survived the rollback');
    perform pg_temp.assert_true(
      (select pg_catalog.count(*)
         from public.banking_pay_scope_change_transactions)=v_tokens_before,
      v_case.label||': a scope-change token survived the rollback');
    perform pg_temp.assert_true(
      (select authorisation_row.current_entitlement_head_id
         from public.weekly_source_root_authorisations as authorisation_row
        where authorisation_row.root_timesheet_id='c0000000-0000-4000-8000-000000000016')
      is not distinct from v_lineage_before,
      v_case.label||': a root authorisation pointer survived the rollback');
    perform pg_temp.assert_true(
      (select scope_row.last_dirty_reason
         from private.banking_pay_workbench_timesheet_scope_state as scope_row
        where scope_row.timesheet_id='c0000000-0000-4000-8000-000000000016')
      is not distinct from v_scope_before,
      v_case.label||': a scope-state change survived the rollback');
  end loop;

  -- 4. interface I-6 refuses.  Only when the real owner is absent, because the
  --    test double must never replace a delivered owner.
  if not v_i6_installed then
    execute $ddl$
      create function private.weekly_source_first_authorise_core_v1(
        p_timesheet_id uuid,p_expected_row_signature text,p_actor_user_id uuid,
        p_lock_result jsonb
      ) returns jsonb language plpgsql as $double$
      begin
        -- TEST-DOUBLE for interface I-6 (WP-07).  Under decision D8 the real
        -- owner inserts generation 1 of the per-ROOT authorisation record, so
        -- the double does exactly that and the coordinator's contract with I-6
        -- is exercised.  Two deliberate misbehaviours are available:
        --   FORCE-I6-FAILURE    - answers ok:false (R11's fourth forced step);
        --   FORCE-I6-SILENT-OK  - answers ok:TRUE and inserts NOTHING, which is
        --                         the breach handoff N6 warns about and the one
        --                         thing the pointer guard defends against
        --                         (review finding F14: this is why M6 is not an
        --                         equivalent mutant).
        if p_expected_row_signature='FORCE-I6-FAILURE' then
          return pg_catalog.jsonb_build_object('ok',false,'code','TEST_DOUBLE_REFUSAL');
        end if;
        if p_expected_row_signature='FORCE-I6-SILENT-OK' then
          return pg_catalog.jsonb_build_object('ok',true,'generation',1,'inserted',false);
        end if;
        insert into public.weekly_source_root_authorisations(
          root_timesheet_id,family_booking_id,timesheet_version,authorisation_generation,
          authorised_row_signature,authorised_by_user_id
        )
        select p_timesheet_id,timesheet_row.booking_id,timesheet_row.version,1,
               p_expected_row_signature,p_actor_user_id
          from public.timesheets as timesheet_row
         where timesheet_row.timesheet_id=p_timesheet_id;
        return pg_catalog.jsonb_build_object('ok',true,'generation',1);
      end;
      $double$;
    $ddl$;
  end if;
end
$verify_publication_forced_failures$;

-- ---------------------------------------------------------------------------
-- 11. The bounded Contract A to B amendment (24 section 4.5; H2-024)
-- ---------------------------------------------------------------------------
do $verify_publication_cross_contract$
declare
  v_result jsonb;
  v_request jsonb;
  v_variant jsonb;
  v_whole jsonb;
  v_lock jsonb;
  v_heads_before bigint;
  v_receipts_before bigint;
  v_jobs_before uuid[];
  v_new_jobs uuid[];
begin
  -- Root B (WSPUB-0002) is already authorised (its lineage generation is in the
  -- fixture), so this is the "already-authorised B keeps every existing
  -- component" case and no I-6 call is made.
  v_lock:=pg_temp.lock_result(
    pg_catalog.jsonb_build_array('c0000000-0000-4000-8000-000000000006',
                                 'c0000000-0000-4000-8000-000000000016'),
    pg_catalog.jsonb_build_array('WSPUB-0001','WSPUB-0002'),
    pg_catalog.jsonb_build_array(1,1));

  v_request:=pg_catalog.jsonb_build_object(
    'decision_bundle_id','c0000000-0000-4000-8000-0000000000b6','pending_bundle_id',null,
    'bundle_revision',1,'candidate_id','c0000000-0000-4000-8000-000000000003',
    'member_root_ids',pg_catalog.jsonb_build_array(
      'c0000000-0000-4000-8000-000000000006','c0000000-0000-4000-8000-000000000016'),
    'member_family_booking_ids',pg_catalog.jsonb_build_array('WSPUB-0001','WSPUB-0002'),
    'member_root_versions',pg_catalog.jsonb_build_array(1,1),
    'head_ids',pg_catalog.jsonb_build_array(
      'c0000000-0000-4000-8000-0000000000c6','c0000000-0000-4000-8000-0000000000c7'),
    'decision_id','c0000000-0000-4000-8000-0000000000d6','publication_mode','IMMEDIATE',
    'financial_request',pg_catalog.jsonb_build_object(
      'source_revision',pg_catalog.jsonb_build_object(
        'final_revision_id','c0000000-0000-4000-8000-0000000000fa',
        'source_cycle_id','c0000000-0000-4000-8000-0000000000f3','revision_number',1,
        'manifest_hash',pg_catalog.repeat('c1',32),
        'policy_fingerprint',pg_catalog.repeat('c2',32)),
      'contract_choices',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object('root_ordinal',1,
          'contract_id','c0000000-0000-4000-8000-000000000004',
          'week_ending_date','2026-03-08','selection_method','UNCHANGED'),
        pg_catalog.jsonb_build_object('root_ordinal',2,
          'contract_id','c0000000-0000-4000-8000-000000000014',
          'week_ending_date','2026-03-08','selection_method','OFFICE_SELECTED')),
      'member_entitlements',pg_catalog.jsonb_build_array(
        -- A-after keeps component 2 only: component 1 moves to B.
        pg_catalog.jsonb_build_object('root_ordinal',1,
          'authority_kind','LOCKED_FINAL_SOURCE','certified_zero',false,'component_count',1,
          'components',pg_catalog.jsonb_build_array(
            pg_temp.component(1,'c1c1c1c1-0000-4000-8000-000000000002','2.25','22.50'))),
        pg_catalog.jsonb_build_object('root_ordinal',2,
          'authority_kind','LOCKED_FINAL_SOURCE','certified_zero',false,'component_count',1,
          'components',pg_catalog.jsonb_build_array(
            pg_temp.component(1,'c1c1c1c1-0000-4000-8000-000000000001','7.5','75.00',
              'c0000000-0000-4000-8000-0000000000a1'::uuid))))),
    'control',pg_catalog.jsonb_build_object(
      'bundle_kind','CROSS_CONTRACT_A_B','reason','WEEKLY_SOURCE_ENTITLEMENT_PUBLICATION',
      'expected_current_head_ids',pg_catalog.jsonb_build_array(
        'c0000000-0000-4000-8000-0000000000c1',null),
      'before_positions',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object('root_ordinal',1,
          'component_ids',pg_catalog.jsonb_build_array(
            'c1c1c1c1-0000-4000-8000-000000000001','c1c1c1c1-0000-4000-8000-000000000002'),
          'inventory_digest',pg_catalog.repeat('00',32)),
        pg_catalog.jsonb_build_object('root_ordinal',2,
          'component_ids',pg_catalog.jsonb_build_array(),
          'inventory_digest',pg_catalog.repeat('00',32))),
      'moved_component_ids',pg_catalog.jsonb_build_array(
        'c1c1c1c1-0000-4000-8000-000000000001'),
      'target_root_authorisation',null,'whole_root_office_review',null));

  -- The accepted decision is built from this A-to-B request, so its source and
  -- TARGET identities, its week and all four approval digests bind it (U1).
  perform pg_temp.mk_bundle(v_request);

  -- ======================= review finding U3(b) ===========================
  -- Root B has no committed head, so its before-position cannot be read from a
  -- committed authority.  Until interface I-7 exists (decision D9, WP-06) the
  -- A-to-B bundle is REFUSED rather than published on the caller's word - which
  -- is the ordinary first A-to-B correction on any root, because the first
  -- entitlement is always the ordinary financial snapshot (24 section 4.1).
  if pg_catalog.to_regprocedure(
       'private.weekly_source_effective_inventory_v1(uuid)') is null then
    v_result:=private.weekly_source_entitlement_publish_core_v1(
      v_request,'IMMEDIATE',v_lock,null,null,null,'{}'::jsonb,'{}'::jsonb);
    perform pg_temp.assert_true(
      (v_result->>'ok')::boolean is false
      and v_result->>'code'='WEEKLY_SOURCE_PUBLICATION_BEFORE_POSITION_UNPROVABLE'
      and v_result->'detail'->>'required_owner'
          ='private.weekly_source_effective_inventory_v1(uuid)',
      'U3(b): an A-to-B bundle with a head-less member must refuse until I-7 exists, got '
        ||v_result::text);
    perform pg_temp.assert_true(
      (select pg_catalog.count(*) from public.weekly_source_entitlement_heads
        where id in ('c0000000-0000-4000-8000-0000000000c6',
                     'c0000000-0000-4000-8000-0000000000c7'))=0,
      'U3(b): the fail-closed refusal writes nothing');

    -- TEST-DOUBLE for interface I-7 (decision D9, owned by WP-06), so the rest
    -- of H2-024 can still be proved here.  It derives the effective inventory
    -- of a head-less root from the ordinary financial snapshot, which is what
    -- the real owner must do; a root with no current snapshot is empty.
    -- It returns the COMPLETE I-3 section 12 shape - `ok`, `code` and
    -- `component_count` included - because the coordinator reads all three and
    -- treats a missing `ok` as a refusal (WP-02b: the earlier double omitted
    -- them, which made it a double the coordinator would have refused).
    execute $ddl$
      create function private.weekly_source_effective_inventory_v1(
        p_root_timesheet_id uuid
      ) returns jsonb language sql stable as $double$
        select pg_catalog.jsonb_build_object(
          'ok',true,'code',null,
          'authority','TSFIN',
          'head_id',null,
          'components','[]'::jsonb,
          'component_count',0,
          'inventory_digest',pg_catalog.encode(
            pg_catalog.sha256(pg_catalog.convert_to(
              'tsfin:'||p_root_timesheet_id::text,'UTF8')),'hex'))
        from (select p_root_timesheet_id as probe) as probe_row
        left join public.timesheets_financials as financial_row
          on financial_row.timesheet_id=p_root_timesheet_id
         and financial_row.is_current;
      $double$;
    $ddl$;
  end if;
  -- ====================== end review finding U3(b) ========================
  perform pg_temp.assert_true(
    (select bundle_row.target_root_timesheet_id='c0000000-0000-4000-8000-000000000016'
        and bundle_row.target_root_family_booking_id='WSPUB-0002'
        and bundle_row.target_contract_id='c0000000-0000-4000-8000-000000000014'
        and bundle_row.bundle_kind='CROSS_CONTRACT_A_B'
       from public.weekly_source_entitlement_decision_bundles as bundle_row
      where bundle_row.decision_bundle_id='c0000000-0000-4000-8000-0000000000b6'),
    'U1: the accepted A-to-B decision carries the target root identity');

  select pg_catalog.count(*) into v_heads_before from public.weekly_source_entitlement_heads;

  -- U1: the TARGET root may not be swapped either.
  v_result:=private.weekly_source_entitlement_publish_core_v1(
    pg_catalog.jsonb_set(v_request,
      '{financial_request,contract_choices,1,contract_id}',
      '"c0000000-0000-4000-8000-000000000004"'::jsonb),
    'IMMEDIATE',v_lock,null,null,null,'{}'::jsonb,'{}'::jsonb);
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->'detail'->>'reason'='TARGET_ROOT_DISAGREES_WITH_ACCEPTED_DECISION',
    'U1: a different target Contract must refuse, got '||v_result::text);

  -- ======================= review finding U3(a) ===========================
  -- A RETAINED component must be byte-identical, not merely "kept by id".  The
  -- committed head stores component_sha256, so re-pricing is detectable and is
  -- refused.  A-after keeps C2 at 22.50; here Office ACCEPTS a decision that
  -- re-prices it to 2250.00, so the U1 digest binding passes and the retained
  -- component check is the thing under test.
  v_variant:=pg_catalog.jsonb_set(pg_catalog.jsonb_set(pg_catalog.jsonb_set(
    v_request,
    '{decision_bundle_id}','"c0000000-0000-4000-8000-00000000b600"'::jsonb),
    '{head_ids}',pg_catalog.jsonb_build_array(
      'c0000000-0000-4000-8000-00000000c600','c0000000-0000-4000-8000-00000000c601')),
    '{financial_request,member_entitlements,0,components,0,pay_ex_vat}','"2250.00"'::jsonb);
  perform pg_temp.mk_bundle(v_variant);
  v_result:=private.weekly_source_entitlement_publish_core_v1(
    v_variant,'IMMEDIATE',v_lock,null,null,null,'{}'::jsonb,'{}'::jsonb);
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->>'code'='WEEKLY_SOURCE_PUBLICATION_RETAINED_COMPONENT_CHANGED',
    'U3(a): re-pricing a retained component must refuse even when Office accepted it, got '
      ||v_result::text);

  -- An adjustment is never in a head, whatever key it arrives under, and again
  -- not even when the accepted decision carries it.
  v_variant:=pg_catalog.jsonb_set(pg_catalog.jsonb_set(pg_catalog.jsonb_set(
    v_request,
    '{decision_bundle_id}','"c0000000-0000-4000-8000-00000000b601"'::jsonb),
    '{head_ids}',pg_catalog.jsonb_build_array(
      'c0000000-0000-4000-8000-00000000c602','c0000000-0000-4000-8000-00000000c603')),
    '{financial_request,member_entitlements,1,components,0,component_kind}',
    '"TS_PAY_ADJUSTMENT"'::jsonb);
  perform pg_temp.mk_bundle(v_variant);
  v_result:=private.weekly_source_entitlement_publish_core_v1(
    v_variant,'IMMEDIATE',v_lock,null,null,null,'{}'::jsonb,'{}'::jsonb);
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->>'code'='WEEKLY_SOURCE_PUBLICATION_COMPONENT_KIND_FORBIDDEN',
    'U3(a): an adjustment-kind component must refuse, got '||v_result::text);

  v_variant:=pg_catalog.jsonb_set(pg_catalog.jsonb_set(pg_catalog.jsonb_set(
    v_request,
    '{decision_bundle_id}','"c0000000-0000-4000-8000-00000000b602"'::jsonb),
    '{head_ids}',pg_catalog.jsonb_build_array(
      'c0000000-0000-4000-8000-00000000c604','c0000000-0000-4000-8000-00000000c605')),
    '{financial_request,member_entitlements,1,components,0,origin}',
    '"TS_PAY_ADJUSTMENTS"'::jsonb);
  perform pg_temp.mk_bundle(v_variant);
  v_result:=private.weekly_source_entitlement_publish_core_v1(
    v_variant,'IMMEDIATE',v_lock,null,null,null,'{}'::jsonb,'{}'::jsonb);
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->>'code'='WEEKLY_SOURCE_PUBLICATION_COMPONENT_KIND_FORBIDDEN',
    'U3(a): an adjustment ORIGIN must refuse too, got '||v_result::text);
  -- ====================== end review finding U3(a) ========================

  -- The WHOLE-entitlement move over the same two roots: A holds …0001 and …0002
  -- and both of them go to B, so A is left holding nothing.  Since the round-5
  -- ruling (Part E below) this is the only supported shape, so every check that
  -- is about something OTHER than partiality is stated over it; `v_request`,
  -- which is partial, is kept for the checks that run before the scope gate and
  -- for the Part E refusal itself.
  v_whole:=pg_catalog.jsonb_set(pg_catalog.jsonb_set(pg_catalog.jsonb_set(
    v_request,
    '{financial_request,member_entitlements,0}',
    pg_catalog.jsonb_build_object('root_ordinal',1,
      'authority_kind','LOCKED_FINAL_SOURCE','certified_zero',true,'component_count',0,
      'components',pg_catalog.jsonb_build_array())),
    '{financial_request,member_entitlements,1}',
    pg_catalog.jsonb_build_object('root_ordinal',2,
      'authority_kind','LOCKED_FINAL_SOURCE','certified_zero',false,'component_count',2,
      'components',pg_catalog.jsonb_build_array(
        pg_temp.component(1,'c1c1c1c1-0000-4000-8000-000000000001','7.5','75.00',
          'c0000000-0000-4000-8000-0000000000a1'::uuid),
        pg_temp.component(2,'c1c1c1c1-0000-4000-8000-000000000002','2.25','22.50',
          'c0000000-0000-4000-8000-0000000000a2'::uuid)))),
    '{control,moved_component_ids}',
    pg_catalog.jsonb_build_array('c1c1c1c1-0000-4000-8000-000000000001',
                                 'c1c1c1c1-0000-4000-8000-000000000002'));

  -- ================ WP-06c review finding F1 (HIGH) =======================
  -- The retained checks above join a component's AFTER position to the BEFORE
  -- authority of the SAME root.  A MOVED component is in A-before and not
  -- A-after, and in B-after and not B-before, so it was joined on NEITHER side
  -- and its content was never compared with anything.  Executed on the review's
  -- own hand-built request, a moved shift was re-priced from 85.50 to 850.50 and
  -- from 4.25 hours to 40 and the coordinator published it with a receipt.  Here
  -- the accepted decision itself carries the re-priced figure, exactly as in the
  -- retained case above, so the U1 digest binding passes and the moved-component
  -- check is the thing under test.
  v_variant:=pg_catalog.jsonb_set(pg_catalog.jsonb_set(pg_catalog.jsonb_set(
    v_whole,
    '{decision_bundle_id}','"c0000000-0000-4000-8000-00000000b603"'::jsonb),
    '{head_ids}',pg_catalog.jsonb_build_array(
      'c0000000-0000-4000-8000-00000000c606','c0000000-0000-4000-8000-00000000c607')),
    '{financial_request,member_entitlements,1,components,0,pay_ex_vat}','"750.00"'::jsonb);
  perform pg_temp.mk_bundle(v_variant);
  v_result:=private.weekly_source_entitlement_publish_core_v1(
    v_variant,'IMMEDIATE',v_lock,null,null,null,'{}'::jsonb,'{}'::jsonb);
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->>'code'='WEEKLY_SOURCE_PUBLICATION_MOVED_COMPONENT_CHANGED'
    and v_result->'detail'->'component'->>'component_id'
        ='c1c1c1c1-0000-4000-8000-000000000001'
    and v_result->'detail'->'component'->>'source_authority'='HEAD',
    'F1: re-pricing a MOVED component must refuse even when Office accepted it, got '
      ||v_result::text);

  -- The same thing done to the hours rather than the money.
  v_variant:=pg_catalog.jsonb_set(pg_catalog.jsonb_set(pg_catalog.jsonb_set(
    v_whole,
    '{decision_bundle_id}','"c0000000-0000-4000-8000-00000000b604"'::jsonb),
    '{head_ids}',pg_catalog.jsonb_build_array(
      'c0000000-0000-4000-8000-00000000c608','c0000000-0000-4000-8000-00000000c609')),
    '{financial_request,member_entitlements,1,components,0,hours_day}','"40.000000"'::jsonb);
  perform pg_temp.mk_bundle(v_variant);
  v_result:=private.weekly_source_entitlement_publish_core_v1(
    v_variant,'IMMEDIATE',v_lock,null,null,null,'{}'::jsonb,'{}'::jsonb);
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->>'code'='WEEKLY_SOURCE_PUBLICATION_MOVED_COMPONENT_CHANGED',
    'F1: changing a MOVED component''s hours must refuse, got '||v_result::text);

  -- What a move legitimately DOES change must still pass: the destination
  -- ordinal, the movement identity and the movement group are excluded from the
  -- content identity, so a moved component that keeps its money and its hours is
  -- accepted.  Proved by the legal publication at the end of this section, which
  -- carries `component_ordinal` 1 in B against ordinal 1 in A's before-position
  -- and a non-null `movement_id` that A's head row does not have.
  perform pg_temp.assert_true(
    private.weekly_source_publication_component_content_v1(
      pg_temp.component(1,'c1c1c1c1-0000-4000-8000-000000000001','7.5','75.00',
        'c0000000-0000-4000-8000-0000000000a1'::uuid))
    =private.weekly_source_publication_component_content_v1(
      pg_temp.component(3,'c1c1c1c1-0000-4000-8000-000000000001','7.5','75.00')),
    'F1: the content identity must exclude the ordinal and the movement identity');

  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_heads
      where id in ('c0000000-0000-4000-8000-00000000c606',
                   'c0000000-0000-4000-8000-00000000c607',
                   'c0000000-0000-4000-8000-00000000c608',
                   'c0000000-0000-4000-8000-00000000c609'))=0,
    'F1: a moved-component refusal writes nothing');
  -- ================== end WP-06c review finding F1 ========================

  -- A moved component without a movement identity in its destination head.
  -- Stated over the WHOLE move, because the movement-identity proof runs after
  -- the scope gate and a partial request would be refused before reaching it.
  v_result:=private.weekly_source_entitlement_publish_core_v1(
    pg_temp.accept(pg_catalog.jsonb_set(v_whole,
      '{financial_request,member_entitlements,1,components,0,movement_id}','null'::jsonb),
      'h024-movement'),
    'IMMEDIATE',v_lock,null,null,null,'{}'::jsonb,'{}'::jsonb);
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->>'code'='WEEKLY_SOURCE_PUBLICATION_MOVEMENT_IDENTITY_INVALID',
    'H2-024: a moved component must carry movement_id, got '||v_result::text);

  -- A retained component that also appears in B (the same shift in both heads).
  v_result:=private.weekly_source_entitlement_publish_core_v1(
    pg_temp.accept(pg_catalog.jsonb_set(v_request,'{control,moved_component_ids}',
      pg_catalog.jsonb_build_array()),'h024-emptymove'),
    'IMMEDIATE',v_lock,null,null,null,'{}'::jsonb,'{}'::jsonb);
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->>'code'='WEEKLY_SOURCE_PUBLICATION_MOVE_SET_INVALID',
    'H2-024: an empty move set on an A/B bundle must refuse, got '||v_result::text);

  -- A-before must equal A-after union moved.
  v_result:=private.weekly_source_entitlement_publish_core_v1(
    pg_temp.accept(pg_catalog.jsonb_set(v_request,'{control,before_positions,0,component_ids}',
      pg_catalog.jsonb_build_array('c1c1c1c1-0000-4000-8000-000000000001',
                                   'c1c1c1c1-0000-4000-8000-000000000002',
                                   'c1c1c1c1-0000-4000-8000-000000000003')),'h024-before'),
    'IMMEDIATE',v_lock,null,null,null,'{}'::jsonb,'{}'::jsonb);
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false,
    'H2-024: a before-position that is not A-after union moved must refuse');

  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_heads)=v_heads_before,
    'none of the H2-024 refusals wrote anything');
  select pg_catalog.count(*) into v_receipts_before
    from private.weekly_source_entitlement_publication_receipts;

  -- ============ round-5 ruling, Part E: no PARTIAL move ===================
  -- "Not in scope for this release.  The supported operation is the
  --  whole-entitlement move.  A requested partial move must take the named
  --  fail-closed path and explain that partial movement is unsupported; it must
  --  not approximate the move."
  --
  -- `v_request` above IS a partial move: A holds …0001 and …0002, only …0001
  -- moves, and A keeps …0002.  Every H2-024 set proof passes, so the scope gate
  -- is the only thing that can refuse it — which is the point.  Until this
  -- ruling it published, and the assertions that used to stand here described
  -- that outcome; they are replaced by the whole move below.
  v_result:=private.weekly_source_entitlement_publish_core_v1(
    v_request,'IMMEDIATE',v_lock,null,null,null,'{}'::jsonb,'{}'::jsonb);
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->>'code'='WEEKLY_SOURCE_PUBLICATION_PARTIAL_MOVE_UNSUPPORTED'
    and v_result->'detail'->>'reason'
        ='ONLY_A_WHOLE_ENTITLEMENT_MOVE_IS_SUPPORTED_IN_THIS_RELEASE'
    and (v_result->'detail'->>'a_after_component_count')::integer=1
    and (v_result->'detail'->>'moved_is_the_whole_source_position')::boolean is false
    -- a plain-English explanation, not a technical code alone
    and v_result->'detail'->>'message' like 'This bundle moves only part of the entitlement%',
    'Part E: a partial Contract-to-Contract move must be refused and explained, got '
      ||v_result::text);
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_heads)=v_heads_before
    and (select pg_catalog.count(*)
           from private.weekly_source_entitlement_publication_receipts)=v_receipts_before,
    'Part E: the partial-move refusal writes no head and no receipt');
  -- ================== end round-5 ruling, Part E ==========================

  -- The legal A/B publication: the WHOLE entitlement moves, so A is left holding
  -- nothing.  Same two roots, same head ids, same before-positions as the
  -- partial request above; only the move set and the two after-positions differ,
  -- and the accepted decision is a new bundle because the request digest moved
  -- with them.
  v_variant:=pg_catalog.jsonb_set(pg_catalog.jsonb_set(
    v_whole,
    '{decision_bundle_id}','"c0000000-0000-4000-8000-00000000b60a"'::jsonb),
    '{decision_id}','"c0000000-0000-4000-8000-00000000d60a"'::jsonb);
  perform pg_temp.mk_bundle(v_variant);
  select coalesce(pg_catalog.array_agg(job_row.id),array[]::uuid[]) into v_jobs_before
    from public.banking_pay_workbench_jobs as job_row;
  v_result:=private.weekly_source_entitlement_publish_core_v1(
    v_variant,'IMMEDIATE',v_lock,null,null,null,'{}'::jsonb,'{}'::jsonb);
  perform pg_temp.assert_true((v_result->>'ok')::boolean,
    'the whole-entitlement A/B publication must succeed: '||v_result::text);

  -- A keeps nothing and says so explicitly; B-after is B-before plus everything
  -- that moved; the old A head is superseded, not deleted.
  perform pg_temp.assert_true(
    (select head_row.certified_zero and head_row.component_count=0
       from public.weekly_source_entitlement_heads as head_row
      where head_row.id='c0000000-0000-4000-8000-0000000000c6')
    and (select pg_catalog.count(*) from public.weekly_source_entitlement_head_components
          where head_id='c0000000-0000-4000-8000-0000000000c6')=0,
    'a whole move leaves A holding nothing, as an explicit certified-zero head');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*)=2
       from public.weekly_source_entitlement_head_components as component_row
      where component_row.head_id='c0000000-0000-4000-8000-0000000000c7'
        and component_row.movement_id is not null),
    'every moved component carries its movement identity in B-after');
  perform pg_temp.assert_true(
    (select head_row.state='SUPERSEDED'
        and head_row.superseded_by_head_id='c0000000-0000-4000-8000-0000000000c6'
       from public.weekly_source_entitlement_heads as head_row
      where head_row.id='c0000000-0000-4000-8000-0000000000c1'),
    'the previous A head is superseded by the new one, not deleted');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_heads
      where state='COMMITTED_CURRENT')=2,
    'exactly one committed current head per member root');

  -- H2-036: one aligned invalidation for BOTH pairs, one job for the Candidate.
  select coalesce(pg_catalog.array_agg(job_row.id),array[]::uuid[]) into v_new_jobs
    from public.banking_pay_workbench_jobs as job_row
   where not (job_row.id=any(v_jobs_before));
  -- HANDOVER 2 round-7 ruling A7.  "REJECT THE SENTENCE.  …The raw persisted
  -- row count is not fixed at one."  What must hold is the invariant below;
  -- how many rows carry it is not a money property.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*)
       from public.banking_pay_workbench_jobs as job_row
      where job_row.id=any(v_new_jobs)
        and job_row.job_type='WORKBENCH_CANDIDATE_DIRTY_APPLY'
        and job_row.status in ('QUEUED','RUNNING')
        and private.weekly_source_uuid_set_equals_v1(
              (select coalesce(pg_catalog.array_agg(distinct target.value::uuid),array[]::uuid[])
                 from pg_catalog.jsonb_array_elements_text(
                        coalesce(job_row.payload_json->'targeted_timesheet_ids','[]'::jsonb))
                      as target(value)),
              array['c0000000-0000-4000-8000-000000000006',
                    'c0000000-0000-4000-8000-000000000016']::uuid[]))=1,
    'A7: the A/B publication coalesces to exactly ONE effective complete-scope outcome, '
      ||'whatever the raw persisted row count');
  perform pg_temp.assert_true(
    (select pg_catalog.count(distinct job_row.scope_change_tx_token)
       from public.banking_pay_workbench_jobs as job_row
      where job_row.id=any(v_new_jobs))=1
    and (select pg_catalog.count(*)
           from public.banking_pay_workbench_jobs as job_row
          where job_row.id=any(v_new_jobs)
            and job_row.scope_change_generation is not null)=0,
    'A7: every persisted job of the transaction carries the same token and no commit '
      ||'generation before commit');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*)
       from public.banking_pay_workbench_jobs as job_row
      where job_row.id=any(v_new_jobs)
        and coalesce(job_row.payload_json->>'reason','') not like 'DIRTY_TRIGGER:%'
        and coalesce(job_row.payload_json->>'reason','')
            not in ('WEEKLY_SOURCE_ENTITLEMENT_HEAD_PUBLICATION',
                    'PAY_BATCH_ITEMS_INSERT'))=0,
    'A7: every persisted job carries a permitted registered reason');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_heads
      where state='COMMITTED_CURRENT')=2
    and (select pg_catalog.count(*)
           from private.weekly_source_entitlement_publication_receipts
          where request_digest=(select request_digest
                                  from private.weekly_source_entitlement_publication_receipts
                                 where candidate_id='c0000000-0000-4000-8000-000000000003'
                                 order by created_at_utc desc limit 1))=1,
    'A7: no duplicate publication and no duplicate financial effect');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.banking_pay_scope_change_transactions)=1,
    'H2-036: still exactly one scope-change token in the transaction');
end
$verify_publication_cross_contract$;

-- ---------------------------------------------------------------------------
-- 12. The target root's authorisation state (24 section 4.5 step 4)
-- ---------------------------------------------------------------------------
do $verify_publication_target_root$
declare
  v_result jsonb;
  v_request jsonb;
  v_lock jsonb;
  v_heads_before bigint;
begin
  select pg_catalog.count(*) into v_heads_before from public.weekly_source_entitlement_heads;

  -- WSPUB-0003 has no lineage generation at all: a head over it would never
  -- reach payroll, so publishing without authorising it is refused.
  v_request:=pg_catalog.jsonb_set(pg_catalog.jsonb_set(pg_catalog.jsonb_set(
      pg_temp.single_root_request(
        'c0000000-0000-4000-8000-0000000000b7',1,'c0000000-0000-4000-8000-0000000000c8',
        'c0000000-0000-4000-8000-0000000000d7',null,'[]'::jsonb,
        pg_catalog.jsonb_build_array(
          pg_temp.component(1,'c4c4c4c4-0000-4000-8000-000000000001','1.0','10.00'))),
      '{member_root_ids}','["c0000000-0000-4000-8000-000000000026"]'::jsonb),
      '{member_family_booking_ids}','["WSPUB-0003"]'::jsonb),
      '{financial_request,contract_choices,0,contract_id}',
      '"c0000000-0000-4000-8000-000000000004"'::jsonb);
  perform pg_temp.mk_bundle(v_request);
  v_result:=private.weekly_source_entitlement_publish_core_v1(
    v_request,
    'IMMEDIATE',
    pg_temp.lock_result(
      pg_catalog.jsonb_build_array('c0000000-0000-4000-8000-000000000026'),
      pg_catalog.jsonb_build_array('WSPUB-0003'),
      pg_catalog.jsonb_build_array(1)),
    null,null,null,'{}'::jsonb,'{}'::jsonb);
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->>'code'='WEEKLY_SOURCE_PUBLICATION_TARGET_NOT_AUTHORISED',
    'an unauthorised root must refuse a head, got '||v_result::text);

  -- An A/B bundle that asks to authorise a target root which is ALREADY
  -- authorised.  The whole move above left WSPUB-0001 holding nothing and
  -- WSPUB-0002 holding both components, so this bundle moves the entitlement
  -- BACK: member 1 is now WSPUB-0002 (the source) and member 2 is WSPUB-0001
  -- (the target).  WSPUB-0001 is already authorised, which is what makes it the
  -- right target for this test, and moving everything back is again a WHOLE
  -- move, as the round-5 ruling requires.
  v_lock:=pg_temp.lock_result(
    pg_catalog.jsonb_build_array('c0000000-0000-4000-8000-000000000016',
                                 'c0000000-0000-4000-8000-000000000006'),
    pg_catalog.jsonb_build_array('WSPUB-0002','WSPUB-0001'),
    pg_catalog.jsonb_build_array(1,1));
  v_request:=pg_catalog.jsonb_build_object(
    'decision_bundle_id','c0000000-0000-4000-8000-0000000000b8','pending_bundle_id',null,
    'bundle_revision',1,'candidate_id','c0000000-0000-4000-8000-000000000003',
    'member_root_ids',pg_catalog.jsonb_build_array(
      'c0000000-0000-4000-8000-000000000016','c0000000-0000-4000-8000-000000000006'),
    'member_family_booking_ids',pg_catalog.jsonb_build_array('WSPUB-0002','WSPUB-0001'),
    'member_root_versions',pg_catalog.jsonb_build_array(1,1),
    'head_ids',pg_catalog.jsonb_build_array(
      'c0000000-0000-4000-8000-0000000000ca','c0000000-0000-4000-8000-0000000000cb'),
    'decision_id','c0000000-0000-4000-8000-0000000000d8','publication_mode','IMMEDIATE',
    'financial_request',pg_catalog.jsonb_build_object(
      'source_revision',pg_catalog.jsonb_build_object(
        'final_revision_id','c0000000-0000-4000-8000-0000000000fa',
        'source_cycle_id','c0000000-0000-4000-8000-0000000000f3','revision_number',1,
        'manifest_hash',pg_catalog.repeat('c1',32),
        'policy_fingerprint',pg_catalog.repeat('c2',32)),
      'contract_choices',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object('root_ordinal',1,
          'contract_id','c0000000-0000-4000-8000-000000000014',
          'week_ending_date','2026-03-08','selection_method','UNCHANGED'),
        pg_catalog.jsonb_build_object('root_ordinal',2,
          'contract_id','c0000000-0000-4000-8000-000000000004',
          'week_ending_date','2026-03-08','selection_method','OFFICE_SELECTED')),
      'member_entitlements',pg_catalog.jsonb_build_array(
        -- The source keeps NOTHING: that is what makes this a whole move.
        pg_catalog.jsonb_build_object('root_ordinal',1,
          'authority_kind','LOCKED_FINAL_SOURCE','certified_zero',true,'component_count',0,
          'components',pg_catalog.jsonb_build_array()),
        pg_catalog.jsonb_build_object('root_ordinal',2,
          'authority_kind','LOCKED_FINAL_SOURCE','certified_zero',false,'component_count',2,
          -- Both components move back, so both carry a movement identity.
          'components',pg_catalog.jsonb_build_array(
            pg_temp.component(1,'c1c1c1c1-0000-4000-8000-000000000001','7.5','75.00',
              'c0000000-0000-4000-8000-0000000000e1'),
            pg_temp.component(2,'c1c1c1c1-0000-4000-8000-000000000002','2.25','22.50',
              'c0000000-0000-4000-8000-0000000000e2'))))),
    'control',pg_catalog.jsonb_build_object(
      'bundle_kind','CROSS_CONTRACT_A_B','reason','WEEKLY_SOURCE_ENTITLEMENT_PUBLICATION',
      'expected_current_head_ids',pg_catalog.jsonb_build_array(
        'c0000000-0000-4000-8000-0000000000c7','c0000000-0000-4000-8000-0000000000c6'),
      'before_positions',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object('root_ordinal',1,
          'component_ids',pg_catalog.jsonb_build_array(
            'c1c1c1c1-0000-4000-8000-000000000001','c1c1c1c1-0000-4000-8000-000000000002'),
          'inventory_digest',pg_catalog.repeat('00',32)),
        pg_catalog.jsonb_build_object('root_ordinal',2,
          'component_ids',pg_catalog.jsonb_build_array(),
          'inventory_digest',pg_catalog.repeat('00',32))),
      'moved_component_ids',pg_catalog.jsonb_build_array(
        'c1c1c1c1-0000-4000-8000-000000000001','c1c1c1c1-0000-4000-8000-000000000002'),
      'target_root_authorisation',pg_catalog.jsonb_build_object(
        'timesheet_id','c0000000-0000-4000-8000-000000000006',
        'expected_row_signature','signature-a-generation-1',
        'actor_user_id','c0000000-0000-4000-8000-000000000001'),
      'whole_root_office_review',null));
  perform pg_temp.mk_bundle(v_request);

  select pg_catalog.count(*) into v_heads_before from public.weekly_source_entitlement_heads;
  v_result:=private.weekly_source_entitlement_publish_core_v1(
    v_request,'IMMEDIATE',v_lock,null,null,null,'{}'::jsonb,'{}'::jsonb);
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->>'code'='WEEKLY_SOURCE_PUBLICATION_TARGET_ALREADY_AUTHORISED',
    'an already authorised target must never be authorised again, got '||v_result::text);
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_heads)=v_heads_before,
    'the already-authorised refusal writes nothing');
  -- This bundle deliberately stops at the refusal and publishes nothing: the
  -- whole move above already left WSPUB-0001 certified zero and WSPUB-0002
  -- holding both components, which is the state every later section reads, and
  -- moving the entitlement back again would only invert it.  The "source
  -- certified zero, target gains everything" outcome is asserted on that
  -- publication, above.
end
$verify_publication_target_root$;

-- ---------------------------------------------------------------------------
-- 12a. Interface I-6 — the genuinely new B root (24 section 4.5 step 4)
-- ---------------------------------------------------------------------------
-- WSPUB-0003 has no head in any state and no lineage generation at all, so it
-- is "genuinely new, never authorised" and needs no whole-root Office review.
-- The coordinator calls I-6 AFTER staging and BEFORE the single invalidation.
do $verify_publication_new_target_root$
declare
  v_result jsonb;
  v_request jsonb;
  v_lock jsonb;
  v_heads_before bigint;
  v_lineages_before bigint;
  v_jobs_before uuid[];
  v_new_jobs uuid[];
  v_failed boolean;
begin
  v_lock:=pg_temp.lock_result(
    pg_catalog.jsonb_build_array('c0000000-0000-4000-8000-000000000016',
                                 'c0000000-0000-4000-8000-000000000026'),
    pg_catalog.jsonb_build_array('WSPUB-0002','WSPUB-0003'),
    pg_catalog.jsonb_build_array(1,1));
  v_request:=pg_catalog.jsonb_build_object(
    'decision_bundle_id','c0000000-0000-4000-8000-0000000000bb','pending_bundle_id',null,
    'bundle_revision',1,'candidate_id','c0000000-0000-4000-8000-000000000003',
    'member_root_ids',pg_catalog.jsonb_build_array(
      'c0000000-0000-4000-8000-000000000016','c0000000-0000-4000-8000-000000000026'),
    'member_family_booking_ids',pg_catalog.jsonb_build_array('WSPUB-0002','WSPUB-0003'),
    'member_root_versions',pg_catalog.jsonb_build_array(1,1),
    'head_ids',pg_catalog.jsonb_build_array(
      'c0000000-0000-4000-8000-0000000000cc','c0000000-0000-4000-8000-0000000000cd'),
    'decision_id','c0000000-0000-4000-8000-0000000000db','publication_mode','IMMEDIATE',
    'financial_request',pg_catalog.jsonb_build_object(
      'source_revision',pg_catalog.jsonb_build_object(
        'final_revision_id','c0000000-0000-4000-8000-0000000000fa',
        'source_cycle_id','c0000000-0000-4000-8000-0000000000f3','revision_number',1,
        'manifest_hash',pg_catalog.repeat('c1',32),
        'policy_fingerprint',pg_catalog.repeat('c2',32)),
      'contract_choices',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object('root_ordinal',1,
          'contract_id','c0000000-0000-4000-8000-000000000014',
          'week_ending_date','2026-03-08','selection_method','UNCHANGED'),
        pg_catalog.jsonb_build_object('root_ordinal',2,
          'contract_id','c0000000-0000-4000-8000-000000000004',
          'week_ending_date','2026-03-08','selection_method','OFFICE_SELECTED')),
      'member_entitlements',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object('root_ordinal',1,
          -- Round-5 Part E: the WHOLE entitlement moves to the new C root, so
          -- WSPUB-0002 is left holding nothing.  This bundle used to move one of
          -- the two components and keep the other, which is no longer a
          -- supported shape and is refused before any write.
          'authority_kind','LOCKED_FINAL_SOURCE','certified_zero',true,'component_count',0,
          'components',pg_catalog.jsonb_build_array()),
        pg_catalog.jsonb_build_object('root_ordinal',2,
          'authority_kind','LOCKED_FINAL_SOURCE','certified_zero',false,'component_count',2,
          'components',pg_catalog.jsonb_build_array(
            pg_temp.component(1,'c1c1c1c1-0000-4000-8000-000000000001','7.5','75.00',
              'c0000000-0000-4000-8000-0000000000e4'),
            pg_temp.component(2,'c1c1c1c1-0000-4000-8000-000000000002','2.25','22.50',
              'c0000000-0000-4000-8000-0000000000e3'))))),
    'control',pg_catalog.jsonb_build_object(
      'bundle_kind','CROSS_CONTRACT_A_B','reason','WEEKLY_SOURCE_ENTITLEMENT_PUBLICATION',
      'expected_current_head_ids',pg_catalog.jsonb_build_array(
        'c0000000-0000-4000-8000-0000000000c7',null),
      'before_positions',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object('root_ordinal',1,
          'component_ids',pg_catalog.jsonb_build_array(
            'c1c1c1c1-0000-4000-8000-000000000001','c1c1c1c1-0000-4000-8000-000000000002'),
          'inventory_digest',pg_catalog.repeat('00',32)),
        pg_catalog.jsonb_build_object('root_ordinal',2,
          'component_ids',pg_catalog.jsonb_build_array(),
          'inventory_digest',pg_catalog.repeat('00',32))),
      'moved_component_ids',pg_catalog.jsonb_build_array(
        'c1c1c1c1-0000-4000-8000-000000000001','c1c1c1c1-0000-4000-8000-000000000002'),
      'target_root_authorisation',pg_catalog.jsonb_build_object(
        'timesheet_id','c0000000-0000-4000-8000-000000000026',
        'expected_row_signature',pg_temp.row_signature('c0000000-0000-4000-8000-000000000026','signature-c-generation-1'),
        'actor_user_id','c0000000-0000-4000-8000-000000000001'),
      'whole_root_office_review',null));
  perform pg_temp.mk_bundle(v_request);

  select pg_catalog.count(*) into v_heads_before from public.weekly_source_entitlement_heads;
  select pg_catalog.count(*) into v_lineages_before
    from public.weekly_source_root_authorisations;

  -- R11, fourth forced step: I-6 refuses AFTER the heads have been staged.
  v_failed:=false;
  begin
    v_result:=private.weekly_source_entitlement_publish_core_v1(
      pg_catalog.jsonb_set(v_request,
        '{control,target_root_authorisation,expected_row_signature}',
        '"FORCE-I6-FAILURE"'::jsonb),
      'IMMEDIATE',v_lock,null,null,null,'{}'::jsonb,'{}'::jsonb);
  exception when others then
    v_failed:=true;
  end;
  perform pg_temp.assert_true(v_failed,
    'R11: a refusal from interface I-6 must roll the publication back, got '
      ||coalesce(v_result::text,'<null>'));
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_heads)=v_heads_before
    and (select pg_catalog.count(*)
           from public.weekly_source_root_authorisations)=v_lineages_before,
    'R11: nothing survives a refusal from interface I-6');

  -- The legal new-B-root publication.
  select coalesce(pg_catalog.array_agg(job_row.id),array[]::uuid[]) into v_jobs_before
    from public.banking_pay_workbench_jobs as job_row;
  v_result:=private.weekly_source_entitlement_publish_core_v1(
    v_request,'IMMEDIATE',v_lock,null,null,null,'{}'::jsonb,'{}'::jsonb);
  perform pg_temp.assert_true((v_result->>'ok')::boolean,
    'a genuinely new B root must be authorised through I-6 and receive its head: '
      ||v_result::text);
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_root_authorisations
      where root_timesheet_id='c0000000-0000-4000-8000-000000000026'
        and withdrawn_at_utc is null)=1,
    'interface I-6 inserted the root authorisation generation for the new B root');
  perform pg_temp.assert_true(
    (select authorisation_row.current_entitlement_head_id='c0000000-0000-4000-8000-0000000000cd'
       from public.weekly_source_root_authorisations as authorisation_row
      where authorisation_row.root_timesheet_id='c0000000-0000-4000-8000-000000000026'),
    'the new generation points at the head the coordinator activated');
  select coalesce(pg_catalog.array_agg(job_row.id),array[]::uuid[]) into v_new_jobs
    from public.banking_pay_workbench_jobs as job_row
   where not (job_row.id=any(v_jobs_before));
  perform pg_temp.assert_true(
    (select pg_catalog.count(*)
       from public.banking_pay_workbench_jobs as job_row
      where job_row.job_type='WORKBENCH_CANDIDATE_DIRTY_APPLY'
        and job_row.status='QUEUED'
        and job_row.payload_json->'targeted_timesheet_ids'
            =pg_catalog.jsonb_build_array('c0000000-0000-4000-8000-000000000016',
                                          'c0000000-0000-4000-8000-000000000026'))=1,
    'H2-036: exactly one complete-scope job covers both roots of the A-to-B bundle');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.banking_pay_scope_change_transactions)=1,
    'H2-036: still exactly one scope-change token in the transaction');
end
$verify_publication_new_target_root$;

-- ---------------------------------------------------------------------------
-- 12b. The immediate entry point, through the REAL I-1 and the REAL I-2
-- ---------------------------------------------------------------------------
-- Runs only when interface I-1 (WP-03) is installed.  Interface I-5 (WP-08b)
-- is stubbed when absent, because the FROZEN branch must be reachable.
do $verify_publication_immediate_entry$
declare
  v_result jsonb;
  v_request jsonb;
  v_heads_before bigint;
begin
  if pg_catalog.to_regprocedure(
       'private.weekly_source_lock_and_resolve_families_v1(uuid,uuid[],text,uuid,text)') is null then
    raise notice 'SKIPPED: interface I-1 is not installed; the core was proved with hand-built lock results';
    return;
  end if;
  if pg_catalog.to_regprocedure(
       'private.weekly_source_pending_entitlement_bundle_save_v1(jsonb,jsonb,jsonb)') is null then
    execute $ddl$
      create function private.weekly_source_pending_entitlement_bundle_save_v1(
        p_request jsonb,p_lock_result jsonb,p_census jsonb
      ) returns jsonb language sql as $double$
        -- TEST-DOUBLE for interface I-5 (WP-08b).
        select pg_catalog.jsonb_build_object('ok',true,'state','PENDING','test_double',true);
      $double$;
    $ddl$;
  end if;

  v_request:=pg_temp.single_root_request(
    'c0000000-0000-4000-8000-0000000000be',1,'c0000000-0000-4000-8000-0000000000ce',
    'c0000000-0000-4000-8000-0000000000de','c0000000-0000-4000-8000-0000000000c6',
    '[]'::jsonb,
    pg_catalog.jsonb_build_array(
      pg_temp.component(1,'c6c6c6c6-0000-4000-8000-000000000001','3.0','30.00')));
  perform pg_temp.mk_bundle(v_request);

  -- proof/32 section 6 step 1: while a Workbench build is queued for the
  -- Candidate the serial gate returns BLOCKED, which is a RETRYABLE refusal
  -- with no write.  Earlier sections of this file left such jobs queued, so
  -- this state is reached without arranging anything.
  v_result:=private.weekly_source_entitlement_publish_immediate_v1(v_request);
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->>'code'='WEEKLY_SOURCE_CANDIDATE_BUSY'
    and (v_result->>'retryable')::boolean,
    'a queued Candidate job must give a retryable WEEKLY_SOURCE_CANDIDATE_BUSY, got '
      ||v_result::text);
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_heads
      where id='c0000000-0000-4000-8000-0000000000ce')=0,
    'a BLOCKED serial gate writes nothing');

  -- TEST SCAFFOLDING: retire the queued Candidate jobs so the gate can grant.
  -- This is the verifier arranging a state, not the coordinator writing: the
  -- coordinator never touches a Workbench job.
  update public.banking_pay_workbench_jobs
     set status='SUCCEEDED',completed_at_utc=pg_catalog.clock_timestamp()
   where status in ('QUEUED','RUNNING');

  -- No Banking Pay evidence exists for this family, so the installed census
  -- returns RELEASABLE and the entry point publishes through the core.
  v_result:=private.weekly_source_entitlement_publish_immediate_v1(v_request);
  perform pg_temp.assert_true((v_result->>'ok')::boolean and (v_result->>'published')::boolean,
    'the immediate entry point must publish when the census is RELEASABLE: '
      ||(v_result-'lock_result'-'census')::text);
  perform pg_temp.assert_true(v_result->'census'->>'result'='RELEASABLE',
    'the installed freeze census returned '
      ||coalesce(v_result->'census'->>'result','<null>'));
  perform pg_temp.assert_true(
    (select head_row.state='COMMITTED_CURRENT'
       from public.weekly_source_entitlement_heads as head_row
      where head_row.id='c0000000-0000-4000-8000-0000000000ce'),
    'the entry point activated the head');

  -- 24 section 4.4: a frozen root saves the decision as pending, leaves the
  -- previous effective entitlement current, and publishes nothing.
  insert into public.pay_batches(
    id,pay_date,status,banking_system_snapshot,external_paye_system_snapshot)
  values ('c0000000-0000-4000-8000-00000000ba01','2026-03-18','DRAFT','MONZO_CSV','SAGE');
  insert into public.pay_batch_candidates(id,pay_batch_id,candidate_id,settlement_status)
  values ('c0000000-0000-4000-8000-00000000ba02','c0000000-0000-4000-8000-00000000ba01',
          'c0000000-0000-4000-8000-000000000003',null);
  insert into public.pay_batch_items(
    id,pay_batch_candidate_id,item_type,timesheet_id,pay_channel,is_voided,amount_inc_vat)
  values ('c0000000-0000-4000-8000-00000000ba03','c0000000-0000-4000-8000-00000000ba02',
          'TIMESHEET_PAY','c0000000-0000-4000-8000-000000000006','PAYE',false,100.00);

  select pg_catalog.count(*) into v_heads_before from public.weekly_source_entitlement_heads;
  v_request:=pg_temp.single_root_request(
      'c0000000-0000-4000-8000-0000000000bd',1,'c0000000-0000-4000-8000-0000000000cf',
      'c0000000-0000-4000-8000-0000000000dd','c0000000-0000-4000-8000-0000000000ce',
      pg_catalog.jsonb_build_array('c6c6c6c6-0000-4000-8000-000000000001'),
      pg_catalog.jsonb_build_array(
        pg_temp.component(1,'c7c7c7c7-0000-4000-8000-000000000001','4.0','40.00')));
  perform pg_temp.mk_bundle(v_request);
  v_result:=private.weekly_source_entitlement_publish_immediate_v1(v_request);
  perform pg_temp.assert_true(
    v_result->'census'->>'result'='FROZEN',
    'a live Draft item must freeze the root, census said '
      ||coalesce(v_result->'census'->>'result','<null>'));
  perform pg_temp.assert_true(
    (v_result->>'published')::boolean is false
    and v_result->>'code'='WEEKLY_SOURCE_PUBLICATION_DEFERRED_PENDING_FREEZE',
    '24 section 4.4: a frozen root publishes nothing and saves the decision as pending');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_heads)=v_heads_before,
    'no head was written while the root was frozen');
  perform pg_temp.assert_true(
    (select head_row.state='COMMITTED_CURRENT'
       from public.weekly_source_entitlement_heads as head_row
      where head_row.id='c0000000-0000-4000-8000-0000000000ce'),
    'the previous effective entitlement remains current while the root is frozen');
  -- proof/32 section 11: the coordinator touched no Banking Pay row.
  perform pg_temp.assert_true(
    (select pay_batch_row.status='DRAFT' and pay_batch_row.cancelled_at_utc is null
       from public.pay_batches as pay_batch_row
      where pay_batch_row.id='c0000000-0000-4000-8000-00000000ba01')
    and (select item_row.is_voided is false
           from public.pay_batch_items as item_row
          where item_row.id='c0000000-0000-4000-8000-00000000ba03'),
    'the existing Draft and its frozen evidence are unchanged');
end
$verify_publication_immediate_entry$;

-- Fire WP-01b's commit-time asserts again, now that the A-to-B bundle, the
-- certified-zero head, the new-B-root head and the immediate entry point's two
-- heads have all been written.
set constraints weekly_source_entitlement_head_inventory_assert,
                weekly_source_entitlement_head_receipt_assert,
                weekly_source_entitlement_head_component_inventory_assert immediate;
set constraints weekly_source_entitlement_head_inventory_assert,
                weekly_source_entitlement_head_receipt_assert,
                weekly_source_entitlement_head_component_inventory_assert deferred;

-- ---------------------------------------------------------------------------
-- 13. R12 — the second writer never publishes twice
-- ---------------------------------------------------------------------------
-- The two-session race itself is executed outside this file (two psql
-- processes; see IMPL\reports\WP-02_REPORT.md).  What is proved here is the
-- property that makes the race safe: the receipt relation's own uniqueness.
do $verify_publication_second_writer$
declare
  v_digest bytea;
begin
  select receipt_row.request_digest into v_digest
    from private.weekly_source_entitlement_publication_receipts as receipt_row
   order by receipt_row.created_at_utc limit 1;
  perform pg_temp.expect_failure(
    pg_catalog.format($sql$
      insert into private.weekly_source_entitlement_publication_receipts(
        decision_bundle_id,bundle_revision,request_digest,publication_mode,candidate_id,
        member_root_ids,member_family_booking_ids,member_root_versions,head_ids,
        scope_change_tx_token,decision_id,decided_by_user_id,census_json,proof_json
      ) values (
        'c0000000-0000-4000-8000-0000000000bf',1,%L::bytea,'IMMEDIATE',
        'c0000000-0000-4000-8000-000000000003',
        array['c0000000-0000-4000-8000-000000000006']::uuid[],array['WSPUB-0001']::text[],
        array[1]::integer[],array['c0000000-0000-4000-8000-0000000000cf']::uuid[],
        'c0000000-0000-4000-8000-0000000000ef','c0000000-0000-4000-8000-0000000000df',
        'c0000000-0000-4000-8000-000000000001','{}'::jsonb,'{}'::jsonb);
    $sql$,v_digest),
    '23505','','R12: a second receipt for the same request digest');
end
$verify_publication_second_writer$;

-- ---------------------------------------------------------------------------
-- 14. The independent review's attacks, as permanent cases
-- ---------------------------------------------------------------------------
-- Every executed attack in `handoffs\WP-02_REVIEW_URGENT.md` and
-- `reports\WP-02_REVIEW.md` that is not already covered above.
-- `p_accepted_review` is what the ACCEPTED DECISION BUNDLE is made to carry, as
-- distinct from `p_review`, which is what the request CLAIMS.  Leaving it null
-- means "the bundle carries whatever the request quotes, when the request quotes
-- a well-formed review" — the honest case.  Passing it explicitly is how the two
-- dishonest cases are staged: a request that quotes a review the accepted
-- decision does not carry, and a request that quotes a DIFFERENT reviewer from
-- the one the accepted decision carries.
-- `p_b_version` is B's canonical Timesheet version, which is not 1 once the B
-- family has rotated.  It is a parameter rather than a `jsonb_set` on the
-- returned request because this helper also writes the ACCEPTED bundle, and a
-- request edited afterwards no longer matches the acceptance digest.
create function pg_temp.ab_request(
  p_tag text,p_b_root uuid,p_b_booking text,p_target_auth jsonb,p_review jsonb,
  p_accepted_review jsonb default null,
  p_b_version integer default 1
) returns jsonb language plpgsql as $function$
declare
  v_head uuid;
  v_components uuid[];
  v_moved uuid;
  v_kept uuid[];
  v_a_after jsonb:='[]'::jsonb;
  v_b_after jsonb:='[]'::jsonb;
  v_component jsonb;
  v_ordinal integer:=0;
  v_request jsonb;
  v_accepted_review jsonb;
begin
  -- A is WSPUB-0001, whatever its current committed head happens to be.
  select head_row.id into v_head
    from public.weekly_source_entitlement_heads as head_row
   where head_row.root_timesheet_id='c0000000-0000-4000-8000-000000000006'
     and head_row.state='COMMITTED_CURRENT';
  select coalesce(pg_catalog.array_agg(component_row.component_id
                                       order by component_row.component_ordinal),
                  array[]::uuid[])
    into v_components
    from public.weekly_source_entitlement_head_components as component_row
   where component_row.head_id=v_head;
  v_moved:=v_components[pg_catalog.cardinality(v_components)];
  v_kept:=private.weekly_source_uuid_set_difference_v1(v_components,array[v_moved]);

  for v_ordinal in 1..pg_catalog.cardinality(v_kept) loop
    select pg_catalog.jsonb_build_object(
             'component_ordinal',v_ordinal,'component_id',component_row.component_id,
             'component_kind',component_row.component_kind,
             'economic_key_type',component_row.economic_key_type,
             'economic_key_value',component_row.economic_key_value,
             'component_member_identity',component_row.component_member_identity,
             'segment_id',component_row.segment_id,'segment_key',component_row.segment_key,
             'segment_stable_key',component_row.segment_stable_key,
             'work_date',pg_catalog.to_char(component_row.work_date,'YYYY-MM-DD'),
             'reference_number',component_row.reference_number,
             'hours_day',component_row.hours_day::text,'hours_night',component_row.hours_night,
             'hours_sat',component_row.hours_sat,'hours_sun',component_row.hours_sun,
             'hours_bh',component_row.hours_bh,
             'additional_code_raw',component_row.additional_code_raw,
             'unit_count',component_row.unit_count,'unit_pay_rate',component_row.unit_pay_rate,
             'unit_charge_rate',component_row.unit_charge_rate,
             'expense_code',component_row.expense_code,
             'pay_ex_vat',component_row.pay_ex_vat::text,
             'charge_ex_vat',component_row.charge_ex_vat,
             'exclude_from_pay',component_row.exclude_from_pay,'origin',component_row.origin,
             'movement_id',null,'movement_group_id',null)
      into v_component
      from public.weekly_source_entitlement_head_components as component_row
     where component_row.head_id=v_head and component_row.component_id=v_kept[v_ordinal];
    v_a_after:=v_a_after||pg_catalog.jsonb_build_array(v_component);
  end loop;

  select pg_catalog.jsonb_build_object(
           'component_ordinal',1,'component_id',component_row.component_id,
           'component_kind',component_row.component_kind,
           'economic_key_type',component_row.economic_key_type,
           'economic_key_value',component_row.economic_key_value,
           'component_member_identity',component_row.component_member_identity,
           'segment_id',component_row.segment_id,'segment_key',component_row.segment_key,
           'segment_stable_key',component_row.segment_stable_key,
           'work_date',pg_catalog.to_char(component_row.work_date,'YYYY-MM-DD'),
           'reference_number',component_row.reference_number,
           'hours_day',component_row.hours_day::text,'hours_night',component_row.hours_night,
           'hours_sat',component_row.hours_sat,'hours_sun',component_row.hours_sun,
           'hours_bh',component_row.hours_bh,
           'additional_code_raw',component_row.additional_code_raw,
           'unit_count',component_row.unit_count,'unit_pay_rate',component_row.unit_pay_rate,
           'unit_charge_rate',component_row.unit_charge_rate,
           'expense_code',component_row.expense_code,
           'pay_ex_vat',component_row.pay_ex_vat::text,
           'charge_ex_vat',component_row.charge_ex_vat,
           'exclude_from_pay',component_row.exclude_from_pay,'origin',component_row.origin,
           'movement_id',(pg_catalog.md5('move:'||p_tag))::uuid,'movement_group_id',null)
    into v_b_after
    from public.weekly_source_entitlement_head_components as component_row
   where component_row.head_id=v_head and component_row.component_id=v_moved;

  v_request:=pg_catalog.jsonb_build_object(
    'decision_bundle_id',(pg_catalog.md5('bundle:'||p_tag))::uuid,
    'pending_bundle_id',null,'bundle_revision',1,
    'candidate_id','c0000000-0000-4000-8000-000000000003',
    'member_root_ids',pg_catalog.jsonb_build_array(
      'c0000000-0000-4000-8000-000000000006',p_b_root),
    'member_family_booking_ids',pg_catalog.jsonb_build_array('WSPUB-0001',p_b_booking),
    'member_root_versions',pg_catalog.jsonb_build_array(1,p_b_version),
    'head_ids',pg_catalog.jsonb_build_array(
      (pg_catalog.md5('heada:'||p_tag))::uuid,(pg_catalog.md5('headb:'||p_tag))::uuid),
    'decision_id',(pg_catalog.md5('decision:'||p_tag))::uuid,
    'publication_mode','IMMEDIATE',
    'financial_request',pg_catalog.jsonb_build_object(
      'source_revision',pg_catalog.jsonb_build_object(
        'final_revision_id','c0000000-0000-4000-8000-0000000000fa',
        'source_cycle_id','c0000000-0000-4000-8000-0000000000f3','revision_number',1,
        'manifest_hash',pg_catalog.repeat('c1',32),
        'policy_fingerprint',pg_catalog.repeat('c2',32)),
      'contract_choices',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object('root_ordinal',1,
          'contract_id','c0000000-0000-4000-8000-000000000004',
          'week_ending_date','2026-03-08','selection_method','UNCHANGED'),
        pg_catalog.jsonb_build_object('root_ordinal',2,
          'contract_id','c0000000-0000-4000-8000-000000000014',
          'week_ending_date','2026-03-08','selection_method','OFFICE_SELECTED')),
      'member_entitlements',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object('root_ordinal',1,
          'authority_kind','LOCKED_FINAL_SOURCE',
          'certified_zero',pg_catalog.jsonb_array_length(v_a_after)=0,
          'component_count',pg_catalog.jsonb_array_length(v_a_after),
          'components',v_a_after),
        pg_catalog.jsonb_build_object('root_ordinal',2,
          'authority_kind','LOCKED_FINAL_SOURCE','certified_zero',false,'component_count',1,
          'components',pg_catalog.jsonb_build_array(v_b_after)))),
    'control',pg_catalog.jsonb_build_object(
      'bundle_kind','CROSS_CONTRACT_A_B','reason','WEEKLY_SOURCE_ENTITLEMENT_PUBLICATION',
      'expected_current_head_ids',pg_catalog.jsonb_build_array(v_head,null),
      'before_positions',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object('root_ordinal',1,
          'component_ids',pg_catalog.to_jsonb(v_components),
          'inventory_digest',pg_catalog.repeat('00',32)),
        pg_catalog.jsonb_build_object('root_ordinal',2,
          'component_ids',pg_catalog.jsonb_build_array(),
          'inventory_digest',pg_catalog.repeat('00',32))),
      'moved_component_ids',pg_catalog.jsonb_build_array(v_moved),
      'target_root_authorisation',p_target_auth,
      'whole_root_office_review',p_review));
  v_accepted_review:=case
    when p_accepted_review is not null then p_accepted_review
    when pg_catalog.jsonb_typeof(coalesce(p_review,'null'::jsonb))='object'
         and (p_review->>'reviewed_by_user_id') is not null
         and (p_review->>'reviewed_at_utc') is not null then p_review
    else null end;
  perform pg_temp.mk_bundle(v_request,
    (v_accepted_review->>'reviewed_by_user_id')::uuid,
    (v_accepted_review->>'reviewed_at_utc')::timestamptz);
  return v_request;
end;
$function$;

-- A valid SUCCESSOR publication over WSPUB-0001: the same components, kept
-- byte-identical, on top of whatever head is current at the time.
create function pg_temp.successor_request(p_tag text) returns jsonb
language plpgsql as $function$
declare
  v_head uuid;
  v_ids uuid[];
  v_components jsonb:='[]'::jsonb;
  v_request jsonb;
begin
  select head_row.id into v_head
    from public.weekly_source_entitlement_heads as head_row
   where head_row.root_timesheet_id='c0000000-0000-4000-8000-000000000006'
     and head_row.state='COMMITTED_CURRENT';
  select coalesce(pg_catalog.array_agg(component_row.component_id
                                       order by component_row.component_ordinal),
                  array[]::uuid[]),
         coalesce(pg_catalog.jsonb_agg(
           pg_catalog.jsonb_build_object(
             'component_ordinal',component_row.component_ordinal,
             'component_id',component_row.component_id,
             'component_kind',component_row.component_kind,
             'economic_key_type',component_row.economic_key_type,
             'economic_key_value',component_row.economic_key_value,
             'component_member_identity',component_row.component_member_identity,
             'segment_id',component_row.segment_id,'segment_key',component_row.segment_key,
             'segment_stable_key',component_row.segment_stable_key,
             'work_date',pg_catalog.to_char(component_row.work_date,'YYYY-MM-DD'),
             'reference_number',component_row.reference_number,
             'hours_day',component_row.hours_day::text,'hours_night',component_row.hours_night,
             'hours_sat',component_row.hours_sat,'hours_sun',component_row.hours_sun,
             'hours_bh',component_row.hours_bh,
             'additional_code_raw',component_row.additional_code_raw,
             'unit_count',component_row.unit_count,'unit_pay_rate',component_row.unit_pay_rate,
             'unit_charge_rate',component_row.unit_charge_rate,
             'expense_code',component_row.expense_code,
             'pay_ex_vat',component_row.pay_ex_vat::text,
             'charge_ex_vat',component_row.charge_ex_vat,
             'exclude_from_pay',component_row.exclude_from_pay,'origin',component_row.origin,
             'movement_id',null,'movement_group_id',null)
           order by component_row.component_ordinal),'[]'::jsonb)
    into v_ids,v_components
    from public.weekly_source_entitlement_head_components as component_row
   where component_row.head_id=v_head;

  v_request:=pg_temp.single_root_request(
    (pg_catalog.md5('bundle:'||p_tag))::uuid,1,
    (pg_catalog.md5('head:'||p_tag))::uuid,
    (pg_catalog.md5('decision:'||p_tag))::uuid,
    v_head,pg_catalog.to_jsonb(v_ids),v_components);
  perform pg_temp.mk_bundle(v_request);
  return v_request;
end;
$function$;

do $verify_publication_review_attacks$
declare
  v_request jsonb;
  v_result jsonb;
  v_lock jsonb;
  v_heads_before bigint;
  v_jobs_before uuid[];
  v_receipt jsonb;
begin
  -- ============================ U2 ======================================
  -- A pre-existing, unauthorised B that carries its own unrelated shifts is
  -- unknown to Weekly Source but NOT blank, so it is never authorised silently.
  v_lock:=pg_temp.lock_result(
    pg_catalog.jsonb_build_array('c0000000-0000-4000-8000-000000000006',
                                 'c0000000-0000-4000-8000-000000000036'),
    pg_catalog.jsonb_build_array('WSPUB-0001','WSPUB-0004'),
    pg_catalog.jsonb_build_array(1,1));
  select pg_catalog.count(*) into v_heads_before from public.weekly_source_entitlement_heads;
  v_request:=pg_temp.ab_request('u2-silent','c0000000-0000-4000-8000-000000000036','WSPUB-0004',
    pg_catalog.jsonb_build_object(
      'timesheet_id','c0000000-0000-4000-8000-000000000036',
      'expected_row_signature',pg_temp.row_signature('c0000000-0000-4000-8000-000000000036','signature-d'),'actor_user_id',
      'c0000000-0000-4000-8000-000000000001'),
    null);
  v_result:=private.weekly_source_entitlement_publish_core_v1(
    v_request,'IMMEDIATE',v_lock,null,null,null,'{}'::jsonb,'{}'::jsonb);
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->>'code'='WEEKLY_SOURCE_PUBLICATION_TARGET_ROOT_REVIEW_REQUIRED',
    'U2: a pre-existing B carrying unrelated work must never be authorised silently, got '
      ||v_result::text);
  perform pg_temp.assert_true(
    v_result->'detail'->'blank_check'->>'blank'='false'
    and v_result->'detail'->'blank_check'->'reasons' @> '["CARRIES_ITS_OWN_SCHEDULE"]'::jsonb,
    'U2: the refusal names the reason the root is not blank, got '
      ||coalesce((v_result->'detail'->'blank_check')::text,'<null>'));
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_heads)=v_heads_before
    and (select pg_catalog.count(*) from public.weekly_source_root_authorisations
          where root_timesheet_id='c0000000-0000-4000-8000-000000000036')=0,
    'U2: nothing was written and B was not authorised');
  perform pg_temp.assert_true(
    v_result->'detail'->>'reason'='THE_ACCEPTED_DECISION_CARRIES_NO_WHOLE_ROOT_OFFICE_REVIEW',
    'U2: the refusal says the ACCEPTED DECISION carries no review, got '||v_result::text);

  -- The review is a real recorded act or it is nothing: a JSON STRING "true",
  -- with no reviewer and no time, is not a control.  The accepted decision is
  -- given a genuine persisted review here, so the refusal can only come from
  -- the request's own malformed claim and not from the bundle carrying none.
  v_request:=pg_temp.ab_request('u2-stringtrue','c0000000-0000-4000-8000-000000000036','WSPUB-0004',
    pg_catalog.jsonb_build_object(
      'timesheet_id','c0000000-0000-4000-8000-000000000036',
      'expected_row_signature',pg_temp.row_signature('c0000000-0000-4000-8000-000000000036','signature-d'),'actor_user_id',
      'c0000000-0000-4000-8000-000000000001'),
    pg_catalog.jsonb_build_object('reviewed','true'),
    pg_catalog.jsonb_build_object(
      'reviewed_by_user_id','c0000000-0000-4000-8000-000000000001',
      'reviewed_at_utc','2026-09-17T18:00:00Z'));
  v_result:=private.weekly_source_entitlement_publish_core_v1(
    v_request,'IMMEDIATE',v_lock,null,null,null,'{}'::jsonb,'{}'::jsonb);
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->>'code'='WEEKLY_SOURCE_PUBLICATION_TARGET_ROOT_REVIEW_REQUIRED'
    and v_result->'detail'->>'reason'='WHOLE_ROOT_OFFICE_REVIEW_MISSING_OR_MALFORMED',
    'U2: a JSON string "true" is not a whole-root Office review, got '||v_result::text);

  -- A well-formed review that the ACCEPTED DECISION does not carry: the caller
  -- has invented an approval for itself.  Staged against a reviewer who really
  -- exists, so what is being refused is the BINDING and not the identity.
  v_request:=pg_temp.ab_request('u2-wrongreviewer','c0000000-0000-4000-8000-000000000036','WSPUB-0004',
    pg_catalog.jsonb_build_object(
      'timesheet_id','c0000000-0000-4000-8000-000000000036',
      'expected_row_signature',pg_temp.row_signature('c0000000-0000-4000-8000-000000000036','signature-d'),'actor_user_id',
      'c0000000-0000-4000-8000-000000000001'),
    pg_catalog.jsonb_build_object(
      'reviewed',true,
      'reviewed_by_user_id','c0000000-0000-4000-8000-000000000051',
      'reviewed_at_utc','2026-09-17T18:00:00Z',
      'decision_id',(pg_catalog.md5('decision:u2-wrongreviewer'))::uuid),
    pg_catalog.jsonb_build_object(
      'reviewed_by_user_id','c0000000-0000-4000-8000-000000000001',
      'reviewed_at_utc','2026-09-17T18:00:00Z'));
  v_result:=private.weekly_source_entitlement_publish_core_v1(
    v_request,'IMMEDIATE',v_lock,null,null,null,'{}'::jsonb,'{}'::jsonb);
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->>'code'='WEEKLY_SOURCE_PUBLICATION_TARGET_ROOT_REVIEW_REQUIRED'
    and v_result->'detail'->>'reason'='REVIEW_IS_NOT_THE_ONE_ON_THE_ACCEPTED_DECISION',
    'U2: a review the accepted decision does not carry is not a review, got '||v_result::text);

  -- The same, with the right reviewer but the wrong TIME: quoting half of a
  -- real approval is quoting no approval.
  v_request:=pg_temp.ab_request('u2-wrongtime','c0000000-0000-4000-8000-000000000036','WSPUB-0004',
    pg_catalog.jsonb_build_object(
      'timesheet_id','c0000000-0000-4000-8000-000000000036',
      'expected_row_signature',pg_temp.row_signature('c0000000-0000-4000-8000-000000000036','signature-d'),'actor_user_id',
      'c0000000-0000-4000-8000-000000000001'),
    pg_catalog.jsonb_build_object(
      'reviewed',true,
      'reviewed_by_user_id','c0000000-0000-4000-8000-000000000001',
      'reviewed_at_utc','2026-09-17T19:30:00Z',
      'decision_id',(pg_catalog.md5('decision:u2-wrongtime'))::uuid),
    pg_catalog.jsonb_build_object(
      'reviewed_by_user_id','c0000000-0000-4000-8000-000000000001',
      'reviewed_at_utc','2026-09-17T18:00:00Z'));
  v_result:=private.weekly_source_entitlement_publish_core_v1(
    v_request,'IMMEDIATE',v_lock,null,null,null,'{}'::jsonb,'{}'::jsonb);
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->'detail'->>'reason'='REVIEW_IS_NOT_THE_ONE_ON_THE_ACCEPTED_DECISION',
    'U2: the review TIME is part of the binding, got '||v_result::text);

  -- And the honest case: the accepted decision carries the review, the request
  -- quotes exactly it, and B is authorised — with the act left on the record.
  select pg_catalog.count(*) into v_heads_before from public.weekly_source_entitlement_heads;
  v_request:=pg_temp.ab_request('u2-realreview','c0000000-0000-4000-8000-000000000036','WSPUB-0004',
    pg_catalog.jsonb_build_object(
      'timesheet_id','c0000000-0000-4000-8000-000000000036',
      'expected_row_signature',pg_temp.row_signature('c0000000-0000-4000-8000-000000000036','signature-d'),'actor_user_id',
      'c0000000-0000-4000-8000-000000000001'),
    pg_catalog.jsonb_build_object(
      'reviewed',true,
      'reviewed_by_user_id','c0000000-0000-4000-8000-000000000001',
      'reviewed_at_utc','2026-09-17T18:00:00Z',
      'decision_id',(pg_catalog.md5('decision:u2-realreview'))::uuid));
  v_result:=private.weekly_source_entitlement_publish_core_v1(
    v_request,'IMMEDIATE',v_lock,null,null,null,'{}'::jsonb,'{}'::jsonb);
  perform pg_temp.assert_true((v_result->>'ok')::boolean,
    'U2: a persisted whole-root review on the accepted decision must let B be authorised, got '
      ||v_result::text);
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_root_authorisations
      where root_timesheet_id='c0000000-0000-4000-8000-000000000036')=1,
    'U2: the reviewed B root really is authorised now');
  -- The point of persisting it: the act can be shown afterwards, from the
  -- bundle, without trusting anything the caller said.
  perform pg_temp.assert_true(
    (select bundle_row.whole_root_review_required
            and bundle_row.whole_root_reviewed_by_user_id
                ='c0000000-0000-4000-8000-000000000001'
            and bundle_row.whole_root_reviewed_at_utc='2026-09-17T18:00:00Z'::timestamptz
       from public.weekly_source_entitlement_decision_bundles as bundle_row
      where bundle_row.decision_bundle_id
            =(pg_catalog.md5('bundle:u2-realreview'))::uuid
        and bundle_row.bundle_revision=1),
    'U2: the review that let B through is on the record afterwards');

  -- The three columns are IDENTITY, not lifecycle: the review cannot be
  -- attached to, moved on, or withdrawn from a bundle that already exists.
  -- (Were it writable, a caller could grant itself the approval it needs.)
  if pg_catalog.to_regprocedure(
       'private._weekly_source_immutable_fact_guard_v1()') is not null then
    perform pg_temp.expect_failure(
      $sql$update public.weekly_source_entitlement_decision_bundles
              set whole_root_reviewed_by_user_id='c0000000-0000-4000-8000-000000000051'
            where decision_bundle_id=(pg_catalog.md5('bundle:u2-realreview'))::uuid$sql$,
      null,null,
      'U2: a persisted whole-root review can never be re-pointed at another reviewer');
  end if;

  -- ============================ F14 =====================================
  -- Interface I-6 answers ok:TRUE and inserts nothing - the breach handoff N6
  -- warns about.  The pointer guard is the only defence, and it fires.  (This
  -- is why the M6 mutant is NOT equivalent.)
  -- The breach can only be staged with the rollback-only I-6 double: the real
  -- owner always inserts, and replacing it would mean re-creating another
  -- package's definition, which the boundary forbids.  The guard's presence is
  -- asserted in every configuration; its behaviour is executed whenever the
  -- double is the installed one.
  perform pg_temp.assert_true(
    pg_catalog.pg_get_functiondef(pg_catalog.to_regprocedure(
      'private.weekly_source_entitlement_publish_core_v1(jsonb,text,jsonb,uuid,text,uuid,jsonb,jsonb)'))
    ~ 'WEEKLY_SOURCE_PUBLICATION_ROOT_AUTHORISATION_POINTER_FAILED',
    'F14: the root-authorisation pointer guard is present in the installed coordinator');
  if pg_catalog.pg_get_functiondef(pg_catalog.to_regprocedure(
       'private.weekly_source_first_authorise_core_v1(uuid,text,uuid,jsonb)'))
     ~ 'FORCE-I6-SILENT-OK' then
    v_lock:=pg_temp.lock_result(
      pg_catalog.jsonb_build_array('c0000000-0000-4000-8000-000000000006',
                                   'c0000000-0000-4000-8000-000000000046'),
      pg_catalog.jsonb_build_array('WSPUB-0001','WSPUB-0005'),
      pg_catalog.jsonb_build_array(1,1));
    select pg_catalog.count(*) into v_heads_before from public.weekly_source_entitlement_heads;
    v_request:=pg_temp.ab_request('f14-silent','c0000000-0000-4000-8000-000000000046','WSPUB-0005',
      pg_catalog.jsonb_build_object(
        'timesheet_id','c0000000-0000-4000-8000-000000000046',
        'expected_row_signature','FORCE-I6-SILENT-OK','actor_user_id',
        'c0000000-0000-4000-8000-000000000001'),
      null);
    begin
      v_result:=private.weekly_source_entitlement_publish_core_v1(
        v_request,'IMMEDIATE',v_lock,null,null,null,'{}'::jsonb,'{}'::jsonb);
      perform pg_temp.assert_true(false,
        'F14: an I-6 that answers ok and inserts nothing must not publish, got '
          ||coalesce(v_result::text,'<null>'));
    exception when sqlstate '55000' then
      null;
    end;
    perform pg_temp.assert_true(
      (select pg_catalog.count(*) from public.weekly_source_entitlement_heads)=v_heads_before,
      'F14: the pointer guard rolled the whole publication back');
  end if;

  -- ============================ F7 ======================================
  -- A SECOND, DIFFERENT request under an already COMMITTED bundle revision is
  -- refused with a code, not left to the head primary key as a raised 23505
  -- that WP-08b would count as a technical failure.
  v_result:=pg_temp.publish(
    pg_catalog.jsonb_set(
      pg_temp.single_root_request(
        'c0000000-0000-4000-8000-0000000000b1',1,'c0000000-0000-4000-8000-00000000f701',
        'c0000000-0000-4000-8000-0000000000d1',null,'[]'::jsonb,
        pg_catalog.jsonb_build_array(
          pg_temp.component(1,'cf01cf01-0000-4000-8000-000000000001','1.0','10.00'))),
      '{bundle_revision}','1'::jsonb),
    pg_temp.lock_result_a());
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->>'code' in ('WEEKLY_SOURCE_PUBLICATION_BUNDLE_REVISION_ALREADY_PUBLISHED',
                              'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'),
    'F7: a second different request under a COMMITTED bundle revision must be refused '
      ||'with a code, got '||v_result::text);

  -- ============================ F8 ======================================
  -- A pass-through wrapper that strips `candidate_count` from the invalidator's
  -- result must FAIL the publication, not slip through a NULL comparison.
  -- Proved here on the expression itself, because wrapping the installed
  -- invalidator would mean re-creating a Banking Pay definition, which the
  -- boundary forbids.
  perform pg_temp.assert_true(
    (('{"ok":true}'::jsonb->>'candidate_count')::integer is distinct from 1) is true,
    'F8: a missing candidate_count must fail closed');
  perform pg_temp.assert_true(
    pg_catalog.pg_get_functiondef(pg_catalog.to_regprocedure(
      'private.weekly_source_entitlement_publish_core_v1(jsonb,text,jsonb,uuid,text,uuid,jsonb,jsonb)'))
    ~ 'candidate_count''\)::integer is distinct from 1',
    'F8: the installed coordinator uses `is distinct from 1`, never `<>`');

  -- ===================== WP-08b handoff N4 ==============================
  -- `v_before_source := v_before_source || 'HEAD'` is `anyarray || anyarray`,
  -- so PostgreSQL parses the literal as an array and the statement fails at
  -- FIRST EXECUTION with `malformed array literal` - the compiles-then-fails
  -- class the workspace AGENTS.md rule warns about.  All three assignments now
  -- carry an explicit `::text`, and all three paths are driven here so the
  -- regression cannot come back silently.  Each value below could only have
  -- been produced by executing its own branch.
  perform pg_temp.assert_true(
    pg_catalog.pg_get_functiondef(pg_catalog.to_regprocedure(
      'private.weekly_source_entitlement_publish_core_v1(jsonb,text,jsonb,uuid,text,uuid,jsonb,jsonb)'))
    !~ 'v_before_source\|\|''[A-Z_]+'';',
    'N4: no untyped literal is concatenated onto a text[] in the installed definition');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_heads
      where id='c0000000-0000-4000-8000-0000000000c1')=1,
    'N4: the DECLARED_UNPROVED path ran in section 6 and published a head');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_heads
      where id='c0000000-0000-4000-8000-0000000000c6')=1,
    'N4: the HEAD path ran in section 11 and published A-after');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_heads
      where id='c0000000-0000-4000-8000-0000000000c7')=1,
    'N4: the I7 path ran in section 11 and published B-after');

  -- ===================== HANDOVER 2 round 4, ruling 6 ====================
  -- R25 restated: both possibilities must be tested.  Possibility ONE - one or
  -- more registered DIRTY_TRIGGER: paths occur in the release transaction and
  -- coalesce - is the state every publication above ran in, because the fixture
  -- inserts and the ordinary trigger route queue their own jobs for the same
  -- Candidate under the same token.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*)
       from public.banking_pay_workbench_jobs as job_row
      where job_row.candidate_id='c0000000-0000-4000-8000-000000000003'
        and job_row.payload_json->>'reason' like 'DIRTY_TRIGGER:%')>0,
    'ruling 6, possibility one: registered DIRTY_TRIGGER paths did occur in this transaction');
  perform pg_temp.assert_true(
    (select pg_catalog.count(distinct job_row.scope_change_tx_token)
       from public.banking_pay_workbench_jobs as job_row
      where job_row.candidate_id='c0000000-0000-4000-8000-000000000003')=1,
    'ruling 6(1): every job queued for the Candidate carries ONE token UUID');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*)
       from public.banking_pay_workbench_jobs as job_row
      where job_row.candidate_id='c0000000-0000-4000-8000-000000000003'
        and job_row.scope_change_generation is not null)=0,
    'ruling 6(3): no job for the Candidate is visible with a generation before commit');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.banking_pay_scope_change_transactions)=1,
    'ruling 6(1): one scope-change transaction token for the whole release transaction');

  -- Possibility TWO - NO registered DIRTY_TRIGGER path arises for the
  -- publication itself.  Inside one transaction the only way to reach that
  -- state is to retire the queued work first (test scaffolding, as in section
  -- 12b), then publish and look at what the publication alone added.
  update public.banking_pay_workbench_jobs
     set status='SUCCEEDED',completed_at_utc=pg_catalog.clock_timestamp()
   where status in ('QUEUED','RUNNING');
  select coalesce(pg_catalog.array_agg(job_row.id),array[]::uuid[]) into v_jobs_before
    from public.banking_pay_workbench_jobs as job_row;
  v_request:=pg_temp.successor_request('r6-nodirty');
  v_result:=pg_temp.publish(v_request,pg_temp.lock_result_a());
  perform pg_temp.assert_true((v_result->>'ok')::boolean,
    'ruling 6, possibility two: the publication must succeed, got '||v_result::text);
  perform pg_temp.assert_true(
    (select pg_catalog.count(*)
       from public.banking_pay_workbench_jobs as job_row
      where not (job_row.id=any(v_jobs_before))
        and coalesce(job_row.payload_json->>'reason','') like 'DIRTY_TRIGGER:%')=0,
    'ruling 6, possibility two: no DIRTY_TRIGGER job arose for this publication');
  -- HANDOVER 2 round-7 ruling A7.  Possibility two retires the queue first, so
  -- the raw count here happens to be one; that is a coincidence of the fixture,
  -- not the property.  The property is ONE effective complete-scope outcome, and
  -- it is stated that way so the assertion cannot pass for the wrong reason.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*)
       from public.banking_pay_workbench_jobs as job_row
      where not (job_row.id=any(v_jobs_before))
        and job_row.candidate_id='c0000000-0000-4000-8000-000000000003'
        and job_row.job_type='WORKBENCH_CANDIDATE_DIRTY_APPLY'
        and job_row.status in ('QUEUED','RUNNING')
        and private.weekly_source_uuid_set_equals_v1(
              (select coalesce(pg_catalog.array_agg(distinct target.value::uuid),array[]::uuid[])
                 from pg_catalog.jsonb_array_elements_text(
                        coalesce(job_row.payload_json->'targeted_timesheet_ids','[]'::jsonb))
                      as target(value)),
              array['c0000000-0000-4000-8000-000000000006']::uuid[]))=1,
    'ruling 6, possibility two (A7): the publication coalesces to exactly ONE '
      ||'effective complete-scope outcome for the Candidate');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*)
       from public.banking_pay_workbench_jobs as job_row
      where not (job_row.id=any(v_jobs_before))
        and job_row.candidate_id='c0000000-0000-4000-8000-000000000003'
        and job_row.scope_change_tx_token
            is distinct from (v_result->>'scope_change_tx_token')::uuid)=0,
    'ruling 6(1): every job the publication persisted is under the same '
      ||'controlling token');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.banking_pay_scope_change_transactions)=1,
    'ruling 6(1): still one token for the transaction');

  -- ============================ U5 ======================================
  -- DEFERRED mode does not trust the census it is handed, and requires the
  -- pending bundle to exist.
  select pg_catalog.count(*) into v_heads_before from public.weekly_source_entitlement_heads;
  v_request:=pg_temp.single_root_request(
    'c0000000-0000-4000-8000-00000000d501',1,'c0000000-0000-4000-8000-00000000d502',
    'c0000000-0000-4000-8000-00000000d503','c0000000-0000-4000-8000-0000000000cf',
    '[]'::jsonb,
    pg_catalog.jsonb_build_array(
      pg_temp.component(1,'cd01cd01-0000-4000-8000-000000000001','1.0','10.00')));
  perform pg_temp.mk_bundle(v_request);
  v_result:=private.weekly_source_entitlement_publish_core_v1(
    v_request,'DEFERRED',pg_temp.lock_result_a(),
    'c0000000-0000-4000-8000-00000000d504','worker-1',
    'c0000000-0000-4000-8000-00000000d505',
    '{"result":"FROZEN","reason":"ACTIVE_DRAFT_ITEM"}'::jsonb,
    '{"note":"not a proof"}'::jsonb);
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->>'code'='WEEKLY_SOURCE_PUBLICATION_CENSUS_NOT_RELEASABLE',
    'U5: a DEFERRED release may not publish on a FROZEN census, got '||v_result::text);
  v_result:=private.weekly_source_entitlement_publish_core_v1(
    v_request,'DEFERRED',pg_temp.lock_result_a(),
    'c0000000-0000-4000-8000-00000000d504','worker-1',
    'c0000000-0000-4000-8000-00000000d505',
    '{"result":"RELEASABLE"}'::jsonb,'{}'::jsonb);
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->>'code'='WEEKLY_SOURCE_PUBLICATION_PENDING_BUNDLE_INVALID',
    'U5: a DEFERRED release needs a real pending bundle row, got '||v_result::text);
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_heads)=v_heads_before,
    'U5: neither DEFERRED refusal wrote anything');
end
$verify_publication_review_attacks$;

-- ---------------------------------------------------------------------------
-- 15. U4 — exact replay is the FIRST thing the immediate entry point does
-- ---------------------------------------------------------------------------
do $verify_publication_replay_first$
declare
  v_request jsonb;
  v_first jsonb;
  v_replay jsonb;
  v_pending_before bigint;
begin
  if pg_catalog.to_regprocedure(
       'private.weekly_source_lock_and_resolve_families_v1(uuid,uuid[],text,uuid,text)') is null then
    raise notice 'SKIPPED U4: interface I-1 is not installed';
    return;
  end if;
  -- A publication already exists for this decision (section 12b published head
  -- …0ce over WSPUB-0001 through the immediate entry point), and the root is
  -- now FROZEN by the live Draft item section 12b inserted.  A retry whose
  -- first response was lost must get its RECEIPT back - not a pending bundle
  -- for a decision that is already published, and not a stale warning to the
  -- very Draft that is paying it.
  v_request:=pg_temp.single_root_request(
    'c0000000-0000-4000-8000-0000000000be',1,'c0000000-0000-4000-8000-0000000000ce',
    'c0000000-0000-4000-8000-0000000000de','c0000000-0000-4000-8000-0000000000c6',
    '[]'::jsonb,
    pg_catalog.jsonb_build_array(
      pg_temp.component(1,'c6c6c6c6-0000-4000-8000-000000000001','3.0','30.00')));
  perform pg_temp.assert_true(
    (select pg_catalog.count(*)
       from private.weekly_source_entitlement_publication_receipts as receipt_row
      where receipt_row.decision_bundle_id='c0000000-0000-4000-8000-0000000000be')=1,
    'U4: the first publication left exactly one receipt to replay');

  v_replay:=private.weekly_source_entitlement_publish_immediate_v1(v_request);
  perform pg_temp.assert_true(
    (v_replay->>'ok')::boolean and (v_replay->>'replayed')::boolean
    and (v_replay->>'published')::boolean,
    'U4: an exact replay must return the committed receipt even though the root is now '
      ||'frozen, got '||(v_replay-'census'-'lock_result')::text);
  perform pg_temp.assert_true(
    (v_replay->'receipt'->>'decision_bundle_id')::uuid
    ='c0000000-0000-4000-8000-0000000000be',
    'U4: and it is the right receipt');
  perform pg_temp.assert_true(
    v_replay->'pending' is null,
    'U4: no pending bundle is saved for an already published decision');

  -- A conflicting replay - same digest scope, tampered receipt - still refuses
  -- at the entry point, before the gate and the census.
  set local session_replication_role='replica';
  update private.weekly_source_entitlement_publication_receipts
     set member_root_versions=array[99]::integer[]
   where decision_bundle_id='c0000000-0000-4000-8000-0000000000be';
  set local session_replication_role='origin';
  v_replay:=private.weekly_source_entitlement_publish_immediate_v1(v_request);
  perform pg_temp.assert_true(
    (v_replay->>'ok')::boolean is false
    and v_replay->>'code'='WEEKLY_SOURCE_PUBLICATION_REPLAY_CONFLICT',
    'U4: a conflicting replay refuses at the entry point, got '||v_replay::text);
  -- Round-5 ruling A1 control 5: a conflicting replay is a PERMANENT integrity
  -- failure that goes DIRECTLY to manual review, not one of ten retries.
  perform pg_temp.assert_true(
    coalesce((v_replay->'detail'->>'integrity_failure')::boolean,false)
    and v_replay->'detail'->>'disposition'='MANUAL_REVIEW'
    and coalesce((v_replay->>'retryable')::boolean,true) is false,
    'A1 control 5: a conflicting replay must carry integrity_failure and disposition '
      ||'MANUAL_REVIEW, got '||v_replay::text);
  set local session_replication_role='replica';
  update private.weekly_source_entitlement_publication_receipts
     set member_root_versions=array[1]::integer[]
   where decision_bundle_id='c0000000-0000-4000-8000-0000000000be';
  set local session_replication_role='origin';
end
$verify_publication_replay_first$;

-- ---------------------------------------------------------------------------
-- 16. Decision D10 — the stored request and its digest are verified together
-- ---------------------------------------------------------------------------
-- The migration cannot bind `request_json` to `request_digest`, because such a
-- trigger would have to call the canonical encoder, which lives in a repeatable,
-- and migrations are applied before repeatables: a rebuild from empty would
-- fail.  The safety belongs in the coordinator, at BOTH points, under the lock
-- it already holds.
do $verify_publication_d10$
declare
  v_request jsonb;
  v_result jsonb;
  v_pending_id uuid;
  v_heads_before bigint;
  v_failed boolean;
begin
  if pg_catalog.to_regprocedure(
       'private.weekly_source_pending_entitlement_bundle_save_v1(jsonb,jsonb,jsonb)') is null
     or (select pg_catalog.count(*) from pg_catalog.pg_attribute
          where attrelid='public.weekly_source_pending_entitlement_bundles'::regclass
            and not attisdropped and attname='request_json')=0 then
    raise notice 'SKIPPED D10: interface I-5 or request_json is not installed';
    return;
  end if;

  -- ---- D10, point one: SAVE time -------------------------------------
  -- Section 12b already drove the FROZEN branch through the real I-1, the real
  -- I-2 and interface I-5, and the entry point verified what was stored before
  -- returning.  A pending bundle exists and its stored request digests to its
  -- stored digest.
  select pending_row.id into v_pending_id
    from public.weekly_source_pending_entitlement_bundles as pending_row
   where pending_row.state in ('PENDING','RELEASING')
   order by pending_row.created_at_utc desc
   limit 1;
  if v_pending_id is null then
    raise notice 'SKIPPED D10: no pending bundle was saved in this run';
    return;
  end if;
  perform pg_temp.assert_true(
    (select private.weekly_source_publication_request_digest_v1(
              private.weekly_source_publication_request_canonical_v1(
                pending_row.request_json,'DEFERRED',pending_row.id))
            =pending_row.request_digest
       from public.weekly_source_pending_entitlement_bundles as pending_row
      where pending_row.id=v_pending_id),
    'D10 save: the stored request digests to the stored digest');

  -- ---- D10, point two: RELEASE time ----------------------------------
  select pg_catalog.count(*) into v_heads_before from public.weekly_source_entitlement_heads;
  select pending_row.request_json into v_request
    from public.weekly_source_pending_entitlement_bundles as pending_row
   where pending_row.id=v_pending_id;
  v_result:=private.weekly_source_entitlement_publish_core_v1(
    v_request,'DEFERRED',pg_temp.lock_result_a(),v_pending_id,'worker-d10',
    'c0000000-0000-4000-8000-00000000d10a',
    '{"result":"RELEASABLE"}'::jsonb,'{"settlement":[]}'::jsonb);
  perform pg_temp.assert_true(
    v_result->>'code' is distinct from 'WEEKLY_SOURCE_PUBLICATION_PENDING_BUNDLE_INVALID',
    'D10 release: an untampered pending bundle passes the paired check, got '||v_result::text);

  -- ---- D10, point three: a TAMPERED stored request refuses the release ----
  -- First, the schema's own defence: `request_json` is an identity column, so
  -- the ACL immutable-fact guard refuses an ordinary update outright.
  v_failed:=false;
  begin
    update public.weekly_source_pending_entitlement_bundles
       set request_json='{}'::jsonb
     where id=v_pending_id;
  exception when others then
    v_failed:=true;
  end;
  perform pg_temp.assert_true(v_failed,
    'D10: the ACL immutable-fact guard refuses an ordinary update to request_json');

  -- Then the state the migration cannot prevent and D10 exists for: the stored
  -- request altered while the stored digest is left alone.  Reached here only
  -- by suspending triggers, which is what a mis-written row or an out-of-band
  -- change would look like to the release.
  set local session_replication_role='replica';
  update public.weekly_source_pending_entitlement_bundles
     set request_json=pg_catalog.jsonb_set(request_json,
           '{financial_request,member_entitlements,0,components,0,pay_ex_vat}','"9999.00"'::jsonb)
   where id=v_pending_id;
  set local session_replication_role='origin';
  select pending_row.request_json into v_request
    from public.weekly_source_pending_entitlement_bundles as pending_row
   where pending_row.id=v_pending_id;
  v_result:=private.weekly_source_entitlement_publish_core_v1(
    v_request,'DEFERRED',pg_temp.lock_result_a(),v_pending_id,'worker-d10',
    'c0000000-0000-4000-8000-00000000d10a',
    '{"result":"RELEASABLE"}'::jsonb,'{"settlement":[]}'::jsonb);
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->>'code'='WEEKLY_SOURCE_PUBLICATION_PENDING_BUNDLE_INVALID'
    and v_result->'detail'->>'reason'='THE_STORED_REQUEST_DOES_NOT_MATCH_ITS_STORED_DIGEST',
    'D10 release: a pending bundle whose stored request no longer matches its digest must '
      ||'refuse the release, got '||v_result::text);
  -- Round-5 ruling A1 controls 3 and 5.  The release recalculated the digest
  -- from the STORED canonical request itself and refused before any effect; and
  -- a tampered row is a PERMANENT integrity failure that goes DIRECTLY to manual
  -- review, so the refusal carries the disposition the deferred release owner
  -- must honour instead of counting one of its ten technical failures.
  perform pg_temp.assert_true(
    coalesce((v_result->'detail'->>'integrity_failure')::boolean,false)
    and v_result->'detail'->>'disposition'='MANUAL_REVIEW'
    and coalesce((v_result->>'retryable')::boolean,true) is false
    and v_result->'detail'->>'recomputed_digest'
        is distinct from v_result->'detail'->>'stored_digest',
    'A1 controls 3 and 5: a tampered pending bundle must be refused as a permanent '
      ||'integrity failure routed to MANUAL_REVIEW, with the digest the coordinator '
      ||'recomputed from the stored request, got '||v_result::text);
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_heads)=v_heads_before,
    'D10: the refused release wrote nothing');
  -- A1 control 1: nobody but the owner may write either column.  Proved from the
  -- catalogue rather than asserted: no table privilege of any kind is held by a
  -- browser role, and the request/digest columns are outside the immutable-fact
  -- guard's lifecycle allowlist, which is control 4.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*)=0
       from information_schema.role_table_grants
      where table_schema='public'
        and table_name='weekly_source_pending_entitlement_bundles'
        and grantee in ('anon','authenticated','service_role','PUBLIC')),
    'A1 control 1: no browser role may write the pending bundle''s request or digest');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*)=1
       from pg_catalog.pg_trigger trigger_row
      where trigger_row.tgrelid='public.weekly_source_pending_entitlement_bundles'::regclass
        and not trigger_row.tgisinternal
        and trigger_row.tgname='weekly_source_immutable_fact_guard'
        and pg_catalog.pg_get_triggerdef(trigger_row.oid) not like '%request_json%'
        and pg_catalog.pg_get_triggerdef(trigger_row.oid) not like '%request_digest%'),
    'A1 control 4: request_json and request_digest are outside the lifecycle allowlist, '
      ||'so they are immutable after the accepted save');
end
$verify_publication_d10$;

-- ---------------------------------------------------------------------------
-- 17. WP-06 review finding F2 — a ROTATED family resolves by FAMILY, never TSFIN
-- ---------------------------------------------------------------------------
-- The finding: interface I-7 resolved the current head by the PHYSICAL
-- `root_timesheet_id`, so a family that rotated after its head was committed
-- was reported as "no head, authority TSFIN" — an UNDERSTATED effective
-- entitlement, which is the input shape that turns a residual into an
-- overpayment.  It is the same defect class as schema change S8: once
-- `unique (root_timesheet_id)` is gone, nothing keyed on the physical id is
-- unique, and the physical key and the family key can disagree.
--
-- The coordinator's own defence does not depend on I-7 at all: it resolves the
-- committed head by the FAMILY key first, and only consults I-7 when the family
-- genuinely has no head.  These cases execute that, so it cannot regress
-- silently whatever I-7 does.
do $verify_publication_rotated_family$
declare
  v_result jsonb;
  v_request jsonb;
  v_lock jsonb;
  v_effective jsonb;
  v_heads_before bigint;
  v_head_id uuid;
begin
  -- A family with ONE physical root, canonical, authorised, and head-less.
  insert into public.timesheets(
    timesheet_id,booking_id,version,is_current,status,sheet_scope,submission_mode,line_type,
    occupant_key_norm,hospital_norm,ward_norm,job_title_norm,shift_label_norm,
    week_ending_date,contract_id,actual_schedule_json,qr_payload_json,is_adjustment,
    created_at,updated_at
  ) values (
    'c0000000-0000-4000-8000-000000000056','WSPUB-0006',1,true,
    'RECEIVED'::public.timesheet_status_enum,'WEEKLY'::public.timesheet_scope_enum,
    'MANUAL'::public.submission_mode_enum,'HOURS'::public.timesheet_line_type_enum,
    'pub-occupant-f','pub-hospital','pub-ward','pub-role','weekly-0','2026-03-08',
    'c0000000-0000-4000-8000-000000000004','[]'::jsonb,'{}'::jsonb,false,
    pg_catalog.statement_timestamp(),pg_catalog.statement_timestamp());
  insert into public.contract_weeks(id,contract_id,week_ending_date,additional_seq,timesheet_id)
  values ('c0000000-0000-4000-8000-000000000055','c0000000-0000-4000-8000-000000000004',
          '2026-03-08',3,'c0000000-0000-4000-8000-000000000056');
  insert into public.timesheets_financials(
    timesheet_id,timesheet_version,candidate_id,is_current,processing_status
  ) values ('c0000000-0000-4000-8000-000000000056',1,'c0000000-0000-4000-8000-000000000003',
            true,'PENDING_AUTH'::public.ts_fin_processing_status_enum);
  insert into public.weekly_source_root_authorisations(
    root_timesheet_id,family_booking_id,timesheet_version,authorisation_generation,
    authorised_row_signature,authorised_by_user_id
  ) values ('c0000000-0000-4000-8000-000000000056','WSPUB-0006',1,1,
            'signature-f-generation-1','c0000000-0000-4000-8000-000000000001');

  v_lock:=pg_temp.lock_result(
    pg_catalog.jsonb_build_array('c0000000-0000-4000-8000-000000000056'),
    pg_catalog.jsonb_build_array('WSPUB-0006'),
    pg_catalog.jsonb_build_array(1));

  -- The FIRST head over that root.  There is no head yet, so the before-position
  -- is the one case where I-7 is consulted — and the empty declared position
  -- must equal the empty effective inventory the ordinary snapshot yields.
  v_request:=pg_catalog.jsonb_set(pg_catalog.jsonb_set(
      pg_temp.single_root_request(
        'c0000000-0000-4000-8000-0000000000f0',1,'c0000000-0000-4000-8000-0000000000f5',
        'c0000000-0000-4000-8000-0000000000f8',null,'[]'::jsonb,
        pg_catalog.jsonb_build_array(
          pg_temp.component(1,'c6c6c6c6-0000-4000-8000-000000000001','7.5','75.00'))),
      '{member_root_ids}','["c0000000-0000-4000-8000-000000000056"]'::jsonb),
      '{member_family_booking_ids}','["WSPUB-0006"]'::jsonb);
  perform pg_temp.mk_bundle(v_request);
  v_result:=private.weekly_source_entitlement_publish_core_v1(
    v_request,'IMMEDIATE',v_lock,null,null,null,'{}'::jsonb,'{}'::jsonb);
  perform pg_temp.assert_true((v_result->>'ok')::boolean,
    'rotation: the first head over a head-less canonical root must publish, got '
      ||v_result::text);
  -- Before the rotation the two keyings agree, so I-7 is the before-position
  -- source and the coordinator records that it was.
  perform pg_temp.assert_true(
    v_result->'before_position_source'=case
      when pg_catalog.to_regprocedure('private.weekly_source_effective_inventory_v1(uuid)')
           is not null then '["I7"]'::jsonb else '["DECLARED_UNPROVED"]'::jsonb end,
    'rotation: a head-less root takes its before-position from I-7 when I-7 exists, got '
      ||coalesce((v_result->'before_position_source')::text,'<null>'));
  v_head_id:='c0000000-0000-4000-8000-0000000000f5';

  -- ---- the rotation itself ------------------------------------------------
  -- A new physical Timesheet joins the SAME family and becomes canonical.  The
  -- committed head stays where it was written, on the old physical root.  The
  -- old row is demoted FIRST: `timesheets_booking_id_current_uidx` allows only
  -- one current Timesheet per booking, which is precisely why the family, not
  -- the physical id, is the identity.
  update public.timesheets set is_current=false
   where timesheet_id='c0000000-0000-4000-8000-000000000056';
  insert into public.timesheets(
    timesheet_id,booking_id,version,is_current,status,sheet_scope,submission_mode,line_type,
    occupant_key_norm,hospital_norm,ward_norm,job_title_norm,shift_label_norm,
    week_ending_date,contract_id,actual_schedule_json,qr_payload_json,is_adjustment,
    created_at,updated_at
  ) values (
    'c0000000-0000-4000-8000-000000000066','WSPUB-0006',2,true,
    'RECEIVED'::public.timesheet_status_enum,'WEEKLY'::public.timesheet_scope_enum,
    'MANUAL'::public.submission_mode_enum,'HOURS'::public.timesheet_line_type_enum,
    'pub-occupant-f','pub-hospital','pub-ward','pub-role','weekly-0','2026-03-08',
    'c0000000-0000-4000-8000-000000000004','[]'::jsonb,'{}'::jsonb,false,
    pg_catalog.statement_timestamp(),pg_catalog.statement_timestamp());
  insert into public.timesheets_financials(
    timesheet_id,timesheet_version,candidate_id,is_current,processing_status
  ) values ('c0000000-0000-4000-8000-000000000066',2,'c0000000-0000-4000-8000-000000000003',
            true,'PENDING_AUTH'::public.ts_fin_processing_status_enum);

  -- The family demonstrably HAS a committed head.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_heads as head_row
      where pg_catalog.btrim(head_row.root_family_booking_id)='WSPUB-0006'
        and head_row.state='COMMITTED_CURRENT')=1,
    'rotation: the rotated family still has exactly one committed current head');

  -- ---- F2, asserted on interface I-7 directly -----------------------------
  -- Keyed on the FAMILY, I-7 must never answer "no head, take the ordinary
  -- snapshot" for a family that has one.  It may answer HEAD, or it may refuse
  -- because the head names another physical root; `ok:true` with authority
  -- TSFIN is the defect and is the one answer forbidden here.
  if pg_catalog.to_regprocedure(
       'private.weekly_source_effective_inventory_v1(uuid)') is not null then
    v_effective:=private.weekly_source_effective_inventory_v1(
      'c0000000-0000-4000-8000-000000000066');
    perform pg_temp.assert_true(
      not (coalesce((v_effective->>'ok')::boolean,false)
           and v_effective->>'authority'='TSFIN'),
      'F2: I-7 must not report authority TSFIN for a rotated family that has a committed '
        ||'head, got '||v_effective::text);
    -- I-7 contract REVISION 2 (WP-06_DESIGN.md, 18 September 2026): this exact
    -- state is the new refusal row.  Asserted positively so a silent return to
    -- revision 1 is caught here and not only by the negative above.
    perform pg_temp.assert_true(
      (v_effective->>'ok')::boolean is false
      and v_effective->>'code'='WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE'
      and v_effective->'detail'->>'reason'='COMMITTED_HEAD_BELONGS_TO_ANOTHER_PHYSICAL_ROOT'
      and v_effective->'detail'->>'current_head_id' is not null
      and v_effective->'detail'->>'head_root_timesheet_id' is not null,
      'I-7 revision 2: the rotated family must produce the named refusal, got '
        ||v_effective::text);
    -- And the coordinator treats that refusal AS a refusal.  It never reaches
    -- I-7 in this state, because its own family-keyed guard fires first — which
    -- is the stronger position — but if it ever did, the `ok:false` branch
    -- returns WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE.  Both halves are asserted:
    -- the source shape here, the behaviour below.
    perform pg_temp.assert_true(
      pg_catalog.pg_get_functiondef(pg_catalog.to_regprocedure(
        'private.weekly_source_entitlement_publish_core_v1(jsonb,text,jsonb,uuid,text,uuid,jsonb,jsonb)'))
      ~ 'INTERFACE_I7_REFUSED_THE_ROOT',
      'the coordinator must treat an I-7 ok:false as a refusal, never as an absent head');
  end if;

  -- ---- and through the coordinator ----------------------------------------
  -- The publication now names the rotated-to canonical root.  The head is found
  -- by the FAMILY key, so the coordinator sees that the family's head sits on a
  -- different physical root and refuses.  It never reaches I-7, and it never
  -- takes a TSFIN before-position for a root that has a head.
  select pg_catalog.count(*) into v_heads_before from public.weekly_source_entitlement_heads;
  v_lock:=pg_temp.lock_result(
    pg_catalog.jsonb_build_array('c0000000-0000-4000-8000-000000000066'),
    pg_catalog.jsonb_build_array('WSPUB-0006'),
    pg_catalog.jsonb_build_array(2));
  v_request:=pg_catalog.jsonb_set(pg_catalog.jsonb_set(pg_catalog.jsonb_set(
      pg_temp.single_root_request(
        'c0000000-0000-4000-8000-0000000000f9',1,'c0000000-0000-4000-8000-0000000000fc',
        'c0000000-0000-4000-8000-0000000000fd',v_head_id,
        pg_catalog.jsonb_build_array('c6c6c6c6-0000-4000-8000-000000000001'),
        pg_catalog.jsonb_build_array(
          pg_temp.component(1,'c6c6c6c6-0000-4000-8000-000000000001','7.5','75.00'))),
      '{member_root_ids}','["c0000000-0000-4000-8000-000000000066"]'::jsonb),
      '{member_family_booking_ids}','["WSPUB-0006"]'::jsonb),
      '{member_root_versions}','[2]'::jsonb);
  perform pg_temp.mk_bundle(v_request);
  v_result:=private.weekly_source_entitlement_publish_core_v1(
    v_request,'IMMEDIATE',v_lock,null,null,null,'{}'::jsonb,'{}'::jsonb);
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->>'code'='WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE'
    and v_result->'detail'->>'reason'='COMMITTED_HEAD_BELONGS_TO_ANOTHER_PHYSICAL_ROOT',
    'F2: a rotated family whose head sits on the old physical root must fail closed, never '
      ||'fall back to TSFIN, got '||v_result::text);
  -- The specific thing the orchestrator asked to be impossible: a rotated family
  -- that HAS a head must never be recorded as an unproved or TSFIN
  -- before-position.  A refusal carries no before_position_source at all, and
  -- most certainly not DECLARED_UNPROVED.
  perform pg_temp.assert_true(
    v_result->'before_position_source' is null
    and coalesce((v_result->'before_position_source')::text,'') !~ 'DECLARED_UNPROVED'
    and coalesce((v_result->'before_position_source')::text,'') !~ 'I7',
    'F2: a rotated family with a head must never record an unproved or I-7 before-position, got '
      ||coalesce((v_result->'before_position_source')::text,'<none>'));
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_heads)=v_heads_before,
    'F2: the refused rotated publication wrote nothing');

  -- ---- the mirror: the physical root has a head, the declared family has not -
  -- The same disagreement running the other way.  `WSPUB-0007` is a real,
  -- separate family whose canonical root is the very Timesheet that carries
  -- WSPUB-0006's head, which is only possible because S8 removed the physical
  -- uniqueness.  The family key finds nothing, so without the mirror guard the
  -- coordinator would take an I-7 or unproved before-position for a root that
  -- demonstrably has a committed head.
  -- The root keeps its existing live authorisation: the mirror guard fires in
  -- the head-resolution loop, long before the authorisation and pointer checks,
  -- which is the point — the disagreement is caught at the earliest moment it
  -- can be seen.
  update public.timesheets set booking_id='WSPUB-0007',is_current=true
   where timesheet_id='c0000000-0000-4000-8000-000000000056';
  v_lock:=pg_temp.lock_result(
    pg_catalog.jsonb_build_array('c0000000-0000-4000-8000-000000000056'),
    pg_catalog.jsonb_build_array('WSPUB-0007'),
    pg_catalog.jsonb_build_array(1));
  v_request:=pg_catalog.jsonb_set(pg_catalog.jsonb_set(
      pg_temp.single_root_request(
        'c0000000-0000-4000-8000-0000000000fe',1,'c0000000-0000-4000-8000-0000000000ff',
        'c0000000-0000-4000-8000-0000000000f1',null,'[]'::jsonb,
        pg_catalog.jsonb_build_array(
          pg_temp.component(1,'c7c7c7c7-0000-4000-8000-000000000001','1.0','10.00'))),
      '{member_root_ids}','["c0000000-0000-4000-8000-000000000056"]'::jsonb),
      '{member_family_booking_ids}','["WSPUB-0007"]'::jsonb);
  perform pg_temp.mk_bundle(v_request);
  v_result:=private.weekly_source_entitlement_publish_core_v1(
    v_request,'IMMEDIATE',v_lock,null,null,null,'{}'::jsonb,'{}'::jsonb);
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->>'code'='WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE'
    and v_result->'detail'->>'reason'
        ='COMMITTED_HEAD_EXISTS_FOR_THE_PHYSICAL_ROOT_BUT_NOT_THE_DECLARED_FAMILY',
    'F2 mirror: a physical root that carries a committed head must never take an I-7 or '
      ||'unproved before-position, got '||v_result::text);
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_heads)=v_heads_before,
    'F2 mirror: the refused publication wrote nothing');
end
$verify_publication_rotated_family$;

-- ---------------------------------------------------------------------------
-- 18. WP-07 review finding F2 — a rotated B family is never authorised twice
-- ---------------------------------------------------------------------------
-- The finding: interface I-6's already-authorised test is per PHYSICAL root, so
-- a family whose NON-CANONICAL member already carries a live generation can be
-- authorised again.  The root then holds TWO live generations: it is payable,
-- and every withdrawal path treats more than one as impossible, so it can never
-- be withdrawn.
--
-- The coordinator does not do I-6's context read, so it cannot inherit I-6's
-- fix.  It makes the family-wide test itself, from the member list the lock
-- result carries and from the family booking id the authorisation row records.
-- This case proves the protection from the coordinator's own entry point, so it
-- holds whether or not WP-07b's widening is installed.
do $verify_publication_rotated_b_authorisation$
declare
  v_result jsonb;
  v_request jsonb;
  v_lock jsonb;
  v_a_head uuid;
  v_heads_before bigint;
  v_authorisations_before bigint;
begin
  -- Family WSPUB-0008: an OLD physical member that is already authorised, and a
  -- NEW canonical member that is not.  The canonical member is provably blank,
  -- so nothing but the family-wide test stands between it and a second live
  -- generation.
  insert into public.timesheets(
    timesheet_id,booking_id,version,is_current,status,sheet_scope,submission_mode,line_type,
    occupant_key_norm,hospital_norm,ward_norm,job_title_norm,shift_label_norm,
    week_ending_date,contract_id,actual_schedule_json,qr_payload_json,is_adjustment,
    created_at,updated_at
  ) values (
    'c0000000-0000-4000-8000-000000000076','WSPUB-0008',1,true,
    'RECEIVED'::public.timesheet_status_enum,'WEEKLY'::public.timesheet_scope_enum,
    'MANUAL'::public.submission_mode_enum,'HOURS'::public.timesheet_line_type_enum,
    'pub-occupant-h','pub-hospital','pub-ward','pub-role','weekly-0','2026-03-08',
    'c0000000-0000-4000-8000-000000000014','[]'::jsonb,'{}'::jsonb,false,
    pg_catalog.statement_timestamp(),pg_catalog.statement_timestamp());
  -- The OLD member is the one that is authorised.
  insert into public.weekly_source_root_authorisations(
    root_timesheet_id,family_booking_id,timesheet_version,authorisation_generation,
    authorised_row_signature,authorised_by_user_id
  ) values ('c0000000-0000-4000-8000-000000000076','WSPUB-0008',1,1,
            'signature-h-generation-1','c0000000-0000-4000-8000-000000000001');
  -- Then the family rotates: the old member steps down, a new canonical one
  -- appears, and it carries no authorisation of its own.
  update public.timesheets set is_current=false
   where timesheet_id='c0000000-0000-4000-8000-000000000076';
  insert into public.timesheets(
    timesheet_id,booking_id,version,is_current,status,sheet_scope,submission_mode,line_type,
    occupant_key_norm,hospital_norm,ward_norm,job_title_norm,shift_label_norm,
    week_ending_date,contract_id,actual_schedule_json,qr_payload_json,is_adjustment,
    created_at,updated_at
  ) values (
    'c0000000-0000-4000-8000-000000000086','WSPUB-0008',2,true,
    'RECEIVED'::public.timesheet_status_enum,'WEEKLY'::public.timesheet_scope_enum,
    'MANUAL'::public.submission_mode_enum,'HOURS'::public.timesheet_line_type_enum,
    'pub-occupant-h','pub-hospital','pub-ward','pub-role','weekly-0','2026-03-08',
    'c0000000-0000-4000-8000-000000000014','[]'::jsonb,'{}'::jsonb,false,
    pg_catalog.statement_timestamp(),pg_catalog.statement_timestamp());
  insert into public.contract_weeks(id,contract_id,week_ending_date,additional_seq,timesheet_id)
  values ('c0000000-0000-4000-8000-000000000085','c0000000-0000-4000-8000-000000000004',
          '2026-03-08',5,'c0000000-0000-4000-8000-000000000086');
  insert into public.timesheets_financials(
    timesheet_id,timesheet_version,candidate_id,is_current,processing_status
  ) values ('c0000000-0000-4000-8000-000000000086',2,'c0000000-0000-4000-8000-000000000003',
            true,'PENDING_AUTH'::public.ts_fin_processing_status_enum);

  -- The canonical member really is blank and really is unauthorised, so the
  -- ONLY thing that can refuse this is the family-wide test.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_root_authorisations
      where root_timesheet_id='c0000000-0000-4000-8000-000000000086'
        and withdrawn_at_utc is null)=0,
    'WP-07 F2: the canonical member carries no live generation of its own');
  -- WP-30 supersession, stated in place rather than implied.  This setup
  -- assertion pinned the PRE-WP-30 truth: private.weekly_source_publication_
  -- target_root_blank_v1 keyed its authorisation and lineage limbs on the
  -- PHYSICAL root id, so a canonical member whose FAMILY is already authorised
  -- was reported "provably blank" — while the head limb beside them was
  -- already keyed on the family booking id.  Standing rule 3 forbids keying on
  -- a physical root id alone after schema change S8, and "provably blank" is
  -- precisely a statement about the family, so WP-30 moved both limbs onto the
  -- one installed resolver adapter.
  --
  -- The premise this assertion exists to establish is UNCHANGED and is proved
  -- by the assertion immediately above: the canonical member carries no live
  -- generation of its own, so the refusal proved below can still only come
  -- from publish_core's own family-wide authorisation test, which runs before
  -- the blank check.  The assertion is restated as the new truth and made
  -- stronger: the blank check must now REPORT the family-derived reason.
  perform pg_temp.assert_true(
    (private.weekly_source_publication_target_root_blank_v1(
       'c0000000-0000-4000-8000-000000000086','WSPUB-0008')->>'blank')::boolean is false
    and (private.weekly_source_publication_target_root_blank_v1(
       'c0000000-0000-4000-8000-000000000086','WSPUB-0008')->'reasons')
        @> pg_catalog.jsonb_build_array('HAS_A_ROOT_AUTHORISATION_GENERATION'),
    'WP-30/WP-07 F2: the canonical member is NOT blank, because its family holds a live '
      ||'authorisation generation on the demoted member; got '
      ||(private.weekly_source_publication_target_root_blank_v1(
           'c0000000-0000-4000-8000-000000000086','WSPUB-0008'))::text);

  -- A (WSPUB-0001) has been emptied by the earlier sections, and an A-to-B
  -- bundle needs something to move.  Give A one component back through a normal
  -- successor publication, so the bundle below is a genuine move and is refused
  -- on the authorisation rule alone.
  select head_row.id into v_a_head
    from public.weekly_source_entitlement_heads as head_row
   where head_row.root_timesheet_id='c0000000-0000-4000-8000-000000000006'
     and head_row.state='COMMITTED_CURRENT';
  v_request:=pg_temp.single_root_request(
    'c0000000-0000-4000-8000-0000000000ea',1,'c0000000-0000-4000-8000-0000000000eb',
    'c0000000-0000-4000-8000-0000000000ec',v_a_head,'[]'::jsonb,
    pg_catalog.jsonb_build_array(
      pg_temp.component(1,'c8c8c8c8-0000-4000-8000-000000000001','7.5','75.00')));
  perform pg_temp.mk_bundle(v_request);
  v_result:=private.weekly_source_entitlement_publish_core_v1(
    v_request,'IMMEDIATE',pg_temp.lock_result_a(),null,null,null,'{}'::jsonb,'{}'::jsonb);
  perform pg_temp.assert_true((v_result->>'ok')::boolean,
    'WP-07 F2 setup: A must regain a component to move, got '||v_result::text);

  select pg_catalog.count(*) into v_heads_before from public.weekly_source_entitlement_heads;
  select pg_catalog.count(*) into v_authorisations_before
    from public.weekly_source_root_authorisations;

  -- The lock result carries BOTH physical members of the family, which is the
  -- member list WP-07b widens interface I-6 to read.  The coordinator reads it
  -- for itself.
  v_lock:=pg_catalog.jsonb_build_object('ok',true,'gate','GRANTED',
    'families',pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object(
        'requested_timesheet_id','c0000000-0000-4000-8000-000000000006',
        'family_booking_id','WSPUB-0001',
        'canonical_timesheet_id','c0000000-0000-4000-8000-000000000006',
        'canonical_version',1,'requested_is_canonical',true,'family_is_current',true,
        'member_timesheet_ids',pg_catalog.jsonb_build_array(
          'c0000000-0000-4000-8000-000000000006')),
      pg_catalog.jsonb_build_object(
        'requested_timesheet_id','c0000000-0000-4000-8000-000000000086',
        'family_booking_id','WSPUB-0008',
        'canonical_timesheet_id','c0000000-0000-4000-8000-000000000086',
        'canonical_version',2,'requested_is_canonical',true,'family_is_current',true,
        'member_timesheet_ids',pg_catalog.jsonb_build_array(
          'c0000000-0000-4000-8000-000000000076','c0000000-0000-4000-8000-000000000086'))));

  v_request:=pg_temp.ab_request('wp07f2','c0000000-0000-4000-8000-000000000086','WSPUB-0008',
    pg_catalog.jsonb_build_object(
      'timesheet_id','c0000000-0000-4000-8000-000000000086',
      'expected_row_signature',
      pg_temp.row_signature('c0000000-0000-4000-8000-000000000086','signature-h'),
      'actor_user_id','c0000000-0000-4000-8000-000000000001'),
    null,null,2);
  v_result:=private.weekly_source_entitlement_publish_core_v1(
    v_request,'IMMEDIATE',v_lock,null,null,null,'{}'::jsonb,'{}'::jsonb);
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->>'code'='WEEKLY_SOURCE_PUBLICATION_TARGET_ALREADY_AUTHORISED',
    'WP-07 F2: a rotated family whose non-canonical member is already authorised must never '
      ||'be authorised a second time, got '||v_result::text);
  -- The refusal is made on the FAMILY, and says so: the physical root really
  -- does carry none of its own.
  perform pg_temp.assert_true(
    (v_result->'detail'->>'live_generations')::integer=1
    and (v_result->'detail'->>'live_generations_on_this_physical_root')::integer=0,
    'WP-07 F2: the refusal is the family-wide count, not the physical one, got '
      ||coalesce((v_result->'detail')::text,'<null>'));
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_root_authorisations)
      =v_authorisations_before
    and (select pg_catalog.count(*) from public.weekly_source_entitlement_heads)=v_heads_before,
    'WP-07 F2: no second live generation and no head were written');
  -- And the state the finding is really about never arises.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_root_authorisations
      where pg_catalog.btrim(family_booking_id)='WSPUB-0008'
        and withdrawn_at_utc is null)=1,
    'WP-07 F2: the family still holds exactly one live generation, so it stays withdrawable');
end
$verify_publication_rotated_b_authorisation$;

-- ---------------------------------------------------------------------------
-- 19. The Candidate identifier comes from `contracts`, and a null fails closed
-- ---------------------------------------------------------------------------
-- WP-06 fixed the same class on its own side: a null Candidate used to be
-- carried into the serial gate and the lock set, which is the mechanism that
-- stops two concurrent publications for one Candidate — a null there silently
-- disarms it.  The coordinator's position is asserted rather than assumed.
do $verify_publication_candidate_identity$
declare
  v_result jsonb;
  v_request jsonb;
  v_jobs_before bigint;
begin
  -- (a) A null Candidate is refused by the canonicaliser, BEFORE the serial
  --     gate, the locks, the census and interface I-5 are touched at all.
  select pg_catalog.count(*) into v_jobs_before from public.banking_pay_workbench_jobs;
  v_request:=pg_catalog.jsonb_set(
    pg_temp.single_root_request(
      'c0000000-0000-4000-8000-0000000000e1',1,'c0000000-0000-4000-8000-0000000000e3',
      'c0000000-0000-4000-8000-0000000000e4',null,'[]'::jsonb,'[]'::jsonb),
    '{candidate_id}','null'::jsonb);
  v_result:=private.weekly_source_entitlement_publish_immediate_v1(v_request);
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->>'code'='WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'
    and v_result->'detail'->>'field'='candidate_id'
    and v_result->'detail'->>'reason'='NULL_NOT_ALLOWED',
    'a null Candidate must be refused by the canonicaliser, got '||v_result::text);
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.banking_pay_workbench_jobs)=v_jobs_before,
    'a null Candidate must never reach the serial gate or queue a job');

  -- (b) A Candidate that does not own the Contract the request chose is refused
  --     against `public.contracts` itself — the same relation WP-06 now reads
  --     the Candidate from — so the two can never disagree.
  v_request:=pg_temp.single_root_request(
    'c0000000-0000-4000-8000-0000000000e5',1,'c0000000-0000-4000-8000-0000000000e8',
    'c0000000-0000-4000-8000-0000000000e9',null,'[]'::jsonb,'[]'::jsonb,
    'c0000000-0000-4000-8000-000000000023');
  perform pg_temp.mk_bundle(v_request);
  v_result:=private.weekly_source_entitlement_publish_core_v1(
    v_request,'IMMEDIATE',pg_temp.lock_result_a(),null,null,null,'{}'::jsonb,'{}'::jsonb);
  perform pg_temp.assert_true(
    (v_result->>'ok')::boolean is false
    and v_result->'detail'->>'reason'='CANDIDATE_DOES_NOT_OWN_THE_ROOT',
    'the Candidate must own the Contract the request chose, proved against public.contracts, got '
      ||v_result::text);
end
$verify_publication_candidate_identity$;

-- ---------------------------------------------------------------------------
-- 15. The "provably blank" target root is decided on CONTENT, not on timing
--     (WP-06c review observation O2, reproduced and fixed by WP-02b)
-- ---------------------------------------------------------------------------
do $verify_publication_target_blank_content$
declare
  v_blank_empty jsonb;
  v_blank_loaded jsonb;
  v_i7_is_real boolean;
begin
  -- Two roots that differ in one thing only: what their current financial
  -- snapshot CARRIES.  Both are created in this transaction, which is what the
  -- old timing test waved through unconditionally.
  insert into public.timesheets(
    timesheet_id,booking_id,version,is_current,status,sheet_scope,submission_mode,line_type,
    occupant_key_norm,hospital_norm,ward_norm,job_title_norm,shift_label_norm,
    week_ending_date,contract_id,actual_schedule_json,qr_payload_json,is_adjustment,
    created_at,updated_at
  ) values
   ('c0000000-0000-4000-8000-000000000201','WSPUB-0201',1,true,
    'RECEIVED'::public.timesheet_status_enum,'WEEKLY'::public.timesheet_scope_enum,
    'MANUAL'::public.submission_mode_enum,'HOURS'::public.timesheet_line_type_enum,
    'pub-occupant-n1','pub-hospital','pub-ward','pub-role','weekly-0','2026-03-08',
    'c0000000-0000-4000-8000-000000000014','[]'::jsonb,'{}'::jsonb,false,
    pg_catalog.statement_timestamp(),pg_catalog.statement_timestamp()),
   ('c0000000-0000-4000-8000-000000000202','WSPUB-0202',1,true,
    'RECEIVED'::public.timesheet_status_enum,'WEEKLY'::public.timesheet_scope_enum,
    'MANUAL'::public.submission_mode_enum,'HOURS'::public.timesheet_line_type_enum,
    'pub-occupant-n2','pub-hospital','pub-ward','pub-role','weekly-0','2026-03-08',
    'c0000000-0000-4000-8000-000000000014','[]'::jsonb,'{}'::jsonb,false,
    pg_catalog.statement_timestamp(),pg_catalog.statement_timestamp());
  insert into public.contract_weeks(id,contract_id,week_ending_date,additional_seq,timesheet_id)
  values ('c0000000-0000-4000-8000-000000000203','c0000000-0000-4000-8000-000000000014',
          '2026-03-08',201,'c0000000-0000-4000-8000-000000000201'),
         ('c0000000-0000-4000-8000-000000000204','c0000000-0000-4000-8000-000000000014',
          '2026-03-08',202,'c0000000-0000-4000-8000-000000000202');
  insert into public.timesheets_financials(
    timesheet_id,timesheet_version,candidate_id,is_current,processing_status,
    invoice_breakdown_json
  ) values
   ('c0000000-0000-4000-8000-000000000201',1,'c0000000-0000-4000-8000-000000000003',true,
    'PENDING_AUTH'::public.ts_fin_processing_status_enum,'{}'::jsonb),
   ('c0000000-0000-4000-8000-000000000202',1,'c0000000-0000-4000-8000-000000000003',true,
    'PENDING_AUTH'::public.ts_fin_processing_status_enum,
    pg_catalog.jsonb_build_object('segments',pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object(
        'segment_id','weekly-source-event:wspub-0202','date','2026-03-06',
        'ref_num','WSPUB-0202','hours_day',9,'pay_amount',180.00,'charge_amount',360.00,
        'exclude_from_pay',false,
        'weekly_source',pg_catalog.jsonb_build_object(
          'work_event_id','wspub-0202','calculation_fingerprint','wspub-calc-0202')))));

  v_blank_empty:=private.weekly_source_publication_target_root_blank_v1(
    'c0000000-0000-4000-8000-000000000201','WSPUB-0201');
  v_blank_loaded:=private.weekly_source_publication_target_root_blank_v1(
    'c0000000-0000-4000-8000-000000000202','WSPUB-0202');

  -- A root whose only mark on the world is the empty current snapshot that the
  -- ordinary Authorise owner requires is genuinely new.  The old test said this
  -- only inside the creating transaction, which made a retryable
  -- `WEEKLY_SOURCE_CANDIDATE_BUSY` or a `DEFERRED` release demand a whole-root
  -- Office review the accepted decision could not carry (O2).
  perform pg_temp.assert_true(
    (v_blank_empty->>'blank')::boolean,
    'O2: a root with an EMPTY current snapshot must be provably blank, got '
      ||v_blank_empty::text);

  -- Whether the CONTENT half can be asserted here depends on the real interface
  -- I-7 being installed: the TEST-DOUBLE above always answers "empty".  Ask it.
  v_i7_is_real:=coalesce((
    private.weekly_source_effective_inventory_v1(
      'c0000000-0000-4000-8000-000000000202')->>'component_count')::integer,0)>0;
  if v_i7_is_real then
    perform pg_temp.assert_true(
      (v_blank_loaded->>'blank')::boolean is false
      and v_blank_loaded->'reasons' ? 'HAS_A_CURRENT_FINANCIAL_SNAPSHOT',
      'O2: a root whose current snapshot carries unrelated work is NOT blank, '
        ||'even when the row was written in this transaction, got '||v_blank_loaded::text);
  else
    raise notice 'O2 content half skipped: interface I-7 is the TEST-DOUBLE here';
  end if;
end
$verify_publication_target_blank_content$;

select pg_catalog.jsonb_build_object(
  'ok',true,
  'verification','weekly_source_entitlement_publication_v1',
  'scenarios',pg_catalog.jsonb_build_array(
    'R7','R8','R9','R10','R11','R12','R25-single-root','R25-all-authorised','R39',
    'H2-024','H2-032','H2-035','H2-036','H2-038',
    'immediate-entry-RELEASABLE','immediate-entry-FROZEN','serial-gate-BLOCKED',
    'new-B-root-through-I-6',
    'D8-root-authorisation-pointer','D9-I7-before-position','ruling6-possibility-one',
    'ruling6-possibility-two','D10-stored-request-and-digest-verified-together',
    'rotated-family-before-position-from-the-head-never-TSFIN',
    'rotated-B-family-never-authorised-twice',
    'candidate-identifier-from-contracts-and-null-fails-closed',
    'moved-component-repriced-is-refused-before-any-write',
    'target-root-blankness-decided-on-content-not-on-the-transaction'),
  'review_findings_covered',pg_catalog.jsonb_build_array(
    'U1-accepted-decision-binds-the-publication',
    'U2-target-root-provably-blank-and-a-real-recorded-review',
    'U3a-retained-components-byte-identical',
    'U3b-before-position-from-I7-or-fail-closed',
    'U4-replay-first-in-the-immediate-entry-point',
    'U5-deferred-census-and-pending-bundle',
    'F6-locale-independent-digest','F7-committed-bundle-revision',
    'F8-candidate-count-fails-closed','F9-no-numeric-formatting-collision',
    'F10-malformed-values-returned-not-raised','F12-target-component-dropped-reachable',
    'F14-I6-silent-ok-reaches-the-pointer-guard',
    'WP-08b-N4-no-untyped-array-concatenation',
    'N9-whole-root-review-persisted-on-the-accepted-decision',
    'WP-06-F2-rotated-family-resolves-by-family-not-physical-root',
    'WP-07-F2-already-authorised-is-a-family-test-not-a-physical-one',
    'WP-06c-F1-moved-component-content-verified-against-the-source-authority',
    'WP-06c-O2-target-root-blankness-is-content-not-timing',
    'R5-A1-control-1-only-the-owner-writes-the-request-and-its-digest',
    'R5-A1-control-2-the-coordinator-digests-and-verifies-under-the-save-lock',
    'R5-A1-control-3-release-locks-recalculates-and-refuses-before-effects',
    'R5-A1-control-4-request-and-digest-immutable-after-the-accepted-save',
    'R5-A1-control-5-conflicting-replay-and-tampered-row-go-to-manual-review',
    'R5-A1-control-6-these-checks-are-proved-by-this-verifier-before-activation'),
  'interfaces_used',pg_catalog.jsonb_build_object(
    'I-1','the installed WP-03 helper when present, otherwise hand-built lock results for the core',
    'I-2','the installed WP-08a census, through the immediate entry point',
    'I-5','rollback-only TEST-DOUBLE when WP-08b is absent',
    'I-6','the installed WP-07 owner when present, otherwise a rollback-only TEST-DOUBLE'),
  'banking_pay_rows_written',0,
  'banking_pay_definitions_changed',0
) as result;

rollback;

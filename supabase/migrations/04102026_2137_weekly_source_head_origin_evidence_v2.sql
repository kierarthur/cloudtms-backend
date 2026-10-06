-- JOINT-CONTRACT-V4 I1/I2 exact original HEAD provenance. Both coordinators
-- approved this finite storage seam on 4 October 2026. No financial authority
-- is manufactured and no historical row is backfilled from current truth.

\set ON_ERROR_STOP on

begin;

alter table public.weekly_source_entitlement_heads
  add column source_origin_json jsonb;
alter table public.weekly_source_entitlement_heads
  add constraint weekly_source_head_origin_shape_v2 check (
    source_origin_json is null or (
      jsonb_typeof(source_origin_json)='object'
      and octet_length(source_origin_json::text)<=32768
    )
  );
comment on column public.weekly_source_entitlement_heads.source_origin_json is
  'Exact immutable canonical financial_request.source_revision at genuine HEAD allocation. Hash equals existing source_generation_digest. NULL legacy evidence is unavailable, never a guessed Final or automatic zero.';

commit;

select jsonb_build_object('signatures',jsonb_agg(jsonb_build_object('signature',wanted.signature,
 'present',p.oid is not null,'definition_md5',case when p.oid is null then null else md5(pg_get_functiondef(p.oid)) end,
 'owner',case when p.oid is null then null else pg_get_userbyid(p.proowner) end,'acl',p.proacl::text) order by wanted.signature))
from (values ('private.candidate_workflow_factual_snapshot_assert_v1(jsonb)')) wanted(signature)
left join pg_proc p on p.oid=to_regprocedure(wanted.signature);

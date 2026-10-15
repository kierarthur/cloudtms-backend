import fs from'node:fs';import cp from'node:child_process';import assert from'node:assert/strict';import{createHash}from'node:crypto';
import{scanManagedSql}from'../scripts/cloudtms-managed-release-sql.mjs';import{mapExecutableSqlSource,logicalPostgresExecutablePolicy}from'../scripts/cloudtms-db-release-lib.mjs';
const pin='d48f05205ae9ce066762be8a307d87b304ce71da',base='docs/healthroster-build/proposals/a0-source-policy-selector-successor-20261015/forward/',dir='.codex-tmp/source-arbiter-native-kernel-20261015',sha=b=>createHash('sha256').update(b).digest('hex');assert(!fs.existsSync(dir));
const inputs=[],read=f=>{const p=base+f,b=fs.readFileSync(p),g=cp.execFileSync('git',['cat-file','blob',pin+':'+p]);assert.deepEqual(b,g);inputs.push({file:p,commit:pin,bytes:b.length,raw_sha256:sha(b)});return b.toString().replaceAll('\r\n','\n');};
const schema=read('01_complete_same_arbiter_schema.sqlinc'),grammar=read('00_source_token_grammar.sqlinc'),scalar=read('02_complete_scalar_helpers.sqlinc'),coverage=read('03_coverage_and_compatibility_acquire.sqlinc');
const scalarName='private.timesheet_correction_a0_fence_scalar_acquire_core_v1';const selected=scanManagedSql(scalar).statements.map(x=>scalar.slice(x.start,x.end)).filter(s=>s.includes(scalarName+'(')&&/^(?:create or replace function|alter function|revoke all on function)\b/i.test(s.trim()));assert.equal(selected.length,3);assert(selected[0].startsWith('create or replace function '+scalarName+'('));
const names=['private.timesheet_correction_a0_source_token_assert_v1(jsonb)',scalarName+'(jsonb,boolean)','private.timesheet_correction_a0_source_coverage_acquire_core_v1(text,uuid,daterange,boolean)','private.timesheet_correction_a0_source_coverage_acquire_v1(uuid,daterange,boolean)','private.timesheet_correction_a0_fence_acquire_v1(jsonb,boolean)'];
const declarations=schema+'\n'+grammar+'\n'+selected.join('\n')+'\n'+coverage;let mapped,policy;const taskPriorOwnerMode=process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER;try{process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER='CURRENT_USER';policy=logicalPostgresExecutablePolicy();mapped=mapExecutableSqlSource(declarations,base+'NATIVE_KERNEL_SELECTION.sqlinc');}finally{if(taskPriorOwnerMode===undefined)delete process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER;else process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER=taskPriorOwnerMode;}
assert.equal((mapped.match(/OWNER TO CURRENT_USER/g)||[]).length,6);assert(!/owner\s+to\s+postgres\s*;/i.test(mapped));
const vacancy=`do $vacancy$ begin if to_regclass('private.timesheet_correction_a0_transaction_fences') is not null or exists(select 1 from unnest(array[${names.map(s=>"'"+s+"'").join(',')}]) id where to_regprocedure(id) is not null) then raise exception 'HR_A0_KERNEL_NATIVE_PREREQUISITE_NOT_VACANT';end if;end $vacancy$;`;
const checks=`
set local lock_timeout='1379ms';
do $kernel$
declare c uuid:='62d7638d-5bbb-4549-b4dc-082d3b1e3276';other_c uuid:='23e2a7ba-ad08-4c1c-aa81-784ff23a6310';n integer;caught boolean;before_touch jsonb;t jsonb;
begin
 -- Identical retained ranges touch exactly one row; overlap is not unioned.
 perform private.timesheet_correction_a0_fence_acquire_v1(jsonb_build_array(jsonb_build_object('kind','SOURCE_CONTRACT_COVERAGE','uuid',c,'coverage',jsonb_build_array('2026-09-01','2026-09-07'))),true);
 perform private.timesheet_correction_a0_fence_acquire_v1(jsonb_build_array(jsonb_build_object('kind','SOURCE_CONTRACT_COVERAGE','uuid',c,'coverage',jsonb_build_array('2026-09-01','2026-09-07'))),true);
 if (select count(*) from private.timesheet_correction_a0_transaction_fences)<>1 then raise exception 'HR_A0_KERNEL_RETAINED_DEDUP_FAILED';end if;
 perform private.timesheet_correction_a0_fence_acquire_v1(jsonb_build_array(jsonb_build_object('kind','SOURCE_CONTRACT_COVERAGE','uuid',c,'coverage',jsonb_build_array('2026-09-03','2026-09-09'))),true);
 if (select count(*) from private.timesheet_correction_a0_transaction_fences)<>2 then raise exception 'HR_A0_KERNEL_RETAINED_RANGES_MERGED';end if;
 perform private.timesheet_correction_a0_fence_acquire_v1(jsonb_build_array(jsonb_build_object('kind','SOURCE_POLICY_MEMBERSHIP_COVERAGE','uuid',c,'coverage',jsonb_build_array('2026-09-01','2026-09-07'))),true);
 if (select count(*) from private.timesheet_correction_a0_transaction_fences)<>3 then raise exception 'HR_A0_KERNEL_RANGE_DOMAIN_COLLISION';end if;
 select jsonb_agg(to_jsonb(f) order by f.token_kind,f.coverage_span) into before_touch from private.timesheet_correction_a0_transaction_fences f;
 -- Ordinary overlap acquires existing rows without rewriting retained scopes.
 perform private.timesheet_correction_a0_fence_acquire_v1(jsonb_build_array(jsonb_build_object('kind','SOURCE_CONTRACT_COVERAGE','uuid',c,'coverage',jsonb_build_array('2026-09-04','2026-09-05'))),false);
 if (select jsonb_agg(to_jsonb(f) order by f.token_kind,f.coverage_span) from private.timesheet_correction_a0_transaction_fences f) is distinct from before_touch then raise exception 'HR_A0_KERNEL_ORDINARY_CHANGED_RETAINED';end if;
 -- Disjoint native date or Client leaves no ordinary intent row behind.
 perform private.timesheet_correction_a0_fence_acquire_v1(jsonb_build_array(jsonb_build_object('kind','SOURCE_CONTRACT_COVERAGE','uuid',c,'coverage',jsonb_build_array('2026-10-01','2026-10-07'))),false);
 perform private.timesheet_correction_a0_fence_acquire_v1(jsonb_build_array(jsonb_build_object('kind','SOURCE_CONTRACT_COVERAGE','uuid',other_c,'coverage',jsonb_build_array('2026-09-01','2026-09-07'))),false);
 if (select count(*) from private.timesheet_correction_a0_transaction_fences)<>3 or exists(select 1 from private.timesheet_correction_a0_transaction_fences where not retained) then raise exception 'HR_A0_KERNEL_ORDINARY_INTENT_LEAK';end if;
 -- Scalar dispatch uses the exact existing acquisition core in the same table.
 t:=jsonb_build_array(jsonb_build_object('kind','SOURCE_GLOBAL_SETTINGS','json','true'),jsonb_build_object('kind','SOURCE_OBJECT','json',jsonb_build_array('public.contracts',c)::text));
 perform private.timesheet_correction_a0_fence_acquire_v1(t,true);perform private.timesheet_correction_a0_fence_acquire_v1(t,true);
 if (select count(*) from private.timesheet_correction_a0_transaction_fences)<>5 then raise exception 'HR_A0_KERNEL_SCALAR_DISPATCH_FAILED';end if;
 -- Invalid mixed envelopes must refuse before any earlier token is admitted.
 caught:=false;begin
  perform private.timesheet_correction_a0_fence_acquire_v1(jsonb_build_array(jsonb_build_object('kind','SOURCE_OBJECT','json',jsonb_build_array('public.contracts',other_c)::text),jsonb_build_object('kind','SOURCE_POLICY_MEMBERSHIP_COVERAGE','uuid',c,'coverage',jsonb_build_array('2026-09-01'))),true);
 exception when sqlstate '55000' then caught:=true;end;
 if not caught or (select count(*) from private.timesheet_correction_a0_transaction_fences)<>5 then raise exception 'HR_A0_KERNEL_PARTIAL_INVALID_ADMISSION';end if;
 caught:=false;begin perform private.timesheet_correction_a0_source_coverage_acquire_core_v1('FOREIGN_KIND',c,daterange('2026-09-01','2026-09-07','[]'),true);exception when sqlstate '55000' then caught:=true;end;
 if not caught then raise exception 'HR_A0_KERNEL_KIND_WHITELIST_FAILED';end if;
 if current_setting('lock_timeout')<>'1379ms' then raise exception 'HR_A0_KERNEL_TIMEOUT_NOT_RESTORED';end if;
 if exists(select 1 from unnest(array[${names.map(s=>"'"+s+"'").join(',')}]) id cross join unnest(array['anon','authenticated','service_role']) r where has_function_privilege(r,id,'EXECUTE')) then raise exception 'HR_A0_KERNEL_PRIVATE_ACL_FAILED';end if;
end $kernel$;
select current_database() as physical_database,true as actual_scalar_coverage_kernel_assertions_passed,
 (select count(*) from private.timesheet_correction_a0_transaction_fences) as retained_native_intents;
`;
const sql="set local jit=off;\nset local pg_show_plans.is_enabled=off;\n"+vacancy+'\n'+mapped+'\n'+checks;
const verification=`do $absence$ begin if to_regclass('private.timesheet_correction_a0_transaction_fences') is not null or exists(select 1 from unnest(array[${names.map(s=>"'"+s+"'").join(',')}]) id where to_regprocedure(id) is not null) then raise exception 'HR_A0_KERNEL_POST_ROLLBACK_ABSENCE_FAILED';end if;end $absence$;
select current_database() as physical_database,true as table_and_five_functions_absent;`;
const manifest={source_only_preparation:true,native_status:'PENDING',source_commit:pin,inputs,selected_scalar_statements:3,selected_scalar_sha256:sha(selected.join('\n')),logical_owner_policy:policy,selected_definitions_sha256:sha(declarations),mapped_definitions_sha256:sha(mapped),sql_sha256:sha(sql),verification_sha256:sha(verification),sql_bytes:Buffer.byteLength(sql),packaging_statements:scanManagedSql(sql).statements.length,functions:names,scope:'Actual scalar/range kernel and schema compilation/truth only. No financial, claimant, business writer, RAW/ACTIVE or RR/concurrency/full release proof.',shared_lock_required:[20261013,1]};
assert(manifest.sql_bytes<2000000&&manifest.packaging_statements<250);fs.mkdirSync(dir,{recursive:true});fs.writeFileSync(dir+'/TRIAL.sql',sql);fs.writeFileSync(dir+'/ABSENCE.sql',verification);fs.writeFileSync(dir+'/MANIFEST.json',JSON.stringify(manifest,null,2)+'\n');console.log(JSON.stringify({manifest,sql,verification}));

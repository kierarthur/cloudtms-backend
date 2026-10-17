"""Author a finite, unmounted Health-only selection; never executes SQL."""
from pathlib import Path
import hashlib,json,re,subprocess,datetime

HERE=Path('\\\\?\\'+str(Path(__file__).resolve().parent))
MONEY=HERE.parents[3]
I2=Path('D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026')
PARENT='e5b113e4a985f08574e558844e3e81579faf49e1'
PREFIX='17102026_1251_health_closed_private_helpers'
ROOT=f'supabase/repeatable/{PREFIX}_v1.sql'
MIG=f'supabase/migrations/{PREFIX}_admission.sql'
VERIFY='supabase/verification/17102026_1251_health_closed_private_helpers_security.sql'
REG='docs/bpay-next/final-definer-closures.json'
REL='supabase/release/current-release.json'
LOCK='supabase/release/migration-lock.json'
sha=lambda b:hashlib.sha256(b).hexdigest()
def git(*args):
 p=subprocess.run(['git','-c','gc.auto=0','-c','maintenance.auto=false','-C',str(I2),*args],capture_output=True,check=True)
 return p.stdout
def write(name,data):
 p=HERE/name;p.parent.mkdir(parents=True,exist_ok=True)
 p.write_bytes(data.encode() if isinstance(data,str) else data)
def js(name,data):write(name,json.dumps(data,indent=2,ensure_ascii=False)+'\n')
def pinned(path):
 b=(I2/path).read_bytes()
 assert b==git('show',f'{PARENT}:{path}')==git('show',f':{path}'),path
 return b

assert str(MONEY.resolve()).removeprefix('\\\\?\\')==str(Path('D:/Development/GitHub/.codex-worktrees/healthroster-v2-money-build-13102026').resolve())
assert git('rev-parse',PARENT).decode().strip()==PARENT

# The nine literal expectations are independently declared, not generated from
# the runtime classifier or extracted at native verification time.
SPEC=[
 ('private.daily_rate_correction_health_financial_projection_assert_v1(jsonb,boolean)','s','pg_catalog,private,pg_temp','01_daily_presentation.sqlinc'),
 ('private.daily_rate_correction_health_invoice_effect_v1(jsonb)','s','pg_catalog,private,pg_temp','01_daily_presentation.sqlinc'),
 ('private.daily_rate_correction_health_actions_assert_v1(jsonb)','s','pg_catalog,private,pg_temp','01_daily_presentation.sqlinc'),
 ('private.daily_rate_correction_health_review_enroll_v1(jsonb)','v','pg_catalog,public,private,pg_temp','02_daily_review_enrollment.sqlinc'),
 ('private.daily_rate_correction_health_revision_apply_v1(jsonb)','v','pg_catalog,public,private,pg_temp','03_daily_revision_apply.sqlinc'),
 ('private.daily_rate_correction_health_current_placement_v1(uuid,uuid)','v','pg_catalog,public,private,pg_temp','04_daily_current_placement.sqlinc'),
 ('private.timesheet_contract_move_public_assert_v1(jsonb,text)','s','pg_catalog,private,pg_temp','05_contract_public_assert.sqlinc'),
 ('private.timesheet_contract_move_actions_assert_v1(jsonb)','s','pg_catalog,private,pg_temp','06_contract_projection.sqlinc'),
 ('private.timesheet_contract_move_invoice_effect_v1(jsonb,uuid)','s','pg_catalog,public,private,pg_temp','06_contract_projection.sqlinc'),
]
SOURCES=[
 ('01_daily_presentation.sqlinc','040bb062b9b0fdf3fa6bb45e400494d65929f40a','daily-health-current-placement-source-successor-20261017/health/00_presentation.sqlinc'),
 ('02_daily_review_enrollment.sqlinc','040bb062b9b0fdf3fa6bb45e400494d65929f40a','daily-health-current-placement-source-successor-20261017/health/05_review_enrollment.sqlinc'),
 ('03_daily_revision_apply.sqlinc','040bb062b9b0fdf3fa6bb45e400494d65929f40a','daily-health-current-placement-source-successor-20261017/health/10_revision_apply.sqlinc'),
 ('04_daily_current_placement.sqlinc','174cf17ff3368b84cbb51d1f57e46d94666105f2','daily-health-current-placement-native-identity-forward-20261017/health/20_current_placement.sqlinc'),
 ('05_contract_public_assert.sqlinc','2d1c81cf5a6939b8ab05d4b3d8db398037c13ed4','contract-preview-current-health-20261017/health/00_public_assert.sqlinc'),
 ('06_contract_projection.sqlinc','2d1c81cf5a6939b8ab05d4b3d8db398037c13ed4','contract-preview-current-health-20261017/health/01_projection.sqlinc'),
]
proof=[]
for target,commit,source in SOURCES:
 source='docs/healthroster-build/proposals/'+source
 b=pinned(source);assert b==git('show',f'{commit}:{source}')
 target=f'supabase/repeatable/{PREFIX}/{target}'
 write('payload/'+target,b);write('reference/selected/'+Path(target).name,b)
 proof.append({'target':target,'source':source,'sourceCommit':commit,'sha256':sha(b),'bytes':len(b),'savedIndexParentHistoricalEqual':True})

for target in [MIG,ROOT]:
 p=MONEY/target
 # This invocation uses the actual managed CLI-created files only once.
 if p.exists():
  b=p.read_bytes();write('scaffold/'+Path(target).name,b)
  destination=HERE/'payload'/target;destination.parent.mkdir(parents=True,exist_ok=True)
  assert destination.resolve().is_relative_to(MONEY.resolve())
  assert not destination.exists()
  p.rename(destination)
 assert (HERE/'scaffold'/Path(target).name).exists(),target

names=[s.split('(')[0].split('.')[1] for s,_,_,_ in SPEC]
migration="\\set ON_ERROR_STOP on\nbegin;\n-- One-time namespace admission, before any new definition. Every overload/kind.\ndo $admission$\ndeclare routine_name text;\nbegin\n foreach routine_name in array array[\n  "+',\n  '.join("'"+n+"'" for n in names)+"\n ] loop\n  if exists(select 1 from pg_catalog.pg_proc routine join pg_catalog.pg_namespace namespace\n   on namespace.oid=routine.pronamespace where namespace.nspname='private' and routine.proname=routine_name) then\n   raise exception 'HEALTH_CLOSED_HELPER_NAMESPACE_COLLISION: %',routine_name using errcode='55000';\n  end if;\n end loop;\nend;\n$admission$;\ncommit;\n"
write('payload/'+MIG,migration)
root="\\set ON_ERROR_STOP on\nbegin;\n-- Complete accepted Health helpers only; no held composition or public entry.\n"+''.join('\\ir '+PREFIX+'/'+x[0]+'\n' for x in SOURCES)+'\\ir '+PREFIX+'/99_private_acl.sqlinc\ncommit;\n'
write('payload/'+ROOT,root)
acl='-- Explicit private owner-only contract for exactly nine accepted Health helpers.\n'+''.join(f'alter function {s} owner to postgres;\nrevoke all on function {s} from public,anon,authenticated,service_role;\n' for s,_,_,_ in SPEC)
write(f'payload/supabase/repeatable/{PREFIX}/99_private_acl.sqlinc',acl)
values=',\n'.join(f"  ('{s}','jsonb','{v}','{p}')" for s,v,p,_ in SPEC)
verifier="""\\set ON_ERROR_STOP on
begin;
-- Independent literal helper-only inventory. No business/helper invocation.
do $verify$
declare expected record; actual record; fn oid; role_name text;
begin
 for expected in select * from (values
"""+values+"""
 ) contract(signature,result_type,volatility,search_path) loop
  fn:=pg_catalog.to_regprocedure(expected.signature);
  if fn is null then raise exception 'HEALTH_CLOSED_HELPER_MISSING: %',expected.signature;end if;
  select routine.*,namespace.nspname,language.lanname into strict actual
   from pg_catalog.pg_proc routine join pg_catalog.pg_namespace namespace on namespace.oid=routine.pronamespace
   join pg_catalog.pg_language language on language.oid=routine.prolang where routine.oid=fn;
  if actual.nspname<>'private' or actual.lanname<>'plpgsql' or actual.prokind<>'f' or not actual.prosecdef
   or actual.proretset or actual.prorettype<>pg_catalog.to_regtype(expected.result_type)
   or actual.provolatile::text<>expected.volatility or actual.proowner<>(current_user::regrole)::oid
   or not exists(select 1 from pg_catalog.unnest(actual.proconfig) setting
    where pg_catalog.replace(setting,' ','')='search_path='||expected.search_path)
   or exists(select 1 from pg_catalog.aclexplode(coalesce(actual.proacl,pg_catalog.acldefault('f',actual.proowner))) permission
    where permission.privilege_type='EXECUTE' and permission.grantee<>actual.proowner) then
   raise exception 'HEALTH_CLOSED_HELPER_SECURITY_INVALID: %',expected.signature;end if;
  foreach role_name in array array['anon','authenticated','service_role'] loop
   if pg_catalog.has_function_privilege(role_name,fn,'EXECUTE') then
    raise exception 'HEALTH_CLOSED_HELPER_NONOWNER_EXECUTE: %',expected.signature;end if;
  end loop;
 end loop;
end;
$verify$;
rollback;
"""
write('payload/'+VERIFY,verifier)
originals={x:pinned(x) for x in [REG,REL,LOCK]};edits=[]
def metadata(target,post):
 before=originals[target];after=(json.dumps(post,indent=2)+'\n').encode()
 assert (json.dumps(json.loads(before),indent=2)+'\n').encode()==before,target
 # Full originals and inverse are retained; the sole JSON changes are checked
 # structurally in verify.py, not by trusting the author summary.
 write('current/'+target,before);write('payload/'+target,after)
 prefix=0
 while prefix<min(len(before),len(after)) and before[prefix]==after[prefix]:prefix+=1
 end=0
 while end<min(len(before)-prefix,len(after)-prefix) and before[-1-end]==after[-1-end]:end+=1
 edits.append({'target':target,'beforeSha256':sha(before),'afterSha256':sha(after),'hunks':[{'offset':prefix,'before':before[prefix:len(before)-end].decode(),'after':after[prefix:len(after)-end].decode()}]})
reg=json.loads(originals[REG]);assert len(reg['chains'])==1650
assert not set(s for s,_,_,_ in SPEC)&set(x['signature'] for x in reg['chains'])
for s,_,_,leaf in SPEC:
 physical=f'supabase/repeatable/{PREFIX}/{leaf}'
 reg['chains'].append({'signature':s,'finalRoot':ROOT,'finalPhysical':physical,'allowedPhysical':[physical],'predecessors':[],'finalClosures':[ROOT],'owner':'Health closed private helper; immutable accepted source body','healthClosedHelperReview':str(HERE.relative_to(MONEY)).replace('\\','/')+'/README.md'})
metadata(REG,reg)
release=json.loads(originals[REL]);assert len(release['verificationFiles'])==182 and len(release['newVerificationFiles'])==183
for field in ['verificationFiles','newVerificationFiles']:
 assert VERIFY not in release[field];release[field].append(VERIFY)
metadata(REL,release)
lock=json.loads(originals[LOCK]);assert len(lock['migrations'])==426
assert not any(x['path']==MIG for x in lock['migrations'])
lock['migrations'].append({'path':MIG,'sha256':sha(migration.encode())})
metadata(LOCK,lock)
js('FULL_METADATA_EDITS.json',edits)
js('SOURCE_PROVENANCE.json',proof)
js('EXPECTED_SECURITY_DELTA.json',{'privateOwnerOnly':[{'signature':s,'result':'jsonb','volatility':v,'searchPath':p,'language':'plpgsql','securityDefiner':True} for s,v,p,_ in SPEC],'newPublicOrService':[],'nonOwnerExecuteDenied':['PUBLIC','anon','authenticated','service_role'],'schemaChanges':0,'publicDtoChanges':0,'heldTemplatesSelected':0,'runtimeBankBodySelected':0})
keep=['supabase/release/current-contract.json','docs/healthroster-build/v5-amendment-20261015/SCHEMA_INTERFACE_REGISTER.json','supabase/repeatable/17102026_0910_nhsp_v6_daily_role_identity/01_complete_context.sqlinc','supabase/repeatable/17102026_0248_nhsp_v6_daily_health_support/01_complete_helpers.sqlinc','supabase/repeatable/16102026_0858_nhsp_v6_daily_native_authority/02_admission.sqlinc','supabase/repeatable/16102026_1342_nhsp_v6_daily_public_services/01_public_services.sqlinc','supabase/repeatable/16102026_0955_nhsp_v6_invoice_native_authority/01_complete_invoice_authorities.sqlinc','supabase/repeatable/16102026_1003_nhsp_v6_candidate_native_authority/01_final_functions.sqlinc','supabase/repeatable/17102026_0741_weekly_source_complete_classification/01_complete_current_classification.sqlinc','supabase/verification/15092026_1534_weekly_source_acl_contract_v1.sql','broker/src/contract-correction-v3.mjs','broker/src/weekly-source/source-pay-decision-worker.mjs','broker/src/weekly-source/source-pay-decision-contract.mjs','broker/src/weekly-source/routes.js']
pins=[]
for target in keep:
 b=pinned(target);pins.append({'file':target,'bytes':len(b),'sha256':sha(b),'savedIndexParentEqual':True})
write('reference/parent-runtime-tree.txt',git('ls-tree','-r',PARENT,'--','supabase','broker/src','scripts'))
js('CURRENT_PARENT.json',{'commit':PARENT,'root':str(I2),'physicalMoney':str(MONEY),'selectedTargets':[{k:v for k,v in e.items() if k!='hunks'} for e in edits],'unchangedPins':pins,'allExistingRuntimeIdentities':'reference/parent-runtime-tree.txt; Git tree metadata only, no Banking semantic body read','migrationsBefore':426,'repeatablesBefore':1096,'registeredBefore':1650,'verificationArraysBefore':[182,183],'native':'NOT_RUN'})
targets=[]
for p in sorted((HERE/'payload').rglob('*')):
 if p.is_file():
  relative=p.relative_to(HERE/'payload').as_posix();b=p.read_bytes();targets.append({'target':relative,'source':'payload/'+relative,'bytes':len(b),'sha256':sha(b),'new':relative not in originals})
js('PAYLOAD_SELECTION.json',{'parent':PARENT,'targets':targets,'privateDefinitions':9,'newSignatures':9,'existingOwnerBodiesChanged':0,'heldBankCompositions':0,'publicEntries':0,'registryBefore':1650,'registryAfter':1659,'migrationsBefore':426,'migrationsAfter':427,'repeatablesBefore':1096,'repeatablesAfter':1097,'contractChanges':0,'schemaObjectIdentitiesChanged':0,'native':'NOT_RUN','installation':'NOT_RUN'})
print(json.dumps({'payloads':len(targets),'sourceBodies':9,'currentParent':PARENT,'proposal':str(HERE),'native':'NOT_RUN'}))

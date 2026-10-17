"""Author-only finite source assembly. Never writes a registered runtime file."""
from pathlib import Path
import copy, hashlib, json, re, subprocess, sys
sys.dont_write_bytecode = True
HERE=Path(__file__).resolve().parent
ROOT=Path('D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026')
OWN=Path('D:/Development/GitHub/.codex-worktrees/healthroster-checklist-native-14102026')
OLD='docs/healthroster-build/proposals/checklist-current-native-module-recipe-20261015'
AUTHORED='0d002966b022734f7b86c9a6f98d7a2332080721'
ORIGINAL='6346f699029201de90288514e4c0751d43e10553'
STAMP='17102026_1900'
DIR=f'supabase/repeatable/{STAMP}_health_d10_query_reader'
MODULE=f'supabase/repeatable/{STAMP}_health_d10_query_reader_v1.sql'
MIGRATION=f'supabase/migrations/{STAMP}_health_d10_query_reader_admission.sql'
VERIFY=f'supabase/verification/{STAMP}_health_d10_query_reader_security.sql'
CLASS='supabase/repeatable/17102026_0741_weekly_source_complete_classification/01_complete_current_classification.sqlinc'
MASTER='supabase/verification/15092026_1534_weekly_source_acl_contract_v1.sql'
REGISTRY='docs/bpay-next/final-definer-closures.json'
RELEASE='supabase/release/current-release.json'
LOCK='supabase/release/migration-lock.json'
def long(p): return Path('\\\\?\\'+str(Path(p).resolve()))
def sha(b): return hashlib.sha256(b).hexdigest()
def git(*args,root=ROOT):
 return subprocess.check_output(['git','-c','gc.auto=0','-c','maintenance.auto=false','-c','core.longpaths=true',*args],cwd=root)
def put(name,b):
 p=long(HERE/name);p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(b.encode() if isinstance(b,str) else b)
def dump(name,x):put(name,json.dumps(x,ensure_ascii=False,indent=2)+'\n')
def replace_once(s,a,b):
 assert s.count(a)==1,('EXACT_LITERAL_ANCHOR',a[:100]);return s.replace(a,b)
def function(source,name):
 # Full CREATE through its own matching dollar body and terminating semicolon.
 m=re.search(r'create\s+or\s+replace\s+function\s+'+re.escape(name)+r'\s*\(',source,re.I);assert m,name
 a=re.search(r'\bas\s+(\$[A-Za-z_0-9]*\$)',source[m.start():],re.I);assert a,name
 start=m.start();body=m.start()+a.end();tag=a[1];end=source.index(tag,body)+len(tag)
 assert source[end:end+1]==';';return source[start:end+1]
def main():
 assert not (HERE/'MANIFEST.json').exists(),'FROZEN_PACKET_REFUSED'
 head=git('rev-parse','HEAD').decode().strip()
 parents={};sources={};targets=[]
 def capture(p):
  if p in parents:return sources[p]
  raw=long(ROOT/p).read_bytes();assert raw==git('show',head+':'+p)==git('show',':'+p),('CURRENT_SAVED_INDEX_HEAD',p)
  dest='parents/'+p;put(dest,raw);parents[p]={'target':p,'capture':dest,'sha256':sha(raw),'bytes':len(raw),'commit':head};sources[p]=raw
  return raw
 def target(p,b):
  if isinstance(b,str):b=b.encode()
  before=capture(p) if long(ROOT/p).exists() else None
  if before is None:
   assert not git('ls-files','--',p).strip(),('NEW_PATH_TRACKED',p)
  out='payload/'+p;put(out,b);targets.append({'target':p,'source':out,'sha256':sha(b),'bytes':len(b),'beforeSha256':sha(before) if before else None,'beforeBytes':len(before) if before else None})
 recipe_raw=git('show',AUTHORED+':'+OLD+'/MODULE_RECIPE.json',root=OWN)
 assert long(OWN/OLD/'MODULE_RECIPE.json').read_bytes()==recipe_raw
 put('provenance/MODULE_RECIPE.json',recipe_raw)
 recipe=json.loads(recipe_raw)
 tokens=['actual_timesheet_submission','signed_submission_read_context','check_only_query_rows','check_only_issue_filter_keys','check_only_query_groups','candidate_reminder_subject','candidate_reminder_context']
 owners=[x for x in recipe['owners'] if any(t in x['name'] for t in tokens)];assert len(owners)==7
 direct=json.loads((HERE/'CURRENT_DEFINERS.json').read_bytes())
 definitions={x['signature']:x['definitions'] for x in direct['rows']}
 existing=[x for x in owners if definitions[x['id']]];fresh=[x for x in owners if not definitions[x['id']]]
 assert len(existing)==3 and len(fresh)==4
 # Main reader is last after its factual/reminder dependencies; all are complete original pages.
 ordered=[owners[i] for i in [0,1,2,3,5,6,4]]
 emitted={};inverse=[]
 for index,o in enumerate(ordered,1):
  rel=OLD+'/'+o['output'];raw=git('show',AUTHORED+':'+rel,root=OWN)
  assert long(OWN/rel).read_bytes()==raw and sha(raw)==o['output_sha256']
  original=git('show',ORIGINAL+':'+o['source_file'],root=OWN)
  assert sha(original)==o['source_file_sha256']
  original_target='provenance/'+o['source_file'];put(original_target,original)
  assert function(original.decode(),o['name'])==function(raw.decode(),o['name'])
  dest=DIR+'/'+f'{index:02d}_'+o['name'].split('.',1)[1]+'.sqlinc';target(dest,raw);emitted[o['id']]=dest
  if definitions[o['id']]:
   d=definitions[o['id']][-1];old=capture(d['physical']);capture(d['root'])
   assert len(definitions[o['id']])==1
   inverse.append({'signature':o['id'],'currentRoot':d['root'],'currentPhysical':d['physical'],'before':old.decode(),'beforeSha256':sha(old),'beforeBytes':len(old),'after':raw.decode(),'afterSha256':sha(raw),'afterBytes':len(raw),'successorPhysical':dest})
 classifier=capture(CLASS).decode();name='private._weekly_source_acl_private_helper_contract_v1'
 old_fn=function(classifier,name);fresh_sigs=sorted(x['id'] for x in fresh)
 # Add four exact literal rows ahead of the original first row; original ordering/content retained.
 anchor="  values\n    ('private._weekly_source_acl_lifecycle_column_contract_v1()'),"
 new_fn=replace_once(old_fn,anchor,'  values\n'+''.join("    ('"+x+"'),\n" for x in fresh_sigs)+"    ('private._weekly_source_acl_lifecycle_column_contract_v1()'),")
 classifier_acl='\nalter function '+name+'() owner to postgres;\nrevoke all on function '+name+'() from public,anon,authenticated,service_role;\n'
 classifier_target=DIR+'/98_private_helper_classification.sqlinc';target(classifier_target,new_fn+classifier_acl)
 # Exact whole current classifier definition inverse; no other five classifier bodies relocated.
 inverse.append({'signature':name+'()','currentRoot':definitions[name+'()'][-1]['root'],'currentPhysical':CLASS,'before':old_fn,'beforeSha256':sha(old_fn.encode()),'beforeBytes':len(old_fn.encode()),'after':new_fn,'afterSha256':sha(new_fn.encode()),'afterBytes':len(new_fn.encode()),'successorPhysical':classifier_target})
 # Preserve the complete current verifier; insert the same four literals in only its private array.
 master=capture(MASTER);s=master.decode();anchor='  v_expected_private_helpers text[]:=array[\n'
 changed=replace_once(s,anchor,anchor+''.join("    '"+x+"',\n" for x in fresh_sigs));target(MASTER,changed.encode())
 # One-time namespace admission is never part of rerunnable repeatable closure.
 new_names=[x['name'].split('.')[1] for x in fresh]
 admission="\\set ON_ERROR_STOP on\nbegin;\n-- ONE_TIME_ADMISSION: all overloads and routine kinds, not a repeatable guard.\ndo $admission$\ndeclare expected_name text;\nbegin\n foreach expected_name in array array[\n"+',\n'.join("  '"+x+"'" for x in new_names)+"\n ] loop\n  if exists(select 1 from pg_catalog.pg_proc routine join pg_catalog.pg_namespace namespace\n   on namespace.oid=routine.pronamespace where namespace.nspname='private' and routine.proname=expected_name) then\n   raise exception 'HEALTH_D10_READER_NAMESPACE_COLLISION: %',expected_name using errcode='55000';\n  end if;\n end loop;\nend;\n$admission$;\ncommit;\n"
 target(MIGRATION,admission.encode())
 module="\\set ON_ERROR_STOP on\nbegin;\n-- Exact preserved Health D10 read-only closure. No public RPC, writer or dispatch.\n"+''.join('\\ir '+p.split('/')[-2]+'/'+p.split('/')[-1]+'\n' for p in list(emitted.values())+[classifier_target])+"commit;\n"
 target(MODULE,module.encode())
 # Catalogue-only independent verifier; no business/helper invocation or browser grants.
 specs=[(x['id'],'i' if 'issue_filter_keys' in x['id'] else 's') for x in owners]
 verify="\\set ON_ERROR_STOP on\nbegin;\ndo $verify$\ndeclare expected record; actual record; fn oid; role_name text;\nbegin\n for expected in select * from (values\n"+',\n'.join("  ('"+sig+"','"+vol+"')" for sig,vol in specs)+"\n ) contract(signature,volatility) loop\n  fn:=pg_catalog.to_regprocedure(expected.signature);\n  if fn is null then raise exception 'HEALTH_D10_READER_MISSING: %',expected.signature;end if;\n  select routine.*,namespace.nspname,language.lanname into strict actual\n   from pg_catalog.pg_proc routine join pg_catalog.pg_namespace namespace on namespace.oid=routine.pronamespace\n   join pg_catalog.pg_language language on language.oid=routine.prolang where routine.oid=fn;\n  if actual.nspname<>'private' or actual.lanname<>'plpgsql' or actual.prokind<>'f' or not actual.prosecdef\n   or actual.proretset or actual.prorettype<>pg_catalog.to_regtype('jsonb')\n   or actual.provolatile::text<>expected.volatility or actual.proowner<>(current_user::regrole)::oid\n   or not exists(select 1 from pg_catalog.unnest(actual.proconfig) setting\n    where pg_catalog.replace(setting,' ','')='search_path=pg_catalog,public,private,pg_temp')\n   or exists(select 1 from pg_catalog.aclexplode(coalesce(actual.proacl,pg_catalog.acldefault('f',actual.proowner))) permission\n    where permission.privilege_type='EXECUTE' and permission.grantee<>actual.proowner) then\n   raise exception 'HEALTH_D10_READER_SECURITY_INVALID: %',expected.signature;end if;\n  foreach role_name in array array['anon','authenticated','service_role'] loop\n   if pg_catalog.has_function_privilege(role_name,fn,'EXECUTE') then\n    raise exception 'HEALTH_D10_READER_NONOWNER_EXECUTE: %',expected.signature;end if;\n  end loop;\n end loop;\nend;\n$verify$;\nrollback;\n"
 target(VERIFY,verify.encode())
 registry=json.loads(capture(REGISTRY));old_registry=copy.deepcopy(registry)
 for o in owners:
  assert not any(x['signature']==o['id'] for x in registry['chains']),('UNEXPECTED_EXISTING_CHAIN',o['id'])
  prior=definitions[o['id']]
  registry['chains'].append({'signature':o['id'],'finalRoot':MODULE,'finalPhysical':emitted[o['id']],'allowedPhysical':list(dict.fromkeys([x['physical'] for x in prior]+[emitted[o['id']]])),'predecessors':list(dict.fromkeys(x['root'] for x in prior)),'finalClosures':[MODULE],'owner':'Health D10 preserved seven-reader closure; source-only peer required','healthD10SourceProvenance':OLD+'/MODULE_RECIPE.json'})
 chain=next(x for x in registry['chains'] if x['signature']==name+'()')
 for field,item in [('allowedPhysical',classifier_target),('predecessors',chain['finalRoot']),('finalClosures',MODULE)]:
  if item not in chain[field]:chain[field].append(item)
 chain['finalRoot']=MODULE;chain['finalPhysical']=classifier_target
 chain['healthD10SourceProvenance']=OLD+'/MODULE_RECIPE.json'
 target(REGISTRY,(json.dumps(registry,indent=2)+'\n').encode())
 release=json.loads(capture(RELEASE));before_release=copy.deepcopy(release)
 for key in ['verificationFiles','newVerificationFiles']:
  assert VERIFY not in release[key];release[key].append(VERIFY)
 target(RELEASE,(json.dumps(release,indent=2)+'\n').encode())
 lock=json.loads(capture(LOCK));lock['migrations'].append({'path':MIGRATION,'sha256':sha(admission.encode())});target(LOCK,(json.dumps(lock,indent=2)+'\n').encode())
 for p in ['AGENTS.md','scripts/cloudtms-db-release-lib.mjs','scripts/cloudtms-managed-release-sql.mjs','scripts/bpay-next-final-definers.mjs','scripts/bpay-next-source-gates.mjs','supabase/release/current-contract.json',
           'supabase/migrations/13102026_1127_healthroster_timesheet_correction_requests_v1.sql','supabase/migrations/13102026_1133_weekly_source_rule_sets_schema.sql',
           'supabase/repeatable/17102026_0231_weekly_source_paid_same_separate_health/037_public_weekly_source_office_workspace_v1.sqlinc','supabase/repeatable/17102026_0231_weekly_source_paid_same_separate_health/064_public_weekly_source_workspace_scopes_v1.sqlinc']:
  capture(p)
 # Preserve complete direct dependency physical owners and their complete root includes as source pins.
 edges=[x for x in recipe['dependencies'] if x['caller'] in set(o['id'] for o in owners)]
 for x in direct['rows']:
  if x['definitions']:
   capture(x['definitions'][-1]['physical']);capture(x['definitions'][-1]['root'])
 dump('SOURCE_PROVENANCE.json',{'version':'HEALTH_D10_PRESERVED_SEVEN_V1','authoredModuleCommit':AUTHORED,'originalBodyCommit':ORIGINAL,'originalModule':OLD,'originalRecipeSha256':sha(recipe_raw),'staleWhole60TargetSelectionExcluded':True,'owners':owners,'directDependencies':edges,'readOnlySourceContribution':True,'BankSemanticBodiesRead':False,'native':'NOT_RUN'})
 dump('SOURCE_OWNER_REVERSES.json',{'version':'HEALTH_D10_WHOLE_CURRENT_OWNERS_V1','currentParent':head,'rows':inverse})
 changes=[]
 for row in targets:
  if row['beforeSha256']:
   before=sources[row['target']];after=long(HERE/row['source']).read_bytes()
   changes.append({'target':row['target'],'before':before.decode(),'after':after.decode(),'beforeSha256':sha(before),'afterSha256':sha(after),'beforeBytes':len(before),'afterBytes':len(after)})
 dump('FULL_PARENT_FORWARD.json',{'version':'HEALTH_D10_COMPLETE_EXISTING_FILE_FORWARD_V1','parent':head,'rows':changes})
 dump('EXPECTED_SECURITY_DELTA.json',{'sevenOwnerOnly':specs,'newPrivateLiteralSignatures':fresh_sigs,'publicServiceDelta':[],'tableColumnDelta':[],'defaultAclDelta':[],'privateHelperExistingRevokesReapplied':True,'noDefaultGrantReliance':'All seven complete pages explicitly revoke public,anon,authenticated,service_role after logical owner mapping. Existing default-privilege policy is unchanged.'})
 dump('PARENTS.json',{'version':'HEALTH_D10_CURRENT_COMPLETE_PARENTS_V1','root':str(ROOT).replace('\\','/'),'parent':head,'rows':list(parents.values())})
 dump('PAYLOAD_SELECTION.json',{'version':'HEALTH_D10_CURRENT_SELECTION_V1','parent':head,'module':MODULE,'admission':MIGRATION,'verifier':VERIFY,'targets':targets})
 dump('ASSEMBLY.json',{'version':'HEALTH_D10_MANAGED_SOURCE_ASSEMBLY_V1','parent':head,'module':MODULE,'includeOrder':list(emitted.values())+[classifier_target],'ownerPages':emitted,'newNames':new_names,'sourceDefinitionChanges':3,'newPrivateDefinitions':4,'classifierSuccessorDefinitions':1,'registry':{'before':len(old_registry['chains']),'after':len(registry['chains']),'newChains':7,'existingChainSuccessors':1},'verifiers':{'before':{k:len(before_release[k]) for k in ['verificationFiles','newVerificationFiles']},'after':{k:len(release[k]) for k in ['verificationFiles','newVerificationFiles']}},'migrationHistoryPreserved':len(lock['migrations'])-1,'schemaHistory92Unchanged':True,'currentContractFileUnchanged':True,'mechanicalSealRequiredAfterReview':True})
 assert git('rev-parse','HEAD').decode().strip()==head,'ROOT_HEAD_MOVED_DURING_CAPTURE'
 print(json.dumps({'parent':head,'targets':len(targets),'parents':len(parents),'helperPages':7,'existingOwnerReverses':3,'newPrivateNames':4,'module':MODULE}))
if __name__=='__main__':main()

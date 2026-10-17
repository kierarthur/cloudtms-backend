"""Finite helper-only source/ACL/namespace/metadata proof; no SQL invocation."""
from pathlib import Path
import json,hashlib,subprocess,sys,re
sys.dont_write_bytecode=True
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
from pglast import ast
HERE=Path('\\\\?\\'+str(Path(__file__).resolve().parent))
I2=Path('D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026')
PARENT='e5b113e4a985f08574e558844e3e81579faf49e1'
sha=lambda b:hashlib.sha256(b).hexdigest()
read=lambda p:(HERE/p).read_bytes()
load=lambda p:json.loads(read(p))
def git(*args):return subprocess.check_output(['git','-c','gc.auto=0','-c','maintenance.auto=false','-c','core.longpaths=true','-C',str(I2),*args])
selection=load('PAYLOAD_SELECTION.json');assert selection['parent']==PARENT
assert len(selection['targets'])==13
targets={x['target']:read(x['source']) for x in selection['targets']}
assert len(targets)==13
for row in selection['targets']:
 assert sha(targets[row['target']])==row['sha256'] and len(targets[row['target']])==row['bytes']
 if row['new']:assert not (I2/row['target']).exists(),row['target']
for p in load('SOURCE_PROVENANCE.json'):
 b=targets[p['target']]
 assert b==git('show',p['sourceCommit']+':'+p['source'])==git('show',PARENT+':'+p['source'])==git('show',':'+p['source'])
 assert b==(I2/p['source']).read_bytes()
for e in load('FULL_METADATA_EDITS.json'):
 before=read('current/'+e['target']);after=targets[e['target']]
 assert before==git('show',PARENT+':'+e['target'])
 assert sha(before)==e['beforeSha256'] and sha(after)==e['afterSha256']
 reverse=after
 for h in reversed(e['hunks']):
  at=h['offset'];new=h['after'].encode();old=h['before'].encode()
  assert reverse[at:at+len(new)]==new
  reverse=reverse[:at]+old+reverse[at+len(new):]
 assert reverse==before
reg='docs/bpay-next/final-definer-closures.json';rel='supabase/release/current-release.json';lock='supabase/release/migration-lock.json'
old=load('current/'+reg);new=json.loads(targets[reg]);assert new['chains'][:1650]==old['chains'] and len(new['chains'])==1659
assert {k:v for k,v in new.items() if k!='chains'}=={k:v for k,v in old.items() if k!='chains'}
expected=load('EXPECTED_SECURITY_DELTA.json')['privateOwnerOnly'];signatures=[s['signature'] for s in expected]
assert [x['signature'] for x in new['chains'][1650:]]==signatures
roots={x['finalRoot'] for x in new['chains'][1650:]};assert len(roots)==1;root=roots.pop()
for chain in new['chains'][1650:]:
 assert chain['finalPhysical'] in targets and chain['allowedPhysical']==[chain['finalPhysical']]
 assert chain['predecessors']==[] and chain['finalClosures']==[root]
 assert '@@' not in targets[chain['finalPhysical']].decode()
release=json.loads(targets[rel]);before=load('current/'+rel)
verifiers=[x for x in targets if x.startswith('supabase/verification/')];assert len(verifiers)==1
for field in ['verificationFiles','newVerificationFiles']:
 assert release[field]==before[field]+verifiers
assert {k:v for k,v in release.items() if k not in ['verificationFiles','newVerificationFiles']}=={k:v for k,v in before.items() if k not in ['verificationFiles','newVerificationFiles']}
migration=[x for x in targets if x.startswith('supabase/migrations/')];assert len(migration)==1;migration=migration[0]
before=load('current/'+lock);after=json.loads(targets[lock]);assert len(before['migrations'])==426 and after['migrations']==before['migrations']+[{'path':migration,'sha256':sha(targets[migration])}]
assert after['policy']==before['policy'] and after['formatVersion']==before['formatVersion']
sql='\n'.join(x for x in targets[migration].decode().splitlines() if not x.startswith('\\'))
loop=pglast.parse_plpgsql(sql)[0]['PLpgSQL_function']['action']['PLpgSQL_stmt_block']['body'][0]['PLpgSQL_stmt_foreach_a']
names=[x.val.sval for x in pglast.parse_sql('select '+loop['expr']['PLpgSQL_expr']['query'])[0].stmt.targetList[0].val.elements]
assert names==[x.split('(')[0].split('.')[1] for x in signatures]
condition=pglast.parse_sql('select '+loop['body'][0]['PLpgSQL_stmt_if']['cond']['PLpgSQL_expr']['query'])[0].stmt.targetList[0].val.subselect
assert condition.limitCount is None and condition.limitOffset is None
join=condition.fromClause[0];assert join.larg.schemaname==join.rarg.schemaname=='pg_catalog'
assert join.larg.relname=='pg_proc' and join.rarg.relname=='pg_namespace'
def evaluate(node,context):
 if isinstance(node,ast.BoolExpr):
  assert node.boolop.name=='AND_EXPR';return all(evaluate(x,context) for x in node.args)
 if isinstance(node,ast.A_Expr):
  assert len(node.name)==1 and node.name[0].sval=='=';return evaluate(node.lexpr,context)==evaluate(node.rexpr,context)
 if isinstance(node,ast.ColumnRef):
  value=context
  for f in node.fields:value=value[f.sval]
  return value
 if isinstance(node,ast.A_Const):return node.val.sval
 raise AssertionError(type(node))
vectors=0
for name in names:
 for schema in ['private','public','unrelated']:
  for kind in ['f','p','a','w']:
   for arity in [0,1,4]:
    context={'namespace':{'oid':10,'nspname':schema},'routine':{'pronamespace':10,'proname':name,'prokind':kind,'pronargs':arity},'routine_name':name}
    admitted=evaluate(join.quals,context) and evaluate(condition.whereClause,context)
    assert admitted==(schema=='private');vectors+=1
context={'namespace':{'oid':10,'nspname':'private'},'routine':{'pronamespace':10,'proname':'other_owner'},'routine_name':names[0]}
assert not evaluate(condition.whereClause,context);vectors+=1
acl=next(b.decode() for p,b in targets.items() if p.endswith('/99_private_acl.sqlinc'))
assert 'grant ' not in acl.lower()
for signature in signatures:
 assert acl.count('alter function '+signature+' owner to postgres;')==1
 assert acl.count('revoke all on function '+signature+' from public,anon,authenticated,service_role;')==1
v=targets[verifiers[0]].decode();assert all(v.count("('"+s+"','jsonb'")==1 for s in signatures)
assert re.findall(r"^\s+\('([^']+)','jsonb','([sv])','([^']+)'\)",v,re.M)==[(x['signature'],x['volatility'],x['searchPath']) for x in expected]
assert all(x not in v for x in ['private.daily_rate_correction_review_v1(','private.daily_rate_correction_accept_v1(','private.daily_rate_correction_result_v1(','public.timesheet_contract_move_preview_v1('])
assert not any(x.startswith('broker/') or x.startswith('supabase/migrations/16102026_') for x in targets)
assert '\\ir' not in targets[migration].decode()
for pin in load('CURRENT_PARENT.json')['unchangedPins']:
 assert sha((I2/pin['file']).read_bytes())==pin['sha256']
schema=load('CURRENT_PARENT.json')['unchangedPins'];schema=next(x for x in schema if x['file'].endswith('/SCHEMA_INTERFACE_REGISTER.json'))
assert len(json.loads((I2/schema['file']).read_bytes())['objects'])==92
report={'version':'HEALTH_CLOSED9_FINITE_SOURCE_VERIFICATION_V1','parent':PARENT,'payloads':13,'acceptedWholeBodyFiles':6,'privateDefinitions':9,'existingRegistryChainsPreserved':1650,'metadataWholeReversals':3,'migrationRowsPreserved':426,'namespaceActualPredicateVectors':vectors,'namespaceOverloadsKinds':'ALL_UNFILTERED','newServicePublicOrBrowserGrants':0,'heldTemplatesInVerifier':0,'originalSchemaIdentitiesPreserved':92,'existingClassifiersMandatoryArraysSchemaAndTransport':'UNCHANGED','native':'NOT_RUN'}
if '--verify' in sys.argv:assert load('VERIFICATION_RESULTS.json')==report
else:(HERE/'VERIFICATION_RESULTS.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(report))

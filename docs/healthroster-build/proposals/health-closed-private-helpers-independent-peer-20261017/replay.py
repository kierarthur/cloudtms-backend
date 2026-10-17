"""Finite read-only peer of the frozen 0f80 helper selection. No DB or build."""
from pathlib import Path
import hashlib,json,subprocess,sys,copy
sys.dont_write_bytecode=True
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
from pglast import ast
HERE=Path('\\\\?\\'+str(Path(__file__).resolve().parent))
MONEY=Path('D:/Development/GitHub/.codex-worktrees/healthroster-v2-money-build-13102026')
ROOT=Path('D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026')
PREFIX='docs/healthroster-build/proposals/health-closed-private-helpers-managed-source-20261017/'
FROZEN='0f80d35ec5e63e4eb44b6c873d08a72acf3bd1cd'
PARENT='e5b113e4a985f08574e558844e3e81579faf49e1'
sha=lambda b:hashlib.sha256(b).hexdigest()
def git(root,*args):
 return subprocess.check_output(['git','-c','gc.auto=0','-c','maintenance.auto=false','-c','core.longpaths=true','-C',str(root),*args])
def read(name):return (Path('\\\\?\\'+str(MONEY))/PREFIX/name).read_bytes()
def load(name):return json.loads(read(name))
manifest=load('MANIFEST.json')
assert sha(read('MANIFEST.json'))=='442727720f45dfecbad72c391e8ea07442dc8c53be2b40bc85181e59e492702e'
assert sha(read('PAYLOAD_SELECTION.json'))=='eee7e0ef611aca0bbe94ccd96002fafbbb0cd4288271527e808dae76ee10940f'
assert sha(read('SOURCE_CHECK_RESULTS.json'))=='881e5acb0566f410f1c1865602083624a719aa52180ba7a38e8c556647b2d3c3'
inputs=[]
for item in manifest['files']:
 b=read(item['file']);p=PREFIX+item['file']
 assert sha(b)==item['sha256'] and len(b)==item['bytes']
 assert b==git(MONEY,'show',FROZEN+':'+p)==git(MONEY,'show',':'+p)
 inputs.append({'file':item['file'],'sha256':sha(b),'bytes':len(b)})
assert len(inputs)==41
# The manifest itself is the forty-second frozen saved/index/HEAD input.
assert read('MANIFEST.json')==git(MONEY,'show',FROZEN+':'+PREFIX+'MANIFEST.json')==git(MONEY,'show',':'+PREFIX+'MANIFEST.json')
selection=load('PAYLOAD_SELECTION.json');assert selection['parent']==PARENT
targets={r['target']:read(r['source']) for r in selection['targets']};assert len(targets)==13
for row in selection['targets']:assert sha(targets[row['target']])==row['sha256'] and len(targets[row['target']])==row['bytes']
registry='docs/bpay-next/final-definer-closures.json';release='supabase/release/current-release.json';lock='supabase/release/migration-lock.json'
base='supabase/repeatable/17102026_1251_health_closed_private_helpers/'
root='supabase/repeatable/17102026_1251_health_closed_private_helpers_v1.sql'
migration='supabase/migrations/17102026_1251_health_closed_private_helpers_admission.sql'
verifier='supabase/verification/17102026_1251_health_closed_private_helpers_security.sql'
spec=[
 ('daily_rate_correction_health_financial_projection_assert_v1','jsonb,boolean','s','pg_catalog,private,pg_temp'),
 ('daily_rate_correction_health_invoice_effect_v1','jsonb','s','pg_catalog,private,pg_temp'),
 ('daily_rate_correction_health_actions_assert_v1','jsonb','s','pg_catalog,private,pg_temp'),
 ('daily_rate_correction_health_review_enroll_v1','jsonb','v','pg_catalog,public,private,pg_temp'),
 ('daily_rate_correction_health_revision_apply_v1','jsonb','v','pg_catalog,public,private,pg_temp'),
 ('daily_rate_correction_health_current_placement_v1','uuid,uuid','v','pg_catalog,public,private,pg_temp'),
 ('timesheet_contract_move_public_assert_v1','jsonb,text','s','pg_catalog,private,pg_temp'),
 ('timesheet_contract_move_actions_assert_v1','jsonb','s','pg_catalog,private,pg_temp'),
 ('timesheet_contract_move_invoice_effect_v1','jsonb,uuid','s','pg_catalog,public,private,pg_temp')]
sigs=['private.'+n+'('+a+')' for n,a,_,_ in spec]
def walk(n):
 if isinstance(n,ast.Node):
  yield n
  for f in n:yield from walk(getattr(n,f))
 elif isinstance(n,(tuple,list)):
  for x in n:yield from walk(x)
def dictionaries(n):
 if isinstance(n,dict):
  yield n
  for v in n.values():yield from dictionaries(v)
 elif isinstance(n,list):
  for v in n:yield from dictionaries(v)
def sql(b):return '\n'.join(x for x in b.decode().splitlines() if not x.startswith('\\'))
def typ(t):return '.'.join(n.sval for n in t.names).removeprefix('pg_catalog.').replace('bool','boolean')
def signature(h):return '.'.join(n.sval for n in h.funcname)+'('+','.join(typ(p.argType) for p in h.parameters or ()) +')'
def contract(t):
 # Complete approved bytes are a provenance requirement, not a hazard waiver.
 for p in load('SOURCE_PROVENANCE.json'):
  assert sha(t[p['target']])==p['sha256']
 acl=sql(t[base+'99_private_acl.sqlinc']);statements=pglast.parse_sql(acl)
 assert len(statements)==18
 owners=[];revokes=[]
 for wrapped in statements:
  q=wrapped.stmt
  if isinstance(q,ast.AlterOwnerStmt):
   assert q.newowner.rolename=='postgres';owners.append(q)
  else:
   assert isinstance(q,ast.GrantStmt) and not q.is_grant and not q.grant_option
   assert len(q.objects)==1 and q.privileges is None
   roles={r.rolename if r.rolename else 'PUBLIC' for r in q.grantees}
   assert roles=={'PUBLIC','anon','authenticated','service_role'}
   revokes.append(q)
 assert len(owners)==len(revokes)==9
 definitions=[]
 for p,b in t.items():
  if p.startswith(base) and not p.endswith('99_private_acl.sqlinc'):
   definitions.extend(q.stmt for q in pglast.parse_sql(sql(b)) if isinstance(q.stmt,ast.CreateFunctionStmt))
 assert [signature(h) for h in definitions]==sigs
 assert all(h.replace and typ(h.returnType)=='jsonb' for h in definitions)
 roots=t[root].decode();includes=[x[4:] for x in roots.splitlines() if x.startswith('\\ir ')]
 assert includes==[p.removeprefix('supabase/repeatable/') for p in t if p.startswith(base)]
 assert 'do ' not in sql(t[root]).lower() and 'create ' not in sql(t[migration]).lower()
 prog=pglast.parse_plpgsql(sql(t[migration]))[0]['PLpgSQL_function']
 loop=prog['action']['PLpgSQL_stmt_block']['body'][0]['PLpgSQL_stmt_foreach_a']
 names=pglast.parse_sql('select '+loop['expr']['PLpgSQL_expr']['query'])[0].stmt.targetList[0].val.elements
 assert [x.val.sval for x in names]==[n for n,_,_,_ in spec]
 cond=pglast.parse_sql('select '+loop['body'][0]['PLpgSQL_stmt_if']['cond']['PLpgSQL_expr']['query'])[0].stmt.targetList[0].val.subselect
 assert cond.limitCount is None and cond.limitOffset is None and cond.distinctClause is None
 columns={'.'.join(x.sval for x in n.fields) for n in walk(cond) if isinstance(n,ast.ColumnRef)}
 assert columns=={'namespace.oid','routine.pronamespace','namespace.nspname','routine.proname','routine_name'}
 # All overloads and procedure/function kinds are covered: no signature/kind filter.
 assert {n.relname for n in walk(cond) if isinstance(n,ast.RangeVar)}=={'pg_proc','pg_namespace'}
 v=sql(t[verifier]);tree=pglast.parse_sql(v)
 assert isinstance(tree[-1].stmt,ast.TransactionStmt) and tree[-1].stmt.kind.name=='TRANS_STMT_ROLLBACK'
 for s,(_,_,vol,path) in zip(sigs,spec):assert "('"+s+"','jsonb','"+vol+"','"+path+"')" in v
 expressions=[d['PLpgSQL_expr']['query'] for d in dictionaries(pglast.parse_plpgsql(v)) if 'PLpgSQL_expr' in d]
 all_expressions='\n'.join(expressions)
 for token in ['actual.prokind','actual.proretset','actual.prorettype','actual.proowner','actual.prosecdef','actual.provolatile','actual.proconfig','actual.lanname','actual.nspname','permission.grantee<>actual.proowner']:
  assert token in all_expressions
 assert "array['anon','authenticated','service_role']" in all_expressions
 assert 'pg_catalog.has_function_privilege' in all_expressions and 'pg_catalog.aclexplode' in all_expressions
 assert not any(x in all_expressions for x in ['perform private.','public.timesheet_contract_move_preview_v1(', 'private.daily_rate_correction_review_v1('])
 return True
assert contract(targets)
negative=[]
def reject(label,path,before,after):
 mutant=copy.copy(targets);assert mutant[path].count(before)==1
 mutant[path]=mutant[path].replace(before,after)
 try:contract(mutant)
 except (AssertionError,pglast.Error):negative.append(label)
 else:raise AssertionError('mutation admitted: '+label)
reject('service grant',base+'99_private_acl.sqlinc',b'revoke all on function '+sigs[0].encode()+b' from public,anon,authenticated,service_role;',b'grant execute on function '+sigs[0].encode()+b' to service_role;')
# One targeted first-line omission (all identical role clauses otherwise occur nine times).
reject('service revoke omitted',base+'99_private_acl.sqlinc',('revoke all on function '+sigs[0]+' from public,anon,authenticated,service_role;').encode(),('revoke all on function '+sigs[0]+' from public,anon,authenticated;').encode())
reject('overload-only namespace',migration,b'routine.proname=routine_name)',b'routine.proname=routine_name and routine.pronargs=1)')
reject('function-only namespace',migration,b'routine.proname=routine_name)',b"routine.proname=routine_name and routine.prokind='f')")
reject('missing exact include',root,b'\\ir 17102026_1251_health_closed_private_helpers/04_daily_current_placement.sqlinc',b'-- omitted current placement')
reject('security function-kind check omitted',verifier,b"actual.prokind<>'f'",b'false')
reject('security effective role omitted',verifier,b"array['anon','authenticated','service_role']",b"array['anon','authenticated']")
reject('unapproved original body change',base+'03_daily_revision_apply.sqlinc',b"raise exception 'DAILY_RATE_NATIVE_PUBLICATION_UNAVAILABLE'",b"raise exception 'unreviewed alternative'")
# Whole metadata inverse independently executes raw bounded slices; JSON equality alone is insufficient.
inverse=[]
for edit in load('FULL_METADATA_EDITS.json'):
 b=targets[edit['target']];parent=git(ROOT,'show',PARENT+':'+edit['target'])
 assert parent==read('current/'+edit['target'])
 for h in reversed(edit['hunks']):
  offset=h['offset'];old=h['before'].encode();new=h['after'].encode()
  assert b[offset:offset+len(new)]==new
  b=b[:offset]+old+b[offset+len(new):]
 assert b==parent and sha(b)==edit['beforeSha256']
 inverse.append({'target':edit['target'],'parentSha256':sha(parent),'postimageSha256':sha(targets[edit['target']]),'hunks':len(edit['hunks'])})
old=json.loads(read('current/'+registry));new=json.loads(targets[registry]);assert new['chains'][:1650]==old['chains'] and [r['signature'] for r in new['chains'][1650:]]==sigs
assert {k:v for k,v in new.items() if k!='chains'}=={k:v for k,v in old.items() if k!='chains'}
for field in ['verificationFiles','newVerificationFiles']:
 assert json.loads(targets[release])[field]==json.loads(read('current/'+release))[field]+[verifier]
assert json.loads(targets[lock])['migrations'][:-1]==json.loads(read('current/'+lock))['migrations']
historical=[]
for p in load('SOURCE_PROVENANCE.json'):
 b=targets[p['target']]
 assert b==git(ROOT,'show',p['sourceCommit']+':'+p['source'])==git(ROOT,'show',PARENT+':'+p['source'])
 historical.append({'target':p['target'],'sourceCommit':p['sourceCommit'],'sha256':sha(b)})
actual_parent=[]
for row in selection['targets']:
 p=ROOT/row['target']
 if row['new']:
  assert git(ROOT,'ls-tree','--name-only',PARENT,'--',row['target'])==b''
  state='ABSENT_AT_PARENT'
 else:
  parent=git(ROOT,'show',PARENT+':'+row['target']);assert p.read_bytes()==parent==git(ROOT,'show',':'+row['target'])
  state='CURRENT_SAVED_INDEX_PARENT_EQUAL'
 actual_parent.append({'target':row['target'],'parentState':state})
result={'verdict':'ACCEPT_BOUNDED_SOURCE_ONLY','frozenCommit':FROZEN,'parent':PARENT,'frozenFiles':42,'selectedTargets':13,'completeApprovedPages':historical,'privateDefinitions':9,'inverseProofs':inverse,'originalRegistryChainsPreserved':1650,'originalMigrationRowsPreserved':426,'counterexamples':negative,'actualParent':actual_parent,'sourceClassification':'No new raw F23/write/numeric hazards in ten selected SQL inputs; no allowance added.','qualification':{'native':'NOT_RUN','effectiveRoleRuntime':'NOT_RUN','publicComposers':'EXCLUDED','missingTwoUuidPublisher':'PRESERVED_55000_REFUSAL','nativeContextFinanceBoundary':'RETAINED_UNQUALIFIED','fullJoinedRelease':'NOT_PROVED'}}
if '--record' in sys.argv:(HERE/'RESULTS.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf8')
else:assert json.loads((HERE/'RESULTS.json').read_bytes())==result
print(json.dumps({'verdict':result['verdict'],'frozenFiles':42,'targets':13,'privateDefinitions':9,'wholeInverses':len(inverse),'independentNegativeCases':len(negative),'native':'NOT_RUN'}))

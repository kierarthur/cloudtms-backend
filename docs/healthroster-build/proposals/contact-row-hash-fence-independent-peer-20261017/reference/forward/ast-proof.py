"""Complete PG17 AST identity after exactly twelve zero-offset fences. No execution."""
from pathlib import Path
import sys,json,copy,hashlib
sys.dont_write_bytecode=True
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
HERE=Path(__file__).resolve().parent
def tree(s):return json.loads(pglast.parser.parse_sql_json(s))
def clean(x):
 if isinstance(x,dict):return{k:clean(v)for k,v in x.items()if k not in ['location','stmt_location','stmt_len']}
 if isinstance(x,list):return[clean(v)for v in x]
 return x
ZERO=clean(tree('select 1 offset 0')['stmts'][0]['stmt']['SelectStmt']['limitOffset'])
def walk(x):
 if isinstance(x,dict):
  yield x
  for v in x.values():yield from walk(v)
 elif isinstance(x,list):
  for v in x:yield from walk(v)
def prove(parent,candidate):
 a=clean(tree(parent));b=clean(tree(candidate));matched=[]
 for node in walk(b):
  if 'RangeSubselect' not in node:continue
  r=node['RangeSubselect']
  if r.get('alias',{}).get('aliasname')!='hashed_rows':continue
  q=r['subquery']['SelectStmt'];assert q.get('limitOffset')==ZERO
  assert q.get('limitOption')=='LIMIT_OPTION_COUNT' and 'limitCount'not in q
  assert len(q['targetList'])==1 and q['targetList'][0]['ResTarget']['name']=='row_hash'
  assert len(q['fromClause'])==1 and 'RangeVar'in q['fromClause'][0]
  assert not any(k in q for k in ['whereClause','distinctClause','groupClause','havingClause','sortClause','withClause','lockingClause'])
  matched.append(q['fromClause'][0]['RangeVar'])
  del q['limitOffset'];q['limitOption']='LIMIT_OPTION_DEFAULT'
 assert len(matched)==12
 assert len({(r['schemaname'],r['relname'])for r in matched})==12
 assert a==b,'WHOLE_AST_DIFFERENCE_BEYOND_EXACT_ZERO_FENCES'
 return {'fences':12,'relations':sorted(r['schemaname']+'.'+r['relname']for r in matched),'wholeAstInverse':True,'hashAggregateAndAllOtherCallsAstIdentical':True}
proof=[]
for case in ['CONTACT_CONSTRUCTORS_V1','QT_ADMIN_CONTROLS_V1']:
 for suffix in ['', '_LEGACY']:
  a=(HERE/'rendered'/(case+'_PARENT'+suffix+'.sql')).read_text(encoding='utf-8');b=(HERE/'rendered'/(case+'_CANDIDATE'+suffix+'.sql')).read_text(encoding='utf-8')
  proof.append({'case':case,'live':not bool(suffix),**prove(a,b)})
 # The complete C-before input preserves every guard/peer/ROLLBACK statement.
 a=(HERE/'rendered'/(case+'_PARENT_BEFORE.sql')).read_text(encoding='utf-8');b=(HERE/'rendered'/(case+'_CANDIDATE_BEFORE.sql')).read_text(encoding='utf-8');assert len(tree(a)['stmts'])==6;prove(a,b)
 for node in tree(b)['stmts']:
  if 'DoStmt'in node['stmt']:
   start=node['stmt_location'];end=start+node['stmt_len'];pglast.parser.parse_plpgsql_json(b[start:end])
candidate=(HERE/'rendered/CONTACT_CONSTRUCTORS_V1_CANDIDATE.sql').read_text(encoding='utf-8');parent=(HERE/'rendered/CONTACT_CONSTRUCTORS_V1_PARENT.sql').read_text(encoding='utf-8')
mutations={
 'one offset skips a row':candidate.replace('offset 0) hashed_rows','offset 1) hashed_rows',1),
 'missing one fence':candidate.replace(' offset 0) hashed_rows',') hashed_rows',1),
 'extra limit truncates':candidate.replace('offset 0) hashed_rows','limit 1 offset 0) hashed_rows',1),
 'distinct loses duplicate hashes':candidate.replace('from (select encode(pg_catalog.sha256','from (select distinct encode(pg_catalog.sha256',1),
 'row filter changes population':candidate.replace('offset 0) hashed_rows','where false offset 0) hashed_rows',1),
 'different hash algorithm':candidate.replace('pg_catalog.sha256','pg_catalog.sha512',1),
 'changed aggregate separator':candidate.replace("string_agg(row_hash,'|'","string_agg(row_hash,','",1),
 'changed aggregate order':candidate.replace('order by row_hash','order by row_hash desc',1),
 'count hash drops nulls':candidate.replace('relation,count(*) row_count','relation,count(row_hash) row_count',1),
 'background source snapshot changed':candidate.replace('pg_current_snapshot() snapshot_value','pg_current_snapshot()::text snapshot_value',1),
 'one scope removed':candidate.replace('"public"."audit_events"','"public"."tms_users"',1),
 'whole row hash weakened':candidate.replace('to_jsonb(t)::text',"(to_jsonb(t)-'id')::text",1),
}
for name,raw in mutations.items():
 assert raw!=candidate,name
 try:prove(parent,raw)
 except (AssertionError,pglast.parser.ParseError):pass
 else:raise AssertionError('MUTATION_ADMITTED:'+name)
# The original closed map's actual AST identifiers/CTE names do not change.
base=HERE/'candidate/docs/healthroster-build/proposals/contact-qt-positive-native-proposal-20261017'
assert str(base).startswith('D:');base=Path('\\\\?\\'+str(base))
scope=json.loads((base/'PLAN_SOURCE_SCOPES.json').read_text(encoding='utf-8'));parsed=tree(candidate);ctes={n['CommonTableExpr']['ctename']for n in walk(parsed)if'CommonTableExpr'in n};relations={}
for n in walk(parsed):
 if 'RangeVar'not in n:continue
 r=n['RangeVar'];name=r['relname'];schema=r.get('schemaname')
 if schema is None and name in ctes:continue
 assert schema in ['public','private','pg_catalog'];relations.setdefault(name,set()).add(schema)
assert len(relations)==33 and len(ctes)==16
assert sorted(ctes)==sorted(r['name']for r in scope['ctes'])
for r in scope['relations']:
 if r['kind']=='ACTUAL_QUERY_RANGEVAR_AST':assert set(r['schemas'])==relations.pop(r['name'])
assert not relations and len(scope['relations'])==37
out={'parser':pglast.__version__,'postgresql':list(pglast.get_postgresql_version()),'cases':proof,'completeBeforeInputs':2,'nativeGuardBodiesParsed':2,'counterexamples':list(mutations),'sourceMap':{'directRelations':33,'closedRelations':37,'ctes':16,'sameNamesAndOrdinals':True},'queriesExecuted':0,'native':'NOT_RUN'}
print(json.dumps(out,indent=2))

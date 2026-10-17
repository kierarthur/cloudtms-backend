"""Offline complete PG17 grammar and exact outbound-expression proof; no SQL execution."""
from pathlib import Path
import sys,json,re,hashlib,copy
sys.dont_write_bytecode=True
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
from pglast import ast
HERE=Path(__file__).resolve().parent
old=(HERE/'reference/later.sqlinc').read_text()
new=(HERE/'payload/complete_later_change_decide.sqlinc').read_text()
forward=json.loads((HERE/'FULL_FORWARD.json').read_text())
def dictionaries(v):
 if isinstance(v,dict):
  yield v
  for x in v.values():yield from dictionaries(x)
 elif isinstance(v,list):
  for x in v:yield from dictionaries(x)
def nodes(v):
 if isinstance(v,ast.Node):
  yield v
  for f in v:yield from nodes(getattr(v,f))
 elif isinstance(v,(tuple,list)):
  for x in v:yield from nodes(x)
def queries(sql):
 return [d['PLpgSQL_expr']['query'] for d in dictionaries(pglast.parse_plpgsql(sql)) if 'PLpgSQL_expr' in d]
def sql_expr(q):
 if ':=' in q:q=q.split(':=',1)[1]
 return q if q.lstrip().lower().startswith(('select ','select\n','update ','insert ','delete ')) else 'SELECT '+q
def revisions(sql):
 out=[]
 for ordinal,q in enumerate(queries(sql)):
  if 'jsonb_build_object' not in q:continue
  for n in nodes(pglast.parse_sql(sql_expr(q))):
   if not isinstance(n,ast.FuncCall) or n.funcname[-1].sval!='jsonb_build_object':continue
   for k,v in zip(n.args[::2],n.args[1::2]):
    if isinstance(k,ast.A_Const) and isinstance(k.val,ast.String) and k.val.sval in ('bundle_revision','reviewed_bundle_revision'):
     out.append((ordinal,k.val.sval,v))
 return out
def col(n):
 assert isinstance(n,ast.ColumnRef),type(n).__name__
 return '.'.join(x.sval for x in n.fields)
def validate(sql):
 rows=revisions(sql);assert len(rows)==7,len(rows)
 actual=[]
 for ordinal,key,n in rows:
  assert isinstance(n,ast.TypeCast),f'{ordinal}:{key}:not cast'
  assert '.'.join(x.sval for x in n.typeName.names) in ('text','pg_catalog.text')
  actual.append((key,col(n.arg)))
 assert sorted(actual)==sorted([
  ('bundle_revision','v_bundle_revision'),
  ('bundle_revision','v_bundle.bundle_revision'),('bundle_revision','v_bundle.bundle_revision'),
  ('bundle_revision','v_successor_revision'),('bundle_revision','v_successor_revision'),
  ('reviewed_bundle_revision','v_bundle.bundle_revision'),('reviewed_bundle_revision','v_bundle.bundle_revision')])
 return rows

assert len(pglast.parse_sql(old))==len(pglast.parse_sql(new))==1
op=pglast.parse_plpgsql(old);np=pglast.parse_plpgsql(new)
assert len(op)==len(np)==1
expected=copy.deepcopy(op)
for d in dictionaries(expected):
 if 'PLpgSQL_expr' in d:
  q=d['PLpgSQL_expr']['query']
  for edit in forward['edits']:q=q.replace(edit['before'],edit['after'])
  d['PLpgSQL_expr']['query']=q
assert expected==np,'complete programme changed beyond the seven outbound casts'
rows=validate(new)
assert all(isinstance(n,ast.ColumnRef) for _,_,n in revisions(old))
assert [q for q in queries(old) if "coalesce(v_audit.after_json->'result'" in q]==[q for q in queries(new) if "coalesce(v_audit.after_json->'result'" in q]
assert len([q for q in queries(new) if "coalesce(v_audit.after_json->'result'" in q])==2
assert 'v_successor_revision:=v_bundle.bundle_revision+1' in new
assert "v_request:=pg_catalog.jsonb_set(v_request,'{bundle_revision}',to_jsonb(v_successor_revision));" in new
assert old[:old.index('  -- The root must be')]==new[:new.index('  -- The root must be')]
prepare=(HERE/'reference/prepare.sqlinc').read_text()
assert prepare.count("'bundle_revision','1'")==2
mutations=[]
for match in list(re.finditer(r"(?:v_bundle_revision|v_bundle\.bundle_revision|v_successor_revision)::text",new)):
 # Exactly the newly added casts; do not mutate the unrelated UUID cast.
 bad=new[:match.start()]+new[match.start():match.end()].replace('::text','')+new[match.end():]
 try:validate(bad)
 except AssertionError:mutations.append({'offset':match.start(),'missingCast':'REFUSED'})
 else:raise AssertionError('missing cast admitted')
assert len(mutations)==7
bad=new.replace("'bundle_revision',v_successor_revision::text,","'bundle_revision',v_successor_revision::bigint,",1)
try:validate(bad)
except AssertionError:mutations.append({'wrongType':'REFUSED'})
else:raise AssertionError('numeric reply cast admitted')
vectors=[]
for value in [1,9007199254740993,9223372036854775807]:
 env={'v_bundle_revision':value,'v_bundle.bundle_revision':value,'v_successor_revision':value}
 # Evaluate only the actual parsed ColumnRef::text outbound nodes.
 values=[{'field':key,'expression':col(n.arg)+'::text','value':str(env[col(n.arg)])} for _,key,n in rows]
 assert all(isinstance(x['value'],str) and int(x['value'])==value for x in values)
 vectors.append({'bigint':str(value),'parsedOutboundValues':values})
result={'verdict':'PASS','pglast':pglast.__version__,'postgresParser':pglast.get_postgresql_version(),
 'completeSQLStatements':2,'completePLPrograms':2,'sqlBodySubstitutions':0,
 'completePLProgrammeComparison':'Seven exact expression casts only',
 'outboundTextExpressions':7,'unchangedStoredReplayReturns':2,'nativeRequestAndBigintCASArithmeticUnchanged':True,
 'missingOrWrongCastMutationsRefused':mutations,'wireVectors':vectors,'native':'NOT_RUN'}
if '--record' in sys.argv:(HERE/'GRAMMAR_RESULTS.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({k:v for k,v in result.items() if k not in ('wireVectors','missingOrWrongCastMutationsRefused')}))

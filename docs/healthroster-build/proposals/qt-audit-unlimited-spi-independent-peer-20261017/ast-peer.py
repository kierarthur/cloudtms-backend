import pathlib,sys,json
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
p=pathlib.Path('D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/docs/healthroster-build/proposals/qt-audit-unlimited-spi-forward-20261017')
s=(p/'payload/qt-assertions.sql').read_text();pl=json.loads(pglast.parser.parse_plpgsql_json(s))
def walk(n):
 if isinstance(n,dict):
  for k,v in n.items():yield k,v;yield from walk(v)
 elif isinstance(n,list):
  for v in n:yield from walk(v)
dyn=[v for k,v in walk(pl) if k=='PLpgSQL_stmt_dynexecute'];assert len(dyn)==1 and not dyn[0].get('params')
expr=dyn[0]['query']['PLpgSQL_expr']['query'];node=json.loads(pglast.parser.parse_sql_json('select '+expr))['stmts'][0]['stmt']['SelectStmt']['targetList'][0]['ResTarget']['val'];assert set(node)=={'A_Const'}
q=node['A_Const']['sval']['sval'];a=json.loads(pglast.parser.parse_sql_json(q));assert len(a['stmts'])==1
t=a['stmts'][0]['stmt']['SelectStmt'];assert len(t['targetList'])==1 and len(t['fromClause'])==1 and t['fromClause'][0]['RangeVar']['schemaname']=='public' and t['fromClause'][0]['RangeVar']['relname']=='audit_events'
assert not any(k in t for k in ['whereClause','groupClause','distinctClause','limitCount','havingClause','lockingClause','withClause'])
assert set(v['funcname'][0]['String']['sval'] for k,v in walk(t) if k=='FuncCall')=={'md5','string_agg','to_jsonb'}
assert len([v for k,v in walk(t) if k=='CoalesceExpr'])==1
def clean(n):
 if isinstance(n,dict):return {k:clean(v) for k,v in n.items() if k not in ['location','stmt_location','stmt_len']}
 if isinstance(n,list):return [clean(v) for v in n]
 return n
mutations=[q+' where false',q+' limit 1',q.replace('string_agg(','string_agg(distinct '),q.replace("'|'","','"),q.replace('to_jsonb(a)','to_jsonb(a.id)'),q.replace(' order by md5(to_jsonb(a)::text)','')]
for m in mutations:assert clean(json.loads(pglast.parser.parse_sql_json(m)))!=clean(a)
print(json.dumps({'independentLiteralAndScalarShape':'PASS','parsedCounterexamples':len(mutations),'native':'NOT_RUN','performance':'NOT_PROVED'}))

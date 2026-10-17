"""Parse source vectors only. No PostgreSQL, connection, EXPLAIN or query execution."""
from pathlib import Path
import sys,json,subprocess,copy
sys.dont_write_bytecode=True
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
from pglast import ast
HERE=Path(__file__).resolve().parent
q=json.loads((HERE/'QUERY.json').read_text())['unchangedFullAuditQuery']
code="import{buildInput}from "+json.dumps((HERE/'selection.mjs').as_uri())+";console.log(JSON.stringify(buildInput({physicalDatabase:'cloudtms_test_clone',databaseOid:1234,backendPid:4321,turnId:'hr_turn_'+'1'.repeat(32),key:[20261013,1],takenAt:'2026-10-17T13:00:00.000Z',task:'SYNTHETIC SOURCE TEST ONLY'})));"
built=json.loads(subprocess.check_output(['C:/Program Files/nodejs/node.exe','--input-type=module','-e',code]))
def clean(v):
 if isinstance(v,ast.Node):return {type(v).__name__:{f:clean(getattr(v,f)) for f in v if f not in ('location','stmt_location','stmt_len')}}
 if isinstance(v,(tuple,list)):return [clean(x) for x in v]
 return v
def walk(v):
 if isinstance(v,ast.Node):
  yield v
  for f in v:yield from walk(getattr(v,f))
 elif isinstance(v,(tuple,list)):
  for x in v:yield from walk(x)
def check(sql):
 parsed=pglast.parse_sql(sql);assert len(parsed)==8
 statements=[s.stmt for s in parsed];metadata,plan=statements[-2:]
 expected=pglast.parse_sql("set local transaction_read_only=on;set local jit=off;set local pg_show_plans.is_enabled=off;set local work_mem='4MB';set local maintenance_work_mem='32MB';set local search_path=pg_catalog,public;")
 assert clean(statements[:6])==clean([s.stmt for s in expected]),'fixed readonly/QT settings changed'
 assert isinstance(metadata,ast.SelectStmt) and isinstance(plan,ast.ExplainStmt)
 assert {o.defname:o.arg.sval for o in plan.options}=={'format':'json','verbose':'true','settings':'true'}
 assert clean(plan.query)==clean(pglast.parse_sql(q)[0].stmt),'planned query differs from original full digest'
 allowed={('pg_catalog','pg_settings'),('pg_catalog','pg_locks'),('pg_catalog','pg_database'),
  ('pg_catalog','pg_class'),('pg_catalog','pg_namespace'),('private','cloudtms_database_identity'),
  ('private','bpay_next_module_control'),('private','cloudtms_database_releases'),(None,'expected_settings')}
 assert {(n.schemaname,n.relname) for n in walk(metadata) if isinstance(n,ast.RangeVar)}<=allowed
 assert not any(isinstance(n,(ast.CreateStmt,ast.InsertStmt,ast.UpdateStmt,ast.DeleteStmt,ast.DoStmt)) for n in walk(parsed))
 return parsed
parsed=check(built['input']['sql']);fresh=pglast.parse_sql(built['input']['verification_sql']);assert len(fresh)==1 and isinstance(fresh[0].stmt,ast.SelectStmt)
assert not any(isinstance(n,ast.RangeVar) and n.relname=='audit_events' for n in walk(fresh))
agg=next(n for n in walk(pglast.parse_sql(q)) if isinstance(n,ast.FuncCall) and n.funcname[-1].sval=='string_agg')
assert clean(agg.args[0])==clean(agg.agg_order[0].node),'original argument/order node differs'
mutations=[
 ('ANALYZE',built['input']['sql'].replace('format json,','analyze true,format json,')),
 ('changed delimiter',built['input']['sql'].replace("'|' order by","',' order by")),
 ('row filter',built['input']['sql'].replace('from public.audit_events a;','from public.audit_events a where false;')),
 ('partial row',built['input']['sql'].replace('to_jsonb(a)::text','to_jsonb(a.id)::text')),
 ('business SELECT outside plan',built['input']['sql'].replace('from pg_catalog.pg_class c','from public.audit_events c')),
 ('extra statement',built['input']['sql']+'select * from public.audit_events;'),
 ('timeout change',built['input']['sql'].replace('transaction_read_only=on','statement_timeout=180000')),
 ('readonly off',built['input']['sql'].replace('transaction_read_only=on','transaction_read_only=off')),
 ('unrequested EXPLAIN option',built['input']['sql'].replace('format json,','buffers true,format json,'))]
refused=[]
for name,sql in mutations:
 try:check(sql)
 except AssertionError:refused.append(name)
 else:raise AssertionError('mutation admitted: '+name)
result={'verdict':'PASS_SOURCE_ONLY','parser':pglast.get_postgresql_version(),'pglast':pglast.__version__,
 'mainStatements':len(parsed),'freshStatements':len(fresh),'originalQueryASTEqual':True,
 'originalArgumentAndOrderASTShared':True,'noAnalyzeOption':True,'metadataReadsAuditBusinessRows':False,
 'mutationsRefused':refused,'SQLExecuted':False,'native':'NOT_RUN'}
if '--record' in sys.argv:(HERE/'GRAMMAR_RESULTS.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result))

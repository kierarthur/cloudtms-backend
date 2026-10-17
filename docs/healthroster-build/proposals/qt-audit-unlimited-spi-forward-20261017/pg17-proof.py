"""PG17 source/AST proof only; no native call or physical-plan claim."""
import pathlib,json,sys,re
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
HERE=pathlib.Path(__file__).resolve().parent
old=(HERE/'reference/adapter/qt-assertions.sql').read_bytes().decode();new=(HERE/'payload/qt-assertions.sql').read_bytes().decode()
digest="md5(coalesce(string_agg(md5(to_jsonb(a)::text),'|' order by md5(to_jsonb(a)::text)),''))"
before=f' select {digest} into audits_before from public.audit_events a;'
after=f' execute $qt_full_audit_digest$select {digest} from public.audit_events a$qt_full_audit_digest$ into audits_before;'
assert old.count(before)==new.count(after)==1 and new.replace(after,before)==old
def walk(n):
 if isinstance(n,dict):
  for k,v in n.items():yield k,v;yield from walk(v)
 elif isinstance(n,list):
  for v in n:yield from walk(v)
def clean(n):
 if isinstance(n,dict):return {k:clean(v) for k,v in n.items() if k not in {'location','stmt_location','stmt_len'}}
 if isinstance(n,list):return [clean(v) for v in n]
 return n
def pl(s):return json.loads(pglast.parser.parse_plpgsql_json(s))
o,n=pl(old),pl(new)
statement=[v for k,v in walk(o) if k=='PLpgSQL_stmt_execsql' and 'public.audit_events' in v['sqlstmt']['PLpgSQL_expr']['query']]
dyn=[v for k,v in walk(n) if k=='PLpgSQL_stmt_dynexecute']
assert len(statement)==len(dyn)==1
dyn=dyn[0]
assert dyn['into'] is True and not dyn.get('strict',False) and not dyn.get('params')
assert [f['name'] for f in dyn['target']['PLpgSQL_row']['fields']]==['audits_before']
expr=json.loads(pglast.parser.parse_sql_json('select '+dyn['query']['PLpgSQL_expr']['query']))['stmts'][0]['stmt']['SelectStmt']
assert len(expr['targetList'])==1
value=expr['targetList'][0]['ResTarget']['val']
assert set(value)=={'A_Const'} and value['A_Const']['sval']['sval']==f'select {digest} from public.audit_events a'
query=value['A_Const']['sval']['sval'];inner=json.loads(pglast.parser.parse_sql_json(query))
assert len(inner['stmts'])==1 and set(inner['stmts'][0]['stmt'])=={'SelectStmt'}
new_ast=inner['stmts'][0]['stmt'];old_ast=json.loads(pglast.parser.parse_sql_json(statement[0]['sqlstmt']['PLpgSQL_expr']['query']))['stmts'][0]['stmt']
assert clean(old_ast)==clean(new_ast)
def reverse_ast(v):
 if isinstance(v,dict):
  if 'PLpgSQL_stmt_dynexecute' in v:return {'PLpgSQL_stmt_execsql':statement[0]}
  return {k:reverse_ast(x) for k,x in v.items()}
 if isinstance(v,list):return [reverse_ast(x) for x in v]
 return v
assert reverse_ast(n)==o
assert 'found' not in re.sub(r"'([^']|'')*'",'',old,flags=re.I).lower()
aggregate=next(v for k,v in walk(new_ast) if k=='FuncCall' and v['funcname'][0]['String']['sval']=='string_agg')
assert clean(aggregate['args'][0])==clean(aggregate['agg_order'][0]['SortBy']['node'])
counterexamples=[]
for label,changed in [('filter',query+' where false'),('order',query.replace('order by md5(to_jsonb(a)::text)','order by a.id')),('separator',query.replace("'|'","','")),('null',query.replace(",''))",",'changed'))")),('wholeRow',query.replace('to_jsonb(a)','to_jsonb(a.id)')),('hash',query.replace('md5(','sha256(')),('distinct',query.replace('string_agg(','string_agg(distinct ')),('limit',query+' limit 1')]:
 assert clean(json.loads(pglast.parser.parse_sql_json(changed))['stmts'][0]['stmt'])!=clean(new_ast);counterexamples.append(label)
proof=json.loads((HERE/'reference/PG17_EXECUTOR_SOURCE_EXCERPTS.json').read_text());f={k:v for r in proof for k,v in r['functions'].items()}
assert 'tcount = 1;' in f['exec_stmt_execsql'] and 'tcount = 2;' in f['exec_stmt_execsql']
assert 'estate->readonly_func, tcount);' in f['exec_stmt_execsql']
assert 'options.tcount = tcount;' in f['SPI_execute_plan_with_paramlist']
assert 'options->tcount : 0' in f['_SPI_execute_plan']
assert 'ExecutorRun(queryDesc, ForwardScanDirection, tcount, true);' in f['_SPI_pquery']
assert 'if (queryDesc->already_executed || numberTuples != 0)' in f['ExecutePlan'] and 'use_parallel_mode = false;' in f['ExecutePlan']
assert 'memset(&options, 0, sizeof(options));' in f['exec_stmt_dynexecute'] and 'SPI_execute_extended(querystr, &options);' in f['exec_stmt_dynexecute']
assert 'options.tcount' not in f['exec_stmt_dynexecute']
assert 'plan.cursor_options = CURSOR_OPT_PARALLEL_OK;' in f['SPI_execute_extended']
assert 'exec_set_found' not in f['exec_stmt_dynexecute']
assert 'exec_eval_boolean(estate, stmt->cond, &isnull)' in f['exec_stmt_if']
assert 'exprdatum = exec_eval_expr(estate, expr,' in f['exec_eval_boolean']
assert 'exec_prepare_plan(estate, expr, CURSOR_OPT_PARALLEL_OK);' in f['exec_eval_expr'] and 'rc = exec_run_select(estate, expr, 0, NULL);' in f['exec_eval_expr']
assert 'estate->readonly_func, maxtuples);' in f['exec_run_select']
programmes=json.loads((HERE/'payload/PROGRAMMES.json').read_bytes());inputs=json.loads((HERE/'payload/OFFLINE_INPUTS.json').read_bytes())
print(json.dumps({'version':'QT_LITERAL_EXECUTE_UNLIMITED_SPI_SOURCE_AST_V1','parser':pglast.__version__,'postgresql':list(pglast.get_postgresql_version()),'programmes':{k:len(pglast.parse_sql(v['sql'])) for k,v in programmes.items()},'inputs':{k:{f:len(pglast.parse_sql(v['input'][f])) for f in ['sql','verification_sql']} for k,v in inputs.items()},'wholePlAstInverse':True,'aggregateSelectAstEqual':True,'aggregateArgumentSortAstEqual':True,'fixedLiteralOnly':True,'usingParameters':0,'executorTupleLimit':0,'foundConsumers':0,'finalIfPath':['exec_stmt_if','exec_eval_boolean','exec_eval_expr','CURSOR_OPT_PARALLEL_OK','exec_run_select_tcount0'],'counterexamples':counterexamples,'parallelPlan':'NOT_OBSERVED','performance':'NOT_PROVED','native':'NOT_RUN'}))

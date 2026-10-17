"""Offline PostgreSQL17 parser proof. No database, subprocess or private input."""
import pathlib,json,sys,re
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
HERE=pathlib.Path(__file__).resolve().parent
old=(HERE/'reference/adapter/qt-assertions.sql').read_bytes().decode()
new=(HERE/'payload/qt-assertions.sql').read_bytes().decode()
digest="md5(coalesce(string_agg(md5(to_jsonb(a)::text),'|' order by md5(to_jsonb(a)::text)),''))"
before=f' audits_before:=(select {digest} from public.audit_events a);'
after=f' select {digest} into audits_before from public.audit_events a;'
assert old.count(before)==1 and new.count(after)==1
assert new.replace(after,before)==old
assert 'found' not in re.sub(r"'([^']|'')*'",'',old,flags=re.I).lower()
def walk(n):
    if isinstance(n,dict):
        for k,v in n.items():
            yield k,v
            yield from walk(v)
    elif isinstance(n,list):
        for v in n:yield from walk(v)
def pl(s):return json.loads(pglast.parser.parse_plpgsql_json(s))
o,n=pl(old),pl(new)
assign=[x for k,x in walk(o) if k=='PLpgSQL_stmt_assign' and 'audits_before:=' in x['expr']['PLpgSQL_expr']['query']]
selects=[x for k,x in walk(n) if k=='PLpgSQL_stmt_execsql' and 'public.audit_events' in x['sqlstmt']['PLpgSQL_expr']['query']]
assert len(assign)==len(selects)==1
assert selects[0]['into'] is True and not selects[0].get('strict',False)
assert [f['name'] for f in selects[0]['target']['PLpgSQL_row']['fields']]==['audits_before']
oldq=assign[0]['expr']['PLpgSQL_expr']['query'].split(':=',1)[1].strip()
newq=selects[0]['sqlstmt']['PLpgSQL_expr']['query']
def clean(n):
    if isinstance(n,dict):return {k:clean(v) for k,v in n.items() if k not in {'location','stmt_location','stmt_len'}}
    if isinstance(n,list):return [clean(v) for v in n]
    return n
old_ast=json.loads(pglast.parser.parse_sql_json('select '+oldq))['stmts'][0]['stmt']['SelectStmt']['targetList'][0]['ResTarget']['val']['SubLink']['subselect']
new_ast=json.loads(pglast.parser.parse_sql_json(newq))['stmts'][0]['stmt']
assert clean(old_ast)==clean(new_ast)
aggregate=next(v for k,v in walk(new_ast) if k=='FuncCall' and v['funcname'][0]['String']['sval']=='string_agg')
assert clean(aggregate['args'][0])==clean(aggregate['agg_order'][0]['SortBy']['node'])
assert not any(k in {'whereClause','distinctClause','limitCount','groupClause'} for k,_ in walk(new_ast))
outer_old=pglast.parse_sql(old);outer_new=pglast.parse_sql(new)
assert len(outer_old)==len(outer_new)==3
# All actual programme statements still parse; no isolated toy replaces them.
programmes=json.loads((HERE/'payload/PROGRAMMES.json').read_bytes())
inputs=json.loads((HERE/'payload/OFFLINE_INPUTS.json').read_bytes())
counts={k:len(pglast.parse_sql(v['sql'])) for k,v in programmes.items()}
input_counts={k:{f:len(pglast.parse_sql(v['input'][f])) for f in ['sql','verification_sql']} for k,v in inputs.items()}
negative=[]
for label,mut in [
    ('filter',after.replace('from public.audit_events a','from public.audit_events a where false')),
    ('differentOrder',after.replace('order by md5(to_jsonb(a)::text)','order by a.id')),
    ('differentSeparator',after.replace("'|'","','")),
    ('differentEmptyValue',after.replace(",''))",",'changed'))")),
    ('differentWholeRow',after.replace('to_jsonb(a)','to_jsonb(a.id)')),
    ('differentBinding',after.replace('into audits_before','into global_before')),
    ('differentHash',after.replace('md5(', 'sha256(')),
    ('distinct',after.replace('string_agg(', 'string_agg(distinct ')),
]:
    changed=new.replace(after,mut)
    assert changed.replace(after,before)!=old
    mutant=pl(changed)
    candidate=[x for k,x in walk(mutant) if k=='PLpgSQL_stmt_execsql' and 'public.audit_events' in x['sqlstmt']['PLpgSQL_expr']['query']]
    assert len(candidate)==1
    mutant_sql=json.loads(pglast.parser.parse_sql_json(candidate[0]['sqlstmt']['PLpgSQL_expr']['query']))['stmts'][0]['stmt']
    target=[f['name'] for f in candidate[0]['target']['PLpgSQL_row']['fields']]
    assert clean(mutant_sql)!=clean(new_ast) or target!=['audits_before']
    negative.append(label)
sources={f:(HERE/'reference/postgresql-17.11'/f).read_text() for f in ['pl_exec.c','parse_agg.c','parse_clause.c','planner.c']}
assert 'exec_prepare_plan(estate, expr, 0);' in sources['pl_exec.c'][sources['pl_exec.c'].index('exec_assign_expr(PLpgSQL_execstate *estate, PLpgSQL_datum *target,\n'):]
assert 'exec_prepare_plan(estate, expr, CURSOR_OPT_PARALLEL_OK);' in sources['pl_exec.c'][sources['pl_exec.c'].index('exec_stmt_execsql(PLpgSQL_execstate *estate,\n'):]
assert 'exec_set_found(estate, (SPI_processed != 0));' in sources['pl_exec.c']
assert 'tcount = 1;' in sources['pl_exec.c'] and 'tcount = 2;' in sources['pl_exec.c']
def function(name):
    matches=re.findall(r'static\s+(?:void|int|Datum|bool)\s+\n?'+name+r'\([^;{]*\)\s*\{.*?^\}',sources['pl_exec.c'],flags=re.S|re.M)
    assert len(matches)==1,(name,len(matches))
    return matches[0]
assert 'exec_prepare_plan(estate, expr, 0);' in function('exec_assign_expr')
assert 'exec_prepare_plan(estate, expr, CURSOR_OPT_PARALLEL_OK);' in function('exec_stmt_execsql')
assert 'exec_eval_boolean(estate, stmt->cond, &isnull)' in function('exec_stmt_if')
assert 'exprdatum = exec_eval_expr(estate, expr,' in function('exec_eval_boolean')
assert 'exec_prepare_plan(estate, expr, CURSOR_OPT_PARALLEL_OK);' in function('exec_eval_expr')
assert 'rc = exec_run_select(estate, expr, 0, NULL);' in function('exec_eval_expr')
assert 'if ((cursorOptions & CURSOR_OPT_PARALLEL_OK) != 0 &&' in sources['planner.c']
assert 'true /* force SQL99 rules */' in sources['parse_agg.c']
assert 'if (equal(expr, texpr))\n\t\t\treturn tle;' in sources['parse_clause.c']
result={'version':'QT_ONE_BINDING_PG17_SOURCE_AST_PROOF_V1','parser':pglast.__version__,'postgresql':list(pglast.get_postgresql_version()),'completeAssertionOuterStatements':3,'programmes':counts,'inputs':input_counts,'originalBinding':'PLpgSQL_stmt_assign','candidateBinding':'PLpgSQL_stmt_execsql','aggregateSelectAstEqual':True,'aggregateArgumentSortAstEqual':True,'foundConsumers':0,'finalIfPath':['exec_stmt_if','exec_eval_boolean','exec_eval_expr','CURSOR_OPT_PARALLEL_OK','exec_run_select'],'counterexamples':negative,'parallelPlan':'NOT_OBSERVED','performance':'NOT_PROVED','native':'NOT_RUN'}
print(json.dumps(result))

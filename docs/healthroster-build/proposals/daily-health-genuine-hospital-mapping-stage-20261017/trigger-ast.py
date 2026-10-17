"""Actual Health trigger AST/effect vectors. Synthetic rows, no database."""
import hashlib,json,pathlib,sys
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
HERE=pathlib.Path(__file__).resolve().parent
raw=(HERE/'reference/native/05012026_fix_trg_tsfin_client_hospitals_wakeup.sql').read_bytes().decode()
assert len(pglast.parse_sql(raw))==1
pl=json.loads(pglast.parser.parse_plpgsql_json(raw))
def walk(n):
    if isinstance(n,dict):
        for k,v in n.items():
            yield k,v
            yield from walk(v)
    elif isinstance(n,list):
        for v in n:yield from walk(v)
queries=[v['sqlstmt']['PLpgSQL_expr']['query'] for k,v in walk(pl) if k=='PLpgSQL_stmt_execsql']
assert len(queries)==1
tree=json.loads(pglast.parser.parse_sql_json(queries[0]));stmt=tree['stmts'][0]['stmt']['InsertStmt']
assert stmt['relation']['schemaname']=='public' and stmt['relation']['relname']=='ts_financials_outbox'
assert [x['ResTarget']['name'] for x in stmt['cols']]==['timesheet_id','reason']
assert not [v for k,v in walk(pl) if k in {'PLpgSQL_stmt_dynexecute','PLpgSQL_stmt_perform'}]
select=stmt['selectStmt']['SelectStmt'];joins=select['fromClause'][0]['JoinExpr'];left=joins['larg']['JoinExpr'];where=select['whereClause']
assert left['larg']['RangeVar']['relname']=='timesheets' and left['rarg']['RangeVar']['relname']=='alias_norms'
assert joins['rarg']['RangeVar']['relname']=='timesheets_financials'
def strings(nodes):return[n['String']['sval'] for n in nodes]
def val(n,env):
    if 'ColumnRef'in n:
        a,c=strings(n['ColumnRef']['fields']);return env[a].get(c)
    if 'A_Const'in n:
        x=n['A_Const']
        if 'boolval'in x:return x['boolval'].get('boolval',False)
        if x.get('isnull'):return None
    raise ValueError('UNREVIEWED_VALUE_AST:'+str(n))
def pred(n,env):
    if 'BoolExpr'in n:
        b=n['BoolExpr'];xs=[pred(x,env) for x in b['args']]
        if b['boolop']=='AND_EXPR':return all(xs)
        if b['boolop']=='OR_EXPR':return any(xs)
    if 'NullTest'in n:
        x=n['NullTest'];assert x['nulltesttype']=='IS_NULL';return val(x['arg'],env)is None
    if 'A_Expr'in n:
        x=n['A_Expr'];assert strings(x['name'])==['='];a,b=val(x['lexpr'],env),val(x['rexpr'],env);return a is not None and b is not None and a==b
    raise ValueError('UNREVIEWED_PREDICATE_AST:'+str(n))
assert strings(left['quals']['A_Expr']['lexpr']['ColumnRef']['fields'])==['ts','hospital_norm']
assert strings(left['quals']['A_Expr']['rexpr']['ColumnRef']['fields'])==['a','norm']
vectors=[]
for label,change,expected in [('matching unauthorised current/no finance',{},True),('different Client still wakes',{'client_id':'unrelated-client'},True),
 ('archived flag not filtered',{'archived_at_utc':'2026-01-01'},True),('different alias',{'hospital_norm':'unrelated'},False),('history row',{'is_current':False},False),
 ('revoked row',{'revoked_at':'2026-01-01'},False),('invoice locked',{'financial':{'timesheet_id':'synthetic-ts','locked_by_invoice_id':'invoice','paid_at_utc':None}},False),
 ('already paid',{'financial':{'timesheet_id':'synthetic-ts','locked_by_invoice_id':None,'paid_at_utc':'2026-01-01'}},False),
 ('unlocked unpaid financial',{'financial':{'timesheet_id':'synthetic-ts','locked_by_invoice_id':None,'paid_at_utc':None}},True)]:
    ts={'timesheet_id':'synthetic-ts','is_current':True,'revoked_at':None,'hospital_norm':'daily_health_basis_20261017_client','authorised_at_server':None,**{k:v for k,v in change.items() if k!='financial'}}
    env={'ts':ts,'a':{'norm':'daily_health_basis_20261017_client'},'tf':change.get('financial',{'timesheet_id':None,'locked_by_invoice_id':None,'paid_at_utc':None})}
    actual=pred(left['quals'],env) and pred(where,env);assert actual==expected,label
    vectors.append({'case':label,'selectsOutboxTarget':actual})
columns=[strings(v['fields']) for k,v in walk(where) if k=='ColumnRef']
assert not any(c[-1]in {'client_id','authorised_at_server','archived_at_utc'} for c in columns)
result={'version':'DAILY_GENUINE_S1A_HEALTH_TRIGGER_SOURCE_AST_V1','parser':pglast.__version__,'postgresql':list(pglast.get_postgresql_version()),'rawSourceSha256':hashlib.sha256(raw.encode()).hexdigest(),
 'outerGrammar':'PASS','plpgsqlGrammar':'PASS','actualWriteStatements':1,'actualTarget':'public.ts_financials_outbox','actualSqlAst':tree,'actualPredicateVectors':vectors,
 'globalAliasVacancyRequired':True,'clientOnlyVacancyInsufficient':True,'oldArchivedCurrentRowsNotExcluded':True,'nativeCompilation':'NOT_PROVED','nativeExecution':'NOT_RUN','substitutions':0}
(HERE/'TRIGGER_AST_PROOF.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({'outerGrammar':'PASS','plGrammar':'PASS','predicateVectors':len(vectors),'native':'NOT_RUN'}))

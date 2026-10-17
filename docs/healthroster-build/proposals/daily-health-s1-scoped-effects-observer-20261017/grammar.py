"""Offline PG17 parse only. No SQL execution and no native/body completion claim."""
import sys, os, json, subprocess, pathlib, hashlib
os.environ['PYTHONDONTWRITEBYTECODE']='1'
sys.path.insert(0,r'D:\Development\Legacy-Work\C-tmp\healthroster-v6-invoice-parser-20261016')
from pglast.parser import parse_sql_json
HERE=pathlib.Path(__file__).resolve().parent
def read(p):
    return pathlib.Path('\\\\?\\'+str(p)).read_text(encoding='utf-8')
# A literal file URL is passed as data to a dynamic import. It is not shell code.
runner="const m=await import(process.argv[1]);const c={identity:{physicalDatabase:'cloudtms_test_clone',databaseOid:123,backendPid:456,key:[20261013,1],turnId:'hr_turn_'+'a'.repeat(32)},actorId:'f717d043-0000-4000-8000-000000000099',phase:'BEFORE'};process.stdout.write(JSON.stringify({inputs:['METADATA','EFFECTS'].map(k=>m.build(k,c).input),scopes:m.scopedSql}));"
data=json.loads(subprocess.check_output(['node','--input-type=module','-e',runner,(HERE/'observer.mjs').as_uri()],text=True))
def walk(v):
    if isinstance(v,dict):
        yield v
        for x in v.values(): yield from walk(x)
    elif isinstance(v,list):
        for x in v: yield from walk(x)
def tree(sql): return json.loads(parse_sql_json(sql))
def closed_select(sql,outer=False):
    t=tree(sql)
    kinds={k for n in walk(t) for k in n if k.endswith('Stmt')}
    assert kinds <= ({'SelectStmt','VariableSetStmt'} if outer else {'SelectStmt'}),kinds
    for n in walk(t):
        s=n.get('SelectStmt')
        if s: assert not s.get('intoClause') and not s.get('lockingClause')
        f=n.get('FuncCall')
        if f:
            names=[x['String']['sval'] for x in f['funcname']]
            assert len(names)==2 and names[0]=='pg_catalog',names
            assert names[1] not in ['nextval','setval','set_config','pg_sleep','pg_advisory_lock','pg_advisory_xact_lock'],names
    return t
input_results=[]
for i in data['inputs']:
    t=closed_select(i['sql'],True);assert len(t['stmts'])==2
    setting=t['stmts'][0]['stmt']['VariableSetStmt'];assert setting['name']=='transaction_read_only' and setting['is_local']
    assert len(closed_select(i['verification_sql'])['stmts'])==1
    input_results.append({'mode':'METADATA' if '_METADATA_' in i['label'] else 'EFFECTS','sqlSha256':hashlib.sha256(i['sql'].encode()).hexdigest(),'statements':2,'freshStatements':1})
expected={'client':{'public.clients'},'settings':{'public.client_settings'},'audit':{'public.audit_events'},'contracts':{'public.contracts'},'plannedWeeks':{'public.contract_weeks','public.contracts'},'clientsCounter':{'public.app_change_counters'},'counterTransaction':{'public.banking_pay_scope_change_transactions','public.app_change_counters'}}
for key,sql in data['scopes'].items():
    t=closed_select(sql);rels={n['RangeVar'].get('schemaname','')+'.'+n['RangeVar']['relname'] for n in walk(t) if 'RangeVar'in n};assert rels==expected[key],(key,rels)
    for n in walk(t):
        if 'SelectStmt'in n:
            s=n['SelectStmt'];assert not s.get('distinctClause') and not s.get('groupClause') and not s.get('limitCount') and not s.get('limitOffset')
    assert 'where' in sql.lower()
# Every direct data-changing mutation refuses the read-only AST boundary.
negatives=['insert into public.clients(id) values(null)','update public.clients set name=name','delete from public.clients','select * into temp x from public.clients','select * from public.clients for update',"select pg_catalog.nextval('public.global_rev_seq')"]
for sql in negatives:
    try: closed_select(sql)
    except AssertionError: pass
    else: raise AssertionError('mutation admitted: '+sql)
selection=json.loads(read(HERE/'SOURCE_SELECTION.json'))
for c in selection['captures']:
    text=read(HERE/c['capture']);assert len(tree(text)['stmts'])==1
result={'version':'S1_OBSERVER_OFFLINE_GRAMMAR_V1','parser':'pglast7.20/PG17','inputs':input_results,'scopeRelationSets':expected|{},'completeSourceStatementsParsed':len(selection['captures']),'readonlyAstNegatives':len(negatives),'sqlExecution':'NOT_RUN','nativeBodyCompilation':'NOT_PROVED'}
result['scopeRelationSets']={k:sorted(v)for k,v in expected.items()}
dest=HERE/'GRAMMAR.json'
if '--record' in sys.argv:
    assert not (HERE/'MANIFEST.json').exists()
    pathlib.Path('\\\\?\\'+str(dest)).write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
else: assert json.loads(read(dest))==result
print(json.dumps({'inputs':2,'scopes':7,'sourceStatements':len(selection['captures']),'readonlyNegatives':len(negatives),'grammar':'PASS','native':'NOT_RUN'}))

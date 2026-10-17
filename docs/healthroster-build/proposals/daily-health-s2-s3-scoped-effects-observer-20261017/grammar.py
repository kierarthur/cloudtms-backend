"""PG17 source parsing and actual original-scope proof, never SQL execution."""
import sys,os,json,pathlib,subprocess,hashlib
os.environ['PYTHONDONTWRITEBYTECODE']='1'
sys.path.insert(0,r'D:\Development\Legacy-Work\C-tmp\healthroster-v6-invoice-parser-20261016')
from pglast.parser import parse_sql_json,parse_plpgsql_json
HERE=pathlib.Path(__file__).resolve().parent
def read(p):return pathlib.Path('\\\\?\\'+str(p)).read_text(encoding='utf-8')
def walk(v):
    if isinstance(v,dict):
        yield v
        for x in v.values():yield from walk(x)
    elif isinstance(v,list):
        for x in v:yield from walk(x)
def tree(s):return json.loads(parse_sql_json(s))
def closed_select(sql,outer=False):
    t=tree(sql);kinds={k for n in walk(t)for k in n if k.endswith('Stmt')}
    assert kinds<=({'SelectStmt','VariableSetStmt'}if outer else{'SelectStmt'}),kinds
    for n in walk(t):
        s=n.get('SelectStmt')
        if s:assert not s.get('intoClause')and not s.get('lockingClause')
        f=n.get('FuncCall')
        if f:
            names=[x['String']['sval']for x in f['funcname']];assert len(names)==2 and names[0]=='pg_catalog',names
            assert names[1]not in['nextval','setval','set_config','pg_sleep','pg_advisory_lock','pg_advisory_xact_lock']
    return t
runner="const m=await import(process.argv[1]);const c={identity:{physicalDatabase:'cloudtms_test_clone',databaseOid:123,backendPid:456,key:[20261013,1],turnId:'hr_turn_'+'a'.repeat(32)},actorId:'f717d043-0000-4000-8000-000000000099',phase:'BEFORE'};process.stdout.write(JSON.stringify(['S2','S3'].flatMap(stage=>['METADATA','EFFECTS'].map(mode=>({stage,mode,input:m.build(mode,{...c,stage}).input})))));"
inputs=json.loads(subprocess.check_output(['node','--input-type=module','-e',runner,(HERE/'observer.mjs').as_uri()],text=True))
proof=[]
for row in inputs:
    t=closed_select(row['input']['sql'],True);assert len(t['stmts'])==2
    s=t['stmts'][0]['stmt']['VariableSetStmt'];assert s['name']=='transaction_read_only'and s['is_local']
    assert len(closed_select(row['input']['verification_sql'])['stmts'])==1
    proof.append({'stage':row['stage'],'mode':row['mode'],'sqlSha256':hashlib.sha256(row['input']['sql'].encode()).hexdigest(),'statements':2,'freshStatements':1})
def sval(v):return v.get('A_Const',{}).get('sval',{}).get('sval')if isinstance(v,dict)else None
scope_proofs=[]
for stage in['S2','S3']:
    original=json.loads(read(HERE/('reference/operator-'+stage+'-synthetic-input.json')))['input'];t=closed_select(original['sql'],True)
    scope=[]
    for n in walk(t):
        f=n.get('FuncCall')
        if not f:continue
        a=f.get('args',[])
        for k in range(0,len(a)-1,2):
            if sval(a[k])=='scope':scope.append(a[k+1]['FuncCall']['args'])
    assert len(scope)==1;fields=scope[0];assert len(fields)==22
    found={}
    for k in range(0,len(fields),2):
        key=sval(fields[k]);args=fields[k+1]['FuncCall']['args'];assert sval(args[0])=='count'
        q=args[1]['SubLink']['subselect']['SelectStmt']['fromClause'][0]['RangeSubselect']['subquery'];found[key]=q
        for n in walk(q):
            s=n.get('SelectStmt')
            if s:assert not s.get('limitCount')and not s.get('limitOffset')and not s.get('distinctClause')and not s.get('groupClause')
    assert set(found)=={'clients','settings','mappings','candidates','candidateJobs','rates','overrides','timesheets','financials','outbox','audit'}
    # Prove the real scope's raw WHERE columns; no enabled/current/paid-only filter
    # narrows vacancy. A current producer subset is not the complete before scope.
    required={'rates':{'client_id'},'overrides':{'candidate_id','client_id'},'financials':{'candidate_id','client_id','timesheet_id'},'candidateJobs':{'candidate_id'}}
    for key,wanted in required.items():
        q=found[key]['SelectStmt'];cols={n['ColumnRef']['fields'][-1]['String']['sval']for n in walk(q['whereClause'])if 'ColumnRef'in n and'String'in n['ColumnRef']['fields'][-1]};assert cols==wanted,(key,cols)
        assert q.get('whereClause')
    for key in['timesheets','outbox','financials','rates','overrides']:
        text=json.dumps(found[key]);assert'"sval": "is_current"'not in text and'"sval": "disabled_at_utc"'not in text and'"sval": "locked_by_invoice_id"'not in text and'"sval": "paid_at_utc"'not in text
    scope_proofs.append({'stage':stage,'completeScopes':11,'completeRateAndOverrideHistory':True,'completeFinancialAndTimesheetHistory':True,'noPaginationOrDeduplication':True})
native=[]
selection=json.loads(read(HERE/'SOURCE_SELECTION.json'))
for row in selection['nativeStatements']:
    text=read(HERE/row['capture']);assert len(tree(text)['stmts'])==1;pl=json.loads(parse_plpgsql_json(text));queries=[n['PLpgSQL_expr']['query']for n in walk(pl)if'PLpgSQL_expr'in n]
    inserts=[]
    for q in queries:
        if q.lstrip().lower().startswith(('insert','with')):
            qt=tree(q);inserts.extend(n['InsertStmt']for n in walk(qt)if'InsertStmt'in n)
    if'trg_tsfin_candidates_wakeup'in row['signature']:
        assert len(inserts)==2
        for ins in inserts:
            assert ins['relation']['schemaname']=='public'and ins['relation']['relname']=='ts_financials_outbox'
            oc=ins['onConflictClause'];assert oc['action']=='ONCONFLICT_UPDATE';assert[len(oc['targetList'])]==[1]and oc['targetList'][0]['ResTarget']['name']=='next_attempt_at'
    native.append({'signature':row['signature'],'plGrammar':'PASS','outboxInsertStatements':len(inserts),'catalogueFieldBinding':'NOT_PROVED'})
negatives=['delete from public.candidates','update public.rates_client_defaults set date_to=null','insert into public.candidate_job_titles default values','select * into temp forbidden from public.candidates','select * from public.candidates for update',"select pg_catalog.nextval('public.candidate_tmsref_seq')"]
for q in negatives:
    try:closed_select(q)
    except AssertionError:pass
    else:raise AssertionError('readonly mutation admitted')
result={'version':'S2_S3_OFFLINE_SOURCE_GRAMMAR_V1','parser':'pglast7.20/PG17','inputs':proof,'originalScopeProof':scope_proofs,'healthNativeSourceGrammar':native,'readonlyNegatives':len(negatives),'nativeExecution':'NOT_RUN'}
dest=HERE/'GRAMMAR.json'
if'--record'in sys.argv:
    assert not(HERE/'MANIFEST.json').exists();pathlib.Path('\\\\?\\'+str(dest)).write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
else:assert json.loads(read(dest))==result
print(json.dumps({'fixedInputs':4,'existingOperatorScopes':11,'healthTriggerGrammars':2,'nativeOutboxInsertStatements':2,'readonlyNegatives':len(negatives),'native':'NOT_RUN'}))

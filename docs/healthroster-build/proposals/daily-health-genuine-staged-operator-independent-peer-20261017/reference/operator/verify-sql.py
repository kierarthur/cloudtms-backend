"""Exact generated read-only PG17 grammar; no catalogue, native or execution claim."""
import json,sys,pathlib
sys.dont_write_bytecode=True
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
from pglast import ast
HERE=pathlib.Path(__file__).resolve().parent
vectors=json.loads((HERE/'OFFLINE_NATIVE_INPUTS.json').read_bytes())
allowed_relations={'public.clients','public.client_hospitals','public.client_settings','public.candidates','public.candidate_job_titles','public.rates_client_defaults','public.rates_candidate_overrides','public.timesheets','public.timesheets_financials','public.ts_financials_outbox','public.audit_events','public.tms_users','private.cloudtms_database_identity','private.bpay_next_module_control','private.cloudtms_database_releases',
 'pg_catalog.pg_locks','pg_catalog.pg_database','pg_catalog.pg_class','pg_catalog.pg_namespace','pg_catalog.pg_attribute','pg_catalog.pg_attrdef','pg_catalog.pg_constraint','pg_catalog.pg_trigger','pg_catalog.pg_rewrite','pg_catalog.pg_policy','pg_catalog.pg_proc'}
allowed_unqualified={'target_timesheets','target_financials','expected_routines','selected_routines','catalogue'}
for vector in vectors:
    for field in ['sql','verification_sql']:
        tree=json.loads(pglast.parser.parse_sql_json(vector['input'][field]))
        def walk(n):
            if isinstance(n,dict):
                for k,v in n.items():
                    yield k,v
                    yield from walk(v)
            elif isinstance(n,list):
                for v in n:yield from walk(v)
        statements=tree['stmts']
        assert len(statements)==(2 if field=='sql' else 1)
        if field=='sql':
            setting=statements[0]['stmt']['VariableSetStmt']
            assert setting['name']=='transaction_read_only' and setting['is_local']
        assert 'SelectStmt' in statements[-1]['stmt']
        for kind,value in walk(tree):
            assert kind not in {'InsertStmt','UpdateStmt','DeleteStmt','MergeStmt','CreateStmt','CreateFunctionStmt','DoStmt','CopyStmt','CallStmt','TransactionStmt'},kind
            if kind=='RangeVar':
                name=value.get('schemaname','')+'.'+value['relname']
                assert name in allowed_relations or value.get('schemaname') is None and value['relname'] in allowed_unqualified,name
            if kind=='SelectStmt':assert not value.get('intoClause') and not value.get('lockingClause')
            if kind=='FuncCall':
                names=[item['String']['sval'] for item in value['funcname']]
                assert len(names)==2 and names[0]=='pg_catalog',names
print(json.dumps({'vectors':len(vectors),'statements':len(vectors)*3,'pg17Grammar':'PASS','onlyLocalReadOnlyAndSelect':True,'businessDmlStatements':0,'native':'NOT_RUN'}))

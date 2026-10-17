"""Actual generated PG17 AST: finite metadata-only range vars, no business heap evaluation."""
import sys,pathlib,json
sys.dont_write_bytecode=True
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
HERE=pathlib.Path(__file__).resolve().parent
scope=json.loads((HERE/'S0_SCOPE_DOSSIER.json').read_bytes())
expected={(r['schema'],r['name']) for r in scope['exactHeaps']}
original=json.loads((HERE.parent/'daily-health-genuine-staged-operator-20261017/OFFLINE_NATIVE_INPUTS.json').read_bytes())[0]['input']['sql']
def walk(n):
    if isinstance(n,dict):
        for k,v in n.items():yield k,v;yield from walk(v)
    elif isinstance(n,list):
        for v in n:yield from walk(v)
tree=json.loads(pglast.parser.parse_sql_json(original))
actual={(v.get('schemaname'),v['relname']) for k,v in walk(tree) if k=='RangeVar' and v.get('schemaname') in ['public','private']}
assert actual==expected and len(actual)==15
probe=json.loads((HERE/'S0_CATALOGUE_SYNTHETIC_GRAMMAR_INPUT.json').read_bytes())
allowed={'pg_namespace','pg_class','pg_index','pg_opclass','pg_amproc','pg_amop','pg_operator','pg_attribute','pg_type','pg_database','pg_locks','pg_inherits','pg_foreign_table','pg_rewrite','pg_policy','pg_enum','pg_am','pg_collation','pg_depend','pg_extension','pg_proc','pg_language'}
ctes={'wanted','selected_relations','selected_indexes','selected_classes','support_ids','type_walk','selected_types'}
count=0
for field in ['sql','verification_sql']:
    tree=json.loads(pglast.parser.parse_sql_json(probe['input'][field]));count+=len(tree['stmts'])
    assert len(tree['stmts'])==(2 if field=='sql' else 1)
    if field=='sql':assert tree['stmts'][0]['stmt']['VariableSetStmt']['name']=='transaction_read_only' and tree['stmts'][0]['stmt']['VariableSetStmt']['is_local']
    for k,v in walk(tree):
        assert k not in {'InsertStmt','UpdateStmt','DeleteStmt','MergeStmt','DoStmt','CreateFunctionStmt','CallStmt','CopyStmt','TransactionStmt'}
        if k=='RangeVar':assert v.get('schemaname')=='pg_catalog' and v['relname'] in allowed or not v.get('schemaname') and v['relname'] in ctes
        if k=='SelectStmt':assert not v.get('intoClause') and not v.get('lockingClause')
        if k=='FuncCall':assert [x['String']['sval'] for x in v['funcname']][0]=='pg_catalog'
print(json.dumps({'actualOriginalHeapSet':15,'catalogueProbeStatements':count,'pg17Grammar':'PASS','businessHeapRangeVarsInProbe':0,'businessRowsEvaluated':0,'native':'NOT_RUN'}))

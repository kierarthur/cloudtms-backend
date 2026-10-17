"""Actual S1 RETURN AST and truthful catalogue-dependent full PL hold. No DB."""
import hashlib,json,pathlib,re,sys
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
HERE=pathlib.Path(__file__).resolve().parent
raw=(HERE/'reference/current/22092026_1226_client_initial_settings_baseline_v1.sql').read_bytes().decode()
source=raw[raw.index('CREATE OR REPLACE FUNCTION'):raw.index('$function$;')+len('$function$;')]
assert len(pglast.parse_sql(source))==1
try:
    pglast.parser.parse_plpgsql_json(source)
    pl={'status':'GRAMMAR_ONLY_PASS_NATIVE_NOT_PROVED'}
except Exception as error:pl={'status':'NOT_PROVED_CATALOGUE_DEPENDENT','reason':str(error)}
query=source[source.rindex('RETURN jsonb_build_object(')+len('RETURN '):source.rindex('\nEND;')].strip()
assert query.endswith(';');query=query[:-1]
tree=json.loads(pglast.parser.parse_sql_json('SELECT '+query))
def walk(n):
    if isinstance(n,dict):
        for k,v in n.items():
            yield k,v
            yield from walk(v)
    elif isinstance(n,list):
        for v in n:yield from walk(v)
calls=[v for k,v in walk(tree) if k=='FuncCall' and [s['String']['sval'] for s in v['funcname']]==['public','import_auto_authorise_policy_resolve_v1']]
assert len(calls)==2
for call,source_kind in zip(calls,['HEALTHROSTER','NHSP']):
    assert len(call['args'])==4
    assert call['args'][0]['TypeCast']['arg']['A_Const']['sval']['sval']==source_kind
    # pglast/PG17 JSON omits the protobuf boolean's default false value.
    assert call['args'][3]['A_Const']['boolval']=={}
result={'version':'ACTUAL_CLIENT_RETURN_SOURCE_AST_V1','parser':pglast.__version__,'postgresql':list(pglast.get_postgresql_version()),
 'actualCompleteFunctionSha256':hashlib.sha256(source.encode()).hexdigest(),'completeOuterGrammar':'PASS','completePlBody':pl,
 'actualReturnExpression':query,'actualReturnAst':tree,'genuinePolicyCalls':calls,'nativeInvocation':'NOT_RUN','substitutions':0}
(HERE/'CLIENT_RETURN_AST.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({'outerGrammar':'PASS','fullPlBody':pl['status'],'actualReturnPolicyCalls':2,'native':'NOT_RUN'}))

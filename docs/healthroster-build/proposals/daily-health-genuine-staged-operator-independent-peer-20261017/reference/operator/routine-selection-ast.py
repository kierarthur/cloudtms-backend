"""Evaluate exact generated PG17 join/presence AST against synthetic catalogue rows only."""
import json,sys,pathlib,copy
sys.dont_write_bytecode=True
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
HERE=pathlib.Path(__file__).resolve().parent
meta=json.loads((HERE/'reference/audit/SOURCE_SCHEMA_EFFECT_BOUNDARIES.json').read_bytes())
mapping=json.loads((HERE/'reference/mapping/SOURCE_METADATA.json').read_bytes())
direct=json.loads((HERE/'reference/audit/FINAL_SOURCE_REVIEW.json').read_bytes())['distinctCustomDirectRoutines']
rows=meta['callbackRoutines']+meta['producerRoutineMetadata']+[mapping['triggerRoutine']]+[r['baselineMetadata'] for r in direct]
expected={(r['schema'],r['identity'].split('(')[0].split('.')[-1]) for r in rows}
vectors=json.loads((HERE/'OFFLINE_NATIVE_INPUTS.json').read_bytes())
checks=0
for vector in vectors:
    query=json.loads(pglast.parser.parse_sql_json(vector['input']['sql']))['stmts'][1]['stmt']['SelectStmt']
    ctes={x['CommonTableExpr']['ctename']:x['CommonTableExpr']['ctequery']['SelectStmt'] for x in query['withClause']['ctes']}
    values=ctes['expected_routines']['fromClause'][0]['RangeSubselect']['subquery']['SelectStmt']['valuesLists']
    actual={tuple(c['A_Const']['sval']['sval'] for c in r['List']['items']) for r in values}
    assert actual==expected and len(values)==len(expected)
    routine_query=ctes['selected_routines']
    args=query['targetList'][0]['ResTarget']['val']['FuncCall']['args']
    ix=next(i for i,a in enumerate(args) if a.get('A_Const',{}).get('sval',{}).get('sval')=='catalogueComplete')
    presence=args[ix+1]['BoolExpr']['args'][0] # exact generated NOT EXISTS(missing expected tuple)
    data={}
    def expr(node,env):
        kind,value=next(iter(node.items()))
        if kind=='ColumnRef':
            fields=[f['String']['sval'] for f in value['fields']]
            assert len(fields)==2
            return env[fields[0]][fields[1]]
        if kind=='A_Expr':
            assert value['kind']=='AEXPR_OP' and value['name']==[{'String':{'sval':'='}}]
            return expr(value['lexpr'],env)==expr(value['rexpr'],env)
        if kind=='BoolExpr':
            values=[expr(n,env) for n in value['args']]
            if value['boolop']=='AND_EXPR':return all(values)
            if value['boolop']=='NOT_EXPR':assert len(values)==1;return not values[0]
            raise AssertionError(value['boolop'])
        if kind=='SubLink':
            assert value['subLinkType']=='EXISTS_SUBLINK'
            return bool(select(value['subselect']['SelectStmt'],env))
        raise AssertionError(kind)
    def from_node(node,env):
        if 'RangeVar' in node:
            r=node['RangeVar'];return [dict(env,**{r['alias']['aliasname']:x}) for x in data[r['relname']]]
        j=node['JoinExpr'];assert j['jointype']=='JOIN_INNER'
        return [right for left in from_node(j['larg'],env) for right in from_node(j['rarg'],left) if expr(j['quals'],right)]
    def select(q,env):
        assert len(q['fromClause'])==1
        return [x for x in from_node(q['fromClause'][0],env) if 'whereClause' not in q or expr(q['whereClause'],x)]
    schemas={'public':101,'private':102}
    native=[{'proname':name,'pronamespace':schemas[schema],'oid':i+1000} for i,(schema,name) in enumerate(sorted(expected))]
    def evaluate(native_rows):
        data.update(pg_proc=native_rows,pg_namespace=[{'nspname':k,'oid':v} for k,v in schemas.items()],expected_routines=[{'schema_name':s,'routine_name':n} for s,n in sorted(expected)])
        selected=select(routine_query,{})
        data['selected_routines']=[dict(e['p'],nspname=e['n']['nspname']) for e in selected]
        return expr(presence,{}),data['selected_routines']
    complete,selected=evaluate(native);assert complete and len(selected)==len(expected);checks+=1
    for i,original in enumerate(native):
        # Every callback, producer and direct dependency must be present in its genuine schema.
        assert evaluate(native[:i]+native[i+1:])[0] is False;checks+=1
        wrong=copy.deepcopy(native);wrong[i]['pronamespace']=101 if original['pronamespace']==102 else 102
        assert evaluate(wrong)[0] is False;checks+=1
    # Same-schema extra overload is retained; a wrong-schema overload cannot enter the selected image.
    extra=dict(native[0],oid=900001);complete,selected=evaluate(native+[extra]);assert complete and len(selected)==len(native)+1;checks+=1
    extra['pronamespace']=101 if extra['pronamespace']==102 else 102
    complete,selected=evaluate(native+[extra]);assert complete and len(selected)==len(native);checks+=1
print(json.dumps({'version':'DAILY_GENUINE_ROUTINE_SELECTION_AST_V1','generatedInputs':len(vectors),'expectedSchemaNameTuples':len(expected),'actualJoinAndPresenceChecks':checks,'wrongSchemaAndMissingEachRefused':True,'sameSchemaOverloadsRetained':True,'native':'NOT_RUN'}))

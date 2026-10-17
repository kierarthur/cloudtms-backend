"""Evaluate the actual parsed admission predicate over finite synthetic catalogues.
No database, connector, fixture mutation or native result is involved.
"""
import copy,hashlib,json,pathlib,re,sys
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
HERE=pathlib.Path(__file__).resolve().parent
PAIRS=[('private','weekly_contact_assert_closed_v1'),('private','weekly_contact_revision_v1'),
 ('private','candidate_push_timing_policy_validate_v1'),('private','weekly_contact_deployment_lock_v1'),
 ('private','candidate_push_timing_immutable_guard_v1'),('public','candidate_push_timing_get_v1'),
 ('public','candidate_push_timing_save_v1')]
TABLES=['public.candidate_push_timing_policy','public.candidate_push_timing_commands']

def require(ok,code):
    if not ok:raise ValueError(code)

def extract(sql):
    matches=re.findall(r'do \$qt_absence\$.*?\$qt_absence\$;',sql,re.S)
    require(len(matches)==1,'EXACT_ONE_QT_ADMISSION_REQUIRED')
    body=json.loads(pglast.parser.parse_plpgsql_json(matches[0]))[0]['PLpgSQL_function']['action']['PLpgSQL_stmt_block']['body']
    require(len(body)==2 and 'PLpgSQL_stmt_return' in body[1],'ONE_IF_AND_RETURN_REQUIRED')
    stmt=body[0]['PLpgSQL_stmt_if']
    require(stmt['then_body'][0]['PLpgSQL_stmt_raise']['message']=='CONTACT_QT_ONE_TIME_NATIVE_ABSENCE_REQUIRED','ORIGINAL_REFUSAL_REQUIRED')
    query=stmt['cond']['PLpgSQL_expr']['query']
    tree=json.loads(pglast.parser.parse_sql_json('SELECT '+query))['stmts'][0]['stmt']['SelectStmt']['targetList'][0]['ResTarget']['val']
    return matches[0],query,tree

def strings(nodes):return [n['String']['sval'] for n in nodes]

def value(node,env):
    if 'A_Const' in node:
        c=node['A_Const'];return c['sval']['sval'] if 'sval' in c else c['ival']['ival']
    if 'ColumnRef' in node:
        alias,col=strings(node['ColumnRef']['fields']);return env[alias][col]
    if 'RowExpr' in node:return tuple(value(v,env) for v in node['RowExpr']['args'])
    if 'List' in node:return [value(v,env) for v in node['List']['items']]
    raise ValueError('UNREVIEWED_VALUE_AST:'+str(node))

def predicate(node,env):
    a=node['A_Expr'];require(strings(a['name'])==['='],'ONLY_EQUALITY_OR_POSITIVE_IN_ALLOWED')
    if a['kind']=='AEXPR_OP':return value(a['lexpr'],env)==value(a['rexpr'],env)
    require(a['kind']=='AEXPR_IN','ONLY_POSITIVE_ROW_IN_ALLOWED')
    return value(a['lexpr'],env) in value(a['rexpr'],env)

def validate(tree):
    root=tree['BoolExpr'];require(root['boolop']=='OR_EXPR' and len(root['args'])==3,'EXACT_ORIGINAL_TWO_TABLES_PLUS_NAMESPACE_REQUIRED')
    for node,table in zip(root['args'][:2],TABLES):
        n=node['NullTest'];f=n['arg']['FuncCall']
        require(n['nulltesttype']=='IS_NOT_NULL' and strings(f['funcname'])==['to_regclass'] and len(f['args'])==1 and value(f['args'][0],{})==table,'ORIGINAL_TABLE_GUARD_REQUIRED')
    sub=root['args'][2]['SubLink'];require(sub['subLinkType']=='EXISTS_SUBLINK','EXACT_EXISTS_REQUIRED')
    q=sub['subselect']['SelectStmt']
    require(set(q)=={'targetList','fromClause','whereClause','limitOption','op'} and q['op']=='SETOP_NONE' and q['limitOption']=='LIMIT_OPTION_DEFAULT','NO_EXTRA_SELECT_RESTRICTION_ALLOWED')
    require(len(q['targetList'])==1 and value(q['targetList'][0]['ResTarget']['val'],{})==1 and len(q['fromClause'])==1,'EXACT_SELECT_ONE_JOIN_REQUIRED')
    join=q['fromClause'][0]['JoinExpr'];require(join['jointype']=='JOIN_INNER' and set(join)=={'jointype','larg','rarg','quals'},'EXACT_INNER_CATALOGUE_JOIN_REQUIRED')
    for side,name,alias in [('larg','pg_proc','qt_routine'),('rarg','pg_namespace','qt_namespace')]:
        r=join[side]['RangeVar'];require(r['schemaname']=='pg_catalog' and r['relname']==name and r['alias']['aliasname']==alias,'EXACT_CATALOGUE_RELATIONS_REQUIRED')
    quals=join['quals']['A_Expr']
    require(quals['kind']=='AEXPR_OP' and strings(quals['name'])==['='] and strings(quals['lexpr']['ColumnRef']['fields'])==['qt_namespace','oid'] and strings(quals['rexpr']['ColumnRef']['fields'])==['qt_routine','pronamespace'],'EXACT_NAMESPACE_OID_JOIN_REQUIRED')
    w=q['whereClause']['A_Expr']
    require(w['kind']=='AEXPR_IN' and strings(w['name'])==['='],'NAME_ONLY_POSITIVE_IN_REQUIRED')
    require([strings(c['ColumnRef']['fields']) for c in w['lexpr']['RowExpr']['args']]==[['qt_namespace','nspname'],['qt_routine','proname']],'EXACT_SCHEMA_AND_NAME_ONLY_REQUIRED')
    require(value(w['rexpr'],{})==PAIRS,'EXACT_SEVEN_SCHEMA_NAME_PAIRS_REQUIRED')
    return q

def refused(tree,relations=(),routines=(),namespaces=None):
    # Evaluation consumes the actual parsed predicate, not a second membership implementation.
    q=validate(tree);join=q['fromClause'][0]['JoinExpr']
    if namespaces is None:namespaces=[{'oid':11,'nspname':'private'},{'oid':22,'nspname':'public'},{'oid':33,'nspname':'unrelated'}]
    results=[]
    for node in tree['BoolExpr']['args']:
        if 'NullTest' in node:
            table=value(node['NullTest']['arg']['FuncCall']['args'][0],{});results.append(table in relations)
        else:
            matched=False
            for routine in routines:
                for namespace in namespaces:
                    env={'qt_routine':routine,'qt_namespace':namespace}
                    if predicate(join['quals'],env) and predicate(q['whereClause'],env):matched=True
            results.append(matched)
    return any(results)

def old_signatures(sql):
    _,_,tree=extract(sql)
    sub=tree['BoolExpr']['args'][2]['SubLink']['subselect']['SelectStmt']
    r=sub['fromClause'][0]['RangeFunction']['functions'][0]['List']['items'][0]['FuncCall']
    require(strings(r['funcname'])==['unnest'],'EXACT_OLD_UNNEST_REQUIRED')
    w=sub['whereClause']['NullTest'];f=w['arg']['FuncCall']
    require(w['nulltesttype']=='IS_NOT_NULL' and strings(f['funcname'])==['to_regprocedure'] and strings(f['args'][0]['ColumnRef']['fields'])==['n'],'EXACT_OLD_SIGNATURE_ONLY_TEST_REQUIRED')
    return [value(v,{}) for v in r['args'][0]['A_ArrayExpr']['elements']]

def run():
    plans=json.loads((HERE/'current/PROGRAMMES.json').read_text())
    old=json.loads((HERE/'PARENT_REVERSES.json').read_text())
    original_plans=next(r['beforeRaw'] for r in old['changed'] if r['file']=='PROGRAMMES.json')
    original=json.loads(original_plans)['QT_ADMIN_CONTROLS_V1']['sql']
    signatures=old_signatures(original);require(len(signatures)==7,'ORIGINAL_SEVEN_SIGNATURES_REQUIRED')
    sql=plans['QT_ADMIN_CONTROLS_V1']['sql'];body,query,tree=extract(sql);validate(tree)
    cases=[]
    def record(label,expected,**catalogue):
        actual=refused(tree,**catalogue);require(actual==expected,'ACTUAL_AST_VECTOR_FAILED:'+label)
        cases.append({'case':label,'expectedRefusal':expected,'actualRefusal':actual})
    record('empty catalogue admitted',False)
    for table in TABLES:record('original table presence '+table,True,relations=[table])
    for schema,name in PAIRS:
        for kind in ['f','p','a','w']:
            for args in ['bigint','text,integer']:
                routine={'pronamespace':11 if schema=='private' else 22,'proname':name,'prokind':kind,'proargtypes':args}
                record('unexpected '+kind+' overload '+schema+'.'+name+'('+args+')',True,routines=[routine])
                require(schema+'.'+name+'('+args+')' not in signatures,'OLD_GAP_COUNTEREXAMPLE_MUST_BE_UNEXPECTED_SIGNATURE')
        record('same name in unrelated schema '+name,False,routines=[{'pronamespace':33,'proname':name,'prokind':'p','proargtypes':'bigint'}])
        record('unrelated name in target schema '+name,False,routines=[{'pronamespace':11 if schema=='private' else 22,'proname':name+'_other','prokind':'f','proargtypes':'jsonb'}])
        record('unjoined namespace oid '+name,False,routines=[{'pronamespace':99,'proname':name,'prokind':'f','proargtypes':'jsonb'}])
    record('mixed catalogue includes genuine unexpected procedure',True,routines=[{'pronamespace':33,'proname':'unrelated','prokind':'f','proargtypes':''},{'pronamespace':22,'proname':'candidate_push_timing_save_v1','prokind':'p','proargtypes':'bigint'}])
    mutations=[('omitted seventh name',lambda t:t['BoolExpr']['args'][2]['SubLink']['subselect']['SelectStmt']['whereClause']['A_Expr']['rexpr']['List']['items'].pop()),
     ('wrong namespace join',lambda t:t['BoolExpr']['args'][2]['SubLink']['subselect']['SelectStmt']['fromClause'][0]['JoinExpr']['quals']['A_Expr']['rexpr']['ColumnRef']['fields'][1]['String'].update(sval='oid')),
     ('extra prokind filter',lambda t:t['BoolExpr']['args'][2]['SubLink']['subselect']['SelectStmt'].update(whereClause={'BoolExpr':{'boolop':'AND_EXPR','args':[t['BoolExpr']['args'][2]['SubLink']['subselect']['SelectStmt']['whereClause'],{'A_Const':{'boolval':True}}]}})),
     ('removed original table guard',lambda t:t['BoolExpr']['args'].pop(0))]
    for label,mutate in mutations:
        t=copy.deepcopy(tree);mutate(t)
        try:validate(t)
        except (ValueError,KeyError,TypeError):pass
        else:raise ValueError('MUTATION_WAS_NOT_REFUSED:'+label)
    return {'version':'CONTACT_QT_NAME_ADMISSION_ACTUAL_PREDICATE_AST_PROOF_V1','parser':pglast.__version__,'postgresql':list(pglast.get_postgresql_version()),
     'sourceProgrammeSha256':hashlib.sha256(sql.encode()).hexdigest(),'admissionBody':body,'parsedCondition':query,'actualConditionAst':tree,
     'closedPairs':PAIRS,'syntheticCatalogueVectors':cases,'mutationRefusals':[m[0] for m in mutations],
     'oldExactSignatureGap':'Every tested unexpected overload/procedure is absent from the original parsed seven-signature array and was not refused by that old signature-only test.',
     'cataloguePresenceOrNamesNotObserved':True,'nativeExecution':'NOT_RUN','nativeCompilation':'NOT_PROVED'}

if __name__=='__main__':
    result=run();(HERE/'ADMISSION_AST_PROOF.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({'actualAstVectors':len(result['syntheticCatalogueVectors']),'mutationRefusals':len(result['mutationRefusals']),'native':'NOT_RUN'}))

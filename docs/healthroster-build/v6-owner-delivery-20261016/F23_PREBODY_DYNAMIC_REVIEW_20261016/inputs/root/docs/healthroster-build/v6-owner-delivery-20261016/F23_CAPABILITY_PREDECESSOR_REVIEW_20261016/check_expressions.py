"""Offline exact source/native expression comparison; no database/network calls."""
import copy,hashlib,json,re,sys
from pathlib import Path
sys.path.insert(0,sys.argv[1])
import pglast
from pglast import parser
d=Path(__file__).resolve().parent
if sys.platform=='win32' and not str(d).startswith('\\\\?\\'):d=Path('\\\\?\\'+str(d))
h=lambda b:hashlib.sha256(b).hexdigest()
capture=json.loads((d/'inputs/NATIVE_PREDECESSOR_CENSUS.json').read_bytes())
proposal=json.loads((d/'PROPOSAL.json').read_bytes())
prefix_mentions=[]
for unit in proposal['prefixInputs']:
    raw=(d/unit['preserved']).read_bytes();assert h(raw)==unit['sourceSha256']
    text=raw.decode('utf-8');parser.parse_sql_json(re.sub(r'^\\set[^\n]*','',text,flags=re.M))
    if 'weekly_source_group_rule_assignments'in text:prefix_mentions.append(unit['path'])
assert prefix_mentions==[capture['constructor_path']]
source=(d/'inputs/CONSTRUCTOR_1133.sql').read_text(encoding='utf-8')
consumer=(d/'inputs/IMMUTABLE_CONSUMER_0400.sql').read_text(encoding='utf-8')
parsed=json.loads(parser.parse_sql_json(re.sub(r'^\\set[^\n]*','',source,flags=re.M)))
tables=[s['stmt']['CreateStmt'] for s in parsed['stmts'] if s['stmt'].get('CreateStmt',{}).get('relation',{}).get('relname')=='weekly_source_group_rule_assignments']
assert len(tables)==1
columns={x['ColumnDef']['colname']:x['ColumnDef'] for x in tables[0]['tableElts'] if 'ColumnDef'in x}
assert [n['String']['sval'] for n in columns['feature_purpose']['typeName']['names']]==['text']
assert [n['String']['sval'] for n in columns['rule_set_version_id']['typeName']['names']]==['uuid']
checks=[]
for x in tables[0]['tableElts']:
    if 'ColumnDef'in x:
        checks.extend({'column':x['ColumnDef']['colname'],**c['Constraint']} for c in x['ColumnDef'].get('constraints',[]) if c['Constraint']['contype']=='CONSTR_CHECK')
    elif x.get('Constraint',{}).get('contype')=='CONSTR_CHECK': checks.append({'column':None,**x['Constraint']})
assert len(checks)==10
selected=[x for x in checks if 'feature_purpose'in json.dumps(x['raw_expr']) and 'SIGNED_CHECK_ONLY'in json.dumps(x['raw_expr'])]
assert len(selected)==2
def clean(x):
    if isinstance(x,list):return[clean(y) for y in x]
    if not isinstance(x,dict):return x
    x={k:clean(v) for k,v in x.items() if k not in('location','stmt_location','stmt_len')}
    if 'TypeCast'in x:
        t=x['TypeCast'];names=[n['String']['sval'] for n in t['typeName']['names']]
        if names in(['text'],['pg_catalog','text']) and 'A_Const'in t['arg'] and 'sval'in t['arg']['A_Const']:return t['arg']
    a=x.get('A_Expr')
    if a and a['kind']=='AEXPR_OP_ANY':
        assert a['name']==[{'String':{'sval':'='}}]
        elements=a['rexpr']['A_ArrayExpr']['elements']
        assert elements==[{'A_Const':{'sval':{'sval':'RULE_SET'}}},{'A_Const':{'sval':{'sval':'SIGNED_CHECK_ONLY'}}}]
        return{'A_Expr':{'kind':'AEXPR_IN','name':a['name'],'lexpr':a['lexpr'],'rexpr':{'List':{'items':elements}}}}
    return x
def expr(text):return json.loads(parser.parse_sql_json('select '+text))['stmts'][0]['stmt']['SelectStmt']['targetList'][0]['ResTarget']['val']
evidence=[];old={}
for c in selected:
    role='PURPOSE' if c['column']=='feature_purpose' else'VERSION';old[role]=clean(c['raw_expr'])
    native=[x for x in capture['selected_checks'] if x['selector_role']==role][0]
    variants={k:clean(expr(native[k][6:] if native[k].startswith('CHECK ') else native[k])) for k in['selector_definition_default_false','evidence_definition_pretty_true','evidence_expression_pretty_true']}
    for n in variants.values():assert n==old[role],role
    evidence.append({'role':role,'constraintName':native['constraint_name'],'constructorColumn':c['column'],'sourceNormalizedAst':old[role],'nativeVariantsEqualAfterFiniteTextInAdaptation':True,'nativeVariants':variants})
# Extract actual immutable successor static SQL from the complete PL/pgSQL AST.
pl=json.loads(parser.parse_plpgsql_json(consumer));queries=[]
def walk(x):
    if isinstance(x,dict):
        q=x.get('PLpgSQL_expr',{}).get('query')
        if q and q.lower().startswith('alter table public.weekly_source_group_rule_assignments add constraint'):queries.append(q)
        for v in x.values():walk(v)
    elif isinstance(x,list):
        for v in x:walk(v)
walk(pl);assert len(queries)==2
successors={}
for q in queries:
    stmt=json.loads(parser.parse_sql_json(q))['stmts'][0]['stmt']['AlterTableStmt']
    c=stmt['cmds'][0]['AlterTableCmd']['def']['Constraint'];assert c['contype']=='CONSTR_CHECK'
    role='PURPOSE' if c['conname']=='weekly_source_assignment_feature_purpose_v2' else'VERSION'
    successors[role]=clean(c['raw_expr'])
def evaluate(x,row):
    if 'ColumnRef'in x:return row[x['ColumnRef']['fields'][0]['String']['sval']]
    if 'A_Const'in x:return x['A_Const']['sval']['sval']
    if 'List'in x:return[evaluate(v,row) for v in x['List']['items']]
    if 'NullTest'in x:
        n=x['NullTest'];assert n['nulltesttype']=='IS_NULL';return evaluate(n['arg'],row)is None
    if 'BoolExpr'in x:
        b=x['BoolExpr'];assert b['boolop']=='OR_EXPR';v=[evaluate(a,row) for a in b['args']]
        return True if True in v else(None if None in v else False)
    a=x['A_Expr'];assert a['name']==[{'String':{'sval':'='}}]
    left,right=evaluate(a['lexpr'],row),evaluate(a['rexpr'],row)
    if a['kind']=='AEXPR_IN':return None if left is None else left in right
    assert a['kind']=='AEXPR_OP';return None if left is None or right is None else left==right
matrix=[]
for purpose in[None,'RULE_SET','SIGNED_CHECK_ONLY','PROTECTED_SOURCE','OTHER']:
    for version in[None,'non-null UUID']:
        row={'feature_purpose':purpose,'rule_set_version_id':version}
        a={r:evaluate(v,row) for r,v in old.items()};b={r:evaluate(v,row) for r,v in successors.items()}
        old_ok=all(v is not False for v in a.values());new_ok=all(v is not False for v in b.values())
        assert not old_ok or new_ok
        assert(new_ok and not old_ok)==(purpose=='PROTECTED_SOURCE' and version is None)
        matrix.append({**row,'oldChecks':a,'newChecks':b,'oldCheckAccepts':old_ok,'newCheckAccepts':new_ok})
program=(d/'16102026_1120_healthroster_capability_predecessor_assertion.sqlinc').read_bytes()
sql=json.loads(parser.parse_sql_json(program.decode('utf-8')));assert len(sql['stmts'])==2
assert 'LockStmt'in sql['stmts'][0]['stmt'] and 'DoStmt'in sql['stmts'][1]['stmt']
assert len(json.loads(parser.parse_plpgsql_json(program.decode('utf-8'))))==1
out={'version':'HEALTHROSTER_CAPABILITY0400_EXACT_EXPRESSION_PEER_REVIEW_V1','status':'PASS_OFFLINE_SOURCE_AND_PUBLISHED_NATIVE_EXPRESSION_COMPARISON','postgresqlParserVersion':parser.get_postgresql_version(),'pglastVersion':pglast.__version__,
 'sourceCheckCount':10,'selectedSourceCheckCount':2,'retainedSourceCheckCount':8,'canonicalPendingPrefixUnitsVerified':9,'prefixAssignmentMentionsOnlyConstructor':prefix_mentions,'comparisons':evidence,'actualSuccessorAsts':successors,'checkTruthMatrix':matrix,
 'normalization':'Only locations removed, literal ::text casts on string constants removed after actual constructor column TEXT proof, and this exact two-member positive IN versus =ANY ARRAY comparison normalized. No arbitrary expression equivalence rule.',
 'sourceDesignExpressionApproved':True,'nativeCompanionApproved':False,'nativeCompanion':'NOT_RUN','companionSha256':h(program),'companionTopLevelStatements':2,'databaseCalls':0}
(d/'EXPRESSION_REVIEW.json').write_text(json.dumps(out,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'status':out['status'],'comparisons':len(evidence),'truthCases':len(matrix),'companionTopLevelStatements':2,'nativeCompanion':'NOT_RUN'}))

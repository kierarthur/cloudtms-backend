"""PG17 grammar and actual operand AST. Never native catalogue compilation."""
from pathlib import Path
import hashlib,json,re,sys
HERE=Path(__file__).resolve().parent
MONEY=Path(r'\\?\D:\Development\GitHub\.codex-worktrees\healthroster-v2-money-build-13102026')
PARSER=Path(r'D:\Development\Legacy-Work\C-tmp\healthroster-v6-invoice-parser-20261016')
assert PARSER.is_dir();sys.path.insert(0,str(PARSER))
import pglast
from pglast import ast
data=json.loads((HERE/'CLASSIFICATION.json').read_text(encoding='utf-8'))
sha=lambda x:hashlib.sha256(x).hexdigest()
tuples=[];programs=[];failures=[];scalar=[]
expected={
 '011_':'"v_prior_movement.id" is not a known variable',
 '018_':'"v_snapshot.work_date" is not a known variable',
 '051_':'"p.id" is not a known variable',
 '054_':'"v_charge_acceptance.id" is not a known variable'}
def walk(value):
    if isinstance(value,ast.Node):
        yield value
        for key in value:yield from walk(getattr(value,key))
    elif isinstance(value,(list,tuple)):
        for child in value:yield from walk(child)
def opname(node):return node.name[0].sval if isinstance(node,ast.A_Expr) else None
for file in data['files']:
    raw=(MONEY/file['sourcePath']).read_bytes();assert sha(raw)==file['rawSha256']
    source=raw.decode().replace('\r\n','\n');assert sha(source.encode())==file['sha256']
    for p in file['points']:
        sig,hazard,sql=json.loads(p['key'])
        if hazard.startswith('ROWTYPE_SELECT:'):
            _,variable,relation=hazard.split(':');relation=relation.replace(' ','').lower()
            start=re.search(r'\bselect\s+\*\s+into\s+(?:strict\s+)?'+re.escape(variable)+r'\b',sql,re.I);assert start
            query,count=re.subn(r'\binto\s+(?:strict\s+)?'+re.escape(variable)+r'\b','',sql[start.start():],flags=re.I);assert count==1
            parsed=pglast.parse_sql(query);assert len(parsed)==1 and isinstance(parsed[0].stmt,ast.SelectStmt)
            select=parsed[0].stmt;assert len(select.targetList)==1 and len(select.fromClause)==1
            projection=select.targetList[0].val;assert isinstance(projection,ast.ColumnRef) and len(projection.fields)==1 and isinstance(projection.fields[0],ast.A_Star)
            actual=select.fromClause[0];assert isinstance(actual,ast.RangeVar) and actual.schemaname+'.'+actual.relname==relation
            assert len(data['schema']['relations'][relation]['columns'])>0
            tuples.append({'file':file['file'],'signature':sig,'variable':variable,'actualRelation':relation,'count':p['count']})
        elif file['file']!=data['classifier']['target'] and not any(t['file']==file['file'] and t['key']==p['key'] for t in data['transfers']) and hazard in ('UUID_CARDINALITY_UNCLASSIFIED','AGGREGATE_TYPE_UNCLASSIFIED'):
            query=re.sub(r'\binto\s+(?:v_count,v_snapshot,v_rows|counted,movement_id|v_scope_count,v_scope|counted,p\.id|v_count,v_event,v_foreign_owner)\b','',sql)
            query=re.sub(r'\breturning id into v_presentation_id\b','returning id',query)
            parsed=pglast.parse_sql(query);assert len(parsed)==1
            aggs=[n for n in walk(parsed[0].stmt) if isinstance(n,ast.FuncCall) and n.funcname[-1].sval in ('max','min')]
            assert len(aggs)==(2 if '/018_' in file['file'] else 1)
            for agg in aggs:
                arg=agg.args[0]
                if isinstance(arg,ast.TypeCast):assert arg.typeName.names[-1].sval=='text' and isinstance(arg.arg,ast.ColumnRef)
                else:assert opname(arg)=='->>' # jsonb text extraction, never max(uuid)
            if '/018_' in file['file']:
                subqueries=[n for n in walk(parsed[0].stmt) if isinstance(n,ast.SelectStmt) and n.havingClause]
                assert len(subqueries)==2
                for subquery in subqueries:
                    having=subquery.havingClause;assert opname(having)=='=' and isinstance(having.lexpr,ast.FuncCall)
                    assert having.lexpr.funcname[-1].sval=='count' and having.lexpr.agg_star
                    assert isinstance(having.rexpr,ast.A_Const) and having.rexpr.val.ival==1
            scalar.append({'file':file['file'],'signature':sig,'aggregates':len(aggs),'operand':'TEXT_BEFORE_UUID_CAST','native':'NOT_RUN'})
    program='\n'.join(line for line in source.splitlines() if not line.startswith('\\'))
    parsed=pglast.parse_sql(program)
    try:
        bodies=len(pglast.parse_plpgsql(program));state='GRAMMAR_ONLY_NOT_NATIVE'
    except pglast.parser.ParseError as error:
        leaf=Path(file['file']).name;key=next((key for key in expected if leaf.startswith(key)),None)
        assert key and expected[key] in str(error),(file['file'],str(error))
        recipe=next(o for o in data['owners'] if o['file']==file['file']);previous_error=None
        if recipe.get('priorFile'):
            prior=(HERE/recipe['priorFile'].split('/same-separate-f23-classification-20261017/')[1]).read_text(encoding='utf-8')
            try:pglast.parse_plpgsql(prior)
            except pglast.parser.ParseError as old:previous_error=str(old)
        bodies=0;state='NOT_PROVED_CATALOGUE_DEPENDENT_RECORD'
        failures.append({'file':file['file'],'signature':recipe['signature'],'error':str(error),'completePredecessorError':previous_error,'bodyCompilation':'NOT_PROVED'})
    programs.append({'file':file['file'],'topLevelStatements':len(parsed),'parsedBodies':bodies,'bodyState':state})
pglast.parse_sql("select '$' as anchor, '$&' as amp, '$`' as tick, $$ $' $$ as tail;")
expression=pglast.parse_sql("select v_values->'pay_vector'||jsonb_build_object('source_expense_pence',0);")[0].stmt.targetList[0].val
assert opname(expression)=='||' and opname(expression.lexpr)=='->'
assert isinstance(expression.rexpr,ast.FuncCall) and expression.rexpr.funcname[-1].sval=='jsonb_build_object'
assert sum(p['count'] for p in tuples)==409 and len(failures)==6 and len(set(f['signature'] for f in failures))==4
assert len(scalar)==7
result={'version':'SAME_SEPARATE_PG17_SOURCE_AST_V1','parser':pglast.__version__,'postgresqlGrammar':pglast.get_postgresql_version(),
 'classificationSha256':sha((HERE/'CLASSIFICATION.json').read_bytes()),'tupleOccurrences':409,'tuplePoints':len(tuples),'relationSourceTypes':41,
 'tuples':tuples,'newScalarPoints':scalar,'expenseOperator':'JSONB_EXTRACTION_THEN_JSONB_OBJECT_CONCAT',
 'programs':programs,'catalogueDependentBodies':failures,'uniqueCatalogueDependentSignatures':4,
 'nativeBodyCompilation':'NOT_PROVED','native':'NOT_RUN','dbConnections':0,'bankingBodiesRead':0,'substitutions':0}
(HERE/'AST_RESULTS.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'tuples':409,'newScalarPoints':len(scalar),'catalogueDependentOccurrencesNotProved':6,'uniqueNotProved':4,'native':'NOT_RUN'}))

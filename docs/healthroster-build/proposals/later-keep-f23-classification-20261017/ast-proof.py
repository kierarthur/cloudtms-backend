"""PostgreSQL17 tuple/operator source proof, never native catalogue proof."""
from pathlib import Path
import hashlib,json,re,sys
HERE=Path(__file__).resolve().parent
ROOT=HERE.parents[3].parent/'healthroster-install2-14102026'
PARSER=Path(r'D:\Development\Legacy-Work\C-tmp\healthroster-v6-invoice-parser-20261016')
assert PARSER.is_dir()
sys.path.insert(0,str(PARSER))
import pglast
from pglast import ast
data=json.loads((HERE/'CLASSIFICATION.json').read_text(encoding='utf-8'))
rows=[];tuples=0;programs=[];failures=[]
for item in data['files']:
    raw=(ROOT/item['file']).read_bytes()
    source=raw.decode('utf-8').replace('\r\n','\n')
    assert hashlib.sha256(source.encode()).hexdigest()==item['sha256']
    for point in item['points']:
        if not point['hazard'].startswith('ROWTYPE_SELECT:'):continue
        signature,hazard,sql=json.loads(point['key'])
        _,variable,relation=hazard.split(':')
        relation=relation.replace(' ','').lower()
        # The diagnostic may begin with the enclosing FOR query. Select the
        # exact complete tuple query for this declared target, not its prefix.
        tuple_start=re.search(r'\bselect\s+\*\s+into\s+(?:strict\s+)?'+re.escape(variable)+r'\b',sql,flags=re.I)
        assert tuple_start
        query,count=re.subn(r'\binto\s+(?:strict\s+)?'+re.escape(variable)+r'\b','',sql[tuple_start.start():],flags=re.I)
        assert count==1
        try:parsed=pglast.parse_sql(query)
        except pglast.parser.ParseError as error:raise RuntimeError((item['file'],signature,query,str(error))) from error
        assert len(parsed)==1 and isinstance(parsed[0].stmt,ast.SelectStmt)
        selected=parsed[0].stmt
        assert len(selected.targetList)==1
        projected=selected.targetList[0].val
        assert isinstance(projected,ast.ColumnRef) and len(projected.fields)==1 and isinstance(projected.fields[0],ast.A_Star)
        assert len(selected.fromClause)==1 and isinstance(selected.fromClause[0],ast.RangeVar)
        actual=selected.fromClause[0]
        assert actual.schemaname+'.'+actual.relname==relation
        assert len(data['schema']['relations'][relation]['columns'])>0
        tuples+=point['count']
        rows.append({'file':item['file'],'signature':signature,'variable':variable,'relation':relation,'multiplicity':point['count'],'proof':'ONE_ACTUAL_RELATION_COMPLETE_TUPLE_AST'})
    sql='\n'.join(line for line in source.splitlines() if not line.startswith('\\'))
    parsed=pglast.parse_sql(sql)
    try:
        bodies=len(pglast.parse_plpgsql(sql));body_state='GRAMMAR_ONLY'
    except pglast.parser.ParseError as error:
        recipes=[r for r in data['ownerReverses'] if r['file']==item['file']]
        assert len(recipes)==1
        prior=(HERE/recipes[0]['priorFile'].split('/later-keep-f23-classification-20261017/')[1]).read_text(encoding='utf-8')
        try:pglast.parse_plpgsql(prior)
        except pglast.parser.ParseError as original:assert str(original)==str(error)
        else:raise
        bodies=0;body_state='NOT_PROVED_INHERITED_CATALOGUE_DEPENDENCY'
        failures.append({'file':item['file'],'signature':recipes[0]['signature'],'error':str(error),'actualCompletePredecessorSameError':True,'bodyCompilation':'NOT_PROVED'})
    programs.append({'file':item['file'],'topLevelStatements':len(parsed),'parsedBodies':bodies,'bodyStatus':body_state,'native':'NOT_RUN'})
assert tuples==180
assert len(failures)==2
json_expression=pglast.parse_sql("select v_saved.after_json->'result'||jsonb_build_object('idempotent_replay',true,'historical_proposal',true);")[0].stmt.targetList[0].val
assert isinstance(json_expression,ast.A_Expr) and json_expression.name[0].sval=='||'
assert isinstance(json_expression.lexpr,ast.A_Expr) and json_expression.lexpr.name[0].sval=='->'
result={'parserVersion':pglast.__version__,'postgresqlGrammar':pglast.get_postgresql_version(),'tupleOccurrences':tuples,'tuplePoints':len(rows),'relations':len(data['schema']['relations']),'tuples':rows,'replayOperator':'JSONB_EXTRACTION_THEN_JSONB_CONCAT','programs':programs,'catalogueDependentBodies':failures,'sourceTypeClassification':'PASS','nativeBodyCompilation':'NOT_PROVED','native':'NOT_RUN','dbConnections':0}
(HERE/'AST_RESULTS.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'tupleOccurrences':tuples,'tuplePoints':len(rows),'relations':len(data['schema']['relations']),'catalogueDependentBodiesNotProved':len(failures),'native':'NOT_RUN'}))

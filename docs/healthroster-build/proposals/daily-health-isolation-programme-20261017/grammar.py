"""PG17 grammar only. No connection, catalogue substitution or native verdict."""
from pathlib import Path
import sys,json,hashlib
HERE=Path(__file__).resolve().parent
PARSER=Path(r'D:\Development\Legacy-Work\C-tmp\healthroster-v6-invoice-parser-20261016')
assert PARSER.is_dir()
sys.path.insert(0,str(PARSER))
import pglast
from pglast import ast
source=json.loads((HERE/'PROGRAMMES.json').read_text(encoding='utf-8'))
assert source['dbConnections']==0 and source['syntheticFinancialFacts'] is False
rows=[]
for e in source['examples']:
    sql=e['sql']
    assert hashlib.sha256(sql.encode()).hexdigest()==e['sha256']
    parsed=pglast.parse_sql(sql)
    bodies=pglast.parse_plpgsql(sql)
    definitions=[r.stmt for r in parsed if isinstance(r.stmt,ast.CreateFunctionStmt)]
    assert len(definitions)==4
    assert not any(isinstance(r.stmt,(ast.InsertStmt,ast.UpdateStmt,ast.DeleteStmt,ast.MergeStmt)) for r in parsed)
    if e['kind']=='REVIEW_SCRIPT':
        assert isinstance(parsed[0].stmt,ast.TransactionStmt) and isinstance(parsed[-1].stmt,ast.TransactionStmt)
    rows.append({'name':e['name'],'statements':len(parsed),'definitions':len(definitions),'plpgsqlBodies':len(bodies),'grammar':'PASS','nativeBodyCompilation':'NOT_PROVED'})
result={'parser':pglast.__version__,'postgresqlGrammar':pglast.get_postgresql_version(),'examples':rows,'substitutions':[],'native':'NOT_RUN','dbConnections':0}
(HERE/'GRAMMAR_RESULTS.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
print(json.dumps(result))

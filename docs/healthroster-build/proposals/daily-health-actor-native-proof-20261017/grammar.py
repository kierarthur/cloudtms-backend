"""PG17 source grammar only; no connection, catalogue compilation or native verdict."""
from pathlib import Path
import sys,json,hashlib
HERE=Path(__file__).resolve().parent
PARSER=Path(r'D:\Development\Legacy-Work\C-tmp\healthroster-v6-invoice-parser-20261016')
assert PARSER.is_dir()
sys.path.insert(0,str(PARSER))
import pglast
from pglast import ast
inputs=json.loads((HERE/'OFFLINE_INPUTS.json').read_text(encoding='utf-8'))
assert inputs['syntheticIdentity'] is True and inputs['dbConnections']==0
rows=[]
for example in inputs['examples']:
    sql=example['sql']
    assert hashlib.sha256(sql.encode()).hexdigest()==example['sha256']
    parsed=pglast.parse_sql(sql)
    bodies=pglast.parse_plpgsql(sql)
    definitions=[r.stmt for r in parsed if isinstance(r.stmt,ast.CreateFunctionStmt)]
    if example['name'] in ('ORIGINAL_PROGRAMME','SYNTHETIC_B'):
        assert len(definitions)==1
        for definition in definitions:
            options={r.defname:r.arg for r in definition.options}
            assert options['security'].boolval is True
            assert 'search_path' in str(options['set'])
    tables=[r.stmt for r in parsed if isinstance(r.stmt,ast.CreateStmt)]
    assert len(tables)==(2 if example['name']=='SYNTHETIC_B' else 1 if example['name']=='ORIGINAL_PROGRAMME' else 0)
    rows.append({'example':example['name'],'statements':len(parsed),'parsedPlpgsqlBodies':len(bodies),'definitions':len(definitions),
                 'grammar':'PASS','nativeBodyCompilation':'NOT_PROVED'})
result={'parser':pglast.__version__,'postgresqlGrammar':pglast.get_postgresql_version(),'examples':rows,
        'noBankSubstitutions':True,'programme':'GENUINE_NATIVE_ACTOR_FIXTURES_ONLY','nativeBodyCompilation':'NOT_PROVED','native':'NOT_RUN','dbConnections':0}
(HERE/'GRAMMAR_RESULTS.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
print(json.dumps(result))

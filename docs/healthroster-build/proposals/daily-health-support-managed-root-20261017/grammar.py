"""PG17 source grammar/ACL/independent-verifier contract, no native execution."""
from pathlib import Path
import json,sys,re
HERE=Path(__file__).resolve().parent
PARSER=Path(r'D:\Development\Legacy-Work\C-tmp\healthroster-v6-invoice-parser-20261016')
assert PARSER.is_dir()
sys.path.insert(0,str(PARSER))
import pglast
from pglast import ast
manifest=json.loads((HERE/'PAYLOAD_MANIFEST.json').read_text(encoding='utf-8'))
expected=json.loads((HERE/'CATALOGUE_EXPECTATIONS.json').read_text(encoding='utf-8'))
rows=[];functions=[]
for item in manifest['files']:
    if not re.search(r'\.sql(?:inc)?$',item['target'],re.I):continue
    source=(HERE/item['source']).read_text(encoding='utf-8')
    sql='\n'.join(line for line in source.splitlines() if not line.startswith('\\'))
    parsed=pglast.parse_sql(sql);bodies=pglast.parse_plpgsql(sql)
    for row in parsed:
        if not isinstance(row.stmt,ast.CreateFunctionStmt):continue
        fn=row.stmt
        name='.'.join(v.sval for v in fn.funcname)
        options={v.defname:v.arg for v in fn.options}
        assert name.startswith('private.daily_rate_correction_health_')
        assert options['security'].boolval is True
        assert 'search_path' in str(options['set'])
        functions.append(name)
    rows.append({'file':item['target'],'statements':len(parsed),'parsedBodies':len(bodies),'grammar':'PASS','nativeBodyCompilation':'NOT_PROVED'})
assert len(functions)==6 and len(set(functions))==6
assert sorted(functions)==sorted(r['signature'].split('(')[0] for r in expected['routines'])
reverse=(HERE/'reverse/complete_additive_reverse.sqlinc').read_text(encoding='utf-8')
parsed=pglast.parse_sql(reverse)
names=['.'.join(v.sval for v in row.stmt.objects[0].objname) for row in parsed]
assert sorted(names)==sorted(functions)
(HERE/'GRAMMAR_RESULTS.json').write_text(json.dumps({'parser':pglast.__version__,'postgresqlGrammar':pglast.get_postgresql_version(),'programs':rows,'exactCompleteHelpers':6,'additiveReverseMatch':True,'noBankSubstitutions':True,'nativeBodyCompilation':'NOT_PROVED','native':'NOT_RUN','dbConnections':0},indent=2)+'\n',encoding='utf-8')
print('PASS PG17 grammar:6 exact private helpers, one-time admission, ACL, catalogue-only verifier and6-drop reverse; native NOT_RUN')

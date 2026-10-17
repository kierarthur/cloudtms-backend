"""PG17 source grammar; never substitute catalogue-dependent row types."""
from pathlib import Path
import sys,json,hashlib
HERE=Path(__file__).resolve().parent
PARSER=Path(r'D:\Development\Legacy-Work\C-tmp\healthroster-v6-invoice-parser-20261016')
assert PARSER.is_dir();sys.path.insert(0,str(PARSER))
import pglast
from pglast import ast
manifest=json.loads((HERE/'PAYLOAD_MANIFEST.json').read_text(encoding='utf-8'))
rows=[]
for p in manifest['files']:
    if not p['target'].endswith(('.sql','.sqlinc')):continue
    sql=(HERE/p['sourcePath']).read_text(encoding='utf-8')
    if p['target'].endswith('_v1.sql'):
        lines=[]
        for line in sql.splitlines():
            if line.startswith('\\ir '):
                relative=Path(p['target']).parent/line[4:]
                lines.append((HERE/'payload'/relative).read_text(encoding='utf-8'))
            elif not line.startswith('\\'):lines.append(line)
        sql='\n'.join(lines)
    else:sql='\n'.join(l for l in sql.splitlines() if not l.startswith('\\'))
    parsed=pglast.parse_sql(sql)
    try:
        bodies=pglast.parse_plpgsql(sql)
        body={'grammar':'PASS','parsedBodies':len(bodies),'nativeBodyCompilation':'NOT_PROVED'}
    except Exception as e:
        body={'grammar':'NOT_PROVED_CATALOGUE_DEPENDENT','diagnostic':str(e),'nativeBodyCompilation':'NOT_PROVED'}
    rows.append({'file':p['target'],'statements':len(parsed),'sqlGrammar':'PASS','plpgsql':body})
result={'parser':pglast.__version__,'postgresqlGrammar':pglast.get_postgresql_version(),'files':rows,'catalogueSubstitutions':[],'native':'NOT_RUN','dbConnections':0}
(HERE/'GRAMMAR_RESULTS.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8');print(json.dumps(result))

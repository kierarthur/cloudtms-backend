from pathlib import Path
import sys,json,hashlib
here=Path(__file__).parent
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
p=here/'reference/r2/payload/docs/healthroster-build/proposals/daily-health-actor-native-proof-20261017/OFFLINE_INPUTS.json'
raw=open('\\\\?\\'+str(p.absolute()),'rb').read();source=json.loads(raw);rows=[]
assert source['syntheticIdentity'] is True and source['dbConnections']==0
for x in source['examples']:
 assert hashlib.sha256(x['sql'].encode()).hexdigest()==x['sha256']
 rows.append(dict(example=x['name'],sha256=x['sha256'],statements=len(pglast.parse_sql(x['sql'])),programs=len(pglast.parse_plpgsql(x['sql'])),nativeBodyCompilation='NOT_PROVED'))
assert len(rows)==5
(here/'INDEPENDENT_GRAMMAR_RESULTS.json').write_text(json.dumps(dict(sourceSha256=hashlib.sha256(raw).hexdigest(),parser=pglast.__version__,PostgreSQLGrammar=pglast.get_postgresql_version(),examples=rows,noSubstitutions=True,native='NOT_RUN',dbConnections=0),indent=2)+'\n',encoding='utf8',newline='\n')
print(json.dumps(dict(examples=len(rows),native='NOT_RUN')))

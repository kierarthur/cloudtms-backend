"""Offline PostgreSQL17 grammar only: no PostgreSQL session or catalogue."""
import sys,json,hashlib,os
from pathlib import Path
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
here=Path(__file__).parent
rows=[]
for file in sorted([*(here/'forward').glob('*.sqlinc'),*(here/'payload'/'supabase').rglob('*.sql'),*(here/'payload'/'supabase').rglob('*.sqlinc')]):
    # This finite managed payload exceeds MAX_PATH in its review folder.
    data=open(('\\\\?\\'+str(file.absolute())) if os.name=='nt' else file,'rb').read()
    source=data.decode('utf8')
    # Psql meta-commands are scanned independently by the managed-source parser.
    source='\n'.join(line for line in source.splitlines() if not line.startswith('\\'))
    sql=pglast.parse_sql(source)
    functions=pglast.parse_plpgsql(source)
    rows.append(dict(path=file.relative_to(here).as_posix(),sha256=hashlib.sha256(data).hexdigest(),statements=len(sql),plpgsqlBodies=len(functions)))
(here/'GRAMMAR_RESULTS.json').write_text(json.dumps(dict(parser='pglast '+pglast.__version__,postgresGrammar='17.7',files=rows,statements=sum(r['statements'] for r in rows),plpgsqlBodies=sum(r['plpgsqlBodies'] for r in rows),catalogueResolution='NOT_RUN',native='NOT_RUN',dbConnections=0),indent=2)+'\n',newline='\n')
print(json.dumps(dict(files=len(rows),statements=sum(r['statements'] for r in rows),plpgsqlBodies=sum(r['plpgsqlBodies'] for r in rows),native='NOT_RUN')))

"""Offline PostgreSQL17 grammar only; no catalogue, native or DB execution."""
from pathlib import Path
import sys,os,json,hashlib
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
here=Path(__file__).parent
def raw(file):
    return open(('\\\\?\\'+str(file.absolute())) if os.name=='nt' else file,'rb').read()
rows=[]
for file in sorted([*(here/'forward').glob('*.sqlinc'),*(here/'payload'/'supabase').rglob('*.sql'),*(here/'payload'/'supabase').rglob('*.sqlinc')]):
    data=raw(file);source='\n'.join(line for line in data.decode('utf8').splitlines() if not line.startswith('\\'))
    statements=len(pglast.parse_sql(source))
    try:
        bodies=len(pglast.parse_plpgsql(source));inherited=None
    except pglast.parser.ParseError as error:
        print(str(file.relative_to(here))+': '+str(error),file=sys.stderr)
        current=here/'current'/file.name
        if not current.exists(): raise
        try: pglast.parse_plpgsql(raw(current).decode('utf8'))
        except pglast.parser.ParseError as original:
            if str(original)!=str(error): raise error
        else: raise error
        bodies=0;inherited=str(error)
    rows.append(dict(path=file.relative_to(here).as_posix(),sha256=hashlib.sha256(data).hexdigest(),statements=statements,parsedBodies=bodies,inheritedCurrentCatalogueRequirement=inherited))
result=dict(parser='pglast '+pglast.__version__,PostgresGrammar='17.7',files=rows,statements=sum(r['statements'] for r in rows),parsedPlpgsqlBodies=sum(r['parsedBodies'] for r in rows),inheritedCurrentCatalogueRequirements=sum(r['inheritedCurrentCatalogueRequirement'] is not None for r in rows),fullPlpgsqlCatalogueGrammar='NOT_PROVED',native='NOT_RUN',catalogue='NOT_RUN',dbConnections=0)
(here/'GRAMMAR_RESULTS.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf8',newline='\n')
print(json.dumps(dict(files=len(rows),statements=result['statements'],parsedBodies=result['parsedPlpgsqlBodies'],inherited=result['inheritedCurrentCatalogueRequirements'],native='NOT_RUN')))

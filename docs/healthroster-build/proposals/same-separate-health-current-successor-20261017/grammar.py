"""Offline PostgreSQL17 grammar only; no catalogue, native or DB execution."""
from pathlib import Path
import sys,os,json,hashlib,re
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
here=Path(__file__).parent
def raw(file):
    return open(('\\\\?\\'+str(file.absolute())) if os.name=='nt' else file,'rb').read()
rows=[]
for file in sorted([*(here/'forward').glob('*.sqlinc'),*(here/'payload'/'supabase').rglob('*.sql'),*(here/'payload'/'supabase').rglob('*.sqlinc')]):
    data=raw(file);source='\n'.join(line for line in data.decode('utf8').splitlines() if not line.startswith('\\'))
    statements=len(pglast.parse_sql(source))
    surrogate=False
    try:
        bodies=len(pglast.parse_plpgsql(source));inherited=None
    except pglast.parser.ParseError as error:
        print(str(file.relative_to(here))+': '+str(error),file=sys.stderr)
        if not re.fullmatch(r'"[a-z_][a-z0-9_]*[.][a-z_][a-z0-9_]*" is not a known variable',str(error)): raise
        # Independent grammar-only surrogate after actual parser stopped at an
        # unresolved catalogue rowtype. Never an install/native body or proof
        # that the real table/composite field exists or compiles.
        grammar_source=re.sub(r'\b([a-z_][a-z0-9_]*)\s+(?:public|private)[.][a-z_][a-z0-9_]*%rowtype',r'\1 record',source,flags=re.I)
        bodies=len(pglast.parse_plpgsql(grammar_source));inherited=str(error);surrogate=True
    rows.append(dict(path=file.relative_to(here).as_posix(),sha256=hashlib.sha256(data).hexdigest(),statements=statements,parsedBodies=bodies,actualCatalogueRequirement=inherited,grammarOnlyRecordSurrogate=surrogate))
result=dict(parser='pglast '+pglast.__version__,PostgresGrammar='17.7',files=rows,statements=sum(r['statements'] for r in rows),parsedPlpgsqlBodies=sum(r['parsedBodies'] for r in rows),inheritedCurrentCatalogueRequirements=sum(r['actualCatalogueRequirement'] is not None for r in rows),fullPlpgsqlCatalogueGrammar='NOT_PROVED',native='NOT_RUN',catalogue='NOT_RUN',dbConnections=0)
(here/'GRAMMAR_RESULTS.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf8',newline='\n')
print(json.dumps(dict(files=len(rows),statements=result['statements'],parsedBodies=result['parsedPlpgsqlBodies'],inherited=result['inheritedCurrentCatalogueRequirements'],native='NOT_RUN')))

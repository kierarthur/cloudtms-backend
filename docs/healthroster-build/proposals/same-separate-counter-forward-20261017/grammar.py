"""Parse exact proposed SQL offline. No catalogue, native or database execution."""
from pathlib import Path
import os,sys,json,hashlib,re
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
here=Path(__file__).parent
def raw(p): return open(('\\\\?\\'+str(p.absolute())) if os.name=='nt' else p,'rb').read()
rows=[]
manifest=json.loads((here/'COMPOSED_SELECTION.json').read_text())
selected=[r for r in manifest['files'] if r['sourceCommit'] is None and r['target'].startswith('supabase/') and r['target'].lower().endswith(('.sql','.sqlinc'))]
assert len(selected)==12
for row in selected:
    p=here/row['sourcePath']
    data=raw(p);source='\n'.join(x for x in data.decode().splitlines() if not x.startswith('\\'))
    assert hashlib.sha256(data).hexdigest()==row['sha256']
    statements=len(pglast.parse_sql(source));requirement=None
    surrogate_bodies=0
    try: bodies=len(pglast.parse_plpgsql(source))
    except pglast.parser.ParseError as error:
        requirement=str(error)
        if not re.fullmatch(r'"[a-z_][a-z0-9_]*[.][a-z_][a-z0-9_]*" is not a known variable',requirement): raise
        bodies=None
        grammar_source=re.sub(r'\b([a-z_][a-z0-9_]*)\s+(?:public|private)[.][a-z_][a-z0-9_]*%rowtype',r'\1 record',source,flags=re.I)
        surrogate_bodies=len(pglast.parse_plpgsql(grammar_source))
    rows.append(dict(path=p.relative_to(here).as_posix(),sha256=hashlib.sha256(data).hexdigest(),statements=statements,parsedBodies=bodies,actualCatalogueRequirement=requirement,grammarOnlyRecordSurrogateBodies=surrogate_bodies))
result=dict(parser='pglast '+pglast.__version__,PostgreSQLGrammar='17.7',files=rows,statements=sum(r['statements'] for r in rows),parsedBodies=sum(r['parsedBodies'] or 0 for r in rows),catalogueDependent=sum(r['actualCatalogueRequirement'] is not None for r in rows),grammarOnlyRecordSurrogateBodies=sum(r['grammarOnlyRecordSurrogateBodies'] for r in rows),censusVectorsParsedStatements=len(pglast.parse_sql(raw(here/'FINAL_CENSUS_VECTORS.sql').decode())),grammarOnly=True,native='NOT_RUN',dbConnections=0)
(here/'GRAMMAR_RESULTS.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf8',newline='\n')
print(json.dumps({k:result[k] for k in ['statements','parsedBodies','catalogueDependent','native']}))

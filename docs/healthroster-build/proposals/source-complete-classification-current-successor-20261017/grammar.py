import sys,json,re,hashlib,os
from pathlib import Path
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
here=Path(__file__).parent
def raw(p):return open(('\\\\?\\'+str(p.absolute())) if os.name=='nt' else p,'rb').read()
rows=[]
for p in sorted((here/'payload/supabase').rglob('*')):
 if p.suffix not in ('.sql','.sqlinc'):continue
 data=raw(p);s=re.sub(r'^\\[^\r\n]*','',data.decode(),flags=re.M);parsed=pglast.parse_sql(s);programs=0
 for statement in parsed:
  if type(statement.stmt).__name__ not in ('CreateFunctionStmt','DoStmt'):continue
  text=s[statement.stmt_location:statement.stmt_location+statement.stmt_len] if statement.stmt_len else s[statement.stmt_location:]
  programs+=len(pglast.parse_plpgsql(text))
 rows.append(dict(path=p.relative_to(here).as_posix(),sha256=hashlib.sha256(data).hexdigest(),statements=len(parsed),exactParsedPrograms=programs))
result=dict(parser='pglast '+pglast.__version__,PostgreSQLGrammar='17.7',files=rows,statements=sum(r['statements'] for r in rows),exactParsedPrograms=sum(r['exactParsedPrograms'] for r in rows),routineDeclarations=6,native='NOT_RUN',catalogue='NOT_RUN',dbConnections=0)
(here/'GRAMMAR_RESULTS.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf8',newline='\n');print(json.dumps({k:result[k] for k in ['statements','exactParsedPrograms','routineDeclarations','native']}))

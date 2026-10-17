import sys,json,re,hashlib,os
from pathlib import Path
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
here=Path(__file__).parent
p=next((here/'payload/supabase/migrations').glob('*.sql'));data=open(('\\\\?\\'+str(p.absolute())) if os.name=='nt' else p,'rb').read();text=data.decode();s=pglast.parse_sql(text);b=pglast.parse_plpgsql(text)
(here/'GRAMMAR_RESULTS.json').write_text(json.dumps(dict(path=p.relative_to(here).as_posix(),sha256=hashlib.sha256(data).hexdigest(),statements=len(s),exactParsedPrograms=len(b),newRoutineDefinitions=0,parser='pglast '+pglast.__version__,PostgreSQLGrammar='17.7',native='NOT_RUN',catalogue='NOT_RUN',dbConnections=0),indent=2)+'\n',encoding='utf8',newline='\n');print(json.dumps(dict(statements=len(s),programs=len(b),native='NOT_RUN')))

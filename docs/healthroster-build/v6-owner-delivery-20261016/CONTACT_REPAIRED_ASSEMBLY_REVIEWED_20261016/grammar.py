"""Offline PG17 SQL grammar and exact closed-body parse; no native catalog."""
import hashlib,json,pathlib,re,sys
sys.path.insert(0,r'C:/tmp/healthroster-v6-invoice-parser-20261016')
import pglast
from pglast import parser
here=pathlib.Path(__file__).resolve().parent
def read(p):return pathlib.Path('\\\\?\\'+str(here/p)).read_bytes()
manifest=json.loads(read(pathlib.Path('ASSEMBLY_MANIFEST.json')));rows=[]
for p in manifest['payloads']:
 if not p['path'].startswith('supabase/'):continue
 data=read(pathlib.Path('payload')/p['path']);assert hashlib.sha256(data).hexdigest()==p['sha256']
 sql=re.sub(r'^\\(?:set|ir)\b.*$','',data.decode('utf-8'),flags=re.M)
 ast=json.loads(parser.parse_sql_json(sql));rows.append({'path':p['path'],'sha256':p['sha256'],'statements':len(ast.get('stmts',[])),'qualification':'Offline PG17 SQL grammar; function bodies opaque, native catalog unresolved'})
closed=read(pathlib.Path('forward/01_complete_current_closed_command_start.sqlinc')).decode('utf-8')
pl=json.loads(parser.parse_plpgsql_json(closed));assert len(pl)==1
result={'version':'CONTACT_REPAIRED_OFFLINE_PG17_GRAMMAR_V1','library':pglast.__version__,'postgres':parser.get_postgresql_version(),'files':rows,'closedNativeFunctionBodyGrammar':'PASS_PG17_OFFLINE_PARSE_ONLY','nativeCompile':'NOT_RUN','databaseConnections':0,'failures':0}
(here/'GRAMMAR_RESULT.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8',newline='\n')
print(json.dumps({'grammarFiles':len(rows),'failures':0,'native':'NOT_RUN'}))

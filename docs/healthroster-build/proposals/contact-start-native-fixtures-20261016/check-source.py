"""Offline source verification. Never connects to PostgreSQL or a provider."""
import argparse,hashlib,json,re,subprocess,sys,tempfile
from pathlib import Path
args=argparse.ArgumentParser()
args.add_argument('--parser-path',required=True)
args.add_argument('--root',required=True)
options=args.parse_args()
sys.path.insert(0,options.parser_path)
from pglast import parser
if parser.get_postgresql_version()[0]!=17:raise RuntimeError('PG17 parser required')
base=Path(__file__).resolve().parent
root=Path(options.root)
contract=json.loads((root/'supabase/release/current-contract.json').read_text(encoding='utf-8'))
columns={r['schema']+'.'+r['name']:{c['name'] for c in r['columns']} for r in contract['relations']}
for name in ['15102026_0800_candidate_push_timing_authority.sql','15102026_0802_weekly_contact_pause_permits.sql']:
 source=re.sub(r'(?m)^\\[^\r\n]*','',(root/'supabase/migrations'/name).read_text(encoding='utf-8'))
 for stmt in parser.parse_sql(source):
  n=stmt.stmt
  if type(n).__name__=='CreateStmt':columns[n.relation.schemaname+'.'+n.relation.relname]={c.colname for c in n.tableElts if type(c).__name__=='ColumnDef'}
rowvars=0
for file in sorted(base.glob('*.sql')):
 source=file.read_text(encoding='utf-8')
 parser.parse_sql(source)
 parser.parse_plpgsql_json(re.sub(r'\b(?:public|private)\.\w+%rowtype\b','record',source,flags=re.I))
 for var,relation in re.findall(r'\b(\w+)\s+((?:public|private)\.\w+)%rowtype',source,re.I):
  if relation not in columns:raise RuntimeError('Unknown rowtype '+relation)
  for field in re.findall(r'\b'+re.escape(var)+r'\.(\w+)',source,re.I):
   if field not in columns[relation]:raise RuntimeError(file.name+': unknown '+relation+'.'+field)
  rowvars+=1
 # A fixture is never a replacement deployed function or trigger suppression.
 if re.search(r'create\s+(?:or\s+replace\s+)?function\s+(public|private)\.',source,re.I):raise RuntimeError('Non-TEMP fixture routine')
 if re.search(r'disable\s+trigger|session_replication_role|\bdblink\b|\bcommit\s*;',source,re.I):raise RuntimeError('Unsafe fixture mechanism')
 print(file.name+': PG17 SQL/PLpgSQL grammar parsed')
# Verify exact table/column names used by the dynamic before/fresh probe.
absence=(base/'08_before-and-fresh-absence.sql').read_text(encoding='utf-8')
for relation,column in re.findall(r"\('([a-z0-9_]+)','([a-z0-9_]+)'\)",re.search(r"FROM\(VALUES(.*?)\)x",absence,re.S).group(1)):
 if column not in columns.get('public.'+relation,set()):raise RuntimeError('Unknown absence column '+relation+'.'+column)
# Emitter unit tests use explicit SYNTHETIC UNIT input only, never native proof.
valid={'version':'CONTACT_FIXTURE_ABSENCE_SET_V1','database':'cloudtms_test_clone','objects':[
 {'relation_name':'public.weekly_message_target_attempts','column_name':'id','object_id':'f716c000-0000-4000-8000-000000000001'}]}
with tempfile.TemporaryDirectory(prefix='contact-start-emitter-unit-') as temp:
 receipt=Path(temp)/'receipt.json'
 def emit(obj):
  receipt.write_text(json.dumps(obj),encoding='utf-8')
  return subprocess.run([sys.executable,str(base/'emit-fresh-verification.py'),str(receipt)],capture_output=True,text=True)
 result=emit(valid)
 if result.returncode:raise RuntimeError(result.stderr)
 parser.parse_sql(result.stdout);parser.parse_plpgsql_json(result.stdout)
 bad=[{**valid,'database':'LIVE'},{**valid,'objects':[]},{**valid,'objects':[{**valid['objects'][0],'relation_name':"public.x;delete"}]},
  {**valid,'objects':[{**valid['objects'][0],'column_name':'id) OR true'}]},
  {**valid,'objects':[{**valid['objects'][0],'object_id':'F716C000-0000-4000-8000-000000000001'}]},
  {**valid,'objects':[{**valid['objects'][0],'extra':True}]}]
 for obj in bad:
  if emit(obj).returncode==0:raise RuntimeError('Malformed emitter receipt accepted')
print('Emitter: valid synthetic unit receipt parsed; six malformed receipt units rejected')
manifest=base/'SOURCE_MANIFEST.json'
if manifest.exists():
 data=json.loads(manifest.read_text(encoding='utf-8'))
 for file in data['payloads']:
  if hashlib.sha256((base/file['path']).read_bytes()).hexdigest()!=file['sha256']:raise RuntimeError('Payload digest changed '+file['path'])
 print('All source manifest payload digests match')
print(str(rowvars)+' declared native/new row variables checked; native cases/ACL/prefix/concurrency NOT_RUN')

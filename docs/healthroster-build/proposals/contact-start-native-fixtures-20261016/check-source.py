"""Offline source verification. Never connects to PostgreSQL or a provider."""
import argparse,hashlib,json,re,subprocess,sys,tempfile
from pathlib import Path
args=argparse.ArgumentParser()
args.add_argument('--parser-path',required=True)
args.add_argument('--root',required=True)
args.add_argument('--root-commit',help='Explicit immutable source qualification; never claims current saved integration')
options=args.parse_args()
sys.path.insert(0,options.parser_path)
from pglast import parser
if parser.get_postgresql_version()[0]!=17:raise RuntimeError('PG17 parser required')
base=Path(__file__).resolve().parent
root=Path(options.root)
def root_read(path):
 if options.root_commit:return subprocess.check_output(['git','show',options.root_commit+':'+path],cwd=root).decode('utf-8')
 return (root/path).read_text(encoding='utf-8')
contract=json.loads(root_read('supabase/release/current-contract.json'))
columns={r['schema']+'.'+r['name']:{c['name'] for c in r['columns']} for r in contract['relations']}
for name in ['15102026_0800_candidate_push_timing_authority.sql','15102026_0802_weekly_contact_pause_permits.sql']:
 source=re.sub(r'(?m)^\\[^\r\n]*','',root_read('supabase/migrations/'+name))
 for stmt in parser.parse_sql(source):
  n=stmt.stmt
  if type(n).__name__=='CreateStmt':columns[n.relation.schemaname+'.'+n.relation.relname]={c.colname for c in n.tableElts if type(c).__name__=='ColumnDef'}
rowvars=0
for file in sorted(p for p in base.rglob('*.sql') if 'preparer' not in p.relative_to(base).parts):
 source=file.read_text(encoding='utf-8')
 for relation,listed in re.findall(r'insert\s+into\s+((?:public|private)\.\w+)\s*\(([^)]+)\)',source,re.I):
  for field in listed.split(','):
   if field.strip().lower() not in columns.get(relation.lower(),set()):raise RuntimeError(file.name+': unknown INSERT '+relation+'.'+field.strip())
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
 aliases={}
 for relation,alias in re.findall(r'(?:from|join)\s+((?:public|private)\.\w+)\s+(?:as\s+)?(\w+)',source,re.I):
  if alias.lower() not in ('where','set','on','join','union','order','for','returning','values'):
   aliases.setdefault(alias.lower(),set()).update(columns.get(relation.lower(),set()))
 for alias,known in aliases.items():
  for field in re.findall(r'\b'+re.escape(alias)+r'\.(\w+)',source,re.I):
   if field.lower() not in known:raise RuntimeError(file.name+': unknown native alias field '+alias+'.'+field)
 print(str(file.relative_to(base))+': PG17 SQL/PLpgSQL grammar parsed')
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
 bad=[{**valid,'database':'LIVE'},{**valid,'database':'banking_modal_v2_test'},{**valid,'objects':[]},{**valid,'objects':[{**valid['objects'][0],'relation_name':"public.x;delete"}]},
  {**valid,'objects':[{**valid['objects'][0],'column_name':'id) OR true'}]},
  {**valid,'objects':[{**valid['objects'][0],'object_id':'F716C000-0000-4000-8000-000000000001'}]},
  {**valid,'objects':[{**valid['objects'][0],'extra':True}]}]
 for obj in bad:
  if emit(obj).returncode==0:raise RuntimeError('Malformed emitter receipt accepted')
 restored={**valid,'restoration':{'settings_defaults_sha256':'1'*64,'global_contact_settings_sha256':'2'*64}}
 result=emit(restored)
 if result.returncode:raise RuntimeError(result.stderr)
 parser.parse_sql(result.stdout);parser.parse_plpgsql_json(result.stdout)
 for obj in [{**restored,'restoration':{'settings_defaults_sha256':'x'}},
  {**restored,'restoration':{**restored['restoration'],'global_contact_settings_sha256':"';bad"}}]:
  if emit(obj).returncode==0:raise RuntimeError('Invalid restoration seal accepted')
print('Emitter: two synthetic unit receipts parsed; nine malformed receipt units rejected')
manifest=base/'SOURCE_MANIFEST.json'
if manifest.exists():
 data=json.loads(manifest.read_text(encoding='utf-8'))
 for file in data['payloads']:
  if hashlib.sha256((base/file['path']).read_bytes()).hexdigest()!=file['sha256']:raise RuntimeError('Payload digest changed '+file['path'])
 for item in data['source_inputs']:
  current=root_read(item['path']).replace('\r\n','\n').encode()
  if hashlib.sha256(current).hexdigest()!=item.get('canonical_source_sha256',item['git_blob_sha256']):raise RuntimeError('Pinned root input changed '+item['path'])
 print('All payload and pinned root source digests match; source view='+('immutable Git '+options.root_commit if options.root_commit else 'current saved disk'))
print(str(rowvars)+' declared native/new row variables checked; native cases/ACL/prefix/concurrency NOT_RUN')

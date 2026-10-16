"""Prepare only actual pinned0339 query slice + pg_temp fixtures, never execute SQL."""
import pathlib,sys,json,re,hashlib
sys.path.insert(0,sys.argv[1])
import pglast
fixture=pathlib.Path(__file__).resolve().parent
proposal=fixture.parent
record=json.loads((fixture/'QUERY_NATIVE_PREFIX_INPUTS.json').read_text())
native=pathlib.Path(record['root']).resolve()
pins={x['file']:x['sha256'] for x in record['inputs']}
used=[]

def flatten(relative,stack=()):
 file=(native/relative).resolve()
 key=file.relative_to(native).as_posix()
 if key in stack:raise RuntimeError('Recursive native include: '+key)
 raw=file.read_bytes()
 if hashlib.sha256(raw).hexdigest()!=pins[key]:raise RuntimeError('Current native bytes differ from pinned input: '+key)
 used.append(key)
 source=raw.decode('utf-8')
 lines=[]
 for line in source.splitlines(keepends=True):
  include=re.fullmatch(r'\s*\\ir\s+([^\r\n]+)\s*',line)
  if include:
   child=(file.parent/include[1].strip()).resolve().relative_to(native).as_posix()
   lines.append(flatten(child,stack+(key,)))
  elif line.lstrip().startswith('\\'):
   if not re.fullmatch(r'\s*\\set\s+ON_ERROR_STOP\s+on\s*',line,re.I):raise RuntimeError('Unexpected native psql directive: '+line)
  else:lines.append(line)
 return ''.join(lines)

def server_owned_body(source):
 tree=json.loads(pglast.parser.parse_sql_json(source))
 parts=[]
 for entry in tree['stmts']:
  node=entry['stmt']
  if 'TransactionStmt' in node:
   if node['TransactionStmt']['kind'] not in('TRANS_STMT_BEGIN','TRANS_STMT_COMMIT'):raise RuntimeError('Unexpected native transaction statement')
   continue
  if 'NotifyStmt' in node:
   if node['NotifyStmt']['conditionname']!='pgrst' or node['NotifyStmt'].get('payload')!='reload schema':raise RuntimeError('Unexpected native notify')
   continue
  start=entry.get('stmt_location',0);length=entry.get('stmt_len',0)
  # libpg_query offsets are UTF-8 byte offsets, not Python character offsets.
  raw=source.encode('utf-8')
  part=raw[start:start+length if length else None].decode('utf-8').rstrip().removesuffix(';')
  parts.append(part+';\n')
 return '\n'.join(parts)

units=['supabase/migrations/16102026_0339_weekly_contact_query_lineage_and_claim_fence.sql',
 'supabase/repeatable/16102026_0339_weekly_contact_query_native_lifecycle_v1.sql']
sql='-- SOURCE ONLY: query slice in root server-owned TEST rollback; genuine native base still required.\n'
for unit in units:sql+='\n-- Actual current owner: '+unit+'\n'+server_owned_body(flatten(unit))
for file in ['native-query-contact-assertions.sqlinc','query-native-inputs.sqlinc','query-native-prefix.sqlinc','query-native-cases.sqlinc','query-native-older-cases.sqlinc']:
 sql+='\n-- Temporary unexecuted fixture: '+file+'\n'+(fixture/file).read_text(encoding='utf-8')
statement_count=len(pglast.parse_sql(sql));byte_count=len(sql.encode('utf-8'))
if byte_count>8_000_000 or statement_count>1_000:raise RuntimeError('Query slice itself exceeds fixed Kier envelope; no fallback or omitted checks')
output=proposal/'test-runtime'
output.mkdir(exist_ok=True)
(output/'query-native-current-closure.sql').write_text(sql,encoding='utf-8')
result={'version':'QUERY_NATIVE_CLOSURE_PREPARATION_V1','database_execution':False,'root_source_head':record['root_head'],
 'source_units':units,'flattened_input_files':sorted(set(used)),'query_slice_sql_bytes':byte_count,'query_slice_statements':statement_count,
 'query_slice_sha256':hashlib.sha256(sql.encode('utf-8')).hexdigest(),'whole_native_trial_size_proved':False,
 'native_business_constructor_required':True,'default_controlled_success_enabled':False,'accepted_result_assertions':'REQUIRED_NOT_RUN_PENDING_REVIEWED_FAKE_EFFECT_HARNESS',
 'output':'test-runtime/query-native-current-closure.sql','native':'REQUIRED_NOT_RUN'}
(fixture/'QUERY_NATIVE_CLOSURE_PREPARATION.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
print(json.dumps(result))

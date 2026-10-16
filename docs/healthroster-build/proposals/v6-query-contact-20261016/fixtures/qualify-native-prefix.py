"""Offline fixture scope/write inventory. No connection, role or SQL execution."""
import pathlib,sys,json,re,hashlib
sys.path.insert(0,sys.argv[1])
import pglast
from pglast.stream import RawStream
fixture=pathlib.Path(__file__).resolve().parent
root=fixture.parent
failures=[]
functions=[]
writes=[]

def walk(value):
 if isinstance(value,dict):
  yield value
  for child in value.values():yield from walk(child)
 elif isinstance(value,list):
  for child in value:yield from walk(child)

def inventory(tree,file,function):
 for node in walk(tree):
  for kind in ('InsertStmt','UpdateStmt','DeleteStmt'):
   if kind not in node:continue
   relation=node[kind]['relation']
   target=relation.get('schemaname','')+'.'+relation['relname']
   writes.append({'file':file,'function':function,'kind':kind,'target':target})
   allowed=(kind=='InsertStmt' and target in('pg_temp.query_contact_native_prefix_receipts_v1','pg_temp.query_contact_native_legacy_claim_receipts_v1'))
   allowed=allowed or(kind=='UpdateStmt' and target=='public.mail_outbox' and function=='pg_temp.query_contact_old_claim_guard_assert_v1')
   if not allowed:failures.append({'forbidden_direct_fixture_write':writes[-1]})
  if 'CreateStmt'in node:
   relation=node['CreateStmt']['relation']
   if relation.get('relpersistence')!='t' or relation['relname'] not in('query_contact_native_prefix_receipts_v1','query_contact_native_legacy_claim_receipts_v1'):
    failures.append({'non_receipt_table_creation':relation})

for file in sorted(fixture.glob('*.sqlinc')):
 sql=file.read_text(encoding='utf-8')
 parsed=json.loads(pglast.parser.parse_sql_json(sql))
 for statement in parsed['stmts']:
  body=statement['stmt']
  if set(body)!=set(['CreateFunctionStmt']):
   failures.append({'non_function_top_level_statement':file.name,'statement':list(body)})
   continue
  fn=body['CreateFunctionStmt']
  name='.'.join(x['String']['sval'] for x in fn['funcname'])
  if not name.startswith('pg_temp.'):failures.append({'permanent_fixture_function':name})
  functions.append({'file':file.name,'function':name})
  options={x['DefElem']['defname']:x['DefElem'].get('arg') for x in fn['options']}
  language=options['language']['String']['sval']
  function_source=options['as']['List']['items'][0]['String']['sval']
  if language=='sql':inventory(json.loads(pglast.parser.parse_sql_json(function_source)),file.name,name)
  elif language=='plpgsql':
   # Parse this one exact function with declaration-only ROWTYPE parser aliases.
   one=RawStream()(pglast.parse_sql(sql)[parsed['stmts'].index(statement)])
   one=re.sub(r'\b(?:public|private)\.[a-z0-9_]+%rowtype\b','record',one,flags=re.I)
   for node in walk(pglast.parse_plpgsql(one)):
    if 'PLpgSQL_expr' not in node:continue
    expr=node['PLpgSQL_expr'];q=expr['query'];mode=expr.get('parseMode',0)
    if mode==2:q='SELECT '+q
    elif mode in(3,4,5):q='SELECT '+re.split(r':=|=(?!=)',q,maxsplit=1)[1]
    inventory(json.loads(pglast.parser.parse_sql_json(q)),file.name,name)
  else:failures.append({'unexpected_fixture_language':language})

grammar=json.loads((root/'SOURCE_PARSE_RESULT.json').read_text())
native=json.loads((root/'NATIVE_SOURCE_CHECK.json').read_text())
inputs=json.loads((fixture/'QUERY_NATIVE_PREFIX_INPUTS.json').read_text())
if grammar['failures'] or native['failures']:failures.append({'required_source_checks_failed':True})
result={'version':'QUERY_NATIVE_PREFIX_QUALIFICATION_V1','database_execution':False,'provider_called':False,'r2_route_verified':False,
 'root_source_head':inputs['root_head'],'source_input_files':len(inputs['inputs']),
 'checks':{'offline_pg17_and_inner_sql_grammar':'PASS' if not grammar['failures'] else 'FAIL',
  'temporary_fixture_functions':len(functions),'native_declared_rowtype_checks':len(native['rowtype_checks']),
  'original_native_render_attachment_ast_queries_unchanged':native['native_render_attachment_ast_queries_unchanged'],
  'fixture_direct_write_inventory':'PASS' if not failures else 'FAIL'},
 'temporary_functions':functions,'direct_writes':writes,
 'source_files':[{'file':p.name,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for p in sorted(fixture.glob('*')) if p.suffix=='.sqlinc'],
 'native_case_status':{x:'REQUIRED_NOT_RUN' for x in ['real_review_selected_query_apply_outcomes_enqueue','old_cached_claim_update_fenced',
  'current_native_claim_stamp_cleanup','genuine_pre_ddl_active_legacy_snapshot','genuine_older_expired_legacy_inflight',
  'original_multiworker_pause_subset_html_attachment_lineage_replay','controlled_sql_result_replay_resume_no_refill',
  'genuine_older_unknown_reclaim_no_second_call','shared_pdf_refusal_and_source_preservation','fresh_rollback_restoration']},
 'controlled_sql_success':'Do not enable in current recommended trial. Accepted-result/resume remains NOT_RUN pending separately reviewed explicit fake-effect SQL harness; never external acceptance proof.',
 'clock_limit':'Original native claim uses transaction_timestamp(); no backdating or fake expiry. Genuine older inputs required.',
 'failures':failures,'native':'REQUIRED_NOT_RUN'}
(fixture/'QUERY_NATIVE_PREFIX_QUALIFICATION.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'temporary_functions':len(functions),'direct_writes':writes,'failures':failures,'native':'REQUIRED_NOT_RUN'}))
sys.exit(bool(failures))

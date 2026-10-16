"""Offline actual catalogue-shape and finite composition checks; no DB."""
import pathlib,json,re,sys
sys.path.insert(0,sys.argv[1]);import pglast
root=pathlib.Path(__file__).resolve().parent
native=pathlib.Path(json.loads((root/'SOURCE_INPUTS.json').read_text())['root']);catalog={};checks=[];failures=[]
def walk(value):
 if isinstance(value,dict):
  yield value
  for child in value.values():yield from walk(child)
 elif isinstance(value,list):
  for child in value:yield from walk(child)
def source(file):return re.sub(r'^\\.*$','',file.read_text(encoding='utf-8'),flags=re.M)
def grammar(sql):return re.sub(r'\b(?:public|private)\.[a-z0-9_]+%rowtype\b','record',sql,flags=re.I)
def expressions(node):
 for item in walk(node):
  if 'PLpgSQL_expr' in item:yield item['PLpgSQL_expr']
for file in [native/'supabase/baseline/22082026_1500_cloudtms_test_structural_baseline.sql',native/'supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql',native/'supabase/migrations/15092026_2310_weekly_source_delivery_targets.sql',native/'supabase/migrations/15102026_0802_weekly_contact_pause_permits.sql']:
 for item in walk(json.loads(pglast.parser.parse_sql_json(source(file)))):
  if 'CreateStmt' in item:
   stmt=item['CreateStmt'];rel=stmt['relation'];name=rel.get('schemaname','public')+'.'+rel['relname']
   catalog.setdefault(name,set()).update(c['ColumnDef']['colname'] for c in stmt.get('tableElts',[])if 'ColumnDef'in c)
  if 'AlterTableStmt'in item:
   stmt=item['AlterTableStmt'];rel=stmt['relation'];name=rel.get('schemaname','public')+'.'+rel['relname']
   for c in walk(stmt.get('cmds',[])):
    if 'ColumnDef'in c:catalog.setdefault(name,set()).add(c['ColumnDef']['colname'])
for file in list((root/'forward').glob('*.sqlinc'))+list((root/'fixtures').glob('*.sqlinc')):
 sql=file.read_text(encoding='utf-8')
 for variable,table in re.findall(r'\b([a-z_][a-z0-9_]*)\s+((?:public|private)\.[a-z0-9_]+)%rowtype',sql,re.I):
  if table not in catalog:failures.append({'file':file.name,'table_missing':table});continue
  for field in sorted(set(re.findall(r'\b'+re.escape(variable)+r'\.([a-z_][a-z0-9_]*)',sql,re.I))):
   if field not in catalog[table]:failures.append({'file':file.name,'variable':variable,'table':table,'field':field})
  checks.append({'file':file.name,'variable':variable,'table':table})
writes=[]
for file in (root/'forward').glob('*.sqlinc'):
 sql=file.read_text(encoding='utf-8')
 if not re.search(r'language\s+plpgsql',sql,re.I):continue
 for expr in expressions(pglast.parse_plpgsql(grammar(sql))):
  q=expr['query'];mode=expr.get('parseMode',0)
  if mode==2:q='SELECT '+q
  elif mode in(3,4,5):q='SELECT '+re.split(r':=|=(?!=)',q,maxsplit=1)[1]
  for node in walk(json.loads(pglast.parser.parse_sql_json(q))):
   for command in ['InsertStmt','UpdateStmt','DeleteStmt']:
    if command in node:
     stmt=node[command];relation=stmt['relation'];name=relation.get('schemaname','public')+'.'+relation['relname']
     columns=[x.get('ResTarget',{}).get('name')for x in stmt.get('targetList',[])]
     writes.append({'file':file.name,'command':command,'relation':name,'update_columns':columns})
     if name!='public.weekly_contact_delivery_memberships' or command=='DeleteStmt' or(command=='UpdateStmt' and columns!=['accepted_at_utc']):
      failures.append({'forbidden_native_mutation':name,'command':command,'columns':columns})
for file in (root/'test-runtime').glob('*.sql'):
 sql=source(file);pglast.parse_sql(sql)
 for expr in expressions(pglast.parse_plpgsql(grammar(sql))):
  q=expr['query'];mode=expr.get('parseMode',0)
  if mode==2:q='SELECT '+q
  elif mode in(3,4,5):q='SELECT '+re.split(r':=|=(?!=)',q,maxsplit=1)[1]
  pglast.parse_sql(q)
result={'database_execution':False,'rowtype_checks':checks,'composed_current_native_grammar':'PASS','native_write_scope':writes,
 'sibling_target_command_render_generation_writes_from_new_owners':False,'failures':failures,'native':'REQUIRED_NOT_RUN'}
(root/'NATIVE_SOURCE_CHECK.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'rowtype_checks':len(checks),'failures':failures,'native':'REQUIRED_NOT_RUN'}));sys.exit(bool(failures))

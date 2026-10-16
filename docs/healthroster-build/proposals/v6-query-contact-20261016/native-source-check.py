"""Read-only catalogue-shape/grammar/projection checks; never connects to PostgreSQL."""
import pathlib,json,re,sys
sys.path.insert(0,sys.argv[1])
import pglast
root=pathlib.Path(__file__).resolve().parent
native=pathlib.Path(json.loads((root/'SOURCE_INPUTS.json').read_text())['root'])
catalog={}
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
def canonical(value):
 if isinstance(value,dict):return {k:canonical(v) for k,v in value.items() if k not in('location','stmt_location','stmt_len')}
 if isinstance(value,list):return [canonical(x) for x in value]
 return value
for file in [native/'supabase/baseline/22082026_1500_cloudtms_test_structural_baseline.sql',native/'supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql',native/'supabase/migrations/15102026_0800_candidate_push_timing_authority.sql',native/'supabase/migrations/15102026_0802_weekly_contact_pause_permits.sql',root/'forward/010_existing_delivery_lineage.sqlinc']:
 tree=json.loads(pglast.parser.parse_sql_json(source(file)))
 for item in walk(tree):
  if 'CreateStmt' in item:
   stmt=item['CreateStmt'];rel=stmt['relation'];name=rel.get('schemaname','public')+'.'+rel['relname']
   catalog.setdefault(name,set()).update(col['ColumnDef']['colname'] for col in stmt.get('tableElts',[]) if 'ColumnDef'in col)
  if 'AlterTableStmt' in item:
   stmt=item['AlterTableStmt'];rel=stmt['relation'];name=rel.get('schemaname','public')+'.'+rel['relname']
   for col in walk(stmt.get('cmds',[])):
    if 'ColumnDef' in col:catalog.setdefault(name,set()).add(col['ColumnDef']['colname'])
checks=[];failures=[]
# Exact corrected contact fixture aliases, checked against the real baseline.
contact_fixture=(root.parent/'v6-contact-timing-20261016/fixtures/native-contact-assertions.sqlinc').read_text()
for alias,table in [('m','public.weekly_manager_recipient_memberships'),('g','public.weekly_candidate_outreach_generations'),('e','public.weekly_manager_cohort_due_events'),('i','public.weekly_discrepancy_incidents')]:
 for field in sorted(set(re.findall(r'\b'+alias+r'\.([a-z_][a-z0-9_]*)',contact_fixture,re.I))):
  if field not in catalog.get(table,set()):failures.append({'contact_fixture_alias':alias,'table':table,'field':field})
for file in list((root/'forward').glob('*.sqlinc'))+list((root/'fixtures').glob('*.sqlinc')):
 sql=file.read_text();variables=re.findall(r'\b([a-z_][a-z0-9_]*)\s+((?:public|private)\.[a-z0-9_]+)%rowtype',sql,re.I)
 for variable,table in variables:
  if table not in catalog:failures.append({'file':file.name,'table_missing':table});continue
  for field in sorted(set(re.findall(r'\b'+re.escape(variable)+r'\.([a-z_][a-z0-9_]*)',sql,re.I))):
   if field not in catalog[table]:failures.append({'file':file.name,'variable':variable,'table':table,'field':field})
  checks.append({'file':file.name,'variable':variable,'table':table})
# Root0226 finite composition, including PL/SQL expressions, without installing.
sql=(root/'test-runtime/root0226.sqlinc').read_text();pglast.parse_sql(sql)
procedures=pglast.parse_plpgsql(grammar(sql))
for e in expressions(procedures):
 q=e['query'];mode=e.get('parseMode',0)
 if mode==2:q='SELECT '+q
 elif mode in(3,4,5):q='SELECT '+re.split(r':=|=(?!=)',q,maxsplit=1)[1]
 pglast.parse_sql(q)
# Compare the actual native HTML/text render and attachment projection ASTs.
original=(root/'reference/issued-query.sql').read_text();start=original.index('create or replace function public.timesheet_query_email_enqueue_v1(')
original=original[start:original.index('end $function$;',start)+len('end $function$;')]
amended=(root/'forward/030_native_enqueue_core.sqlinc').read_text()
def render_queries(sql):
 result=[]
 for e in expressions(pglast.parse_plpgsql(grammar(sql))):
  q=e['query']
  if q.startswith('with lines as (') or q.startswith('select array_agg(q.issue_id'):
   result.append(canonical(json.loads(pglast.parser.parse_sql_json(q))))
 return result
before=render_queries(original);after=render_queries(amended)
if len(before)!=3 or before!=after:failures.append({'native_render_projection':'CHANGED_OR_MISSING'})
result={'database_execution':False,'rowtype_checks':checks,'root0226_composition_grammar':'PASS','native_render_attachment_ast_queries_unchanged':len(before) if before==after else False,'failures':failures,'native':'REQUIRED_NOT_RUN'}
(root/'NATIVE_SOURCE_CHECK.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({'rowtype_checks':len(checks),'unchanged_render_queries':len(before),'failures':failures}))
sys.exit(bool(failures))

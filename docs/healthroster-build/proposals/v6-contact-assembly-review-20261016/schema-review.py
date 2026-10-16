"""Independent typed native CREATE/ADD, constraint and managed grammar review."""
import hashlib,json,pathlib,re,subprocess,sys
HERE=pathlib.Path(__file__).resolve().parent;ROOT=pathlib.Path(sys.argv[1]);COMMIT='2d74b1fdced14d4a60882720d7e590ad83f8dd5a';CURRENT='efd18231c06df14ed91c7f6b137ddb6fec7e2df2';BASE='docs/healthroster-build/proposals/v6-contact-managed-assembly-20261016/'
sys.path.insert(0,r'C:/tmp/healthroster-v6-invoice-parser-20261016')
import pglast
from pglast import parser
frozen=lambda p,ref=COMMIT:subprocess.check_output(['git','show',ref+':'+p],cwd=ROOT).decode('utf-8')
sha=lambda x:hashlib.sha256(x if isinstance(x,bytes)else x.encode()).hexdigest()
proof=json.loads(frozen(BASE+'PREREQUISITE_RELATION_REVIEW.json'));manifest=json.loads(frozen(BASE+'ASSEMBLY_MANIFEST.json'));contract=json.loads(frozen('supabase/release/current-contract.json',CURRENT));relations={r['schema']+'.'+r['name']:r for r in contract['relations']};checks=[]
def check(name,b):
 assert b,name
 checks.append({'name':name,'result':'PASS'})
def sql_ast(text):return json.loads(parser.parse_sql_json(re.sub(r'^\\[^\r\n]*','',text,flags=re.M)))
cache={}
def native_columns(p):
 if p in cache:return cache[p]
 text=frozen(p,proof['rootCommit']);tree=sql_ast(text);fields={}
 for stmt in tree['stmts']:
  s=stmt['stmt']
  if 'CreateStmt'in s:
   create=s['CreateStmt'];r=create['relation'];name=r.get('schemaname','public')+'.'+r['relname']
   for col in create.get('tableElts',[]):
    if 'ColumnDef'in col:fields[name+'.'+col['ColumnDef']['colname']]=col['ColumnDef']
  if 'AlterTableStmt'in s:
   alter=s['AlterTableStmt'];r=alter['relation'];name=r.get('schemaname','public')+'.'+r['relname']
   for cmd in alter['cmds']:
    c=cmd['AlterTableCmd']
    if c['subtype']=='AT_AddColumn'and'ColumnDef'in c.get('def',{}):fields[name+'.'+c['def']['ColumnDef']['colname']]=c['def']['ColumnDef']
 cache[p]={'sha256':sha(text),'fields':fields};return cache[p]
review=[]
for relation in proof['relations']:
 columns=[]
 for col in relation['columns']:
  key=relation['relation']+'.'+col['column'];evidence=[]
  for source in col['sourceEvidence']:
   parsed=native_columns(source['path']);check('Exact pinned native schema file '+source['path'],parsed['sha256']==source['sha256'])
   if key in parsed['fields']:evidence.append({'path':source['path'],'sha256':source['sha256'],'actualColumnDef':parsed['fields'][key]})
  check('Qualified relation/field exists in actual native CREATE/ADD AST '+key,bool(evidence))
  current=next((c for c in relations.get(relation['relation'],{}).get('columns',[])if c['name']==col['column']),None)
  columns.append({'name':col['column'],'actualNativeDeclarationEvidence':evidence,'currentContractColumn':current,'qualification':'Native source type/constraint evidence; actual catalogue/field binding and NULL/currentness still need native first use.'})
 review.append({'relation':relation['relation'],'columns':columns})
grammar=[]
for row in manifest['payloads']:
 if not re.search(r'\.sql(?:inc)?$',row['path']):continue
 text=frozen(BASE+'payload/'+row['path']);tree=sql_ast(text);grammar.append({'path':row['path'],'sha256':sha(text),'statements':len(tree['stmts'])})
check('All61SQLmanaged payloads parse PG17 grammar',len(grammar)==61)
schema02=frozen(BASE+'payload/supabase/migrations/16102026_1201_weekly_contact_automatic_followup_tranche.sql');schema03=frozen(BASE+'payload/supabase/migrations/16102026_1202_weekly_contact_submission_cohort_identity.sql')
enum=[]
for rel,col,cname,temporary,addition in [('public.weekly_message_dispatch_commands','tranche_kind','weekly_message_dispatch_commands_tranche_kind_check','weekly_contact_expected_original_tranche_ck','MANAGER_AUTOMATIC_FOLLOWUP'),('public.weekly_manager_recipient_generations','trigger_kind','weekly_manager_recipient_generations_trigger_kind_check','weekly_contact_expected_original_generation_ck','CONTACT_COMPOSITION')]:
 native=next(c for c in relations[rel]['constraints']if c['name']==cname)
 old=re.findall(r"'([^']+)'::text",native['definition']);pre=re.search(r'add constraint '+temporary+r'\s+check\('+col+r' in\((.*?)\)\) not valid;',schema02,re.S)[1];new=re.search(r'add constraint '+cname+r'\s+check\('+col+r' in\((.*?)\)\);',schema02,re.S)[1]
 check('Exact native enum predecessor and sole allowed addition '+rel,re.findall(r"'([^']+)'",pre)==old and re.findall(r"'([^']+)'",new)==old+[addition])
 enum.append({'relation':rel,'constraint':native,'oldValues':old,'newValues':old+[addition],'guard':'Exact named validated field shape, CURRENT_USER owner, server-parsed temporary NOTVALID expression equality, atomic complete replacement; native catalogue/lock proof still required.'})
submission=relations['public.weekly_timesheet_submission_requests'];generation=relations['public.weekly_candidate_outreach_generations'];priorUnique=next(c for c in submission['constraints']if c['type']=='u'and c['definition']=='UNIQUE (source_cycle_id, candidate_id, request_generation)')
active=next(i for i in submission['indexes']if i['name']=='weekly_timesheet_submission_requests_active_uq');gactive=next(i for i in generation['indexes']if i['name']=='weekly_candidate_outreach_generations_active_uq')
check('Exact prior request unique shape and native active index are retained in prerequisite guard',"array['source_cycle_id','candidate_id','request_generation']"in schema03 and"array['source_cycle_id','candidate_id']"in schema03 and"[''READY''::text, ''ACTIVE''::text, ''OVERDUE''::text, ''PARTLY_SUBMITTED''::text]"in schema03 and'(source_cycle_id, candidate_id)'in active['definition'])
check('Native per-cohort purpose active-generation index guarded',"array['candidate_cohort_id','request_kind']"in schema03 and'(candidate_cohort_id, request_kind)'in gactive['definition'])
check('Full retained identity/overlap proof precedes actual identity DDL',schema03.index("raise exception 'CONTACT_SUBMISSION_RETAINED_IDENTITY_UNRESOLVED'")<schema03.index("execute format('alter table public.weekly_timesheet_submission_requests drop constraint"))
effectReview={'schema':'Nine contact metadata tables with owner/service RLS and direct table grants revoked; immutable update/delete and truncate guards installed once; complete unchanged raw schema preserved.','repeatable':'Eight ordinary CREATE OR REPLACE triggers and seven explicit DROP IF EXISTS/constraint CREATE pairs are repeatable native setup; original full definitions/effects preserved. Exact native metadata/rollback/pending-event/reapplication proof remains required.','predecessorAtomicity':'1201 takes actual ALTER TABLE lock before its canonical CHECK comparison/replacement;1202 actual native owner/index/data assertions require a protected installation with genuine concurrency/rollback verification. No native data/count/assertion was executed.'}
result={'version':'INDEPENDENT_CONTACT_NATIVE_SCHEMA_FIELD_REVIEW_V1','sourceCommit':COMMIT,'currentRootCommit':CURRENT,'qualification':'SOURCE_ONLY_TYPED_DDL_AST_AND_GUARD_REVIEW_NOT_NATIVE_CATALOGUE_OR_DATA_ACCEPTANCE','parser':{'pglast':pglast.__version__,'postgresql':pglast.get_postgresql_version()},'relations':review,'managedGrammar':grammar,'enumPredecessors':enum,'submissionPredecessors':{'unique':priorUnique,'activeRequestIndex':active,'activeGenerationIndex':gactive},'effects':effectReview,'checks':checks,'passed':len(checks),'native':'NOT_RUN','databaseConnections':0}
(HERE/'SCHEMA_REVIEW.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8',newline='\n');print(json.dumps({'passed':len(checks),'relations':len(review),'fields':sum(len(r['columns'])for r in review),'sha256':sha((HERE/'SCHEMA_REVIEW.json').read_bytes())},indent=2))

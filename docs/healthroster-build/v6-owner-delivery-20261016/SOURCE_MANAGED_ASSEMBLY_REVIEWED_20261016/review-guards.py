"""Exact native DDL/guard/whole-SQL grammar review; no DB/type resolution."""
import hashlib,json,os,pathlib,re,subprocess,sys
resolved=pathlib.Path(__file__).resolve().parent
HERE=pathlib.Path('\\\\?\\'+str(resolved))if os.name=='nt'else resolved
ROOT=pathlib.Path(sys.argv[1])
sys.path.insert(0,r'C:/tmp/healthroster-v6-invoice-parser-20261016')
import pglast
from pglast import parser
sha=lambda x:hashlib.sha256(x if isinstance(x,bytes)else x.encode()).hexdigest()
M=json.loads((HERE/'ASSEMBLY_MANIFEST.json').read_text(encoding='utf-8'))
frozen=lambda ref,p:subprocess.check_output(['git','show',ref+':'+p],cwd=ROOT).decode('utf-8')
def clean(x):
 if isinstance(x,dict):return{k:clean(v)for k,v in x.items()if k not in('location','stmt_location','stmt_len')}
 if isinstance(x,list):return[clean(v)for v in x]
 return x
checks=[]
def check(name,b):
 assert b,name
 checks.append({'name':name,'result':'PASS'})
parsed=[]
for item in M['output']:
 text=(HERE/'managed'/item['path']).read_text(encoding='utf-8')
 # Leaves are parsed separately. Entry-only psql controls have no SQL grammar.
 sql=re.sub(r'^\\(?:set|ir)\b.*$','',text,flags=re.M)
 ast=json.loads(parser.parse_sql_json(sql))
 parsed.append({'path':item['path'],'sha256':item['sha256'],'sqlStatements':len(ast['stmts']),'qualification':'PG17SQLgrammaronly; function bodies opaque, actual native dependencies unresolved'})
check('All13managed files SQL grammar parses',len(parsed)==13)
effect=(HERE/M['guards']['triggers']['path']).read_text(encoding='utf-8')
effect_pl=json.loads(parser.parse_plpgsql_json(effect))
check('All15static trigger DO bodies PLpgSQL grammar parses',len(effect_pl)==15)
effect_sql=json.loads(parser.parse_sql_json(effect))['stmts']
check('15locks plus15DO blocks',len(effect_sql)==30 and sum('LockStmt'in x['stmt']for x in effect_sql)==15)
shape_review=[]
for number,t in enumerate(M['triggerSpecs'],1):
 old=frozen(M['sourceCommits']['foundation'],t['origin']['path'])
 if t['origin']['derivation'].startswith('Exact11'):
  old_array=re.search(r'foreach name in array array\[([\s\S]*?)\] loop',old)[1]
  check('Metadata table belongs to exact native FOREACH '+t['table'],"'"+t['table']+"'"in old_array)
  template=re.search(r"execute format\('(create trigger [^']+)',name\)",old)[1]
  native=template.replace('%I',t['table'])+';'
 else:
  native=next(x['statement']for x in M['suppressedNativeTriggerSetupStatements']if x['statementSha256']==t['origin']['statementSha256'])
  check('Exact original direct trigger statement '+t['name'],native in old)
 native_ast=clean(json.loads(parser.parse_sql_json(native)))
 generated_ast=clean(json.loads(parser.parse_sql_json(t['createSql'])))
 check('Whole native trigger CREATE AST equal '+t['qualifiedTable']+'.'+t['name'],native_ast==generated_ast)
 a=generated_ast['stmts'][0]['stmt']['CreateTrigStmt']
 check('Exact trigger catalog type/deferral '+t['name'],a.get('row',False)and(a.get('timing',0)|a['events']|1)==t['type']and a.get('isconstraint',False)==t['deferred']and a.get('deferrable',False)==t['deferred']and a.get('initdeferred',False)==t['deferred']and not a.get('args')and not a.get('columns')and not a.get('whenClause')and not a.get('transitionRels'))
 check('Exact relation/function binding '+t['name'],a['relation']['schemaname']+'.'+a['relation']['relname']==t['qualifiedTable']and'.'.join(x['String']['sval']for x in a['funcname'])+'()'==t['function'])
 body=re.search(r'\$source_trigger_'+str(number).zfill(2)+r'\$(.*?)\$source_trigger_'+str(number).zfill(2)+r'\$;',effect,re.S)[1]
 check('Absent-create/existing-mismatch raise branch '+t['name'],body.count(t['createSql'])==1 and len(re.findall(r'^ if not exists\(',body,re.M))==1 and len(re.findall(r'^ elsif not exists\(',body,re.M))==1 and "raise exception 'SOURCE_HEALTH_TRIGGER_PREDECESSOR_MISMATCH' using errcode='55000'"in body)
 for field in ['tgfoid','tgtype','tgisinternal','tgenabled','tgparentid','tgattr','tgnargs','tgargs','tgqual','tgoldtable','tgnewtable','tgconstrrelid','tgconstrindid','tgdeferrable','tginitdeferred','tgconstraint']:
  check('Explicit existing-trigger field '+t['name']+'.'+field,'x.'+field in body)
 shape_review.append({'trigger':t,'nativeCreateAstSha256':sha(json.dumps(native_ast,sort_keys=True,separators=(',',':'))),'verdict':'EXACT_NATIVE_DDL_AST_AND_BOUNDED_REPLAY_GUARD_SOURCE_REVIEWED','limits':'Actual pg_trigger/pg_constraint values, first installation, exact reapplication and mismatched existing-trigger refusal need genuine PG17 native proof.'})
g=(HERE/M['guards']['incident']['path']).read_text(encoding='utf-8')
check('Incident predecessor guard SQL and PL grammar',len(json.loads(parser.parse_sql_json(g))['stmts'])==2 and len(json.loads(parser.parse_plpgsql_json(g)))==1)
contract_ref=M['guards']['incident']['contractCommit'];contract_path=M['guards']['incident']['contractPath'];contract=json.loads(frozen(contract_ref,contract_path))
rel=next(x for x in contract['relations']if x['schema']=='public'and x['name']=='weekly_discrepancy_incidents');constraint=next(x for x in rel['constraints']if x['name']=='weekly_discrepancy_incidents_resolution_kind_check')
check('Exact native named CHECK definition is required',constraint['definition']in g and constraint['validated']and not constraint['deferrable'])
native_path=M['guards']['incident']['nativeMigrationPath'];native_ref=M['guards']['incident']['nativeMigrationCommit'];native_ddl=frozen(native_ref,native_path)
old_values=re.search(r'resolution_kind text check \(resolution_kind is null or resolution_kind in \((.*?)\)\)',native_ddl,re.S)[1]
new_body=frozen(M['sourceCommits']['foundation'],'docs/healthroster-build/proposals/source-pay-decision-v5-foundation-20261015/one-time/06_native_source_contact_resolution.sqlinc')
old_values=re.findall(r"'([^']+)'",old_values);new_values=re.findall(r"'([^']+)'",new_body[new_body.index('check(resolution_kind'):])
check('Six exact prior values plus sole SOURCE_PAY_DECISION',new_values==old_values+['SOURCE_PAY_DECISION'])
migration=next(x for x in M['output']if x['path'].startswith('supabase/migrations/16102026_1105_'));migration_text=(HERE/'managed'/migration['path']).read_text(encoding='utf-8')
check('Table lock and predecessor guard precede complete unchanged native addition',migration_text.index(g)<migration_text.index(new_body)and'lock table public.weekly_discrepancy_incidents in access exclusive mode;'in g)
check('Guard neither silently drops CHECK nor accepts its new form',not re.search(r'\bdrop\s+constraint\b',g,re.I)and"'SOURCE_PAY_DECISION'"not in g and'into strict actual_definition'in g)
incident={'predecessorContract':{'commit':contract_ref,'path':contract_path,'sha256':sha(frozen(contract_ref,contract_path)),'constraint':constraint},'nativeMigration':{'commit':native_ref,'path':native_path,'sha256':sha(native_ddl),'oldValues':old_values},'newValues':new_values,'guardSha256':sha(g),'verdict':'EXACT_PREDECESSOR_AND_ONE_MEMBER_EXTENSION_SOURCE_REVIEWED','limits':'Actual catalog pretty definition, lock/rollback, historical row preservation and rejected unexpected predecessor require genuine PG17 native acceptance.'}
result={'version':'HEALTH_SOURCE_MANAGED_GUARD_FINITE_REVIEW_V1','qualification':'SOURCE_ONLY_PG17GRAMMAR_NOT_NATIVE_COMPILATION_REAPPLY_OR_PERMISSION_PROOF','parser':{'pglast':pglast.__version__,'postgresql':pglast.get_postgresql_version()},'manifestSha256':sha((HERE/'ASSEMBLY_MANIFEST.json').read_bytes()),'managedGrammar':parsed,'triggerReview':shape_review,'incidentReview':incident,'checks':checks,'passed':len(checks),'native':'NOT_RUN','databaseConnections':0}
(HERE/'GUARD_REVIEW.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8',newline='\n')
print(json.dumps({'passed':len(checks),'guardReviewSha256':sha((HERE/'GUARD_REVIEW.json').read_bytes()),'native':'NOT_RUN'},indent=2))

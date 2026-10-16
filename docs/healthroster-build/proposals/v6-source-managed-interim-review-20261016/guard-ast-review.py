"""Independent PG17 grammar/AST and exact replay-guard source review. No DB."""
import hashlib,json,pathlib,re,subprocess,sys
HERE=pathlib.Path(__file__).resolve().parent
REPO=HERE.parents[3]
sys.path.insert(0,r'C:/tmp/healthroster-v6-invoice-parser-20261016')
import pglast
from pglast import parser
COMMIT='9ac637ef3bdc50ad1b82b616283cb226a7bbb69b'
BASE='docs/healthroster-build/proposals/v6-source-managed-assembly-20261016/'
sha=lambda value:hashlib.sha256(value.encode() if isinstance(value,str) else value).hexdigest()
def git(ref,file):return subprocess.check_output(['git','show',ref+':'+file],cwd=REPO).decode('utf8').replace('\r\n','\n')
M=json.loads(git(COMMIT,BASE+'ASSEMBLY_MANIFEST.json'))
effect=git(COMMIT,BASE+M['guards']['triggers']['path'])
def clean(value):
 if isinstance(value,dict):return {key:clean(item) for key,item in value.items() if key not in ('location','stmt_location','stmt_len')}
 if isinstance(value,list):return [clean(item) for item in value]
 return value
def ast(text):return clean(json.loads(parser.parse_sql_json(text)))
rows=[]
for number,t in enumerate(M['triggerSpecs'],1):
 original=git(M['sourceCommits']['foundation'],t['origin']['path'])
 if t['origin']['derivation'].startswith('Exact11'):
  table_array=re.search(r'foreach name in array array\[([\s\S]*?)\] loop',original)[1]
  assert t['table'] in re.findall(r"'([^']+)'",table_array)
  template=re.search(r"execute format\('(create trigger [^']+)',name\)",original)[1]
  original_create=template.replace('%I',t['table'])+';'
 else:
  original_create=next(row['statement'] for row in M['suppressedNativeTriggerSetupStatements'] if row['statementSha256']==t['origin']['statementSha256'])
  assert original_create in original
 assert ast(original_create)==ast(t['createSql'])
 create=ast(original_create)['stmts'][0]['stmt']['CreateTrigStmt']
 mask=create.get('timing',0)|create['events']|1
 assert create['row'] and mask==t['type']
 deferred=create.get('deferrable',False)
 assert create.get('isconstraint',False)==deferred==create.get('initdeferred',False)==t['deferred']
 assert not any(create.get(key) for key in ('args','columns','whenClause','transitionRels','constrrel'))
 body=re.search(r'\$source_trigger_'+str(number).zfill(2)+r'\$(.*?)\$source_trigger_'+str(number).zfill(2)+r'\$;',effect,re.S)[1]
 lock='lock table '+t['qualifiedTable']+' in share row exclusive mode;'
 assert effect.index(lock)<effect.index('$source_trigger_'+str(number).zfill(2)+'$')
 required=["x.tgrelid='"+t['qualifiedTable']+"'::regclass", "x.tgname='"+t['name']+"'",
  "x.tgfoid='"+t['function']+"'::regprocedure",'x.tgtype='+str(mask),'not x.tgisinternal',"x.tgenabled='O'",'x.tgparentid=0',
  "x.tgattr=''::int2vector",'x.tgnargs=0',"x.tgargs=''::bytea",'x.tgqual is null','x.tgoldtable is null','x.tgnewtable is null',
  'x.tgconstrrelid=0','x.tgconstrindid=0','x.tgdeferrable='+str(deferred).lower(),'x.tginitdeferred='+str(deferred).lower()]
 for condition in required:assert condition in body,condition
 assert body.count(t['createSql'])==1
 assert re.search(r'^ if not exists\(',body,re.M) and re.search(r'^ elsif not exists\(',body,re.M)
 assert not re.search(r'\bor\b',body[body.index('elsif not exists'):body.index('then',body.index('elsif not exists'))],re.I)
 assert "raise exception 'SOURCE_HEALTH_TRIGGER_PREDECESSOR_MISMATCH' using errcode='55000'" in body
 if deferred:
  for condition in ['x.tgconstraint<>0','c.oid=x.tgconstraint',"c.conname='"+t['name']+"'","c.contype='t'",'c.conrelid=x.tgrelid','c.condeferrable','c.condeferred','c.convalidated']:assert condition in body
 else:assert 'x.tgconstraint=0' in body
 rows.append({'name':t['name'],'relation':t['qualifiedTable'],'function':t['function'],'mask':mask,'deferred':deferred,
  'originalCreateAstEqualsGenerated':True,'guardPredicatesConjunctive':True,'wholeOriginalCreateSha256':sha(original_create),
  'native':'NOT_RUN'})
assert len(rows)==15
assert not re.search(r'\b(drop\s+trigger|create\s+or\s+replace\s+trigger|enable\s+trigger)\b',effect,re.I)
assert len(json.loads(parser.parse_plpgsql_json(effect)))==15
guard=git(COMMIT,BASE+M['guards']['incident']['path'])
assert len(json.loads(parser.parse_sql_json(guard))['stmts'])==2
assert len(json.loads(parser.parse_plpgsql_json(guard)))==1
for condition in ["lock table public.weekly_discrepancy_incidents in access exclusive mode;",'into strict actual_definition',
 "n.nspname='public'","t.relname='weekly_discrepancy_incidents'","t.relkind='r'",'not a.attisdropped',"a.atttypid='pg_catalog.text'::regtype",
 "a.attname='resolution_kind'",'not a.attnotnull',"a.attgenerated=''","c.conname='weekly_discrepancy_incidents_resolution_kind_check'",
 "c.contype='c'",'c.convalidated','not c.condeferrable','not c.condeferred','c.conislocal','c.coninhcount=0','not c.connoinherit','c.conkey=array[a.attnum]::smallint[]',
 'actual_definition is distinct from', 'exception when no_data_found or too_many_rows']:
 assert condition in guard,condition
assert 'SOURCE_PAY_DECISION' not in guard
assert not re.search(r'\b(drop|alter)\s+(table|constraint)\b',guard,re.I)
result={'version':'SOURCE_GUARD_INDEPENDENT_INTERIM_AST_REVIEW_V1','qualification':'PG17_GRAMMAR_AND_SOURCE_NOT_NATIVE_CATALOGUE_REPLAY_OR_LOCK_PROOF',
 'proposalCommit':COMMIT,'parser':{'pglast':pglast.__version__,'postgresql':pglast.get_postgresql_version()},
 'triggers':rows,'incidentGuardSourcePredicatesChecked':True,'noDropReplaceEnable':True,'native':'NOT_RUN',
 'postgresSourceReferences':['https://raw.githubusercontent.com/postgres/postgres/REL_17_STABLE/src/backend/commands/trigger.c#L754-L929',
  'https://www.postgresql.org/docs/17/catalog-pg-trigger.html','https://www.postgresql.org/docs/17/catalog-pg-constraint.html']}
(HERE/'GUARD_AST_REVIEW.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf8',newline='\n')
print(json.dumps({'triggers':len(rows),'incidentGuard':'PASS_SOURCE','native':'NOT_RUN'}))

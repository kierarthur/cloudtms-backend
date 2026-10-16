"""Independent frozen-source/PG17 AST check. No database connection."""
import hashlib,json,pathlib,re,subprocess,sys
sys.dont_write_bytecode=True
sys.path.insert(0,r'C:/tmp/healthroster-v6-invoice-parser-20261016')
import pglast
from pglast import parser
root=pathlib.Path(r'C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees')
physical=root/'healthroster-checklist-native-14102026'
i2=root/'healthroster-install2-14102026'
here=pathlib.Path('\\\\?\\'+str(pathlib.Path(__file__).resolve().parent))
pin='bdb86a015c343b685f94682e5ae644f63fab7468'
folder='docs/healthroster-build/proposals/contact-lock-grouping-successor-1205-20261016'
def sha(b):return hashlib.sha256(b).hexdigest()
def blob(p):return subprocess.check_output(['git','show',pin+':'+folder+'/'+p],cwd=physical)
def disk(base,p):return pathlib.Path('\\\\?\\'+str(base/p)).read_bytes().replace(b'\r\n',b'\n')
def normalize(v):
 if isinstance(v,dict):return{k:normalize(x)for k,x in v.items()if not k.endswith('location')and k!='stmt_len'}
 if isinstance(v,list):return[normalize(x)for x in v]
 return v
def parse(text):return json.loads(parser.parse_sql_json(re.sub(r'^\\(?:set|ir)\b.*$','',text,flags=re.M)))['stmts']
p=json.loads(blob('PROPOSAL.json'));assert sha(blob('PROPOSAL.json'))=='06da6de8e47842bdf3b52c855ebc11526431fc07812a22d5351c7a142d465756'
expected=[[("weekly_manager_recipient_generations",8),("weekly_message_dispatch_commands",8)],[("weekly_candidate_cohorts",5),("weekly_timesheet_submission_requests",8),("weekly_candidate_outreach_generations",5),("weekly_timesheet_submission_request_memberships",5)]]
rows=[]
for number,f in enumerate(p['files']):
 before=blob('inputs/before/'+f['file']);after=blob('forward/'+f['file'])
 assert sha(before)==f['beforeSha256'];assert sha(after)==f['afterSha256']
 assert disk(physical,folder+'/forward/'+f['file'])==after
 restored=after.decode()
 for e in reversed(f['edits']):
  assert restored.count(e['to'])==1;restored=restored.replace(e['to'],e['from'])
 assert restored.encode()==before
 old_ast=parse(before.decode());new_ast=parse(after.decode())
 root_saved=disk(i2,f['file']);assert root_saved in(before,after)
 row={'file':f['file'],'beforeSha256':sha(before),'afterSha256':sha(after),'wholeSourceReverseEqual':True,'committedDiskEqual':True,'rootSavedStillPreimage':root_saved==before,'rootSavedEqualsAfter':root_saved==after}
 if number<2:
  locks=[s['stmt']['LockStmt']for s in new_ast if 'LockStmt'in s['stmt']]
  assert len(locks)==len(expected[number])
  assert [list(s['stmt'])[0]for s in new_ast[:1+len(locks)]]==['TransactionStmt']+['LockStmt']*len(locks)
  for lock,(name,mode)in zip(locks,expected[number]):
   assert lock['mode']==mode and lock['nowait']is True
   assert len(lock['relations'])==1
   relation=lock['relations'][0]['RangeVar'];assert relation['schemaname']=='public'and relation['relname']==name
  assert normalize([s for s in new_ast if 'LockStmt'not in s['stmt']])==normalize(old_ast)
  row.update({'newLocks':locks,'allBeforeOriginalChecks':True,'everyOriginalStatementAstEqual':True})
 else:
  assert len(old_ast)==len(new_ast)==2
  from_text="facts->'pairs'||p_extra_pairs";to_text="(facts->'pairs')||p_extra_pairs"
  assert before.decode().count(from_text)==1 and after.decode().count(to_text)==1
  assert after.decode().replace(to_text,from_text)==before.decode()
  query=lambda s:re.search(r'for pair in (select[\s\S]*?order by 1,2) loop',s).group(1)
  assert normalize(parse(query(before.decode())))==normalize(parse(query(after.decode())))
  before_fn=blob('inputs/GROUPING_FUNCTION_BEFORE.sql');after_fn=blob('inputs/GROUPING_FUNCTION_AFTER.sql')
  assert after_fn.decode().replace(to_text,from_text)==before_fn.decode()
  assert sha(before_fn)==f['function']['beforeSha256']and sha(after_fn)==f['function']['afterSha256']
  parser.parse_plpgsql_json(before_fn.decode());parser.parse_plpgsql_json(after_fn.decode())
  row.update({'completeForSelectAstEqual':True,'oneOccurrenceOnly':True,'completeFunctionReverseEqual':True,'bothCompletePlpgsqlBodiesOfflineGrammar':'PASS'})
 rows.append(row)
owners=[]
for owner in p['ownerEvidence']:
 data=blob(owner['preserved']);assert sha(data)==owner['completeStatementSha256']
 assert sha(disk(i2,owner['file']))==owner['sourceSha256']
 owners.append({'signature':owner['signature'],'completeStatementSha256':sha(data),'currentRootCompleteSourcePinEqual':True})
out={'version':'CONTACT_LOCK_AND_GROUPING_INDEPENDENT_PEER_V1','physicalCommit':pin,'sourceAssemblyCommit':p['sourceAssemblyCommit'],'proposalSha256':sha(blob('PROPOSAL.json')),'postgres':parser.get_postgresql_version(),'library':pglast.__version__,'rows':rows,'completeNativeOwners':owners,'totalNewLocks':6,'newAclGrantOrFunctionAbi':False,'qualification':'PASS_BOUNDED_SOURCE_LOCK_GROUPING_SUCCESSOR_NATIVE_NOT_RUN','outerIsolation':'READ_COMMITTED_OR_NATIVE_OUTER_SNAPSHOT_CURRENTNESS_MUST_BE_QUALIFIED','native':'NOT_RUN','databaseCalls':0,'I2Edits':0,'PartB':'NOT_RUN'}
(here/'EVIDENCE.json').write_text(json.dumps(out,indent=2)+'\n',encoding='utf8',newline='\n')
print(json.dumps({'files':len(rows),'locks':6,'owners':len(owners),'allOriginalAstEqual':True,'groupingAstEqual':True,'native':'NOT_RUN'}))

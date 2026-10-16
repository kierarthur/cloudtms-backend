from pathlib import Path
import unittest,json,hashlib,subprocess,sys,re
sys.path.insert(0,'C:/tmp/healthroster-v6-physical-parser')
from pglast import parse_sql
HERE=Path(__file__).resolve().parents[1];REPO=HERE.parents[3]
sha=lambda b:hashlib.sha256(b).hexdigest()
read=lambda p:(HERE/p).read_bytes()
j=lambda p:json.loads(read(p))
def blob(commit,path):return subprocess.check_output(['git','show',commit+':'+path],cwd=REPO)
def statements(raw):
 text=raw.decode('utf-8');result=[]
 for unit in parse_sql(text):
  a=unit.stmt_location;z=a+unit.stmt_len if unit.stmt_len else len(text)
  if text[z:z+1]==';':z+=1
  result.append(text[a:z].encode('utf-8'))
 return result
def code(raw):
 return raw[re.search(rb'create\s+or\s+replace\s+function\s+',raw,re.I).start():].strip().replace(b'\r\n',b'\n')
class Closure(unittest.TestCase):
 def test_01_exact_unique_census(self):
  m=j('JOINED_SQL_MAP.json');self.assertEqual(len(m['functions']),55);self.assertEqual(len(m['supersededDefinitionsExcluded']),29)
  self.assertEqual(len(parse_sql(read('forward/CANDIDATE_FINAL_FUNCTIONS.sqlinc').decode())),55)
  self.assertEqual(len({u['signature'] for u in m['functions']}),55)
 def test_02_complete_git_spans_and_joined_functions_equal(self):
  m=j('JOINED_SQL_MAP.json');joined=statements(read('forward/CANDIDATE_FINAL_FUNCTIONS.sqlinc'));self.assertEqual(len(joined),len(m['functions']))
  for u,emitted in zip(m['functions'],joined):
   original=blob(u['commit'],u['path']);self.assertEqual(sha(original),u['fileSha256'])
   span=original[u['startByte']:u['endByteExclusive']];self.assertEqual(sha(span),u['spanSha256']);self.assertEqual(len(parse_sql(span.decode('utf-8'))),1)
   self.assertEqual(code(span),code(emitted),u['signature'])
 def test_03_final_workflow_once_and_body_witness_current(self):
  m=j('JOINED_SQL_MAP.json');w=[u for u in m['functions'] if u['signature'].startswith('public.candidate_workflow_transition_atomic_v1(')]
  self.assertEqual(len(w),1);self.assertTrue(w[0]['commit'].startswith('975376fd'))
  c=[u for u in m['functions'] if u['signature'].startswith('private.candidate_contract_move_context_v1(')][0];self.assertTrue(c['commit'].startswith('11afc9c6'))
  self.assertIn(b'private.candidate_contract_move_workflow_facts_v1(v.id',read('forward/CANDIDATE_FINAL_FUNCTIONS.sqlinc'))
 def test_04_only_reviewed_actual_routine_ddl_effects(self):
  s=parse_sql(read('forward/CANDIDATE_EFFECTS.sqlinc').decode());self.assertEqual([type(u.stmt).__name__ for u in s],['DropStmt','CreateTrigStmt'])
  self.assertIn(b'candidate_contract_move_summary_invalidate_v1',read('forward/CANDIDATE_EFFECTS.sqlinc'))
  self.assertFalse(any(type(u.stmt).__name__ in ['DoStmt','CallStmt','TransactionStmt','InsertStmt','UpdateStmt','DeleteStmt'] for u in s))
 def test_05_sql_definition_dependencies_have_required_order(self):
  names=[u['signature'].split('(')[0] for u in j('JOINED_SQL_MAP.json')['functions']]
  for earlier,later in [('private.candidate_contract_move_sources_v1','private.timesheet_contract_move_evidence_live_v1'),('private.candidate_contract_move_signature_facts_v1','private.candidate_contract_move_facts_v1'),('private.timesheet_contract_native_allocation_binding_v1','private.timesheet_contract_native_allocation_alias_v1'),('private.timesheet_contract_native_allocation_alias_v1','private.timesheet_contract_native_booking_bound_v1'),('private.timesheet_contract_native_booking_bound_v1','private.timesheet_contract_native_rotation_alias_v1'),('private.timesheet_contract_native_rotation_alias_v1','private.timesheet_contract_native_pending_v1'),('private.candidate_contract_move_workflow_facts_v1','private.candidate_contract_move_context_v1')]:self.assertLess(names.index(earlier),names.index(later))
 def test_06_exact_final_acl_and_private_owner_only(self):
  sql=read('forward/CANDIDATE_FINAL_ACL.sqlinc').decode()
  for u in j('JOINED_SQL_MAP.json')['functions']:
   sig=u['signature'];self.assertIn('revoke all on function '+sig+' from public,anon,authenticated,service_role;',sql)
   self.assertEqual('grant execute on function '+sig+' to service_role;' in sql,sig.startswith('public.'))
  self.assertNotIn('grant execute on function private.',sql)
 def test_07_worker_frozen_spans_and_one_complete_declaration_each(self):
  m=j('WORKER_MAP.json');self.assertEqual(len(m['functions']),10)
  for u in m['functions']:
   raw=blob(u['sourceCommit'],u['sourcePath']);self.assertEqual(sha(raw),u['sourceFileSha256']);span=raw[u['sourceStartByte']:u['sourceEndByteExclusive']]
   self.assertEqual(sha(span),u['sourceSpanSha256']);self.assertEqual(read(u['payload']),span.strip()+b'\n')
   self.assertEqual(len(re.findall(rb'^(?:async )?function \w+\(',read(u['payload']),re.M)),1)
  self.assertEqual(sha(read(m['sharedDecoder']['payload'])),'172e9f0e1c513751c9713c7581fa869c4055f6342195fcd8e523b0308d7ef6b3')
 def test_08_immutable_migration_and_predecessor_record(self):
  m=j('MOUNT_RECIPE.json');self.assertEqual(read('forward/NATIVE_PENDING_MIGRATION_BODY.sqlinc'),blob('975376fd97855248c76b0ccbe027eed9b50f25ff','docs/healthroster-build/proposals/candidate-native-pending-20261016/one-time/01_nullable_pending_schema.sqlinc'))
  self.assertTrue(m['prerequisites']['physicalInvoice']['recordCommit'].startswith('55437f5c'));self.assertEqual(m['verification']['databaseCalls'],0)
 def test_09_only_known_nonsemantic_lf_translation(self):
  changes=j('JOINED_SQL_MAP.json')['newlineCanonicalization'];self.assertEqual(len(changes),1);self.assertGreater(changes[0]['literalTokensChecked'],0)
  self.assertIn('03_captured_paper_source_lock',changes[0]['path']);self.assertNotIn(b'\r',read('forward/CANDIDATE_FINAL_FUNCTIONS.sqlinc'))
 def test_10_multibyte_qr_span_is_complete_not_character_sliced(self):
  u=[u for u in j('JOINED_SQL_MAP.json')['functions'] if u['signature']=='public.timesheet_qr_refuse_and_reset(uuid,uuid,text,uuid)'][0]
  raw=blob(u['commit'],u['path']);span=raw[u['startByte']:u['endByteExclusive']]
  self.assertIn('\u2705'.encode('utf-8'),span);self.assertTrue(span.rstrip().endswith(b'$function$;'));self.assertEqual(len(parse_sql(span.decode('utf-8'))),1)
 def test_11_baseline_covers_all_functions_plus_only_migration_trigger(self):
  sql=read('tests/BASELINE_FRESH.sql').decode()
  for u in j('JOINED_SQL_MAP.json')['functions']:self.assertIn("('"+u['signature']+"')",sql)
  self.assertIn("('private.timesheet_contract_native_pending_commit_guard_v1()')",sql)
  self.assertIn('timesheet_contract_native_pending_audit_lookup',sql);self.assertEqual(len(parse_sql(sql)),1)
if __name__=='__main__':
 result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Closure))
 (HERE/'tests/SOURCE_RESULT.json').write_text(json.dumps({'status':'PASS_SOURCE_COMPOSITION_ONLY' if result.wasSuccessful() else 'FAIL','cases':result.testsRun,'failures':len(result.failures),'errors':len(result.errors),'native':'NOT_RUN','databaseCalls':0},indent=2)+'\n',encoding='utf-8',newline='\n')
 sys.exit(0 if result.wasSuccessful() else 1)

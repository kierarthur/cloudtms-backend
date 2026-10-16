"""Read-only integrity tests for the finite review. Never admits a SQL gate."""
import hashlib,json,pathlib,sys,unittest
HERE=pathlib.Path(__file__).resolve().parent
ROOT=pathlib.Path('\\\\?\\'+str(pathlib.Path(sys.argv.pop(1)).resolve()))
R=json.loads((HERE/'AGGREGATE_REVIEW.json').read_text(encoding='utf-8'))
sha=lambda s:hashlib.sha256(s if isinstance(s,bytes) else s.encode()).hexdigest()
class FiniteReview(unittest.TestCase):
 def test_exact_count_and_no_admission(self):
  self.assertEqual((R['savedCensusCount'],len(R['rows']),R['aggregateOccurrenceCount'],R['distinctExpressionCount']),(69,74,356,146))
  self.assertEqual(R['qualification'],'SOURCE_REVIEW_ONLY_NOT_GATE_ADMISSION');self.assertEqual(R['native'],'NOT_RUN')
 def test_all_current_file_pins(self):
  files={r['file']:r for r in R['rows']};self.assertEqual(len(files),51)
  for file,r in files.items():
   b=(ROOT/file).read_bytes();self.assertEqual(sha(b),r['sourceBytesSha256'],file);self.assertEqual(sha(b.decode().replace('\r\n','\n')),r['sourceSha256'],file)
 def test_exact_statement_key_and_occurrences(self):
  for r in R['rows']:
   self.assertEqual(sha(r['statement']),r['statementSha256'])
   key=json.dumps([r['signature'],r['hazard'],r['statement']],separators=(',',':'),ensure_ascii=False)
   self.assertEqual(sha(key),r['keySha256'])
   for a in r['aggregates']:
    self.assertEqual(sha(a['input']),a['inputSha256']);self.assertEqual(r['statement'][a['offset']:a['end']].split('(',1)[1][:-1],a['input'])
 def test_complete_finite_expression_types(self):
  seen={}
  for r in R['rows']:
   for a in r['aggregates']:
    self.assertIn(a['inputType'],['text','numeric','integer','bigint','date','timestamp with time zone'])
    self.assertNotEqual(a['inputType'],'uuid');self.assertTrue(a['bindingProof']);seen[a['expressionIndex']]=a['expression']
  self.assertEqual(set(seen),set(range(1,147)))
 def test_native_schema_pins_and_missing_ref_rejected(self):
  self.assertEqual(sha((ROOT/R['schema']['path']).read_bytes()),R['schema']['sha256'])
  for r in R['rows']:
   for a in r['aggregates']:
    for ref in a['nativeColumnRefs']:self.assertIn(ref,R['schema']['columns'])
  self.assertNotIn('public.unrelated_alias.id',R['schema']['columns'])
  for ref,d in R['schema']['additiveNativeDdl'].items():self.assertEqual(sha((ROOT/d['path']).read_text(encoding='utf-8')),d['sha256'])
 def test_identity_boundaries_remain_explicit(self):
  self.assertEqual(R['findings'][0]['expressionIndex'],119);self.assertIn('ROOT_REVIEW_REQUIRED',R['findings'][0]['status'])
  for n in [52,53,60,78,109,126,132]:
   a=next(a for r in R['rows'] for a in r['aggregates'] if a['expressionIndex']==n)
   self.assertIn('SINGLETON',a['role']);self.assertIn('count',a['bindingProof'].lower())
 def test_changed_source_and_statement_are_not_equal(self):
  r=R['rows'][0];self.assertNotEqual(sha(r['statement']+' '),r['statementSha256'])
  self.assertNotEqual(sha(r['statement'].replace('min(', 'max(',1)),r['statementSha256'])
unittest.main()

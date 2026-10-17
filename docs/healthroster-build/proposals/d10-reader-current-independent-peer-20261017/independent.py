"""Independent complete-parent/metadata/header/dependency checks, no SQL execution."""
from pathlib import Path
import json,hashlib,re,subprocess,os,unittest,copy
A=Path('D:/Development/GitHub/.codex-worktrees/healthroster-checklist-native-14102026/.codex-tmp/d10-reader-current-source-20261017');R=Path('D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026')
j=lambda p:json.loads((A/p).read_bytes());sha=lambda b:hashlib.sha256(b).hexdigest();S=j('PAYLOAD_SELECTION.json');P=j('PARENTS.json');V=j('SOURCE_PROVENANCE.json');O=j('ASSEMBLY.json')
sources={r['target']:(A/r['source']).read_bytes() for r in S['targets']};parents={r['target']:r for r in P['rows']}
def git(*args):return subprocess.check_output(['git','-c','gc.auto=0','-c','maintenance.auto=false','-C',str(R),*args],env={**os.environ,'GIT_OPTIONAL_LOCKS':'0','GIT_NO_LAZY_FETCH':'1'})
head=git('rev-parse','HEAD').decode().strip()
def exact_parent(target,b):
 row=parents[target];captured=(A/row['capture']).read_bytes();assert b==captured and len(b)==row['bytes'] and sha(b)==row['sha256']
 assert b==(R/target).read_bytes()==git('show',':'+target)==git('show',head+':'+target)==git('show',row['commit']+':'+target)
class Peer(unittest.TestCase):
 def test_every_whole_forward_parent_is_actual_complete_capture_not_self_consistent_snippet(self):
  for row in j('FULL_PARENT_FORWARD.json')['rows']:
   b=row['before'].encode();exact_parent(row['target'],b);self.assertEqual(row['after'].encode(),sources[row['target']]);self.assertEqual(sha(b),row['beforeSha256'])
   for fake in [b[:158],b+b' ',b.replace(b'\n',b'\r\n')]:
    if fake!=b:
     with self.assertRaises(AssertionError):exact_parent(row['target'],fake)
 def test_existing_three_helpers_and_classifier_definition_are_whole_actual_owners(self):
  for row in j('SOURCE_OWNER_REVERSES.json')['rows']:
   b=(A/parents[row['currentPhysical']]['capture']).read_bytes();old=row['before'].encode();new=row['after'].encode();self.assertEqual(b.count(old),1);self.assertEqual(sources[row['successorPhysical']].count(new),1);self.assertEqual(sha(old),row['beforeSha256']);self.assertEqual(len(old),row['beforeBytes'])
   if '_acl_' not in row['signature']:self.assertEqual(b,old)
 def test_seven_exact_private_headers_no_public_or_service_grants(self):
  pages=[sources[O['ownerPages'][r['id']]].decode() for r in V['owners']];self.assertEqual(len(pages),7)
  for row,s in zip(V['owners'],pages):
   self.assertTrue(s.startswith('create or replace function '+row['name']+'('));self.assertIn('stable security definer' if 'issue_filter_keys' not in row['name'] else 'immutable security definer',s);self.assertIn("set search_path to 'pg_catalog','public','private','pg_temp'",s)
   for acl in row['ACL']:self.assertEqual(s.count(acl),1)
   self.assertNotRegex(s.lower(),r'\bgrant\b|\bexecute\s+\x27|\binsert\s+into\b|\bdelete\s+from\b|\bupdate\s+public\b')
 def test_all_seven_finite_helper_dependencies_precede_consumers(self):
  order=O['includeOrder'];self.assertEqual(len(order),8);positions={f:i for i,f in enumerate(order)};names={r['name']:O['ownerPages'][r['id']] for r in V['owners']}
  for name,file in names.items():
   s=sources[file].decode()
   for called,dependency in names.items():
    if called!=name and re.search(r'\b'+re.escape(called)+r'\s*\(',s):self.assertLess(positions[dependency],positions[file],(name,called))
 def test_old_registry_chains_and_lock_history_and_release_arrays_are_retained(self):
  old=json.loads((A/parents['docs/bpay-next/final-definer-closures.json']['capture']).read_bytes());new=json.loads(sources['docs/bpay-next/final-definer-closures.json']);self.assertEqual(len(new['chains']),len(old['chains'])+7)
  for a,b in zip(old['chains'],new['chains']):
   if a['signature']!='private._weekly_source_acl_private_helper_contract_v1()':self.assertEqual(a,b)
  for k,v in old.items():
   if k!='chains':self.assertEqual(v,new[k])
  for f,key in [('supabase/release/migration-lock.json','migrations'),('supabase/release/current-release.json','verificationFiles'),('supabase/release/current-release.json','newVerificationFiles')]:
   a=json.loads((A/parents[f]['capture']).read_bytes());b=json.loads(sources[f]);self.assertEqual(b[key][:-1],a[key]);self.assertEqual(len(b[key]),len(a[key])+1)
 def test_four_namewide_admissions_are_separate_from_repeatable_and_original_037_064_match_contract(self):
  adm=sources[S['admission']].decode();module=sources[S['module']].decode();self.assertNotIn('pg_proc',module);self.assertIn("namespace.nspname='private' and routine.proname=expected_name",adm);self.assertNotIn('prokind=',adm);self.assertNotIn('pronargs',adm);self.assertNotIn('to_regprocedure',adm)
  for n in O['newNames']:self.assertEqual(adm.count("'"+n+"'"),1)
  caller=[p for p in P['rows'] if '/037_public_' in p['target'] or '/064_public_' in p['target']];self.assertEqual(len(caller),2)
  for p in caller:
   s=(A/p['capture']).read_text();self.assertIn('private.weekly_source_check_only_query_groups_v1',s);self.assertIn('WEEKLY_SOURCE_CHECK_ONLY_QUERY_GROUPS_V1',s);self.assertIn('source_read_revision',s)
 def test_three_hunk_f23_inverse_restores_actual_whole_gate(self):
  f=j('F23_BRIDGE_FORWARD.json');s=sources[f['target']].decode();self.assertEqual(len(f['hunks']),3)
  for h in reversed(f['hunks']):self.assertEqual(s.count(h['after']),1);s=s.replace(h['after'],h['before'])
  exact_parent(f['target'],s.encode());self.assertEqual(sum(len(r['hazards']) for r in j('RAW_F23_CENSUS.json')),13)
if __name__=='__main__':
 result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Peer));assert git('rev-parse','HEAD').decode().strip()==head
 raise SystemExit(not result.wasSuccessful())

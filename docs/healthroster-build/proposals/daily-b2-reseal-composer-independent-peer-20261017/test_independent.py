"""Synthetic-only independent tests. No GitValues instance or actual materialization."""
from pathlib import Path
import hashlib,sys,unittest
AUTHOR=Path('D:/Development/GitHub/.codex-worktrees/healthroster-checklist-native-14102026/.codex-tmp/daily-b2-real-commit-reseal-source-20261017')
assert hashlib.sha256((AUTHOR/'MANIFEST.json').read_bytes()).hexdigest()=='48664620d82be753cd5bc48d6fdff323e4cd080fe72ffe7a37229395cbb7c77c'
sys.path.insert(0,str(AUTHOR))
import reseal as r
import test_reseal as original
from test_reseal import Fake
from git_values import GitValues

class Independent(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  original.TestReseal.setUpClass();cls.a=original.TestReseal.a
 def completed(self):
  t=original.TestReseal();v,c,_=t.scenario();v.remember('report','2'*40);v.remember('root','3'*40)
  proof=r.assert_resealed(self.a,c,v)
  return v,c,proof
 def test_all_fourteen_saved_metadata_substitutions_refuse(self):
  _,_,p=self.completed()
  for row in p['metadataRows']:
   with self.subTest(file=row['file']):
    v,c,_=self.completed();v.tree[row['owner'],row['file']]+=b'\n'
    with self.assertRaises(ValueError):r.assert_resealed(self.a,c,v)
 def test_all_fourteen_index_metadata_substitutions_refuse(self):
  _,_,p=self.completed()
  for row in p['metadataRows']:
   with self.subTest(file=row['file']):
    v,c,_=self.completed();v.indexBad[row['owner'],row['file']]=b'SYNTHETIC_INDEX_SUBSTITUTION'
    with self.assertRaises(ValueError):r.assert_resealed(self.a,c,v)
 def test_all_fourteen_head_metadata_substitutions_refuse(self):
  _,_,p=self.completed()
  for row in p['metadataRows']:
   with self.subTest(file=row['file']):
    v,c,_=self.completed();v.historical[row['owner'],v.head(row['owner']),row['file']]=b'SYNTHETIC_HEAD_SUBSTITUTION'
    with self.assertRaises(ValueError):r.assert_resealed(self.a,c,v)
 def test_all_fourteen_configured_commit_metadata_substitutions_refuse(self):
  _,_,p=self.completed()
  for row in p['metadataRows']:
   with self.subTest(file=row['file']):
    v,c,_=self.completed();v.historical[row['owner'],row['commit'],row['file']]=b'SYNTHETIC_CONFIGURED_OBJECT_SUBSTITUTION'
    with self.assertRaises(ValueError):r.assert_resealed(self.a,c,v)
 def test_functional_root_without_new_composer_cannot_start_recipe(self):
  v=Fake(self.a);file=self.a.composer['file'];v.historical['root','b'*40,file]=self.a.parents['root',file]
  with self.assertRaisesRegex(ValueError,'ACTUAL_COMPLETE_FUNCTIONAL_COMMIT_REQUIRED'):
   r.propose(self.a,'operator',dict(functionalReport='a'*40,functionalRoot='b'*40),v,acl=False)
 def test_actual_git_reader_commit_guard_rejects_noncommit_objects_and_bad_refs_without_git(self):
  reader=GitValues.__new__(GitValues)
  for kind in [b'blob\n',b'tree\n',b'tag\n',b'commit\r\n']:
   calls=[]
   def fake(owner,*args):
    calls.append(args);return kind
   reader._git=fake
   with self.assertRaisesRegex(ValueError,'EXISTING_COMMIT_OBJECT_REQUIRED'):reader.commit('root','a'*40,'b'*40)
   self.assertEqual(calls,[('cat-file','-t','a'*40)])
  for ref in ['HEAD','A'*40,None,'a'*39,'a'*41]:
   reader._git=lambda *args:self.fail('bad ref reached fake Git')
   with self.assertRaisesRegex(ValueError,'EXACT_REAL_COMMIT_REQUIRED'):reader.commit('root',ref,'b'*40)
 def test_actual_git_reader_preserves_ancestor_failure(self):
  reader=GitValues.__new__(GitValues);calls=[]
  def fake(owner,*args):
   calls.append(args)
   if args[0]=='cat-file':return b'commit\n'
   raise ValueError('SYNTHETIC_NOT_ANCESTOR')
  reader._git=fake
  with self.assertRaisesRegex(ValueError,'SYNTHETIC_NOT_ANCESTOR'):reader.commit('report','a'*40,'b'*40)
  self.assertEqual(calls,[('cat-file','-t','a'*40),('merge-base','--is-ancestor','a'*40,'b'*40)])

if __name__=='__main__':unittest.main()

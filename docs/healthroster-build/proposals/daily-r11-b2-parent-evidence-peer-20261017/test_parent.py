"""P2 regression: self-consistent record text is not complete-parent evidence."""
import copy,unittest
import audit
class CompleteParentTests(unittest.TestCase):
 def rows(self):
  p=audit.load(audit.OLD/'PARENTS.json')['rows'];e=audit.load(audit.OLD/'ENTRYPOINT_FORWARD.json')['targets'];r=audit.load(audit.OLD/'RECIPE_FORWARD.json')['files']
  for kind,items in [('entry',e),('recipe',r)]:
   for item in items:
    matches=[x for x in p if (x['file']==item['parentFile'] if kind=='entry' else x['captured']==item['parentReference'])]
    self.assertEqual(len(matches),1)
    yield item,(audit.OLD/matches[0]['captured']).read_bytes()
 def test_all_six_complete_parent_vectors(self):
  for original,actual in self.rows():
   row=copy.deepcopy(original);row['wholeInverse']['before']=actual.decode();row['beforeSha256']=audit.sha(actual);row['beforeBytes']=len(actual)
   self.assertEqual(audit.before_errors(row,actual),[])
 def test_self_consistent_snippet_is_not_whole_parent(self):
  for original,actual in self.rows():
   snippet=actual[:79];row=copy.deepcopy(original);row['wholeInverse']['before']=snippet.decode();row['beforeSha256']=audit.sha(snippet);row['beforeBytes']=len(snippet)
   self.assertEqual(audit.before_errors(row,actual),['WHOLE_BEFORE_NOT_COMPLETE_PARENT','BEFORE_HASH_NOT_ACTUAL_PARENT','BEFORE_SIZE_NOT_ACTUAL_PARENT'])
 def test_each_claim_is_checked_against_complete_bytes(self):
  for original,actual in self.rows():
   row=copy.deepcopy(original);row['wholeInverse']['before']=actual.decode();row['beforeSha256']=audit.sha(actual);row['beforeBytes']=len(actual)
   for name,change,error in [('text',lambda x:x['wholeInverse'].__setitem__('before',x['wholeInverse']['before']+'\n'),'WHOLE_BEFORE_NOT_COMPLETE_PARENT'),('hash',lambda x:x.__setitem__('beforeSha256','0'*64),'BEFORE_HASH_NOT_ACTUAL_PARENT'),('size',lambda x:x.__setitem__('beforeBytes',len(actual)+1),'BEFORE_SIZE_NOT_ACTUAL_PARENT')]:
    with self.subTest(file=original['file'],field=name):
     mutated=copy.deepcopy(row);change(mutated);self.assertIn(error,audit.before_errors(mutated,actual))
 def test_actual_old_publisher_failure_not_hidden_by_hash_consistency(self):
  rows=audit.audit(audit.OLD,actual_git=False);bad=[x for x in rows if x['errors']]
  self.assertEqual(len(bad),1);self.assertEqual(bad[0]['claimedBeforeBytes'],158);self.assertEqual(bad[0]['actualBeforeBytes'],22021)
if __name__=='__main__':unittest.main(verbosity=2)

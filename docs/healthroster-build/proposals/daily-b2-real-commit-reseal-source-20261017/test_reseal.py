"""Synthetic in-memory DAG tests. No real materialization, Git or file writes."""
import copy,unittest,json
from reseal import *
class Fake:
 def __init__(self,a,acl=False):
  self.tree=dict(a.parents);self.tree.update(expected_functional(a,acl));self.historical={};self.objects=set();self.heads={};self.indexBad={}
  for r in a.parentRows+a.extraParents:self.historical[r['owner'],r['commit'],r['file']]=a.parents[r['owner'],r['file']]
  self.remember('report','a'*40);self.remember('root','b'*40)
 def remember(self,owner,c):
  self.objects.add((owner,c));self.heads[owner]=c
  for (o,f),b in self.tree.items():
   if o==owner:self.historical[o,c,f]=b
 def head(self,o):return self.heads[o]
 def commit(self,o,c,h):must((o,c) in self.objects and self.heads[o]==h,'SYNTHETIC_EXISTING_ANCESTOR_REQUIRED')
 def blob(self,o,c,f):must((o,c,f) in self.historical,'SYNTHETIC_NO_SUCH_GIT_BLOB');return self.historical[o,c,f]
 def saved(self,o,f):return self.tree[o,f]
 def index(self,o,f):return self.indexBad.get((o,f),self.tree[o,f])
 def apply(self,result,c):
  for r in result['records']:self.tree[r['owner'],r['file']]=r['after']
  self.remember('root' if result['stage']=='root' else 'report',c)
class TestReseal(unittest.TestCase):
 @classmethod
 def setUpClass(cls):cls.a=Authority()
 def scenario(self,acl=False,through='root'):
  a=self.a;v=Fake(a,acl);commits=dict(functionalReport='a'*40,functionalRoot='b'*40);results=[]
  for stage,ref,nextkey in [('operator','c'*40,'operatorMetadata'),('coordinator','d'*40,'coordinatorMetadata'),('one','e'*40,'oneMetadata'),('bookkeeper','f'*40,'bookkeeperMetadata'),('root','1'*40,'rootMetadata')]:
   args={k:val for k,val in commits.items() if k in REQUIRED[stage]|{'functionalReport','functionalRoot'}}
   result=propose(a,stage,args,v,acl);results.append(result);v.apply(result,ref)
   if nextkey:commits[nextkey]=ref
   if stage==through:break
  return v,commits,results
 def test_complete_fourteen_metadata_owners_and_dag_whole_inverses(self):
  _,_,r=self.scenario();rows=[x for q in r for x in q['records']];self.assertEqual(len(rows),14);self.assertEqual(len({(x['owner'],x['file']) for x in rows}),14)
  for x in rows:self.assertEqual(x['wholeInverse']['before'].encode(),self.a.parents[x['owner'],x['file']]);self.assertEqual(x['wholeInverse']['after'].encode(),x['after'])
 def test_two_new_modules_and_all153_coordinator_pins(self):
  _,_,r=self.scenario();o=json.loads(r[0]['records'][0]['after']);self.assertEqual(len([x for x in o['files'] if x['file'] in ['current-read.mjs','current-ipc.mjs']]),2)
  v=json.loads(r[1]['records'][0]['after']);self.assertEqual(len(v),153);self.assertEqual(len({x['file'] for x in v}),153)
 def test_all12_runtime_five_real_remaps_seven_exact_old_records(self):
  _,_,r=self.scenario();old=json.loads(self.a.parents['report',BK+'PARENTS.json']);new=json.loads(r[3]['records'][0]['after']);count=0
  for x,y in zip(old['fixedRuntime'],new['fixedRuntime']):
   if x!=y:self.assertEqual(y['commit'],'a'*40);count+=1
  self.assertEqual(count,5);self.assertEqual(new['rows'],old['rows']);self.assertEqual(new['actualS0Prefix'],old['actualS0Prefix'])
 def test_holder_raw_unchanged(self):
  _,_,r=self.scenario();self.assertTrue(all(not x['changed'] for x in r[0]['records'][1:]))
 def test_optional_acl_exact_composition_propagates_all_hashes(self):
  _,_,r=self.scenario(True);v=json.loads(r[3]['records'][0]['after']);row=next(x for x in v['fixedRuntime'] if x['file']==self.a.acl['file']);self.assertEqual(row['sha256'],self.a.acl['combinedSha256']);self.assertTrue(all(q['aclComposed'] for q in r))
 def test_parent_current_wrong_body_is_not_accepted_as_historical(self):
  v=Fake(self.a);v.tree['report',OP+'worker.mjs']=b'foreign';v.remember('report','2'*40)
  with self.assertRaises(ValueError):propose(self.a,'operator',dict(functionalReport='a'*40,functionalRoot='b'*40),v)
 def test_index_only_drift_refused(self):
  v=Fake(self.a);v.indexBad['report',OP+'worker.mjs']=b'index drift'
  with self.assertRaises(ValueError):propose(self.a,'operator',dict(functionalReport='a'*40,functionalRoot='b'*40),v)
 def test_missing_unknown_future_noncommit_are_not_real_values(self):
  for c in ['HEAD',None,'A'*40,'9'*40]:
   with self.assertRaises(ValueError):propose(self.a,'operator',dict(functionalReport=c,functionalRoot='b'*40),Fake(self.a))
 def test_downstream_refuses_functional_only_commit_without_resealed_manifest(self):
  with self.assertRaises(ValueError):propose(self.a,'coordinator',dict(functionalReport='a'*40,functionalRoot='b'*40,operatorMetadata='a'*40),Fake(self.a))
 def test_missing_dependency_and_extra_argument_refused(self):
  for stage,args in [('bookkeeper',dict(functionalReport='a'*40,functionalRoot='b'*40)),('operator',dict(functionalReport='a'*40,functionalRoot='b'*40,extra='c'*40))]:
   with self.assertRaises(ValueError):propose(self.a,stage,args,Fake(self.a))
 def test_original_historical_mismatch_refused(self):
  v=Fake(self.a);r=self.a.parentRows[0];v.historical[r['owner'],r['commit'],r['file']]=b'changed original'
  with self.assertRaises(ValueError):propose(self.a,'operator',dict(functionalReport='a'*40,functionalRoot='b'*40),v)
 def test_unreviewed_metadata_change_refused(self):
  v=Fake(self.a);v.tree['report',C+'MANIFEST.json']=b'{}';v.remember('report','2'*40)
  with self.assertRaises(ValueError):propose(self.a,'operator',dict(functionalReport='a'*40,functionalRoot='b'*40),v)
 def test_acl_choice_must_match_actual_functional_commit(self):
  with self.assertRaises(ValueError):propose(self.a,'operator',dict(functionalReport='a'*40,functionalRoot='b'*40),Fake(self.a),True)
 def test_no_closed_stage_or_arbitrary_acl_coercion(self):
  for stage,acl in [('unknown',False),('operator','yes')]:
   with self.assertRaises(ValueError):propose(self.a,stage,dict(functionalReport='a'*40,functionalRoot='b'*40),Fake(self.a),acl)
 def test_current_head_move_refused(self):
  v=Fake(self.a);original=v.head;n=0
  def head(o):
   nonlocal n;n+=1
   return original(o) if n<=2 else '9'*40
  v.head=head
  with self.assertRaises(ValueError):propose(self.a,'operator',dict(functionalReport='a'*40,functionalRoot='b'*40),v)
 def test_final_admission_requires_all_fourteen_actual_metadata_commit_images(self):
  v,c,_=self.scenario();p=assert_resealed(self.a,c,v);self.assertEqual(len(p['metadataRows']),14);self.assertEqual(p['publicAdmissionConfig']['s1AdapterCommit'],c['rootMetadata']);self.assertEqual(p['publicAdmissionConfig']['operatorCommit'],v.head('report'))
 def test_final_admission_refuses_old_or_subset_manifest_even_if_committed(self):
  for file in [OP+'MANIFEST.json',P+'daily-s1-after-clock-adapter-20261017/MANIFEST.json',BK+'PARENTS.json']:
   v,c,_=self.scenario();owner='root' if file.startswith(P+'daily-s1-after-clock') else 'report';v.tree[owner,file]=self.a.parents[owner,file];v.remember(owner,'9'*40)
   with self.assertRaises(ValueError):assert_resealed(self.a,c,v)
 def test_final_config_cannot_use_earlier_functional_only_adapter_commit(self):
  v,c,_=self.scenario();c['rootMetadata']=c['functionalRoot']
  with self.assertRaises(ValueError):assert_resealed(self.a,c,v)
if __name__=='__main__':unittest.main()

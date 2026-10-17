"""Source-only in-memory historical guard counterexamples; no subprocess or live reader."""
import unittest,json,copy,hashlib,importlib.util,types
from pathlib import Path
H=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('original',H/'reference/b2/reseal.py');original=importlib.util.module_from_spec(spec);spec.loader.exec_module(original)
spec=importlib.util.spec_from_file_location('subject',H/'historical-baseline.py');subject=importlib.util.module_from_spec(spec);spec.loader.exec_module(subject)
fixture=json.loads((H/'HISTORICAL_SOURCE_FIXTURE.json').read_text())
class Reader:
 def __init__(self,v):self.values={(r['owner'],r['ref'],r['file']):r['text'].encode() for r in v['blobs']};self.heads=dict(root='1'*40,report='2'*40)
 def head(self,o):return self.heads[o]
 def commit(self,o,c,h):assert len(c)==40 and h==self.heads[o]
 def blob(self,o,c,f):return self.values[o,c,f]
def seeded():
 v=copy.deepcopy(fixture);a=types.SimpleNamespace(**{k:v[k] for k in ['parentRows','extraParents','edits','map','composer','acl','metadata']});a.parents={(r['owner'],r['file']):r['text'].encode() for r in v['parents']};a.functional={(r['owner'],r['file']):r['text'].encode() for r in v['functional']};return a,v['base'],Reader(v)
def run(a,b,r):return subject.historical_baseline(a,b,r,original.expected_functional,original.metadata,original.sha,original.must)
class Guards(unittest.TestCase):
 def test_full_original_history_19_bodies_14_metadata(self):
  a,b,r=seeded();v=run(a,b,r);self.assertEqual(v['originalParents'],55);self.assertEqual(len(v['metadataImages']),14);self.assertEqual(len(v['baselineRows']),62)
 def test_each_original_55_history_object(self):
  for row in fixture['parentRows']:
   with self.subTest(file=row['file']):
    a,b,r=seeded();r.values[row['owner'],row['commit'],row['file']]=b'CHANGED'
    with self.assertRaises((ValueError,KeyError)):run(a,b,r)
 def test_each_original_19_functional_history_owner(self):
  a,b,r=seeded()
  for o,file in original.expected_functional(a,False):
   with self.subTest(file=file):
    a,b,r=seeded();c=b['config']['metadataCommits']['functionalReport' if o=='report' else 'functionalRoot'];r.values[o,c,file]=b'CHANGED'
    with self.assertRaises((ValueError,KeyError)):run(a,b,r)
 def test_each_original_14_metadata_history_owner(self):
  a,b,r=seeded();v=run(a,b,r)
  for row in v['metadataImages']:
   with self.subTest(file=row['file']):
    a,b,r=seeded();o,file=row['owner'],row['file']
    keys=[key for key in r.values if key[0]==o and key[2]==file and r.values[key]==original.metadata(a,b['config']['metadataCommits'],r,False)[o,file]]
    for key in keys:r.values[key]=b'CHANGED'
    with self.assertRaises((ValueError,KeyError)):run(a,b,r)
 def test_complete_functional_reverse_and_baseline_counts(self):
  a,b,r=seeded();key=next(iter(a.functional));a.functional[key]=a.functional[key]+b' '
  with self.assertRaises(ValueError):run(a,b,r)
  a,b,r=seeded();a.parentRows.pop()
  with self.assertRaises(ValueError):run(a,b,r)
 def test_historical_accepted_baseline_not_current_alias_or_count(self):
  for owner in ['root','report']:
   a,b,r=seeded();row=next(x for x in b['rows'] if x['owner']==owner);r.values[owner,b[owner+'Commit'],row['file']]=b'CHANGED'
   with self.assertRaises(ValueError):run(a,b,r)
if __name__=='__main__':unittest.main()

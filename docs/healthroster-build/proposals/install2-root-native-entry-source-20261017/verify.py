"""Complete actual parent/inverse/additive source proof. Never executes entries."""
from pathlib import Path
import json,subprocess,hashlib,sys
H=Path(__file__).resolve().parent
def sha(b):return hashlib.sha256(b).hexdigest()
def g(root,*args):return subprocess.check_output(['git','-c','gc.auto=0','-c','maintenance.auto=false','-C',root,*args],stderr=subprocess.DEVNULL)
def verify():
 rows=json.loads((H/'PARENTS.json').read_bytes())['rows'];heads={};byfile={}
 for r in rows:
  b=(H/r['captured']).read_bytes();assert len(b)==r['bytes'] and sha(b)==r['sha256'];byfile[r['file']]=b
  if '--current' in sys.argv:
   root=r['root'];heads.setdefault(root,g(root,'rev-parse','HEAD').decode().strip())
   assert b==(Path(root)/r['file']).read_bytes()==g(root,'show',':'+r['file'])==g(root,'show',heads[root]+':'+r['file'])==g(root,'show',r['commit']+':'+r['file'])
 for r in json.loads((H/'FULL_PARENT_FORWARD.json').read_bytes())['targets']:
  before=byfile[r['parentFile']];after=(H/'payload'/r['file']).read_bytes()
  assert r['wholeInverse']['before'].encode()==before and r['wholeInverse']['after'].encode()==after
  for label,b in [('before',before),('after',after)]:assert r[label+'Bytes']==len(b) and r[label+'Sha256']==sha(b)
  assert after.replace(r['wholeInverse']['after'].encode(),r['wholeInverse']['before'].encode(),1)==before
 selection=json.loads((H/'SELECTION.json').read_bytes())['targets'];assert len(selection)==3
 for r in selection:
  b=(H/'payload'/r['file']).read_bytes();assert r['bytes']==len(b) and r['sha256']==sha(b)
 # Complete unchanged algorithms, not just matching names/hashes of new wrappers.
 old=byfile['scripts/healthroster-install1-native-coordinator.mjs'].decode();new=(H/'payload/scripts/healthroster-install2-native-coordinator.mjs').read_text()
 pairs=[('    const snapshot=async()=>{','    return await runFullRehearsalWithinOwnedTurn('),('     async freshAfter(){','     async publishManaged(')]
 for a,z in pairs:assert old[old.index(a):old.index(z)]==new[new.index(a):new.index(z)]
 assert "runFullRehearsalWithinOwnedTurn({installNumber:2,turn" in new
 assert "turn.runManagedRelease({npmCli:" in new and 'officePath:config.officePath' in new
 assert 'compilePlan:' not in new
 for root,h in heads.items():assert g(root,'rev-parse','HEAD').decode().strip()==h
 if (H/'MANIFEST.json').exists():
  listed=json.loads((H/'MANIFEST.json').read_bytes())['files'];actual={p.relative_to(H).as_posix() for p in H.rglob('*') if p.is_file() and p!=H/'MANIFEST.json' and '__pycache__' not in p.parts}
  assert actual=={r['file'] for r in listed}
  for r in listed:
   b=(H/r['file']).read_bytes();assert len(b)==r['bytes'] and sha(b)==r['sha256']
 return dict(status='INSTALL2_ADDITIVE_SOURCE_PARENT_PROOF_PASS',completeParents=len(rows),wholeInverses=2,additiveTargets=3,unchangedSnapshotAndFreshQualifier=True,actualCurrentGit='--current' in sys.argv,native='NOT_RUN')
if __name__=='__main__':print(json.dumps(verify()))

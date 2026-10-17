"""Read-only complete-parent + immutable-history evidence verification."""
import json,hashlib,subprocess,sys
from pathlib import Path
import audit
NEW_TEXT=audit.REPORT+'/.codex-tmp/daily-auth-r11-b2-complete-parent-evidence-forward-20261017'
NEW=audit.extended(NEW_TEXT)
PRIOR=audit.extended('D:/Development/GitHub/.codex-worktrees/healthroster-checklist-native-14102026/.codex-tmp/daily-r11-b2-independent-peer-20261017')
CHANGED={'ENTRYPOINT_FORWARD.json','RECIPE_FORWARD.json','prepare-entry.py','verify.mjs'}
def inventory(p,wanted):
 raw=(p/'MANIFEST.json').read_bytes();assert audit.sha(raw)==wanted,'FROZEN_MANIFEST_CHANGED'
 rows=json.loads(raw)['files'];keys=[r['file'] for r in rows];assert len(set(keys))==len(keys)
 actual={q.relative_to(p).as_posix() for q in p.rglob('*') if q.is_file() and '__pycache__' not in q.parts and q!=p/'MANIFEST.json'}
 assert set(keys)==actual,'COMPLETE_INVENTORY_REQUIRED'
 for r in rows:
  b=(p/r['file']).read_bytes();assert len(b)==r['bytes'] and audit.sha(b)==r['sha256']
 return {r['file']:r for r in rows}
def verify():
 old=inventory(audit.OLD,'2dfd4d6ffc8859456cd16aa102c125cd7e45035322724d743453f8871fc634e1')
 new=inventory(NEW,'6da7eb1e73db34abcf743a818ff30c0a25ad48f42d6c833ce83110a372dcd6f7')
 prior=inventory(PRIOR,'2c98d89ca8acf644bbeffae6df42f74b4a4f5e8fb3706748d8ea5c563ed954fe')
 assert not set(old)-set(new),'OLD_SOURCE_REMOVAL'
 changed={f for f in old if (audit.OLD/f).read_bytes()!=(NEW/f).read_bytes()};assert changed==CHANGED
 proof=audit.load(NEW/'PARENT_EVIDENCE_FORWARD.json');assert proof['heldParentManifestSha256']==audit.sha((audit.OLD/'MANIFEST.json').read_bytes()) and proof['runtimeChanges']==[]
 inv=proof['completeEvidenceFileInverses'];assert len(inv)==4 and {r['file'] for r in inv}==CHANGED
 for r in inv:
  before=(audit.OLD/r['file']).read_bytes();after=(NEW/r['file']).read_bytes()
  assert r['wholeInverse']['before'].encode()==before and r['wholeInverse']['after'].encode()==after
  assert r['beforeBytes']==len(before) and r['afterBytes']==len(after)
  assert r['beforeSha256']==audit.sha(before) and r['afterSha256']==audit.sha(after)
  assert after.replace(r['wholeInverse']['after'].encode(),r['wholeInverse']['before'].encode(),1)==before
 rows=audit.audit(NEW);assert not any(r['errors'] for r in rows)
 unchanged=audit.assert_runtime_unchanged(NEW)
 # Every recipe evidence addition is mechanically derivable from its actual complete PARENTS record.
 old_recipe=audit.load(audit.OLD/'RECIPE_FORWARD.json');expected=json.loads(json.dumps(old_recipe))
 parents=audit.load(NEW/'PARENTS.json')['rows']
 for r in expected['files']:
  p=next(p for p in parents if p['captured']==r['parentReference'])
  r['parentFile']=p['file'];r['beforeBytes']=p['bytes']
 assert audit.load(NEW/'RECIPE_FORWARD.json')==expected,'UNEXPECTED_RECIPE_EVIDENCE_CHANGE'
 assert (NEW/'history/ENTRYPOINT_FORWARD_HELD.json').read_bytes()==(audit.OLD/'ENTRYPOINT_FORWARD.json').read_bytes()
 assert (NEW/'history/R11_109_HELD_MANIFEST.json').read_bytes()==(audit.OLD/'MANIFEST.json').read_bytes()
 return dict(status='ACCEPT_BOUNDED_COMPLETE_PARENT_EVIDENCE_ONLY',authorFiles=len(new)+1,heldAuthorFiles=len(old)+1,priorPeerFiles=len(prior)+1,changedEvidenceFiles=sorted(changed),addedFiles=sorted(set(new)-set(old)),sixActualCompleteParentRows=rows,unchangedRuntimeImages=len(unchanged),oldP2='REPRODUCED_PRESERVED_SUPERSEDED_HOLD',auth='NOT_RUN',native='NOT_RUN',actualSourceAdmission='NOT_RUN')
if __name__=='__main__':print(json.dumps(verify()))

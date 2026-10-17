"""Independent complete-parent evidence audit. Public files/read-only real Git only."""
from pathlib import Path
import hashlib,json,subprocess,sys,re
OLD_TEXT='D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/.codex-tmp/daily-auth-r11-b2-source-recipe-20261017'
ROOT='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026'
REPORT='D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026'
P='docs/healthroster-build/proposals/'
PAIRS={
 P+'daily-genuine-root-s0-b2-r11-bookkeeper-20261017.mjs':P+'daily-genuine-root-s0-b2-bookkeeper-20261017.mjs',
 P+'daily-genuine-s0-b2-r11-tools-orchestration-20261017.js.txt':P+'daily-genuine-s0-b2-tools-orchestration-20261017.js.txt',
 P+'daily-b2-r11-s0-publish-stageplans-20261017.mjs':P+'daily-b2-s0-publish-stageplans-20261017.mjs',
 P+'daily-five-stage-b2-r11-source-plan-20261017/compose.mjs':P+'daily-five-stage-producer-hash-forward-20261017/compose.mjs'}
RECIPE={'recipe.mjs':'reference/R10_recipe.mjs','materialize.mjs':'reference/R10_materialize.mjs'}
def extended(s):return Path('\\\\?\\'+str(s).replace('/','\\'))
OLD=extended(OLD_TEXT)
def sha(b):return hashlib.sha256(b).hexdigest()
def load(p):return json.loads(p.read_bytes())
def git(root,*args):return subprocess.check_output(['git','-c','gc.auto=0','-c','maintenance.auto=false','-C',root,*args],stderr=subprocess.DEVNULL)
def before_errors(row,actual):
 before=row['wholeInverse']['before'].encode();bad=[]
 if before!=actual:bad.append('WHOLE_BEFORE_NOT_COMPLETE_PARENT')
 if row['beforeSha256']!=sha(actual):bad.append('BEFORE_HASH_NOT_ACTUAL_PARENT')
 if 'beforeBytes' in row and row['beforeBytes']!=len(actual):bad.append('BEFORE_SIZE_NOT_ACTUAL_PARENT')
 return bad
def audit(packet,actual_git=True):
 parents=load(packet/'PARENTS.json')['rows'];entry=load(packet/'ENTRYPOINT_FORWARD.json');recipe=load(packet/'RECIPE_FORWARD.json')
 assert {r['file']:r['parentFile'] for r in entry['targets']}==PAIRS,'EXACT_FOUR_EXPECTED_PARENT_TARGET_PAIRS_REQUIRED'
 assert {r['file']:r['parentReference'] for r in recipe['files']}==RECIPE,'EXACT_TWO_PRIOR_RECIPE_PAIRS_REQUIRED'
 rows=[]
 for kind,items in [('entry',entry['targets']),('recipe',recipe['files'])]:
  for r in items:
   matches=[p for p in parents if (p['file']==r['parentFile'] if kind=='entry' else p['captured']==r['parentReference'])]
   assert len(matches)==1,'UNIQUE_COMPLETE_CAPTURED_PARENT_REQUIRED';p=matches[0]
   assert p['root']==ROOT and p['file'].startswith(P) and '..' not in p['file'].split('/')
   actual=(packet/p['captured']).read_bytes();assert len(actual)==p['bytes'] and sha(actual)==p['sha256']
   if actual_git:
    current=extended(p['root'])/p['file'];assert current.read_bytes()==actual
    for ref in ['','HEAD',p['commit']]:assert git(p['root'],'show',ref+':'+p['file'])==actual,'REAL_COMPLETE_PARENT_CHANGED'
   after=(packet/('payload/'+r['file'] if kind=='entry' else r['file'])).read_bytes()
   assert r['wholeInverse']['after'].encode()==after and r['afterSha256']==sha(after)
   if 'afterBytes' in r:assert r['afterBytes']==len(after)
   rows.append(dict(kind=kind,file=r['file'],parentFile=p['file'],parentCommit=p['commit'],
    claimedBeforeBytes=r.get('beforeBytes',len(r['wholeInverse']['before'].encode())),actualBeforeBytes=len(actual),
    claimedBeforeSha256=r['beforeSha256'],actualBeforeSha256=sha(actual),errors=before_errors(r,actual),afterSha256=sha(after)))
 return rows
def assert_runtime_unchanged(new):
 oldEntry=load(OLD/'ENTRYPOINT_FORWARD.json')
 paths=['recipe.mjs','materialize.mjs','auth-supersession.mjs','historical-baseline.py',
  *['payload/'+r['file'] for r in oldEntry['targets']],
  'payload/'+oldEntry['newComposerManifest']['file']]
 for file in paths:assert (new/file).read_bytes()==(OLD/file).read_bytes(),'EVIDENCE_ONLY_RUNTIME_BYTES_CHANGED: '+file
 old=load(OLD/'ENTRYPOINT_FORWARD.json');fixed=load(new/'ENTRYPOINT_FORWARD.json')
 expected=json.loads(json.dumps(old));r=expected['targets'][2]
 parent=next(p for p in load(OLD/'PARENTS.json')['rows'] if p['file']==r['parentFile'])
 before=(OLD/parent['captured']).read_bytes()
 r['beforeBytes']=len(before);r['beforeSha256']=sha(before);r['wholeInverse']['before']=before.decode()
 assert fixed==expected,'UNEXPECTED_ENTRYPOINT_FORWARD_CHANGE'
 return paths
def main():
 assert sys.argv[1:] in [['--old']] or len(sys.argv)==3 and sys.argv[1]=='--successor'
 if sys.argv[1]=='--old':
  rows=audit(OLD);bad=[r for r in rows if r['errors']]
  assert len(bad)==1 and bad[0]['file']==P+'daily-b2-r11-s0-publish-stageplans-20261017.mjs'
  print(json.dumps(dict(status='OLD_P2_REPRODUCED_HOLD',rows=rows,priorPeerSuperseded=True,native='NOT_RUN')))
 else:
  name=sys.argv[2].replace('\\','/');assert name.startswith(REPORT+'/.codex-tmp/daily-auth-r11-')
  new=extended(name);rows=audit(new);assert not any(r['errors'] for r in rows),'COMPLETE_PARENT_EVIDENCE_STILL_INVALID'
  checked=assert_runtime_unchanged(new)
  print(json.dumps(dict(status='COMPLETE_REAL_PARENT_AND_EVIDENCE_ONLY_FORWARD_PASS',rows=rows,unchangedRuntimeImages=len(checked),native='NOT_RUN')))
if __name__=='__main__':main()

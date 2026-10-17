from pathlib import Path
import hashlib,json,subprocess
HERE=Path('\\\\?\\'+str(Path(__file__).resolve().parent))
REPORT=Path('D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026')
ROOT=Path('D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026')
PIN='6b5ebedecf95c8a1adfda5141bbf3eccf5294d23'
P='docs/healthroster-build/proposals/'
OP=P+'daily-health-genuine-staged-operator-20261017/'
S0=P+'daily-health-s0-stage-coordinator-20261017/'
ONE=P+'daily-health-genuine-single-stage-coordinator-20261017/'
HOLDER=P+'daily-health-s0-catalogue-holder-20261017/'
BOOK=P+'daily-genuine-stage-root-bookkeeper-20261017/'
def git(root,*args):return subprocess.check_output(['git','-c','gc.auto=0','-c','maintenance.auto=false','-C',str(root),*args])
def sha(b):return hashlib.sha256(b).hexdigest()
def put(file,b):
 p=HERE/file;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(b)
def dump(file,obj):put(file,(json.dumps(obj,indent=2,ensure_ascii=False)+'\n').encode())
def main():
 assert git(REPORT,'rev-parse','HEAD').decode().strip()==PIN
 files=[OP+x for x in ['contract.mjs','native-read.mjs','worker.mjs','stages.mjs','network.mjs','source.mjs','MANIFEST.json','SOURCE_PINS.json','reference/audit/SOURCE_SCHEMA_EFFECT_BOUNDARIES.json','reference/audit/FINAL_SOURCE_REVIEW.json','reference/mapping/SOURCE_METADATA.json','reference/mapping/stage.mjs','reference/daily-health-genuine-fixture-recipe-20261017/RECIPE.json']]
 files +=[S0+x for x in ['phase-parent.mjs','coordinate.mjs','sequence.mjs','source.mjs','SOURCE_PINS.json','MANIFEST.json']]
 files +=[ONE+x for x in ['coordinate.mjs','stage.mjs','sequence.mjs','progress.mjs','source.mjs','SOURCE_PINS.json','MANIFEST.json']]
 files +=[HOLDER+x for x in ['source.mjs','EXTERNAL_SOURCE_PINS.json','ROOT_SOURCE_PINS.json','MANIFEST.json']]
 files +=[BOOK+x for x in ['core.mjs','bookkeeper.mjs','PARENTS.json','MANIFEST.json']]
 rows=[]
 for file in files:
  b=(REPORT/file).read_bytes();assert b==git(REPORT,'show',PIN+':'+file)==git(REPORT,'show',':'+file),file
  put('parents/report/'+file,b);put('payload/report/'+file,b)
  rows.append(dict(owner='report',file=file,commit=PIN,bytes=len(b),sha256=sha(b)))
 roothead=git(ROOT,'rev-parse','HEAD').decode().strip()
 for file in [P+'daily-genuine-root-stage-bookkeeper-20261017.mjs',P+'daily-genuine-stage-tools-orchestration-20261017.js.txt','scripts/healthroster-turn-control.mjs','scripts/healthroster-rehearsal-turn.mjs']:
  b=(ROOT/file).read_bytes();assert b==git(ROOT,'show',roothead+':'+file)==git(ROOT,'show',':'+file),file
  put('parents/root/'+file,b);put('payload/root/'+file,b)
  rows.append(dict(owner='root',file=file,commit=roothead,bytes=len(b),sha256=sha(b)))
 dump('PARENTS.json',dict(version='B2_SOURCE_ONLY_COMPLETE_PARENTS_V1',reportCommit=PIN,rootCommit=roothead,files=rows))
 print(json.dumps(dict(captured=len(rows),reportCommit=PIN,rootCommit=roothead)))
if __name__=='__main__':main()

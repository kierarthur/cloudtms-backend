from pathlib import Path
import json,sys,subprocess,hashlib
D=Path('\\\\?\\D:\\Development\\GitHub\\.codex-worktrees\\healthroster-v2-report-build-13102026\\.codex-tmp\\daily-auth-r11-b2-source-recipe-20261017');root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';report='D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026';rel='docs/healthroster-build/proposals/daily-b2-real-commit-reseal-source-20261017/';sys.path.insert(0,root+'/'+rel)
from reseal import Authority,expected_functional,metadata
from git_values import GitValues
# These operations capture only public source/historical Git images, never call admission or private paths.
a=Authority();base=json.loads((D/'B2_BASELINE.json').read_text());c=base['config']['metadataCommits'];f=expected_functional(a,False);requests=[]
for r in a.parentRows+a.extraParents:requests.append((r['owner'],r['commit'],r['file']))
for o,file in f:requests.append((o,c['functionalReport' if o=='report' else 'functionalRoot'],file))
for r in base['rows']:requests.append((r['owner'],base[r['owner']+'Commit'],r['file']))
for o,file in a.metadata:
 if o=='root':k='rootMetadata'
 elif '/daily-health-genuine-staged-operator-' in file or '/daily-health-s0-catalogue-holder-' in file:k='operatorMetadata'
 elif '/daily-health-s0-stage-coordinator-' in file:k='coordinatorMetadata'
 elif '/daily-health-genuine-single-stage-coordinator-' in file:k='oneMetadata'
 else:k='bookkeeperMetadata'
 requests.append((o,c[k],file))
blobs=[]
for o,ref,file in sorted(set(requests)):
 b=subprocess.check_output(['git','-c','gc.auto=0','-c','maintenance.auto=false','-C',root if o=='root' else report,'show',ref+':'+file],stderr=subprocess.DEVNULL);blobs.append(dict(owner=o,ref=ref,file=file,text=b.decode()))
def mapping(m):return[dict(owner=o,file=file,text=b.decode()) for (o,file),b in sorted(m.items())]
fixture=dict(parentRows=a.parentRows,extraParents=a.extraParents,parents=mapping(a.parents),functional=mapping(a.functional),selected=a.selected,edits=a.edits,map=a.map,composer=a.composer,acl=a.acl,metadata=a.metadata,blobs=blobs,base=base)
(D/'HISTORICAL_SOURCE_FIXTURE.json').write_text(json.dumps(fixture,indent=2)+'\n',encoding='utf8');print(json.dumps(dict(historicalSourceBlobs=len(blobs),originalParents=len(a.parentRows))))

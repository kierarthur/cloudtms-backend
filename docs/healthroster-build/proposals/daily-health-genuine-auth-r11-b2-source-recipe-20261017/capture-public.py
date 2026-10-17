from pathlib import Path
import json,hashlib,subprocess,sys
ROOT='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';REPORT='D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026';P='docs/healthroster-build/proposals/';B='docs/healthroster-build/v6-owner-delivery-20261016/';R=P+'daily-health-genuine-auth-r10-source-recipe-20261017/';D=Path('\\\\?\\'+REPORT.replace('/','\\'))/'.codex-tmp/daily-auth-r11-b2-source-recipe-20261017'
h=lambda b:hashlib.sha256(b).hexdigest()
def g(root,*a):return subprocess.check_output(['git','-c','gc.auto=0','-c','maintenance.auto=false','-C',root,*a],stderr=subprocess.DEVNULL)
heads={ROOT:g(ROOT,'rev-parse','HEAD').decode().strip(),REPORT:g(REPORT,'rev-parse','HEAD').decode().strip()};assert heads[REPORT]=='b7694b8b727084fb749c8b015285ae3c74e76f09';rows=[];seen=set()
def capture(root,file,save,commit=None):
 commit=commit or heads[root];b=(Path('\\\\?\\'+root.replace('/','\\'))/file).read_bytes();assert b==g(root,'show',commit+':'+file)==g(root,'show',heads[root]+':'+file)==g(root,'show',':'+file),file
 f=D/save;f.parent.mkdir(parents=True,exist_ok=True);f.write_bytes(b);key=(root,file)
 if key not in seen:rows.append(dict(root=root,file=file,commit=commit,bytes=len(b),sha256=h(b),captured=save));seen.add(key)
 return b
folders=dict(operator=P+'daily-health-genuine-staged-operator-20261017/',holder=P+'daily-health-s0-catalogue-holder-20261017/',coordinator=P+'daily-health-s0-stage-coordinator-20261017/',continuation=P+'daily-health-genuine-single-stage-coordinator-20261017/')
names=dict(operator=['contract.mjs','SOURCE_PINS.json','MANIFEST.json'],holder=['EXTERNAL_SOURCE_PINS.json','MANIFEST.json'],coordinator=['SOURCE_PINS.json','MANIFEST.json'],continuation=['SOURCE_PINS.json','MANIFEST.json'])
for phase in names:
 for name in names[phase]:capture(REPORT,folders[phase]+name,'reference/'+phase+'/'+name)
for name in ['recipe.mjs','materialize.mjs']:
 b=capture(ROOT,R+name,'reference/R10_'+name);out=b.decode().replace('R10','R11').replace('r10','r11').replace('R9_PUBLIC_RECEIPT.json','R10_PUBLIC_RECEIPT.json').replace('auth-r11-source-recipe-20261017','auth-r11-b2-source-recipe-20261017');(D/name).write_text(out,encoding='utf8',newline='')
receipt=json.loads((D/'reference/operator/SOURCE_PINS.json').read_text())['authObservation'];capture(ROOT,receipt['file'],'reference/R10_PUBLIC_RECEIPT.json',receipt['commit'])
config=capture(ROOT,B+'DAILY_GENUINE_B2_S0_ROOT_CONFIG_20261017.json','reference/B2_CONFIG.json');c=json.loads(config)
capture(ROOT,B+'DAILY_GENUINE_B2_S0_SOURCE_ADMISSION_20261017.json','reference/B2_SOURCE_ADMISSION.json',c['sourceReviewCommit']);capture(ROOT,B+'DAILY_AUTH_R10_SOURCE_RECIPE_ROOT_REVIEW_20261017.json','reference/R10_ROOT_REVIEW.json',c['authReviewCommit'])
recipe=P+'daily-b2-real-commit-reseal-source-20261017/'
for name in ['reseal.py','git_values.py','admit-public.mjs','CAPTURE.json','REVIEWED_POSTIMAGES.json','COMPOSER_FORWARD.json']:capture(ROOT,recipe+name,'reference/b2/'+name,c['recipeCommit'])
sys.path.insert(0,str(Path(ROOT)/recipe));from reseal import Authority,expected_functional
au=Authority();baseline=[];keys=set(au.parents)|set(expected_functional(au,False))|set(au.metadata)
for owner,file in sorted(keys):
 root=ROOT if owner=='root' else REPORT;b=(Path('\\\\?\\'+root.replace('/','\\'))/file).read_bytes()
 if (root,file) not in seen:capture(root,file,'reference/baseline/'+owner+'/'+file)
 baseline.append(dict(owner=owner,file=file,bytes=len(b),sha256=h(b)))
for n in ['daily-genuine-root-s0-b2-bookkeeper-20261017.mjs','daily-genuine-s0-b2-tools-orchestration-20261017.js.txt','daily-b2-s0-publish-stageplans-20261017.mjs']:capture(ROOT,P+n,'reference/entry/'+n)
com=P+'daily-five-stage-producer-hash-forward-20261017/';capture(ROOT,com+'compose.mjs','reference/entry/compose.mjs')
(D/'BASE_PARENT_SELECTION.json').write_text(json.dumps(dict(reportCommit=heads[REPORT],rootCommit=heads[ROOT],runtimeTargets=names),indent=2)+'\n')
(D/'PARENTS.json').write_text(json.dumps(dict(rows=rows),indent=2)+'\n')
(D/'B2_BASELINE.json').write_text(json.dumps(dict(version='ORIGINAL_ACCEPTED_B2_BASELINE_V1',config=c,rootCommit=heads[ROOT],reportCommit=heads[REPORT],sourceAdmissionSha256=h((D/'reference/B2_SOURCE_ADMISSION.json').read_bytes()),rows=baseline,originalParentRows=len(au.parentRows),functionalRows=len(expected_functional(au,False)),metadataRows=len(au.metadata)),indent=2)+'\n')
for root,head in heads.items():assert g(root,'rev-parse','HEAD').decode().strip()==head
print(json.dumps(dict(packet=str(D),capturedParents=len(rows),baselineRows=len(baseline),original55=len(au.parentRows))))

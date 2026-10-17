from pathlib import Path
import json,hashlib,subprocess
D=Path('\\\\?\\D:\\Development\\GitHub\\.codex-worktrees\\healthroster-v2-report-build-13102026\\.codex-tmp\\daily-auth-r11-b2-source-recipe-20261017');ROOT='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';P='docs/healthroster-build/proposals/';B='docs/healthroster-build/v6-owner-delivery-20261016/';h=lambda b:hashlib.sha256(b).hexdigest()
def g(*a):return subprocess.check_output(['git','-c','gc.auto=0','-c','maintenance.auto=false','-C',ROOT,*a],stderr=subprocess.DEVNULL)
head=g('rev-parse','HEAD').decode().strip();aliases=[]
for s in ['S1','S1a','S2','S3','S4']:
 file=B+'DAILY_GENUINE_'+s+'_EFFECT_ADMISSION_20261017.json';b=(Path('\\\\?\\'+ROOT.replace('/','\\'))/file).read_bytes();assert b==g('show',head+':'+file)==g('show',':'+file);f=D/'reference/aliases'/Path(file).name;f.parent.mkdir(parents=True,exist_ok=True);f.write_bytes(b);aliases.append(dict(file=file,commit=head,bytes=len(b),sha256=h(b),captured='reference/aliases/'+Path(file).name))
(D/'ALIAS_PARENTS.json').write_text(json.dumps(aliases,indent=2)+'\n')
oldNames=['daily-genuine-root-s0-b2-bookkeeper-20261017.mjs','daily-genuine-s0-b2-tools-orchestration-20261017.js.txt','daily-b2-s0-publish-stageplans-20261017.mjs'];newNames=[n.replace('s0-b2','s0-b2-r11').replace('s0-b2','s0-b2') for n in oldNames];newNames[2]='daily-b2-r11-s0-publish-stageplans-20261017.mjs'
forward=[]
def emit(file,parent,before,after):
 f=D/'payload'/file;f.parent.mkdir(parents=True,exist_ok=True);f.write_bytes(after.encode());forward.append(dict(file=file,parentFile=parent,beforeSha256=h(before.encode()),afterSha256=h(after.encode()),beforeBytes=len(before.encode()),afterBytes=len(after.encode()),wholeInverse=dict(before=before,after=after)))
for name,newName in zip(oldNames,newNames):
 parentSource=(D/'reference/entry'/name).read_bytes().decode();s=parentSource
 for a,b in zip(oldNames,newNames):s=s.replace(a,b)
 s=s.replace('DAILY_GENUINE_S0_B2_','DAILY_GENUINE_S0_B2_R11_').replace('DAILY_GENUINE_B2_S0_','DAILY_GENUINE_B2_R11_S0_').replace('DAILY_GENUINE_B2_SOURCE_ADMISSION_V1','DAILY_GENUINE_B2_R11_SOURCE_ADMISSION_V1')
 if name==oldNames[0]:s=s.replace('TEST_AUTH_REFRESH_R10_SUCCESS_','TEST_AUTH_REFRESH_R11_SUCCESS_').replace('CURRENT_R10_','CURRENT_R11_').replace('ACTUAL_R10_','ACTUAL_R11_').replace('original R10 auth binding remains unchanged','new R11 auth binding is explicitly superseded')
 if name==oldNames[2]:
  s=s.replace("const cfgVersion='DAILY_GENUINE_B2_R11_S0_CONFIG_V1'","const cfgVersion='DAILY_GENUINE_B2_R11_S0_CONFIG_V1'")
  s=s.replace("['version','operatorCommit','metadataCommits','entrypointCommit','recipeCommit','authReviewCommit',...preparation", "['version','operatorCommit','metadataCommits','entrypointCommit','recipeCommit','authReviewCommit','authSupersession',...preparation")
  s=s.replace("return c;\r\n}","return c;\r\n}")
  anchor="check(exact(c.metadataCommits,metadataKeys)"
  at=s.index(anchor);s=s[:at]+"check(exact(c.authSupersession,['recipeCommit','receiptCommit','receiptSha256','operatorCommit','holderCommit','coordinatorCommit','continuationCommit'])&&/^[a-f0-9]{64}$/.test(c.authSupersession.receiptSha256??'')&&Object.entries(c.authSupersession).filter(([k])=>k!=='receiptSha256').every(([,v])=>/^[a-f0-9]{40}$/.test(v??''))&&c.operatorCommit===c.authSupersession.continuationCommit,'REAL_R11_SUPERSESSION_REQUIRED');\n "+s[at:]
  s=s.replace("PROP+'"+newNames[2]+"'];", "PROP+'"+newNames[2]+"',PROP+'daily-five-stage-b2-r11-source-plan-20261017/compose.mjs',PROP+'daily-five-stage-b2-r11-source-plan-20261017/MANIFEST.json'];")
  s=s.replace("review.authReviewCommit===c.authReviewCommit&&review.metadataProof?.status==='ACTUAL_DERIVED_METADATA_IMAGES_PASS'", "review.authReviewCommit===c.authReviewCommit&&same(review.authSupersession,c.authSupersession)&&review.metadataProof?.status==='ACTUAL_B2_AUTH_SUPERSESSION_IMAGES_PASS'")
  s=s.replace('DAILY_AUTH_R10_SOURCE_RECIPE_ROOT_REVIEW_','DAILY_AUTH_R11_SOURCE_RECIPE_ROOT_REVIEW_')
  begin=s.index('export async function admitConfiguration(c){');end=s.index('// Filled with exact public predecision',begin)
  s=s[:begin]+'''export async function admitConfiguration(c){
 shape(c,true);entryImages(c);
 const relative=PROP+'daily-health-genuine-auth-r11-b2-source-recipe-20261017/',read=committedReader(),mf=read(relative+'MANIFEST.json',c.authSupersession.recipeCommit),manifest=JSON.parse(mf);
 for(const row of manifest.files){check(!row.file.split('/').includes('..'),'RELATIVE_R11_SOURCE_REQUIRED');const b=read(relative+row.file,c.authSupersession.recipeCommit);check(b.length===row.bytes&&hash(b)===row.sha256,'EXACT_R11_SOURCE_IMAGES_REQUIRED');}
 const base=JSON.parse(read(relative+'B2_BASELINE.json',c.authSupersession.recipeCommit));check(same(c.metadataCommits,base.config.metadataCommits)&&c.recipeCommit===base.config.recipeCommit,'ORIGINAL_B2_METADATA_COMMITS_REQUIRED');
 read(BASE+'DAILY_AUTH_R11_SOURCE_RECIPE_ROOT_REVIEW_20261017.json',c.authReviewCommit);
 const {assertSupersession}=await import(pathToFileURL(ROOT+'/'+relative+'auth-supersession.mjs').href),metadataProof=await assertSupersession(c.authSupersession),publicProof=metadataProof.publicProof;
 check(metadataProof.status==='ACTUAL_B2_AUTH_SUPERSESSION_IMAGES_PASS'&&metadataProof.metadataRows.length===14&&publicProof.reportHead===c.operatorCommit,'ACTUAL_R11_METADATA_AND_PUBLIC_SOURCE_REQUIRED');
 return{version:'DAILY_GENUINE_B2_R11_SOURCE_ADMISSION_V1',operatorCommit:c.operatorCommit,metadataCommits:c.metadataCommits,entrypointCommit:c.entrypointCommit,recipeCommit:c.recipeCommit,authReviewCommit:c.authReviewCommit,authSupersession:c.authSupersession,metadataProof,publicProof,native:'NOT_RUN',authRefresh:'NOT_RUN'};
}
'''+s[end:]
  s=s.replace("+'_B2_PREDECISION_","+'_B2_R11_PREDECISION_")
  s=s.replace("const composerRelative=P+'daily-five-stage-producer-hash-forward-20261017/'", "const composerRelative=P+'daily-five-stage-b2-r11-source-plan-20261017/'")
  s=s.replace("selected.config.metadataCommits.rootMetadata+':'+composerRelative", "selected.config.entrypointCommit+':'+composerRelative")
  # Original canonical stage effect filenames are a fixed core ABI. Only these five aliases have finite old-image admission.
  old="for(const r of records)must(!fs.existsSync(root+'/'+r.file),'HISTORY_COLLISION');for(const r of records)fs.writeFileSync(root+'/'+r.file,r.bytes,{flag:'wx'});"
  new="const aliasRows="+json.dumps(aliases,separators=(',',':'))+";for(const r of records){const a=aliasRows.find(a=>a.file===r.file);if(a){const b=fs.readFileSync(root+'/'+r.file);must(b.length===a.bytes&&sha256(b)===a.sha256&&[readHead,'',a.commit].every(ref=>b.equals(g('show',ref+':'+r.file))),'EXACT_PRIOR_STAGE_ALIAS_REQUIRED');}else must(!fs.existsSync(root+'/'+r.file),'HISTORY_COLLISION');}for(const r of records)fs.writeFileSync(root+'/'+r.file,r.bytes,aliasRows.some(a=>a.file===r.file)?{}:{flag:'wx'});"
  assert s.count(old)==1;s=s.replace(old,new).replace("function commit(records,msg){","const sha256=b=>createHash('sha256').update(b).digest('hex');\nfunction commit(records,msg){")
 emit(P+newName,P+name,parentSource,s)
old=(D/'reference/entry/compose.mjs').read_bytes().decode();new=old.replace("+'_B2_","+'_B2_R11_")
folder=P+'daily-five-stage-b2-r11-source-plan-20261017/';emit(folder+'compose.mjs',P+'daily-five-stage-producer-hash-forward-20261017/compose.mjs',old,new)
f=D/'payload'/folder/'MANIFEST.json';f.write_text(json.dumps(dict(version='FINITE_R11_NAMESPACE_COMPOSER_V1',files=[dict(file='compose.mjs',bytes=len(new.encode()),sha256=h(new.encode()))]),indent=2)+'\n')
(D/'ENTRYPOINT_FORWARD.json').write_text(json.dumps(dict(version='B2_R11_DISTINCT_ENTRY_AND_COMPOSER_FORWARD_V1',targets=forward,newComposerManifest=dict(file=folder+'MANIFEST.json',bytes=len(f.read_bytes()),sha256=h(f.read_bytes())),aliasParents=aliases,oldFilesRemainUntouched=True),indent=2)+'\n')
print(json.dumps(dict(newEntryTargets=len(forward)+1,aliases=len(aliases))))

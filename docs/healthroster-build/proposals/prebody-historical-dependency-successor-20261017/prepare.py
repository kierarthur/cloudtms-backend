"""Author-only exact historical source reconstructions, no SQL/native/I2 write."""
from pathlib import Path
import json,hashlib,subprocess,difflib
ROOT=Path('D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026')
HERE=Path('\\\\?\\'+str(Path(__file__).resolve().parent))
PARENT='51177b9abcdc9f089e680c9e7e9ef5285f09ff57'
PRE='scripts/healthroster-prebody-dynamic-review.mjs';SAME='scripts/healthroster-same-separate-type-review.mjs';READ='scripts/healthroster-source-read-type-review.mjs'
REVIEW='docs/healthroster-build/v6-owner-delivery-20261016/F23_PREBODY_DYNAMIC_REVIEW_20261016/CLASSIFICATION.json'
sha=lambda b:hashlib.sha256(b.encode()if isinstance(b,str)else b).hexdigest()
canon=lambda b:(b.decode()if isinstance(b,bytes)else b).replace('\r\n','\n')
def git(*a):return subprocess.check_output(['git','-c','gc.auto=0','-c','maintenance.auto=false','-c','core.longpaths=true','-C',str(ROOT),*a])
def put(f,b):
 assert not(HERE/'MANIFEST.json').exists(),'FROZEN_HISTORY'
 p=HERE/f;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(b.encode()if isinstance(b,str)else b)
def js(v):return json.dumps(v,ensure_ascii=False,separators=(',',':'))
def dump(f,v):put(f,json.dumps(v,indent=2,ensure_ascii=False)+'\n')
review=json.loads((ROOT/REVIEW).read_bytes());assert sha((ROOT/REVIEW).read_bytes())=='69d2b3aee092e78e36386642a8fbf65df406d5fc9f5d8e6a6d42bccf993895b0'
expected={
 'scripts/bpay-next-final-definers.mjs':'b6a8143f2bae077a4b72857419b4f8fab9f7652181f6ad3bf1fdcfbd4b77bafa',
 'scripts/healthroster-install1-full-rehearsal.mjs':'31acd390acf2ef6dbbd062fbb157feea66f9b5d328440414e67b2bfd4a3f78ab',
 'scripts/healthroster-live-test-restoration.mjs':'a91c1bf04f868ff372b20fb6beca239a7807fe9cc92c0c2682afffcd79554adc'}
rows=[];pins=[]
for file,current_sha in expected.items():
 dep=next(x for x in review['dependencies']if x['file']==file);old=(ROOT/dep['preservation']).read_bytes();current=(ROOT/file).read_bytes()
 assert current==git('show',PARENT+':'+file)==git('show','HEAD:'+file)==git('show',':'+file)
 assert sha(current)==current_sha and sha(canon(old))==dep['sha256']
 a=canon(old).splitlines(keepends=True);b=canon(current).splitlines(keepends=True);hunks=[]
 for group in difflib.SequenceMatcher(a=a,b=b,autojunk=False).get_grouped_opcodes(n=2):
  before=''.join(a[group[0][1]:group[-1][2]]);after=''.join(b[group[0][3]:group[-1][4]])
  assert before and after and canon(old).count(before)==1 and canon(current).count(after)==1
  hunks.append({'before':before,'after':after})
 back=canon(current)
 for h in reversed(hunks):assert back.count(h['after'])==1;back=back.replace(h['after'],h['before'],1)
 assert back==canon(old)
 put('reference/historical/'+file,old);put('reference/current/'+file,current)
 rows.append({'file':file,'priorSha256':dep['sha256'],'currentSha256':sha(canon(current)),'currentRawSha256':sha(current),'historicalRawSha256':sha(old),'historicalPreservation':dep['preservation'],'hunks':hunks,'wholeHistoricalSourceInverse':True})
 pins.append({'file':file,'bytes':len(current),'sha256':sha(current)})
original={f:(ROOT/f).read_bytes()for f in [PRE,SAME,READ]}
for f,b in original.items():
 assert b==git('show',PARENT+':'+f)==git('show','HEAD:'+f)==git('show',':'+f)
 put('reference/target/'+f,b);pins.append({'file':f,'bytes':len(b),'sha256':sha(b)})
assert sha(original[PRE])=='3504d0e858e1fdefe95edae2c86f14324944a96e1f27dc1a5caf097266898eef'
assert sha(original[SAME])=='897df1661b265921f960c107f5189470475866ad8311aec358b15f8841391259'
assert sha(original[READ])=='03504c269e29432de19dc82a93c62b221489b42ac6e62c2c95f6e6bedd149d2f'
proofs=[]
def changed(file,h):
 before=original[file].decode();assert before.count(h['before'])==1
 after=before.replace(h['before'],h['after'],1);assert after.count(h['after'])==1 and after.replace(h['after'],h['before'],1)==before
 put('payload/'+file,after);proofs.append({'file':file,'beforePath':'reference/target/'+file,'afterPath':'payload/'+file,'beforeSha256':sha(original[file]),'afterSha256':sha(after),'hunks':[h],'wholeRawInverse':True});return after
anchor=' const source=canonical(value);'
branch='\n // Exact approved stdout/snapshot successors have no prebody compiler effect.\n const historicalDependencies='+js([{k:v for k,v in r.items()if k in ['file','priorSha256','currentSha256','hunks']}for r in rows])+';\n const historical=historicalDependencies.find(row=>row.file===input.file&&row.priorSha256===input.sha256);\n if(historical&&digest(source)!==input.sha256){\n  if(digest(source)!==historical.currentSha256)fail(\'BOUND_SOURCE_CHANGED\');\n  let reversed=source;for(const h of [...historical.hunks].reverse()){if(reversed.split(h.after).length!==2)fail(\'BOUND_SOURCE_CHANGED\');reversed=reversed.replace(h.after,()=>h.before);}\n  if(digest(reversed)!==input.sha256)fail(\'BOUND_SOURCE_CHANGED\');return reversed;\n }'
pre_hunk={'before':anchor,'after':anchor+branch};new_pre=changed(PRE,pre_hunk)
anchor="    if(sha(source)!=='3504d0e858e1fdefe95edae2c86f14324944a96e1f27dc1a5caf097266898eef')fail('DEPENDENCY_CHANGED');"
same_bridge="    // Reconstruct only the exact current prebody successor before the retained older inverse.\n    if(sha(source)==="+js(sha(canon(new_pre)))+"){\n     const h="+js(pre_hunk)+";if(source.split(h.after).length!==2)fail('PREBODY_SUCCESSOR_REVERSE_ANCHOR');source=source.replace(h.after,()=>h.before);\n    }\n"
same_hunk={'before':anchor,'after':same_bridge+anchor};new_same=changed(SAME,same_hunk)
anchor="    for(const dependency of proof.dependencies)if(sha(raw(dependency.path))!==dependency.sha256)fail('PRIOR_HELPER_CHANGED');"
read_bridge="    for(const dependency of proof.dependencies){\n      let dependencySource=raw(dependency.path);\n      if(dependency.path==='scripts/healthroster-same-separate-type-review.mjs'&&dependency.sha256==='897df1661b265921f960c107f5189470475866ad8311aec358b15f8841391259'&&sha(dependencySource)!==dependency.sha256){\n        if(sha(dependencySource)!=="+js(sha(new_same))+")fail('PRIOR_HELPER_CHANGED');\n        const h="+js(same_hunk)+";const source=dependencySource.toString();if(source.split(h.after).length!==2)fail('PRIOR_HELPER_REVERSE_ANCHOR');dependencySource=source.replace(h.after,()=>h.before);\n      }\n      if(sha(dependencySource)!==dependency.sha256)fail('PRIOR_HELPER_CHANGED');\n    }"
new_read=changed(READ,{'before':anchor,'after':read_bridge})
for f in [REVIEW,'docs/healthroster-build/proposals/same-separate-f23-classification-20261017/CLASSIFICATION.json','docs/healthroster-build/proposals/source-current-read-peer-f23-20261017/CLASSIFICATION.json',review['priorReview']['path'],'docs/healthroster-build/v6-owner-delivery-20261016/SAME_SEPARATE_PREBODY_DEPENDENCY_FORWARD_20261017.json']:
 b=(ROOT/f).read_bytes();assert b==git('show',PARENT+':'+f)==git('show','HEAD:'+f)==git('show',':'+f);pins.append({'file':f,'bytes':len(b),'sha256':sha(b)})
dump('SOURCE_RECONSTRUCTIONS.json',{'parent':PARENT,'reviewPath':REVIEW,'reviewSha256':sha((ROOT/REVIEW).read_bytes()),'files':rows,'sourceOnly':True,'native0400':'NOT_RUN'})
dump('FULL_HELPER_REVERSES.json',{'parent':PARENT,'files':proofs,'wholeRawReversals':3,'native':'NOT_RUN'})
dump('PARENT_PINS.json',{'parent':PARENT,'files':pins,'sourceOnly':True,'semanticBankBodiesRead':0})
dump('PAYLOAD_SELECTION.json',{'parent':PARENT,'files':[{'target':x['file'],'payload':x['afterPath'],'parentSha256':x['beforeSha256'],'sha256':x['afterSha256']}for x in proofs],'runtimeSqlChanges':0,'native':'NOT_RUN'})
print(json.dumps({'targets':3,'historicalWholeSources':3,'literalHistoricalHunks':[len(x['hunks'])for x in rows],'immutableDependencies':len(review['dependencies']),'sourceOnly':True}))

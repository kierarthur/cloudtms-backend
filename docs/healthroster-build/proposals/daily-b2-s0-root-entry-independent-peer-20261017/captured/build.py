"""AUTHOR ONLY: create three inert source postimages, never invoke an entrypoint."""
from pathlib import Path
import json,hashlib
H=Path('\\\\?\\'+str(Path(__file__).resolve().parent));P='docs/healthroster-build/proposals/';B='docs/healthroster-build/v6-owner-delivery-20261016/'
sha=lambda b:hashlib.sha256(b).hexdigest()
parents=json.loads((H/'PARENTS.json').read_text())['rows'];rows={r['file']:r for r in parents}
def read(f):return (H/'parents'/f).read_text()
def one(s,a,b):
 assert s.count(a)==1,(a,s.count(a));return s.replace(a,b,1)
def put(f,b):
 p=H/f;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(b)
oldbk=P+'daily-genuine-root-s0-r9-bookkeeper-20261017.mjs';newbk=P+'daily-genuine-root-s0-b2-bookkeeper-20261017.mjs'
oldtools=P+'daily-genuine-s0-r9-tools-orchestration-20261017.js.txt';newtools=P+'daily-genuine-s0-b2-tools-orchestration-20261017.js.txt'
oldpub=P+'daily-r10-s0-r9-publish-stageplans-20261017.mjs';newpub=P+'daily-b2-s0-publish-stageplans-20261017.mjs'
s=read(oldbk)
s=one(s,"if(!/^[a-f0-9]{40}$/.test(process.env.HR_OPERATOR_COMMIT??'')||source!==process.env.HR_OPERATOR_COMMIT)","const {loadConfig}=await import(pathToFileURL(i+'/"+newpub+"').href);\nconst {config}=loadConfig();if(!/^[a-f0-9]{64}$/.test(process.env.HR_B2_CONFIG_SHA??''))throw Error('ACTUAL_CONFIG_HASH_REQUIRED');\nif(!/^[a-f0-9]{40}$/.test(process.env.HR_OPERATOR_COMMIT??'')||source!==config.operatorCommit||source!==process.env.HR_OPERATOR_COMMIT)")
s=one(s,"reviewCommit=process.env.HR_R10_REVIEW_COMMIT,reviewFile=p+'DAILY_AUTH_R10_SOURCE_RECIPE_ROOT_REVIEW_20261017.json'","reviewCommit=config.sourceReviewCommit,reviewFile=p+'DAILY_GENUINE_B2_S0_SOURCE_ADMISSION_20261017.json'")
s=s.replace('DAILY_GENUINE_S0_R9_','DAILY_GENUINE_S0_B2_')
before=rows[B+'DAILY_GENUINE_S0_EFFECT_ADMISSION_20261017.json'];after=rows[B+'DAILY_GENUINE_S0_AFTER_EFFECT_QUALIFICATION_20261017.json']
for old,new in [('63f33b767fbc75fefa623758178691443fbee11c5cc7c68028a075014a2eaf66',before['sha256']),('bc9f27ed652a00c3bc85a51cd1df7aadf6c35c05',before['commit']),('00323f57272f687183fc0a2c8b74bafabc7f569cdc2f39b3b17e421661ace9b4',after['sha256']),('2d5496c6bea7ce559ca1575e357b2147fd474090',after['commit']),('EXACT_R8_EFFECT_PARENT','EXACT_R9_EFFECT_PARENT'),('EXACT_R7_AFTER_EFFECT_PARENT','EXACT_R9_AFTER_EFFECT_PARENT')]:s=one(s,old,new)
s=one(s,"reviewRecord:p+'DAILY_AUTH_R10_SOURCE_RECIPE_ROOT_REVIEW_20261017.json'","reviewRecord:p+'DAILY_GENUINE_B2_S0_SOURCE_ADMISSION_20261017.json'")
s=s.replace('ACTUAL_R10_REVIEW_COMMIT_REQUIRED','ACTUAL_B2_REVIEW_COMMIT_REQUIRED').replace('Genuine public R10 source/review binding only','Genuine B2 source review; original R10 auth binding remains unchanged')
bk=s
s=read(oldtools).replace("load('dailyR10FinalK4')","load('dailyB2OperatorCommit')").replace("key='dailyS0R9Actual'","key='dailyS0B2Actual'").replace('daily-genuine-root-s0-r9-bookkeeper-20261017.mjs','daily-genuine-root-s0-b2-bookkeeper-20261017.mjs')
s=one(s,"$env:HR_R10_REVIEW_COMMIT='\"+load('dailyR10ReviewCommit')+\"'","$env:HR_B2_CONFIG_SHA='\"+state.sourceAdmission.configSha256+\"'")
s=one(s," try{\n  const prepared=", " try{\n  state.sourceAdmission=JSON.parse(await run(\"$env:HR_OPERATOR_COMMIT='\"+K4+\"'\\nnode docs/healthroster-build/proposals/daily-b2-s0-publish-stageplans-20261017.mjs --source-only\",root,3000));\n  if(state.sourceAdmission.status!=='B2_S0_PUBLIC_SOURCE_ADMITTED'||state.sourceAdmission.operatorCommit!==K4||!/^([a-f0-9]{64})$/.test(state.sourceAdmission.configSha256))throw Error('ACTUAL_B2_SOURCE_REQUIRED');store(key,state);\n  const prepared=")
tools=s
s=read(oldpub);s=s[s.index('\n')+1:];s=s.replace('process.env','env');s=s.replace('DAILY_GENUINE_S0_R9_','DAILY_GENUINE_S0_B2_')
s=one(s,"HR_ROOT_HELPER_COMMIT:'8971881e6347f4597a8547aabeb404fe578011c0'","HR_ROOT_HELPER_COMMIT:selected.config.metadataCommits.rootMetadata")
s=one(s,"g('show','267f4e2c5fe30b02cbe69ca143f17d50dd89e960:'+composerRelative+row.file)","g('show',selected.config.metadataCommits.rootMetadata+':'+composerRelative+row.file)")
s=s.replace('actual R10 S0','actual B2 S0').replace('genuine R10 S0','genuine B2 S0')
header=(H/'publisher-header.mjs').read_text();reviewRows=[r for r in parents if r['file'] not in [oldbk,oldtools,oldpub,B+'DAILY_GENUINE_S0_EFFECT_ADMISSION_20261017.json',B+'DAILY_GENUINE_S0_AFTER_EFFECT_QUALIFICATION_20261017.json']]
header=one(header,'__APPROVED_REVIEW_PARENTS__',json.dumps(reviewRows,separators=(',',':')))
pub=header+"\nexport async function publish(env=process.env){\nconst selected=loadConfig(env),{sourceReviewCommit,...preparation}=selected.config;await admitConfiguration(preparation);assertPreparedPublicInputs(selected);\n"+s+"\n}\nif(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url))try{\n if(process.argv.length===3&&process.argv[2]==='--source-only'){const selected=loadConfig(),{sourceReviewCommit,...preparation}=selected.config;await admitConfiguration(preparation);assertPreparedPublicInputs(selected);console.log(JSON.stringify({status:'B2_S0_PUBLIC_SOURCE_ADMITTED',operatorCommit:selected.config.operatorCommit,configSha256:selected.configSha256}));}\n else if(process.argv.length===2)await publish();else throw Error('B2_S0_FIXED_ENTRY_ARGUMENTS_REQUIRED');\n}catch(e){console.log(JSON.stringify({status:'HOLD',code:/^[A-Z0-9_]+$/.test(e?.message??'')?e.message:'B2_S0_ROOT_ENTRY_HELD'}));process.exitCode=1;}\n"
out=[]
for old,new,body in [(oldbk,newbk,bk),(oldtools,newtools,tools),(oldpub,newpub,pub)]:
 b=body.encode();put('payload/'+new,b);out.append(dict(file=new,bytes=len(b),sha256=sha(b),parentFile=old,parentCommit=rows[old]['commit'],parentSha256=rows[old]['sha256'],wholeInverse=dict(before=read(old),after=body),newTarget=True))
put('SELECTION.json',(json.dumps(dict(version='B2_S0_THREE_NEW_ENTRY_SOURCE_V1',targets=out),indent=2)+'\n').encode())
print(json.dumps(dict(targets=len(out),publisherBytes=len(pub.encode()))))

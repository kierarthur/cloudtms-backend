from pathlib import Path
import json,hashlib,subprocess
root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';relative='docs/healthroster-build/proposals/qt-audit-readonly-cost1000-20261017/'
p=Path('\\\\?\\D:\\Development\\GitHub\\.codex-worktrees\\healthroster-v2-report-build-13102026\\.codex-tmp\\qt-cost1000-success-abi-source-20261017');src=Path('\\\\?\\'+root.replace('/','\\'))/relative
h=lambda b:hashlib.sha256(b).hexdigest()
def g(*a):return subprocess.check_output(['git','-c','gc.auto=0','-c','maintenance.auto=false','-C',root,*a],stderr=subprocess.DEVNULL)
head=g('rev-parse','HEAD').decode().strip();historical='736dc59f1644c04e7315950d59bb4687b6b0d3e1';raw=(p/'MANIFEST.json').read_bytes();assert h(raw)=='3177911190336340676bb1fbd6756bc2b193850f8722ece225a9d9865c8e8344';mf=json.loads(raw);rows=mf['files']+[dict(file='MANIFEST.json',bytes=len(raw),sha256=h(raw))]
receipt='docs/healthroster-build/v6-owner-delivery-20261016/QT_COST1000_ACTUAL_SCAN_PROJECT_HEADER_HOLD_20261017.json';native=(Path('\\\\?\\'+root.replace('/','\\'))/receipt).read_bytes();observed=json.loads(native);success=observed['rawNativeResult']['structuredContent'];assert success==json.loads(observed['rawNativeResult']['content'][0]['text']) and 'fatalError' not in success and observed['status']=='ACTUAL_COST_HOLD';(p/'reference/ACTUAL_SCAN_PROJECT_HEADER_HOLD.json').write_bytes(native)
provenance=dict(root=root,file=receipt,observedRootHead=head,commit=None,provenance='SAVED_ROOT_ACTUAL_RECEIPT_NOT_YET_COMMITTED',bytes=len(native),sha256=h(native),originalDisposition=observed['status'],originalCode=observed['code'])
old={r['file']:(p/r['file']).read_bytes() for r in rows};edits=[]
def change(file,before,after):
 b=(p/file).read_bytes();assert b.count(before.encode())==1,(file,before);n=b.replace(before.encode(),after.encode());(p/file).write_bytes(n);edits.append(dict(file=file,before=before,after=after))
change('root-tools.mjs',"'rollbackVerification','warnings','fatalError'","'rollbackVerification','warnings'");change('root-tools.mjs',"&&r.fatalError===null",'')
change('validator.mjs',"&&result.fatalError===null",'');change('validator.mjs',"&&!result.fatalError",'');change('fixtures.mjs',',fatalError:null','')
newabi=dict(version='ACTUAL_ACCEPTED_SUCCESS_NATIVE_HEADER_V1',**provenance,topKeys=list(success),freshKeys=list(success['rollbackVerification']),statementKeys=list(success['statements'][0]),warnings=success['warnings'],scope='Actual SCAN_PROJECT returned accepted/rolledBack/fresh true without fatalError; original actual controller HOLD is not requalified. Exact present fatalError or any unknown top key is refused.')
(p/'reference/NATIVE_ABI_REFERENCE.json').write_text(json.dumps(newabi,indent=2)+'\n',encoding='utf8')
change('root-tools.test.mjs',"let source=fs.readFileSync(new URL('./root-tools.mjs',import.meta.url),'utf8');for(const e", "let source=fs.readFileSync(new URL('./reference/success-before/root-tools.mjs',import.meta.url),'utf8');for(const e")
change('stream.test.mjs',"ioExtra=false}={})", "ioExtra=false,legacy=false}={})");change('stream.test.mjs',"return{structuredContent:f.result};", "return{structuredContent:legacy?{...f.result,fatalError:null}:f.result};");change('stream.test.mjs',"const old=fixture({tail})", "const old=fixture({tail,legacy:true})")
change('stream.test.mjs',"let text=fs.readFileSync(new URL('./root-tools.mjs',import.meta.url),'utf8');", "let text=fs.readFileSync(new URL('./reference/success-before/root-tools.mjs',import.meta.url),'utf8');")
change('stream.test.mjs',"const b=fs.readFileSync(new URL('./'+r.file,import.meta.url));", "const forward=JSON.parse(fs.readFileSync(new URL('./SUCCESS_ABI_FORWARD.json',import.meta.url))),changed=forward.files.some(x=>x.file===r.file),b=fs.readFileSync(new URL('./'+(changed?'reference/success-before/':'')+r.file,import.meta.url));")
changed=[]
for file,before in old.items():
 if file=='MANIFEST.json':continue
 after=(p/file).read_bytes()
 if after!=before:
  f=p/'reference/success-before'/file;f.parent.mkdir(parents=True,exist_ok=True);f.write_bytes(before);changed.append(dict(file=file,beforeBytes=len(before),beforeSha256=h(before),afterBytes=len(after),afterSha256=h(after),wholeInverse=dict(before=before.decode(),after=after.decode())))
proof=dict(version='QT_COST1000_ACTUAL_SUCCESS_ABI_FORWARD_V1',root=root,actualRootParent=head,historicalParent=historical,parentManifestSha256=h(raw),sourceProvenance=provenance,runtimeFiles=['root-tools.mjs','validator.mjs'],files=changed,literalEdits=edits,originalFileCount=len(rows),unchangedRows=[r for r in rows if r['file'] not in [x['file'] for x in changed] and r['file']!='MANIFEST.json'],limitations=['Prior actual header/controller HOLD unchanged; offline replay is not a new capture','No SQL/source/holder/budget/closed frame/identity/clock/consumption change','No native operation or actual source admission'])
(p/'SUCCESS_ABI_FORWARD.json').write_text(json.dumps(proof,indent=2)+'\n',encoding='utf8');assert g('rev-parse','HEAD').decode().strip()==head
print(json.dumps(dict(changedFiles=len(changed),literalEdits=len(edits),provenance=provenance)))

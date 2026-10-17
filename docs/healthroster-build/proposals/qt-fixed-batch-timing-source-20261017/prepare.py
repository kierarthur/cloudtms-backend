from pathlib import Path
import json,hashlib,subprocess
H=Path(__file__).resolve().parent;R=Path('D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026');D='docs/healthroster-build/proposals/contact-qt-positive-native-proposal-20261017';F='5cf0a18f161327d9513c6b107761aa437997c10b'
sha=lambda b:hashlib.sha256(b).hexdigest();git=lambda *a:subprocess.check_output(['git','-C',str(R),*a]);head=git('rev-parse','HEAD').decode().strip();git('merge-base','--is-ancestor',F,head)
names=['fixed-metadata-reader.mjs','MANIFEST.json','batch-source.mjs','component-source.mjs','source.mjs','input-builder.mjs','control.mjs','hold-case.mjs','metadata-diagnostic.mjs','metadata-progress-diagnostic.mjs','SOURCE_PINS.json']
rows=[]
for n in names:
 b=(R/D/n).read_bytes()
 for ref in [':',head+':',F+':']:assert b==git('show',ref+D+'/'+n)
 (H/'parent'/n).write_bytes(b);rows.append(dict(file=D+'/'+n,captured='parent/'+n,bytes=len(b),sha256=sha(b),historicalCommit=F))
shared=['scripts/healthroster-install1-full-rehearsal.mjs','scripts/healthroster-live-test-restoration.mjs','scripts/healthroster-rehearsal-turn.mjs','scripts/healthroster-turn-control.mjs','scripts/cloudtms-managed-release-sql.mjs','supabase/release/export_contract.sql']
(H/'parent/shared').mkdir(exist_ok=True)
for file in shared:
 b=(R/file).read_bytes()
 for ref in [':',head+':',F+':']:assert b==git('show',ref+file)
 captured='parent/shared/'+Path(file).name;(H/captured).write_bytes(b);rows.append(dict(file=file,captured=captured,bytes=len(b),sha256=sha(b),historicalCommit=F))
text=(H/'parent/fixed-metadata-reader.mjs').read_bytes().decode()
edits=[
 ("import {compileNativeBatch,joinNativeSnapshotFrames} from './batch-source.mjs';", "import {compileNativeBatch,joinNativeSnapshotFrames} from './batch-source.mjs';\nimport {BatchTimingCapture,compileBatchTimingInput,batchTimingProgressFor} from './batch-timing-diagnostic.mjs';\nexport {batchTimingProgressFor};"),
 ("async function runFixedChild({input,env,turn,phase,diagnosticMode=null,batch=false}){", "async function runFixedChild({input,env,turn,phase,diagnosticMode=null,batch=false,batchTiming=false}){\n check(!batchTiming||(phase==='BEFORE'&&batch&&diagnosticMode===null),'LOCAL_METADATA_BATCH_TIMING_MODE_REFUSED');"),
 ("const capture=diagnosticMode===null?new BoundedMemoryCapture(phase,batch):new DiagnosticMemoryCapture(diagnosticMode);", "const capture=batchTiming?new BatchTimingCapture(new BoundedMemoryCapture(phase,true)):diagnosticMode===null?new BoundedMemoryCapture(phase,batch):new DiagnosticMemoryCapture(diagnosticMode);"),
 ("if(diagnosticMode!==null)capture.attachFailure(error,closed);", "if(diagnosticMode!==null||batchTiming)capture.attachFailure(error,closed);")]
for a,b in edits:assert text.count(a)==1;text=text.replace(a,b)
extra=(H/'factory.txt').read_text()
anchor="if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url))throw Error('SOURCE_ONLY_ROOT_METADATA_CALLBACK_REQUIRED');"
assert text.count(anchor)==1;text=text.replace(anchor,extra+'\n'+anchor);edits.append((anchor,extra+'\n'+anchor))
(H/'payload/fixed-metadata-reader.mjs').write_bytes(text.encode());inverse=text
for a,b in reversed(edits):assert inverse.count(b)==1;inverse=inverse.replace(b,a)
assert inverse.encode()==(H/'parent/fixed-metadata-reader.mjs').read_bytes()
mf=json.loads((H/'parent/MANIFEST.json').read_bytes());before=mf.copy();changed=[]
for row in mf['files']:
 if row['file']=='fixed-metadata-reader.mjs':b=(H/'payload'/row['file']).read_bytes();changed.append(dict(row));row['bytes']=len(b);row['sha256']=sha(b)
assert len(changed)==1
for n in ['batch-timing-diagnostic.mjs','hold-batch-timing.mjs']:
 b=(H/'payload'/n).read_bytes();assert all(r['file']!=n for r in mf['files']);mf['files'].append(dict(file=n,bytes=len(b),sha256=sha(b)))
(H/'payload/MANIFEST.json').write_bytes((json.dumps(mf,indent=2)+'\n').encode())
targets=[]
for n in ['fixed-metadata-reader.mjs','batch-timing-diagnostic.mjs','hold-batch-timing.mjs','MANIFEST.json']:
 b=(H/'payload'/n).read_bytes();row=dict(target=D+'/'+n,payload='payload/'+n,bytes=len(b),sha256=sha(b))
 if n in names:p=(H/'parent'/n).read_bytes();row.update(parent='parent/'+n,beforeBytes=len(p),beforeSha256=sha(p))
 else:assert not (R/D/n).exists();assert subprocess.run(['git','-C',str(R),'cat-file','-e',head+':'+D+'/'+n],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode!=0
 targets.append(row)
(H/'FORWARD.json').write_text(json.dumps(dict(version='CONTACT_FIXED_BATCH_TIMING_SOURCE_FORWARD_V1',root=str(R).replace('\\','/'),parentCommit=F,capturedHead=head,targets=targets,parents=rows,readerEdits=[dict(before=a,after=b) for a,b in edits],wholeReaderInverse=dict(before=(H/'parent/fixed-metadata-reader.mjs').read_bytes().decode(),after=text),wholeManifestInverse=dict(before=(H/'parent/MANIFEST.json').read_bytes().decode(),after=(H/'payload/MANIFEST.json').read_bytes().decode()),manifestOriginalRow=changed[0],newManifestRows=['batch-timing-diagnostic.mjs','hold-batch-timing.mjs'],sharedCanonicalRuntimeChanges=0,native='NOT_RUN'),indent=2)+'\n')
assert git('rev-parse','HEAD').decode().strip()==head
print(json.dumps(dict(targets=4,new=2,readerEdits=len(edits),parents=len(rows),native='NOT_RUN')))

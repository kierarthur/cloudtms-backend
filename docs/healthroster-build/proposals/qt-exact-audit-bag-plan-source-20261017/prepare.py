from pathlib import Path
import json,hashlib,subprocess
H=Path(__file__).resolve().parent;R=Path('D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026');D='docs/healthroster-build/proposals/contact-qt-positive-native-proposal-20261017';F='bb9786de3c4efa4327cbf01cc8da58f285657604'
sha=lambda b:hashlib.sha256(b).hexdigest();git=lambda *a:subprocess.check_output(['git','-c','gc.auto=0','-c','maintenance.auto=false','-C',str(R),*a]);head=git('rev-parse','HEAD').decode().strip();assert head==F
names=['fixed-metadata-reader.mjs','MANIFEST.json','batch-source.mjs','source.mjs','input-builder.mjs','control.mjs','hold-case.mjs','metadata-diagnostic.mjs','metadata-progress-diagnostic.mjs','SOURCE_PINS.json','hold-batch-timing.mjs','batch-timing-diagnostic.mjs','PLAN_SOURCE_SCOPES.json','reference/source-store-metadata.json'];rows=[]
for n in names:
 b=(R/D/n).read_bytes()
 for ref in [':',head+':',F+':']:assert b==git('show',ref+D+'/'+n)
 p=H/'parent'/n;p.parent.mkdir(exist_ok=True,parents=True);p.write_bytes(b);rows.append(dict(file=D+'/'+n,captured='parent/'+n,bytes=len(b),sha256=sha(b),historicalCommit=F))
for file in ['scripts/healthroster-install1-full-rehearsal.mjs','scripts/healthroster-live-test-restoration.mjs','scripts/healthroster-rehearsal-turn.mjs','scripts/healthroster-turn-control.mjs','scripts/cloudtms-managed-release-sql.mjs','supabase/release/export_contract.sql','docs/healthroster-build/proposals/qt-audit-record-sha256-independent-peer-20261017/R11_WATCHED_PUBLIC_SOURCE_PATHS.json']:
 b=(R/file).read_bytes()
 for ref in [':',head+':',F+':']:assert b==git('show',ref+file)
 p='parent/shared/'+Path(file).name;(H/p).write_bytes(b);rows.append(dict(file=file,captured=p,bytes=len(b),sha256=sha(b),historicalCommit=F))
text=(H/'parent/fixed-metadata-reader.mjs').read_bytes().decode();edits=[
 ("export {batchTimingProgressFor};", "export {batchTimingProgressFor};\nimport {AuditBagPlanCapture,compileAuditBagPlan,auditBagPlanProgressFor} from './audit-bag-plan.mjs';\nexport {auditBagPlanProgressFor};"),
 ("async function runFixedChild({input,env,turn,phase,diagnosticMode=null,batch=false,batchTiming=false}){", "async function runFixedChild({input,env,turn,phase,diagnosticMode=null,batch=false,batchTiming=false,auditBagPlan=false}){\n check(!auditBagPlan||(phase==='BEFORE'&&!batch&&!batchTiming&&diagnosticMode===null),'LOCAL_METADATA_AUDIT_PLAN_MODE_REFUSED');"),
 ("const capture=batchTiming?new BatchTimingCapture", "const capture=auditBagPlan?new AuditBagPlanCapture():batchTiming?new BatchTimingCapture"),
 ("if(diagnosticMode!==null||batchTiming)capture.attachFailure(error,closed);", "if(diagnosticMode!==null||batchTiming||auditBagPlan)capture.attachFailure(error,closed);")]
for a,b in edits:assert text.count(a)==1;text=text.replace(a,b)
anchor="if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url))throw Error('SOURCE_ONLY_ROOT_METADATA_CALLBACK_REQUIRED');";extra=(H/'factory.txt').read_bytes().decode();assert text.count(anchor)==1;text=text.replace(anchor,extra+'\n'+anchor);edits.append((anchor,extra+'\n'+anchor));(H/'payload/fixed-metadata-reader.mjs').write_bytes(text.encode());inverse=text
for a,b in reversed(edits):assert inverse.count(b)==1;inverse=inverse.replace(b,a)
assert inverse.encode()==(H/'parent/fixed-metadata-reader.mjs').read_bytes()
hold=(H/'parent/hold-batch-timing.mjs').read_bytes().decode();holdedits=[('Root-only exact C batch timing.','Root-only exact executed audit-bag estimated plan.'),('batch-timing','audit-bag-plan'),('BatchTiming','AuditBagPlan'),('batchTiming','auditBagPlan'),('BATCH_TIMING','AUDIT_BAG_PLAN'),('FIXED_BATCH_TIMINGS_CAPTURED','EXACT_AUDIT_BAG_PLAN_CAPTURED'),("'before-contact-qt-mcp-ready',",''),('queryProgress','planProgress'),('phaseTimings','planPhases')]
for a,b in holdedits:hold=hold.replace(a,b)
(H/'payload/hold-audit-bag-plan.mjs').write_bytes(hold.encode())
mf=json.loads((H/'parent/MANIFEST.json').read_bytes());originalrows=json.loads(json.dumps(mf['files']));changed=[]
for row in mf['files']:
 if row['file']=='fixed-metadata-reader.mjs':b=(H/'payload'/row['file']).read_bytes();changed.append(dict(row));row['bytes']=len(b);row['sha256']=sha(b)
assert len(changed)==1
for n in ['audit-bag-plan.mjs','hold-audit-bag-plan.mjs']:
 b=(H/'payload'/n).read_bytes();assert all(r['file']!=n for r in mf['files']);mf['files'].append(dict(file=n,bytes=len(b),sha256=sha(b)))
(H/'payload/MANIFEST.json').write_bytes((json.dumps(mf,indent=2)+'\n').encode());targets=[]
for n in ['fixed-metadata-reader.mjs','audit-bag-plan.mjs','hold-audit-bag-plan.mjs','MANIFEST.json']:
 b=(H/'payload'/n).read_bytes();row=dict(target=D+'/'+n,payload='payload/'+n,bytes=len(b),sha256=sha(b))
 if n in names:p=(H/'parent'/n).read_bytes();row.update(parent='parent/'+n,beforeBytes=len(p),beforeSha256=sha(p))
 else:assert not (R/D/n).exists();assert subprocess.run(['git','-C',str(R),'cat-file','-e',head+':'+D+'/'+n],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode!=0
 targets.append(row)
(H/'FORWARD.json').write_bytes((json.dumps(dict(version='CONTACT_EXACT_EXECUTED_AUDIT_BAG_PLAN_FORWARD_V1',root=str(R).replace('\\','/'),parentCommit=F,capturedHead=head,targets=targets,parents=rows,readerEdits=[dict(before=a,after=b) for a,b in edits],wholeReaderInverse=dict(before=(H/'parent/fixed-metadata-reader.mjs').read_bytes().decode(),after=text),wholeManifestInverse=dict(before=(H/'parent/MANIFEST.json').read_bytes().decode(),after=(H/'payload/MANIFEST.json').read_bytes().decode()),holderDerivation=dict(parent='parent/hold-batch-timing.mjs',replacements=[dict(before=a,after=b) for a,b in holdedits]),manifestOriginalRow=changed[0],unchangedManifestRows=[r for r in originalrows if r['file']!='fixed-metadata-reader.mjs'],newManifestRows=['audit-bag-plan.mjs','hold-audit-bag-plan.mjs'],sharedCanonicalRuntimeChanges=0,native='NOT_RUN'),indent=2)+'\n').encode());assert git('rev-parse','HEAD').decode().strip()==head
print(json.dumps(dict(targets=4,parents=len(rows),readerEdits=len(edits),native='NOT_RUN')))

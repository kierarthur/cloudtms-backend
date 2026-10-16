from pathlib import Path
import json,hashlib,subprocess
HERE=Path(__file__).resolve().parent;REPO=HERE.parents[3];ROOT=REPO.parent/'healthroster-install2-14102026'
sha=lambda b:hashlib.sha256(b).hexdigest()
def write(name,v):(HERE/name).write_text(json.dumps(v,indent=2)+'\n',encoding='utf-8',newline='\n')
def blob(commit,path):return subprocess.check_output(['git','show',commit+':'+path],cwd=REPO)
sql=json.loads((HERE/'JOINED_SQL_MAP.json').read_text());worker=json.loads((HERE/'WORKER_MAP.json').read_text())
migration_path='docs/healthroster-build/proposals/candidate-native-pending-20261016/one-time/01_nullable_pending_schema.sqlinc'
migration=blob('975376fd97855248c76b0ccbe027eed9b50f25ff',migration_path)
(HERE/'forward/NATIVE_PENDING_MIGRATION_BODY.sqlinc').write_bytes(migration)
frozen='docs/healthroster-build/proposals/checklist-v6-contract-correction-20261016/FROZEN_CONTRACT_INTEGRATION_C03.json'
frozen_raw=blob('55437f5ced7b82f27ac6975f2e7a2da3e32fc5ab',frozen)
(HERE/'FROZEN_PHYSICAL_INVOICE_PREREQUISITES.json').write_bytes(frozen_raw)
catalogue=blob('975376fd97855248c76b0ccbe027eed9b50f25ff','docs/healthroster-build/proposals/candidate-native-pending-20261016/tests/BASELINE_FRESH.sql').decode('utf-8')
start=catalogue.index('with signatures(signature) as(values\n')+len('with signatures(signature) as(values\n');end=catalogue.index('\n), routines as(',start)
sigs=[u['signature'] for u in sql['functions']]+['private.timesheet_contract_native_pending_commit_guard_v1()']
catalogue=catalogue[:start]+',\n'.join(" ('"+s+"')" for s in sorted(sigs))+catalogue[end:]
(HERE/'tests').mkdir(exist_ok=True);(HERE/'tests/BASELINE_FRESH.sql').write_text(catalogue,encoding='utf-8',newline='\n')
lines=['signature\tcommit\tcomplete_payload_source\tsource_file_sha256\tsource_span_sha256\tcanonical_complete_sha256']
lines += ['\t'.join([u[k] for k in ['signature','commit','path','fileSha256','spanSha256','canonicalCompleteSha256']]) for u in sql['functions']]
(HERE/'FINAL_FUNCTIONS.tsv').write_text('\n'.join(lines)+'\n',encoding='utf-8',newline='\n')
payloads=['forward/NATIVE_PENDING_MIGRATION_BODY.sqlinc','forward/CANDIDATE_FINAL_FUNCTIONS.sqlinc','forward/CANDIDATE_EFFECTS.sqlinc','forward/CANDIDATE_FINAL_ACL.sqlinc']
write('MOUNT_RECIPE.json',{
 'version':1,'status':'SOURCE_COMPOSITION_NATIVE_NOT_RUN','kind':'Exact finite final definition selection; no installation adapter or registry mutation',
 'rootObservedHead':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
 'rootSavedInputs':[{'path':p,'sha256':sha((ROOT/p).read_bytes())} for p in ['broker/src/candidate-app-backend.js','broker/src/index.js','supabase/release/current-contract.json']],
 'frozenPackages':sql['packages'],'explicitSelection':'REVIEWED_SELECTION.json','completePhysicalSourceMap':'JOINED_SQL_MAP.json','humanSignatureMap':'FINAL_FUNCTIONS.tsv',
 'prerequisites':{
  'physicalInvoice':{'recordCommit':'55437f5ced7b82f27ac6975f2e7a2da3e32fc5ab','path':frozen,'sha256':sha(frozen_raw),'copy':'FROZEN_PHYSICAL_INVOICE_PREREQUISITES.json','mode':'Already-root-composed7d/00f stable physical, full Daily and Invoice c86 then330a015/035. Do not reinstall historical schema or superseded routine whole roots.'},
  'expenseIngress':{'kernelCommit':'f0172356c678695a68db80cd066ed69e80648a29','kernel':'v6-expense-daily-ingress-20261016/forward/001_family_entry.sqlinc','sha256':'fd49adbd4620aee76d0ec0782658c7cca7046d80f0e2518e8689cefaf1d82034','outerPrefixCommit':'150ac08f4a9ae4196a6f6319f1478a693f70fd39','mode':'Both exact kernel functions before final WF. All12 full actual native prefix successors remain150ac; no duplicate competing helper.'},
  'invoicePending':{'commit':'b45eae27977007991ba449c1345890c7a4ee5f85','recipe':'v6-claim-native-pending-20261016/MOUNT_RECIPE.json','sha256':'90370acf2f334ed5fd2584ebe0790ba0cdf879f1097afbfb7a791844faae8294','mode':'Own complete025/026/027/028/030 +claim_pending helper; replaces rotation-only ffc where superseded, no original/current fallback for genuine pending. Root review required.'},
  'sourcePending':'Source-owned bound-origin pending/current reader composition remains explicit unprovided qualification; do not infer Work/Bank/Source facts.',
  'mytms':{'sourceCommit':'f8aba9d7b7a7a5ba9c67d4c5ece424d594e3bd79','manifest':'docs/healthroster-build/MYTMS_FACTUAL_CREATE_RESUBMIT_V6_SOURCE_MANIFEST.json','sha256':'45602075d6f532bd2a48a9b112f6d73680aba4e8a69d82ac1a3f224d7f3d5e88','mode':'Existing same72/same17 optional factual CREATE and resubmit request-schema repairs + exact request/body journals; native/API still NOT_RUN.'}},
 'orderedMount':[
  {'stage':'MIGRATION','payload':payloads[0],'mode':'Exact975 one-time body after actual0159. Root allocates/reviews one new db:new migration and generated contract; never alter/relock7d. Includes its deferred trigger function once; no repeated CREATE from this file in routine root.'},
  {'stage':'FINAL_FUNCTIONS','payload':payloads[1],'mode':'55 complete controlling definitions in supplied order, after all current historical owners and stable physical/Invoice prerequisites. Final975 workflow appears ONCE. Do not then mount original predecessor payloads. Every complete function source span/hash is pinned; no latest-body fallback.'},
  {'stage':'ACTUAL_DDL_EFFECTS','payload':payloads[2],'mode':'Exact two96c5 existing summary trigger drop/create after all selected functions. Pending deferred trigger/index is only the migration.'},
  {'stage':'FINAL_ACL','payload':payloads[3],'mode':'Explicit55 functions plus migration trigger function; logical postgres owner mapping only existing approved adapter, all private owner-only/public existing RPCs service-only; final notify.'},
  {'stage':'WORKER_SOURCE','record':'WORKER_MAP.json','mode':'10 complete same-name functions at exact listed modules, shared unchanged17decoder +sole caller access argument. Preserve outer correction/manager/Source/contact owners.'}],
 'schemaAndRegistry':'Root allocates final controlling repeatable/include; finite exact55 signature chain metadata, preserving EVERY existing companion root and historical owner. Migration trigger function has its actual migration owner. No automated blind register-to-current, no changed Bank body. Generated current-contract/native RLS/ACL/trigger catalogue required before release.',
 'nativeQualification':'NOT_RUN: no DB/local infrastructure/locks/provider/deploy. 25/28 earlier pure native proofs remain separately immutable and do not qualify lifecycle. Root actual full176 verifier/rehearsal/feature/native producer gates remain mandatory.',
 'knownSourceCorrections':['11af complete context restores independently reviewed c475 workflow_facts accepted-body/native receipt proof omitted by c03/975 raw aggregate; no silent prior PASS','2b5 complete detail adds actual WORKER_DRAFT Expense purpose policy; separate worked-anchor/carrier canonical preparation; movedExpense native submit remains controlled','Only selected a10f paper-source helper CRLF canonically becomes LF after native literal-token scan; raw+canonical hashes recorded. No SQL literals changed. Historical CRLF ACL omitted in favour of explicit final ACL.','Raw pglast character spans converted to UTF8 physical bytes; multibyte QR comment regression checked, complete function parsed individually.'],
 'payloads':[{'path':p,'sha256':sha((HERE/p).read_bytes()),'bytes':(HERE/p).stat().st_size} for p in payloads],
 'counts':{'uniqueFinalFunctions':55,'workflowDefinitions':1,'supersededDefinitionsExcluded':29,'actualRoutineDdlEffects':2,'migrationOnlyTriggerFunctions':1,'workerFunctions':10},
 'verification':{'source':'tests/SOURCE_RESULT.json','grammar':'tests/GRAMMAR_RESULT.json','native':'NOT_RUN','acl':'NOT_RUN','api':'NOT_RUN','financial':'NOT_RUN','race':'NOT_RUN','databaseCalls':0}})
all=[]
for f in HERE.rglob('*'):
 if f.is_file() and f.name!='SOURCE_MANIFEST.json' and '__pycache__' not in f.parts:
  all.append({'path':f.relative_to(HERE).as_posix(),'sha256':sha(f.read_bytes()),'bytes':f.stat().st_size})
write('SOURCE_MANIFEST.json',{'status':'SOURCE_COMPOSITION_NATIVE_NOT_RUN','files':sorted(all,key=lambda x:x['path'])})
print('Joined finite recipe, migration,55-signature map,56-signature catalogue capture and exact Worker map recorded; no SQL execution.')

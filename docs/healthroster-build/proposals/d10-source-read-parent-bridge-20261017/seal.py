from pathlib import Path
import hashlib,json,subprocess,os
HERE=Path(__file__).resolve().parent
ROOT=Path('D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026')
def lp(p):return Path('\\\\?\\'+str(Path(p).resolve()))
def read(p):return lp(p).read_bytes()
def put(p,b):
 p=lp(HERE/p);p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(b if isinstance(b,bytes) else b.encode())
def doc(p,o):put(p,json.dumps(o,ensure_ascii=False,indent=2)+'\n')
def sha(b):return hashlib.sha256(b).hexdigest()
def git(*a):return subprocess.check_output(['git','-c','gc.auto=0','-c','maintenance.auto=false','-c','core.longpaths=true',*a],cwd=ROOT,env={**os.environ,'GIT_OPTIONAL_LOCKS':'0','GIT_NO_LAZY_FETCH':'1'})
head=git('rev-parse','HEAD').decode().strip()
assert head=='bb9786de3c4efa4327cbf01cc8da58f285657604'
review='docs/healthroster-build/proposals/source-current-read-peer-f23-20261017/CLASSIFICATION.json'
proof=json.loads(read(ROOT/review));paths={review}
paths.update(x['path'] for x in proof['dependencies'])
paths.update(x.get('source',x['path']) for x in proof['actualParents'])
paths.update(x['source'] for x in proof['variants'])
for r in proof['reverses']:paths.update([r['output'],r.get('parentSource',r['parent'])])
rows=[]
for name in sorted(paths):
 b=read(ROOT/name);assert b==git('show',':'+name)==git('show','HEAD:'+name),('PUBLIC_PROOF_HEAD_INDEX',name)
 capture='reference/retained-proof/'+name;put(capture,b)
 rows.append({'file':name,'capture':capture,'sha256':sha(b),'bytes':len(b),'commit':head})
doc('RETAINED_REAL_PROOF_INPUTS.json',{'qualification':'Actual unmodified Source READ load() complete inputs; existing prior factories execute their original proof checks in the test','rows':rows})
tests=subprocess.run(['node','--test',str(HERE/'test.mjs')],capture_output=True,check=True)
put('TEST_RESULTS.txt',tests.stdout)
doc('RESULTS.json',{'status':'PASS_BOUNDED_SOURCE_ONLY','nodeTests':22,'proofs':'Both real factories, complete real gate prior callback, original relevant FROZEN_BASE census, unmodified Source READ whole artifacts','retainedWholeProofFiles':len(rows),'runtimeSQLChanges':0,'fullJoinedGate':'ROOT_RERUN_REQUIRED','native':'NOT_RUN','original86':'PRESERVED_UNCHANGED_HISTORICAL_JOINED_GATE_HOLD','originalGateFailure':'HEALTH_SOURCE_READ_TYPE_CURRENT_SOURCE_CHANGED','note':'Initial isolated integration fixture omitted the original frozen-baseline allowance and correctly refused PRIOR_STATEMENT_UNPROVED; final test calculates actual baseline from the real historical Git source rather than supplying a synthetic allowance.'})
doc('REVIEW_CHAIN.json',{'version':'HEALTH_D10_SOURCE_READ_PARENT_BRIDGE_V1','currentRootHEAD':head,'selectedParentState':'Original frozen D10 staged images, not yet HEAD','oldSourceRead':'Unchanged complete original factory/proofs; exactly one mandatory-verifier input is reversed before that factory','newD10':'Still validates actual complete new verifier and actual unchanged census; no reconstructed file fed to outer D10','gateDelta':'One import extends existing D10 export list; one factory initializer uses the narrow wrapper while retaining exact original prior callback/options','statementTransfer':'Exactly four new private-helper literal lines, whole inverse must equal pinned complete original parent, all current/parent/supplied census keys/counts/hazards identical','scope':'No SQL/provider/native/auth/contract/runtime body/permission changes','truth':'Source-only repair of ordering; original source-only peer is preserved, original full joined gate HOLD is preserved; new full joined source gate must be rerun by root'})
put('README.md','''# D10 retained READ parent-image bridge\n\nThis separate successor preserves the frozen 86-file D10 packet and its actual joined-gate HOLD. The retained Source READ classifier owns the complete pre-D10 mandatory verifier. Its eager whole-file check previously ran before the D10 outer unchanged-statement proof.\n\nOnly two JavaScript files change. The new D10 wrapper accepts only the exact original or exact D10 verifier images, removes exactly four pinned private-helper literal lines, proves the complete original byte image and identical real hazard census, and calls the unmodified original Source READ factory/proofs on that image. The outer D10 factory still sees and validates the real new image. All other paths dispatch to the original factory unchanged. No allowances are fabricated.\n\nThe tests import the complete real gate with only import locators/test exports adjusted. Both real classifier factories and their actual full prior callbacks/proof artifacts run. The baseline is calculated by the actual census function from the exact relevant original FROZEN_BASE Git owner. Original eager failure, corrected composition, all 13 D10 points, four missing/duplicate/altered literals, wholebody/census/proof drift and retained tuple proof are covered.\n\nNon-writing replay from this frozen folder:\n\n- `node --test test.mjs`\n- `python -X utf8 -B verify.py --current` (expects the original D10 staged parents until root mounts this successor; afterward omit `--current` for archival replay).\n\nDo not run build.py or seal.py during peer replay. They are retained construction sources. The two selected postimages require independent peer and root adoption, followed by the actual complete joined gate. No native, SQL, auth, contract-generation or installation result is claimed. Root is the only writer of the integration worktree.\n''')
subprocess.run(['python','-X','utf8','-B',str(HERE/'verify.py'),'--current'],check=True)
files=[]
for p in sorted(lp(HERE).rglob('*')):
 if p.is_file() and p!=lp(HERE/'MANIFEST.json') and '__pycache__' not in p.parts:
  b=p.read_bytes();files.append({'file':str(p.relative_to(lp(HERE))).replace('\\','/'),'bytes':len(b),'sha256':sha(b)})
doc('MANIFEST.json',{'version':'HEALTH_D10_SOURCE_READ_PARENT_BRIDGE_FROZEN_V1','qualification':'SOURCE_ONLY_NO_NATIVE_OR_JOINED_GATE_CLAIM','files':files})
print(json.dumps({'filesIncludingManifest':len(files)+1,'manifestSha256':sha(read(HERE/'MANIFEST.json')),'selectionSha256':sha(read(HERE/'PAYLOAD_SELECTION.json')),'proofFiles':len(rows)},indent=2))

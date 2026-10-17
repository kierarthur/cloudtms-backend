"""Finite source verification. No SQL, native holder, credentials or network."""
from pathlib import Path
import hashlib,json,re,subprocess,sys
sys.dont_write_bytecode=True
HERE=Path(__file__).resolve().parent
ROOT=Path('D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026')
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
sha=lambda b:hashlib.sha256(b).hexdigest()
git=lambda *a:subprocess.check_output(['git','-c','gc.auto=0','-c','maintenance.auto=false','-C',str(ROOT),*a])
checks=[]
def check(name,ok):
 assert ok,name
 checks.append(name)
pins=json.loads((HERE/'SOURCE_PINS.json').read_bytes())
manifest=json.loads((HERE/'SOURCE_MANIFEST.json').read_bytes())
check('sealed-Source-source-pins',pins['status']=='SEALED_SOURCE_ONLY' and pins['rootCommit']=='bb6252f46597cf15a6450adfb2af27c1e615fb3d')
check('sealed-Source-adapter-version',manifest['version']=='SOURCE_STATUS_CHILD_PURE_ADAPTER_EXECUTABLE_V1')
check('source-pins-hash',sha((HERE/'SOURCE_PINS.json').read_bytes())==manifest['sourcePinsSha256'])
for r in manifest['files']:check('saved-executable:'+r['file'],sha((HERE/r['file']).read_bytes())==r['sha256'])
for r in pins['rootInputs']:
 b=(ROOT/r['file']).read_bytes()
 check('exact-saved-root:'+r['file'],len(b)==r['bytes'] and sha(b)==r['sha256'])
 check('exact-pinned-current-index-root:'+r['file'],b==git('show',pins['rootCommit']+':'+r['file'])==git('show','HEAD:'+r['file'])==git('show',':'+r['file']))
for r in json.loads((HERE/'SOURCE_TRANSFERS.json').read_bytes()):
 b=(HERE/r['file']).read_bytes();check('forward:'+r['file'],sha(b)==r['afterSha256'])
 text=b.decode()
 for h in reversed(r['hunks']):
  check('whole-hunk-one-occurrence:'+r['file'],text.count(h['after'])==1)
  text=text.replace(h['after'],h['before'],1)
 original=git('show',r['ownerCommit']+':'+r['ownerFile'])
 check('complete-current-parent-inverse:'+r['file'],text.encode()==original and sha(original)==r['beforeSha256'])
 check('saved-complete-parent-reference:'+r['file'],original==(HERE/'reference/daily'/r['file']).read_bytes())
for f in ['control.mjs','timing.mjs']:check('byte-unchanged:'+f,(HERE/f).read_bytes()==(HERE/'reference/daily'/f).read_bytes())
check('fixed-reader-only-application-name-forward',
 (HERE/'fixed-metadata-reader.mjs').read_bytes().replace(b'healthroster_source_status_pure_fixed_metadata',b'healthroster_daily_pure_fixed_metadata')==(HERE/'reference/daily/fixed-metadata-reader.mjs').read_bytes())
ORIGINAL=HERE.parent/'source-status-child-pure-rollback-adapter-20261017'
milestone=json.loads((ORIGINAL/'PROGRAMME_MILESTONE_MANIFEST.json').read_bytes())
milestone_prefix='docs/healthroster-build/proposals/source-status-child-pure-rollback-adapter-20261017/'
milestone_commit='fb235cfae7a6d3f5eb3ef581bebebf7daa68315e'
check('immutable-programme-milestone-manifest',
 (ORIGINAL/'PROGRAMME_MILESTONE_MANIFEST.json').read_bytes()==git('show',milestone_commit+':'+milestone_prefix+'PROGRAMME_MILESTONE_MANIFEST.json'))
for r in milestone['files']:
 b=(ORIGINAL/r['file']).read_bytes()
 check('immutable-programme-milestone:'+r['file'],len(b)==r['bytes'] and sha(b)==r['sha256'] and b==git('show',milestone_commit+':'+milestone_prefix+r['file']))
for f in [*HERE.glob('*.mjs'),HERE/'programme/build-program.mjs']:
 subprocess.run(['node','--check',str(f)],check=True,capture_output=True)
 text=f.read_text()
 check('operational-paths-D:'+f.name,not re.search(r'C:[/\\](?:Users|tmp)[/\\]',text,re.I))
examples=json.loads((HERE/'OFFLINE_INPUTS.json').read_bytes())
check('explicit-synthetic-examples-only',examples['syntheticIdentity'] and examples['native']=='NOT_RUN' and examples['dbConnections']==0)
grammar=[]
for r in examples['examples']:
 check('synthetic-SQL-hash:'+r['name'],sha(r['sql'].encode())==r['sha256'] and len(r['sql'].encode())==r['bytes'])
 statements=len(pglast.parse_sql(r['sql']));programs=len(pglast.parse_plpgsql(r['sql']))
 grammar.append(dict(name=r['name'],statements=statements,plpgsqlPrograms=programs))
 check('PG17-complete-synthetic-program:'+r['name'],statements>0)
check('exact-Source-programme-statement-count',grammar[0]['statements']==6 and grammar[0]['plpgsqlPrograms']==4)
check('fresh-name-wide-absence-four-selects',grammar[2]['statements']==4 and grammar[2]['plpgsqlPrograms']==0)
result=dict(checks=len(checks),sourceRootCommit=pins['rootCommit'],rootInputs=len(pins['rootInputs']),wholeCurrentParents=9,
 grammar=grammar,native='NOT_RUN',restoration='NOT_RUN',DBConnections=0,publicFinancialResult='NOT_PROVIDED')
(HERE/'SOURCE_VERIFY_RESULTS.json').write_bytes((json.dumps(result,indent=2)+'\n').encode())
print(json.dumps(result))

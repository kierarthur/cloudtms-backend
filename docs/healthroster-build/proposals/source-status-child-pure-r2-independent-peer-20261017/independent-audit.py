"""Independent complete-parent and parsed record/range-alias audit. Offline only."""
from pathlib import Path
import sys,json,hashlib,subprocess,difflib
sys.dont_write_bytecode=True
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
from pglast import ast
from pglast.visitors import Visitor
BASE=Path(__file__).resolve().parent
R2=BASE/'frozen/source-status-child-pure-r2-alias-forward-20261017'
R1=BASE/'frozen/source-status-child-pure-rollback-adapter-20261017'
I2=Path('D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026')
sha=lambda b:hashlib.sha256(b).hexdigest()
checks=[]
def check(name,condition):
    assert condition,name
    checks.append(name)
def git(root,*args):
    return subprocess.check_output(['git','-c','gc.auto=0','-c','maintenance.auto=false','-C',str(root),*args])
def walk(v):
    if isinstance(v,dict):
        yield v
        for x in v.values():yield from walk(x)
    elif isinstance(v,list):
        for x in v:yield from walk(x)
class Names(Visitor):
    def __init__(self):
        super().__init__();self.aliases=set();self.references=set()
    def visit_RangeVar(self,anc,node):
        self.aliases.add(node.alias.aliasname if node.alias else node.relname)
    def visit_RangeFunction(self,anc,node):
        if node.alias:self.aliases.add(node.alias.aliasname)
    def visit_ColumnRef(self,anc,node):
        if len(node.fields)>=2 and isinstance(node.fields[0],ast.String):
            self.references.add(tuple(f.sval if isinstance(f,ast.String) else '*' for f in node.fields))
def audit(text):
    scopes=[];collisions=[]
    for ordinal,wrapper in enumerate(pglast.parse_plpgsql(text)):
        fn=wrapper['PLpgSQL_function'];records={}
        for datum in fn['datums']:
            for d in datum.values():
                if not isinstance(d,dict):continue
                typ=d.get('datatype',{}).get('PLpgSQL_type',{}).get('typname','')
                if typ.endswith('%rowtype') or typ=='record':records[d['refname']]=typ
        expressions=0
        for item in walk(fn['action']):
            if 'PLpgSQL_expr' not in item:continue
            expr=item['PLpgSQL_expr'];query=expr['query'];mode=expr.get('parseMode',0)
            if mode!=0:continue # Complete SQL only; all three actual failing counts are here.
            parsed=pglast.parse_sql(query);names=Names();names(parsed);expressions+=1
            for alias in sorted(set(records)&names.aliases):
                refs=sorted(r for r in names.references if r[0]==alias)
                if refs:collisions.append({'programme':ordinal,'record':alias,'recordType':records[alias],'qualifiedReferences':refs,'query':query})
        scopes.append({'programme':ordinal,'recordVariables':records,'completeSqlExpressions':expressions})
    return {'scopes':scopes,'collisions':collisions}
old=(R1/'programme/PROGRAMME.sql').read_bytes();new=(R2/'programme/PROGRAMME.sql').read_bytes()
check('original-full-programme-pin',len(old)==20303 and sha(old)=='ed1eb564947cea75131284ba62967e0654154d7374eebb9bba4a436bc12908a8')
check('R2-full-programme-pin',len(new)==20447 and sha(new)=='ec52dff75ea03a6886691cab3b426186d07b6636a97e6a94f2ff3b2ef5fe6f64')
before=audit(old.decode());after=audit(new.decode())
check('original-real-record-alias-collisions-three',len(before['collisions'])==3)
check('original-two-preflight-one-postflight',[r['programme'] for r in before['collisions']]==[0,0,2])
check('original-actual-p-record-and-namespace-fields',all(r['record']=='p' and r['recordType']=='pg_catalog.pg_proc%rowtype' and r['qualifiedReferences']==[('p','proname'),('p','pronamespace')] for r in before['collisions']))
check('R2-zero-record-range-collisions',after['collisions']==[])
check('all-four-original-programmes-preserved',before['scopes']==after['scopes'] and len(after['scopes'])==4)
needle="from pg_catalog.pg_proc catalogue_routine join pg_catalog.pg_namespace s on s.oid=catalogue_routine.pronamespace\n  where s.nspname='private' and catalogue_routine.proname="
oldneedle="from pg_catalog.pg_proc p join pg_catalog.pg_namespace s on s.oid=p.pronamespace\n  where s.nspname='private' and p.proname="
check('exact-three-new-query-occurrences',new.decode().count(needle)==3)
check('complete-programme-raw-inverse',new.decode().replace(needle,oldneedle).encode()==old)
mutation_results=[]
parts=new.decode().split(needle)
for index in range(3):
    mutant=parts[0]+''.join((oldneedle if n==index else needle)+part for n,part in enumerate(parts[1:]))
    result=audit(mutant)
    check('independent-negative-alias-reintroduction-'+str(index),len(result['collisions'])==1 and result['collisions'][0]['programme']==[0,0,2][index])
    mutation_results.append({'site':index,'collisions':result['collisions']})
edits=json.loads((R2/'R2_FULL_PARENT_TRANSFERS.json').read_bytes());changes=[];diff=[]
check('exact-21-complete-parent-forwards',len(edits)==21)
for row in edits:
    b=(R2/row['file']).read_bytes();parent=(R1/row['file']).read_bytes()
    check('forward-hash:'+row['file'],sha(b)==row['afterSha256'])
    text=b.decode()
    for h in reversed(row['hunks']):
        check('single-whole-image:'+row['file'],text.count(h['after'])==1)
        text=text.replace(h['after'],h['before'],1)
    check('literal-whole-parent-inverse:'+row['file'],text.encode()==parent and sha(parent)==row['beforeSha256'])
    changes.append({'file':row['file'],'hunks':len(row['hunks']),'before':sha(parent),'after':sha(b)})
    if row['file'] not in ['OFFLINE_INPUTS.json','SOURCE_TRANSFERS.json']:
        diff.extend(difflib.unified_diff(parent.decode().splitlines(True),b.decode().splitlines(True),fromfile='5931/'+row['file'],tofile='d56d/'+row['file']))
critical=['control.mjs','timing.mjs','fixed-metadata-reader.mjs','programme/source-child.sqlinc','programme/key-source.sqlinc','programme/assertions.sql','programme/build-program.mjs','SOURCE_PINS.json','PROGRAMME_SOURCE_TRANSFER.json','GENUINE_R2_COMMAND_EVIDENCE.json']
for file in critical:check('unchanged-authority-and-execution-guard:'+file,(R1/file).read_bytes()==(R2/file).read_bytes())
for file in ['result-validator.mjs','input-builder.mjs','hold-pure.mjs']:
    b=(R2/file).read_bytes()
    check('single-required-programme-length-pin:'+file,b.count(b'programmeBytes:20447')==1 and b.replace(b'programmeBytes:20447',b'programmeBytes:20303')==(R1/file).read_bytes())
b=(R2/'source.mjs').read_bytes()
check('source-loader-only-programme-pins-and-exact-new-folder',b.replace(b'ec52dff75ea03a6886691cab3b426186d07b6636a97e6a94f2ff3b2ef5fe6f64',b'ed1eb564947cea75131284ba62967e0654154d7374eebb9bba4a436bc12908a8').replace(b'source-status-child-pure-r2-alias-forward-20261017',b'source-status-child-pure-rollback-adapter-20261017').replace(b'programme.sqlBytes===20447',b'programme.sqlBytes===20303')==(R1/'source.mjs').read_bytes())
evidence=json.loads((R2/'R1_ROOT_EVIDENCE.json').read_bytes());root_records=[]
for row in evidence['records']:
    b=git(I2,'show',evidence['rootEvidenceCommit']+':'+row['file'])
    check('actual-root-R1-record:'+row['file'],len(b)==row['bytes'] and sha(b)==row['sha256'])
    target=BASE/'reference/r1-root'/Path(row['file']).name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(b)
    root_records.append({'file':row['file'],'bytes':len(b),'sha256':sha(b)})
check('truthful-root-native-HOLD',evidence['statement']==10 and evidence['sqlstate']=='42702' and not evidence['childCreateStatement11Reached'] and evidence['accepted']==False and evidence['rolledBack'] and evidence['connectedFresh'] and evidence['freshNameWideChildCount']==0 and evidence['markerAbsent'] and evidence['module']=='LEGACY:1' and evidence['Cafter']=='NOT_RUN' and evidence['fullRestoration']=='NOT_RUN' and evidence['ownAUnlockAndCloseConfirmed'] and not evidence['provenByOriginalQualifier'])
(BASE/'FINITE_PARENT_DIFF.txt').write_bytes(''.join(diff).encode())
result={'checks':len(checks),'checkNames':checks,'parser':{'pglast':pglast.__version__,'postgres':pglast.get_postgresql_version()},'before':before,'after':after,'threeNegativeAliasMutations':mutation_results,'fullParentForwards':changes,'rootR1Records':root_records,'native':'NOT_RUN','restoration':'NOT_RUN','bindingQualification':'CONSERVATIVE_SOURCE_AUDIT_NATIVE_R2_STILL_REQUIRED'}
(BASE/'INDEPENDENT_AUDIT.json').write_bytes((json.dumps(result,indent=2)+'\n').encode())
print(json.dumps({'checks':len(checks),'oldCollisions':len(before['collisions']),'newCollisions':len(after['collisions']),'wholeParents':len(edits),'negativeAliasMutations':len(mutation_results),'native':'NOT_RUN'}))

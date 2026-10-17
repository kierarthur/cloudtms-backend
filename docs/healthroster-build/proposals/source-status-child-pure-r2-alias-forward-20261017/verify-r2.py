"""Exact R1-to-R2 source inverse and conservative PL/SQL alias-scope audit.
No SQL is executed. PostgreSQL native binding remains required.
"""
from pathlib import Path
import hashlib,json,subprocess,sys
sys.dont_write_bytecode=True
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
from pglast import ast
HERE=Path(__file__).resolve().parent
OLD=HERE.parent/'source-status-child-pure-rollback-adapter-20261017'
sha=lambda b:hashlib.sha256(b).hexdigest()
checks=[]
def check(name,ok):
 assert ok,name
 checks.append(name)
git=lambda *a:subprocess.check_output(['git','-c','gc.auto=0','-c','maintenance.auto=false','-C',str(HERE),*a])
prefix='docs/healthroster-build/proposals/source-status-child-pure-rollback-adapter-20261017/'
freeze=json.loads((OLD/'FREEZE_MANIFEST.json').read_bytes())
for r in [*freeze['files'],dict(file='FREEZE_MANIFEST.json',bytes=len((OLD/'FREEZE_MANIFEST.json').read_bytes()),sha256=sha((OLD/'FREEZE_MANIFEST.json').read_bytes()))]:
 b=(OLD/r['file']).read_bytes()
 check('original-R1-unchanged:'+r['file'],len(b)==r['bytes'] and sha(b)==r['sha256'] and b==git('show','5931ae9e2f1ab11de810996f5aea6a02570b7ce9:'+prefix+r['file']))
edits=json.loads((HERE/'NAMESPACE_ALIAS_ONLY_EDITS.json').read_bytes())
check('three-catalogue-query-aliases-only',sum(e['namespaceQueries'] for e in edits['edits'])==3)
for r in edits['edits']:
 b=(HERE/r['file']).read_bytes();text=b.decode()
 check('corrected-alias-file:'+r['file'],sha(b)==r['afterSha256'])
 for h in reversed(r['hunks']):
  check('exact-range-alias-occurrences:'+r['file'],text.count(h['after'])==h['occurrences'])
  text=text.replace(h['after'],h['before'])
 check('entire-pre-post-source-inverse:'+r['file'],text.encode()==(OLD/r['file']).read_bytes() and sha(text.encode())==r['beforeSha256'])
for f in ['programme/source-child.sqlinc','programme/key-source.sqlinc','programme/assertions.sql',
 'programme/build-program.mjs','control.mjs','timing.mjs','fixed-metadata-reader.mjs','SOURCE_PINS.json']:
 check('byte-unchanged:'+f,(HERE/f).read_bytes()==(OLD/f).read_bytes())
sql=(HERE/'programme/PROGRAMME.sql').read_bytes().decode()
for r in edits['edits']:
 for h in r['hunks']:sql=sql.replace(h['after'],h['before'])
check('complete-combined-programme-alias-only-inverse',sql.encode()==(OLD/'programme/PROGRAMME.sql').read_bytes())
check('no-conflict-directive-or-GUC',not any(t in (HERE/'programme/PROGRAMME.sql').read_text().lower() for t in ['#variable_conflict','plpgsql.variable_conflict','use_column','use_variable']))

def dict_walk(v):
 if isinstance(v,dict):
  yield v
  for value in v.values():yield from dict_walk(value)
 elif isinstance(v,list):
  for value in v:yield from dict_walk(value)
def sql_walk(v):
 if isinstance(v,ast.Node):
  yield v
  for field in v:yield from sql_walk(getattr(v,field))
 elif isinstance(v,(tuple,list)):
  for item in v:yield from sql_walk(item)
def audit(sql):
 collisions=[];programs=[]
 for ordinal,program in enumerate(pglast.parse_plpgsql(sql)):
  function=program['PLpgSQL_function']
  names={d.get('refname') for node in function['datums'] for d in node.values() if isinstance(d,dict)}
  names.discard(None);names.discard('(unnamed row)')
  queries=0;aliases=[]
  for d in dict_walk(function):
   if 'PLpgSQL_expr' not in d:continue
   e=d['PLpgSQL_expr'];q=e['query'];mode=e.get('parseMode',0)
   if mode==3:q=q.split(':=',1)[1] if ':=' in q else q.split('=',1)[1]
   parsed=pglast.parse_sql(('select ' if mode else '')+q);queries+=1
   for node in sql_walk(parsed):
    if isinstance(node,(ast.RangeVar,ast.RangeFunction)) and node.alias:
     alias=node.alias.aliasname
     aliases.append(alias)
     if alias in names:
      refs=[tuple(f.sval for f in col.fields) for col in sql_walk(parsed)
       if isinstance(col,ast.ColumnRef) and len(col.fields)==2
       and all(isinstance(f,ast.String) for f in col.fields) and col.fields[0].sval==alias]
      collisions.append(dict(program=ordinal,alias=alias,relation=getattr(node,'relname',None),qualifiedReferences=refs))
  programs.append(dict(program=ordinal,sqlExpressions=queries,aliases=aliases,declaredNames=sorted(names)))
 return collisions,programs

old_collisions,old_programs=audit((OLD/'programme/PROGRAMME.sql').read_text())
new_collisions,new_programs=audit((HERE/'programme/PROGRAMME.sql').read_text())
check('R1-static-scope-reproduces-three-real-collision-sites',len(old_collisions)==3 and all(c['alias']=='p' and c['relation']=='pg_proc' for c in old_collisions))
check('R1-collisions-in-both-DO-blocks',sorted(c['program'] for c in old_collisions)==[0,0,2])
check('R1-exact-record-versus-range-qualified-fields',all(sorted(c['qualifiedReferences'])==[('p','proname'),('p','pronamespace')] for c in old_collisions))
check('R2-all-programme-DO-and-function-scope-no-range-alias-collisions',new_collisions==[])
check('same-four-complete-PL-programmes',len(new_programs)==len(old_programs)==4)
check('same-SQL-expression-census',[p['sqlExpressions'] for p in old_programs]==[p['sqlExpressions'] for p in new_programs])
for f in ['preflight.sql','postflight.sql']:
 check('record-p-type-unchanged:'+f,'declare p pg_catalog.pg_proc%rowtype; n integer;' in (HERE/'programme'/f).read_text())
for r in json.loads((HERE/'R2_FULL_PARENT_TRANSFERS.json').read_bytes()):
 b=(HERE/r['file']).read_bytes();check('R2-selected-forward:'+r['file'],sha(b)==r['afterSha256'])
 text=b.decode()
 for h in reversed(r['hunks']):
  check('R2-one-whole-current-image:'+r['file'],text.count(h['after'])==1)
  text=text.replace(h['after'],h['before'],1)
 check('R2-full-5931-parent-inverse:'+r['file'],text.encode()==git('show','5931ae9e2f1ab11de810996f5aea6a02570b7ce9:'+prefix+r['file']) and sha(text.encode())==r['beforeSha256'])
result=dict(checks=len(checks),R1ActualFailure='42702: column reference p.pronamespace is ambiguous; statement10 preflight, child CREATE11 not reached',
 R1Restoration='NOT_RUN_C_AFTER_NOT_RUN',R1MarkerRollbackAndFreshAbsence='ROOT_REPORTED_NOT_FULL_RESTORATION',
 original46='BYTE_UNCHANGED',originalProgramme13='RETAINED_BY_ORIGINAL_PACKET',
 oldCollisionSites=old_collisions,programmes=new_programs,
 native='NOT_RUN',restoration='NOT_RUN',bindingQualification='STATIC_SCOPE_AUDIT_ONLY_NATIVE_R2_REQUIRED')
(HERE/'R2_SOURCE_RESULTS.json').write_bytes((json.dumps(result,indent=2)+'\n').encode())
print(json.dumps({k:result[k] for k in ['checks','R1ActualFailure','original46','oldCollisionSites','native','restoration','bindingQualification']}))

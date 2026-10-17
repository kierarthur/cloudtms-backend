import copy, hashlib, json, pathlib, subprocess, sys
sys.path.insert(0,r'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
from pglast import parse_sql, ast
from pglast.stream import RawStream
HERE=pathlib.Path(__file__).resolve().parent
ROOT=pathlib.Path(r'D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026')
sha=lambda b:hashlib.sha256(b).hexdigest()
def git(*a):return subprocess.check_output(['git','-C',str(ROOT),*a])
HEAD=git('rev-parse','HEAD').decode().strip()
names=['16102026_0159_nhsp_v6_01_physical_source_review_schema.sql','16102026_1100_nhsp_v6_source_health_metadata.sql','16102026_1101_nhsp_v6_source_retained_native_preparations.sql','16102026_1102_nhsp_v6_source_acceptance_applications_and_adoptions.sql','16102026_1103_nhsp_v6_source_canonical_publication_bridge.sql','16102026_1104_nhsp_v6_source_owner_only_rls.sql']
files=['supabase/migrations/'+x for x in names]
shared=['scripts/cloudtms-managed-release.mjs','scripts/cloudtms-managed-release-sql.mjs','scripts/cloudtms-db-release-lib.mjs','scripts/healthroster-install2-local-schema.mjs','scripts/compose-healthroster-v6-root01.mjs','supabase/release/migration-lock.json','docs/DATABASE_RELEASE_BIBLE.md']
pins=[]
for f in files+shared:
 b=(ROOT/f).read_bytes();assert b==git('show',':'+f)==git('show','HEAD:'+f)==git('show','907ee50aaaacefeedfc4a52afc637c7144731ee0:'+f),f
 dst=HERE/'parents'/f;dst.parent.mkdir(parents=True,exist_ok=True);dst.write_bytes(b)
 pins.append(dict(file=f,bytes=len(b),sha256=sha(b),head=HEAD,historical='907ee50aaaacefeedfc4a52afc637c7144731ee0',capture='parents/'+f))
def text(f):return (ROOT/f).read_text(encoding='utf8').replace('\r\n','\n')
def sql(f):return '\n'.join(l for l in text(f).splitlines() if not l.startswith('\\'))
def parsed(f):return [s.stmt for s in parse_sql(sql(f)) if not isinstance(s.stmt,ast.TransactionStmt)]
ctor=parsed(files[0]);duplicate=parsed(files[1])+parsed(files[2]);render=lambda s:RawStream()(s)
assert len(duplicate)==11
for s in duplicate:assert [render(t) for t in ctor].count(render(s))==1
tables=[s for s in duplicate if isinstance(s,ast.CreateStmt)]
assert len(tables)==8
tnames=[s.relation.relname for s in tables]
assert len(set(tnames))==8
def objname(a,b,label):
 assert a.isascii() and b.isascii();na,nb=len(a),len(b);avail=63-len(label)-2
 while na+nb>avail:
  if na>nb:na-=1
  else:nb-=1
 return a[:na]+'_'+b[:nb]+'_'+label
fks=[];refs=[]
def foreign(c,table,col=None):
 cols=[col] if col else [x.sval for x in c.fk_attrs]
 assert c.pk_attrs and c.pktable.schemaname in ['public','private'] and not c.fk_del_set_cols and c.initially_valid and not c.skip_validation
 actions={'a':'noaction','r':'restrict','c':'cascade','n':'setnull','d':'setdefault'}
 funcs=['pg_catalog.RI_FKey_check_ins','pg_catalog.RI_FKey_check_upd','pg_catalog.RI_FKey_'+actions[c.fk_upd_action]+'_upd','pg_catalog.RI_FKey_'+actions[c.fk_del_action]+'_del']
 fks.append(dict(table=table,name=c.conname or objname(table,'_'.join(cols),'fkey'),columns=cols,refSchema=c.pktable.schemaname,refTable=c.pktable.relname,refColumns=[x.sval for x in c.pk_attrs],match=c.fk_matchtype,update=c.fk_upd_action,delete=c.fk_del_action,deleteColumns=None,deferrable=c.deferrable,deferred=c.initdeferred,validated=True,local=True,inheritanceCount=0,noInherit=True,parent=0,triggerFunctions=sorted(funcs),fourEnabledInternalTriggers=True))
for orig in tables:
 t=parse_sql(render(orig))[0].stmt;t.relation.schemaname='pg_temp';t.relation.relpersistence='t';new=[]
 assert not t.inhRelations and not t.partbound and not t.partspec and not t.ofTypename and not t.options and not t.tablespacename
 for e in t.tableElts:
  if isinstance(e,ast.ColumnDef):
   kept=[]
   for c in e.constraints or ():
    if c.contype.name=='CONSTR_FOREIGN':foreign(c,t.relation.relname,e.colname)
    else:kept.append(c)
   e.constraints=tuple(kept);new.append(e)
  elif isinstance(e,ast.Constraint) and e.contype.name=='CONSTR_FOREIGN':foreign(e,t.relation.relname)
  else:new.append(e)
 t.tableElts=tuple(new);refs.append(render(t)+';')
for s in duplicate:
 if isinstance(s,ast.AlterTableStmt):
  assert s.relation.relname in tnames
  for c in s.cmds:assert isinstance(c.def_,ast.Constraint) and c.def_.contype.name=='CONSTR_FOREIGN';foreign(c.def_,s.relation.relname)
assert len({(f['table'],f['name']) for f in fks})==len(fks)
source_pins={f:dict(sha256=sha(text(f).encode()),bytes=len(text(f).encode())) for f in [files[0],files[1],files[2],files[5]]}
security=text(files[5]);oldarr="array['weekly_source_pay_decisions','weekly_source_pay_decision_requests',\n  'weekly_source_pay_decision_preview_assemblies','weekly_source_pay_decision_preview_quotes','weekly_source_pay_decision_reviews',\n  'weekly_source_pay_decision_members','weekly_source_pay_decision_bindings','weekly_source_pay_decision_unprotect_preparations',\n  'weekly_source_pay_decision_acceptance_applications','weekly_source_pay_decision_native_adoptions','weekly_source_pay_decision_publication_adoptions']"
newnames=['weekly_source_pay_decision_acceptance_applications','weekly_source_pay_decision_native_adoptions','weekly_source_pay_decision_publication_adoptions']
assert security.count(oldarr)==1
security3=security.replace(oldarr,"array["+','.join("'"+x+"'" for x in newnames)+"]")
model=dict(version='HEALTH_SOURCE_EXACT_ONCE_V1',constructor=files[0],consumers=[files[1],files[2],files[5]],pins=source_pins,tables=tnames,newTables=newnames,referenceSql='\n'.join(refs),foreignKeys=fks,security3Source=security3,duplicateStatements=[dict(sql=render(s),constructorOccurrences=1) for s in duplicate])
(HERE/'MODEL.json').write_text(json.dumps(model,indent=2)+'\n',encoding='utf8')
(HERE/'PARENTS.json').write_text(json.dumps(pins,indent=2)+'\n',encoding='utf8')
assert git('rev-parse','HEAD').decode().strip()==HEAD
print(json.dumps(dict(parent=HEAD,tables=8,duplicateStatements=11,foreignKeys=len(fks),parents=len(pins))))

"""PG17 grammar/whole AST/source assertions ONLY; no database or native-byte oracle."""
from pathlib import Path
import sys,json,re,copy,hashlib
sys.dont_write_bytecode=True
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
H=Path(__file__).resolve().parent
f=json.loads((H/'FORWARD.json').read_text());old=(H/'parent/qt-assertions.sql').read_text();new=(H/'payload/qt-assertions.sql').read_text();cat=(H/'encoder-catalogue.sql').read_text().strip()
sha=lambda b:hashlib.sha256(b).hexdigest()
def sql(s):return json.loads(pglast.parser.parse_sql_json(s))
def pl(s):return json.loads(pglast.parser.parse_plpgsql_json(s))
def walk(n):
 if isinstance(n,dict):
  for k,v in n.items():yield k,v;yield from walk(v)
 elif isinstance(n,list):
  for v in n:yield from walk(v)
def clean(n):
 if isinstance(n,dict):return{k:clean(v) for k,v in n.items() if k not in {'location','stmt_location','stmt_len','lineno','stmtid'}}
 if isinstance(n,list):return[clean(v) for v in n]
 return n
def part(s,label):return re.search(r'do \$'+label+r'\$[\s\S]*?end;\$'+label+r'\$;',s).group(0)
def assert_shape(s):
 st=sql(s)['stmts'];assert len(st)==8
 ct=[x['stmt']['CreateTableAsStmt'] for x in st if 'CreateTableAsStmt'in x['stmt']];assert len(ct)==2
 expected=clean(sql("select a.event_sequence,pg_catalog.sha256(pg_catalog.record_send(ROW(a,pg_catalog.current_setting('TimeZone')))) as row_digest from public.audit_events a limit 1500001;")['stmts'][0]['stmt'])
 for phase,c in zip(['before','after'],ct):
  assert c['objtype']=='OBJECT_TABLE' and not c.get('is_select_into',False) and not c.get('if_not_exists',False)
  assert c['into']['rel']['relname']=='hr_contact_qt_audit_'+phase and c['into']['rel']['relpersistence']=='t'
  assert c['into']['onCommit']=='ONCOMMIT_DROP' and not c['into'].get('skipData',False)
  assert clean(c['query'])==expected
 assert part(s,'qt_controls')==part(old,'qt_controls')
 assert s.count(cat)==2 and s.count(") is not true then raise exception 'QT_AUDIT_RECORD_ENCODER_CATALOGUE_UNAVAILABLE'")==2
 assert s.count('pg_catalog.octet_length(row_digest)<>32')==3
 assert s.count('>1500000')==3 and s.count('limit 1500001')==2
 eq=part(s,'qt_audit_equality');old_eq=part(old,'qt_audit_equality')
 expected_eq=old_eq.replace('begin\n','begin\n if (\n'+cat+"\n ) is not true then raise exception 'QT_AUDIT_RECORD_ENCODER_CATALOGUE_UNAVAILABLE';end if;\n",1).replace('<>16','<>32')
 assert eq==expected_eq
 admission=part(s,'qt_audit_admission');expected_admission=part(old,'qt_audit_admission').replace('begin\n','begin\n if (\n'+cat+"\n ) is not true then raise exception 'QT_AUDIT_RECORD_ENCODER_CATALOGUE_UNAVAILABLE';end if;\n",1)
 assert admission==expected_admission
 # Parse all four whole DOs with actual complete bodies and no substitutions.
 for label in['qt_audit_admission','qt_audit_before_bound','qt_controls','qt_audit_equality']:pl(part(s,label))
 assert s.index('QT_AUDIT_WHOLE_TABLE_ROW_BOUND_EXCEEDED')<s.index('insert into public.tms_users')
 condition=re.search(r' if exists\(select event_sequence,row_digest[\s\S]*? then\n',eq).group(0)[4:-6].strip()
 ops=[v for k,v in walk(sql('select '+condition+';')) if k=='SelectStmt' and v.get('op')=='SETOP_EXCEPT']
 assert len(ops)==2 and all(x.get('all') is True for x in ops)
 assert eq.index("set_config('enable_hashagg','off',true)")<eq.index('except all')<eq.index("set_config('enable_hashagg',audit_original_hashagg,true)")
 return st
assert pglast.get_postgresql_version()[0]==17
st=assert_shape(new)
for label in['qt_controls']:
 assert clean(pl(part(new,label)))==clean(pl(part(old,label)))
# Exact inverse of the whole SQL AST, retaining original statement boundaries.
raw=new
for e in reversed([x for x in f['edits'] if x['target']=='qt-assertions.sql']):
 assert raw.count(e['after'])==e.get('occurrences',1)
 raw=raw.replace(e['after'],e['before'])
assert raw==old and clean(sql(raw))==clean(sql(old))
cat_ast=sql(cat);assert len(cat_ast['stmts'])==1
for token in["audit_row_type.typsend=pg_catalog.to_regprocedure('pg_catalog.record_send(record)')","audit_row_type.typrelid=r.oid","audit_row_type.oid=r.reltype","audit_row_type.typisdefined","audit_row_type.typtype='c'","audit_row_type.typbasetype=0","server_version_num')='170011'","server_encoding')='UTF8'","client_encoding')='UTF8'","snd.prosrc=e.send_symbol","outp.prosrc=e.out_symbol","p.prosrc=e.source_symbol","snd.provolatile=e.send_vol","outp.provolatile=e.out_vol","p.provolatile=e.vol","has_function_privilege(current_user,snd.oid,'EXECUTE')"]:assert token in cat
ctes=cat_ast['stmts'][0]['stmt']['SelectStmt']['withClause']['ctes'];lens={c['CommonTableExpr']['ctename']:len(c['CommonTableExpr']['ctequery']['SelectStmt']['valuesLists']) for c in ctes if c['CommonTableExpr']['ctename'].startswith('expected_')}
assert lens=={'expected_columns':16,'expected_types':6,'expected_functions':3}
# Actual AST mutations must be refused by the same complete shape checker.
mutations=[
 ('omit whole audit row',"ROW(a,pg_catalog.current_setting('TimeZone'))","ROW(a.id,pg_catalog.current_setting('TimeZone'))"),
 ('omit context',"ROW(a,pg_catalog.current_setting('TimeZone'))","a"),
 ('timezone guessed',"pg_catalog.current_setting('TimeZone')","'UTC'"),
 ('wrong hash',"pg_catalog.sha256(pg_catalog.record_send(","pg_catalog.sha512(pg_catalog.record_send("),
 ('wrong send schema',"pg_catalog.record_send(ROW","public.record_send(ROW"),
 ('sample instead of cap',"limit 1500001","limit 1000"),
 ('extra predicate',"from public.audit_events a limit","from public.audit_events a where a.id is not null limit"),
 ('column reduced',"select a.event_sequence,pg_catalog.sha256","select a.id,pg_catalog.sha256"),
 ('deduplicate',"select a.event_sequence,pg_catalog.sha256","select distinct a.event_sequence,pg_catalog.sha256"),
 ('drop duplicate bag',"except all select","except select"),
 ('hash length weakened',"octet_length(row_digest)<>32","octet_length(row_digest)<>16"),
 ('cap widened',">1500000",">1500001"),
 ('hashagg guard removed',"set_config('enable_hashagg','off',true)","set_config('enable_hashagg','on',true)"),
 ('unknown failure accepted',") is not true then raise exception 'QT_AUDIT_RECORD_ENCODER_CATALOGUE_UNAVAILABLE'", ") is false then raise exception 'QT_AUDIT_RECORD_ENCODER_CATALOGUE_UNAVAILABLE'"),
 ('composite hook changed',"audit_row_type.typsend=pg_catalog.to_regprocedure('pg_catalog.record_send(record)')","audit_row_type.typoutput=pg_catalog.to_regprocedure('pg_catalog.record_out(record)')"),
 ('six type qualifier dropped',"snd.prosrc=e.send_symbol","true"),
 ('column count weakened',"count(*)=16","count(*)>=16"),
 ('original index guard weakened',"audit_order_index.indisunique and", "")]
for name,a,b in mutations:
 assert a in new,name
 try:assert_shape(new.replace(a,b))
 except (AssertionError,ValueError,IndexError,KeyError):pass
 else:raise AssertionError('MUTATION_ACCEPTED '+name)
programmes=json.loads((H/'payload/PROGRAMMES.json').read_text());offline=json.loads((H/'payload/OFFLINE_INPUTS.json').read_text());assert len(sql(programmes['QT_ADMIN_CONTROLS_V1']['sql'])['stmts'])==56
assert len(sql(offline['QT_ADMIN_CONTROLS_V1']['input']['sql'])['stmts'])==69
assert len(sql(offline['QT_ADMIN_CONTROLS_V1']['input']['verification_sql'])['stmts'])==4
old_offline=json.loads((H/'parent/OFFLINE_INPUTS.json').read_text());assert offline['QT_ADMIN_CONTROLS_V1']['receipt']['indices']==old_offline['QT_ADMIN_CONTROLS_V1']['receipt']['indices']
print(json.dumps({'sourceOnly':True,'parser':pglast.__version__,'parserPostgresql':pglast.get_postgresql_version(),'qtStatements':8,'completeDos':4,'programmeStatements':56,'inputStatements':69,'freshStatements':4,'ctasWholeAst':2,'metadataColumnsTypesFunctions':[16,6,3],'rawWholeAstInverse':True,'mutationsRefused':len(mutations),'native':'NOT_RUN','performance':'NOT_PROVED','noSubstitutions':True}))

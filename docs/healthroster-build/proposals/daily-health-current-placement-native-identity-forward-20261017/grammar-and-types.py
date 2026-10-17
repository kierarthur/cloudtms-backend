"""Finite PG17 grammar and exact referenced-column source checks, not native."""
from pathlib import Path
import sys,json,hashlib,re
sys.dont_write_bytecode=True
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
from pglast import ast
BASE=Path(__file__).resolve().parent
sha=lambda b:hashlib.sha256(b).hexdigest()
files=['health/20_current_placement.sqlinc','health/99_acl.sqlinc','admission/ONE_TIME_ADMISSION.sqlinc','verification/mandatory_security.sql','ADDITIVE_REVERSE.sqlinc']
grammar=[]
for f in files:
    b=(BASE/f).read_bytes();s=b.decode()
    grammar.append({'file':f,'bytes':len(b),'sha256':sha(b),'statements':len(pglast.parse_sql(s)),'plpgsqlPrograms':len(pglast.parse_plpgsql(s)),'substitutions':0})
metadata=json.loads((BASE/'reference/040bb/reference/SCHEMA_TYPES.json').read_bytes())
tables={r['schema']+'.'+r['name']:{c['name']:c['type'] for c in r['columns']} for r in metadata['relations']}
ddl_file='reference/040bb/reference/root/supabase/migrations/16102026_0159_nhsp_v6_01_physical_source_review_schema.sql'
ddl=(BASE/ddl_file).read_bytes()
ddl_spans=[]
for name in ['private.timesheet_contract_move_operations','private.timesheet_contract_move_members']:
    matches=list(re.finditer(r'(?ms)^create table '+re.escape(name)+r'\s*\(.*?;',ddl.decode()))
    assert len(matches)==1,name
    match=matches[0];fragment=match.group();parsed=pglast.parse_sql(fragment)
    assert len(parsed)==1 and isinstance(parsed[0].stmt,ast.CreateStmt)
    n=parsed[0].stmt
    tables[name]={c.colname:'.'.join(a.sval for a in c.typeName.names) for c in n.tableElts if isinstance(c,ast.ColumnDef)}
    ddl_spans.append({'relation':name,'start':match.start(),'end':match.end(),'sha256':sha(fragment.encode()),'sourceBytesChanged':0})
assert len(tables)==6
native_file='reference/root/supabase/repeatable/21072026_1235_36_tsfin_load_context_batch.sql'
native_bytes=(BASE/native_file).read_bytes()
native_decl=pglast.parse_sql(native_bytes.decode())[0].stmt
native_body=next(o.arg[0].sval for o in native_decl.options if o.defname=='as')
native_body_ast=pglast.parse_sql(native_body)
assert len(native_body_ast)==1 and isinstance(native_body_ast[0].stmt,ast.SelectStmt)
native_fields={p.name:'.'.join(n.sval for n in p.argType.names) for p in native_decl.parameters if p.mode.name=='FUNC_PARAM_TABLE'}
assert all(n in native_fields for n in ['effective_timesheet_id','out_candidate','out_client_id','out_cur_fin'])
alias={'w':'private.bpay_next_work','v':'private.bpay_next_work_revision','t':'public.timesheets','f':'public.timesheets_financials','x':'private.timesheet_contract_move_operations','o':'private.timesheet_contract_move_operations','current_o':'private.timesheet_contract_move_operations','m':'private.timesheet_contract_move_members'}
def dict_walk(v):
    if isinstance(v,dict):
        yield v
        for x in v.values():yield from dict_walk(x)
    elif isinstance(v,list):
        for x in v:yield from dict_walk(x)
def ast_walk(v):
    if isinstance(v,ast.Node):
        yield v
        for f in v:yield from ast_walk(getattr(v,f))
    elif isinstance(v,(tuple,list)):
        for x in v:yield from ast_walk(x)
raw=(BASE/files[0]).read_bytes();function=pglast.parse_plpgsql(raw.decode())[0]['PLpgSQL_function'];refs=set();queries=0
for d in dict_walk(function):
    if 'PLpgSQL_expr' not in d:continue
    e=d['PLpgSQL_expr'];q=e['query'];mode=e.get('parseMode',0)
    if mode==3:q=q.split(':=',1)[1]
    sql=pglast.parse_sql(('select ' if mode else '')+q);queries+=1
    for n in ast_walk(sql):
        if isinstance(n,ast.ColumnRef) and len(n.fields)==2 and all(isinstance(f,ast.String) for f in n.fields):
            owner,column=[f.sval for f in n.fields]
            if owner in ['native_context','native_checked']:
                assert column in native_fields,(owner,column)
                refs.add(('public.tsfin_load_context_batch RETURN TABLE',column,native_fields[column]))
            if owner in alias:
                relation=alias[owner];assert column in tables[relation],(owner,relation,column)
                refs.add((relation,column,tables[relation][column]))
alias_scopes=[]
for f in files:
    for ordinal,wrapper in enumerate(pglast.parse_plpgsql((BASE/f).read_text())):
        program=wrapper['PLpgSQL_function'];names={}
        for datum in program['datums']:
            for d in datum.values():
                if isinstance(d,dict) and d.get('refname'):names[d['refname']]=True
        collisions=[]
        for d in dict_walk(program):
            if 'PLpgSQL_expr' not in d:continue
            e=d['PLpgSQL_expr'];q=e['query'];mode=e.get('parseMode',0)
            if mode==3:q=q.split(':=',1)[1]
            parsed=pglast.parse_sql(('select ' if mode else '')+q)
            for n in ast_walk(parsed):
                if isinstance(n,(ast.RangeVar,ast.RangeFunction)) and n.alias and n.alias.aliasname in names:
                    collisions.append(n.alias.aliasname)
        assert not collisions,(f,ordinal,collisions)
        alias_scopes.append({'file':f,'programme':ordinal,'rangeAliasDeclarationCollisions':collisions})
assert tables['private.bpay_next_work']['booking_id']=='text'
assert tables['private.timesheet_contract_move_members']['original_booking_id']=='text'
assert tables['private.bpay_next_work']['current_revision_no']=='bigint'
assert tables['private.bpay_next_work_revision']['revision_no']=='bigint'
assert 'primary key' in ddl.decode().lower() and 'id uuid primary key' in ddl.decode().lower()
native_write_types=(ast.InsertStmt,ast.UpdateStmt,ast.DeleteStmt,ast.MergeStmt,ast.CopyStmt)
assert not any(isinstance(n,native_write_types) for n in ast_walk(native_body_ast))
result={'version':'DAILY_CURRENT_PLACEMENT_FINITE_SOURCE_TYPES_V1','grammar':grammar,'aliasScopes':alias_scopes,'actualNativeReturnTable':{'file':native_file,'sha256':sha(native_bytes),'fields':native_fields,'sqlBodyStatements':len(native_body_ast),'sqlBodyDirectWrites':0,'nestedExistingReadDependencies':'UNCHANGED_NOT_NEW_ABI_NOT_NATIVE_PROVED'},'helperSqlExpressions':queries,'sourceContractSha256':metadata['sourceSha256'],'operationsMembersDdl':{'file':ddl_file,'sha256':sha(ddl),'exactCreateTableSpans':ddl_spans,'wholeMigration':'NOT_PARSED_PSQL_INCLUDE_RETAINED'},'relations':list(tables),'referencedColumns':[{'relation':r,'column':c,'type':t} for r,c,t in sorted(refs)],'strictSelector':'Actual Work ID/current revision/current financial; strict cardinality refuses absence or ambiguity','newTupleReads':'Both operation row SELECTs target the original source-declared primary-key id; no singleton/history inference','ResultTemplate':'NOT_PARSED_UNPROVIDED_BANK_RESULT_SLOT_RETAINED','native':'NOT_RUN','catalogueVerification':'NOT_RUN'}
(BASE/'GRAMMAR_TYPES_RESULTS.json').write_bytes((json.dumps(result,indent=2)+'\n').encode())
print(json.dumps({'inputs':len(grammar),'helperSqlExpressions':queries,'relations':len(tables),'referencedColumns':len(refs),'Result':'HELD_NOT_PARSED','native':'NOT_RUN'}))

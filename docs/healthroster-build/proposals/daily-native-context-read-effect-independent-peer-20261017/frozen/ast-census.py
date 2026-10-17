from pathlib import Path
import sys,json,re
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
HERE=Path(__file__).resolve().parent
def walk(x):
    if isinstance(x,dict):
        yield x
        for v in x.values():yield from walk(v)
    elif isinstance(x,list):
        for v in x:yield from walk(v)
def names(a):return '.'.join(n['String']['sval'] for n in a)
def sqltree(q):
    # PL parser expression versus SQL statement modes; no executable rendering.
    if ':=' in q[:100] and not re.match(r'^\s*(select|with|insert|update|delete)\b',q,re.I):
        q=q.split(':=',1)[1]
    try:return json.loads(pglast.parser.parse_sql_json(q))
    except Exception:return json.loads(pglast.parser.parse_sql_json('SELECT '+q))
def census(trees):
    calls=set();rels=set();types=set();ops=set();nodes=set();locks=[];ctes=set();callsites=[]
    for tree in trees:
        for x in walk(tree):
            nodes.update(x.keys())
            if 'FuncCall' in x:
                f=x['FuncCall'];calls.add(names(f['funcname']));callsites.append({'name':names(f['funcname']),'arity':len(f.get('args',[])),'args':f.get('args',[])})
            if 'CommonTableExpr' in x:ctes.add(x['CommonTableExpr']['ctename'])
            if 'RangeVar' in x:
                r=x['RangeVar'];rels.add((r.get('schemaname','')+'.'+r['relname']).lstrip('.'))
            if 'TypeName' in x:types.add(names(x['TypeName']['names']))
            if isinstance(x.get('typeName'),dict) and 'names' in x['typeName']:types.add(names(x['typeName']['names']))
            if 'A_Expr' in x:ops.add((x['A_Expr'].get('kind',''),names(x['A_Expr'].get('name',[]))))
            if 'lockingClause' in x:locks.append(x['lockingClause'])
    return {'calls':sorted(calls),'callSites':callsites,'ctes':sorted(ctes),'intendedPhysicalRelations':sorted(r if '.' in r else 'public.'+r for r in rels if r not in ctes),'relationsIncludingCtes':sorted(rels),'types':sorted(types),'operators':[list(o) for o in sorted(ops)],'nodeKinds':sorted(nodes),'rowLockClauses':locks}
def run():
    selection=json.loads((HERE/'SOURCE_SELECTION.json').read_text(encoding='utf-8'));out=[]
    for r in selection['captures']:
        raw=(HERE/r['capture']).read_text(encoding='utf-8');outer=json.loads(pglast.parser.parse_sql_json(raw));stmt=outer['stmts'][0]['stmt'];queries=[];fail=[];body='';lang=None;options={};plKinds=[];headerOptions=[]
        if 'CreateFunctionStmt' in stmt:
            headerOptions=[o['DefElem'] for o in stmt['CreateFunctionStmt']['options'] if o['DefElem']['defname']!='as']
            opts={o['DefElem']['defname']:o['DefElem']['arg'] for o in stmt['CreateFunctionStmt']['options']}
            options=opts;lang=opts['language']['String']['sval'];body=opts['as']['List']['items'][0]['String']['sval']
            if lang=='sql':queries.append(sqltree(body))
            elif lang=='plpgsql':
                pl=json.loads(pglast.parser.parse_plpgsql_json(raw));plKinds=sorted(set(k for x in walk(pl) for k in x if k.startswith('PLpgSQL_stmt_')))
                for x in walk(pl):
                    if 'PLpgSQL_expr' in x:
                        q=x['PLpgSQL_expr']['query']
                        try:queries.append(sqltree(q))
                        except Exception as e:fail.append({'query':q,'error':str(e)})
            else:raise ValueError('UNADMITTED_LANGUAGE:'+lang)
        else:queries.append(outer)
        out.append({'identity':r['identity'],'source':r['capture'],'language':lang,'options':options,'headerOptionSequence':headerOptions,'plStatementKinds':plKinds,'sqlFragments':len(queries),'unparsedFragments':fail,**census(queries)})
    return {'version':'DAILY_CONTEXT_COMPLETE_HEALTH_GENERAL_AST_CENSUS_V1','parser':pglast.__version__,'postgresql':list(pglast.get_postgresql_version()),'owners':out,'native':'NOT_RUN','nativeResolution':'NOT_PROVED'}
if __name__=='__main__':
    result=run();(HERE/'AST_CENSUS.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps([{'identity':r['identity'],'calls':r['calls'],'relations':r['relationsIncludingCtes'],'unparsed':r['unparsedFragments']}for r in result['owners']],indent=2))

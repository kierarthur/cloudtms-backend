"""PG17 source grammar and PL-record/range-alias census. Never connects."""
import json,pathlib,re,sys
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
HERE=pathlib.Path(__file__).resolve().parent

def walk(n):
    if isinstance(n,dict):
        for k,v in n.items():
            yield k,v
            yield from walk(v)
    elif isinstance(n,list):
        for v in n:yield from walk(v)

def record_aliases(source):
    tree=json.loads(pglast.parser.parse_plpgsql_json(source))
    records=set()
    for key,row in walk(tree):
        if key=='PLpgSQL_rec':records.add(row['refname'].lower())
        if key=='PLpgSQL_var':
            typename=row.get('datatype',{}).get('PLpgSQL_type',{}).get('typname','').lower()
            if '%rowtype' in typename or typename.strip()=='record':records.add(row['refname'].lower())
    checked=0;unparsed=[];collisions=[]
    for key,row in walk(tree):
        if key!='PLpgSQL_expr':continue
        query=row['query'];mode=row.get('parseMode',0)
        if mode==3:
            query=re.sub(r'^\s*[a-z_][\w]*(?:\.[\w]+|\[[^\]]+\])*\s*(?::=|=)\s*','',query,flags=re.I)
        if mode!=0 and not re.match(r'^\s*(select|with)\b',query,re.I):query='SELECT '+query
        try:
            sql=json.loads(pglast.parser.parse_sql_json(query))
        except Exception as error:
            unparsed.append({'query':row['query'],'reason':str(error)});continue
        checked+=1
        for node,alias in walk(sql):
            if node in {'Alias','alias'} and isinstance(alias,dict) and alias.get('aliasname','').lower() in records:
                collisions.append({'recordAndRangeAlias':alias['aliasname'],'query':row['query']})
            if node=='RangeVar' and not alias.get('alias') and alias.get('relname','').lower() in records:
                collisions.append({'recordAndRangeAlias':alias['relname'],'query':row['query']})
    return {'recordVariables':sorted(records),'parsedSqlExpressions':checked,'unparsedSqlExpressions':unparsed,'recordRangeCollisions':collisions}

def run():
    inputs=json.loads((HERE/'OFFLINE_INPUTS.json').read_text());programmes={case:row['input'] for case,row in inputs.items()};rows=[];unproved=[]
    for case,programme in programmes.items():
        outer=pglast.parse_sql(programme['sql'])
        # Use the exact parser location and length to keep the original body text.
        for ordinal,node in enumerate(outer,1):
            source=programme['sql'][node.stmt_location:node.stmt_location+node.stmt_len] if node.stmt_len else programme['sql'][node.stmt_location:]
            kind=type(node.stmt).__name__
            if kind=='DoStmt' or kind=='CreateFunctionStmt' and re.search(r'language\s+plpgsql',source,re.I):
                try:binding=record_aliases(source)
                except Exception as error:
                    unproved.append({'caseId':case,'statement':ordinal,'kind':kind,'status':'NOT_PROVED_CATALOGUE_DEPENDENT','reason':str(error)});continue
                if binding['recordRangeCollisions']:raise ValueError('PL_RECORD_RANGE_ALIAS_CONFLICT: '+json.dumps(binding['recordRangeCollisions']))
                if binding['unparsedSqlExpressions']:raise ValueError('UNPROVED_SQL_BINDING_EXPRESSIONS: '+json.dumps(binding['unparsedSqlExpressions']))
                rows.append({'caseId':case,'statement':ordinal,'kind':kind,'plpgsqlGrammar':'PASS','recordRangeAliasCensus':binding,'nativeCompilation':'NOT_PROVED'})
    return {'version':'CONTACT_QT_PG17_GRAMMAR_RECORD_BINDING_SOURCE_V1','parser':pglast.__version__,'postgresql':list(pglast.get_postgresql_version()),
      'programmes':len(programmes),'outerStatements':{c:len(pglast.parse_sql(p['sql'])) for c,p in programmes.items()},
      'plpgsqlBodies':rows,'unprovedBodies':unproved,'nativeExecution':'NOT_RUN','noBodySubstitution':True}

if __name__=='__main__':
    # Actual Source R1 failure class: record p and SQL range alias p are ambiguous.
    bad="DO $$DECLARE p record;BEGIN SELECT p.pronamespace INTO p FROM pg_proc p LIMIT 1;END$$;"
    good="DO $$DECLARE metadata_record record;BEGIN SELECT proc_row.pronamespace INTO metadata_record FROM pg_proc proc_row LIMIT 1;END$$;"
    assert record_aliases(bad)['recordRangeCollisions']
    assert not record_aliases(good)['recordRangeCollisions']
    result=run();(HERE/'GRAMMAR_RECORD_BINDING.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({'outerStatements':result['outerStatements'],'parsedBodies':len(result['plpgsqlBodies']),'unprovedBodies':result['unprovedBodies'],'native':'NOT_RUN'}))

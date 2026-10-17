"""Finite source inspection only. It never renders or executes a native programme."""
import hashlib
import json
import pathlib
import re
import sys

PARSER = pathlib.Path('D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
sys.path.insert(0, str(PARSER))
import pglast

HERE = pathlib.Path(__file__).resolve().parent
assert str(HERE).replace('\\', '/').startswith('D:/Development/GitHub/.codex-worktrees/healthroster-checklist-native-14102026/')

def require(condition, message):
    if not condition:
        raise ValueError(message)

def norm(text):
    return re.sub(r'\s+', ' ', text).strip().lower()

def nodes(value):
    if isinstance(value, dict):
        for key, child in value.items():
            yield key, child
            yield from nodes(child)
    elif isinstance(value, list):
        for child in value:
            yield from nodes(child)

def expr(value):
    return value['PLpgSQL_expr']['query']

def parsed(source):
    return json.loads(pglast.parser.parse_plpgsql_json(source))

def statements(value, gates=()):
    if isinstance(value, list):
        for child in value:
            yield from statements(child, gates)
    elif isinstance(value, dict):
        for key, child in value.items():
            if key.startswith('PLpgSQL_stmt_'):
                yield key, child, gates
            if key == 'PLpgSQL_stmt_if':
                condition=norm(expr(child['cond']))
                yield from statements(child.get('then_body', []), gates+('TRUE:'+condition,))
                remaining=gates+('FALSE:'+condition,)
                for branch in child.get('elsif_list', []):
                    branch=branch['PLpgSQL_if_elsif']
                    alternative=norm(expr(branch['cond']))
                    yield from statements(branch.get('stmts', []), remaining+('TRUE:'+alternative,))
                    remaining+=('FALSE:'+alternative,)
                yield from statements(child.get('else_body', []), remaining)
            else:
                yield from statements(child, gates)

def queries(value):
    return [v['query'] for k,v in nodes(value) if k == 'PLpgSQL_expr']

def expression_sites(value, lineno=0):
    if isinstance(value,list):
        for child in value:
            yield from expression_sites(child,lineno)
    elif isinstance(value,dict):
        lineno=value.get('lineno',lineno)
        for key,child in value.items():
            if key=='PLpgSQL_expr':
                yield child['query'],lineno
            else:
                yield from expression_sites(child,lineno)

def pure_selects(tree):
    sql=[]
    for kind,value,_ in statements(tree):
        require(kind not in {'PLpgSQL_stmt_dynexecute','PLpgSQL_stmt_commit','PLpgSQL_stmt_rollback'}, 'dynamic or transaction command')
        if kind == 'PLpgSQL_stmt_execsql':
            query=expr(value['sqlstmt'])
            ast=json.loads(pglast.parser.parse_sql_json(query))
            require(len(ast['stmts']) == 1 and 'SelectStmt' in ast['stmts'][0]['stmt'], 'non-SELECT statement')
            require(not any(k in {'InsertStmt','UpdateStmt','DeleteStmt','MergeStmt','IntoClause','LockingClause'} for k,_ in nodes(ast)), 'write or row lock in SELECT')
            sql.append(query)
    return sql

def load():
    return {p.stem:p.read_text() for p in (HERE/'reference/health').glob('*.sqlinc')}

def inspect(sources, relations, policies):
    guard=parsed(sources['timesheet_lifecycle_guard_signature_v1'])
    rows=list(statements(guard))
    pure_selects(guard)
    assignments=[(v,g) for k,v,g in rows if k=='PLpgSQL_stmt_assign']
    hashes=[(v,g) for v,g in assignments if norm(expr(v['expr']))=='v_signature := md5(v_signature_payload::text)']
    require(len(hashes)==1 and not hashes[0][1], 'one unconditional native MD5 assignment required')
    hash_line=hashes[0][0]['lineno']
    for query,lineno in expression_sites(guard):
        if 'p_include_payload' in query.lower():
            # Argument declaration has no expression. Every body use must follow MD5.
            require(lineno>hash_line, 'payload flag affects pre-hash semantics')
    require(not any(v['lineno']>hash_line and re.match(r'v_signature_payload\s*:?=',expr(v['expr']),re.I) for v,_ in assignments), 'post-hash payload mutation')
    logging=[(v,g) for k,v,g in rows if k=='PLpgSQL_stmt_perform' and 'public._temp_diag_log(' in expr(v['expr'])]
    require(len(logging)==1, 'exactly one diagnostic call required')
    require('TRUE:coalesce(p_include_payload, false)' in logging[0][1], 'diagnostic call is not under false-arm gate')
    require('TRUE:coalesce(v_temp_log_enabled, false)' in logging[0][1], 'diagnostic setting gate missing')
    require(not any(k=='PLpgSQL_stmt_perform' and v is not logging[0][0] for k,v,_ in rows), 'new unreviewed PERFORM')
    src_calls=[(v,g) for v,g in assignments if 'private.weekly_source_timesheet_category_v2(' in expr(v['expr'])]
    require(len(src_calls)==1, 'exact Source category call required')
    source_gate=src_calls[0][1][-1]
    require(source_gate=='TRUE:'+norm('v_current_ts.timesheet_id IS NOT NULL AND ( EXISTS(SELECT 1 FROM public.weekly_source_row_timesheet_lineages l WHERE l.family_booking_id=v_current_ts.booking_id) OR EXISTS(SELECT 1 FROM public.weekly_exceptional_pay_target_families f WHERE f.root_family_booking_id=v_current_ts.booking_id) )'), 'Source association guard changed')
    guard_calls=set(re.findall(r'\b(?:private|public)\.[a-z_0-9]+(?=\s*\()', sources['timesheet_lifecycle_guard_signature_v1'].lower()))
    require(guard_calls=={'public.timesheet_lifecycle_guard_signature_v1','private._candidate_feature_enabled_current_v1','private._candidate_signature_component_v1','private.weekly_source_timesheet_category_v2','public._temp_diag_log'}, 'new signature dependency')

    affected=parsed(sources['timesheet_lifecycle_affected_rows_v1'])
    pure_selects(affected)
    require(any('public.timesheet_lifecycle_guard_signature_v1(v_current_ts.timesheet_id, v_week.id, coalesce(v_temp_log_enabled, false))' in norm(q) for q in queries(affected)), 'route diagnostic flag transport changed')
    diag=parsed(sources['_temp_diag_log'])
    require(any('insert into public.audit_events' in norm(q) for q in queries(diag)), 'real diagnostic INSERT missing')

    component=parsed(sources['_candidate_signature_component_v1'])
    component_sql=pure_selects(component)
    require(len(component_sql)==3, 'component heap census changed')
    require({m for q in component_sql for m in re.findall(r'(?:from|join)\s+public\.([a-z_0-9]+)',q.lower())}=={'candidate_submission_workflows','timesheet_evidence','candidate_submission_components'}, 'new component heap dependency')
    component_calls=set(re.findall(r'\b(?:private|public)\.[a-z_0-9]+(?=\s*\()',sources['_candidate_signature_component_v1'].lower()))
    require(component_calls=={'private._candidate_signature_component_v1','private._candidate_feature_enabled_current_v1','private._candidate_record_capabilities_v1'}, 'new component routine dependency')
    require(any(k=='PLpgSQL_condition' and v.get('condname')=='others' for k,v in nodes(component)), 'existing caught capability path removed')
    require(any(norm(q)=="v_capabilities:=jsonb_build_object('unavailable',true,'sqlstate',sqlstate)" for q in queries(component)), 'unavailable capability no longer genuine original')
    feature=parsed(sources['_candidate_feature_enabled_current_v1']) if 'language plpgsql' in sources['_candidate_feature_enabled_current_v1'].lower() else None
    require(feature is None, 'feature helper language changed')
    require('from public.settings_defaults sd' in norm(sources['_candidate_feature_enabled_current_v1']), 'feature settings dependency changed')

    capability=parsed(sources['_candidate_record_capabilities_v1'])
    caps=list(statements(capability))
    early=[(v,g) for k,v,g in caps if k=='PLpgSQL_stmt_raise' and v.get('message')=='CANDIDATE_CONTRACT_WEEK_NOT_FOUND']
    require(len(early)==1, 'exact early Contract-week refusal required')
    require(early[0][1]==('TRUE:v_week.id is null',"TRUE:v_timesheet.timesheet_id is null or v_timesheet.sheet_scope<>'daily'::public.timesheet_scope_enum or v_timesheet.contract_id is null"), 'Office contractless early refusal changed')
    require(any("'P0002'"==q for q in queries(early[0][0])), 'early refusal SQLSTATE changed')
    early_line=early[0][0]['lineno']
    projections=[(v,g) for k,v,g in caps if k=='PLpgSQL_stmt_return' and v.get('expr') and 'private._candidate_daily_read_projection_v1(' in expr(v['expr'])]
    require(len(projections)==1 and 'TRUE:v_daily_identity_count=1' in projections[0][1], 'Candidate-created projection guard changed')
    identity=[q for q in queries(capability) if 'select count(*)::integer,min(identity_row.candidate_id::text)::uuid' in norm(q)]
    require(len(identity)==1 and all(x in norm(identity[0]) for x in ["w.workflow_kind='daily'",'origin.booking_id=v_timesheet.booking_id',"origin.idempotency_key like 'candidate-daily-first:%'","w.creation_identity_json#>>'{request,daily_source,booking_id}'=v_timesheet.booking_id"]), 'Candidate-created identity association changed')
    for kind,value,gates in caps:
        if value.get('lineno',1e9)>=early_line:
            continue
        require(kind not in {'PLpgSQL_stmt_perform','PLpgSQL_stmt_dynexecute'}, 'effect before early refusal')
        if kind=='PLpgSQL_stmt_execsql':
            pure_selects({kind:value})
        for q in queries(value) if kind not in {'PLpgSQL_stmt_block','PLpgSQL_stmt_if'} else []:
            calls=re.findall(r'\b(?:private|public)\.[a-z_0-9]+(?=\s*\()',q.lower())
            require(not calls or (calls==['private._candidate_daily_read_projection_v1'] and 'TRUE:v_daily_identity_count=1' in gates), 'new routine before early refusal')
    require(early_line<min(v['lineno'] for k,v,_ in caps if k=='PLpgSQL_stmt_assign' and 'private._candidate_policy_resolve_v1(' in expr(v['expr'])), 'policy now precedes Office refusal')

    require(len(relations)==15 and all(r['kind']=='r' and r['owner']=='postgres' and not r['view_definition_sha256'] for r in relations), 'source heap/owner identity changed')
    require(len(policies)==15, 'policy census changed')
    for r in relations:
        related=[p for p in policies if p['schema']=='public' and p['table']==r['name']]
        require(len(related)==1, 'policy multiplicity changed')
        p=related[0]
        require(p['name']=='cloudtms_miget_service_owner_all' and p['roles']==['postgres','service_role'] and p['using']=='true' and p['check']=='true' and p['command']=='ALL' and p['permissive']=='PERMISSIVE', 'policy expression or role changed')
    return {'sourceStatus':'CONDITIONAL_EXACT_OFFICE_MANUAL_FIXTURE_ONLY',
      'nativeQualification':False,'nativeInvocation':'NOT_RUN',
      'signatureComputedBeforePayloadFlag':True,'falseArmLoggingUnreachable':True,
      'candidateDownstreamPolicyAndProjectionExcludedOnlyUnderFreshScope':True,
      'sourceBankBranchExcludedOnlyUnderBothFreshAbsenceGuards':True,
      'normalLifecycleRouteUnconditionallyReadOnly':False,
      'heapRelations':len(relations),'sourcePolicies':len(policies),
      'nativeInstalledTransitiveCatalogueAndBuiltinIdentity':'NOT_PROVED'}

def run():
    census=json.loads((HERE/'INITIAL_CENSUS.json').read_text())
    owners=json.loads((HERE/'ACTUAL_OWNER_ORDER.json').read_text())
    sources=load()
    for item in census['selected']:
        raw=(HERE/item['file']).read_bytes()
        require(hashlib.sha256(raw).hexdigest()==item['sha256'], 'complete source pin changed')
    require(len(owners['finals'])==9 and all(x['matchesSelectedCompleteBody'] for x in owners['finals']), 'actual owner order mismatch')
    require(owners['inventory']=={'baseline':25,'migrations':426,'repeatables':1096,'uppercaseSqlIncluded':True}, 'actual release inventory changed')
    result=inspect(sources,json.loads((HERE/'reference/source_relation_metadata.json').read_text()),json.loads((HERE/'reference/source_policy_metadata.json').read_text()))
    grammar=[]
    for name,source in sources.items():
        pglast.parse_sql(source)
        body='NOT_APPLICABLE_LANGUAGE_SQL'
        if re.search(r'language\s+plpgsql',source,re.I):
            try:
                parsed(source)
                body='PG17_PLPGSQL_GRAMMAR_PASS_NOT_CATALOGUE_COMPILATION'
            except Exception as error:
                body='NOT_PROVED_CATALOGUE_DEPENDENT: '+str(error)
        grammar.append({'name':name,'outerGrammar':'PASS','bodyGrammar':body,'nativeBodyCompilation':'NOT_PROVED'})
    result.update({'version':'S5_BOUNDED_SOURCE_CONTROL_FLOW_PROOF_V1','sourceCommit':census['commit'],
      'parser':pglast.__version__,'postgresqlParserVersion':list(pglast.get_postgresql_version()),
      'completeHealthSources':9,'actualOwnerFinals':9,'bankBodiesSelected':0,
      'productSqlChanged':False,'f23AllowanceChanges':0,'grammar':grammar})
    return result

if __name__=='__main__':
    result=run()
    (HERE/'SOURCE_PROOF.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result))

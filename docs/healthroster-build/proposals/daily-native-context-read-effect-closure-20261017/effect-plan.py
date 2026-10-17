"""Finite Health/general source classification. Never executes selected SQL."""
from pathlib import Path
import json,sys
sys.dont_write_bytecode=True
HERE=Path(__file__).resolve().parent
PG_NAMES=set('array_length array_remove btrim char_length concat_ws convert_to count encode jsonb_agg jsonb_array_elements jsonb_array_elements_text jsonb_array_length jsonb_build_array jsonb_build_object jsonb_each_text jsonb_object_keys jsonb_typeof left length lower max now regexp_replace round split_part statement_timestamp string_agg sum to_jsonb unnest upper timezone'.split())
OWNERS={
 'public.tsfin_load_context_batch(uuid[])','public._ctms_correction_policy_envelope_read_v1(uuid)',
 'public._ctms_correction_policy_leg_read_v1(uuid)','public._ctms_import_correction_classify_v1(uuid)',
 'private._contract_settings_effective_core_v1(uuid,uuid,date,text,uuid)','private._timesheet_settings_authority_frozen_v1(uuid)',
 'public.contract_settings_effective_get_v1(uuid,uuid,date,text,uuid)','public.settings_finance_pick(date)',
 'public.timesheet_pdf_reference_sig(uuid)','public.timesheet_pdf_reference_rows(uuid)',
 'private._candidate_manager_authoriser_effective_v2(jsonb,jsonb)','private._candidate_manager_authoriser_policy_normalize_v2(jsonb,text)',
 'private._candidate_manager_authoriser_email_array_v2(jsonb)','private._candidate_manager_authoriser_domain_array_v2(jsonb)',
 'private._candidate_normalize_email(text)','private._candidate_normalize_domain_v1(text)',
 'public.v_timesheets_summary','public.v_timesheets_summary_base'}
FUNCTIONS={n.split('(')[0] for n in OWNERS if '(' in n}
RELATIONS={'public.'+s for s in 'candidates client_hospitals client_settings clients contract_weeks contracts invoices nhsp_shifts pay_batch_candidates pay_batch_items pay_batches settings_defaults settings_finance_windows timesheet_evidence timesheet_validations timesheets timesheets_financials ts_pay_adjustments umbrellas v_timesheets_summary v_timesheets_summary_base v_ts_invoice_precheck'.split()}
TYPES=set('contract_week_status_enum date jsonb pg_catalog.bool pg_catalog.int4 pg_catalog.numeric pg_catalog.timestamptz public.invoice_status_enum public.timesheet_scope_enum submission_mode_enum text timesheet_fin_basis_enum timesheet_qr_status_enum timesheet_scope_enum timesheet_status_enum timestamptz ts_fin_processing_status_enum uuid validation_status_enum'.split())
PL_ALLOWED={'PLpgSQL_stmt_'+s for s in 'assign block execsql if raise return exit fors return_next'.split()}
BOUNDARY_FUNCTIONS={'public._pay_outstanding_components'}
BOUNDARY_VIEWS={'public.v_ts_invoice_precheck'}
def require(value,code):
    if not value:raise ValueError(code)
def classify(census):
    require({r['identity'] for r in census['owners']}==OWNERS,'EXACT_COMPLETE_HEALTH_OWNER_SET_REQUIRED')
    edges=[];pg=set();relations=set();types=set();operators=set();fragments=0
    for r in census['owners']:
        require(not r['unparsedFragments'],'UNPARSED_SQL_FRAGMENT_HELD')
        require(r['language'] in [None,'sql','plpgsql'],'UNREVIEWED_LANGUAGE_HELD')
        require(set(r['plStatementKinds'])<=PL_ALLOWED,'DYNAMIC_OR_EFFECTFUL_PL_STATEMENT_HELD')
        require(not r['rowLockClauses'],'READ_ROW_LOCK_HELD')
        require({k for k in r['nodeKinds'] if k.endswith('Stmt')}<={'SelectStmt','ViewStmt'},'NON_SELECT_SQL_HELD')
        require('IntoClause' not in r['nodeKinds'] and 'intoClause' not in r['nodeKinds'],'SELECT_INTO_RELATION_HELD')
        require(set(r['intendedPhysicalRelations'])<=RELATIONS,'UNREVIEWED_RELATION_HELD')
        require(set(r['types'])<=TYPES,'UNREVIEWED_TYPE_OR_CAST_HELD')
        fragments+=r['sqlFragments'];relations.update(r['intendedPhysicalRelations']);types.update(r['types']);operators.update(tuple(v)for v in r['operators'])
        for name in r['calls']:
            if name in FUNCTIONS:kind='REVIEWED_HEALTH_GENERAL_SOURCE_NATIVE_BINDING_REQUIRED'
            elif name in BOUNDARY_FUNCTIONS:kind='FINANCE_OWNER_EFFECT_CLOSURE_REQUIRED_METADATA_ONLY'
            elif name in ['digest','extensions.digest']:kind='EXACT_PGCRYPTO_IMPLEMENTATION_AND_RESOLUTION_REQUIRED'
            elif name.removeprefix('pg_catalog.') in PG_NAMES and ('.' not in name or name.startswith('pg_catalog.')):
                kind='PG_CATALOG_IMPLEMENTATION_TYPE_AGGREGATE_OPERATOR_METADATA_REQUIRED';pg.add(name.removeprefix('pg_catalog.'))
            else:raise ValueError('UNREVIEWED_CALL_HELD:'+name)
            edges.append({'owner':r['identity'],'call':name,'classification':kind})
    require(BOUNDARY_VIEWS<=relations,'EXACT_FINANCE_VIEW_BOUNDARY_REQUIRED')
    return {'version':'DAILY_NATIVE_CONTEXT_READ_EFFECT_SOURCE_PLAN_V1','healthGeneralOwners':len(OWNERS),'sourceFragments':fragments,
      'edges':edges,'builtinNames':sorted(pg),'extensionCalls':['digest','extensions.digest'],'intendedRelations':sorted(relations),
      'casts':sorted(types),'operators':[list(v)for v in sorted(operators)],
      'financeBoundaries':{'routineName':'public._pay_outstanding_components','sourceCallArity':1,'sourceArgument':'ARRAY[pt.timesheet_id]',
       'candidateSignatures':['public._pay_outstanding_components(uuid[])','public._pay_outstanding_components(uuid[],uuid)'],
       'view':'public.v_ts_invoice_precheck','bodiesInspected':False,'semanticQualification':'NOT_PROVIDED_IN_THIS_LANE'},
      'status':'HEALTH_GENERAL_SOURCE_CLASSIFIED_NATIVE_AND_FINANCE_CLOSURE_HELD',
      'publicWrapper':'public.contract_settings_effective_get_v1 is an additional native-observed possible view edge; current managed summary source directly calls private core. Bind actual view version, never silently equate these.',
      'readAdmission':'Existing active Office read admission remains separate and independent of operation creator; this loader takes no actor and grants no Office permission.',
      'nativeInvocation':'NOT_RUN','nativePurity':'NOT_PROVED','runtimeTargets':[]}
if __name__=='__main__':
    result=classify(json.loads((HERE/'AST_CENSUS.json').read_text(encoding='utf-8')))
    (HERE/'EFFECT_CLOSURE.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({'owners':result['healthGeneralOwners'],'fragments':result['sourceFragments'],'relations':len(result['intendedRelations']),'callEdges':len(result['edges']),'builtinNames':len(result['builtinNames']),'nativePurity':result['nativePurity']}))

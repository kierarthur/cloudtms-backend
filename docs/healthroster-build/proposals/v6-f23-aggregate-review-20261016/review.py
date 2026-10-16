"""Finite read-only aggregate review; no SQL execution or gate admission."""
import hashlib,json,pathlib,re,subprocess,sys
HERE=pathlib.Path(__file__).resolve().parent
ROOT=pathlib.Path(sys.argv[1])
# Saved disk remains authoritative; long native filenames need Win32's
# extended-length spelling on this PC. This is not a Git-object fallback.
ROOT=pathlib.Path('\\\\?\\'+str(ROOT.resolve()))
sys.path.insert(0,r'C:/tmp/healthroster-v6-invoice-parser-20261016')
from pglast import parser
sha=lambda s:hashlib.sha256(s if isinstance(s,bytes) else s.encode()).hexdigest()
discovery=json.loads((HERE/'DISCOVERY.json').read_text(encoding='utf-8'))
contract_path='supabase/release/current-contract.json'
contract_bytes=(ROOT/contract_path).read_bytes()
contract=json.loads(contract_bytes)
schema={(r['schema']+'.'+r['name']+'.'+c['name']):c for r in contract['relations'] for c in r['columns']}
ddl_evidence={}
for ref,file,pattern in [
 ('public.weekly_source_group_rule_assignments.effective_from_week_ending','supabase/migrations/13102026_1133_weekly_source_rule_sets_schema.sql',r'effective_from_week_ending date not null'),
 ('public.weekly_contact_delivery_memberships.send_ordinal','supabase/migrations/15102026_0802_weekly_contact_pause_permits.sql',r'send_ordinal integer not null check\(send_ordinal>=1\)'),
 ('public.weekly_source_self_bill_reports.invoice_date','supabase/migrations/13102026_1344_weekly_source_self_bill_report_schema.sql',r'invoice_date date not null'),
 ('private.weekly_source_manual_reviews.opened_at_utc','supabase/migrations/02102026_1600_weekly_source_manual_review.sql',r'opened_at_utc timestamptz not null default pg_catalog.transaction_timestamp\(\)'),
]:
    text=(ROOT/file).read_text(encoding='utf-8');m=re.search(pattern,text);assert m,ref
    native_type=m.group(0).split()[1];schema[ref]={'name':ref.split('.')[-1],'type':native_type,'proof':'native migration DDL'}
    ddl_evidence[ref]={'path':file,'sha256':sha(text),'line':text[:m.start()].count('\n')+1,'declaration':m.group(0)}

def expressions(statement):
    tokens=parser.scan(statement)
    out=[]
    for i,t in enumerate(tokens):
        name=statement[t.start:t.end+1]
        if name.lower() not in ('min','max','sum') or i+1==len(tokens) or statement[tokens[i+1].start:tokens[i+1].end+1]!='(':continue
        start=tokens[i+1].end+1;depth=1;j=i+2
        while depth:
            word=statement[tokens[j].start:tokens[j].end+1]
            if word=='(':depth+=1
            elif word==')':depth-=1
            j+=1
        end=tokens[j-1].start
        out.append({'aggregate':name.upper(),'input':statement[start:end],
                    'expression':name.upper()+'('+re.sub(r'\s+',' ',statement[start:end])+')',
                    'offset':t.start,'end':tokens[j-1].end+1})
    return out

unique={}
for row in discovery['rows']:
    row['expressions']=expressions(row['statement'])
    for a in row['expressions']:unique.setdefault(a['expression'],len(unique)+1)
assert len(unique)==146,len(unique)
assert sha(json.dumps(list(unique),separators=(',',':')))=='2c8ca2ec88848212e909d43458823b0343f8e9185480a613662864b37bbd3742','EXACT_EXPRESSION_SEQUENCE_CHANGED'
assert sha(json.dumps([[r['file'],r['signature'],r['sourceSha256'],r['statementSha256']] for r in discovery['rows']],separators=(',',':')))=='e687763357ab5fef4462ab4055eb7de4753ec0f3f7c6eed0a4ce28591cba34a2','EXACT_SOURCE_STATEMENT_SET_CHANGED'
# Indices bind only this exact finite expression inventory. Every observed
# occurrence still carries its own complete file/routine/statement hashes.
proofs={}
def add(ids,typ,role,proof,refs=()):
    for i in ids:
        assert i not in proofs,i
        proofs[i]={'inputType':typ,'role':role,'bindingProof':proof,'nativeColumnRefs':list(refs)}

add([1,2],'date','GROUP_DATE_BOUNDARY','s is source_rows/source_rows_base through settings_resolved: economic_target_week is CASE of explicit ::date and natural_week_start; settings_date COALESCE of those dates and London effective date. MIN chooses the earliest date in the native invoice group.', ['public.timesheets.week_ending_date'])
add([3],'text','BLOCKER_PRIORITY','s.canonical_blocker is the CASE/literal-text blocker carried through canonical_sources. MIN is a deterministic blocker-code representative; no row UUID or valuation is selected.')
add([4],'numeric','MONEY_TOTAL','tf is public.timesheets_financials, joined on reviewed member Timesheet/current finance. COALESCE(numeric,0) stays numeric.', ['public.timesheets_financials.total_pay_ex_vat'])
add([5],'numeric','MONEY_TOTAL','tf is the same current native finance binding; COALESCE(numeric,0) stays numeric.', ['public.timesheets_financials.total_charge_ex_vat'])
add([6,7,8,49],'text','NATIVE_GROUP_LABEL','c is public.invoice_operation_chunks through claim_ids/normal_chunks. payload_json ->> yields text; UPPER/COALESCE preserve text. The existing native chunk/group owns selection; this aggregate introduces no UUID selection.', ['public.invoice_operation_chunks.payload_json'])
add([9],'text','HEADER_DISPLAY','cl is public.clients joined by tf.client_id; MAX carries a native Client header display field across members.', ['public.clients.name'])
add([10],'integer','HEADER_TERMS','cl is public.clients joined by current TF client_id; MAX is integer payment terms, not a UUID.', ['public.clients.payment_terms_days'])
add([11],'text','HEADER_RECIPIENT','cl is public.clients, native plan header recipient display/routing text.', ['public.clients.primary_invoice_email'])
add([12],'numeric','MONEY_TOTAL','l is public.invoice_lines joined to the exact credit source invoice in credit_plans.', ['public.invoice_lines.total_pay_ex_vat'])
add([13,42],'text','SEALED_PAYLOAD_REPRESENTATIVE','m is members derived from actual chunk canonical members; vc is valid_chunks derived from public.invoice_operation_chunks. Explicit payload_json::text makes MIN text; result is parsed back to jsonb as an existing native group payload representative, never min(jsonb). Native source-revision/group re-resolution precedes writes.', ['public.invoice_operation_chunks.payload_json'])
add([14],'text','HEADER_DISPLAY','cl is public.clients in header_source, joined by tf.client_id.', ['public.clients.invoice_address'])
add([15],'text','HEADER_RECIPIENT','a is the lateral private._timesheet_settings_authority_frozen_v1(ts.timesheet_id) authority_json. #>> yields text; existing frozen settings remain the owner.')
add([16],'integer','HEADER_TERMS','Same frozen authority a; the #>> extraction is explicitly ::integer before MAX.')
for i,col in [(17,'agency_name'),(18,'agency_logo'),(19,'registered_address'),(20,'company_reg_number'),(21,'bank_name'),(22,'bank_sort_code'),(23,'bank_account_number'),(24,'vat_registration_number')]:
    add([i],'text','HEADER_DISPLAY','sd is public.settings_defaults CROSS JOIN constrained by sd.id=1 in header_source. MAX repeats that one native settings row across joined members.', ['public.settings_defaults.'+col])
for i,field in [(25,'day'),(26,'night'),(27,'sat'),(28,'sun'),(29,'bh'),(30,'pay_ex'),(31,'charge_ex')]:
    add([i],'numeric','MONEY_OR_HOURS_TOTAL','s is segment_entries. segment_'+('hours_'+field if i<30 else field)+' is CASE of validated segment JSON text explicitly ::numeric ELSE 0. Both segment_daily_lines and segment_weekly_lines sum this exact native expression.')
add([32],'numeric','VAT_RATE','s is segment_entries -> source_rows -> vat_policy. v.vat_rate is the numeric return column of existing _invoice_generation_vat_policy_batch, joined by source_member_key and v.valid; it is not the raw JSON value.')
for i,col in [(33,'charge_day'),(34,'charge_night'),(35,'charge_sat'),(36,'charge_sun'),(37,'charge_bh'),(38,'timesheet_version')]:
    add([i],'integer' if i==38 else 'numeric','SAME_TIMESHEET_FINANCE_FIELD','s is segment_entries carrying source_rows_base tf.* from public.timesheets_financials. Groups include the exact physical timesheet_id; segment expansion repeats its current native finance values.', ['public.timesheets_financials.'+col])
for i,col in [(39,'role'),(40,'display_site'),(41,'ward_hint')]:
    add([i],'text','SAME_TIMESHEET_CONTRACT_DISPLAY','s is segment_entries carrying ct.'+col+' from source_rows_base; ct is public.contracts on COALESCE(ts.contract_id,cw.contract_id). Group includes the same physical Timesheet.', ['public.contracts.'+col])
add([43,44,45],'numeric','MONEY_TOTAL','l is line_union of native segment/nonsegment/additional/expense line CTEs. Every charge_ex/pay/rate arm is native numeric; ROUND(numeric,2) and numeric VAT arithmetic stay numeric. Exact existing rounding order is retained.')
for i,col in [(46,'total_charge_ex_vat'),(47,'vat_amount'),(48,'total_inc_vat')]:
    add([i],'numeric','MONEY_TOTAL','l is all_target_lines/inserted_lines carrying actual public.invoice_lines stored numeric totals. Native aggregate is unchanged.', ['public.invoice_lines.'+col])
add([50,146],'integer','NATIVE_SEQUENCE','w/v is public.contract_weeks, exact Contract+week predicate. MAX(additional_seq)+1 allocates the next native additional sequence; this is an ordinal, not an identity UUID.', ['public.contract_weeks.additional_seq'])
add([51],'date','SETTINGS_SCOPE_DATE','gc is public.weekly_source_group_clients, grouped by gc.client_id after the exact valid-date overlap predicate. MIN(valid_from) is used in GREATEST(v_from-6,...) to measure that Client settings scope.', ['public.weekly_source_group_clients.valid_from'])
add([52],'text','IDENTITY_SINGLETON','m is members through all_current, client_id is COALESCE(f.client_id,c.client_id). It is explicitly cast to text before MIN and back to uuid. rollup client_count=count(distinct m.client_id); final blocker requires client_count=1 before valid output.', ['public.timesheets_financials.client_id','public.contracts.client_id'])
add([53],'text','IDENTITY_SINGLETON','m is members through all_current t.contract_id; explicit ::text before MIN then ::uuid. rollup contract_count and final blocker require one Contract.', ['public.timesheets.contract_id'])
add([54],'date','CORRECTION_SCOPE_WEEK','m is members through all_current t.week_ending_date; rollup week_count and final scope checks verify one natural source week.', ['public.timesheets.week_ending_date'])
add([55],'text','CORRECTION_STREAM','m.import_declared chooses envelope->>invoice_stream text or current_invoice_stream derived from text CASE; scalar text on both branches. Native stream mismatch blockers remain.')
add([56],'integer','EXPECTED_CARDINALITY','m.expected_member_count derives from checked envelope JSON text cast ::integer; MAX carries declared expected member count alongside count(distinct expected_member_count) and the existing correction cardinality blockers.')
add([57],'text','CORRECTION_SHAPE','m.correction_shape derives from checked envelope ->> text; shape count is validated by the original final blockers.')
add([58],'text','DECLARED_ROLE_SET','m.expected_member_roles is JSONB from the checked correction envelope, explicitly ::text before MIN then ::jsonb. This is a declared role-set representative; distinct role-set validation remains, not min(jsonb).')
add([59],'bigint','BLOCKER_ORDER','u is WITH ORDINALITY from unnest(mr.blocker_codes), so ordinal is bigint. MIN selects earliest declared blocker priority, not a business UUID.')
add([60],'text','IDENTITY_SINGLETON','CASE count(distinct m.timesheet_id)=1 THEN MIN(m.timesheet_id::text)::uuid END is explicit in scope_summary. m Timesheet comes from members/member_results.', ['public.timesheets.timesheet_id'])
add([61],'text','CORRECTION_IDENTITY_SINGLETON','m.correction_id is the native text correction ID, guarded by CASE count(distinct m.correction_id)=1 THEN MIN(...) END.', ['public.timesheets.correction_id'])
add([62],'integer','REQUEST_ORDER','request_no is x.ordinality::integer in raw_requests. MIN chooses the first request occurrence for duplicate-key results, not a physical entity identity.')
add([63,64],'text','REFERENCE_DISPLAY','x is reference_scope rows projected by _invoice_reference_rows_batch. reference_source/reference_scope originate from text CASE/COALESCE labels, propagated through fixed lateral alias x; this is reference display metadata.')
add([65],'text','DISPLAY_ROW_KEY','r is reference_scope, row_key is its declared text return. MIN selects deterministic row-key ordering/display reference metadata; it is not a uuid cast or a financial row owner.')
add([66,67],'text','REFERENCE_DISPLAY','CASE branches are literal text SEGMENT/DAY/TIMESHEET or EXACT_SEGMENT/EXACT_DAY/WHOLE_TIMESHEET. MIN is scalar label display metadata.')
add([68,69],'numeric','MONEY_TOTAL','r is invoice_line_component_ranked. net_raw is numeric rate*hours/other native line totals; raw_vat is numeric stored line_vat multiplied/divided by numeric net_raw or bigint count. ROUND(...,2) remains numeric. Existing residual rounding line formula is unchanged.', ['public.invoice_lines.total_charge_ex_vat','public.invoice_lines.vat_amount'])
add([70,71,72],'numeric','MONEY_TOTAL','c is invoice_line_components after native numeric rounding/residual allocation. net_amount/vat_amount/gross_amount remain numeric. These sum stored/projection components for exact invoice total reconciliation.')
add([73],'numeric','FACTUAL_UNITS','v is jsonb_each_text value in additional-unit display arithmetic. Validated numeric-looking text is explicitly ::numeric ELSE 0, so SUM input is numeric, not jsonb.')
add([74],'numeric','FACTUAL_MINUTES','r is timesheet_schedule_rows; shift_model->>paid_minutes is text, explicitly ::numeric before SUM.')
add([75,79],'integer','MATCH_PRIORITY','ranked/candidates match_priority is literal 1,2,3 in UNION ALL candidate matches over native nhsp_shifts. MIN and window MIN select winning integer priority, not identity.')
for i,col in [(76,'ref_num'),(77,'external_row_key')]:
    add([i],'text','MATCHED_DISPLAY_SINGLETON','ranked carries ns.'+col+' from nhsp_shifts. CASE count(*)=1 THEN MIN(...) END only exposes the matched reference/external key at singleton winning priority.', ['public.nhsp_shifts.'+col])
add([78],'text','IDENTITY_SINGLETON','ranked.shift_id is ns.id from each exact native match arm. CASE count(*)=1 THEN MIN(ranked.shift_id::text)::uuid END after winning-priority filtering; ambiguity returns NULL and reference_match_count/AMBIGUOUS.', ['public.nhsp_shifts.id'])
for i,col in [(80,'total_charge_ex_vat'),(81,'vat_amount')]:
    add([i],'numeric','MONEY_TOTAL','Unqualified column is from the sole public.invoice_lines VAT-breakdown subquery. COALESCE(numeric,0) stays numeric.', ['public.invoice_lines.'+col])
add([82],'numeric','MONEY_TOTAL','Sole public.invoice_lines VAT-breakdown relation; all three stored money columns are numeric and COALESCE/numeric addition preserves that type.', ['public.invoice_lines.total_inc_vat','public.invoice_lines.total_charge_ex_vat','public.invoice_lines.vat_amount'])
add([83],'text','CONTROLLED_LEGACY_SETTING_SINGLETON','eligible.presentation is movement.source_facts_json->>correction_presentation text. Dense-rank origin=1 is followed by exact movement_count=1 and presentation_count=1 plus closed allowed presentation values before returning it.', ['public.weekly_source_billing_movements.source_facts_json'])
add([84],'date','COVERAGE_BOUNDARY','history.first_week UNION ALL comprises date GREATEST(assignment date+integer,group-client date+integer) and source_cycle.finalisation_week_ending. MIN(first_week)-6 is the earliest coverage date.', ['public.weekly_source_group_rule_assignments.effective_from_week_ending','public.weekly_source_group_clients.valid_from','public.weekly_source_cycles.finalisation_week_ending'])
add([85],'date','OLDEST_PROTECTED_DATE','event is public.weekly_exceptional_pay_family_events joined to actual approval; MIN is earliest WAIT work date after floor.', ['public.weekly_exceptional_pay_family_events.work_date'])
add([86],'bigint','LATEST_FAMILY_EVENT_SEQUENCE','latest is same native weekly_exceptional_pay_family_events relation, correlated on family_id and durable_work_event_id; MAX picks BIGINT ordinal event_sequence, not UUID.', ['public.weekly_exceptional_pay_family_events.event_sequence'])
add([87],'date','LATEST_COMPLETED_WEEK','week is public.contract_weeks joined to contracts; status excludes CANCELLED and week is strictly before native current London date.', ['public.contract_weeks.week_ending_date'])
add([88,100],'bigint','MONEY_TOTAL','m/movement is public.weekly_source_billing_movements, exact revision/client/window or selected movement-ID scope. SUM(bigint) returns numeric; no rate/rounding rewrite.', ['public.weekly_source_billing_movements.invoice_presentation_charge_pence'])
add([89],'numeric','MONEY_TOTAL','m is public.weekly_source_billing_movements. Stored numeric vat_amount * integer 100 remains numeric; SUM remains numeric.', ['public.weekly_source_billing_movements.vat_amount'])
add([90,105],'numeric','VAT_RATE_SINGLETON','m/movement binds actual weekly_source_billing_movements. MIN(vat_rate_pct) scalar numeric is accompanied by rate_count/count(distinct VAT rate) or exact native adjustment authority validation. Inspect per-row cardinality note; no arbitrary movement identity.', ['public.weekly_source_billing_movements.vat_rate_pct'])
add([91],'text','SOURCE_PROFILE_REPRESENTATIVE','m is actual weekly_source_billing_movements. MIN(source_profile_kind) is text provenance classification carried by the rounding movement, not a money amount or UUID. Rate-count guard is separate; no profile-count guard is asserted by this review.', ['public.weekly_source_billing_movements.source_profile_kind'])
add([92,125],'timestamp with time zone','PREVIOUS_NATIVE_CUTOFF','prior/previous is public.weekly_source_cycles, exact same source_group_id and strictly earlier cutoff. MAX obtains latest earlier cutoff boundary, not revision identity.', ['public.weekly_source_cycles.cutoff_at_utc'])
add([93],'timestamp with time zone','PRESENTATION_ORDER','Unqualified created_at_utc is from the single public.weekly_source_billing_movements input grouped by presentation_group. MIN appears only in ORDER BY after fixed-expense ordering.', ['public.weekly_source_billing_movements.created_at_utc'])
add([94,95,96,97,98],'numeric','HOURS_TOTAL','movement is public.weekly_source_billing_movements; each canonical_pay_vector_json #>> hours extraction is explicitly ::numeric before SUM.', ['public.weekly_source_billing_movements.canonical_pay_vector_json'])
for i,col,typ in [(99,'total_pay_ex_vat','numeric'),(101,'calculated_comparison_charge_pence','bigint'),(102,'source_validation_charge_pence','bigint'),(103,'vat_amount','numeric'),(104,'total_inc_vat','numeric')]:
    add([i],typ,'MONEY_TOTAL','movement is public.weekly_source_billing_movements restricted by the exact native selected v_ids. No amount is chosen by MIN/MAX; SUM retains original money owner.', ['public.weekly_source_billing_movements.'+col])
add([106],'text','PRICE_CHECK_SINGLETON','movement.price_check_result is text; CASE count(distinct movement.price_check_result)=1 THEN MIN(...) ELSE NOT_APPLICABLE END.', ['public.weekly_source_billing_movements.price_check_result'])
add([107],'integer','CURRENT_SOURCE_MINUTES','state is current_state: ranked ROW_NUMBER partition by transition.work_event_id, then event_rank=1 and exact candidate/Contract/week predicates. actual_net_minutes is snapshot.actual_net_minutes integer; selected v_event filter makes this one current state.', ['public.weekly_source_final_snapshot_lines.actual_net_minutes'])
add([108],'text','CURRENT_SOURCE_HASH','Same single current_state per work event. snapshot_line_hash and transition_fingerprint are bytea; COALESCE bytea -> pg_catalog.encode(...,hex) text -> MAX text. This is a selected current evidence digest, not max(bytea).', ['public.weekly_source_final_snapshot_lines.snapshot_line_hash','public.weekly_source_state_transitions.transition_fingerprint'])
add([109],'text','IDENTITY_SINGLETON','row_state derives scope distinct work_event + exact active movement owner. MAX(final_revision_id::text) FILTER exact v_event and source_present; count of live_count>1 raises WEEKLY_PROTECTED_AMBIGUOUS_SOURCE_HEAD before returning either UUID/text representative. Source ladder head is separately counted.', ['public.weekly_source_billing_movements.final_revision_id'])
add([110],'integer','CURRENT_SOURCE_MINUTES','Same row_state singleton/ambiguity refusal. source_facts_json->>actual_net_minutes explicitly ::integer.', ['public.weekly_source_billing_movements.source_facts_json'])
add([111],'text','CURRENT_SOURCE_HASH','Same row_state singleton/ambiguity refusal. movement_economic_hash bytea -> encode(hex) text before MAX.', ['public.weekly_source_billing_movements.movement_economic_hash'])
add([112],'integer','NATIVE_EPISODE_SEQUENCE','Sole FROM public.weekly_discrepancy_incidents, exact source group/work event predicate; MAX(episode_number)+1.', ['public.weekly_discrepancy_incidents.episode_number'])
add([113],'integer','NATIVE_REVISION_SEQUENCE','Sole FROM public.weekly_issue_comparison_revisions, exact incident predicate; MAX(revision_number)+1.', ['public.weekly_issue_comparison_revisions.revision_number'])
add([114],'integer','ACCEPTED_SEND_SEQUENCE','x is public.weekly_contact_delivery_memberships filtered exact logical_contact_key+accepted_at_utc IS NOT NULL. MAX send_ordinal is checked against accepted_count and exact last ordinal timestamp re-read.', ['public.weekly_contact_delivery_memberships.send_ordinal'])
add([115],'date','FIRST_EXPECTED_WEEK_SINGLETON','expected is public.contract_weeks, exact Contract, additional_seq=0, nonadjustment, allowed status and work_date within week. SELECT count(*),MIN(week_ending_date) then v_count<>1 refusal; date is not a UUID.', ['public.contract_weeks.week_ending_date'])
add([116],'timestamp with time zone','FIRST_ACCEPTED_CONTACT_TIME','command is public.weekly_message_dispatch_commands joined through actual candidate/manager memberships and state=ACCEPTED; MIN is factual first provider acceptance time.', ['public.weekly_message_dispatch_commands.provider_accepted_at_utc'])
add([117],'date','MISSING_SCOPE_FIRST_WORK_DATE','missing_rows.work_date derives row.work_date from weekly_source_upload_rows; grouped exact source cycle/candidate/client/Contract/week.', ['public.weekly_source_upload_rows.work_date'])
add([118],'timestamp with time zone','MISSING_SCOPE_FIRST_SEEN','missing_rows.created_at_utc derives row.created_at_utc from weekly_source_upload_rows.', ['public.weekly_source_upload_rows.created_at_utc'])
add([119],'text','ROUTE_IDENTITY_REPRESENTATIVE_REQUIRES_REVIEW','missing_rows.route_context comes from weekly_source_office_route_key_v1(cycle,candidate,client,Contract,row.work_date). ->>route_key is text. MIN selects a hex route identity before native grouping; no count(distinct route_key) exists here. The route producer hashes effective manager recipient, which can vary by row work date: this is a concrete cardinality review boundary, not approved as mere display metadata.')
add([120],'timestamp with time zone','INCIDENT_FIRST_SEEN','incident_facts.created_at_utc is incident.created_at_utc from public.weekly_discrepancy_incidents.', ['public.weekly_discrepancy_incidents.created_at_utc'])
add([121],'timestamp with time zone','MANUAL_FIRST_SEEN','manual is manual_facts carrying review.* from private.weekly_source_manual_reviews; opened_at_utc is native timestamptz.', ['private.weekly_source_manual_reviews.opened_at_utc'])
add([122],'timestamp with time zone','MISSING_GROUP_FIRST_SEEN','scope is missing_scopes: first_seen_at_utc=MIN(missing_rows.created_at_utc), native timestamptz; outer MIN finds earliest across grouped scopes.', ['public.weekly_source_upload_rows.created_at_utc'])
add([123],'date','PRIOR_REPORT_INVOICE_DATE','pr is public.weekly_source_self_bill_reports in the existing prior-binding lookup; MAX(invoice_date) is a date boundary.', ['public.weekly_source_self_bill_reports.invoice_date'])
add([124],'timestamp with time zone','LATEST_PROJECTION_PUBLICATION','ep is public.weekly_source_projection_publications joined through actual current upload and resolved source rows. MAX published time is a freshness boundary; original v_event_count separately checks work-event identity.', ['public.weekly_source_projection_publications.published_at_utc'])
add([126],'text','IDENTITY_SINGLETON','binding is public.weekly_source_invoice_line_bindings, CURRENT selected invoice, grouped presentation_line_id. Root finite guard before result refuses count(distinct invoice_line_id)<>1. Explicit text input and uuid cast; review preserves that guard.', ['public.weekly_source_invoice_line_bindings.invoice_line_id'])
add([127,128,129,130],'text','SOURCE_DISPLAY_SINGLETON','source_row is weekly_source_upload_rows LEFT JOIN movement.nhsp_upload_row_id. JSON ->> yields text; CASE count(distinct source_row.id)=1 before field display, else documented fallback/NULL.', ['public.weekly_source_upload_rows.bounded_raw_columns_json'])
add([131],'text','SOURCE_DISPLAY_SINGLETON','Same exact source_row join and CASE count(distinct source_row.id)=1; role_band_source is native text.', ['public.weekly_source_upload_rows.role_band_source'])
add([132],'text','IDENTITY_SINGLETON','movement is weekly_source_billing_movements joined binding.billing_movement_id; CASE count(distinct movement.invoice_timesheet_id)=1 THEN MIN(...::text)::uuid ELSE NULL.', ['public.weekly_source_billing_movements.invoice_timesheet_id'])
add([133],'timestamp with time zone','RENDER_SOURCE_TIMESTAMP','m is public.weekly_source_billing_movements in the actual legacy invoice render metadata query. MAX(created_at_utc) is a source-created display timestamp.', ['public.weekly_source_billing_movements.created_at_utc'])
add([134],'date','MISSING_CHECK_WEEK_SINGLETON','admitted is actual CTE carrying t.* from public.timesheets after actual signed-submission admission. LATERAL MIN(admitted.week_ending_date) has HAVING count(distinct admitted.week_ending_date)=1 and hr.date_local within that week.', ['public.timesheets.week_ending_date'])
add([135],'date','CHECKLIST_LATEST_WEEK','Unqualified week_ending_date is counted/scoped CTE t.week_ending_date from sole native public.timesheets t; MAX is latest factual eligible Timesheet week.', ['public.timesheets.week_ending_date'])
add([136],'integer','FINAL_REVISION_SEQUENCE','revision is public.weekly_source_final_revisions, exact cycle/authority scope/report scope; MAX(revision_number)+1.', ['public.weekly_source_final_revisions.revision_number'])
add([137],'integer','EXPENSE_GENERATION_SEQUENCE','expense is public.weekly_expense_authority_generations, exact work_event_id; MAX(generation)+1.', ['public.weekly_expense_authority_generations.generation'])
add([138],'integer','COMPLETION_SEQUENCE','completion is public.weekly_source_client_cycle_completions, exact cycle+Client; MAX(completion_generation)+1.', ['public.weekly_source_client_cycle_completions.completion_generation'])
add([139,140],'integer','CLOSED_COLUMN_CENSUS','allowed is private._weekly_source_acl_lifecycle_column_contract_v1() RETURNS TABLE(table_name text,column_name text,column_ordinal integer). MIN=1,MAX=count(*),count(distinct ordinal)=count(*) prove contiguous, unique declared column ordinals.')
add([141],'integer','RENDER_SOURCE_ROW_ORDER','last_row is public.weekly_source_upload_rows restricted revision.upload_id; input to MAX is INTEGER source_row_ordinal, then result is explicitly ::bigint+1 for display order after actual report rows. Not an invented row UUID.', ['public.weekly_source_upload_rows.source_row_ordinal'])
add([142,143],'timestamp with time zone','LONDON_BOUNDARY','x.instant VALUES are v_guess timestamptz plus/minus intervals, or generate_series(timestamptz,timestamptz,interval). v_guess/v_result declared timestamptz; MIN/MAX choose actual matching instants across DST overlap or first real instant after gap.')
add([144],'text','RECIPIENT_SCOPE_SINGLETON','route is the native pg_temp.query_email_issues route JSONB; ->>recipient_scope text. CASE count(distinct route->>recipient_scope_key)=1 THEN MIN(...) ELSE CONTRACT_OVERRIDE; native routing labels only.')
add([145],'integer','LEGACY_REMINDER_SEQUENCE','e is public.hr_issue_emails joined q.issue_id. COALESCE(sent_count,0) integer, MAX plus existing EMAIL_REMINDER increment derives native reminder sequence.', ['public.hr_issue_emails.sent_count'])
assert set(proofs)==set(range(1,147)),sorted(set(range(1,147))-set(proofs))

native_evidence={}
for proof in proofs.values():
    for ref in proof['nativeColumnRefs']:
        if ref not in schema:raise AssertionError('NATIVE_COLUMN_MISSING '+ref)
        native_evidence[ref]={'column':schema[ref],'columnObjectSha256':sha(json.dumps(schema[ref],sort_keys=True,separators=(',',':')))}

rows=[]
for row in discovery['rows']:
    source=(ROOT/row['file']).read_text(encoding='utf-8')
    assert sha(source)==row['sourceSha256'],'SOURCE_CHANGED '+row['file']
    entries=[]
    for a in row['expressions']:
        number=unique[a['expression']];p=proofs[number]
        input_type=p['inputType']
        result_type=('numeric' if input_type in ('bigint','numeric') else 'bigint') if a['aggregate']=='SUM' else input_type
        entries.append({**a,'expressionIndex':number,**p,'resultType':result_type,
            'inputSha256':sha(a['input']),
            'sourceContext':row['statement'][max(0,a['offset']-160):min(len(row['statement']),a['end']+220)]})
    # Exact literal bindings are retained per statement, not an alias/type rule.
    bindings=[m.group(0) for m in re.finditer(r'\b(?:from|join)\s+(?:(?:public|private|pg_catalog)\.[a-z_][a-z0-9_]*(?:\([^;]*?\))?|[a-z_][a-z0-9_]*)\s+(?:as\s+)?[a-z_][a-z0-9_]*',row['statement'],re.I)]
    declaration=[line for line in source.splitlines() if re.search(r'\b(?:v_guess|v_local|v_result|v_presentation|v_(?:rate|revision|expense|completion)_number)\s+(?:timestamp|timestamptz|text|integer)',line,re.I)]
    rows.append({k:v for k,v in row.items() if k not in ('expressions','key')}|{'aggregates':entries,'exactStatementBindings':bindings,'relatedDeclarations':declaration})

auxiliary={
'routeProducer':'supabase/repeatable/15092026_1534_weekly_source_read_projections_v1.sql',
'privateManualSchema':'supabase/migrations/02102026_1600_weekly_source_manual_review.sql',
'nativeVatReturn':'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_private_invoice_generation_vat_policy_batch.sql',
'nativeReferenceReturn':'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_private_invoice_reference_rows_batch.sql',
'nativeReferenceBlockerReturn':'supabase/repeatable/08092026_1605_invoice_expense_source_revision_parity_v1.sql',
}
aux=[]
for purpose,file in auxiliary.items():
    p=ROOT/file
    if not p.exists():continue
    s=p.read_text(encoding='utf-8')
    excerpt=s[:s.lower().find('as $function$')] if purpose.startswith('native') else None
    aux.append({'purpose':purpose,'path':file,'sha256':sha(s),**({'exactReturnDeclaration':excerpt} if excerpt else {})})

out={'version':'HEALTHROSTER_FINITE_AGGREGATE_TYPE_REVIEW_V1','qualification':'SOURCE_REVIEW_ONLY_NOT_GATE_ADMISSION','native':'NOT_RUN',
 'reviewedRootCommit':subprocess.check_output(['git','-c','maintenance.auto=false','-c','gc.auto=0','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
 'savedCensusCount':discovery['censusCount'],'currentStatementCount':len(rows),'aggregateOccurrenceCount':sum(len(r['aggregates']) for r in rows),'distinctExpressionCount':len(unique),
 'censusSha256':discovery['censusSha256'],'countCorrespondence':discovery['countChanges'],
 'schema':{'path':contract_path,'sha256':sha(contract_bytes),'columns':native_evidence,'additiveNativeDdl':ddl_evidence},'auxiliarySources':aux,
 'findings':[{'id':'AGG_ROUTE_SINGLETON_NOT_PROVED','status':'ROOT_REVIEW_REQUIRED_NOT_APPROVED_AS_IDENTITY_SELECTION','rows':[26],
 'expressionIndex':119,'fact':'MIN(missing_rows.route_context->>route_key) groups Contract/week while native route producer selects effective manager recipient using each row work_date. No count-distinct refusal/equality proof is present in this owner.',
 'limit':'Scalar TEXT type is proved. Whether valid effective-policy history can vary recipient inside one Contract/week requires owner review; do not classify as harmless display or change routing/monetary code in this task.'}],
 'rows':rows}
(HERE/'AGGREGATE_REVIEW.json').write_text(json.dumps(out,indent=2)+'\n',encoding='utf-8',newline='\n')
(HERE/'EXPRESSION_INDEX.json').write_text(json.dumps([{'index':n,'expression':e,**proofs[n]} for e,n in unique.items()],indent=2)+'\n',encoding='utf-8',newline='\n')
print(json.dumps({k:out[k] for k in ['savedCensusCount','currentStatementCount','aggregateOccurrenceCount','distinctExpressionCount']})+' schema_columns='+str(len(native_evidence)))

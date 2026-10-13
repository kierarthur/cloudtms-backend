// New review artifacts only. Asserts all frozen Money inputs before generation.
import fs from 'node:fs';
import crypto from 'node:crypto';
const directory='docs/healthroster-build/proposals/m1-whole-shift',out=directory+'/generated';
fs.mkdirSync(out,{recursive:true});
const freeze=JSON.parse(fs.readFileSync('docs/healthroster-build/F5_INTEGRATION_MANIFEST.json','utf8'));
for(const row of freeze.files){const bytes=fs.readFileSync(row.file);if(bytes.length!==row.bytes||crypto.createHash('sha256').update(bytes).digest('hex')!==row.sha256)throw new Error('FROZEN_INPUT_CHANGED: '+row.file);}
const inputs=[];
function read(file){const bytes=fs.readFileSync(file);inputs.push({file,bytes:bytes.length,sha256:crypto.createHash('sha256').update(bytes).digest('hex')});return bytes.toString('utf8').replaceAll('\r\n','\n');}
function replace(source,before,after,count=1){if(source.split(before).length-1!==count)throw new Error('ANCHOR_COUNT: '+before.slice(0,80));return source.replaceAll(before,after);}
function get(file,name){const source=read(file),start=source.indexOf('create or replace function '+name+'(');if(start<0)throw new Error(name);return source.slice(start,source.indexOf('$function$;',start)+11);}
let worker=read('broker/src/weekly-source/upload-publication-owner.mjs');
worker="import { assertMagnitContractPricing, assertMagnitWholeShiftResult } from './magnit-contract-price-admission.js';\n"+worker;
worker=replace(worker,"  const mode = upper(policy.weekly_rate_classification_method);",`  const mode = upper(policy.weekly_rate_classification_method);
  try { assertMagnitContractPricing(contract.effective_rule_set?.values, policy); }
  catch (error) { return { valid: false, code: error.code ?? 'WEEKLY_SOURCE_MAGNIT_PRESET_INVALID' }; }`);
worker=replace(worker,'    if (Number(comparison.calculation.paidMinutes)',`    assertMagnitWholeShiftResult(contract.effective_rule_set?.values, comparison.calculation);
    if (Number(comparison.calculation.paidMinutes)`);
fs.writeFileSync(out+'/upload-publication-owner.mjs',worker);
fs.copyFileSync(directory+'/magnit-contract-price-admission.js',out+'/magnit-contract-price-admission.js');
let scope=get('../healthroster-v2-backend-13102026/supabase/repeatable/13102026_1134_weekly_source_rule_sets_v1.sql','private._weekly_source_rule_group_context_v1');
scope=replace(scope,`    if p_values is not null and (v_policy->>'source_fixed_expenses_enabled')::boolean then
      raise exception 'WEEKLY_SOURCE_RULE_R7' using errcode='22023';
    end if;`,`    if p_values is not null and (v_policy->>'source_fixed_expenses_enabled')::boolean then
      raise exception 'WEEKLY_SOURCE_RULE_R7' using errcode='22023';
    end if;
    perform private.weekly_source_magnit_contract_price_assert_v1(p_values,v_policy);`);
scope=replace(scope,`      if p_values is not null and coalesce((v_contract_policy->>'source_fixed_expenses_enabled')::boolean,false) then
        raise exception 'WEEKLY_SOURCE_RULE_R7' using errcode='22023'; end if;`,`      if p_values is not null and coalesce((v_contract_policy->>'source_fixed_expenses_enabled')::boolean,false) then
        raise exception 'WEEKLY_SOURCE_RULE_R7' using errcode='22023'; end if;
      perform private.weekly_source_magnit_contract_price_assert_v1(p_values,v_contract_policy);`);
let projection=get('supabase/repeatable/13102026_1432_weekly_source_invoice_money_publication_v1.sql','public.weekly_source_projection_rows_apply_atomic_v1');
projection=replace(projection,"      v_invoice_rules:=private.weekly_source_effective_rule_set_v1(v_client_id,v_cycle.id)->'values';",`      v_invoice_rules:=private.weekly_source_effective_rule_set_v1(v_client_id,v_cycle.id)->'values';
      perform private.weekly_source_magnit_contract_price_assert_v1(v_invoice_rules,v_effective_policy);`);
projection=replace(projection,'      v_contract_rate_fingerprint:=private.weekly_source_sha256_jsonb_v1(',`      perform private.weekly_source_magnit_bucket_assert_v1(v_invoice_rules,v_economic);
      v_contract_rate_fingerprint:=private.weekly_source_sha256_jsonb_v1(`);
let economic=get('supabase/repeatable/15092026_1534_weekly_source_finalisation_v1.sql','private.weekly_source_finalisation_economic_assert_v1');
economic=replace(economic,'  v_expected_pay:=(pg_catalog.round(pg_catalog.round(',`  perform private.weekly_source_magnit_bucket_assert_v1(
    private.weekly_source_effective_rule_set_v1(v_resolution.client_id,p_source_cycle_id)->'values',
    v_economic.canonical_result_json);
  v_expected_pay:=(pg_catalog.round(pg_catalog.round(`);
fs.writeFileSync(out+'/COMPLETE_SOURCE_OWNERS.sqlinc','-- REVIEW ONLY: helpers from PRIVATE_ADMISSION_OWNER.sqlinc must precede these definitions.\n'+[scope,projection,economic].join('\n\n')+'\n');
const artifacts=fs.readdirSync(out).filter(file=>file!=='SOURCE_MANIFEST.json').sort().map(file=>{const bytes=fs.readFileSync(out+'/'+file);return{file:out+'/'+file,bytes:bytes.length,sha256:crypto.createHash('sha256').update(bytes).digest('hex')}});
fs.writeFileSync(out+'/SOURCE_MANIFEST.json',JSON.stringify({contract:'HEALTHROSTER_M1_SOURCE_PROPOSAL_V1',frozen28_unchanged:true,source_inputs:inputs,artifacts,
  existing_sp20_guard:'Unchanged F1 R7 refusal; actual published installed/joined expense proof remains root gate.',
  native_calculator_changed:false,new_tables:0,new_service_rpcs:0,hosted_compile:false,financial_acceptance:false},null,2)+'\n');
process.stdout.write(JSON.stringify({generated:true,frozen28_unchanged:true,artifacts})+'\n');

import fs from 'node:fs';import path from 'node:path';import {fileURLToPath} from 'node:url';
const root=path.dirname(fileURLToPath(import.meta.url));const read=f=>fs.readFileSync(path.join(root,f),'utf8');
const inputs=JSON.parse(read('SOURCE_INPUTS.json')),parse=JSON.parse(read('SOURCE_PARSE_RESULT.json')),native=JSON.parse(read('NATIVE_SOURCE_CHECK.json'));
if(parse.failures.length||native.failures.length)throw Error('Source checks failed');
const currency=inputs.inputs.map(input=>{
 const actual=fs.readFileSync(path.join(inputs.root,input.file),'utf8').replaceAll('\r\n','\n');
 if(actual!==read(input.reference).replaceAll('\r\n','\n'))throw Error('Current native owner changed: '+input.file);
 return {file:input.file,current_saved_owner:true};
});
const q={database_execution:false,installation:false,provider_calls:false,native:'REQUIRED_NOT_RUN',
 grammar:{command:'python parse-source.py C:/tmp/healthroster-v6-invoice-parser-20261016',parser:parse.parser,sql_files:parse.checks.length,
  fixture_functions:parse.fixture_grammar_checks.reduce((n,x)=>n+x.plpgsql_functions,0),failures:0},
 source_checks:{command:'python native-source-check.py C:/tmp/healthroster-v6-invoice-parser-20261016',rowtype_variables:native.rowtype_checks.length,
  composed_native_grammar:native.composed_current_native_grammar,new_write_scope:native.native_write_scope,
  native_targets_commands_renders_generations_mutated:false},
 input_currency:currency,finite_current_owner_anchors:4,public_or_service_grants_added:false,schema_changes:false,
 required_native:['Actual READ/START enrollment for CHECK_HOURS, manager review and SUBMIT exact native lineage, unchanged clocks and current permission',
  'Device1 true acceptance followed by already-registered unsent device2 native START, consume and result; logical acceptance timestamp/count unchanged by device2',
  'Exact accepted native result replay, altered result conflict, actual membership alteration, loss of current pause/reply/creator permission afterSTART',
  'Original same-command transport retries/AMBIGUOUS device ownership; new publication/generation cannot refill accepted or original attempted logical slot',
  'Genuine pre-install target/global attempt history without fabricated pause capture/acceptance metadata; genuinely unstarted obsolete native render may recompose',
  'Same-work reimport and expected-week/fingerprint changes preserve key; genuinely new incident episode has separate actual key',
  'Exact owner-only private ACL/RLS/catalogue binding, full repeatable replay and fresh rollback restoration; two-device/concurrent result and admission races'],
 unresolved_native_ownership:['MANAGER_MANUAL_RESEND enum has no current actual producer/explicit reviewed ordinal witness; branch refuses instead of inferring a follow-up.',
  'Pre-install manager history lacking captured contact membership cannot be supplied to accepted-membership follow-up timing as invented provenance; original native budgets remain owned.']};
fs.writeFileSync(path.join(root,'QUALIFICATION.json'),JSON.stringify(q,null,2)+'\n');
console.log(JSON.stringify({sql_files:q.grammar.sql_files,fixture_functions:q.grammar.fixture_functions,rowvars:q.source_checks.rowtype_variables,current_owners:currency.length,native:q.native}));

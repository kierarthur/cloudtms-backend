import fs from 'node:fs';import path from 'node:path';import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),file=path.join(dir,'01_certificate.sqlinc');let source=fs.readFileSync(file,'utf8');
const replace=(old,next)=>{if(source.split(old).length!==2)throw Error('SAME_INSTALL_HUNK_NOT_UNIQUE');source=source.replace(old,()=>next);};
replace("array['schema_version','phase','installation','scope','execution','native_comparisons','decision','decision_binding','source_equivalence','qualification_sha256']",
 "array['schema_version','phase','installation','receipt_input_commit','scope','execution','native_comparisons','decision','decision_binding','source_equivalence','qualification_sha256']");
replace("    or installation is distinct from latest or execution->>'release_id' is not distinct from installation->>'release_id'",
 "    or installation is distinct from latest or execution is distinct from installation\n    or coalesce(p_certificate->>'receipt_input_commit','') !~ '^[0-9a-f]{40}$'");
replace("    or recorded.evidence_json->'weekly_source_report_fin_native_comparisons' is distinct from p_certificate->'native_comparisons'",
 "    or jsonb_typeof(recorded.evidence_json->'weekly_source_report_fin_native_comparisons') is distinct from 'array'\n    or exists(select 1 from jsonb_array_elements(p_certificate->'native_comparisons') certificate_case\n      where not exists(select 1 from jsonb_array_elements(recorded.evidence_json->'weekly_source_report_fin_native_comparisons') retained_case\n        where retained_case=certificate_case))");
replace("    execution->>'source_commit',installation->>'source_commit');",
 "    execution->>'source_commit',p_certificate->>'receipt_input_commit');");
const start=source.indexOf('  for row_value in select value from jsonb_array_elements'),end=source.indexOf('\nexception when undefined_function',start);
if(start<0||end<0)throw Error('NATIVE_ASSERTION_BLOCK_MISSING');
source=source.slice(0,start)+"  perform private.weekly_source_report_fin_native_comparisons_assert_v1(p_certificate->'native_comparisons',\n    execution,p_certificate->>'receipt_input_commit');"+source.slice(end);
source=source.replace("  installation jsonb; execution jsonb; scope_value jsonb; decision jsonb; row_value jsonb; paired jsonb;\n  key_value text; case_value text;",()=>"  installation jsonb; execution jsonb; scope_value jsonb; decision jsonb;\n  case_value text;");
const a=source.indexOf("  assertion_keys constant text[]:="),b=source.indexOf('\nbegin',a);
if(a<0||b<0)throw Error('OLD_ASSERTION_KEYS_MISSING');source=source.slice(0,a)+source.slice(b);
// The preceding required_cases declaration formerly ended in a semicolon,
// followed by the removed declaration. Keep its complete native SQL terminator.
fs.writeFileSync(file,source);
console.log('same-install certificate adapted; no root file touched');

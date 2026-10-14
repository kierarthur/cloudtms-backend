import fs from 'node:fs';import path from 'node:path';import {fileURLToPath} from 'node:url';import {execFileSync} from 'node:child_process';
const dir=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(dir,'../../../..'),frozen=path.join(own,'docs/healthroster-build/proposals/report-ab-protected-dispatch'),target=path.join(dir,'forward/scripts');
const source=fs.readFileSync(path.join(frozen,'weekly-source-report-fin-native-collector.mjs'),'utf8');
if(source!==execFileSync('git',['show','c57746f82558dd698d13fe5b2fc43dd18d72af6c:docs/healthroster-build/proposals/report-ab-protected-dispatch/weekly-source-report-fin-native-collector.mjs'],{cwd:own,encoding:'utf8',maxBuffer:8*1024*1024}))throw Error('FROZEN_COLLECTOR_CHANGED');
let output=source;
const edits=[];
const newline=source.includes('\r\n')?'\r\n':'\n',normalize=s=>s.replace(/\r?\n/g,newline);
const replace=(before,after)=>{before=normalize(before);after=normalize(after);if(output.split(before).length!==2)throw Error('COLLECTOR_FORWARD_HUNK_NOT_UNIQUE '+before.slice(0,100));output=output.replace(before,()=>after);edits.push({before,after});};
const start=output.indexOf("const factKeys=['native_candidate_i1'"),end=output.indexOf('const integer=value=>',start);if(start<0||end<=start)throw Error('GATE_SECTION_CHANGED');
replace(output.slice(start,end),"// Four concrete native slot mappers replace the retired generic equality gate.\r\nconst nativeGates=validateReportNativeGateFacts;\r\n");
replace("import {exact,sha256,domainHash,pgJsonbText} from './weekly-source-protected-release-qualification.mjs';",
 "import {exact,sha256,domainHash,pgJsonbText} from './weekly-source-protected-release-qualification.mjs';\r\nimport {validateReportNativeGateFacts,deriveReportCaseGrain,captureReportNativeGateBefore,captureReportNativeGateAfter} from './weekly-source-report-fin-native-mapper.mjs';");
replace(" const gates=nativeGates(trace.native_gate_facts,trace.case_id,{execution:trace.execution,native_request:r,invoice_id:f.invoice.id,variant:trace.variant});for(const gate of Object.values(trace.native_gate_facts??{})){\r\n  if(gate!==null&&!same(rawJson(gate.result_bytes,'REPORT_NATIVE_GATE_FACTS_INVALID').execution,trace.execution))fail('REPORT_NATIVE_GATE_EXECUTION_CHANGED');}",
 " deriveReportCaseGrain(trace.case_id,b,f);\r\n const gates=nativeGates(trace.native_gate_facts,{execution:trace.execution,native_request:r,invoice_id:f.invoice.id,variant:trace.variant,case_id:trace.case_id,\r\n  basis:b,financial:f,backend_pid:before.backend_pid,transaction_id:commandFrame.transaction_id});");
replace(' originalReportBytes,turnBackendPid,captureFrozenDocument,captureNativeGateFacts},variants){',' originalReportBytes,turnBackendPid,captureFrozenDocument,nativeRegressionFrames},variants){');
replace("  ||(captureNativeGateFacts!==undefined&&typeof captureNativeGateFacts!=='function'))fail('REPORT_NATIVE_SESSION_REQUIRED');",
 "  ||(nativeRegressionFrames!==undefined&&(nativeRegressionFrames===null||typeof nativeRegressionFrames!=='object'||Array.isArray(nativeRegressionFrames))))fail('REPORT_NATIVE_SESSION_REQUIRED');");
replace("  if(variants.length===2)await session.execute('savepoint report_native_variant;');",
 "  if(variants.length===2)await session.execute('savepoint report_native_variant;');\r\n  const gateBefore=await captureReportNativeGateBefore({session,nativeRequest,basis,expectedExecution});");
replace("  const facts=captureNativeGateFacts?await captureNativeGateFacts({session,invoiceId,nativeRequest,caseId,variant,expectedExecution}):null;",
 "  const facts=await captureReportNativeGateAfter({session,nativeRequest,basis,expectedExecution,before:gateBefore,nativeRegressionFrames,caseId,variant,invoiceId});");
fs.mkdirSync(target,{recursive:true});fs.writeFileSync(path.join(target,'weekly-source-report-fin-native-collector.mjs'),output);
for(const name of ['weekly-source-protected-release-qualification.mjs','weekly-source-report-fin-ab-evidence.mjs'])fs.copyFileSync(path.join(frozen,name),path.join(target,name));
fs.writeFileSync(path.join(dir,'COLLECTOR_FORWARD_REVERSE.json'),JSON.stringify({schema_version:'REPORT_NATIVE_COLLECTOR_COMPLETE_FORWARD_REVERSE_V1',input_commit:'c57746f82558dd698d13fe5b2fc43dd18d72af6c',
 input_file:'docs/healthroster-build/proposals/report-ab-protected-dispatch/weekly-source-report-fin-native-collector.mjs',output_file:'forward/scripts/weekly-source-report-fin-native-collector.mjs',hunks:edits,
 native_signatures_or_recorder_slots_added:0,recordable_while_unsealed:false},null,2)+'\n');
let reverse=output;for(const edit of [...edits].reverse()){if(reverse.split(edit.after).length!==2)throw Error('REVERSE_HUNK_NOT_UNIQUE');reverse=reverse.replace(edit.after,()=>edit.before);}
if(reverse!==source)throw Error('ENTIRE_COLLECTOR_REVERSE_FAILED');console.log(JSON.stringify({hunks:edits.length,entire_reverse:true}));

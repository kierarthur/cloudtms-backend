'use strict';
const assert=require('node:assert/strict');const test=require('node:test');
const fs=require('node:fs');const path=require('node:path');const {createHash}=require('node:crypto');
const source=()=>fs.readFileSync(path.join(__dirname,'../supabase/repeatable/28082026_2313_banking_pay_modal_candidate_selection_response.sql'),'utf8').replace(/\r\n/g,'\n');
test('one private post-selection formatter preserves the complete previously proved assembly byte-for-byte',()=>{
 const sql=source();const start=sql.indexOf('CREATE OR REPLACE FUNCTION private.pay_workbench_modal_selection_response_finish_v2(');
 assert.ok(start>=0,'shared private formatter is required');
 const end=sql.indexOf('END;\n$function$;',start);assert.ok(end>start);
 const assembly=sql.slice(sql.indexOf('  v_before:=',start),end);
 assert.equal(createHash('sha256').update(assembly).digest('hex'),'4257bd1a57488f4ec6a4be8d2726ae8cd328d76a5765172ffb8883a008dab112');
 const formatter=sql.slice(start,end);
 assert.doesNotMatch(formatter,/public\.pay_workbench_session_set_selected_rows\(|\b(?:UPDATE|INSERT INTO|DELETE FROM)\s+public\./i);
 assert.match(sql,/REVOKE ALL ON FUNCTION private\.pay_workbench_modal_selection_response_finish_v2\([^;]+FROM PUBLIC, anon, authenticated, service_role/);
 const candidate=sql.slice(sql.indexOf('CREATE OR REPLACE FUNCTION private.pay_workbench_modal_candidate_selection_response_v2('),sql.indexOf('CREATE OR REPLACE FUNCTION public.pay_workbench_session_set_candidate_ready_selection_v1('));
 assert.equal((candidate.match(/public\.pay_workbench_session_set_selected_rows\(/g)||[]).length,1);
 assert.equal((candidate.match(/RETURN private\.pay_workbench_modal_selection_response_finish_v2\(/g)||[]).length,1);
});

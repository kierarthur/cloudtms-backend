import fs from 'node:fs';
import test from 'node:test';
import assert from 'node:assert/strict';
import { assertTypedFinancialProjection,assertNextExactNumericTypes,assertRoutineHistoryPoints,assertNoProcessingAdmission,assertSqlWriteBoundaries,assertSpecialConstructs,assertRpcReload,assertFrozenAuthority,assertExecutableSourceBoundaries,verifyBpayNextSourceGates,specialConstructViolations } from '../scripts/bpay-next-source-gates.mjs';

test('F23 exact frozen, writer, containment and controlling RPC source boundaries',()=>{
  assert.equal(verifyBpayNextSourceGates().frozenFiles,7);
});
test('catalog predecessor dollar metadata is data while actual dynamic execution stays refused',()=>{
  const metadata="if (select pg_catalog.pg_get_constraintdef(c.oid) from pg_catalog.pg_constraint c where c.conrelid='private.bpay_next_net_projection'::pg_catalog.regclass and c.conname='bpay_next_projection_case_cash') is distinct from $expected$CHECK (cash_amount=0)$expected$ then raise exception using errcode='23514',message='BPAY_NEXT_PREDECESSOR_MISMATCH'; end if;";
  const wrap=body=>'do $outer$begin '+body+' end$outer$;';
  assert.doesNotThrow(()=>assertSpecialConstructs(wrap(metadata),'source.sql'));
  for(const body of [metadata+"execute $query$select pg_catalog.coalesce(1,2)$query$;",
    metadata.replace('then raise exception',()=>"then execute $query$select 1$query$; raise exception"),
    metadata.replace('$expected$CHECK (cash_amount=0)$expected$',()=>'$expected$CHECK$expected$ || private.some_function() || $expected$tail$expected$'),
    "q := $expected$CHECK (cash_amount=0)$expected$; execute q;"])
    assert.throws(()=>assertSpecialConstructs(wrap(body),'source.sql'),/DYNAMIC_SQL_UNCLASSIFIED/);
});

test('PF09 NEXT column types require exact numeric while prose and private guards remain allowed',()=>{
  for(const type of ['float','float4','float8','real','double precision','pg_catalog.float8']){
    for(const source of [`create table private.bpay_next_amount(id uuid,amount ${type});`,
      `alter table private.bpay_next_amount add column amount ${type};`,
      `alter table private.bpay_next_amount alter column amount type ${type};`,
      `do $$begin alter table private.bpay_next_amount add column amount ${type}; end$$;`]){
      assert.throws(()=>assertNextExactNumericTypes(source,'next.sql'),/BPAY_NEXT_FLOAT_COLUMN_FORBIDDEN/);
    }
  }
  assert.doesNotThrow(()=>assertNextExactNumericTypes('-- real history\ncreate table private.bpay_next_amount(amount numeric(30,6), label text);','next.sql'));
  assert.doesNotThrow(()=>assertNextExactNumericTypes('select p.* into v_position from private.bpay_next_position p where p.work_id=work for update;','next.sql'));
});
test('PF09 transported rows retain explicit fields and exact decimal text, with private locks permitted',()=>{
  const wrap=body=>`create function public.bpay_next_transport_example(a uuid) returns jsonb language plpgsql
    as $body$ declare v_row private.bpay_next_transfer%rowtype; begin ${body} end $body$;`;
  const signature='public.bpay_next_transport_example(uuid)';
  assert.doesNotThrow(()=>assertTypedFinancialProjection(wrap("select t.* into v_row from private.bpay_next_transfer t where t.id=a for update; return jsonb_build_object('cash_amount',v_row.cash_amount::text);"),'source.sql',signature));
  for(const expression of ['to_jsonb(v_row)','to_jsonb((v_row))','to_jsonb(v_row.*)',
    'row_to_json(c)','jsonb_agg(c)','to_jsonb((select c from private.bpay_next_transfer c))',
    "jsonb_build_object('cash_amount',v_row.cash_amount)",
    "jsonb_build_object('cash_amount',(v_row.cash_amount::text)::numeric)"]){
    assert.throws(()=>assertTypedFinancialProjection(wrap('return '+expression+';'),'source.sql',signature),/BPAY_NEXT_(?:OPAQUE_FINANCIAL_ROW|DECIMAL_TEXT_MISSING)/);
  }
  const file='supabase/repeatable/04102026_0580_banking_pay_next_office_status_v1.sql',source=fs.readFileSync(new URL('../'+file,import.meta.url),'utf8');
  const work='private.bpay_next_current_work_page_v1(uuid,date,uuid,integer)';
  assert.doesNotThrow(()=>assertTypedFinancialProjection(source,file,work));
  for(const changed of [source.replace('w.id as work_id,w.candidate_id','w.*'),
    source.replace('r.approved_source_ex_vat::text','r.approved_source_ex_vat'),
    source.replace('w.id as work_id,w.candidate_id','w.id as work_id,w.candidate_id,w.unknown_field'),
    source.replace("w.approval_state='APPROVED'","w.approval_state='approved'")]){
    assert.throws(()=>assertTypedFinancialProjection(changed,file,work),/BPAY_NEXT_OPAQUE_FINANCIAL_ROW/);
  }
  const prepFile='supabase/repeatable/09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql';
  const prep=fs.readFileSync(new URL('../'+prepFile,import.meta.url),'utf8');
  const prepSignature='public.bpay_next_preparation_page_v1(uuid,text,jsonb)';
  const added=prep.replace('false)) as value',"false),'extra_financial',l) as value");
  assert.notEqual(added,prep);
  assert.throws(()=>assertTypedFinancialProjection(added,prepFile,prepSignature),/BPAY_NEXT_TYPED_REPLY_KEYS_CHANGED/);
});
test('PF04 routine history reads require the whole reviewed point clause and finite helper edges',()=>{
  const signature='private.bpay_next_source_book_seed_proof_v1(uuid,text,uuid,uuid,uuid)';
  const wrap=body=>`create function private.bpay_next_source_book_seed_proof_v1(a uuid,b text,c uuid,d uuid,e uuid)
    returns void language plpgsql as $body$ begin ${body} end $body$;`;
  const point='select e.* into strict v_effect from private.bpay_next_financial_effect e where e.id=p_seed_effect_id;';
  assert.doesNotThrow(()=>assertRoutineHistoryPoints(wrap(point),'source.sql',signature));
  assert.doesNotThrow(()=>assertRoutineHistoryPoints(wrap('-- from private.bpay_next_case_event\nnull;'),'source.sql',signature));
  for(const body of [point.replace('e.id=p_seed_effect_id','e.candidate_id=p_seed_effect_id'),
    point.replace('e.id=p_seed_effect_id','e.id=p_seed_effect_id or true'),point+point,
    'select count(*) from private.bpay_next_case_event where candidate_id=a;',
    'select count(*) from only private.bpay_next_case_event where candidate_id=a;',
    'select count(e.id) from private.bpay_next_job j, private.bpay_next_case_event e where e.candidate_id=j.candidate_id;',
    'table private.bpay_next_case_event;',
    "execute 'select * from private.bpay_next_case_event';",
    'perform public.bpay_next_payment_history_page_v1(null);','perform bpay_next_payment_history_page_v1(null);',
    'perform private.hidden_history_reader(a);','perform hidden_history_reader(a);']) {
    assert.throws(()=>assertRoutineHistoryPoints(wrap(body),'source.sql',signature),/BPAY_NEXT_ROUTINE_HISTORY_/);
  }
});
test('F23 source gates reject actual forbidden writes/constructs and missing tail notification',()=>{
  for(const source of ['update private.bpay_next_module_control set active_owner=\'NEXT\';',
    'insert into private.bpay_next_test_database_marker(marker) values(true);',
    'update "private"."bpay_next_module_control" set owner_epoch=2;',
    "select evidence_kind='SIMULATED_TEST';",
    'update private.bpay_next_rail_capability set enabled=true;']){
    assert.throws(()=>assertSqlWriteBoundaries(source,'supabase/repeatable/control.sql'));
  }
  assert.doesNotThrow(()=>assertSqlWriteBoundaries('-- update private.bpay_next_module_control set active_owner=\'NEXT\';\nselect 1;','source.sql'));
  assert.doesNotThrow(()=>assertSqlWriteBoundaries("create function private.example() returns void language plpgsql as $$begin -- update private.bpay_next_module_control set active_owner='NEXT';\n perform 1;end$$;",'source.sql'));
  for(const seed of ["insert into private.bpay_next_rail_capability(rail,enabled) values ('PROVIDER',false);",
    "insert into private.bpay_next_rail_capability(rail,enabled) values ('CSV',true),('EXTERNAL',true),('PROVIDER',false);",
    'update private.bpay_next_rail_capability set enabled=false;'])assert.doesNotThrow(()=>assertSqlWriteBoundaries(seed,'source.sql'));
  for(const seed of ["insert into private.bpay_next_rail_capability(rail,enabled) values ('CSV',true),('PROVIDER',true);",
    "insert into private.bpay_next_rail_capability(rail) values ('PROVIDER');",
    "update private.bpay_next_rail_capability set enabled=true where rail='PROVIDER';",
    'alter table private.bpay_next_rail_capability alter column enabled set default true;',
    'alter table only private.bpay_next_rail_capability alter column enabled set default true;',
    "update private.bpay_next_rail_capability set (enabled,rail)=(true,'PROVIDER');",
    'alter table private.bpay_next_rail_capability alter column enabled set default false OR true;',
    "insert into only private.bpay_next_rail_capability(rail,enabled) values ('PROVIDER',true);",
    "update private.bpay_next_rail_capability set enabled=false OR true where rail='PROVIDER';",
    "update private.bpay_next_rail_capability set rail='PROVIDER' where rail='CSV';"])
    assert.throws(()=>assertSqlWriteBoundaries(seed,'source.sql'));
  assert.throws(()=>assertExecutableSourceBoundaries("db.from('bpay_next_module_control').update({active_owner:'NEXT'});",'broker/src/example.js'));
  for(const name of ['coalesce','nullif','least','greatest']){
    assert.throws(()=>assertSpecialConstructs(`select pg_catalog.${name}(1,2);`,'source.sql'));
    assert.doesNotThrow(()=>assertSpecialConstructs(`/* pg_catalog.${name}(1,2) */ select ${name}(1,2);`,'source.sql'));
  }
  const rpc="create function public.example(p_value jsonb) returns jsonb language sql as $$select p_value;$$; grant execute on function public.example(jsonb) to service_role;";
  assert.throws(()=>assertRpcReload(rpc,'source.sql'));
  assert.throws(()=>assertRpcReload("notify pgrst,'reload schema';"+rpc,'source.sql'));
  assert.doesNotThrow(()=>assertRpcReload(rpc+"notify pgrst,'reload schema';commit;",'source.sql'));
  assert.throws(()=>assertRpcReload(rpc+"notify pgrst,'reload schema';select 1;commit;",'source.sql'));
  for(const source of ["select 'prefix:' || j->>'key';",'do $ declare r private.example%rowtype; begin select * into r from private.example; end $;',
    'select min(x::uuid);',"do language plpgsql $$begin execute 'select pg_catalog.coalesce(1,2)';end$$;",
    'do $$declare "v x" private.example%rowtype;begin select * into "v x" from private.example;end$$;',
    'do $$declare u uuid;begin create temp table unrelated(u bigint);select min(u);end$$;',
    "create function private.example(j text default 'AS $fake$select 1;$fake$') returns text language sql AS $$select pg_catalog.coalesce(j,'');$$;",
    'do $$ declare r private.example%rowtype; begin for x in select e.* from private.example e loop select * into r from private.example;end loop;end$$;'])assert.throws(()=>assertSpecialConstructs(source,'source.sql'));
  for(const source of ["select 'prefix:' || (j->>'key');","select 'prefix:' || format('%s',j->>'key');",
    'do $$ declare r private.example%rowtype; begin select e.* into strict r from private.example e; end $$;',
    "select case when j->>'x' is null then 'prefix:' || (j->>'key') else null end;"])
    assert.doesNotThrow(()=>assertSpecialConstructs(source,'source.sql'));
  const policy={triggers:new Map([['private.example:actual_guard','private.guard']]),tables:new Set(['private.example']),callbacks:new Set(['private.guard']),routines:new Set(['private.guard'])};
  for(const source of ['alter table private.example disable trigger actual_guard;','drop table private.example;',
    'alter function private.guard() security invoker;',
    'do $$begin alter table private.example disable trigger actual_guard;end$$;',
    "do $$begin foreach v_table in array array['example'] loop execute pg_catalog.format('alter table private.%I owner to postgres',v_table);end loop;end$$;"])
    assert.throws(()=>assertFrozenAuthority(source,'source.sql',policy));
  assert.doesNotThrow(()=>assertFrozenAuthority('alter table private.example add column feature text;','source.sql',policy));

  for(const source of [
    "do $$declare q text;begin q := $first$select pg_catalog.coalesce($1,0)$first$;execute q using 1;q := $later$select 1$later$;execute q using 1;end$$;",
    "do $$begin foreach v_table in array array[v_target] loop execute pg_catalog.format('alter table private.%I owner to postgres',v_table);end loop;end$$;",
    "do $$begin foreach v_table in array array['bpay_next_'||'work'] loop execute pg_catalog.format('alter table private.%I owner to postgres',v_table);end loop;end$$;"
    ,"do $$declare q text;begin q := $first$with x as(select pg_catalog.coalesce($1,0)) select * from x$first$;execute q using 1;q := $later$select 1$later$;execute q using 1;end$$;",
    "do $$begin foreach v_table in array array['feature'] loop execute pg_catalog.format('alter table private.%I owner to postgres',v_table);end loop;foreach v_table in array v_targets loop execute pg_catalog.format('alter table private.%I owner to postgres',v_table);end loop;end$$;"
  ])assert.throws(()=>assertSpecialConstructs(source,'source.sql'));
  const settingsPolicy={...policy,triggers:new Map([...policy.triggers,['public.settings_finance_windows:finance_guard','private.guard']])};
  for(const source of ['alter table public.settings_finance_windows rename to other;','alter table public.settings_finance_windows set schema other;','alter schema private rename to other;'])
    assert.throws(()=>assertFrozenAuthority(source,'source.sql',settingsPolicy));
  const old="create function private.example(j jsonb) returns text language sql as $$select 'prefix:' || j->>'old';$$;";
  const exceptions=new Map(specialConstructViolations(old,'old.sql').map(row=>[row.key,1]));
  assert.doesNotThrow(()=>assertSpecialConstructs(old,'successor.sql',exceptions));
  assert.throws(()=>assertSpecialConstructs(old.replace("'old'","'changed'"),'successor.sql',exceptions));
  const history=fs.readFileSync(new URL('../supabase/repeatable/06102026_1206_banking_pay_payment_history_page_v1.sql',import.meta.url),'utf8');
  for(const changed of [history.replace("v_where:='t.id=$8'","v_where:='t.id=pg_catalog.coalesce($1,$1)'"),
    history.replace("into v_upper using", "into v_upper using").replace("v_order:=", "v_text:='pg_catalog.coalesce(t.id,t.id)'; v_order:=")]) {
    assert.notEqual(changed,history);
    assert.throws(()=>assertSpecialConstructs(changed,'source.sql'));
  }
  for(const replacement of ["select 't.id=pg_catalog.coalesce($1,$1)' into v_where;", "v_where = 't.id=pg_catalog.coalesce($1,$1)';"]){
    const changed=history.replace("v_where:='t.id=$8';",()=>replacement);
    assert.notEqual(changed,history);
    assert.throws(()=>assertSpecialConstructs(changed,'source.sql'));
  }
  const before="create function private.example(j jsonb) returns void language plpgsql as $$begin if 'prefix:'||j->>'old'='x' then select 1;end if;end$$;";
  const prior=new Map(specialConstructViolations(before,'old.sql').map(row=>[row.key,1]));
  assert.throws(()=>assertSpecialConstructs(before.replace("'old'","'new'"),'successor.sql',prior));
});

// Rejected vocabulary describes executable processing-admission mechanisms:
// admission tariff/allowance/budget (R/C/Q/W/B/E or renamed), processing/step
// price/quote/cost/fee, processing payer/ancestor graphs and G01/C1 owners.
test('PF05 rejects processing admission prices/allowances/fee ancestry but preserves money and bounds',()=>{
  for(const source of [
    'await rpc.bpay_next_price_processing_step_v1({command_id});',
    "perform private.bpay_next_admission_allowance_v1(p_request->'R');",
    'select step_price from private.bpay_next_processing_fee_payer_graph;',
    'const renamed = request.processingAllowance;',
    'rpc.bpay_next_g01(request);','perform private.bpay_next_c1(p_request);',
    "const endpoint='/rest/v1/rpc/bpay_next_g01';",
    'const renamed=request.processingCreditAllowance;',
    'const theoretical=request.admissionBudgetBytes;',
    "rpc('bpay_next_processing_fee_quote_v1',args);",
    "const field=request['processing_allowance'];",
    'perform private."bpay_next_processing_fee_quote_v1"(p_request);'
  ])assert.throws(()=>assertNoProcessingAdmission(source,'broker/src/bpay-next-example.js'));
  assert.doesNotThrow(()=>assertNoProcessingAdmission("return ARRAY['processing_fee'];",'source.sql',true));
  for(const dimension of ['R','C','Q','W','B','E'])assert.throws(()=>assertNoProcessingAdmission('const granted=request.admissionBudget.'+dimension+';','broker/src/bpay-next-example.js'));
  for(const source of [
    "perform public.weekly_source_c1_publish_v1(p_work); select pay_rate,price from public.rates;",
    'const budgetProfile={pageRows:100,requestBytes:1024,timeoutMs:8000};',
    'const processingBudgetMs=8000;',
    "const marker='G01'; const section='C1';",
    "console.info('processing_budget'); const diagnosticLabel='processing_fee';",
    "const diagnosticLabels=['processing_fee'];",
    "function diagnostic(){return ['processing_fee'];}",
    "const diagnosticLabel=ready?'processing_fee':'other';",
    "perform public.banking_pay_hot_path_budget_apply('NEXT');",
    "// G01(request); request.admissionBudget.R;\nconst price=earningsRate;",
    "const message='processing budget and fees are not admission requirements';",
    'perform private.bpay_next_assert_financial_scope_v1();'
  ])assert.doesNotThrow(()=>assertNoProcessingAdmission(source,'broker/src/bpay-next-example.js'));
});


test('PS08 all-source frozen capture census rejects actual UPDATE/DELETE and preserves legal progress',()=>{
  for(const target of ['private.bpay_next_run_line','private.bpay_next_run_work','"private"."bpay_next_run_line"','bpay_next_run_line','"bpay_next_run_work"']){
    for(const body of ['update '+target+' set work_id=null;','delete from only '+target+' where true;',
      'with x as(select 1) delete from '+target+';', 'update only ('+target+') set id=null;']){
      assert.throws(()=>assertSqlWriteBoundaries(body,'supabase/repeatable/old-owner.sql'),/FROZEN_CAPTURE_MUTATION_FORBIDDEN/);
      assert.throws(()=>assertSqlWriteBoundaries('create function private.example() returns void language plpgsql as $body$begin '+body+' end$body$;','old-owner.sql'),/FROZEN_CAPTURE_MUTATION_FORBIDDEN/);
    }
  }
  for(const source of [
    'insert into private.bpay_next_run_line(id) values(null);',
    'select id from private.bpay_next_run_work for update;',
    'create trigger frozen before update or delete on private.bpay_next_run_line for each row execute function private.guard();',
    'update private.bpay_next_run_worker set status=null;',
    'delete from private.bpay_next_run_line_extension;',
    '-- update private.bpay_next_run_work set id=null;\nselect 1;',
    "select 'delete from private.bpay_next_run_line';",
    'do $body$begin raise exception using message=$msg$update private.bpay_next_run_work$msg$;end$body$;',
    `do $body$begin raise exception using message=$msg$execute 'update private.bpay_next_run_work set id=null'$msg$;end$body$;`,
    "select 'execute ''delete from private.bpay_next_run_line''';"])
    assert.doesNotThrow(()=>assertSqlWriteBoundaries(source,'source.sql'));
});


test('PS08 direct executed SQL is distinct from diagnostic literal metadata',()=>{
  for(const body of ["execute 'update private.bpay_next_run_line set id=null';",
    'execute $sql$delete from bpay_next_run_work$sql$;',
    "execute pg_catalog.format('delete from private.bpay_next_run_line where id=%L',v_id);",
    "execute format('update \"private\".\"bpay_next_run_work\" set id=null');"])
    assert.throws(()=>assertSqlWriteBoundaries('do $body$begin '+body+' end$body$;','source.sql'),/FROZEN_CAPTURE_MUTATION_FORBIDDEN/);
});

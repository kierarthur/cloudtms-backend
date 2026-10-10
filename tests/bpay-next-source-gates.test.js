import fs from 'node:fs';
import test from 'node:test';
import assert from 'node:assert/strict';
import { assertSqlWriteBoundaries,assertSpecialConstructs,assertRpcReload,assertFrozenAuthority,assertExecutableSourceBoundaries,verifyBpayNextSourceGates,specialConstructViolations } from '../scripts/bpay-next-source-gates.mjs';

test('F23 exact frozen, writer, containment and controlling RPC source boundaries',()=>{
  assert.equal(verifyBpayNextSourceGates().frozenFiles,7);
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

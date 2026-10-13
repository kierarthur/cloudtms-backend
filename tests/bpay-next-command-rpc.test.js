import test from 'node:test';
import assert from 'node:assert/strict';
import { createBpayNextFinancialCommandRpc } from '../broker/src/bpay-next-command-rpc.js';
import { readFile } from 'node:fs/promises';
const actor='b8020000-0000-4000-8000-000000000001';
test('financial command transport uses the fixed wrapper and verified actor without retry/buffering',async()=>{
  let calls=0;
  const raw=Response.json({test:'opaque native response'});
  const args={command_id:'b8020000-0000-4000-8000-000000000060',entered_paye_net:'180.00'};
  const signal=AbortSignal.timeout(8000);
  const rpc=createBpayNextFinancialCommandRpc({baseUrl:'https://agency-test.invalid/',
    headers:{Authorization:'Bearer synthetic-only','Content-Type':'application/json',Accept:'application/json'},
    fetchImpl:async(url,options)=>{
      calls++;
      assert.equal(url,'https://agency-test.invalid/rest/v1/rpc/bpay_next_financial_intake_v1');
      assert.deepEqual(JSON.parse(options.body),{p_action:'PAYE_NET',p_args:args,p_actor_user_id:actor});
      assert.equal(options.signal,signal);
      assert.equal(options.redirect,'error');
      const effective=new Request(url,options);
      assert.equal(effective.headers.get('content-type'),'application/json');
      assert.equal(effective.headers.get('X-CloudTMS-Banking-Pay-Route-Class'),'OPERATION_NUDGE');
      return raw;
    }});
  assert.equal(await rpc('PAYE_NET',args,{actorUserId:actor,signal}),raw);
  assert.equal(calls,1);
});
test('financial transport rejects arbitrary RPC actions and credentials in the URL',async()=>{
  assert.throws(()=>createBpayNextFinancialCommandRpc({baseUrl:'https://hidden:value@test.invalid'}),TypeError);
  const rpc=createBpayNextFinancialCommandRpc({baseUrl:'https://test.invalid',fetchImpl:()=>assert.fail('must not fetch')});
  await assert.rejects(rpc('ARBITRARY_SQL',{}, {actorUserId:actor,signal:new AbortController().signal}),TypeError);
});

test('payout-only projection uses the same fixed intake without a fake net value',async()=>{
  let calls=0;
  const args={command_id:'b8020000-0000-4000-8000-000000000060',
    run_worker_id:'b8020000-0000-4000-8000-000000000061',expected_projection_revision:'0'};
  const signal=new AbortController().signal;
  const raw=Response.json({});
  const rpc=createBpayNextFinancialCommandRpc({baseUrl:'https://agency-test.invalid',fetchImpl:async(url,options)=>{
    calls++;
    assert.equal(url,'https://agency-test.invalid/rest/v1/rpc/bpay_next_financial_intake_v1');
    assert.deepEqual(JSON.parse(options.body),{p_action:'CASE_PAYOUT',p_args:args,p_actor_user_id:actor});
    assert.equal(options.signal,signal);
    return raw;
  }});
  assert.equal(await rpc('CASE_PAYOUT',args,{actorUserId:actor,signal}),raw);
  assert.equal(calls,1);
});
test('financial route binds the authenticated broker user before old Workbench dispatch',async()=>{
  const source=await readFile(new URL('../broker/src/index.js',import.meta.url),'utf8');
  const route=source.indexOf("if (req.method === 'POST' && nextPath === '/api/banking/pay/next/command')");
  const guard=source.indexOf('const requiresBankingPayWorkbenchContract =',route);
  assert.ok(route>0 && guard>route);
  assert.match(source.slice(route,guard),/requireOfficeUser: async \(\) => user/);
  assert.match(source.slice(route,guard),/baseUrl: env\.SUPABASE_URL, headers: sbHeaders\(env\)/);
});

test('internal zero receipt uses the existing exact intake transport without a bank payload',async()=>{
  let calls=0;
  const args={command_id:'b8020000-0000-4000-8000-000000000060',
    transfer_id:'b8020000-0000-4000-8000-000000000061'};
  const signal=new AbortController().signal;
  const raw=new Response('not-yet-consumed');
  const rpc=createBpayNextFinancialCommandRpc({baseUrl:'https://agency-test.invalid',fetchImpl:async(url,options)=>{
    calls++;
    assert.equal(url,'https://agency-test.invalid/rest/v1/rpc/bpay_next_financial_intake_v1');
    assert.deepEqual(JSON.parse(options.body),{p_action:'INTERNAL_SETTLE',p_args:args,p_actor_user_id:actor});
    assert.equal(options.signal,signal);
    return raw;
  }});
  assert.equal(await rpc('INTERNAL_SETTLE',args,{actorUserId:actor,signal}),raw);
  assert.equal(raw.bodyUsed,false);assert.equal(calls,1);
});
test('Office expiry uses only the closed financial-intake transport and exact deadline',async()=>{
  let calls=0;
  const args={command_id:'b8020000-0000-4000-8000-000000000060',
    run_id:'b8020000-0000-4000-8000-000000000061',expected_deadline:'2026-10-07T11:00:00.123456Z'};
  const signal=new AbortController().signal;
  const raw=new Response('not-yet-consumed');
  const rpc=createBpayNextFinancialCommandRpc({baseUrl:'https://agency-test.invalid',fetchImpl:async(url,options)=>{
    calls++;
    assert.equal(url,'https://agency-test.invalid/rest/v1/rpc/bpay_next_financial_intake_v1');
    assert.deepEqual(JSON.parse(options.body),{p_action:'PREPARATION_EXPIRE',p_args:args,p_actor_user_id:actor});
    assert.equal(options.signal,signal);
    return raw;
  }});
  assert.equal(await rpc('PREPARATION_EXPIRE',args,{actorUserId:actor,signal}),raw);
  assert.equal(raw.bodyUsed,false);assert.equal(calls,1);
});

test('stored-credit branch uses only its exact fixed wrapper and raw body, headers and AbortSignal',async()=>{
  const args={command_id:'b8020000-0000-4000-8000-000000000060',legacy_case_id:'b8020000-0000-4000-8000-000000000061'};
  const signal=new AbortController().signal;const raw=new Response('not-consumed');let calls=0;
  const rpc=createBpayNextFinancialCommandRpc({baseUrl:'https://agency-test.invalid/',headers:{Authorization:'Bearer synthetic-only'},
    fetchImpl:async(url,options)=>{
      calls++;assert.equal(url,'https://agency-test.invalid/rest/v1/rpc/bpay_next_bind_stored_credit_v1');
      assert.deepEqual(JSON.parse(options.body),{p_actor_user_id:actor,p_command_id:args.command_id,p_legacy_case_id:args.legacy_case_id});
      assert.equal(options.signal,signal);assert.equal(options.redirect,'error');
      assert.equal(new Headers(options.headers).get('X-CloudTMS-Banking-Pay-Route-Class'),'OPERATION_NUDGE');return raw;
    }});
  assert.equal(await rpc('BIND_STORED_CREDIT',args,{actorUserId:actor,signal}),raw);
  assert.equal(raw.bodyUsed,false);assert.equal(calls,1);
  for(const invalid of [{...args,amount:'40'}, {...args,p_actor_user_id:actor}, {...args,rpc:'arbitrary'}, {...args,legacy_case_id:4}]) {
    await assert.rejects(rpc('BIND_STORED_CREDIT',invalid,{actorUserId:actor,signal}),TypeError);
  }
  assert.equal(calls,1);
});

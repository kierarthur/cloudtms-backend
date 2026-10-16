"""Prepare finite compatibility recipes from saved current native owners; no DB."""
import pathlib,json,hashlib,subprocess
root=pathlib.Path(__file__).resolve().parent
native=pathlib.Path(json.loads((root/'SOURCE_INPUTS.json').read_text())['root'])
office=native.parent/'healthroster-v2-office-13102026'
def write(file,text): (root/file).write_text(text,encoding='utf-8',newline='\n')
def recipe(file,target,patches):write(file,json.dumps({'target':target,'patches':patches},indent=2)+'\n')
inputs=json.loads((root/'SOURCE_INPUTS.json').read_text())
for base,file,ref in [(native,'supabase/repeatable/15102026_0802_weekly_contact_pause_v1.sql','pause.sql'),
 (native,'broker/src/weekly-source/contact-pause-office.mjs','contact-pause-office.mjs'),
 (office,'js/weekly-source/contact-pause-contract.js','office-contact-pause-contract.js'),
 (office,'js/weekly-source/contact-pause-controls.js','office-contact-pause-controls.js')]:
 text=(base/file).read_text(encoding='utf-8');write('reference/'+ref,text)
 record={'file':file,'root':str(base),'commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=base,text=True).strip(),
  'sha256':hashlib.sha256((base/file).read_bytes()).hexdigest(),'reference':'reference/'+ref}
 inputs['inputs']=[x for x in inputs['inputs'] if x['reference']!=record['reference']]+[record]
write('SOURCE_INPUTS.json',json.dumps(inputs,indent=2)+'\n')
drain=(root/'reference/drainEmailOutboxOnce.jsinc').read_text()
begin=drain.index('  const claimEmailOutboxReadyBatch = async (takeCount) => {')
end=drain.index('\n  const fetchSentRemittanceRowsForStateRetry',begin)
old=drain[begin:end]
new=old.replace('    const res = await fetch(`${env.SUPABASE_URL}/rest/v1/rpc/email_outbox_claim_ready_batch`, {',
 '''    const claimName = queryContactDeployment ? 'email_outbox_claim_ready_batch_contact_v1' : 'email_outbox_claim_ready_batch';
    const claimArgs = queryContactDeployment ? {p_request:{
      version:'EMAIL_OUTBOX_CONTACT_CLAIM_V1',limit:Math.max(0,Math.min(takeCount,500)),
      attempt_lease_token:attemptLeaseToken,lease_minutes:leaseMinutes,
      worker_id:'cloudtms:email-outbox-drain',...queryContactDeployment
    }} : {p_limit:takeCount,p_attempt_lease_token:attemptLeaseToken,p_lease_minutes:leaseMinutes};
    const res = await fetch(`${env.SUPABASE_URL}/rest/v1/rpc/${claimName}`, {''')
new=new.replace('''      body: JSON.stringify({
        p_limit: takeCount,
        p_attempt_lease_token: attemptLeaseToken,
        p_lease_minutes: leaseMinutes
      })''','''      body: JSON.stringify(claimArgs)''')
data=json.loads((root/'INDEX_DRAIN_PATCH.json').read_text())
data['patches']=[p for p in data['patches'] if p.get('description')!='Native same-kernel service contact claim bridge']
data['patches'].append({'description':'Native same-kernel service contact claim bridge','before':old,'after':new})
write('INDEX_DRAIN_PATCH.json',json.dumps(data,indent=2)+'\n')
pause=(root/'reference/pause.sql').read_text();start=pause.index('create or replace function private.weekly_contact_pause_inflight_v1(')
end=pause.index('$function$;',start)+len('$function$;');old=pause[start:end]
new=old.replace(' select coalesce(jsonb_agg',' select (select coalesce(jsonb_agg',1).replace(' )x;',' )x) || private.weekly_contact_query_legacy_claims_v1(p_candidate,p_client,p_agency,p_environment);')
recipe('ROOT_PAUSE_PATCH.json','supabase/repeatable/15102026_0802_weekly_contact_pause_v1.sql',[{'description':'Retain exact permit summaries; append typed actual pre-upgrade claim facts','before':old,'after':new}])
write('forward/086_pause_inflight.sqlinc','-- Complete final-repeatable successor after083; original0802 bytes remain intact.\n'+new+'\nalter function private.weekly_contact_pause_inflight_v1(uuid,text,uuid,uuid) owner to postgres;\nrevoke all on function private.weekly_contact_pause_inflight_v1(uuid,text,uuid,uuid) from public,anon,authenticated,service_role;\n')
anchor='  for (const item of value.in_flight) {\n'
arm='''    if (item?.kind==='LEGACY_QUERY_CLAIM') {
      if (!exact(item,['kind','mail_outbox_id','query_delivery_id','claim_identity_sha256','claimed_at_utc','lease_expires_at_utc','status','provider_submission_established','can_auto_resend'])
        || !uuid(item.mail_outbox_id) || !uuid(item.query_delivery_id)
        || typeof item.claim_identity_sha256!=='string' || !/^[0-9a-f]{64}$/.test(item.claim_identity_sha256)
        || (item.claimed_at_utc!==null && !instant(item.claimed_at_utc))
        || (item.lease_expires_at_utc!==null && !instant(item.lease_expires_at_utc))
        || item.status!=='CHECKING_ORIGINAL_LEGACY_CLAIM' || item.provider_submission_established!==false || item.can_auto_resend!==false) unavailable();
      continue;
    }
'''
recipe('WORKER_PAUSE_PATCH.json','broker/src/weekly-source/contact-pause-office.mjs',[{'description':'Closed factual legacy claim variant; no fabricated START identity','before':anchor,'after':anchor+arm}])
recipe('OFFICE_CONTRACT_PATCH.json','js/weekly-source/contact-pause-contract.js',[{'description':'Same strict legacy claim union as Worker','before':anchor,'after':anchor+arm}])
controls=(root/'reference/office-contact-pause-controls.js').read_text(encoding='utf-8')
old=next(line for line in controls.splitlines() if 'const inflight=s?.in_flight.length?' in line)
new='''   const inflight=s?.in_flight.length?`<aside role="status"><strong>An earlier message has an outcome being checked.</strong><ul>${s.in_flight.map(item=>item.kind==='LEGACY_QUERY_CLAIM'?'<li>Earlier query claimed; its original send outcome is being checked. It will not be sent again automatically.</li>':`<li>${item.channel==='EMAIL'?'Email':'Worker push'} · ${item.status==='AMBIGUOUS'?'Checking send result':'Started'} · ${escape(new Intl.DateTimeFormat('en-GB',{timeZone:'Europe/London',dateStyle:'short',timeStyle:'medium',hour12:false}).format(new Date(item.started_at_utc)))} (UK)</li>`).join('')}</ul></aside>`:'';'''
recipe('OFFICE_CONTROLS_PATCH.json','js/weekly-source/contact-pause-controls.js',[{'description':'Describe factual old claim separately from actual started permit; no new action','before':old,'after':new}])

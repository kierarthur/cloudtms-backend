function clearInput(s){const q=s.clear_review;return exact(q,['candidate_id','work_id','expected_revision_id','owner_family','owner_id'])&&[q.candidate_id,q.work_id,q.expected_revision_id].every(id)&&['WORK_BUCKET','GROSS'].includes(q.owner_family)&&nullable(q.owner_id,id);}
function clearSignature(row,work,facts,owner){const b=row.original_basis;return {...componentSignature(row,facts.position_basis),contract_id:work.contract_id,
 target_pay_channel:owner.target_pay_channel,target_umbrella_id:owner.target_umbrella_id,target_umbrella_vat_chargeable:owner.target_umbrella_vat_chargeable,
 owner_policy_id:owner.valuation_policy_id,owner_valuation_date:owner.valuation_date,resolution_mode:owner.resolution_mode,
 manual_replacement_rate:owner.manual_replacement_rate,manual_amount_ex_vat:owner.manual_amount_ex_vat,source_amount:b.semantics==='AMOUNT_LED'?b.component_source_ex_vat:null};}
function clearResponse(payload,args,actorId){
 const bad=()=>{throw fault('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');},q=args.resolution.clear_review;
 if(!exact(payload,['ok','kind','context','works','clear_review'])||encoder.encode(JSON.stringify(payload)).length>120000)bad();
 const scope={candidate_id:q.candidate_id,works:payload.works?.map(w=>({work_id:w.work_id,expected_revision_id:w.expected_revision_id,rows:w.rows?.map(r=>({approved_line_id:r.approved_line_id,rate_family:r.rate_family,rate_code:r.rate_code,intent:null}))})),reason:null};
 validateResolutionResponse({ok:payload.ok,kind:payload.kind,context:payload.context,works:payload.works},{resolution:scope},actorId);
 const review=payload.clear_review,root=payload.context.works.find(w=>w.work_id===q.work_id);
 if(!exact(review,['candidate_id','work_id','expected_revision_id','owner_family','owner_id','complete','header','owners','excluded'])||!['candidate_id','work_id','expected_revision_id','owner_family','owner_id'].every(k=>review[k]===q[k])
  ||review.complete!==true||!root||root.expected_revision_id!==q.expected_revision_id||!exact(review.header,['candidate_display_name','candidate_reference'])
  ||!text(review.header.candidate_display_name)||encoder.encode(review.header.candidate_display_name).length>8192||!nullable(review.header.candidate_reference,v=>text(v)&&encoder.encode(v).length<=8192)
  ||!Array.isArray(review.owners)||review.owners.length<1||review.owners.length>100||!Array.isArray(review.excluded)||review.excluded.length>1000)bad();
 const seen=new Set(),identities=new Set(),rootSignatures=[];let roots=0;
 for(const o of review.owners){
  if(!exact(o,['work_id','expected_revision_id','approved_line_id','rate_family','rate_code','owner_family','owner_id','owner_version','linked','signature','amount_ex_vat','amount_unavailable_code','clear_class','client_display_name','timesheet_reference','week_ending_date','target_pay_channel','target_umbrella_id'])
   ||!id(o.owner_id)||!integer(o.owner_version,true)||!identity(o)||typeof o.linked!=='boolean'||!nullable(o.amount_ex_vat,money)||!unavailable(o.amount_unavailable_code)
   ||(o.amount_ex_vat===null)!==(o.amount_unavailable_code!==null)||o.clear_class!==(o.amount_ex_vat===null?'NOT_CLEARABLE':'RESOLVED_AND_CLEARABLE')
   ||!text(o.client_display_name)||encoder.encode(o.client_display_name).length>8192||!nullable(o.timesheet_reference,v=>text(v)&&encoder.encode(v).length<=8192))bad();
  const work=payload.context.works.find(w=>w.work_id===o.work_id),out=payload.works.find(w=>w.work_id===o.work_id),facts=work?.rows.find(r=>identityKey(r)===identityKey(o)),row=out?.rows.find(r=>identityKey(r)===identityKey(o)),native=facts?.decisions.find(d=>d.decision_id===o.owner_id&&d.decision_version===o.owner_version),k=JSON.stringify([o.work_id,identityKey(o)]);
  if(!work||!facts||!row||!native||work.contract_id!==root.contract_id||o.expected_revision_id!==work.expected_revision_id||o.week_ending_date!==work.week_ending_date
   ||o.target_pay_channel!==native.target_pay_channel||o.target_umbrella_id!==native.target_umbrella_id||o.owner_family!==q.owner_family||o.owner_family!==(o.rate_family==='STANDARD'?'WORK_BUCKET':'GROSS')
   ||o.linked!==(o.work_id!==q.work_id)||seen.has(o.owner_id)||identities.has(k)||!same(o.signature,clearSignature(row,work,facts,native)))bad();
  seen.add(o.owner_id);identities.add(k);
  if(!o.linked){roots++;if(q.owner_id!==null&&o.owner_id!==q.owner_id)bad();if(o.clear_class==='RESOLVED_AND_CLEARABLE')rootSignatures.push(o.signature);}
  else if(o.clear_class!=='RESOLVED_AND_CLEARABLE')bad();
 }
 if(!roots||q.owner_id!==null&&roots!==1||identities.size!==payload.works.reduce((n,w)=>n+w.rows.length,0))bad();
 for(const o of review.owners.filter(o=>o.linked))if(!rootSignatures.some(s=>same(s,o.signature)))bad();
 const excluded=new Set();for(const x of review.excluded){if(!exact(x,['work_id','expected_revision_id','reason'])||!id(x.work_id)||!id(x.expected_revision_id)||x.work_id===q.work_id||excluded.has(x.work_id)||!['IN_DRAFT','CHANGED','UNAVAILABLE'].includes(x.reason))bad();excluded.add(x.work_id);}
 return payload;
}

function channelInput(s){const q=s.channel_review;return exact(q,['candidate_id','work_id','expected_revision_id'])&&[q.candidate_id,q.work_id,q.expected_revision_id].every(id);}
function channelResponse(payload,args,actorId){
 const bad=()=>{throw fault('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');},q=args.resolution.channel_review;
 if(!exact(payload,['ok','kind','context','works','channel_review'])||encoder.encode(JSON.stringify(payload)).length>120000)bad();
 const scope={candidate_id:q.candidate_id,works:payload.works?.map(w=>({work_id:w.work_id,expected_revision_id:w.expected_revision_id,rows:w.rows?.map(r=>({approved_line_id:r.approved_line_id,rate_family:r.rate_family,rate_code:r.rate_code,intent:null}))})),reason:null};
 validateResolutionResponse({ok:payload.ok,kind:payload.kind,context:payload.context,works:payload.works},{resolution:scope},actorId);
 const b=payload.channel_review,c=payload.context;
 if(!exact(b,['candidate_id','work_id','expected_revision_id','complete','header','owners'])||b.complete!==true||!['candidate_id','work_id','expected_revision_id'].every(k=>b[k]===q[k])
  ||!payload.works.some(w=>w.work_id===q.work_id&&w.expected_revision_id===q.expected_revision_id)||!exact(b.header,['candidate_display_name','candidate_reference'])
  ||!text(b.header.candidate_display_name)||encoder.encode(b.header.candidate_display_name).length>8192||!nullable(b.header.candidate_reference,v=>text(v)&&encoder.encode(v).length<=8192)
  ||!Array.isArray(b.owners)||b.owners.length<1||b.owners.length>200)bad();
 const required=new Map();for(const w of payload.works)for(const r of w.rows){const variants=[...r.incompatible_decisions,...(r.saved_decision?[r.saved_decision]:[])];if(!variants.length)variants.push(null);
  for(const d of variants)required.set(JSON.stringify([w.work_id,identityKey(r),d?.decision_id??null]),{w,r,d});}
 if(required.size!==b.owners.length)bad();const seen=new Set();
 for(const o of b.owners){
  if(!exact(o,['work_id','expected_revision_id','approved_line_id','rate_family','rate_code','owner_family','owner_id','owner_version','fingerprint','from_channel','from_umbrella_id','to_channel','to_umbrella_id','clear_class','clear_reason','client_display_name','timesheet_reference','resolver_ready'])
   ||!id(o.work_id)||!id(o.expected_revision_id)||!identity(o)||!nullable(o.owner_id,id)||!nullable(o.owner_version,v=>integer(v,true))||!text(o.client_display_name)||encoder.encode(o.client_display_name).length>8192
   ||!nullable(o.timesheet_reference,v=>text(v)&&encoder.encode(v).length<=8192)||typeof o.resolver_ready!=='boolean')bad();
  const k=JSON.stringify([o.work_id,identityKey(o),o.owner_id]),native=required.get(k);if(!native||seen.has(k))bad();seen.add(k);const {w,r,d}=native,source=r.original_basis;
  if(o.expected_revision_id!==w.expected_revision_id||o.owner_family!==(r.rate_family==='STANDARD'?'WORK_BUCKET':'GROSS')
   ||o.owner_version!==(d?.decision_version??null)||o.from_channel!==source.source_pay_channel||o.from_umbrella_id!==source.source_umbrella_id
   ||o.to_channel!==c.target_pay_channel||o.to_umbrella_id!==c.target_umbrella_id||(o.from_channel===o.to_channel&&o.from_umbrella_id===o.to_umbrella_id)
   ||o.clear_class!==(d?'RESOLVED_AND_CLEARABLE':'NOT_CLEARABLE')||o.clear_reason!==(d?null:'NO_SAVED_DECISION') )bad();
  const fingerprint={original_basis:source,target_pay_channel:d?.target_pay_channel??c.target_pay_channel,
   target_umbrella_id:d?d.target_umbrella_id:c.target_umbrella_id,target_umbrella_vat_chargeable:d?d.target_umbrella_vat_chargeable:c.target_umbrella_vat_chargeable,
   valuation_policy_id:d?.valuation_policy_id??source.valuation_policy_id,valuation_date:d?.valuation_date??c.official_pay_date};
  if(!same(o.fingerprint,fingerprint))bad();
 }
 return payload;
}

function componentInput(s){
 const q=s.component_review;return exact(q,['candidate_id','work_id','expected_revision_id','approved_line_id','rate_family','rate_code'])&&[q.candidate_id,q.work_id,q.expected_revision_id].every(id)&&identity(q);
}
function effectFacts(value,q){
 if(q===null)return value===null;
 return exact(value,['pay_ex_vat','charge_ex_vat','margin_ex_vat','charge_state'])&&money(value.pay_ex_vat)&&cents(value.pay_ex_vat)===cents(q.quote.ex)
  &&nullable(value.charge_ex_vat,money)&&nullable(value.margin_ex_vat,signedMoney)
  &&(value.charge_ex_vat===null?value.charge_state==='UNAVAILABLE'&&value.margin_ex_vat===null:value.charge_state==='APPROVED'&&value.margin_ex_vat!==null&&cents(value.margin_ex_vat)===cents(value.charge_ex_vat)-cents(value.pay_ex_vat));
}
function componentSignature(r,position){const b=r.original_basis;return {rate_family:r.rate_family,rate_code:r.rate_code,component_kind:b.component_kind,tax_treatment:b.tax_treatment,
 source_kind:b.source_kind,source_pay_channel:b.source_pay_channel,source_umbrella_id:b.source_umbrella_id,valuation_policy_id:b.valuation_policy_id,
 unit_label:b.unit_label,semantics:b.semantics,source_rate:b.source_rate,source_charge_rate:b.source_charge_rate,available_rates:r.available_rates,
 valuation_window_id:r.suggestion?.valuation_window_id??null,suggested_rate:r.suggestion?.quote.rate??null,
 amount_position:b.semantics==='AMOUNT_LED'?position:null,amount_quantity:b.semantics==='AMOUNT_LED'?b.source_units:null,
 amount_basis:b.semantics==='AMOUNT_LED'?[b.component_source_ex_vat,r.suggestion?.source_component_residual_ex_vat??null,r.suggestion?.included_source_component_ex_vat??null,r.suggestion?.quote.ex??null,r.effect?.suggestion?.charge_ex_vat??null]:null};}
function componentResponse(payload,args,actorId){
 const bad=()=>{throw fault('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');},q=args.resolution.component_review;
 if(!exact(payload,['ok','kind','context','works','component_review'])||encoder.encode(JSON.stringify(payload)).length>120000)bad();
 const scope={candidate_id:q.candidate_id,works:payload.works?.map(w=>({work_id:w.work_id,expected_revision_id:w.expected_revision_id,rows:w.rows?.map(r=>({approved_line_id:r.approved_line_id,rate_family:r.rate_family,rate_code:r.rate_code,intent:null}))})),reason:null};
 validateResolutionResponse({ok:payload.ok,kind:payload.kind,context:payload.context,works:payload.works},{resolution:scope},actorId);
 const b=payload.component_review,root=payload.works[0],facts=payload.context.works[0];
 if(!exact(b,['work_id','expected_revision_id','approved_line_id','rate_family','rate_code','linked_count','excluded','complete','header'])||b.complete!==true
  ||!['work_id','expected_revision_id','approved_line_id','rate_family','rate_code'].every(k=>b[k]===q[k])||root.work_id!==q.work_id||root.expected_revision_id!==q.expected_revision_id
  ||root.rows.length!==1||identityKey(root.rows[0])!==identityKey(q)||!integer(b.linked_count,false,99n)||b.linked_count!==String(payload.works.length-1)
  ||!exact(b.header,['candidate_display_name','candidate_reference','client_display_name','timesheet_reference','week_ending_date','job_title','band_label'])||b.header.week_ending_date!==facts.week_ending_date
  ||!['candidate_display_name','client_display_name'].every(k=>text(b.header[k])&&encoder.encode(b.header[k]).length<=8192)
  ||!['candidate_reference','timesheet_reference','job_title','band_label'].every(k=>nullable(b.header[k],v=>text(v)&&encoder.encode(v).length<=8192))||!Array.isArray(b.excluded)||b.excluded.length>1000)bad();
 const signature=componentSignature(root.rows[0],facts.rows[0].position_basis);
 for(let n=0;n<payload.works.length;n++){const w=payload.works[n],c=payload.context.works[n];if(c.contract_id!==facts.contract_id)bad();for(const r of w.rows){
  if(!Object.hasOwn(r,'effect')||r.rate_family!==q.rate_family||r.rate_code!==q.rate_code||!same(componentSignature(r,c.rows.find(x=>identityKey(x)===identityKey(r)).position_basis),signature)||n>0&&r.saved_decision!==null)bad();}}
 const seen=new Set();for(const x of b.excluded){if(!exact(x,['work_id','expected_revision_id','reason','partly_included'])||!id(x.work_id)||!id(x.expected_revision_id)||!['IN_DRAFT','CHANGED','UNAVAILABLE'].includes(x.reason)
  ||typeof x.partly_included!=='boolean'||x.partly_included!==payload.works.some(w=>w.work_id===x.work_id)||x.work_id===q.work_id||seen.has(x.work_id))bad();seen.add(x.work_id);}
 return payload;
}

function bucketInput(s){
  if(exact(s,['bucket_review'])){const q=s.bucket_review;return exact(q,['candidate_id','work_id','expected_revision_id','approved_line_id'])&&[q.candidate_id,q.work_id,q.expected_revision_id].every(id)&&nullable(q.approved_line_id,id);}
  if(!exact(s,['bucket_evidence']))return false;
  const q=s.bucket_evidence;if(!exact(q,['candidate_id','works','context','after'])||!id(q.candidate_id)||!commandContext(q.context)||q.context.candidate_id!==q.candidate_id)return false;
  try{normalizeResolutionInput({resolution:{candidate_id:q.candidate_id,works:q.works,reason:null}});}catch{return false;}
  return q.works.every(w=>w.rows.every(r=>r.rate_family==='STANDARD'&&r.intent===null))&&nullable(q.after,a=>exact(a,['approved_line_id','detail_no','shift_id'])&&id(a.approved_line_id)&&integer(a.detail_no,true,2147483647n)&&id(a.shift_id)&&q.works.some(w=>w.rows.some(r=>r.approved_line_id===a.approved_line_id)));
}
function bucketResponse(payload,args,actorId){
  const bad=()=>{throw fault('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');};
  const review=Object.hasOwn(args.resolution,'bucket_review'),q=args.resolution[review?'bucket_review':'bucket_evidence'];
  const key=review?'bucket_review':'bucket_evidence';
  if(!exact(payload,['ok','kind','context','works',key])||encoder.encode(JSON.stringify(payload)).length>120000)bad();
  const scope={candidate_id:q.candidate_id,works:review?payload.works?.map(w=>({work_id:w.work_id,expected_revision_id:w.expected_revision_id,rows:w.rows?.map(r=>({approved_line_id:r.approved_line_id,rate_family:r.rate_family,rate_code:r.rate_code,intent:null}))})):q.works,reason:null};
  const native={ok:payload.ok,kind:payload.kind,context:payload.context,works:payload.works};
  validateResolutionResponse(native,{resolution:scope},actorId);
  if(review){
    const b=payload.bucket_review;
    if(!exact(b,['work_id','expected_revision_id','groups','linked_count','excluded','excluded_count','header','complete'])||b.work_id!==q.work_id||b.expected_revision_id!==q.expected_revision_id||b.complete!==true
      ||!integer(b.excluded_count,false,1000n)||!exact(b.header,['candidate_display_name','candidate_reference','client_display_name','timesheet_reference','week_ending_date','job_title','band_label'])||!date(b.header.week_ending_date)||!['candidate_display_name','client_display_name'].every(k=>text(b.header[k])&&encoder.encode(b.header[k]).length<=8192)||!['candidate_reference','timesheet_reference','job_title','band_label'].every(k=>nullable(b.header[k],v=>text(v)&&encoder.encode(v).length<=8192))
      ||!integer(b.linked_count,false,99n)||b.linked_count!==String(payload.works.length-1)||!Array.isArray(b.groups)||!b.groups.length||b.groups.length>100
      ||!Array.isArray(b.excluded)||b.excluded.length>1000||payload.works[0].work_id!==q.work_id||payload.works[0].expected_revision_id!==q.expected_revision_id)bad();
    if(payload.works.some(w=>w.rows.some(r=>r.rate_family!=='STANDARD'))||q.approved_line_id!==null&&payload.works[0].rows.some(r=>r.approved_line_id!==q.approved_line_id)||b.header.week_ending_date!==payload.context.works[0].week_ending_date)bad();
    const seen=new Set(),excluded=new Set();let last=-1;
    const rawMembers=new Map(payload.works.flatMap(w=>w.rows.map(r=>[JSON.stringify([w.work_id,r.approved_line_id,r.rate_code]),{w,r}])));
    for(const g of b.groups){
      const order=['DAY','NIGHT','SAT','SUN','BH'].indexOf(g?.rate_code);
      if(!exact(g,['signature','rate_code','current_pay_rate','current_charge_rate','current_margin','suggested_pay_rate','suggested_margin','saved_target_rate','units','root_units','members','has_evidence'])||typeof g.has_evidence!=='boolean'||order<last||order<0
        ||!['current_pay_rate','current_charge_rate','suggested_pay_rate','saved_target_rate'].every(k=>nullable(g[k],v=>decimal(v,6,120000)))
        ||!['current_margin','suggested_margin'].every(k=>nullable(g[k],v=>decimal(v,6,120000,true)))||!exact(g.signature,['rate_code','component_kind','tax_treatment','unit_label','saved_mode','valuation_window_id','source_kind','source_pay_channel','source_umbrella_id','target_pay_channel','target_umbrella_id','target_umbrella_vat_chargeable','valuation_policy_id','valuation_date','source_rate','source_charge_rate','available_rates','suggested_target_rate','saved_target_rate'])||g.saved_target_rate!==g.signature.saved_target_rate||!nullable(g.signature.saved_mode,v=>modes.includes(v))
        ||!decimal(g.units,6,16)||!decimal(g.root_units,6,16)||!Array.isArray(g.members)||!g.members.length||g.members.length>100)bad();
      last=order;let ownUnits=0n,rootUnits=0n,nativeGroupAnchor=false;
      for(const m of g.members){
        if(!exact(m,['work_id','expected_revision_id','approved_line_id','rate_family','rate_code','expected_decision','linked'])||!id(m.work_id)||!id(m.expected_revision_id)||!identity(m)||m.rate_family!=='STANDARD'||m.rate_code!==g.rate_code
          ||typeof m.linked!=='boolean'||m.linked!==(m.work_id!==q.work_id)||!nullable(m.expected_decision,pair))bad();
        const k=JSON.stringify([m.work_id,m.approved_line_id,m.rate_code]),nativeRow=rawMembers.get(k);if(!nativeRow||seen.has(k))bad();seen.add(k);
        const {w,r}=nativeRow,basis=r.original_basis;if(r.rate_family!==m.rate_family||w.expected_revision_id!==m.expected_revision_id||!same(r.expected_decision,m.expected_decision))bad();
        const signature={rate_code:r.rate_code,component_kind:basis.component_kind,tax_treatment:basis.tax_treatment,unit_label:basis.unit_label,
          saved_mode:r.saved_decision?.resolution_mode??null,valuation_window_id:r.suggestion?.valuation_window_id??null,source_kind:basis.source_kind,
          source_pay_channel:basis.source_pay_channel,source_umbrella_id:basis.source_umbrella_id,target_pay_channel:payload.context.target_pay_channel,
          target_umbrella_id:payload.context.target_umbrella_id,target_umbrella_vat_chargeable:payload.context.target_umbrella_vat_chargeable,
          valuation_policy_id:basis.valuation_policy_id,valuation_date:payload.context.official_pay_date,source_rate:basis.source_rate,source_charge_rate:basis.source_charge_rate,
          available_rates:r.available_rates,suggested_target_rate:r.suggestion?.quote.rate??null,saved_target_rate:r.saved_quote?.quote.rate??null};
        if(same(signature,g.signature))nativeGroupAnchor=true;
        const matching= same(signature,g.signature)||m.linked&&r.saved_decision===null&&same(Object.fromEntries(Object.entries(signature).filter(([k])=>!['saved_target_rate','saved_mode'].includes(k))),Object.fromEntries(Object.entries(g.signature).filter(([k])=>!['saved_target_rate','saved_mode'].includes(k))));
        if(!matching||g.current_pay_rate!==basis.source_rate||g.current_charge_rate!==basis.source_charge_rate||g.suggested_pay_rate!==signature.suggested_target_rate||!m.linked&&g.saved_target_rate!==signature.saved_target_rate)bad();
        ownUnits+=scaled(basis.source_units,6);if(!m.linked)rootUnits+=scaled(basis.source_units,6);
      }
      if(rootUnits!==scaled(g.root_units,6)||!nativeGroupAnchor||ownUnits===0n||ownUnits!==scaled(g.units,6))bad();
    }
    if(seen.size!==rawMembers.size)bad();
    for(const x of b.excluded){if(!exact(x,['work_id','expected_revision_id','reason','partly_included'])||!id(x.work_id)||!id(x.expected_revision_id)||!['IN_DRAFT','CHANGED','UNAVAILABLE'].includes(x.reason)||typeof x.partly_included!=='boolean'||x.partly_included!==payload.works.some(w=>w.work_id===x.work_id)||excluded.has(x.work_id)||x.work_id===q.work_id)bad();excluded.add(x.work_id);}
    if(b.excluded_count!==String(b.excluded.filter(x=>!x.partly_included).length))bad();
  }else{
    if(!same(resolutionWriteContext(payload.context),q.context))bad();const e=payload.bucket_evidence;
    if(!exact(e,['rows','has_more','after'])||!Array.isArray(e.rows)||e.rows.length>25||typeof e.has_more!=='boolean'||e.has_more!==(e.after!==null)||e.has_more&&e.rows.length!==25)bad();
    let previous=q.after;const parents=new Set(q.works.flatMap(w=>w.rows.map(r=>r.approved_line_id)));
    for(const r of e.rows){
      if(!exact(r,['approved_line_id','shift_id','detail_no','work_date','shift_start_at','shift_end_at','shift_start_local','shift_end_local','shift_overnight','submitted_minutes','approved_minutes','approved_hours','detail_label'])
        ||!parents.has(r.approved_line_id)||!id(r.shift_id)||!integer(r.detail_no,true,2147483647n)||!date(r.work_date)
        ||!['shift_start_at','shift_end_at'].every(k=>nullable(r[k],v=>text(v)&&Number.isFinite(Date.parse(v))))
        ||!['shift_start_local','shift_end_local'].every(k=>nullable(r[k],v=>text(v)&&/^([01][0-9]|2[0-3]):[0-5][0-9]$/.test(v)))
        ||(r.shift_start_at===null)!==(r.shift_end_at===null)||(r.shift_start_local===null)!==(r.shift_end_local===null)
        ||!nullable(r.shift_overnight,v=>typeof v==='boolean')||!['submitted_minutes','approved_minutes'].every(k=>nullable(r[k],v=>integer(v,false,2147483647n)))
        ||!nullable(r.approved_hours,rate)||!nullable(r.detail_label,v=>text(v)&&encoder.encode(v).length<=8192))bad();
      const next={approved_line_id:r.approved_line_id,detail_no:r.detail_no,shift_id:r.shift_id};
      if(previous&&!(next.approved_line_id>previous.approved_line_id||next.approved_line_id===previous.approved_line_id&&(BigInt(next.detail_no)>BigInt(previous.detail_no)||next.detail_no===previous.detail_no&&next.shift_id>previous.shift_id)))bad();previous=next;
    }
    if(e.after!==null&&!same(e.after,previous))bad();
  }
  return payload;
}

/** WB29 typed native decisions. Validation only; prices and units belong to SQL. */
const encoder=new TextEncoder(),uuid=/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
const modes=['SUGGESTED_EQUIVALENT_BASIS','MANUAL_REPLACEMENT_RATE','MANUAL_AMOUNT'];
const channels=['PAYE','UMBRELLA'];
const rowCodes=['BPAY_NEXT_RESOLUTION_RESIDUAL_AMBIGUOUS','BPAY_NEXT_RESOLUTION_SOURCE_BASIS_UNAVAILABLE'];
const object=v=>v!==null&&typeof v==='object'&&!Array.isArray(v)&&[Object.prototype,null].includes(Object.getPrototypeOf(v));
const exact=(v,keys)=>object(v)&&Reflect.ownKeys(v).length===keys.length&&keys.every(k=>Object.hasOwn(v,k)&&Object.getOwnPropertyDescriptor(v,k)?.enumerable===true&&Object.hasOwn(Object.getOwnPropertyDescriptor(v,k),'value'));
const same=(a,b)=>a===b||Array.isArray(a)&&Array.isArray(b)&&a.length===b.length&&a.every((v,n)=>same(v,b[n]))||object(a)&&object(b)&&exact(a,Object.keys(b))&&Object.keys(a).every(k=>same(a[k],b[k]));
const nullable=(v,check)=>v===null||check(v);
const id=v=>typeof v==='string'&&uuid.test(v);
const integer=(v,positive=false,max=9223372036854775807n)=>typeof v==='string'&&(positive?/^[1-9][0-9]{0,18}$/:/^(0|[1-9][0-9]{0,18})$/).test(v)&&BigInt(v)<=max;
const decimal=(v,scale=2,digits=16,signed=false)=>typeof v==='string'&&new RegExp(`^${signed?'-?':''}(0|[1-9][0-9]{0,${digits-1}})(\\.[0-9]{1,${scale}})?$`).test(v);
const rate=v=>decimal(v,6,12),money=v=>decimal(v),signedMoney=v=>decimal(v,2,16,true);
const scaled=(v,scale)=>{const negative=v.startsWith('-'),[a,b='']=v.replace(/^-/,'').split('.');return (negative?-1n:1n)*(BigInt(a)*10n**BigInt(scale)+BigInt(b.padEnd(scale,'0')));};
const cents=v=>scaled(v,2);
const zero=v=>typeof v==='string'&&/^0(?:\.0+)?$/.test(v);
const text=v=>typeof v==='string';
const reason=v=>nullable(v,x=>text(x)&&encoder.encode(x).length<=2000);
const nonblank=v=>text(v)&&v.replace(/^ +| +$/g,'')!=='';
const date=v=>{if(typeof v!=='string'||!/^\d{4}-\d{2}-\d{2}$/.test(v)||v<'0001-01-01')return false;const d=new Date(v+'T00:00:00Z');return Number.isFinite(d.getTime())&&d.toISOString().slice(0,10)===v;};
const identity=r=>id(r.approved_line_id)&&['STANDARD','ADDITIONAL','ADJUSTMENT'].includes(r.rate_family)&&text(r.rate_code)&&[...r.rate_code].length>=1&&[...r.rate_code].length<=256&&(r.rate_family!=='STANDARD'||['DAY','NIGHT','SAT','SUN','BH'].includes(r.rate_code))&&(r.rate_family!=='ADDITIONAL'||[...r.rate_code].length<=245&&r.rate_code===r.rate_code.replace(/^ +| +$/g,'').toUpperCase());
const identityKey=r=>JSON.stringify([r.approved_line_id,r.rate_family,r.rate_code]);
const intent=(v,r)=>exact(v,['resolution_mode','manual_replacement_rate','manual_amount_ex_vat'])&&modes.includes(v.resolution_mode)&&nullable(v.manual_replacement_rate,rate)&&nullable(v.manual_amount_ex_vat,money)&&(v.resolution_mode==='SUGGESTED_EQUIVALENT_BASIS'?v.manual_replacement_rate===null&&v.manual_amount_ex_vat===null:nonblank(r)&&(v.resolution_mode==='MANUAL_REPLACEMENT_RATE'?v.manual_replacement_rate!==null&&v.manual_amount_ex_vat===null:v.manual_replacement_rate===null&&v.manual_amount_ex_vat!==null));
const fault=(code,status=503,retryable=false,reload=false)=>Object.assign(new Error(code),{code,status,retryable,reload});

export function normalizeResolutionInput(input){
  const bad=()=>{throw fault('BPAY_NEXT_RESOLUTION_REQUEST_INVALID',400);};
  if(!exact(input,['resolution']))bad();const s=input.resolution;
  if(exact(s,['clear_review'])){if(!clearInput(s)||encoder.encode(JSON.stringify(input)).length>32768)bad();return input;}
  if(exact(s,['channel_review'])){if(!channelInput(s)||encoder.encode(JSON.stringify(input)).length>32768)bad();return input;}
  if(exact(s,['component_review'])){if(!componentInput(s)||encoder.encode(JSON.stringify(input)).length>32768)bad();return input;}
  if(exact(s,['bucket_review'])||exact(s,['bucket_evidence'])){if(!bucketInput(s)||encoder.encode(JSON.stringify(input)).length>32768)bad();return input;}
  if(!exact(s,['candidate_id','works','reason'])||!id(s.candidate_id)||!reason(s.reason)||!Array.isArray(s.works)||s.works.length<1||s.works.length>100||encoder.encode(JSON.stringify(input)).length>32768)bad();
  const works=new Set();let count=0;
  for(const w of s.works){
    if(!exact(w,['work_id','expected_revision_id','rows'])||!id(w.work_id)||!id(w.expected_revision_id)||!Array.isArray(w.rows)||!w.rows.length||w.rows.length>100)bad();
    if(works.has(w.work_id))throw fault('BPAY_NEXT_RESOLUTION_DUPLICATE_SCOPE',400);works.add(w.work_id);const rows=new Set();
    for(const r of w.rows){
      if(++count>100)throw fault('BPAY_NEXT_RESOLUTION_SCOPE_TOO_LARGE',400);
      if(!exact(r,['approved_line_id','rate_family','rate_code','intent'])||!identity(r)||!nullable(r.intent,x=>intent(x,s.reason)))bad();
      const key=identityKey(r);if(rows.has(key))throw fault('BPAY_NEXT_RESOLUTION_DUPLICATE_SCOPE',400);rows.add(key);
    }
  }
  return input;
}

const target=v=>channels.includes(v.target_pay_channel)&&(v.target_pay_channel==='PAYE'?v.target_umbrella_id===null&&v.target_umbrella_vat_chargeable===null:id(v.target_umbrella_id)&&typeof v.target_umbrella_vat_chargeable==='boolean');
function available(v){
  return exact(v,['provenance','paye_rate','umbrella_rate','charge_rate','unit_label'])&&['UNAVAILABLE','CURRENT_CONTRACT','APPROVAL_FROZEN_DAILY'].includes(v.provenance)&&['paye_rate','umbrella_rate','charge_rate'].every(k=>nullable(v[k],rate))&&nullable(v.unit_label,text)&&(v.provenance!=='UNAVAILABLE'||['paye_rate','umbrella_rate','charge_rate','unit_label'].every(k=>v[k]===null));
}
function basis(v){
  return exact(v,['work_id','physical_timesheet_id','physical_timesheet_version','financial_snapshot_id','source_kind','source_head_id','source_event_id','source_pay_channel','source_umbrella_id','valuation_policy_id','component_key','component_kind','source_component_id','work_date','unit_label','tax_treatment','rate_family','rate_code','source_units','source_rate','source_charge_rate','component_source_ex_vat','semantics'])
    &&['work_id','physical_timesheet_id','valuation_policy_id'].every(k=>id(v[k]))&&['financial_snapshot_id','source_head_id','source_event_id','source_umbrella_id','source_component_id'].every(k=>nullable(v[k],id))
    &&integer(v.physical_timesheet_version,true,2147483647n)&&['ORDINARY','SOURCE','PROTECTED','EXPENSE','ADJUSTMENT'].includes(v.source_kind)&&channels.includes(v.source_pay_channel)
    &&text(v.component_key)&&[...v.component_key].length>=1&&[...v.component_key].length<=256&&['WORK','PROTECTED_WORK','ADDITIONAL','ADJUSTMENT'].includes(v.component_kind)
    &&nullable(v.work_date,date)&&nullable(v.unit_label,text)&&v.tax_treatment==='TAXABLE'&&identity({...v,approved_line_id:'00000000-0000-0000-0000-000000000000'})
    &&['source_units','source_rate','source_charge_rate'].every(k=>nullable(v[k],rate))&&money(v.component_source_ex_vat)&&['RATE_BEARING','AMOUNT_LED'].includes(v.semantics)
    &&(v.rate_family==='STANDARD'?['WORK','PROTECTED_WORK'].includes(v.component_kind)&&v.semantics==='RATE_BEARING':v.rate_family==='ADDITIONAL'?v.component_kind==='ADDITIONAL'&&v.component_key==='ADDITIONAL:'+v.rate_code:v.component_kind==='ADJUSTMENT'&&v.component_key===v.rate_code)
    &&v.semantics===(['STANDARD','ADDITIONAL'].includes(v.rate_family)&&v.source_units!==null&&!zero(v.source_units)&&v.source_rate!==null&&v.source_charge_rate!==null?'RATE_BEARING':'AMOUNT_LED');
}
function decision(v){
  return exact(v,['decision_id','decision_version','target_pay_channel','target_umbrella_id','target_umbrella_vat_chargeable','valuation_policy_id','valuation_date','resolution_mode','manual_replacement_rate','manual_amount_ex_vat','reason','clearable'])
    &&id(v.decision_id)&&integer(v.decision_version,true)&&target(v)&&id(v.valuation_policy_id)&&date(v.valuation_date)&&reason(v.reason)&&v.clearable===true
    &&intent({resolution_mode:v.resolution_mode,manual_replacement_rate:v.manual_replacement_rate,manual_amount_ex_vat:v.manual_amount_ex_vat},v.reason);
}
function quote(v,policy,position,saved=null){
  const keys=['source_ex_vat','source_component_residual_ex_vat','included_source_component_ex_vat','remaining_units','quote','valuation_policy_id','valuation_window_id'];
  if(saved!==null)keys.push('decision_id','decision_version');
  return exact(v,keys)&&nullable(v.source_ex_vat,money)&&money(v.source_component_residual_ex_vat)&&money(v.included_source_component_ex_vat)&&nullable(v.remaining_units,rate)
    &&v.valuation_policy_id===policy&&id(v.valuation_window_id)&&exact(v.quote,['ex','vat','inc','rate'])&&['ex','vat','inc'].every(k=>money(v.quote[k]))
    &&nullable(v.quote.rate,x=>decimal(x,6,120000))&&cents(v.quote.ex)+cents(v.quote.vat)===cents(v.quote.inc)
    &&cents(v.source_component_residual_ex_vat)===cents(position.approved_source_ex_vat)-cents(position.realised_source_ex_vat)-cents(position.held_source_ex_vat)
    &&cents(v.included_source_component_ex_vat)===cents(v.source_component_residual_ex_vat)
    &&(!zero(v.included_source_component_ex_vat)||zero(v.source_ex_vat)&&['ex','vat','inc'].every(k=>zero(v.quote[k]))&&v.quote.rate===null&&zero(v.remaining_units))
    &&(saved===null||v.decision_id===saved.decision_id&&v.decision_version===saved.decision_version);
}
const pair=v=>exact(v,['decision_id','decision_version'])&&id(v.decision_id)&&integer(v.decision_version,true);
const commandContextKeys=['agency_id','actor_user_id','candidate_id','owner_epoch','metadata_version','financial_view_revision','workbench_revision','adoption_revision','last_command_sequence','target_pay_channel','target_umbrella_id','target_umbrella_vat_chargeable','target_umbrella_enabled','official_pay_date'];
const commandContext=c=>exact(c,commandContextKeys)&&encoder.encode(JSON.stringify(c)).length<=4096&&['agency_id','actor_user_id','candidate_id'].every(k=>id(c[k]))
  &&['owner_epoch','metadata_version'].every(k=>integer(c[k],true))&&['financial_view_revision','workbench_revision','adoption_revision','last_command_sequence'].every(k=>integer(c[k]))
  &&target(c)&&c.target_umbrella_enabled===(c.target_pay_channel==='UMBRELLA'?true:null)&&date(c.official_pay_date);
export function resolutionWriteContext(validatedContext){
  if(!exact(validatedContext,[...commandContextKeys,'works']))throw fault('BPAY_NEXT_RESOLUTION_REQUEST_INVALID',400);
  const value=Object.fromEntries(commandContextKeys.map(k=>[k,validatedContext[k]]));
  if(!commandContext(value))throw fault('BPAY_NEXT_RESOLUTION_REQUEST_INVALID',400);return value;
}
export function normalizeResolutionCommand(action,args,actorId=null){
  const bad=()=>{throw fault('BPAY_NEXT_RESOLUTION_REQUEST_INVALID',400);};
  if(!['RESOLUTION_SAVE','RESOLUTION_CLEAR'].includes(action)||!exact(args,['request_id','candidate_id','context','works','reason','zero_confirmed'])
    ||!id(args.request_id)||!id(args.candidate_id)||!commandContext(args.context)||args.context.candidate_id!==args.candidate_id
    ||actorId!==null&&args.context.actor_user_id!==actorId||!reason(args.reason)||typeof args.zero_confirmed!=='boolean'
    ||!Array.isArray(args.works)||!args.works.length||args.works.length>100)bad();
  const works=new Set(),owners=new Set();let count=0;
  for(const w of args.works){
    if(!exact(w,['work_id','expected_revision_id','rows'])||!id(w.work_id)||!id(w.expected_revision_id)||!Array.isArray(w.rows)||!w.rows.length||w.rows.length>100)bad();
    if(works.has(w.work_id))throw fault('BPAY_NEXT_RESOLUTION_DUPLICATE_SCOPE',400);works.add(w.work_id);const identities=new Set();
    for(const r of w.rows){
      if(++count>100)throw fault('BPAY_NEXT_RESOLUTION_SCOPE_TOO_LARGE',413);
      if(!exact(r,['approved_line_id','rate_family','rate_code','intent','expected_decision_id','expected_decision_version'])||!identity(r)
        ||(r.expected_decision_id===null)!==(r.expected_decision_version===null)||!nullable(r.expected_decision_id,id)||!nullable(r.expected_decision_version,x=>integer(x,true))
        ||(action==='RESOLUTION_SAVE'?!intent(r.intent,args.reason):r.intent!==null||r.expected_decision_id===null))bad();
      const key=action==='RESOLUTION_CLEAR'?r.expected_decision_id:identityKey(r);
      if(identities.has(key)||action==='RESOLUTION_CLEAR'&&owners.has(key))throw fault('BPAY_NEXT_RESOLUTION_DUPLICATE_SCOPE',400);
      identities.add(key);if(action==='RESOLUTION_CLEAR')owners.add(key);
    }
  }
  if(encoder.encode(JSON.stringify({actor_user_id:actorId??args.context.actor_user_id,action,args,readback:false})).length>32768)throw fault('BPAY_NEXT_RESOLUTION_SCOPE_TOO_LARGE',413);
  return args;
}
export function validateResolutionCommandReceipt(receipt,action,args){
  normalizeResolutionCommand(action,args);const bad=()=>{throw fault('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');};
  if(!exact(receipt,['request_id','candidate_id','received','replay','result'])||receipt.request_id!==args.request_id||receipt.candidate_id!==args.candidate_id
    ||typeof receipt.received!=='boolean'||typeof receipt.replay!=='boolean'||encoder.encode(JSON.stringify(receipt)).length>131072)bad();
  if(!receipt.received){if(receipt.replay||receipt.result!==null)bad();return receipt;}
  const result=receipt.result;
  if(!exact(result,['action','candidate_id','owner_epoch','works','processing','adoption',...(Object.hasOwn(result,'clear_class')?['clear_class']:[])])||(Object.hasOwn(result,'clear_class')&&(action!=='RESOLUTION_CLEAR'||result.clear_class!=='RESOLVED_AND_CLEARABLE'))||result.action!==action||result.candidate_id!==args.candidate_id
    ||result.owner_epoch!==args.context.owner_epoch||result.processing!==true||!Array.isArray(result.works)||result.works.length!==args.works.length
    ||encoder.encode(JSON.stringify(result)).length>65536||!exact(result.adoption,['workbench_revision','adoption_revision','adoption_state'])
    ||!['workbench_revision','adoption_revision'].every(k=>integer(result.adoption[k]))||!['UNSEEDED','SEEDING','CURRENT','ERROR'].includes(result.adoption.adoption_state))bad();
  const requests=new Map(args.works.map(w=>[w.work_id,w])),commands=new Set(),revisions=new Set(),newOwners=new Set(),oldOwners=new Set();
  let previousWork=null,previousSequence=null,carryCount=0;
  for(const w of result.works){
    const requested=requests.get(w?.work_id);
    if(!exact(w,['work_id','based_on_revision_id','published_revision_id','command_id','agency_sequence','changes','carried_decisions'])||!requested
      ||previousWork!==null&&w.work_id<=previousWork||w.based_on_revision_id!==requested.expected_revision_id||!id(w.published_revision_id)||w.published_revision_id===w.based_on_revision_id
      ||!id(w.command_id)||commands.has(w.command_id)||revisions.has(w.published_revision_id)||!integer(w.agency_sequence,true)
      ||previousSequence!==null&&BigInt(w.agency_sequence)<=previousSequence||!Array.isArray(w.changes)||w.changes.length!==requested.rows.length
      ||!Array.isArray(w.carried_decisions)||w.carried_decisions.length>100)bad();
    previousWork=w.work_id;previousSequence=BigInt(w.agency_sequence);commands.add(w.command_id);revisions.add(w.published_revision_id);
    const pending=[...requested.rows],replaced=new Set(requested.rows.filter(r=>r.expected_decision_id!==null).map(r=>r.expected_decision_id));
    for(const change of w.changes){
      if(!exact(change,['approved_line_id','rate_family','rate_code','decision_id','decision_version'])||!identity(change)||!id(change.decision_id)||!integer(change.decision_version,true))bad();
      const n=pending.findIndex(r=>identityKey(r)===identityKey(change)&&(action==='RESOLUTION_SAVE'||r.expected_decision_id===change.decision_id&&r.expected_decision_version===change.decision_version));
      if(n<0)bad();const source=pending.splice(n,1)[0];
      if(action==='RESOLUTION_SAVE'){
        if(change.decision_id===source.expected_decision_id||newOwners.has(change.decision_id)||BigInt(change.decision_version)!==(source.expected_decision_version===null?1n:BigInt(source.expected_decision_version)+1n))bad();
        newOwners.add(change.decision_id);
      }
    }
    for(const carry of w.carried_decisions){
      if(++carryCount>100||!exact(carry,['previous_decision_id','previous_decision_version','decision_id','decision_version'])
        ||!id(carry.previous_decision_id)||!id(carry.decision_id)||!integer(carry.previous_decision_version,true)||!integer(carry.decision_version,true)
        ||carry.previous_decision_id===carry.decision_id||replaced.has(carry.previous_decision_id)||oldOwners.has(carry.previous_decision_id)||newOwners.has(carry.decision_id)
        ||BigInt(carry.decision_version)!==BigInt(carry.previous_decision_version)+1n)bad();
      oldOwners.add(carry.previous_decision_id);newOwners.add(carry.decision_id);
    }
  }
  const originalOwners=new Set(args.works.flatMap(w=>w.rows.map(r=>r.expected_decision_id).filter(x=>x!==null)));
  if([...newOwners].some(x=>oldOwners.has(x)||originalOwners.has(x))||[...oldOwners].some(x=>originalOwners.has(x)))bad();
  return receipt;
}
const unavailable=v=>v===null||rowCodes.includes(v);
const quoteOrCode=(q,c)=>unavailable(c)&&((q===null)!==(c===null));
function manualEcho(q,choice){
  if(zero(q.included_source_component_ex_vat))return ['ex','vat','inc'].every(k=>zero(q.quote[k]))&&q.quote.rate===null&&zero(q.remaining_units);
  return choice.resolution_mode==='MANUAL_AMOUNT'?cents(q.quote.ex)===cents(choice.manual_amount_ex_vat)&&q.quote.rate===null
    :choice.resolution_mode!=='MANUAL_REPLACEMENT_RATE'||q.quote.rate!==null&&scaled(q.quote.rate,6)===scaled(choice.manual_replacement_rate,6);
}

export function validateResolutionResponse(payload,args,actorId){
  normalizeResolutionInput(args);
  if(exact(args.resolution,['clear_review']))return clearResponse(payload,args,actorId);
  if(exact(args.resolution,['channel_review']))return channelResponse(payload,args,actorId);
  if(exact(args.resolution,['component_review']))return componentResponse(payload,args,actorId);
  if(exact(args.resolution,['bucket_review'])||exact(args.resolution,['bucket_evidence']))return bucketResponse(payload,args,actorId);
 const scope=args.resolution,bad=()=>{throw fault('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');};
  if(!exact(payload,['ok','kind','context','works'])||payload.ok!==true||payload.kind!=='RESOLUTION_CONTEXT'||encoder.encode(JSON.stringify(payload)).length>120000)bad();
  const c=payload.context;
  if(!exact(c,['agency_id','actor_user_id','candidate_id','owner_epoch','metadata_version','financial_view_revision','workbench_revision','adoption_revision','last_command_sequence','target_pay_channel','target_umbrella_id','target_umbrella_vat_chargeable','target_umbrella_enabled','official_pay_date','works'])
    ||!id(c.agency_id)||!id(c.actor_user_id)||c.actor_user_id!==actorId||c.candidate_id!==scope.candidate_id||!integer(c.owner_epoch,true)||!integer(c.metadata_version,true)
    ||!['financial_view_revision','workbench_revision','adoption_revision','last_command_sequence'].every(k=>integer(c[k]))||!target(c)||c.target_umbrella_enabled!==(c.target_pay_channel==='UMBRELLA'?true:null)||!date(c.official_pay_date)
    ||!Array.isArray(c.works)||!Array.isArray(payload.works)||c.works.length!==scope.works.length||payload.works.length!==scope.works.length)bad();
  const owners=new Set();let ownerCount=0;
  for(let n=0;n<scope.works.length;n++){
    const requested=scope.works[n],w=c.works[n],result=payload.works[n];
    if(!exact(w,['work_id','expected_revision_id','applied_revision_id','revision_no','physical_timesheet_id','physical_timesheet_version','original_timesheet_id','contract_id','week_ending_date','valuation_policy_id','source_pay_channel','source_umbrella_id','rows'])
      ||w.work_id!==requested.work_id||w.expected_revision_id!==requested.expected_revision_id||w.applied_revision_id!==requested.expected_revision_id||!integer(w.revision_no,true)
      ||!id(w.physical_timesheet_id)||!integer(w.physical_timesheet_version,true,2147483647n)||!id(w.original_timesheet_id)||!nullable(w.contract_id,id)||!date(w.week_ending_date)||!id(w.valuation_policy_id)||!channels.includes(w.source_pay_channel)||!nullable(w.source_umbrella_id,id)
      ||!exact(result,['work_id','expected_revision_id','rows'])||result.work_id!==w.work_id||result.expected_revision_id!==w.expected_revision_id||!Array.isArray(w.rows)||!Array.isArray(result.rows)||w.rows.length!==requested.rows.length||result.rows.length!==requested.rows.length)bad();
    for(let m=0;m<requested.rows.length;m++){
      const req=requested.rows[m],r=w.rows[m],out=result.rows[m];
      if(!exact(r,['approved_line_id','rate_family','rate_code','original_basis','position_basis','position_applied_revision_id','source_book_state','source_book_revision','source_binding_version_id','available_rates','decisions'])
        ||identityKey(r)!==identityKey(req)||!basis(r.original_basis)||!available(r.available_rates)||r.position_applied_revision_id!==w.applied_revision_id||!['UNBOUND','READY'].includes(r.source_book_state)
        ||!integer(r.source_book_revision,r.source_book_state==='READY')||r.source_book_state==='UNBOUND'&&r.source_book_revision!=='0'||!nullable(r.source_binding_version_id,id)||!Array.isArray(r.decisions)||r.decisions.length>100)bad();
      const b=r.original_basis,p=r.position_basis;
      if(!['work_id','physical_timesheet_id','physical_timesheet_version','valuation_policy_id','source_pay_channel','source_umbrella_id'].every(k=>b[k]===w[k])||b.rate_family!==req.rate_family||b.rate_code!==req.rate_code
        ||!exact(p,['approved_source_ex_vat','realised_source_ex_vat','held_source_ex_vat','source_basis_channel'])||!money(p.approved_source_ex_vat)||!signedMoney(p.realised_source_ex_vat)||!money(p.held_source_ex_vat)||!nullable(p.source_basis_channel,x=>channels.includes(x))||cents(p.approved_source_ex_vat)!==cents(b.component_source_ex_vat))bad();
      if(!exact(out,['approved_line_id','rate_family','rate_code','original_basis','available_rates','suggestion','suggestion_unavailable_code','expected_decision','saved_decision','saved_quote','saved_unavailable_code','incompatible_decisions','preview','preview_unavailable_code','requires_zero_confirmation',...(Object.hasOwn(out,'effect')?['effect']:[])])
        ||identityKey(out)!==identityKey(req)||!same(out.original_basis,b)||!same(out.available_rates,r.available_rates)||!Array.isArray(out.incompatible_decisions)||typeof out.requires_zero_confirmation!=='boolean'
        ||!quoteOrCode(out.suggestion,out.suggestion_unavailable_code)||!nullable(out.suggestion,q=>quote(q,w.valuation_policy_id,p)))bad();
      if(Object.hasOwn(out,'effect')&&(!exact(out.effect,['suggestion','saved','preview'])||!effectFacts(out.effect.suggestion,out.suggestion)||!effectFacts(out.effect.saved,out.saved_quote)||!effectFacts(out.effect.preview,out.preview)))bad();
      let prior=null;
      for(const d of r.decisions){if(!decision(d)||prior!==null&&d.decision_id<=prior||owners.has(d.decision_id)||++ownerCount>100)bad();owners.add(d.decision_id);prior=d.decision_id;}
      const saved=out.saved_decision;
      if(saved===null){if(out.expected_decision!==null||out.saved_quote!==null||out.saved_unavailable_code!==null)bad();}
      else if(!decision(saved)||!pair(out.expected_decision)||out.expected_decision.decision_id!==saved.decision_id||out.expected_decision.decision_version!==saved.decision_version
        ||!['target_pay_channel','target_umbrella_id','target_umbrella_vat_chargeable'].every(k=>saved[k]===c[k])||saved.valuation_policy_id!==w.valuation_policy_id||saved.valuation_date!==c.official_pay_date
        ||!quoteOrCode(out.saved_quote,out.saved_unavailable_code)||!nullable(out.saved_quote,q=>quote(q,w.valuation_policy_id,p,saved)&&manualEcho(q,saved)))bad();
      const all=[...(saved===null?[]:[saved]),...out.incompatible_decisions];
      if(all.length!==r.decisions.length||new Set(all.map(d=>d?.decision_id)).size!==all.length||!all.every(d=>r.decisions.some(x=>same(x,d)))
        ||out.incompatible_decisions.some(d=>d.target_pay_channel===c.target_pay_channel&&d.target_umbrella_id===c.target_umbrella_id&&d.target_umbrella_vat_chargeable===c.target_umbrella_vat_chargeable&&d.valuation_policy_id===w.valuation_policy_id&&d.valuation_date===c.official_pay_date))bad();
      if(req.intent===null){if(out.preview!==null||out.preview_unavailable_code!==null||out.requires_zero_confirmation)bad();}
      else{
        if(!quoteOrCode(out.preview,out.preview_unavailable_code)||!nullable(out.preview,q=>quote(q,w.valuation_policy_id,p)))bad();
        if(out.preview!==null&&(!manualEcho(out.preview,req.intent)||req.intent.resolution_mode==='SUGGESTED_EQUIVALENT_BASIS'&&!same(out.preview,out.suggestion)))bad();
        const needs=out.preview!==null&&(zero(req.intent.manual_replacement_rate)||zero(req.intent.manual_amount_ex_vat)||zero(out.preview.quote.ex));if(out.requires_zero_confirmation!==needs)bad();
      }
    }
  }
  return payload;
}

// Exact native SQLSTATE/message pairs only. Unknown failures retain the normal read fault.
const nativePairs=new Map();
function errors(state,names,status,retryable=false,reload=false){for(const name of names.split(' '))nativePairs.set(state+':BPAY_NEXT_'+name,{status,retryable,reload});}
errors('22023','RESOLUTION_REQUEST_INVALID RESOLUTION_DUPLICATE_SCOPE RESOLUTION_IDENTITY_INVALID',400);
errors('22023','CONTRACT_RATE_SHAPE_INVALID ADDITIONAL_RATE_SHAPE_INVALID ADDITIONAL_RATE_CODE_INVALID RATE_INVALID',409);
errors('22003','RATE_PRECISION_UNSUPPORTED MILEAGE_RATE_INVALID',409);
errors('54000','RESOLUTION_SCOPE_TOO_LARGE WORKBENCH_RESPONSE_TOO_LARGE',413);
errors('42501','WORKBENCH_FORBIDDEN',403);
errors('55000','MODULE_NOT_ACTIVE',409);
errors('55000','WORKBENCH_DATE_UNAVAILABLE RESOLUTION_CONTEXT_UNAVAILABLE',503,true);
errors('23514','RESOLUTION_STALE RESOLUTION_POSITION_BASIS_CHANGED RESOLUTION_BASIS_CHANGED DAILY_REVISION_EVIDENCE_REQUIRED RESOLUTION_PUBLICATION_INCOMPATIBLE RESOLUTION_POSITION_INCOMPATIBLE',409,false,true);
errors('23514','RESOLUTION_IDENTITY_INVALID RESOLUTION_BUCKET_MISSING RESOLUTION_COMPATIBILITY_AMBIGUOUS RESOLUTION_SOURCE_BASIS_UNAVAILABLE RESOLUTION_MODE_INVALID RESOLUTION_POLICY_WINDOW_MISSING RESOLUTION_QUOTE_RANGE_INVALID TARGET_POLICY_WINDOW_AMBIGUOUS RESOLUTION_VERSION_MISSING',409);
const staleResolutionCodes=['BPAY_NEXT_RESOLUTION_STALE','BPAY_NEXT_RESOLUTION_POSITION_BASIS_CHANGED','BPAY_NEXT_RESOLUTION_BASIS_CHANGED'];
function validateResolutionChangedEvidence(value,args,actorId){
 const bad=()=>{throw fault('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');};
 if(!args||!exact(value,['request_id','candidate_id','state','mappings','facts','unavailable_code'])||value.request_id!==args.request_id||value.candidate_id!==args.candidate_id||encoder.encode(JSON.stringify(value)).length>120000)bad();
 if(value.state==='UNAVAILABLE'){if(value.mappings!==null||value.facts!==null||value.unavailable_code!=='BPAY_NEXT_RESOLUTION_CONTEXT_UNAVAILABLE')bad();return value;}
 if(value.state!=='CURRENT'||value.unavailable_code!==null||!Array.isArray(value.mappings)||value.mappings.length!==args.works.reduce((n,w)=>n+w.rows.length,0))bad();
 const seen=new Set(),works=[];
 for(const m of value.mappings){
  if(!exact(m,['work_id','based_on_revision_id','approved_line_id','rate_family','rate_code','component_key','current_revision_id','current_approved_line_id'])||![m.work_id,m.based_on_revision_id,m.current_revision_id,m.current_approved_line_id].every(id)||!identity(m)||!text(m.component_key)||[...m.component_key].length<1||[...m.component_key].length>256)bad();
  const old=args.works.find(w=>w.work_id===m.work_id&&w.expected_revision_id===m.based_on_revision_id),original=old?.rows.find(r=>identityKey(r)===identityKey(m)),k=JSON.stringify([m.work_id,identityKey(m)]);if(!original||seen.has(k))bad();seen.add(k);
  let w=works.find(w=>w.work_id===m.work_id);if(!w){w={work_id:m.work_id,expected_revision_id:m.current_revision_id,rows:[]};works.push(w);}if(w.expected_revision_id!==m.current_revision_id)bad();
  w.rows.push({approved_line_id:m.current_approved_line_id,rate_family:m.rate_family,rate_code:m.rate_code,intent:null});
 }
 const read={resolution:{candidate_id:args.candidate_id,works,reason:null}};normalizeResolutionInput(read);validateResolutionResponse(value.facts,read,actorId);
 if(['agency_id','actor_user_id','candidate_id','owner_epoch'].some(k=>value.facts.context[k]!==args.context[k]))bad();
 for(const m of value.mappings){const w=value.facts.works.find(w=>w.work_id===m.work_id&&w.expected_revision_id===m.current_revision_id),r=w?.rows.find(r=>r.approved_line_id===m.current_approved_line_id&&r.rate_family===m.rate_family&&r.rate_code===m.rate_code);if(!r||r.original_basis.component_key!==m.component_key)bad();}
 return value;
}

const commandPairs=new Map(nativePairs);
function commandErrors(state,names,status,retryable=false){for(const name of names.split(' '))commandPairs.set(state+':BPAY_NEXT_'+name,{status,retryable});}
commandErrors('22023','WORKBENCH_COMMAND_INPUT_INVALID SOURCE_BOOK_SEED_ID_REQUIRED',400);
commandErrors('23514','RESOLUTION_REPLAY_CONFLICT RESOLUTION_DUPLICATE_SCOPE RESOLUTION_ZERO_CONFIRMATION_REQUIRED RESOLUTION_RESIDUAL_AMBIGUOUS SOURCE_BOOK_PAID_WITNESS_MISMATCH SOURCE_BOOK_HELD_WITNESS_MISMATCH SOURCE_BOOK_COMPONENT_MISMATCH SOURCE_BOOK_SEED_REPLAY_CONFLICT',409);
commandErrors('42501','COMMAND_FORBIDDEN',403);
commandErrors('55000','BINDING_NEXT_REQUIRED SOURCE_BOOK_CONTROL_REQUIRED SOURCE_BOOK_SEED_BASIS_UNAVAILABLE SOURCE_BOOK_SIGNED_SEED_UNAVAILABLE SOURCE_BOOK_TYPED_WITNESS_REQUIRED SOURCE_BOOK_ZERO_BASIS_UNAVAILABLE SOURCE_BOOK_ZERO_COMPONENT_UNAVAILABLE',409);
commandErrors('55P03','CANDIDATE_FINANCIAL_FENCE_BUSY',409,true);
export function resolutionCommandNativeFault(payload,args,actorId){const rule=commandPairs.get(payload?.code+':'+payload?.message);if(!rule)return null;const error=fault(payload.message,rule.status,rule.retryable);if(payload.code==='23514'&&staleResolutionCodes.includes(payload.message)&&typeof payload.details==='string'){try{if(encoder.encode(payload.details).length<=120000)error.changedEvidence=validateResolutionChangedEvidence(JSON.parse(payload.details),args,actorId);}catch{/* Known refusal stays definite; malformed detail grants no current authority. */}}return error;}
export function validateResolutionCommandFault(value,args,actorId){
  if(!exact(value,Object.hasOwn(value??{},'changed_evidence')?['ok','error_code','retryable','outcome_unknown','changed_evidence']:['ok','error_code','retryable','outcome_unknown'])||value.ok!==false||typeof value.outcome_unknown!=='boolean'||typeof value.retryable!=='boolean')throw fault('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');
  const rules=[...commandPairs].filter(([key])=>key.slice(6)===value.error_code).map(([,rule])=>rule);
  const generic={BPAY_NEXT_WORKBENCH_UNAVAILABLE:true,BPAY_NEXT_WORKBENCH_RESPONSE_INVALID:false,BPAY_NEXT_WORKBENCH_REQUEST_INVALID:false,BPAY_NEXT_WORKBENCH_FORBIDDEN:false,BPAY_NEXT_WORKBENCH_UNAUTHORIZED:false,BPAY_NEXT_WORKBENCH_METHOD_NOT_ALLOWED:false};
  if(rules.length?value.outcome_unknown||!rules.some(r=>r.retryable===value.retryable):!Object.hasOwn(generic,value.error_code)||generic[value.error_code]!==value.retryable)throw fault('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');
  if(Object.hasOwn(value,'changed_evidence')){if(value.outcome_unknown||value.retryable||!staleResolutionCodes.includes(value.error_code))throw fault('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');validateResolutionChangedEvidence(value.changed_evidence,args,actorId);}
  return value;
}
export const BPAY_NEXT_RESOLUTION_REFUSALS=Object.freeze([...new Set([...nativePairs.keys()].map(k=>k.slice(6)))]);
export function resolutionNativeFault(payload){const rule=nativePairs.get(payload?.code+':'+payload?.message);return rule?fault(payload.message,rule.status,rule.retryable,rule.reload):fault('BPAY_NEXT_WORKBENCH_UNAVAILABLE',503,true);}
export function validateResolutionFault(payload){
  if(!exact(payload,['ok','error_code','retryable','reload_required','workbench_revision','complete','projection_state','dirty_candidate_count','dirty_group_count','pending_task_count'])
    ||payload.ok!==false||payload.complete!==false||typeof payload.retryable!=='boolean'||typeof payload.reload_required!=='boolean'
    ||!['workbench_revision','projection_state','dirty_candidate_count','dirty_group_count','pending_task_count'].every(k=>payload[k]===null)
    ||!BPAY_NEXT_RESOLUTION_REFUSALS.includes(payload.error_code)&&!['BPAY_NEXT_WORKBENCH_UNAVAILABLE','BPAY_NEXT_WORKBENCH_RESPONSE_INVALID','BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE','BPAY_NEXT_WORKBENCH_REQUEST_INVALID','BPAY_NEXT_WORKBENCH_FORBIDDEN','BPAY_NEXT_WORKBENCH_UNAUTHORIZED','BPAY_NEXT_WORKBENCH_METHOD_NOT_ALLOWED'].includes(payload.error_code))throw fault('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');
  const native=[...nativePairs.entries()].filter(([key])=>key.slice(6)===payload.error_code).map(([,rule])=>rule);
  const generic={BPAY_NEXT_WORKBENCH_UNAVAILABLE:{retryable:true,reload:false},BPAY_NEXT_WORKBENCH_RESPONSE_INVALID:{retryable:false,reload:false},BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE:{retryable:false,reload:false},BPAY_NEXT_WORKBENCH_REQUEST_INVALID:{retryable:false,reload:false},BPAY_NEXT_WORKBENCH_FORBIDDEN:{retryable:false,reload:false},BPAY_NEXT_WORKBENCH_UNAUTHORIZED:{retryable:false,reload:false},BPAY_NEXT_WORKBENCH_METHOD_NOT_ALLOWED:{retryable:false,reload:false}};
  const rules=native.length?native:[generic[payload.error_code]];
  if(!rules.some(rule=>rule&&payload.retryable===rule.retryable&&payload.reload_required===rule.reload))throw fault('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');return payload;
}

export {validateResolutionChangedEvidence};

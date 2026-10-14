(function (root) {
  'use strict';
  const CONTRACT = 'WEEKLY_SOURCE_INVOICE_ADJUSTMENTS_V1';
  const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  const own = (value, key) => Object.prototype.hasOwnProperty.call(value, key);
  function context(source) {
    const value = source?.adjustments;
    if (source?.is_weekly_source_invoice !== true || source.editable !== true
      || !value || typeof value !== 'object' || Array.isArray(value)
      || Object.keys(value).some(key => !['contract_version','allowed','credits_shift_available','default_vat_rate_pct'].includes(key))
      || value.contract_version !== CONTRACT || value.allowed !== true || value.credits_shift_available !== false
      || typeof value.default_vat_rate_pct !== 'string' || !/^\d+(?:\.\d+)?$/.test(value.default_vat_rate_pct)) return null;
    return value;
  }
  function decimal(value, negative = false) {
    if (typeof value !== 'string' || !(negative ? /^-\d+(?:\.\d{1,2})?$/ : /^[+-]?\d+(?:\.\d{1,2})?$/).test(value.trim())) throw new Error('Enter an amount with at most two decimal places.');
    const raw = value.trim(), sign = raw.startsWith('-') ? -1n : 1n;
    const [whole, fraction=''] = raw.replace(/^[+-]/,'').split('.');
    const pence = sign * (BigInt(whole)*100n + BigInt(fraction.padEnd(2,'0')));
    if (pence > 999999999999n || pence < -999999999999n || (negative && pence >= 0n)) throw new Error('Enter a minus amount for the Trust credit.');
    return pence;
  }
  function money(pence) {
    const sign = pence < 0n ? '-' : '', absolute = pence < 0n ? -pence : pence;
    return `${sign}${absolute/100n}.${String(absolute%100n).padStart(2,'0')}`;
  }
  // Cosmetic default for the editable box only. The invoice owner computes
  // actual default VAT from frozen movements when no override is supplied.
  function defaultVat(amount, rate) {
    const pence = decimal(amount, true);
    if (typeof rate !== 'string' || !/^\d+(?:\.\d+)?$/.test(rate) || rate.length > 50) throw new Error('VAT is not available.');
    const [whole, fraction=''] = rate.split('.');
    const numerator = (-pence) * BigInt(whole+fraction), denominator = 100n*10n**BigInt(fraction.length);
    return money(-((numerator + denominator/2n)/denominator));
  }
  function payload(value) {
    if (!value || typeof value !== 'object' || Array.isArray(value)
      || Object.keys(value).some(key => !['client_token','description','amount_ex_vat','vat_amount'].includes(key))
      || typeof value.client_token !== 'string' || !/^[A-Za-z0-9:_-]{1,200}$/.test(value.client_token)
      || typeof value.description !== 'string' || !value.description.trim() || value.description.trim().length > 1000) throw new Error('Enter a credit description.');
    const result = {client_token:value.client_token,description:value.description.trim(),amount_ex_vat:money(decimal(value.amount_ex_vat,true))};
    if (own(value,'vat_amount')) result.vat_amount = money(decimal(value.vat_amount));
    return result;
  }
  function rows(items, staged, removed, source, editing, escapeHtml, formatMoney) {
    const allowed = !!context(source) && editing;
    const kept = (Array.isArray(items) ? items : []).filter(item => item.timesheet_id == null && item.booking_id == null
      && String(item.source_key || '').startsWith('ADJ:') && item.meta_json?.weekly_source_adjustment_authority?.schema_version === 'WEEKLY_SOURCE_INVOICE_ADJUSTMENT_AUTHORITY_V1');
    const all = kept.map(item => ({id:item.invoice_line_id,description:item.description || item.meta_json.description,
      ex:item.total_charge_ex_vat,vat:item.vat_amount,token:null})).concat((Array.isArray(staged) ? staged : []).map(item => ({
        id:null,description:item.description,ex:item.amount_ex_vat,
        vat:own(item,'vat_amount') ? item.vat_amount : defaultVat(item.amount_ex_vat,context(source)?.default_vat_rate_pct || '0'),token:item.client_token})));
    if (!all.length) return '';
    return `<div class="mt-3"><div class="fw-semibold mb-2">Trust credits</div><table class="grid" style="width:100%"><thead><tr><th>Description</th><th>Amount (ex VAT)</th><th>VAT</th>${editing?'<th>Remove</th>':''}</tr></thead><tbody>${all.map(item=>{
      const stagedRemoval = !!item.id && removed?.has(String(item.id));
      const action = item.id ? `data-action="inv-toggle-remove-line" data-invoice-line-id="${escapeHtml(item.id)}"` : `data-action="inv-remove-staged-adjustment" data-client-token="${escapeHtml(item.token)}"`;
      return `<tr${stagedRemoval?' class="opacity-50"':''}><td>${escapeHtml(item.description)}${item.token?' <span class="text-muted small">Unsaved</span>':''}</td><td>${escapeHtml(formatMoney(Number(item.ex)))}</td><td>${escapeHtml(formatMoney(Number(item.vat)))}</td>${editing?`<td><button type="button" class="btn btn-sm btn-outline-secondary" ${action} ${allowed && (!item.id||uuid.test(item.id))?'':'disabled'}>${stagedRemoval?'Keep':'Remove'}</button></td>`:''}</tr>`;
    }).join('')}</tbody></table></div>`;
  }
  async function open(modalCtx, deps) {
    const source = deps.invoiceData.weekly_source_invoice, admission = context(source);
    if (!admission || !deps.canEdit) return;
    const html = `<div class="p-3"><div class="h6 mb-3">Trust credit</div><label class="form-label" for="wsAdjDescription">Description</label><input id="wsAdjDescription" class="form-control" maxlength="1000"><label class="form-label mt-2" for="wsAdjAmount">Amount (ex VAT)</label><input id="wsAdjAmount" class="form-control" type="text" inputmode="decimal" placeholder="-0.00"><div class="small text-muted">Enter a minus amount.</div><label class="form-label mt-2" for="wsAdjVat">VAT</label><input id="wsAdjVat" class="form-control" type="text" inputmode="decimal"><div class="small text-muted">VAT is calculated from the invoice. You can change it.</div><div id="wsAdjError" class="alert alert-warning mt-2" hidden></div><div class="d-flex justify-content-end gap-2 mt-3"><button id="wsAdjCancel" type="button" class="btn btn-secondary">Cancel</button><button id="wsAdjAdd" type="button" class="btn btn-primary">Add credit</button></div></div>`;
    deps.showModal('Trust credit',[{key:'main',label:'Credit'}],()=>html,null,true,null,{kind:'invoice-batch-add-adjustment',noParentGate:true,showSave:false,showApply:true});
    const get = id => deps.document.getElementById(id);
    let overwritten = false;
    get('wsAdjVat').oninput = () => {overwritten=true;};
    get('wsAdjAmount').oninput = () => {if (!overwritten) {try {get('wsAdjVat').value=defaultVat(get('wsAdjAmount').value,admission.default_vat_rate_pct);} catch {get('wsAdjVat').value='';}}};
    get('wsAdjCancel').onclick = deps.close;
    get('wsAdjAdd').onclick = () => {
      try {
        const value={client_token:deps.newToken(),description:get('wsAdjDescription').value,amount_ex_vat:get('wsAdjAmount').value};
        if (overwritten) value.vat_amount=get('wsAdjVat').value;
        const item=payload(value);
        modalCtx.invoiceEdits=modalCtx.invoiceEdits||{};
        modalCtx.invoiceEdits.add_adjustments=Array.isArray(modalCtx.invoiceEdits.add_adjustments)?modalCtx.invoiceEdits.add_adjustments:[];
        modalCtx.invoiceEdits.add_adjustments.push(item);
        deps.close();deps.rerender();
      } catch(error) {get('wsAdjError').textContent=error.message;get('wsAdjError').hidden=false;}
    };
  }
  root.CloudTmsSourceInvoiceAdjustments=Object.freeze({context,payload,defaultVat,rows,open});
})(typeof window==='undefined'?globalThis:window);

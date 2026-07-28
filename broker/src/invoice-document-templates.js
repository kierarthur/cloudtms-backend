import { validateFrozenPresentationModel } from './invoice-presentation-contract.js';

const ESCAPE = Object.freeze({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' });
export function escapeInvoiceDocumentHtml(value) { return String(value ?? '').replace(/[&<>"']/g, character => ESCAPE[character]); }
export const escapeInvoiceHtml = escapeInvoiceDocumentHtml;

export function formatFrozenDocumentValue(value, options = {}) {
  if (value == null || value === '') return options.empty ?? '';
  if (options.kind === 'money') {
    const number = Number(value); if (!Number.isFinite(number)) return '';
    return new Intl.NumberFormat('en-GB', { style: 'currency', currency: options.currency || 'GBP', minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(number);
  }
  if (options.kind === 'number') {
    const number = Number(value); if (!Number.isFinite(number)) return '';
    return new Intl.NumberFormat('en-GB', { minimumFractionDigits: options.minimumFractionDigits ?? 0, maximumFractionDigits: options.maximumFractionDigits ?? 2 }).format(number);
  }
  if (options.kind === 'date') {
    const match = /^(\d{4})-(\d{2})-(\d{2})/.exec(String(value));
    return match ? `${match[3]}/${match[2]}/${match[1]}` : String(value);
  }
  return String(value);
}

function renderAddress(address) {
  if (address == null || address === '') return '';

  const lines = (() => {
    if (Array.isArray(address)) return address;

    if (typeof address === 'string') {
      return address
        .split(/\r?\n|,\s*(?=[A-Za-z0-9])/)
        .map(line => line.trim())
        .filter(Boolean);
    }

    if (address && typeof address === 'object') {
      return [
        address.name,
        address.line1 || address.address_line_1 || address.address1,
        address.line2 || address.address_line_2 || address.address2,
        address.line3 || address.address_line_3 || address.address3,
        address.line4 || address.address_line_4 || address.address4,
        address.city || address.town,
        address.county,
        address.postcode || address.post_code || address.zip,
        address.country
      ];
    }

    return [String(address)];
  })();

  return lines
    .map(line => String(line ?? '').trim())
    .filter(Boolean)
    .map(line => `<div>${escapeInvoiceHtml(line)}</div>`)
    .join('');
}

export function buildInvoiceTemplateCss() {
  return `@page{size:A4;margin:14mm 13mm 16mm}*{box-sizing:border-box}html,body{margin:0;padding:0;color:#172033;font:10px/1.35 Arial,Helvetica,sans-serif}body{-webkit-print-color-adjust:exact;print-color-adjust:exact}.document{position:relative;width:100%}.draft-watermark{position:fixed;inset:34% 0 auto;transform:rotate(-28deg);text-align:center;font-size:76px;font-weight:700;color:rgba(153,27,27,.12);z-index:-1;letter-spacing:8px}.brand{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:16px;border-bottom:3px solid #153a67;padding-bottom:8px;margin-bottom:12px}.brand img{max-width:180px;max-height:62px;object-fit:contain}.brand-name{color:#153a67;font-size:18px;font-weight:700}.document-kind{color:#153a67;font-size:24px;font-weight:800;text-align:right}.identity,.references,.meta{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:6px;margin-bottom:12px}.field,.party,.payment,.legal,.verification{border:1px solid #d7deea;border-radius:4px;padding:7px}.label,.party-title{color:#5a6578;font-size:8px;font-weight:700;text-transform:uppercase;letter-spacing:.45px}.value{margin-top:3px;font-weight:600;overflow-wrap:anywhere}.parties{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:12px}.party{min-height:100px}.party-title{color:#153a67;margin-bottom:5px}table{width:100%;border-collapse:collapse;table-layout:fixed}thead{display:table-header-group}tr{break-inside:avoid}th{background:#153a67;color:#fff;padding:6px 5px;font-size:8px;text-align:left;text-transform:uppercase}td{border-bottom:1px solid #e0e5ee;padding:6px 5px;vertical-align:top;overflow-wrap:anywhere}.number{text-align:right}.summary-grid{display:grid;grid-template-columns:minmax(0,1fr) 245px;gap:14px;margin-top:14px;break-inside:avoid}.vat-table th{background:#e9eef6;color:#172033}.totals{border:1px solid #cbd4e2;border-radius:5px;overflow:hidden}.total-row{display:flex;justify-content:space-between;padding:7px 9px;border-bottom:1px solid #e0e5ee}.total-row.grand{background:#153a67;color:white;font-size:13px;font-weight:800}.payment,.legal,.verification{margin-top:12px}.legal{color:#525d70;font-size:8px}.attachment-index,.source-page{break-before:page}.attachment-index h1,.source-page h1{color:#153a67;font-size:20px}.separator{height:245mm;display:flex;flex-direction:column;justify-content:center;align-items:center;text-align:center;border:4px solid #153a67}.separator h1{font-size:30px;color:#153a67}.signature-grid{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:12px}.signature img{max-width:180px;max-height:70px}.subheading{color:#153a67;font-size:13px;margin:12px 0 6px}`;
}

function field(label, value, kind) { return `<div class="field"><div class="label">${escapeInvoiceHtml(label)}</div><div class="value">${escapeInvoiceHtml(formatFrozenDocumentValue(value,{kind}))}</div></div>`; }
function docLabel(model) { return model.document_type === 'CREDIT_NOTE' ? 'CREDIT NOTE' : model.document_type === 'SELF_BILL_INVOICE' ? 'SELF-BILL INVOICE' : 'INVOICE'; }

export function renderInvoiceBrandHeader(model = {}) {
  const supplier = model.supplier || {};
  const logo = model.branding?.logo?.data_url || '';
  return `<header class="brand"><div>${logo ? `<img alt="" src="${escapeInvoiceHtml(logo)}">` : ''}<div class="brand-name">${escapeInvoiceHtml(supplier.legal_name)}</div></div><div class="document-kind">${escapeInvoiceHtml(docLabel(model))}</div></header>`;
}
export function renderInvoiceIdentityPanel(model = {}) {
  return `<section class="identity">${field(model.document_type==='CREDIT_NOTE' ? 'Credit note number' : 'Invoice number',model.invoice_number)}${field('Issue date',model.issue_date,'date')}${field('Tax point',model.tax_point,'date')}${field('Due date',model.due_date,'date')}</section>`;
}
export function renderInvoicePartyBlocks(model = {}) {
  const supplier = model.supplier || {};
  const customer = model.customer || {};
  return `<section class="parties"><div class="party"><div class="party-title">Supplier</div><div>${escapeInvoiceHtml(supplier.legal_name)}</div>${renderAddress(supplier.registered_address)}</div><div class="party"><div class="party-title">Bill to</div><div>${escapeInvoiceHtml(customer.legal_name)}</div>${renderAddress(customer.billing_address)}</div></section>`;
}
export function renderInvoiceReferencePanel(model = {}) {
  const references = model.references || {};
  const credit = model.credit_note || {};
  const workers = [...new Set((Array.isArray(model.lines) ? model.lines : [])
    .map(line => String(line?.worker || '').trim())
    .filter(Boolean))];
  const candidateSummary = workers.length === 1
    ? workers[0]
    : workers.length === 0
      ? String(model.candidate_summary || '').trim()
      : '';
  const cells = [
    references.purchase_order ? field('Purchase order', references.purchase_order) : '',
    references.client_reference ? field('Client reference', references.client_reference) : '',
    references.work_location ? field('Work location', references.work_location) : '',
    candidateSummary ? field('Candidate / worker', candidateSummary) : '',
    field('Payment terms', model.payment?.terms_text),
    field('Currency', model.currency),
    credit.is_credit_note ? field('Original invoice', credit.original_invoice_number) : '',
    credit.is_credit_note ? field('Original invoice date', credit.original_invoice_date, 'date') : ''
  ].join('');
  return `<section class="references">${cells}</section>${credit.is_credit_note ? `<section class="payment"><strong>Credit reason:</strong> ${escapeInvoiceHtml(credit.reason)}</section>` : ''}`;
}
export function renderInvoiceLineTable(model = {}) {
  const currency = model.currency;
  const rows = Array.isArray(model.lines) ? model.lines : [];
  return `<table class="invoice-lines"><thead><tr><th>Rate / item type</th><th>Worker</th><th>Reference</th><th>Unit</th><th class="number">Units</th><th class="number">Cost per unit</th><th class="number">Net</th><th class="number">VAT</th><th class="number">Total</th></tr></thead><tbody>${rows.map(line=>`<tr><td>${escapeInvoiceHtml(line.description)}</td><td>${escapeInvoiceHtml(line.worker)}</td><td>${escapeInvoiceHtml(line.reference)}</td><td>${escapeInvoiceHtml(line.unit)}</td><td class="number">${escapeInvoiceHtml(formatFrozenDocumentValue(line.quantity,{kind:'number',maximumFractionDigits:4}))}</td><td class="number">${escapeInvoiceHtml(formatFrozenDocumentValue(line.unit_price,{kind:'money',currency}))}</td><td class="number">${escapeInvoiceHtml(formatFrozenDocumentValue(line.net_amount,{kind:'money',currency}))}</td><td class="number">${escapeInvoiceHtml(formatFrozenDocumentValue(line.vat_amount,{kind:'money',currency}))}</td><td class="number">${escapeInvoiceHtml(formatFrozenDocumentValue(line.gross_amount,{kind:'money',currency}))}</td></tr>`).join('')}</tbody></table>`;
}
export function renderInvoiceVatBreakdown(model={}) { const currency=model.currency; const rows=model.vat_breakdown; return rows.length?`<table class="vat-table"><thead><tr><th>VAT rate</th><th class="number">Net</th><th class="number">VAT</th><th class="number">Gross</th></tr></thead><tbody>${rows.map(row=>`<tr><td>${escapeInvoiceHtml(formatFrozenDocumentValue(row.rate,{kind:'number'}))}%</td><td class="number">${escapeInvoiceHtml(formatFrozenDocumentValue(row.net_amount,{kind:'money',currency}))}</td><td class="number">${escapeInvoiceHtml(formatFrozenDocumentValue(row.vat_amount,{kind:'money',currency}))}</td><td class="number">${escapeInvoiceHtml(formatFrozenDocumentValue(row.gross_amount,{kind:'money',currency}))}</td></tr>`).join('')}</tbody></table>`:''; }
export function renderInvoiceTotalsPanel(model={}) { const t=model.totals; const currency=model.currency; const row=(label,value,css='')=>`<div class="total-row ${css}"><span>${escapeInvoiceHtml(label)}</span><span>${escapeInvoiceHtml(formatFrozenDocumentValue(value,{kind:'money',currency}))}</span></div>`; return `<div class="totals">${row('Net',t.net)}${row('VAT',t.vat)}${row('Gross',t.gross)}${row('Amount paid',t.amount_paid)}${row('Amount credited',t.amount_credited)}${row('Amount outstanding',t.amount_outstanding,'grand')}</div>`; }
export function renderInvoicePaymentPanel(model={}) { const p=model.payment||{}; return Object.values(p).some(Boolean)?`<section class="payment"><div class="party-title">Payment and remittance</div>${p.terms_text?`<div>Terms: ${escapeInvoiceHtml(p.terms_text)}</div>`:''}${p.due_date_basis?`<div>Due-date basis: ${escapeInvoiceHtml(p.due_date_basis)}</div>`:''}${p.instructions?`<div>${escapeInvoiceHtml(p.instructions)}</div>`:''}${p.account_name?`<div>Account name: ${escapeInvoiceHtml(p.account_name)}</div>`:''}${p.sort_code?`<div>Sort code: ${escapeInvoiceHtml(p.sort_code)}</div>`:''}${p.account_number?`<div>Account number: ${escapeInvoiceHtml(p.account_number)}</div>`:''}${p.remittance_reference?`<div>Reference: ${escapeInvoiceHtml(p.remittance_reference)}</div>`:''}${p.remittance_email?`<div>Remittance: ${escapeInvoiceHtml(p.remittance_email)}</div>`:''}</section>`:''; }
export function renderInvoiceLegalFooter(model={}) { const s=model.supplier; const lines=[...(model.self_bill.is_self_bill?[model.self_bill.legal_wording]:[]),...model.legal_wording].filter(Boolean); const identity=[s.legal_name,s.company_registration_number?`Company ${s.company_registration_number}`:null,s.vat_registration_number?`VAT ${s.vat_registration_number}`:null,model.template_version?`Template ${model.template_version}`:null].filter(Boolean).join(' · '); return `<footer class="legal">${lines.map(line=>`<div>${escapeInvoiceHtml(line)}</div>`).join('')}${identity?`<div>${escapeInvoiceHtml(identity)}</div>`:''}</footer>`; }

export function renderInvoiceAttachmentIndex(entries=[]) { return Array.isArray(entries)&&entries.length?`<section class="attachment-index"><h1>Attachment index</h1><table><thead><tr><th>No.</th><th>Worker / source</th><th>Week / date</th><th>Document type</th><th>Evidence</th><th>Reference</th><th class="number">Start</th><th class="number">Pages</th></tr></thead><tbody>${entries.map((e,i)=>`<tr data-row-id="${escapeInvoiceHtml(e.row_id||'')}"><td>${escapeInvoiceHtml(e.attachment_number??i+1)}</td><td>${escapeInvoiceHtml(e.worker||e.source||'')}</td><td>${escapeInvoiceHtml(formatFrozenDocumentValue(e.week_or_date,{kind:'date'}))}</td><td>${escapeInvoiceHtml(e.document_type||'')}</td><td>${escapeInvoiceHtml(e.evidence_description||'')}</td><td>${escapeInvoiceHtml(e.reference||'')}</td><td class="number">${escapeInvoiceHtml(e.start_page??'')}</td><td class="number">${escapeInvoiceHtml(e.page_count??'')}</td></tr>`).join('')}</tbody></table></section>`:''; }
export function renderInvoicePageNumbering() { return ''; }

export function buildProfessionalInvoiceHtml(model = {}) {
  const original = validateFrozenPresentationModel('INVOICE_CORE', model);
  const m = structuredClone(original);

  if (m.payment?.hide_bank_footer === true) {
    m.payment = {
      ...m.payment,
      account_name: '',
      sort_code: '',
      account_number: '',
      remittance_reference: ''
    };
  }

  const legalWarnings = [];
  if (m.document_type === 'SELF_BILL_INVOICE' && !String(m.self_bill?.legal_wording || '').trim()) {
    legalWarnings.push('Self-bill legal wording is missing.');
  }
  if (Number(m.totals?.vat || 0) !== 0 && !String(m.supplier?.vat_registration_number || '').trim()) {
    legalWarnings.push('Supplier VAT registration number is missing.');
  }
  if (legalWarnings.length) {
    const error = new Error('INVOICE_PRESENTATION_LEGAL_FIELD_MISSING');
    error.code = 'INVOICE_PRESENTATION_LEGAL_FIELD_MISSING';
    error.detail = legalWarnings;
    throw error;
  }

  const paymentPanel = m.payment?.hide_bank_footer === true
    ? ''
    : renderInvoicePaymentPanel(m);

  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<style>${buildInvoiceTemplateCss()}</style>
</head>
<body>
${m.is_draft ? '<div class="draft-watermark">DRAFT</div>' : ''}
<main class="document">
  ${renderInvoiceBrandHeader(m)}
  ${renderInvoiceIdentityPanel(m)}
  ${renderInvoicePartyBlocks(m)}
  ${renderInvoiceReferencePanel(m)}
  ${renderInvoiceLineTable(m)}
  <section class="summary-grid">
    <div>${renderInvoiceVatBreakdown(m)}</div>
    ${renderInvoiceTotalsPanel(m)}
  </section>
  ${paymentPanel}
  ${renderInvoiceLegalFooter(m)}
</main>
</body>
</html>`;
}

function signatureBlock(label,signature={}) { const src=signature.data_url||''; return `<div class="signature"><div class="party-title">${escapeInvoiceHtml(label)}</div>${src?`<img alt="" src="${escapeInvoiceHtml(src)}">`:''}<div>${escapeInvoiceHtml(signature.name||signature.identity||'')}</div><div>${escapeInvoiceHtml(signature.role||'')}</div></div>`; }
function formatFrozenTime(value) {
  if (value == null || value === '') return '';
  const text = String(value);
  const match = /(?:^|[T\s])(\d{2}):(\d{2})(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}(?::?\d{2})?)?$/.exec(text);
  return match ? `${match[1]}:${match[2]}` : text;
}
function scheduleRows(rows=[]) { return `<table><thead><tr><th>Date</th><th>Scheduled</th><th>Worked</th><th>Break</th><th>Reference</th><th class="number">Hours / units</th></tr></thead><tbody>${rows.map(row=>`<tr><td>${escapeInvoiceHtml(formatFrozenDocumentValue(row.date,{kind:'date'}))}</td><td>${escapeInvoiceHtml([formatFrozenTime(row.scheduled_start),formatFrozenTime(row.scheduled_end)].filter(Boolean).join(' – '))}</td><td>${escapeInvoiceHtml([formatFrozenTime(row.worked_start||row.start),formatFrozenTime(row.worked_end||row.end)].filter(Boolean).join(' – '))}</td><td>${escapeInvoiceHtml(row.break_display||[formatFrozenTime(row.break_start),formatFrozenTime(row.break_end),row.break_minutes!=null?`${row.break_minutes} min`:null].filter(Boolean).join(' / '))}</td><td>${escapeInvoiceHtml(row.reference||row.day_reference||'')}</td><td class="number">${escapeInvoiceHtml(formatFrozenDocumentValue(row.hours??row.units,{kind:'number'}))}</td></tr>`).join('')}</tbody></table>`; }
export function buildElectronicTimesheetHtml(model = {}) {
  const m = validateFrozenPresentationModel('ELECTRONIC_TIMESHEET', model);
  const auth = m.authorisation || {};

  const referenceText = rows => (Array.isArray(rows) ? rows : [])
    .map(row => {
      if (typeof row === 'string') return row;
      const label = row.day_key || row.segment_id || row.row_key || '';
      const reference = row.reference || row.current_reference || '';
      return [label, reference].filter(Boolean).join(': ');
    })
    .filter(Boolean)
    .join(', ');

  const renderScheduleSection = (title, rows) => {
    if (!Array.isArray(rows) || rows.length === 0) return '';
    return `${title ? `<h2 class="subheading">${escapeInvoiceHtml(title)}</h2>` : ''}${scheduleRows(rows)}`;
  };

  const additionalUnits = (() => {
    const describeEntry = (entry, index = null) => {
      if (entry == null || entry === '') return '';
      if (typeof entry === 'string') return entry;
      if (typeof entry !== 'object') return formatFrozenDocumentValue(entry, { kind: 'number' });
      if (Object.keys(entry).length === 0) return '';
      return [
        entry.label || entry.description || entry.type || entry.row_key || (index != null ? `Unit ${index + 1}` : 'Additional units'),
        entry.units ?? entry.hours ?? entry.quantity,
        entry.reference
      ].filter(value => value != null && value !== '').join(' · ');
    };
    if (Array.isArray(m.additional_units)) return m.additional_units.map(describeEntry).filter(Boolean).join(', ');
    if (m.additional_units && typeof m.additional_units === 'object') {
      const descriptorKeys = ['label', 'description', 'type', 'row_key', 'units', 'hours', 'quantity', 'reference'];
      if (descriptorKeys.some(key => Object.hasOwn(m.additional_units, key))) return describeEntry(m.additional_units);
      return Object.entries(m.additional_units)
        .map(([label, value], index) => describeEntry(
          value && typeof value === 'object' ? { label, ...value } : { label, units: value },
          index
        ))
        .filter(Boolean)
        .join(', ');
    }
    return describeEntry(m.additional_units);
  })();
  const authoriser = [auth.name, auth.role].filter(Boolean).join(' ');
  const authorisedAt = auth.authorised_at_utc
    ? formatFrozenDocumentValue(auth.authorised_at_utc, { kind: 'date' })
    : '';

  return `<section class="source-page">
<h1>Electronic timesheet</h1>
<div class="meta">
  ${field('Candidate', m.candidate.name)}
  ${field('Candidate ID', m.candidate.id)}
  ${field('Client', m.client.name)}
  ${field('Contract', m.contract.reference)}
  ${field('Hospital / site', m.work.hospital || m.work.site)}
  ${field('Ward', m.work.ward)}
  ${field('Assignment / job', m.work.assignment || m.work.job_title)}
  ${field('Band / shift type', [m.work.band, m.work.shift_type].filter(Boolean).join(' / '))}
  ${field('Week ending', m.week_ending_date, 'date')}
  ${field('Submission mode', m.submission_mode)}
  ${field('Sheet scope', m.sheet_scope)}
  ${field('Document revision', m.document_revision)}
</div>
${renderScheduleSection(m.daily_schedule_rows?.length && m.weekly_schedule_rows?.length ? 'Daily schedule' : '', m.daily_schedule_rows)}
${renderScheduleSection('Weekly schedule', m.weekly_schedule_rows)}
<section class="verification">
  <div>Whole reference: ${escapeInvoiceHtml(m.references.whole || '')}</div>
  <div>Day references: ${escapeInvoiceHtml(referenceText(m.references.day))}</div>
  <div>Segment references: ${escapeInvoiceHtml(referenceText(m.references.segment))}</div>
  ${additionalUnits ? `<div>Additional units: ${escapeInvoiceHtml(additionalUnits)}</div>` : ''}
  <div>QR required: ${m.qr.required ? 'Yes' : 'No'} · QR signed: ${m.qr.signed ? 'Yes' : 'No'}</div>
  ${m.qr.status ? `<div>QR status: ${escapeInvoiceHtml(m.qr.status)}</div>` : ''}
  ${m.qr.signed_hash ? `<div>QR signature: ${escapeInvoiceHtml(m.qr.signed_hash)}</div>` : ''}
  ${m.qr.signed_at_utc ? `<div>QR signed at: ${escapeInvoiceHtml(formatFrozenDocumentValue(m.qr.signed_at_utc, { kind: 'date' }))}</div>` : ''}
  ${m.qr.verification_summary ? `<div>${escapeInvoiceHtml(m.qr.verification_summary)}</div>` : ''}
  <div>Authorised: ${auth.authorised ? 'Yes' : 'No'}${authorisedAt ? ` · ${escapeInvoiceHtml(authorisedAt)}` : ''}${authoriser ? ` · by ${escapeInvoiceHtml(authoriser)}` : ''}</div>
</section>
<div class="signature-grid">
  ${signatureBlock('Candidate / nurse signature', m.signatures.candidate)}
  ${signatureBlock('Authoriser signature', m.signatures.authoriser)}
</div>
</section>`;
}

export function buildAttachmentIndexHtml(model={}) { const m=validateFrozenPresentationModel('ATTACHMENT_INDEX',model); return `<section class="source-page">${renderInvoiceAttachmentIndex(m.display_rows)}</section>`; }
export function buildSectionSeparatorHtml(model={}) { return `<section class="separator"><h1>${escapeInvoiceHtml(model.title||model.label||'Supporting document')}</h1>${model.subtitle?`<p>${escapeInvoiceHtml(model.subtitle)}</p>`:''}${model.reference?`<p>Reference: ${escapeInvoiceHtml(model.reference)}</p>`:''}</section>`; }
function supportTable(title,model,columns) { const rows=Array.isArray(model.rows)?model.rows:[]; return `<section class="source-page"><h1>${escapeInvoiceHtml(title)}</h1><table><thead><tr>${columns.map(c=>`<th>${escapeInvoiceHtml(c.label)}</th>`).join('')}</tr></thead><tbody>${rows.map(row=>`<tr>${columns.map(c=>`<td>${escapeInvoiceHtml(formatFrozenDocumentValue(row?.[c.key],{kind:c.kind}))}</td>`).join('')}</tr>`).join('')}</tbody></table></section>`; }
export function buildHealthRosterSupportHtml(model={}) { const m=validateFrozenPresentationModel('HEALTHROSTER_SUPPORT',model); return supportTable('HealthRoster support',m,[['worker','Worker'],['assignment','Assignment'],['shift_date','Date','date'],['shift_times','Shift'],['site','Site'],['ward','Ward'],['reference','Reference'],['units_hours','Units / hours'],['validation_state','Validation'],['source_identity','Source']].map(([key,label,kind])=>({key,label,kind}))); }
export function buildNhspSupportHtml(model={}) { const m=validateFrozenPresentationModel('NHSP_SUPPORT',model); return supportTable('NHSP support',m,[['worker','Worker'],['nhsp_shift_id','Shift ID'],['booking_reference','Booking / reference'],['site_ward','Site / ward'],['shift_date','Date','date'],['shift_times','Times'],['hours_units','Hours / units'],['source_identity','Source'],['validation_state','Validation']].map(([key,label,kind])=>({key,label,kind}))); }
export function buildHigherRateSupportHtml(model={}) { const m=validateFrozenPresentationModel('HIGHER_RATE_SUPPORT',model); return supportTable('Higher-rate support',m,[['worker_source','Worker / source'],['shift_date','Date / shift'],['original_rate','Original rate'],['applied_rate','Applied rate'],['units','Units'],['display_amount','Amount'],['reason','Reason'],['approval_identity','Approval'],['reference','Reference']].map(([key,label,kind])=>({key,label,kind}))); }
export function buildInvoiceSourceDocumentHtml(renderKind,model={}) { const kind=String(renderKind||'').toUpperCase(); validateFrozenPresentationModel(kind,model); const builder={ELECTRONIC_TIMESHEET:buildElectronicTimesheetHtml,ATTACHMENT_INDEX:buildAttachmentIndexHtml,SECTION_SEPARATOR:buildSectionSeparatorHtml,HEALTHROSTER_SUPPORT:buildHealthRosterSupportHtml,NHSP_SUPPORT:buildNhspSupportHtml,HIGHER_RATE_SUPPORT:buildHigherRateSupportHtml}[kind]; if(!builder) throw new Error(`UNSUPPORTED_INVOICE_SOURCE_RENDER_KIND:${kind||'EMPTY'}`); return `<!doctype html><html lang="en"><head><meta charset="utf-8"><style>${buildInvoiceTemplateCss()}</style></head><body>${builder(model)}</body></html>`; }

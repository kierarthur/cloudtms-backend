/*
 * Deterministic renderer for 04_MODAL_POLICY.json.
 * Screen words, data, ordering, dimensions and visual tokens come from the policy.
 * This file contains only generic component mechanics.
 */
const fs = require('fs');
const path = require('path');

const packDir = path.resolve(__dirname, '..');
const policyPath = path.join(packDir, '04_MODAL_POLICY.json');
const emailPolicyPath = path.join(packDir, '04A_MANAGER_EMAIL_POLICY.json');
const emailPolicy = JSON.parse(fs.readFileSync(emailPolicyPath, 'utf8'));
const outputDir = path.join(packDir, 'assets', 'modal-images');
const tempDir = process.env.CLOUDTMS_MODAL_TEMP_DIR || path.resolve(packDir, '..', '..', '..', 'tmp', 'pdfs', 'cloudtms-weekly-source-reconciliation', 'modal-html');

function loadPlaywright() {
  try { return require('playwright'); } catch (_) {}
  const configured = process.env.CLOUDTMS_PLAYWRIGHT_MODULE;
  const fallback = 'C:\\Users\\KierArthur\\.cache\\codex-runtimes\\codex-primary-runtime\\dependencies\\node\\node_modules\\playwright';
  return require(configured || fallback);
}

let copy = {};
const RENDERER_COPY_KEYS = ['childHeaderLabels', 'childAcceptButton', 'defaultChildAction', 'candidateActionNeeded', 'managerChooseResponse', 'managerTableHeaders', 'sourceDefaultLabel', 'sourceCurrentBadge', 'summaryCloseButton', 'bulkStatusLine', 'bulkFilterLabels', 'bulkListHeaders', 'bulkRowStates', 'compareMatch', 'compareDifferent', 'compareUnitLabel', 'compareResultLabel'];

const esc = (value) => String(value ?? '')
  .replaceAll('&', '&amp;').replaceAll('<', '&lt;')
  .replaceAll('>', '&gt;').replaceAll('"', '&quot;');

const toneClass = (tone) => `tone-${esc(tone || 'neutral')}`;

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function control(label, value, type = 'select') {
  const readonly = type === 'readonly';
  return `<label class="ctx-field ${readonly ? 'readonly' : ''}"><span>${esc(label)}</span><strong>${esc(value)}</strong>${readonly ? '' : '<b>⌄</b>'}</label>`;
}

function button(item) {
  const disabled = item.disabled ? ' disabled' : '';
  return `<button class="action ${esc(item.tone || 'secondary')}"${disabled}>${esc(item.label)}</button>`;
}

function status(value) {
  if (!value) return '';
  if (typeof value === 'string') return esc(value);
  if (!value.text) return '';
  const subtext = value.subtext ? `<small class="status-detail ${toneClass(value.subtone || 'neutral')}">${esc(value.subtext)}</small>` : '';
  return `<span class="status-stack"><span class="status ${toneClass(value.tone)}">${esc(value.text)}</span>${subtext}</span>`;
}

function asked(value) {
  return value ? '<span class="tick" aria-label="Yes">✓</span>' : '<span class="blank" aria-label="No">&nbsp;</span>';
}

function rowActions(value) {
  if (!value) return '';
  const actions = Array.isArray(value) ? value : [value];
  return `<span class="row-actions">${actions.map(label => `<button class="row-action">${esc(label)}</button>`).join('')}</span>`;
}

function headerCheckbox(state = 'unchecked', accessibleName = 'Select all rows') {
  const checked = state === 'checked';
  const indeterminate = state === 'indeterminate';
  return `<span class="header-check check ${checked || indeterminate ? 'checked' : ''} ${indeterminate ? 'indeterminate' : ''}" role="checkbox" aria-checked="${indeterminate ? 'mixed' : checked ? 'true' : 'false'}" aria-label="${esc(accessibleName)}">${checked ? '✓' : indeterminate ? '−' : ''}</span>`;
}

function valueFor(row, column) {
  if (column.key === 'select') return `<span class="check ${row.selected ? 'checked' : ''} ${row.selectDisabled ? 'disabled-check' : ''}" aria-disabled="${row.selectDisabled ? 'true' : 'false'}">${row.selected ? '✓' : ''}</span>`;
  if (column.key === 'choose') return `<span class="radio-choice" aria-label="${row.choose ? 'Selected' : 'Not selected'}">${row.choose ? '●' : '○'}</span>`;
  if (column.key === 'expand') return `<span class="expand">${row.expanded ? '⌄' : '›'}</span>`;
  if (column.key === 'candidate' && Array.isArray(row.children)) {
    return `<span class="candidate-with-expand"><button class="expand-button" aria-label="${row.expanded ? 'Collapse' : 'Expand'} shifts for ${esc(row.candidate)}" aria-expanded="${row.expanded ? 'true' : 'false'}">${row.expanded ? '⌄' : '›'}</button><span>${esc(row.candidate)}</span></span>`;
  }
  if (column.key === 'candidateAsked' || column.key === 'managerInformed') return asked(row[column.key]);
  if (column.key === 'actions') return rowActions(row[column.key]);
  const value = row[column.key];
  if (value && typeof value === 'object') return status(value);
  return esc(value);
}

function detailNotice(item) {
  if (!item) return '';
  const facts = (item.facts || []).map(fact => `<span class="detail-fact"><small>${esc(fact.label)}</small><strong>${esc(fact.value)}</strong></span>`).join('');
  return `<div class="detail-notice ${toneClass(item.tone)}"><div class="detail-notice-head"><strong>${esc(item.title)}</strong><span>${esc(item.body)}</span></div><div class="detail-notice-facts">${facts}</div></div>`;
}

function table(columns, rows, extraClass = '', selectionPolicy = {}) {
  const groupNames = selectionPolicy.outreachHeaderCheckbox?.accessibleNames || {};
  const shiftNames = selectionPolicy.expandedShiftHeaderCheckbox?.accessibleNames || {};
  const head = columns.map(c => {
    const label = c.headerCheckboxState
      ? headerCheckbox(c.headerCheckboxState, groupNames[c.headerCheckboxState] || 'Select all query groups')
      : esc(c.label);
    return `<th class="col-${esc(c.key)}">${label}${c.sort ? `<span class="sort">${c.sort === 'asc' ? '↑' : '↓'}</span>` : ''}</th>`;
  }).join('');
  const body = rows.map(row => {
    const cells = columns.map(c => `<td class="col-${esc(c.key)}">${valueFor(row, c)}</td>`).join('');
    let children = '';
    if (row.children?.length) {
      const selectedChildren = row.children.filter(ch => ch.selected).length;
      const childHeaderState = selectedChildren === 0 ? 'unchecked' : selectedChildren === row.children.length ? 'checked' : 'indeterminate';
      const [dayLabel, candidateLabel, systemLabel, issueLabel, actionsLabel] = copy.childHeaderLabels;
      const childHeader = `<div class="child-row child-header">
        ${headerCheckbox(childHeaderState, shiftNames[childHeaderState] || 'Select all shifts in this group')}
        <span>${esc(dayLabel)}</span>
        <span>${esc(candidateLabel)}</span>
        <span>${esc(systemLabel)}</span>
        <span>${esc(issueLabel)}</span>
        <span>${esc(actionsLabel)}</span>
      </div>`;
      const childRows = row.children.map(ch => `<div class="child-row">
        <span class="check ${ch.selected ? 'checked' : ''}">${ch.selected ? '✓' : ''}</span>
        <div><strong>${esc(ch.day)}</strong><small>${esc(ch.role)}</small></div>
        <div><small>${esc(candidateLabel)}</small>${esc(ch.candidateHours)}</div>
        <div><small>${esc(systemLabel)}</small>${esc(ch.systemHours)}</div>
        <div><small>${esc(issueLabel)}</small><strong>${esc(ch.issue)}</strong><span class="child-status">${esc(ch.status)}</span></div>
        <div class="child-next-action">${ch.nextAction && typeof ch.nextAction === 'object' ? status(ch.nextAction) : rowActions(ch.nextAction || copy.defaultChildAction)}</div>
      </div>${detailNotice(ch.detailNotice)}`).join('');
      children = `<tr class="child-holder"><td colspan="${columns.length}"><div class="child-grid">${childHeader}${childRows}<div class="child-actions"><button class="action secondary">${esc(copy.childAcceptButton)}</button></div></div></td></tr>`;
    }
    return `<tr>${cells}</tr>${children}`;
  }).join('');
  return `<div class="table-wrap ${esc(extraClass)}"><table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table><div class="scroll-fade"></div></div>`;
}

function officeShell(policy, screen, body) {
  const shared = {...policy.shared, ...(screen.shell || {})};
  const tabs = shared.tabs.map((t, i) => `<button class="tab ${i === screen.activeTab ? 'active' : ''}">${esc(t.label)}</button>`).join('');
  const ctx = (screen.contextFields || shared.context).map(x => control(x.label, x.value, x.type)).join('');
  return `<div class="app-shell"><aside><div class="logo-mark">C</div><div class="rail-dot active"></div><div class="rail-dot"></div><div class="rail-dot"></div><div class="rail-dot"></div></aside><main><div class="app-top"><span>CloudTMS Office</span><span class="user-dot">KA</span></div><section class="modal wide-modal">
    <header class="modal-header"><div><h1>${esc(shared.officeTitle)}</h1><p>${esc(shared.officeSubtitle)}</p></div><button class="close" aria-label="Close weekly source imports">×</button></header>
    <div class="context-bar">${ctx}<span class="cycle ${toneClass(screen.cycleState?.tone)}">${esc(screen.cycleState?.text)}</span></div>
    <nav class="tabs">${tabs}</nav>
    ${body}
  </section></main></div>`;
}

function toolbar(screen) {
  if (!screen.toolbar) return '';
  const filters = (screen.toolbar.filters || []).map(f => control(f.label, f.value)).join('');
  const actions = (screen.toolbar.actions || []).map(button).join('');
  return `<div class="toolbar"><div class="filter-row">${filters}</div><div class="action-row">${actions}</div></div>`;
}

function notice(n) {
  if (!n) return '';
  return `<div class="notice ${toneClass(n.tone)}"><strong>${esc(n.title)}</strong><span>${esc(n.body)}</span></div>`;
}

function footer(data) {
  if (!data) return '';
  return `<footer class="modal-footer"><span>${esc(data.left)}</span><div>${(data.actions || []).map(button).join('')}</div></footer>`;
}

function renderOfficeTable(policy, screen) {
  const body = `${notice(screen.notice)}${toolbar(screen)}${screen.officeChecks ? `<h2 style="margin:12px 24px">${esc(screen.officeChecks.title)}</h2>${table(screen.officeChecks.columns, screen.officeChecks.rows)}` : ''}${screen.hoursQuestionsTitle ? `<h2 style="margin:12px 24px">${esc(screen.hoursQuestionsTitle)}</h2>` : ''}${screen.selectionSummary ? `<div class="selection-summary">${esc(screen.selectionSummary)}</div>` : ''}${table(screen.columns, screen.rows, '', policy.interactionPolicies.querySelection)}${footer(screen.footer)}`;
  return officeShell(policy, screen, body);
}

function mytmsShell(screen, body) {
  return `<div class="mobile-bg"><div class="phone mytms-phone"><div class="mytms-stack-header"><span class="mytms-back" aria-hidden="true">‹</span><strong>${esc(screen.navigationTitle)}</strong></div><div class="mytms-screen"><header class="mytms-screen-heading"><span class="mytms-eyebrow">${esc(screen.eyebrow)}</span><h1>${esc(screen.title)}</h1>${screen.description || screen.context ? `<p>${esc(screen.description || screen.context)}</p>` : ''}</header><div class="mytms-content">${body}</div></div></div></div>`;
}

function renderCandidate(screen) {
  const queryRows = Array.isArray(screen.queryRows) && screen.queryRows.length
    ? screen.queryRows
    : [{ comparisons: screen.comparisons || [], prompt: screen.prompt, choices: screen.choices || [], advice: screen.advice }];
  const queries = queryRows.map(row => {
    const comparisons = (row.comparisons || []).map(x => `<div class="comparison ${toneClass(x.tone)}"><span>${esc(x.label)}</span><strong>${esc(x.time)}</strong><small>${esc(x.break)}</small></div>`).join('');
    const choices = (row.choices || []).map(x => `<label class="choice ${x.selected ? 'selected' : ''}"><span class="radio">${x.selected ? '●' : '○'}</span><span>${esc(x.label)}</span></label>`).join('');
    const edited = row.editedHours ? `<div class="edited-hours"><span>${esc(row.editedHours.start)}</span><span>${esc(row.editedHours.finish)}</span><span>${esc(row.editedHours.break)} min break</span></div>` : '';
    const response = row.advice ? `<div class="mobile-advice"><strong>${esc(copy.candidateActionNeeded)}</strong><span>${esc(row.advice)}</span></div>` : row.result ? `<div class="mobile-result">${esc(row.result)}</div>` : '';
    return `<section class="candidate-query mytms-card">${row.day ? `<h2>${esc(row.day)}</h2>` : ''}<div class="comparison-grid">${comparisons}</div><h3>${esc(row.prompt)}</h3><div class="choices">${choices}</div>${edited}${response}</section>`;
  }).join('');
  return mytmsShell(screen, `${queries}<p class="other">${esc(screen.other)}</p><button class="mobile-primary">${esc(screen.primaryAction)}</button>`);
}

function managerCorrectionFields(correction, selectId, selected) {
  if (!correction || !emailPolicy.responsesRequiringIntendedHours.includes(selected)) return '';
  const errorId = correction.errorId || `${selectId}-error`;
  const fields = (correction.fields || []).map((field, index) => {
    const item = typeof field === 'string' ? { label: field, value: '' } : field;
    const inputId = `${selectId}-correction-${index}`;
    return `<label class="correction-field" for="${esc(inputId)}"><span>${esc(item.label)}</span><span class="correction-input"><input id="${esc(inputId)}" value="${esc(item.value)}" inputmode="${esc(item.inputMode || 'text')}" aria-describedby="${esc(errorId)}">${item.suffix ? `<small>${esc(item.suffix)}</small>` : ''}</span></label>`;
  }).join('');
  const error = correction.error ? `<p class="correction-error" id="${esc(errorId)}" role="alert">${esc(correction.error)}</p>` : '';
  return `<fieldset class="correction-fields"${correction.error ? ` aria-describedby="${esc(errorId)}"` : ''}><legend>${esc(correction.legend)}</legend>${fields}${error}<p class="partial-save-state">${esc(correction.partialSaveState)}</p></fieldset>`;
}

function managerTable(section, responseMode) {
  const rows = section.rows.map((r, index) => {
    const rawControl = r.responseControl || { label: `Your response for ${r.day}`, issueFamily: r.issueFamily };
    const control = {...rawControl, options: [copy.managerChooseResponse, ...(emailPolicy.managerResponseText[rawControl.issueFamily] || [])]};
    const selectId = `response-${index}-${String(section.candidate).replace(/[^a-z0-9]/gi, '-').toLowerCase()}`;
    const selected = Number.isInteger(control.selectedResponseIndex) ? control.options[control.selectedResponseIndex] : control.options[0];
    const response = responseMode ? `<td><label class="response-label" for="${esc(selectId)}">${esc(control.label)}</label><select id="${esc(selectId)}" class="response-select"${r.correctionFields?.errorId ? ` aria-describedby="${esc(r.correctionFields.errorId)}"` : ''}>${control.options.map(option => `<option${option === selected ? ' selected' : ''}>${esc(option)}</option>`).join('')}</select>${r.details ? `<small class="response-detail" id="${esc(selectId)}-detail">${esc(r.details)}</small>` : ''}${managerCorrectionFields(r.correctionFields, selectId, selected)}</td>` : '';
    return `<tr><td>${esc(r.day)}</td><td>${esc(r.candidate)}</td><td>${esc(r.system)}</td><td>${esc(r.issue)}</td>${response}</tr>`;
  }).join('');
  const caption = section.tableCaption || `${section.candidate} - ${section.client}`;
  return `<section class="client-candidate-section"><h2>${esc(section.client)}</h2><section class="candidate-section"><h3>${esc(section.candidate)}</h3><div class="public-table-scroll" role="region" aria-label="${esc(caption)}" tabindex="0"><table><caption>${esc(caption)}</caption><thead><tr>${copy.managerTableHeaders.slice(0, 4).map(label => `<th scope="col">${esc(label)}</th>`).join('')}${responseMode ? `<th scope="col">${esc(copy.managerTableHeaders[4])}</th>` : ''}</tr></thead><tbody>${rows}</tbody></table></div></section></section>`;
}

function secureLinkValidity(screen) {
  const link = screen.secureLink;
  assert(link && link.expiresAtUtc && link.renderReferenceUtc && String(link.copy || '').includes('{days}'), `${screen.id} must declare secureLink expiry, render reference and {days} copy in policy`);
  const remainingMs = Date.parse(link.expiresAtUtc) - Date.parse(link.renderReferenceUtc);
  assert(Number.isFinite(remainingMs) && remainingMs > 0, `${screen.id} secureLink expiry must be after the render reference instant`);
  return String(link.copy).replace('{days}', String(Math.floor(remainingMs / 86400000)));
}

function renderManagerReview(screen) {
  const sections = screen.candidateSections.map(s => managerTable(s, true)).join('');
  return `<div class="public-bg"><div class="public-page"><header class="public-header"><strong>${esc(screen.brand)}</strong><span>${esc(secureLinkValidity(screen))}</span></header><main><h1>${esc(screen.title)}</h1><p class="lead">${esc(screen.intro)}</p><p class="complete-count">✓ ${esc(screen.completed)}</p><form>${sections}<div class="public-actions">${screen.actions.map(button).join('')}</div><div class="save-result" aria-live="polite"></div></form></main></div></div>`;
}

function renderFinalise(policy, screen) {
  const inner = screen.innerTabs.map(t => `<button class="inner-tab ${t.active ? 'active' : ''}">${esc(t.label)}</button>`).join('');
  const body = `<div class="source-summary"><div><small>${esc(screen.source.label || copy.sourceDefaultLabel)}</small><strong>${esc(screen.source.file)}</strong><span>${esc(screen.source.summary)}</span></div><span class="status tone-info">${esc(copy.sourceCurrentBadge)}</span></div><div class="inner-tabs">${inner}</div>${table(screen.columns, screen.rows)}<label class="confirm-row"><span class="check"></span><span>${esc(screen.confirmation)}</span></label>${footer(screen.footer)}`;
  return officeShell(policy, screen, body);
}

function renderContractChoice(screen) {
  const facts = screen.context.map(x => `<div class="contract-context-item"><span>${esc(x.label)}</span><strong>${esc(x.value)}</strong></div>`).join('');
  return `<div class="dialog-bg"><section class="dialog extra-wide contract-dialog"><header><h1>${esc(screen.title)}</h1><button class="close">×</button></header><p class="lead">${esc(screen.intro)}</p><div class="contract-context">${facts}</div>${table(screen.columns, screen.rows, 'contract-choice-table')}${footer(screen.footer)}</section></div>`;
}

function confirmationActions(screen) {
  let selected = screen.actions || [];
  if (screen.state === 'PRE_FINAL' && Array.isArray(screen.preFinalActions)) selected = screen.preFinalActions;
  if (screen.state === 'POST_FINAL' && Array.isArray(screen.postFinalActions)) selected = screen.postFinalActions;
  if (Array.isArray(screen.actions) && (screen.state === 'PRE_FINAL' || screen.state === 'POST_FINAL')) {
    assert(
      JSON.stringify(screen.actions) === JSON.stringify(selected),
      `${screen.id} actions must exactly match the ${screen.state} fixture actions`
    );
  }
  return selected;
}

function renderConfirmation(screen) {
  const facts = screen.facts.map(x => `<div class="fact"><span>${esc(x.label)}</span><strong>${esc(x.value)}</strong></div>`).join('');
  const actions = confirmationActions(screen);
  return `<div class="dialog-bg"><section class="dialog"><header><h1>${esc(screen.title)}</h1><button class="close">×</button></header><div class="warning-box">${esc(screen.warning)}</div><div class="fact-grid">${facts}</div><label class="text-field"><span>${esc(screen.field.label)}</span><textarea>${esc(screen.field.value)}</textarea></label><p class="server-note">${esc(screen.note)}</p><label class="confirm-row"><span class="check checked">✓</span><span>${esc(screen.confirmation)}</span></label><footer>${actions.map(button).join('')}</footer></section></div>`;
}

function renderCorrect(screen) {
  const fields = screen.fields.map(x => `<label class="text-field"><span>${esc(x.label)}</span><div>${esc(x.value)}</div></label>`).join('');
  const summaries = screen.summaries.map(x => `<div class="summary-box ${toneClass(x.tone)}"><strong>${esc(x.value)}</strong><span>${esc(x.label)}</span></div>`).join('');
  const tabs = screen.innerTabs.map(t => `<button class="inner-tab ${t.active ? 'active' : ''}">${esc(t.label)}</button>`).join('');
  return `<div class="dialog-bg"><section class="dialog extra-wide"><header><h1>${esc(screen.title)}</h1><button class="close">×</button></header><p class="lead">${esc(screen.intro)}</p><div class="two-fields">${fields}</div><div class="summary-grid">${summaries}</div><div class="inner-tabs">${tabs}</div>${table(screen.columns, screen.rows)}<div class="blocker-line">${esc(screen.blocker)}</div><label class="confirm-row"><span class="check checked">✓</span><span>${esc(screen.confirmation)}</span></label><footer>${screen.actions.map(button).join('')}</footer></section></div>`;
}

function renderEmail(screen) {
  const sections = screen.candidateSections.map(s => managerTable(s, false)).join('');
  return `<div class="email-bg"><div class="email-client"><div class="email-meta"><span>Subject</span><strong>${esc(screen.subject)}</strong></div><article class="email-card"><div class="email-brand">CloudTMS</div><h1>${esc(screen.title)}</h1><p class="lead">${esc(screen.intro)}</p><h2 class="client-name">${esc(screen.client)}</h2>${sections}<div class="cta-row"><button class="email-cta">${esc(screen.cta)}</button></div><p class="email-footer">${esc(screen.footer)}</p></article></div></div>`;
}

function renderAlert(screen) {
  const alerts = screen.alerts.map(a => `<article class="alert-item"><div class="alert-head"><strong>${esc(a.title)}</strong><span class="status ${toneClass(a.tone || (a.state.startsWith('Resolved') ? 'positive' : 'danger'))}">${esc(a.state)}</span></div><p>${esc(a.body)}</p><div class="alert-guidance">${esc(a.guidance)}</div><div class="alert-actions">${a.actions.map((x, i) => button({label:x, tone:i === 0 ? 'primary' : 'ghost'})).join('')}</div></article>`).join('');
  return `<div class="alert-bg"><div class="office-header"><strong>${esc(screen.brand)}</strong><div class="weekly-alert-pill" role="button" aria-label="${esc(screen.accessibleName)}"><span>${esc(screen.headerControl)}</span><b>${esc(screen.badgeCount)}</b></div></div><section class="popover"><div class="popover-head"><h1>${esc(screen.heading)}</h1><span>${esc(screen.badgeCount)} unread</span></div>${alerts}${screen.note ? `<p class="popover-note">${esc(screen.note)}</p>` : ''}</section></div>`;
}

function settingsSections(sections) {
  return sections.map(section => `<section class="settings-section"><h2>${esc(section.title)}</h2>${section.note ? `<p class="settings-note">${esc(section.note)}</p>` : ''}${section.fields.map(field => {
    const on = String(field.value).toLowerCase() === 'on';
    const value = field.type === 'toggle'
      ? `<span class="settings-toggle ${on ? 'on' : 'off'}"><i></i><b>${esc(field.value)}</b></span>`
      : field.type === 'email'
        ? `<span class="settings-input"><b>${esc(field.value)}</b></span>`
        : `<span class="settings-select ${field.readOnly ? 'read-only' : ''}"><b>${esc(field.value)}</b>${field.readOnly ? '' : '<i>⌄</i>'}</span>`;
    return `<label class="settings-field"><span>${esc(field.label)}</span>${value}</label>`;
  }).join('')}</section>`).join('');
}

function renderSettings(screen) {
  const tabs = Array.isArray(screen.tabs) ? screen.tabs : [];
  const activeTab = tabs.find(tab => tab.label === screen.activeTab) || tabs[0];
  const tabList = tabs.length ? `<nav class="settings-tabs" role="tablist" aria-label="Client settings sections">${tabs.map(tab => `<button role="tab" aria-selected="${tab === activeTab ? 'true' : 'false'}" class="settings-tab ${tab === activeTab ? 'active' : ''}">${esc(tab.label)}</button>`).join('')}</nav>` : '';
  const sections = settingsSections(activeTab ? activeTab.sections : (screen.sections || []));
  return `<div class="dialog-bg"><section class="dialog settings-dialog"><header><div><h1>${esc(screen.title)}</h1><p>${esc(screen.subtitle)}</p></div><button class="close">×</button></header>${tabList}<div class="settings-body">${sections}</div><footer>${screen.actions.map(button).join('')}</footer></section></div>`;
}

function renderTimesheetSummary(screen) {
  const filters = (screen.filters || []).map(x => control(x.label, x.value)).join('');
  const heads = (screen.columns || []).map(x => `<th>${esc(x)}</th>`).join('');
  const rows = (screen.rows || []).map((row, index) => {
    const delay = row.delayStatus
      ? `<span class="summary-delay" title="${esc(row.tooltip)}">${esc(row.delayStatus)}</span>${index === screen.hoveredRow ? `<span class="summary-tooltip">${esc(row.tooltip)}</span>` : ''}`
      : '';
    return `<tr><td>${esc(row.candidate)}</td><td>${esc(row.week)}</td><td>${esc(row.client)}</td><td>${esc(row.hours)}</td><td class="summary-status-cell"><span class="status tone-${row.mainStatus === 'Invoiced' ? 'info' : row.mainStatus === 'Processed' ? 'positive' : 'neutral'}">${esc(row.mainStatus)}</span>${delay}</td></tr>`;
  }).join('');
  return `<div class="app-shell"><aside><div class="logo-mark">C</div><div class="rail-dot active"></div><div class="rail-dot"></div><div class="rail-dot"></div></aside><main><div class="app-top"><span>CloudTMS Office</span><span class="user-dot">KA</span></div><section class="modal wide-modal summary-policy-modal"><header class="modal-header"><div><h1>${esc(screen.title)}</h1></div><button class="close">×</button></header><div class="summary-filter-bar">${filters}</div><div class="table-wrap"><table><thead><tr>${heads}</tr></thead><tbody>${rows}</tbody></table></div><footer><span>${esc(screen.footer)}</span><button class="action secondary">${esc(copy.summaryCloseButton)}</button></footer></section></main><style>.summary-policy-modal{top:74px;height:760px}.summary-filter-bar{display:flex;gap:12px;padding:14px 20px;border-bottom:1px solid #dbe3ed}.summary-policy-modal .table-wrap{margin:16px 20px;height:auto}.summary-policy-modal table{min-width:0}.summary-status-cell{position:relative}.summary-status-cell>.status{display:inline-flex}.summary-delay{display:inline-flex;margin-left:8px;padding:5px 10px;border-radius:999px;background:#fff7df;border:1px solid #d19a20;color:#7b5200;font-weight:700;font-size:12px}.summary-tooltip{position:absolute;z-index:4;left:18px;top:46px;width:260px;padding:10px 12px;border-radius:8px;background:#17243a;color:#fff;box-shadow:0 8px 24px rgba(15,23,42,.24);font-size:12px;line-height:1.4}.summary-tooltip:before{content:'';position:absolute;top:-6px;left:82px;border-left:6px solid transparent;border-right:6px solid transparent;border-bottom:6px solid #17243a}.summary-policy-modal footer{display:flex;align-items:center;justify-content:space-between;min-height:58px;padding:10px 20px;border-top:1px solid #2b3952;flex:none}</style></div>`;
}

function factGrid(items) {
  return `<div class="fact-grid">${(items || []).map(x => `<div class="fact"><span>${esc(x.label)}</span><strong>${esc(x.value)}</strong></div>`).join('')}</div>`;
}

function calculationDisclosure(screen) {
  return screen.calculationDisclosure ? `<button class="detail-disclosure" aria-expanded="false">${esc(screen.calculationDisclosure.label)} <span>${esc(screen.calculationDisclosure.summary || '')}</span></button>` : '';
}

function editorField(field, index) {
  const id = `protected-field-${index}`;
  const readOnly = field.readOnly === true;
  const disabled = !readOnly && field.editable === false;
  const value = readOnly
    ? `<div class="readonly-value" id="${id}">${esc(field.value)}</div>`
    : field.multiline
      ? `<textarea id="${id}"${disabled ? ' disabled' : ''}>${esc(field.value)}</textarea>`
      : `<span class="editor-input"><input id="${id}" value="${esc(field.value)}"${disabled ? ' disabled' : ''}>${field.suffix ? `<small>${esc(field.suffix)}</small>` : ''}</span>`;
  const width = field.multiline || field.width === 'full' ? 'editor-field-wide' : field.width === 'quarter' ? 'editor-field-quarter' : 'editor-field-half';
  return `<label class="editor-field ${width}" for="${id}"><span>${esc(field.label)}${readOnly ? ' <i>read-only</i>' : ''}</span>${value}${field.note ? `<small class="field-note">${esc(field.note)}</small>` : ''}</label>`;
}

function renderProtectedShiftEditor(screen) {
  const comparison = screen.comparison
    ? `<div class="decision-table"><table><thead><tr>${screen.comparison.columns.map(label => `<th>${esc(label)}</th>`).join('')}</tr></thead><tbody>${(screen.comparison.rows || []).map(row => `<tr><td>${esc(row.day)}</td><td>${esc(row.candidateHours)}</td><td>${esc(row.systemHours)}</td><td>${esc(row.issue)}</td></tr>`).join('')}</tbody></table></div>`
    : '';
  const stale = screen.staleNotice ? `<div class="stale-notice" role="alert">${esc(screen.staleNotice)}</div>` : '';
  const fields = (screen.fields || []).map(editorField).join('');
  return `<div class="dialog-bg"><section class="dialog protected-editor"><header><div><h1>${esc(screen.title)}</h1><p>${esc(screen.intro)}</p></div><button class="close" aria-label="Close">×</button></header><p class="status-line">${esc(screen.statusLine)}</p><p class="scope-copy">${esc(screen.situation)}</p>${stale}${comparison}<div class="warning-box">${esc(screen.warning)}</div><div class="editor-grid">${fields}</div><p class="scope-copy">${esc(screen.scopeCopy)}</p>${calculationDisclosure(screen)}<label class="confirm-row"><span class="check"></span><span>${esc(screen.confirmation)}</span></label>${footer(screen.footer)}</section></div>`;
}

function renderReconcileApprovedHours(screen) {
  const truth = `<section class="truth-card reconcile-truth"><h2>${esc(screen.truthTitle)}</h2>${(screen.truthRows || []).map(row => `<div class="truth-row"><span>${esc(row.label)}</span><strong>${esc(row.value)}</strong></div>`).join('')}</section>`;
  const position = `<div class="pay-fact-grid">${(screen.position || []).map(x => `<div><span>${esc(x.label)}</span><strong>${esc(x.value)}</strong></div>`).join('')}</div>`;
  const options = `<div class="decision-options">${(screen.options || []).map(option => `<div class="decision-option ${toneClass(option.tone)}"><strong>${esc(option.label)}</strong><span>${esc(option.description)}</span></div>`).join('')}</div>`;
  return `<div class="dialog-bg"><section class="dialog extra-wide reconcile-dialog"><header><div><h1>${esc(screen.title)}</h1><p>${esc(screen.intro)}</p></div><button class="close" aria-label="Close">×</button></header>${factGrid(screen.facts)}${truth}${position}${options}${calculationDisclosure(screen)}${footer(screen.footer)}</section></div>`;
}

function renderRecordNotWorked(screen) {
  const calculation = calculationDisclosure(screen);
  return `<div class="dialog-bg"><section class="dialog decision-dialog"><header><h1>${esc(screen.title)}</h1><button class="close">×</button></header><div class="warning-box">${esc(screen.warning)}</div>${factGrid(screen.facts)}<p class="scope-copy">${esc(screen.otherWork)}</p>${calculation}<label class="text-field"><span>${esc(screen.reason?.label)}</span><textarea>${esc(screen.reason?.value)}</textarea></label><label class="confirm-row"><span class="check"></span><span>${esc(screen.confirmation)}</span></label>${footer(screen.footer)}</section></div>`;
}

function truthRows(screen) {
  const difference = screen.differenceNote ? `<p class="difference-note">${esc(screen.differenceNote)}</p>` : '';
  return `<section class="truth-card"><h2>${esc(screen.truthTitle)}</h2>${(screen.truthRows || []).map(row => `<div class="truth-row"><span>${esc(row.label)}</span><strong>${esc(row.value)}</strong>${row.action ? `<button>${esc(row.action)}</button>` : ''}</div>`).join('')}${difference}</section>`;
}

function payPosition(screen) {
  return `<section class="truth-card"><h2>${esc(screen.positionTitle)}</h2><div class="position-grid">${(screen.position || []).map(row => `<div><span>${esc(row.label)}</span><strong>${esc(row.value)}</strong></div>`).join('')}</div></section>`;
}

function paymentHistory(screen) {
  return `<section class="truth-card payment-history"><h2>${esc(screen.historyTitle)}</h2>${(screen.history || []).map(row => `<article><div class="history-head"><span class="status ${toneClass(row.tone)}">${esc(row.status)}</span><time>${esc(row.date)}</time><strong>${esc(row.gross)}</strong></div><p>${esc(row.hours)}</p><small>${esc(row.transfer)}</small>${row.document ? `<button>${esc(row.document)}</button>` : ''}</article>`).join('')}</section>`;
}

function renderOfficeTimesheetHistory(screen) {
  return `<div class="app-shell"><aside><div class="logo-mark">C</div><div class="rail-dot active"></div><div class="rail-dot"></div><div class="rail-dot"></div></aside><main><div class="app-top"><span>CloudTMS Office</span><span class="user-dot">KA</span></div><section class="modal wide-modal timesheet-history-modal"><header class="modal-header"><div><h1>${esc(screen.title)}</h1><p>${esc(screen.subtitle)}</p></div><div class="history-statuses">${status(screen.mainStatus)}${status(screen.reconciliationStatus)}<button class="close">×</button></div></header><div class="history-scroll">${truthRows(screen)}${payPosition(screen)}${paymentHistory(screen)}</div>${footer(screen.footer)}</section></main></div>`;
}

function renderCandidateTimesheetHistory(screen) {
  const statusRow = `<div class="mytms-status-row">${status(screen.mainStatus)}</div>`;
  const approved = (screen.approvedRows || []).length
    ? `<section class="mytms-card approved-hours-card"><h2>${esc(screen.approvedTitle)}</h2>${screen.approvedRows.map(row => `<div class="approved-hours-row"><span>${esc(row.day)}</span><strong>${esc(row.value)}</strong></div>`).join('')}<p>${esc(screen.approvedNote)}</p></section>`
    : '';
  const expenses = screen.expenses
    ? `<section class="mytms-card expense-card"><h2>${esc(screen.expenses.title)}</h2><p>${esc(screen.expenses.body)}</p><button class="mobile-secondary">${esc(screen.expenses.action)}</button></section>`
    : '';
  return mytmsShell(screen, `${statusRow}<div class="history-mobile-content">${truthRows(screen)}${approved}${expenses}</div>`);
}

function comparisonCards(screen) {
  const stateLabel = (state) => state === 'match' ? copy.compareMatch : copy.compareDifferent;
  const rows = (screen.units || []).map(unit => {
    const tone = unit.state === 'match' ? 'positive' : 'danger';
    return `<div class="compare-unit tone-${tone}"><strong>${esc(unit.label)}</strong><span>${esc(unit.left)}</span><span>${esc(unit.right)}</span><b>${esc(stateLabel(unit.state))}</b></div>`;
  }).join('');
  const banner = screen.banner ? `<div class="compare-banner">${esc(screen.banner)}</div>` : '';
  const reference = screen.reference ? `<div class="reference-note">${esc(screen.reference)}</div>` : '';
  return `${banner}<div class="authority-line">${esc(screen.authorityMessage || screen.classification || '')}</div><div class="shift-heading">${esc(screen.shift || '')}</div><div class="compare-head"><section class="compare-card tone-${esc(screen.left?.tone || 'neutral')}"><h2>${esc(screen.left?.title)}</h2><span>${esc(screen.left?.badge)}</span></section><section class="compare-card tone-${esc(screen.right?.tone || 'neutral')}"><h2>${esc(screen.right?.title)}</h2><span>${esc(screen.right?.badge)}</span></section></div><div class="compare-units"><div class="compare-unit compare-labels"><strong>${esc(copy.compareUnitLabel)}</strong><span>${esc(screen.left?.title)}</span><span>${esc(screen.right?.title)}</span><b>${esc(copy.compareResultLabel)}</b></div>${rows}</div>${reference}`;
}

function renderTimesheetComparison(screen) {
  const tabs = (screen.tabs || []).map(label => `<button class="tab ${label === screen.activeTab ? 'active' : ''}">${esc(label)}</button>`).join('');
  const ordinary = screen.variant === 'ordinary';
  const body = ordinary
    ? `<div class="ordinary-lines"><h2>${esc(screen.scheduleTitle)}</h2>${(screen.ordinaryRows || []).map(row => `<div><strong>${esc(row.day)}</strong><span>${esc(row.hours)}</span><span>${esc(row.break)}</span><b>${esc(row.total)}</b></div>`).join('')}</div>`
    : comparisonCards(screen);
  return `<div class="app-shell"><aside><div class="logo-mark">C</div><div class="rail-dot active"></div><div class="rail-dot"></div><div class="rail-dot"></div></aside><main><div class="app-top"><span>CloudTMS Office</span><span class="user-dot">KA</span></div><section class="modal wide-modal timesheet-compare-modal"><header class="modal-header"><div><h1>${esc(screen.title)}</h1><p>${esc(screen.subtitle)}</p></div><div class="history-statuses">${status(screen.status)}<button class="close">×</button></div></header><nav class="tabs">${tabs}</nav><div class="timesheet-compare-body">${body}</div>${footer(screen.footer)}</section></main></div>`;
}

function renderBulkTimesheetComparison(screen) {
  const listRows = (screen.selectedRows || []).map((name, index) => `<div class="bulk-list-row ${index === 0 ? 'active' : ''}"><span class="check checked">✓</span><strong>${esc(name)}</strong><small>${index === 0 ? esc(copy.bulkRowStates.active) : esc(copy.bulkRowStates.ready)}</small></div>`).join('');
  const [filterAll, filterReady, filterAttention] = copy.bulkFilterLabels;
  return `<div class="app-shell"><aside><div class="logo-mark">C</div><div class="rail-dot active"></div><div class="rail-dot"></div><div class="rail-dot"></div></aside><main><div class="app-top"><span>CloudTMS Office</span><span class="user-dot">KA</span></div><section class="modal wide-modal bulk-compare-modal"><header class="modal-header"><div><h1>${esc(screen.title)}</h1><p>${esc(screen.classification)}</p></div><button class="close">×</button></header><div class="bulk-toolbar"><span>Classification: <strong>${esc(screen.classification)}</strong></span><span>${esc(copy.bulkStatusLine)}</span></div><div class="bulk-three-pane"><section class="bulk-filter"><h2>Filters</h2><button class="filter-active">${esc(filterAll)}</button><button>${esc(filterReady)}</button><button>${esc(filterAttention)}</button></section><section class="bulk-list"><div class="bulk-list-head">${headerCheckbox('checked', 'Select or clear all visible rows')}<strong>${esc(copy.bulkListHeaders[0])}</strong><span>${esc(copy.bulkListHeaders[1])}</span></div>${listRows}</section><section class="bulk-detail"><h2>${esc(screen.activeCandidate)}</h2>${comparisonCards(screen)}</section></div>${footer(screen.footer)}</section></main></div>`;
}

function renderScreen(policy, screen) {
  switch (screen.kind) {
    case 'office-table-modal': return renderOfficeTable(policy, screen);
    case 'candidate-mobile': return renderCandidate(screen);
    case 'manager-review': return renderManagerReview(screen);
    case 'finalise-modal': return renderFinalise(policy, screen);
    case 'contract-choice-modal': return renderContractChoice(screen);
    case 'confirmation-modal': return renderConfirmation(screen);
    case 'correct-final-modal': return renderCorrect(screen);
    case 'office-alert-popover': return renderAlert(screen);
    case 'settings-modal': return renderSettings(screen);
    case 'timesheet-summary': return renderTimesheetSummary(screen);
    case 'protected-shift-editor-modal': return renderProtectedShiftEditor(screen);
    case 'reconcile-approved-hours-modal': return renderReconcileApprovedHours(screen);
    case 'record-not-worked-modal': return renderRecordNotWorked(screen);
    case 'office-timesheet-history': return renderOfficeTimesheetHistory(screen);
    case 'candidate-timesheet-history': return renderCandidateTimesheetHistory(screen);
    case 'timesheet-comparison': return renderTimesheetComparison(screen);
    case 'bulk-timesheet-comparison': return renderBulkTimesheetComparison(screen);
    default: throw new Error(`Unknown screen kind: ${screen.kind}`);
  }
}

function stylesheet(t, mt) {
  return `
    *{box-sizing:border-box} body{margin:0;font-family:${t.fontFamily};font-size:${t.bodyFontPx}px;color:${t.primaryText};background:${t.officeBackground};overflow:hidden} button,input,textarea{font:inherit} button{cursor:default}
    .app-shell{height:100vh;display:grid;grid-template-columns:72px 1fr;background:linear-gradient(145deg,${t.officeBackground},#0a1728)} aside{background:#050b14;border-right:1px solid ${t.divider};display:flex;flex-direction:column;align-items:center;gap:22px;padding-top:20px}.logo-mark{width:38px;height:38px;border-radius:11px;background:${t.primary};display:grid;place-items:center;font-weight:700;font-size:20px}.rail-dot{width:30px;height:30px;border-radius:8px;background:${t.raisedSurface};border:1px solid ${t.divider}}.rail-dot.active{background:${t.primary};opacity:.8} main{position:relative}.app-top{height:58px;border-bottom:1px solid ${t.divider};display:flex;align-items:center;justify-content:space-between;padding:0 24px;color:${t.secondaryText}}.user-dot{width:32px;height:32px;border-radius:50%;background:${t.raisedSurface};display:grid;place-items:center;color:${t.primaryText}}
    .modal{position:absolute;background:${t.modalSurface};border:1px solid ${t.divider};border-radius:${t.modalRadius}px;box-shadow:0 26px 80px rgba(0,0,0,.48);overflow:hidden}.wide-modal{width:min(${t.desktopModalWidthPx}px,calc(100% - ${2 * (t.desktopViewportMarginPx || 60)}px));height:808px;left:50%;top:70px;transform:translateX(-50%);display:flex;flex-direction:column}.modal-header{display:flex;justify-content:space-between;align-items:flex-start;padding:20px 24px 16px;border-bottom:1px solid ${t.divider}}h1{font-size:24px;line-height:1.22;margin:0;font-weight:650;letter-spacing:-.2px}.modal-header p{margin:5px 0 0;color:${t.secondaryText}}.close{border:0;background:transparent;color:${t.secondaryText};font-size:28px;line-height:1;padding:3px 7px}.context-bar{display:flex;align-items:flex-end;gap:12px;padding:14px 24px;background:${t.raisedSurface};border-bottom:1px solid ${t.divider}}.ctx-field{display:flex;flex-direction:column;position:relative;min-width:158px;gap:5px}.ctx-field span{font-size:${t.smallFontPx}px;color:${t.secondaryText}}.ctx-field strong{font-weight:500;background:${t.inputSurface};border:1px solid ${t.divider};border-radius:${t.controlRadius}px;padding:9px 30px 9px 11px}.ctx-field.readonly strong{padding-right:11px;background:rgba(255,255,255,.025);border-style:dashed}.ctx-field b{position:absolute;right:10px;bottom:9px;color:${t.secondaryText}.8}.cycle{margin-left:auto}.tabs{display:flex;gap:4px;padding:0 24px;border-bottom:1px solid ${t.divider};background:${t.modalSurface}}.tab,.inner-tab{border:0;background:transparent;color:${t.secondaryText};padding:13px 14px;border-bottom:2px solid transparent}.tab.active,.inner-tab.active{color:${t.primaryText};border-bottom-color:${t.info};font-weight:600}
    .toolbar{display:flex;justify-content:space-between;gap:14px;padding:12px 24px;border-bottom:1px solid ${t.divider};align-items:flex-end}.filter-row,.action-row{display:flex;gap:8px;align-items:flex-end;flex-wrap:wrap}.toolbar .ctx-field{min-width:110px}.toolbar .ctx-field strong{padding-top:7px;padding-bottom:7px}.action{border:1px solid ${t.divider};border-radius:${t.controlRadius}px;padding:9px 13px;background:${t.raisedSurface};color:${t.primaryText};white-space:nowrap}.action.primary{background:${t.primary};border-color:${t.primary}}.action.danger{background:rgba(239,68,68,.16);border-color:${t.danger};color:#ff9ca6}.action.ghost{background:transparent}.action:disabled{opacity:.42}.selection-summary{padding:8px 24px;background:rgba(79,70,229,.12);color:#c8d1ff;border-bottom:1px solid ${t.divider};font-size:${t.smallFontPx}px}.notice{margin:12px 24px 0;padding:10px 13px;border-radius:${t.controlRadius}px;display:flex;gap:12px;align-items:center;border:1px solid ${t.divider}}.notice span{color:${t.secondaryText}}.notice.tone-danger{background:rgba(239,68,68,.1);border-color:rgba(239,68,68,.55)}.notice.tone-info{background:rgba(96,165,250,.09);border-color:rgba(96,165,250,.45)}
    .table-wrap{margin:0 24px;overflow:auto;flex:1;position:relative;border-bottom:1px solid ${t.divider}}table{width:100%;border-collapse:collapse}thead{position:sticky;top:0;z-index:2;background:${t.raisedSurface}}th{text-align:left;color:${t.secondaryText};font-size:${t.smallFontPx}px;font-weight:600;padding:10px 8px;border-bottom:1px solid ${t.divider};white-space:nowrap}td{padding:12px 8px;border-bottom:1px solid rgba(43,57,82,.72);vertical-align:middle}td.col-client,td.col-candidate{overflow-wrap:anywhere}th.col-select,td.col-select,th.col-expand,td.col-expand{width:34px;padding-left:5px;padding-right:5px}th.col-select{position:sticky;left:0;z-index:3;background:${t.raisedSurface}}th.col-issues,td.col-issues{width:48px}th.col-candidateAsked,td.col-candidateAsked,th.col-managerInformed,td.col-managerInformed{width:102px}th.col-oldest,td.col-oldest{width:100px}th.col-actions,td.col-actions{width:${t.actionColumnWidthPx || 88}px;white-space:normal}tbody tr:hover{background:rgba(255,255,255,.018)}.sort{margin-left:4px;color:${t.info}.8}.status-stack{display:inline-grid;gap:4px}.status{display:inline-flex;border-radius:999px;padding:4px 8px;font-size:${t.smallFontPx}px;line-height:1.15;border:1px solid currentColor;white-space:nowrap}.status-detail{display:inline-flex;border-radius:999px;padding:3px 7px;font-size:11px;line-height:1.1;border:1px solid currentColor;white-space:nowrap;width:max-content}.tone-positive{color:${t.positive};background:rgba(34,197,94,.08)}.tone-warning{color:${t.warning};background:rgba(245,158,11,.08)}.tone-danger{color:#ff7474;background:rgba(239,68,68,.1)}.tone-info{color:${t.info};background:rgba(96,165,250,.08)}.tone-neutral{color:${t.secondaryText};background:rgba(168,179,199,.05)}.tick{display:grid;place-items:center;width:22px;height:22px;border-radius:50%;background:rgba(34,197,94,.16);color:${t.positive};font-weight:700}.check{display:inline-grid;place-items:center;width:19px;height:19px;border:1px solid #71809b;border-radius:4px;font-size:12px}.check.checked{background:${t.primary};border-color:${t.primary}}.check.disabled-check{opacity:.28;background:#202b3e}.header-check{width:22px;height:22px;font-weight:700}.header-check.indeterminate{background:#42526d;border-color:#71809b}.expand{font-size:21px;color:${t.secondaryText}.8}.candidate-with-expand{display:flex;align-items:center;gap:5px}.expand-button{display:inline-grid;place-items:center;width:28px;height:28px;border:0;background:transparent;color:${t.secondaryText};font-size:20px}.row-actions{display:flex;flex-direction:${t.actionDirection || 'row'};align-items:stretch;gap:6px}.status{white-space:${t.statusWhiteSpace || 'normal'}}.row-action{border:0;background:transparent;color:${t.info};padding:5px;font-weight:600}.child-next-action{display:flex;align-items:center;min-width:0}.child-holder td{padding:0 16px 12px 58px;background:#091426}.child-grid{border-left:3px solid ${t.info};background:${t.raisedSurface};border-radius:0 8px 8px 0;padding:6px 10px;overflow:auto}.child-row{display:grid;grid-template-columns:25px 1.05fr 1.45fr 1.45fr 1.15fr auto;gap:12px;align-items:center;padding:8px 4px;border-bottom:1px solid ${t.divider};font-size:13px;min-width:760px}.child-header{position:sticky;top:0;z-index:1;background:${t.raisedSurface};color:${t.secondaryText};font-size:${t.smallFontPx}px;font-weight:600}.child-header .header-check{position:sticky;left:0}.child-row div{min-width:0}.child-row small{display:block;color:${t.secondaryText};font-size:11px;margin-bottom:3px}.child-status{display:block;color:${t.secondaryText};font-size:11px;margin-top:3px}.detail-notice{margin:8px 3px 10px;padding:10px 12px;border:1px solid currentColor;border-radius:8px;display:grid;gap:9px}.detail-notice-head{display:flex;gap:12px;align-items:center}.detail-notice-head span{color:${t.secondaryText};font-size:${t.smallFontPx}px}.detail-notice-facts{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:8px}.detail-fact{display:grid;gap:2px;padding-right:7px;border-right:1px solid ${t.divider}}.detail-fact:last-child{border-right:0}.detail-fact small{color:${t.secondaryText};font-size:11px}.detail-fact strong{color:${t.primaryText};font-size:13px}.child-actions{padding:8px 2px 2px;display:flex;gap:7px}.modal-footer{margin-top:auto;min-height:60px;padding:10px 24px;display:flex;align-items:center;justify-content:space-between;border-top:1px solid ${t.divider};background:${t.raisedSurface};color:${t.secondaryText}.8}.modal-footer div{display:flex;gap:8px}
    .mobile-bg{height:100vh;background:linear-gradient(160deg,${t.officeBackground},${t.raisedSurface});display:grid;place-items:center}.phone{width:368px;height:812px;border-radius:28px;box-shadow:0 24px 60px rgba(0,0,0,.45);overflow:hidden;border:7px solid ${mt.nativeStackHeaderBackground}}.mytms-phone{background:${mt.pageBackground};color:${mt.text}}.mytms-stack-header{height:52px;background:${mt.nativeStackHeaderBackground};color:${mt.nativeStackHeaderText};border-bottom:1px solid ${mt.line};display:flex;align-items:center;gap:10px;padding:0 ${mt.pageHorizontalPaddingPx}px;font-size:${mt.bodyFontPx}px}.mytms-back{color:${mt.blueBright};font-size:30px;line-height:1;margin-top:-2px}.mytms-screen{height:calc(100% - 52px);overflow:auto;padding:0 ${mt.pageHorizontalPaddingPx}px 44px}.mytms-screen-heading{padding-top:16px;padding-bottom:24px}.mytms-eyebrow{display:block;color:${mt.blueBright};font-size:${mt.labelFontPx}px;line-height:${mt.labelLinePx}px;font-weight:700;letter-spacing:.35px;text-transform:uppercase;margin-bottom:8px}.mytms-screen-heading h1{color:${mt.text};font-size:${mt.titleFontPx}px;line-height:${mt.titleLinePx}px;font-weight:800;letter-spacing:-.45px}.mytms-screen-heading p{color:${mt.muted};font-size:${mt.bodyFontPx}px;line-height:${mt.bodyLinePx}px;margin:8px 0 0}.mytms-content{display:grid;gap:${mt.contentGapPx}px}.mytms-card{background:${mt.cardDefault};border:1px solid ${mt.line};border-radius:${mt.cardRadiusPx}px;padding:${mt.cardPaddingPx}px;box-shadow:0 12px 24px rgba(0,0,0,.26)}.candidate-query h2{color:${mt.text};font-size:${mt.headingFontPx}px;line-height:${mt.headingLinePx}px;margin:0 0 16px}.candidate-query h3{color:${mt.text};font-size:${mt.bodyFontPx}px;line-height:${mt.bodyLinePx}px;margin:16px 0 8px;font-weight:600}.comparison-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px}.comparison{border:1px solid ${mt.line};background:${mt.cardSoft};border-radius:${mt.buttonRadiusPx}px;padding:12px}.comparison span,.comparison small{display:block;color:${mt.muted};font-size:${mt.smallFontPx}px;line-height:${mt.smallLinePx}px}.comparison strong{display:block;color:${mt.text};font-size:17px;line-height:22px;margin:6px 0 2px}.comparison.tone-info{border-top:3px solid ${mt.blueBright}}.comparison.tone-warning{border-top:3px solid ${mt.amber}}.choices{display:grid;gap:8px}.choice{display:flex;gap:10px;align-items:flex-start;min-height:48px;background:${mt.controlSecondary};border:1px solid ${mt.lineStrong};padding:11px 12px;border-radius:${mt.buttonRadiusPx}px;color:${mt.blueBright};font-size:${mt.bodyFontPx}px;line-height:${mt.bodyLinePx}px;font-weight:600}.choice.selected{border-color:${mt.blueBright};background:${mt.blueDeep};color:${mt.text}}.radio{color:inherit;font-size:18px}.mobile-advice{margin-top:12px;padding:12px 14px;border-radius:${mt.cardRadiusPx}px;background:${mt.cardWarning};border:1px solid ${mt.amber};display:flex;gap:4px;flex-direction:column;color:${mt.text};font-size:${mt.smallFontPx}px;line-height:${mt.smallLinePx}px}.mobile-result,.edited-hours{margin-top:10px;padding:10px 12px;border-radius:${mt.buttonRadiusPx}px;background:${mt.cardSuccess};border:1px solid ${mt.green};color:${mt.text};font-size:${mt.smallFontPx}px;line-height:${mt.smallLinePx}px}.edited-hours{display:flex;gap:8px;background:${mt.cardSoft};border-color:${mt.line}}.edited-hours span{padding:5px 7px;border:1px solid ${mt.lineStrong};border-radius:8px}.other{color:${mt.muted};font-size:${mt.smallFontPx}px;line-height:${mt.smallLinePx}px;margin:0}.mobile-primary{width:100%;min-height:48px;border:1px solid ${mt.blueBright};border-radius:${mt.buttonRadiusPx}px;background:${mt.blueDeep};color:#fff;padding:12px ${mt.cardPaddingPx}px;font-size:${mt.bodyFontPx}px;line-height:${mt.bodyLinePx}px;font-weight:600}
    .public-bg{height:100vh;background:#eef2f7;color:#172033;padding:24px}.public-page{max-width:1160px;height:100%;margin:auto;background:white;border-radius:14px;box-shadow:0 18px 50px rgba(20,30,50,.16);overflow:auto}.public-header{height:62px;background:#0b1730;color:white;display:flex;align-items:center;justify-content:space-between;padding:0 24px}.public-header span{color:#c4cede}.public-page main{padding:24px 30px}.lead{color:#5e6a7f;line-height:1.5}.complete-count{color:#138c4c;font-weight:600}.client-candidate-section>h2{font-size:18px;margin:22px 0 0;padding:11px 12px;background:#eef2ff;border-left:4px solid #4f46e5;border-radius:7px}.candidate-section{margin-top:14px}.candidate-section h3{font-size:17px;margin:0 0 8px}.public-table-scroll{max-width:100%;overflow-x:auto;border-radius:8px}.public-table-scroll:focus{outline:3px solid #4f46e5;outline-offset:2px}.public-page table,.email-card table{color:#253044;min-width:820px}.public-page table caption{position:absolute;width:1px;height:1px;padding:0;margin:-1px;overflow:hidden;clip:rect(0,0,0,0);white-space:nowrap;border:0}.public-page th,.email-card th{color:#667187;background:#f7f9fc;border-color:#dfe5ee}.public-page td,.email-card td{border-color:#e5e9f0;padding:10px}.response-label{position:absolute;width:1px;height:1px;padding:0;margin:-1px;overflow:hidden;clip:rect(0,0,0,0);white-space:nowrap;border:0}.response-select{display:block;width:100%;min-height:44px;border:1px solid #cbd3df;border-radius:7px;padding:8px;background:#fff;color:#253044}.response-detail{display:block;color:#65728a;margin-top:5px}.correction-fields{margin:10px 0 0;padding:10px;border:1px solid #b7c1d0;border-radius:8px}.correction-fields legend{font-weight:700;padding:0 5px}.correction-field{display:grid;grid-template-columns:70px 1fr;gap:8px;align-items:center;margin-top:7px}.correction-input{display:flex;align-items:center;gap:6px}.correction-input input{width:100%;min-height:38px;border:1px solid #cbd3df;border-radius:6px;padding:7px;color:#253044;background:#fff}.correction-input small{white-space:nowrap;color:#65728a}.correction-error{margin:8px 0 0;color:#b42318;font-weight:650}.partial-save-state{margin:5px 0 0;color:#65728a;font-size:12px}.public-actions{display:flex;justify-content:flex-end;gap:9px;margin-top:20px}.public-actions .action{min-height:44px}.public-actions .action.secondary{background:#fff;color:#283247;border-color:#cbd3df}.public-actions .action.primary{color:white}.save-result{min-height:1px}
    .source-summary{display:flex;justify-content:space-between;align-items:center;padding:14px 24px;background:${t.raisedSurface};border-bottom:1px solid ${t.divider}}.source-summary div{display:grid;gap:4px}.source-summary small,.source-summary span{color:${t.secondaryText}}.source-summary strong{font-size:17px}.inner-tabs{display:flex;gap:5px;padding:0 24px;border-bottom:1px solid ${t.divider}}.confirm-row{display:flex;align-items:center;gap:9px;padding:12px 24px;background:${t.raisedSurface};border-top:1px solid ${t.divider}}
    .dialog-bg{height:100vh;background:radial-gradient(circle at top,#14223a,#050b14);display:grid;place-items:center}.dialog{width:760px;max-height:840px;overflow:hidden;background:${t.modalSurface};border:1px solid ${t.divider};border-radius:${t.modalRadius}px;box-shadow:0 28px 90px rgba(0,0,0,.6)}.dialog.extra-wide{width:1120px}.dialog>header{display:flex;justify-content:space-between;align-items:center;padding:20px 24px;border-bottom:1px solid ${t.divider}}.dialog>footer{display:flex;justify-content:flex-end;gap:9px;padding:14px 24px;border-top:1px solid ${t.divider};background:${t.raisedSurface}}.dialog .lead{color:${t.secondaryText}}.warning-box{margin:18px 24px;padding:13px;border:1px solid rgba(245,158,11,.62);background:rgba(245,158,11,.1);border-radius:9px;line-height:1.45;color:#ffd690}.fact-grid{margin:0 24px;display:grid;grid-template-columns:1fr 1fr;border:1px solid ${t.divider};border-radius:9px;overflow:hidden}.fact{display:grid;gap:5px;padding:10px 13px;border-bottom:1px solid ${t.divider}}.fact:nth-child(odd){border-right:1px solid ${t.divider}}.fact span{font-size:${t.smallFontPx}px;color:${t.secondaryText}}.text-field{display:grid;gap:6px;margin:15px 24px}.text-field>span{font-size:${t.smallFontPx}px;color:${t.secondaryText}}.text-field textarea,.text-field>div{resize:none;background:${t.inputSurface};border:1px solid ${t.divider};border-radius:8px;color:${t.primaryText};padding:10px;min-height:44px}.server-note{margin:0 24px;color:${t.secondaryText};font-size:${t.smallFontPx}px}.two-fields{display:grid;grid-template-columns:1fr 1fr}.summary-grid{display:grid;grid-template-columns:repeat(5,1fr);gap:9px;margin:14px 24px}.summary-box{display:grid;gap:2px;padding:10px 12px;border:1px solid currentColor;border-radius:9px}.summary-box strong{font-size:20px}.summary-box span{font-size:${t.smallFontPx}px}.blocker-line{margin:10px 24px;padding:10px 12px;border-radius:8px;background:rgba(239,68,68,.1);border:1px solid rgba(239,68,68,.55);color:#ff9494}.contract-dialog{display:flex;flex-direction:column;height:680px}.contract-dialog>.lead{margin:18px 24px 12px}.contract-context{margin:0 24px 16px;display:grid;grid-template-columns:repeat(4,minmax(0,1fr));border:1px solid ${t.divider};border-radius:9px;overflow:hidden}.contract-context-item{display:grid;gap:5px;padding:11px 13px;border-right:1px solid ${t.divider}}.contract-context-item:last-child{border-right:0}.contract-context-item span{color:${t.secondaryText};font-size:${t.smallFontPx}px}.contract-context-item strong{font-size:13px}.contract-choice-table{min-height:220px}.radio-choice{display:inline-grid;place-items:center;color:${t.primary};font-size:21px;width:28px;height:28px}
    .email-bg{height:100vh;background:#dfe4eb;color:#1f2937;padding:25px}.email-client{max-width:940px;margin:auto}.email-meta{display:grid;grid-template-columns:70px 1fr;background:white;border-radius:10px;padding:13px 18px;margin-bottom:14px;box-shadow:0 4px 12px rgba(0,0,0,.08)}.email-meta span{color:#6b7280}.email-card{background:white;border-radius:10px;padding:28px 42px;box-shadow:0 12px 30px rgba(0,0,0,.12)}.email-brand{color:${t.primary};font-size:20px;font-weight:750;margin-bottom:22px}.email-card h1{font-size:27px}.client-name{font-size:18px;margin-top:22px;border-bottom:2px solid #e5e7eb;padding-bottom:8px}.email-card .candidate-section h2{background:#f3f6fa}.cta-row{text-align:center;margin:26px 0}.email-cta{background:${t.primary};color:white;border:0;border-radius:8px;padding:13px 22px;font-weight:650}.email-footer{font-size:12px;color:#6b7280;text-align:center;border-top:1px solid #e5e7eb;padding-top:16px}
    .alert-bg{height:100vh;background:linear-gradient(145deg,#07111f,#132139);padding:26px}.office-header{height:58px;background:#0b1221;border:1px solid ${t.divider};border-radius:12px;display:flex;justify-content:space-between;align-items:center;padding:0 18px}.weekly-alert-pill{display:flex;align-items:center;gap:8px;background:${t.raisedSurface};padding:8px 11px;border-radius:9px}.weekly-alert-pill b{display:grid;place-items:center;width:22px;height:22px;border-radius:50%;background:${t.danger};color:white}.popover{width:680px;margin:14px 0 0 auto;background:${t.modalSurface};border:1px solid ${t.divider};border-radius:12px;box-shadow:0 24px 70px rgba(0,0,0,.55);overflow:hidden}.popover-head{display:flex;justify-content:space-between;align-items:center;padding:17px 19px;border-bottom:1px solid ${t.divider}}.popover-head h1{font-size:20px}.popover-head span{color:${t.secondaryText}}.alert-item{padding:16px 19px;border-bottom:1px solid ${t.divider}}.alert-head{display:flex;justify-content:space-between;gap:20px}.alert-item p{color:${t.secondaryText};line-height:1.45}.alert-guidance{border-left:3px solid ${t.warning};padding-left:10px;color:#ffd690}.alert-actions{display:flex;gap:8px;margin-top:12px}.popover-note{padding:0 19px 16px;color:${t.secondaryText};font-size:${t.smallFontPx}px}
    .settings-dialog{width:1060px}.settings-dialog header p{margin:5px 0 0;color:${t.secondaryText}}.settings-tabs{display:flex;gap:5px;padding:0 24px;border-bottom:1px solid ${t.divider};background:${t.modalSurface}}.settings-tab{border:0;border-bottom:3px solid transparent;background:transparent;color:${t.secondaryText};padding:14px 17px;font-weight:650}.settings-tab.active{color:${t.primaryText};border-bottom-color:${t.primary}}.settings-body{padding:18px 24px;display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px;align-items:start;max-height:640px;overflow:auto}.settings-section{border:1px solid ${t.divider};border-radius:10px;overflow:hidden}.settings-section h2{margin:0;padding:12px 14px;background:${t.raisedSurface};font-size:16px}.settings-note{margin:0;padding:10px 14px;color:${t.secondaryText};font-size:${t.smallFontPx}px;border-top:1px solid ${t.divider}}.settings-field{display:grid;grid-template-columns:1.05fr 1.25fr;gap:14px;align-items:center;padding:13px 14px;border-top:1px solid ${t.divider}}.settings-field>span:first-child{color:${t.primaryText}}.settings-select,.settings-input{display:flex;justify-content:space-between;align-items:center;background:${t.inputSurface};border:1px solid ${t.divider};border-radius:8px;padding:9px 11px;min-height:40px}.settings-select b,.settings-input b{font-weight:500;overflow-wrap:anywhere}.settings-select i{font-style:normal;color:${t.secondaryText}}.settings-select.read-only{color:${t.secondaryText}}.settings-toggle{display:flex;align-items:center;gap:9px}.settings-toggle i{display:inline-block;width:38px;height:22px;border-radius:999px;background:#5e6a7f;position:relative}.settings-toggle i:after{content:'';position:absolute;width:16px;height:16px;top:3px;left:3px;border-radius:50%;background:white}.settings-toggle.on i{background:${t.primary}}.settings-toggle.on i:after{left:auto;right:3px}.settings-toggle b{font-weight:500}.settings-toggle.on b{color:${t.positive}}.settings-toggle.off b{color:${t.secondaryText}}
    .decision-dialog{display:flex;flex-direction:column;max-height:850px;overflow:auto}.decision-table{margin:15px 24px 0;border:1px solid ${t.divider};border-radius:9px;overflow:auto}.scope-copy{margin:11px 24px;color:${t.secondaryText};line-height:1.45}.pay-fact-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin:0 24px 16px}.pay-fact-grid>div{display:grid;gap:5px;padding:12px;border:1px solid ${t.divider};border-radius:9px;background:${t.raisedSurface}}.pay-fact-grid span{font-size:${t.smallFontPx}px;color:${t.secondaryText}}.pay-fact-grid strong{font-size:19px}.detail-disclosure{display:flex;align-items:center;justify-content:space-between;gap:16px;margin:0 24px 16px;padding:11px 13px;border:1px solid ${t.divider};border-radius:8px;background:${t.raisedSurface};color:${t.primaryText};font-weight:700}.detail-disclosure span{color:${t.secondaryText};font-size:${t.smallFontPx}px;font-weight:400}.decision-dialog>.modal-footer{margin-top:0}
    .dialog.protected-editor{display:flex;flex-direction:column;width:min(1120px,calc(100vw - 16px));max-height:calc(100vh - 16px);overflow:auto}.protected-editor>*,.reconcile-dialog>*{flex:none}.protected-editor>header,.reconcile-dialog>header{align-items:flex-start;padding:14px 24px 12px}.protected-editor>header p,.reconcile-dialog>header p{margin:4px 0 0;color:${t.secondaryText}}.status-line{margin:10px 24px 0;font-weight:650;color:#ffd690}.protected-editor .scope-copy{margin:8px 24px}.stale-notice{margin:10px 24px 0;padding:10px 13px;border:1px solid ${t.info};background:rgba(96,165,250,.12);border-radius:8px;color:#b8d8ff;font-weight:650}.protected-editor .decision-table{margin:10px 24px 0}.protected-editor .warning-box{margin:10px 24px}.editor-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px 14px;margin:0 24px 10px}.editor-field{display:grid;gap:4px;align-content:start}.editor-field>span{font-size:${t.smallFontPx}px;color:${t.secondaryText}}.editor-field>span i{font-style:normal;opacity:.7;margin-left:6px}.editor-field-half{grid-column:span 2}.editor-field-quarter{grid-column:span 1}.editor-field-wide{grid-column:1/-1}.editor-input{display:flex;align-items:center;gap:8px}.editor-input input,.editor-field textarea{width:100%;min-height:44px;background:${t.inputSurface};border:1px solid ${t.divider};border-radius:8px;color:${t.primaryText};padding:10px}.editor-field textarea{min-height:52px;resize:none}.editor-input input:disabled,.editor-field textarea:disabled{opacity:.55}.editor-input small{white-space:nowrap;color:${t.secondaryText}}.readonly-value{min-height:44px;display:flex;align-items:center;padding:10px;border:1px dashed ${t.divider};border-radius:8px;background:rgba(255,255,255,.025)}.field-note{color:${t.secondaryText};font-size:11px}.protected-editor .detail-disclosure,.reconcile-dialog .detail-disclosure{margin:0 24px 10px}.protected-editor .confirm-row{padding:9px 24px}.protected-editor .modal-footer,.reconcile-dialog .modal-footer{margin-top:auto;min-height:52px;padding:8px 24px}
    .reconcile-dialog{display:flex;flex-direction:column;max-height:calc(100vh - 16px);overflow:auto}.reconcile-dialog .fact-grid{margin-top:12px}.reconcile-truth{margin:10px 24px 0}.reconcile-dialog .pay-fact-grid{margin:10px 24px}.decision-options{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px;margin:0 24px 10px}.decision-option{display:grid;gap:4px;padding:10px 12px;border:1px solid currentColor;border-radius:9px;align-content:start}.decision-option strong{color:${t.primaryText}}.decision-option span{color:${t.secondaryText};font-size:${t.smallFontPx}px}.decision-option.tone-secondary{color:${t.info}}.decision-option.tone-primary{color:${t.positive}}.decision-option.tone-danger{color:#ff9ca6}
    @media (max-width:480px){.dialog-bg{padding:6px}.dialog.protected-editor{width:calc(100vw - 12px);max-height:calc(100vh - 12px)}.protected-editor>header{padding:14px 16px 12px}.editor-grid{grid-template-columns:1fr;margin:0 16px 10px}.editor-field-half,.editor-field-quarter,.editor-field-wide{grid-column:auto}.protected-editor .warning-box,.protected-editor .scope-copy,.status-line,.stale-notice,.protected-editor .decision-table{margin-left:16px;margin-right:16px}.protected-editor .modal-footer{flex-wrap:wrap;gap:8px;padding:10px 16px}.protected-editor .modal-footer div{flex-wrap:wrap}.protected-editor .confirm-row{padding:12px 16px;align-items:flex-start}.protected-editor .detail-disclosure{margin:0 16px 12px}}
    .timesheet-history-modal{height:808px}.history-statuses{display:flex;align-items:center;gap:8px}.history-scroll{padding:16px 22px;overflow:auto;display:grid;grid-template-columns:1.05fr .95fr;gap:14px;align-items:start}.truth-card{border:1px solid ${t.divider};border-radius:10px;background:${t.modalSurface};overflow:hidden}.truth-card h2{margin:0;padding:12px 14px;background:${t.raisedSurface};font-size:16px}.truth-row{display:grid;grid-template-columns:200px minmax(0,1fr) auto;gap:12px;align-items:center;padding:12px 14px;border-top:1px solid ${t.divider}}.truth-row>span{color:${t.secondaryText};font-size:${t.smallFontPx}px}.truth-row>strong{font-weight:600}.truth-row>button,.payment-history article>button{border:0;background:transparent;color:${t.info};font-weight:650;padding:4px}.difference-note{margin:0;padding:11px 14px;background:rgba(245,158,11,.08);color:#ffd690;border-top:1px solid rgba(245,158,11,.35)}.position-grid{display:grid;grid-template-columns:repeat(2,1fr)}.position-grid>div{display:grid;gap:4px;padding:13px 14px;border-top:1px solid ${t.divider};border-right:1px solid ${t.divider}}.position-grid span{color:${t.secondaryText};font-size:${t.smallFontPx}px}.position-grid strong{font-size:18px}.payment-history{grid-column:1/-1}.payment-history article{display:grid;grid-template-columns:1.2fr 1fr auto;gap:7px 16px;align-items:center;padding:12px 14px;border-top:1px solid ${t.divider}}.payment-history article>button{grid-column:3;justify-self:end}.history-head{display:flex;align-items:center;gap:10px}.history-head time{color:${t.secondaryText}}.payment-history article p{margin:0}.payment-history article small{color:${t.secondaryText}}
    .mytms-status-row{display:flex;gap:8px;flex-wrap:wrap}.mytms-phone .status{border:0;border-radius:${mt.pillRadiusPx}px;padding:6px 12px;font-size:${mt.labelFontPx}px;line-height:${mt.labelLinePx}px;font-weight:700}.mytms-phone .tone-positive{color:#8be3bc;background:${mt.cardSuccess}}.mytms-phone .tone-warning{color:#f5cf76;background:${mt.cardWarning}}.mytms-phone .tone-danger{color:#ff9ca6;background:${mt.cardDanger}}.mytms-phone .tone-info{color:${mt.blueBright};background:#15355d}.history-mobile-content{display:grid;gap:${mt.contentGapPx}px}.mytms-phone .truth-card{background:${mt.cardDefault};border-color:${mt.line};border-radius:${mt.cardRadiusPx}px;box-shadow:0 12px 24px rgba(0,0,0,.26)}.mytms-phone .truth-card h2,.approved-hours-card h2,.expense-card h2{background:transparent;color:${mt.text};padding:${mt.cardPaddingPx}px ${mt.cardPaddingPx}px 12px;font-size:${mt.headingFontPx}px;line-height:${mt.headingLinePx}px;margin:0}.mytms-phone .truth-row{grid-template-columns:1fr;padding:12px ${mt.cardPaddingPx}px;gap:3px;border-color:${mt.line}}.mytms-phone .truth-row>span{color:${mt.muted};font-size:${mt.smallFontPx}px;line-height:${mt.smallLinePx}px}.mytms-phone .truth-row>strong{color:${mt.text};font-size:${mt.bodyFontPx}px;line-height:${mt.bodyLinePx}px}.mytms-phone .truth-row>button{color:${mt.blueBright};text-align:left;padding:2px 0;font-size:${mt.bodyFontPx}px;line-height:${mt.bodyLinePx}px;border:0;background:transparent}.approved-hours-row{display:grid;gap:4px;padding:12px ${mt.cardPaddingPx}px;border-top:1px solid ${mt.line}}.approved-hours-row span{color:${mt.muted};font-size:${mt.smallFontPx}px}.approved-hours-row strong{color:${mt.text};font-size:${mt.bodyFontPx}px}.approved-hours-card>p{margin:0;padding:12px ${mt.cardPaddingPx}px;color:#f5cf76;background:${mt.cardWarning};border-top:1px solid ${mt.amber};font-size:${mt.smallFontPx}px;line-height:${mt.smallLinePx}px}.expense-card>p{margin:0;padding:0 ${mt.cardPaddingPx}px 14px;color:${mt.muted};font-size:${mt.smallFontPx}px;line-height:${mt.smallLinePx}px}.mobile-secondary{margin:0 ${mt.cardPaddingPx}px ${mt.cardPaddingPx}px;width:calc(100% - ${mt.cardPaddingPx * 2}px);min-height:48px;border:1px solid ${mt.blueBright};border-radius:${mt.buttonRadiusPx}px;background:${mt.controlSecondary};color:${mt.blueBright};font-weight:650}
    .timesheet-compare-body{padding:18px 24px;overflow:auto;display:grid;gap:12px}.authority-line{font-weight:700;color:${t.info};font-size:16px}.compare-banner{padding:10px 13px;border:1px solid ${t.warning};background:rgba(245,158,11,.08);border-radius:8px;color:#ffd690}.shift-heading{padding:11px 13px;background:${t.raisedSurface};border:1px solid ${t.divider};border-radius:8px;font-weight:650}.compare-head{display:grid;grid-template-columns:1fr 1fr;gap:12px}.compare-card{padding:14px;border:2px solid currentColor;border-radius:10px;background:${t.raisedSurface}}.compare-card h2{margin:0 0 6px;color:${t.primaryText};font-size:17px}.compare-card span{font-size:${t.smallFontPx}px}.compare-units{border:1px solid ${t.divider};border-radius:10px;overflow:hidden}.compare-unit{display:grid;grid-template-columns:160px 1fr 1fr 110px;gap:12px;align-items:center;padding:12px 14px;border-top:1px solid ${t.divider}}.compare-unit:first-child{border-top:0}.compare-unit span{color:${t.primaryText}}.compare-unit b{text-align:right;font-size:${t.smallFontPx}px}.compare-labels{background:${t.raisedSurface};color:${t.secondaryText};font-size:${t.smallFontPx}px}.reference-note{padding:10px 13px;border-radius:8px;background:rgba(239,68,68,.08);color:#ff9ca6}.ordinary-lines{border:1px solid ${t.divider};border-radius:10px;overflow:hidden}.ordinary-lines h2{margin:0;padding:13px 15px;background:${t.raisedSurface};font-size:17px}.ordinary-lines>div{display:grid;grid-template-columns:1.3fr 1fr .8fr .8fr;gap:10px;padding:13px 15px;border-top:1px solid ${t.divider}}.bulk-toolbar{padding:11px 22px;border-bottom:1px solid ${t.divider};display:flex;gap:24px;color:${t.secondaryText}}.bulk-three-pane{display:grid;grid-template-columns:170px 260px 1fr;min-height:0;flex:1}.bulk-filter,.bulk-list,.bulk-detail{overflow:auto;border-right:1px solid ${t.divider};padding:14px}.bulk-detail{border-right:0}.bulk-filter h2,.bulk-detail h2{font-size:16px;margin:0 0 12px}.bulk-filter button{display:block;width:100%;text-align:left;border:0;border-radius:7px;padding:10px;margin-bottom:6px;background:transparent;color:${t.secondaryText}}.bulk-filter button.filter-active{background:${t.raisedSurface};color:${t.primaryText}}.bulk-list{padding:0}.bulk-list-head,.bulk-list-row{display:grid;grid-template-columns:34px 1fr auto;gap:8px;align-items:center;padding:11px;border-bottom:1px solid ${t.divider}}.bulk-list-head{position:sticky;top:0;background:${t.raisedSurface};color:${t.secondaryText};font-size:${t.smallFontPx}px}.bulk-list-row small{color:${t.secondaryText}}.bulk-list-row.active{background:rgba(96,165,250,.1);border-left:3px solid ${t.info}}.bulk-detail .compare-unit{grid-template-columns:105px 1fr 1fr 90px;padding:10px}.bulk-detail .compare-card{padding:11px}.bulk-detail .shift-heading{font-size:13px}
  `;
}

function htmlDocument(policy, screen) {
  return `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width"><style>${stylesheet(policy.theme, policy.mytmsTheme)}</style></head><body>${renderScreen(policy, screen)}</body></html>`;
}

async function main() {
  const policy = JSON.parse(fs.readFileSync(policyPath, 'utf8'));
  assert(/^\d+\.\d+$/.test(String(policy.policyVersion || '')), `Expected a numbered Plan 5 modal policy, received ${policy.policyVersion}`);
  copy = policy.rendererCopy;
  assert(copy && RENDERER_COPY_KEYS.every(key => key in copy), 'Every generic-component label must be owned by policy.rendererCopy');
  const mytms = policy?.mytmsTheme;
  assert(JSON.stringify(mytms?.sourceComponents) === JSON.stringify(['Screen', 'Card', 'Button', 'StatusPill']), 'MyTMS policy must reuse the current candidate-app component set');
  assert(mytms?.pageBackground === '#071426' && mytms?.cardDefault === '#102541' && mytms?.text === '#f5f8ff' && mytms?.muted === '#aebbd0', 'MyTMS policy must use the current candidate-app page, Card and text tokens');
  assert(mytms?.blueDeep === '#2d73d2' && mytms?.blueBright === '#79b1ff' && mytms?.cardRadiusPx === 18 && mytms?.buttonRadiusPx === 14, 'MyTMS policy must use the current Button and radius tokens');
  assert(Array.isArray(mytms?.prohibitedSubstitutes) && mytms.prohibitedSubstitutes.includes('invented branded top bar'), 'MyTMS policy must forbid an invented app header');
  assert(String(policy?.interactionPolicies?.screenSimplicity?.multiSelectTableRule || '').includes('far-left sticky column-header cell'), 'Every multi-select modal table must use the far-left sticky header checkbox');
  assert(String(policy?.interactionPolicies?.screenSimplicity?.multiSelectTableRule || '').includes('Separate Select all and Unselect all buttons are forbidden'), 'Multi-select modal tables must forbid separate select-all buttons');
  const pricePolicy = policy?.interactionPolicies?.nhspPriceComparison;
  assert(pricePolicy?.sourceTokenAuthority?.xlsxNumericCell, 'NHSP policy must define XLSX raw-token authority');
  assert(pricePolicy?.sourceTokenAuthority?.htmlCell, 'NHSP policy must define HTML decoded-text authority');
  assert(pricePolicy?.xlsxBinary64SameValuePence?.profile === 'XLSX_BINARY64_SAME_VALUE_PENCE_V1', 'NHSP policy must seal the exact XLSX binary64 same-value profile');
  assert(pricePolicy?.penceAuthority?.storage?.includes('bigint'), 'NHSP policy must make bigint pence authoritative');
  assert(pricePolicy?.penceAuthority?.api?.includes('strings'), 'NHSP policy must use decimal-string pence APIs');
  assert(pricePolicy?.canonicalChargeOwner?.lockedHistoricalSegmentAllowed === false, 'Locked historical charge must not qualify a Contract');
  assert(String(pricePolicy?.canonicalChargeOwner?.wholeShiftAvailability || '').includes('UNVERIFIABLE'), 'Whole-shift pricing must fail closed before shared-owner parity');
  assert(String(pricePolicy?.results?.UNVERIFIABLE || '').includes('all-zero'), 'All-zero NHSP source rows must be UNVERIFIABLE');
  assert(String(pricePolicy?.isolation || '').includes('candidates and managers receive no money'), 'External query views must exclude pricing money');
  const officeAlerts = policy?.interactionPolicies?.officeAlerts;
  assert(officeAlerts?.headerLabel === 'Weekly source queries' && officeAlerts?.accessibleName === 'Weekly source queries' && officeAlerts?.panelHeading === 'Weekly source queries', 'Weekly source control, accessible name and panel heading must use the fixed label');
  for (const key of ['bankingBadgeDependency', 'bankingCountDependency', 'bankingPopoverDependency', 'bankingApiDependency', 'bankingStorageDependency', 'bankingAcknowledgementDependency']) {
    assert(officeAlerts?.[key] === false, `Weekly source alerts must not depend on Banking: ${key}`);
  }
  const placement = policy?.interactionPolicies?.settingsPlacement;
  assert(JSON.stringify(placement?.clientTabOrder) === JSON.stringify(['Timesheets', 'Shift times', 'Invoicing']), 'Client settings must keep the current three tabs in order');
  assert(placement?.preserveEveryExistingFieldAndSaveRule === true && placement?.allowAdditionalClientTab === false, 'Client settings must preserve existing fields and must not add a tab');
  const sourceExpensePlacement = placement?.clientSections?.find(section => section.section === 'Expenses from the weekly source file');
  assert(sourceExpensePlacement?.tab === 'Invoicing', 'Source-expense settings must remain beside the existing Candidate expense controls in Invoicing');
  assert(String(sourceExpensePlacement?.placement || '').includes('immediately after existing Candidate expense controls'), 'Source-expense settings must follow the existing Candidate expense controls');
  const managerReview = policy?.interactionPolicies?.managerSecureReview;
  assert(managerReview?.subjectPolicyRef === '04A_MANAGER_EMAIL_POLICY.json#/subject', 'Manager-email subject must have one policy owner');
  assert(managerReview?.singularAndPluralPolicyRef === '04A_MANAGER_EMAIL_POLICY.json#/format', 'Manager-email count wording must have one policy owner');
  assert(managerReview?.orderingPolicyRef === '04A_MANAGER_EMAIL_POLICY.json#/ordering', 'Manager-email ordering must have one policy owner');
  for (const duplicate of ['locale', 'timezone', 'normalisation', 'clientOrder', 'candidateOrder', 'shiftOrder']) {
    assert(!Object.hasOwn(managerReview, duplicate), `Modal policy must not duplicate manager-email ${duplicate}`);
  }
  const progressPolicy = policy?.interactionPolicies?.queryProgress;
  const expectedGroupColumns = ['selection', 'Candidate', 'Client', 'Issues', 'Candidate asked', 'Manager informed', 'Status', 'Age', 'Actions'];
  assert(JSON.stringify(progressPolicy?.groupColumns) === JSON.stringify(expectedGroupColumns) && progressPolicy?.expandShifts === true, 'Query Progress must use the fixed grouped columns and expandable shifts');
  assert(progressPolicy?.readyToReconcileForbiddenAsStatus === true, 'Ready to reconcile must not be a query status');
  const reconciliationWait = policy?.interactionPolicies?.reconciliationWait;
  assert(reconciliationWait?.visibleAction === 'Wait' && reconciliationWait?.storesPendingReconciliationOnly === true, 'Wait must store pending reconciliation only');
  assert(reconciliationWait?.createsOrPreparesFinancialOperation === false && reconciliationWait?.blocksSourceBackedInvoice === false, 'Wait must neither prepare a financial operation nor block the source-backed invoice');
  const missingJourney = policy?.interactionPolicies?.querySelection?.missingTimesheetJourney;
  assert(missingJourney?.candidateCta === 'Submit your Timesheet', 'Missing-Timesheet candidate CTA must open the existing Timesheet journey');
  assert(missingJourney?.officeAction === 'Remind candidate' && missingJourney?.officeSecondaryAction === 'View details' && missingJourney?.openTimesheetWhenAbsent === false, 'Missing-Timesheet Office actions must remain Remind candidate and View details while no Timesheet exists');
  assert(String(missingJourney?.t0Plus12Hours || '').includes('no manager membership'), 'Missing-Timesheet T+12 must keep the query visible without manager membership');
  const wrapperError = policy?.interactionPolicies?.uploadValidation?.excelHtmlCoverError;
  assert(wrapperError?.title === 'This file cannot be read on its own' && wrapperError?.body === 'Upload the Excel workbook (.xlsx), or choose an HTML file that contains the worksheet data.' && wrapperError?.action === 'Choose another file', 'Excel HTML wrapper rejection copy must remain fixed');
  const required = new Map([
    ['import-query-review', '01-import-query-review.png'],
    ['candidate-hours-check', '02-candidate-hours-check.png'],
    ['manager-secure-review', '03-manager-secure-review.png'],
    ['query-progress', '04-query-progress.png'],
    ['finalise-week', '05-finalise-week.png'],
    ['protected-shift-add', '06-protected-shift-add.png'],
    ['protected-shift-change', '06a-protected-shift-change.png'],
    ['protected-shift-stop', '06b-protected-shift-stop.png'],
    ['reconcile-approved-hours', '06c-reconcile-approved-hours.png'],
    ['protected-shift-stale', '06d-protected-shift-stale.png'],
    ['protected-shift-add-mobile', '06e-protected-shift-add-mobile.png'],
    ['correct-final-source', '07-correct-final-source.png'],
    ['office-alert', '09-office-alert.png'],
    ['imports-tab', '10-imports-tab.png'],
    ['manager-response-alerts', '11-manager-response-alerts.png'],
    ['record-not-worked', '13-record-not-worked.png'],
    ['client-settings', '14-client-settings.png'],
    ['candidate-nhsp-absent', '15-candidate-nhsp-absent.png'],
    ['timesheet-summary-status', '16-timesheet-summary-status.png'],
    ['contract-settings', '17-contract-settings.png'],
    ['choose-contract', '18-choose-contract.png'],
    ['office-timesheet-pay-history', '19-office-timesheet-pay-history.png'],
    ['worker-timesheet-approved-hours', '20-worker-timesheet-approved-hours.png'],
    ['worker-timesheet-ordinary', '20a-worker-timesheet-ordinary.png'],
    ['worker-timesheet-no-submission', '20b-worker-timesheet-no-submission.png'],
    ['worker-timesheet-source-fixed-expense', '20c-worker-timesheet-source-fixed-expense.png'],
    ['healthroster-finalise', '21-healthroster-finalise.png'],
    ['nhsp-ready-finalise', '22-nhsp-ready-finalise.png'],
    ['weekly-imports-two-journeys', '24-weekly-imports-two-journeys.png'],
    ['weekly-reference-setting', '30-weekly-reference-setting.png']
  ]);
  const ids = new Set();
  const files = new Set();
  for (const screen of policy.screens) {
    assert(screen.id && !ids.has(screen.id), `Duplicate or missing screen id: ${screen.id}`);
    assert(screen.file && !files.has(screen.file), `Duplicate or missing screen file: ${screen.file}`);
    ids.add(screen.id);
    files.add(screen.file);
    for (const variant of screen.captureVariants || []) {
      assert(variant.file && !files.has(variant.file), `Duplicate or missing capture-variant file: ${variant.file}`);
      assert(typeof variant.scrollSelector === 'string' && variant.scrollSelector.length > 0, `Capture variant ${variant.file} has no scroll selector`);
      assert(Number.isFinite(variant.scrollTop) && variant.scrollTop >= 0, `Capture variant ${variant.file} has an invalid scroll position`);
      files.add(variant.file);
    }
  }
  for (const [id, file] of required) {
    const screen = policy.screens.find(item => item.id === id);
    assert(screen && screen.file === file, `Required Plan 6 core screen ${id}/${file} is missing`);
  }
  assert(policy.screens.length === required.size, `Expected exactly ${required.size} modal-policy screens, received ${policy.screens.length}`);
  assert(!policy.screens.some(screen => screen.kind === 'manager-email'), 'Manager email fixtures belong only to 04A_MANAGER_EMAIL_POLICY.json');
  const managerReviewScreen = policy.screens.find(item => item.id === 'manager-secure-review');
  const managerReviewRows = (managerReviewScreen?.candidateSections || []).flatMap(section => section.rows || []);
  assert(managerReviewRows.length > 0, 'The manager secure-review fixture must contain at least one policy-owned query');
  assert(
    managerReviewRows.every(row => row.responseControl?.issueFamily && Array.isArray(emailPolicy.managerResponseText[row.responseControl.issueFamily]) && !Object.prototype.hasOwnProperty.call(row.responseControl, 'options')),
    'Manager response choices must come only from 04A_MANAGER_EMAIL_POLICY.json'
  );
  assert(
    managerReviewRows.every(row => !row.correctionFields || JSON.stringify((row.correctionFields.fields || []).map(field => field.label)) === JSON.stringify(emailPolicy.intendedHoursFields)),
    'Every manager correction fixture must use the policy-owned Start, Finish and Break fields'
  );
  const journey = policy.lockedSourceProfilePolicy?.protectedShiftPay;
  const journeyUi = policy.interactionPolicies?.protectedShiftPayJourney;
  assert(journey && journeyUi && !Object.hasOwn(journeyUi, 'state') && !Object.hasOwn(journeyUi, 'renderControls'), 'Protected shift pay must be governed by lockedSourceProfilePolicy.protectedShiftPay and interactionPolicies.protectedShiftPayJourney with no runtime gate');
  const editorScreens = policy.screens.filter(item => item.kind === 'protected-shift-editor-modal');
  assert(editorScreens.length === 4 && editorScreens.every(item => item.title === journey.userFacingName), 'The protected-shift-pay editor family must use the locked user-facing name');
  for (const screen of editorScreens) {
    assert(JSON.stringify((screen.fields || []).map(field => field.label)) === JSON.stringify(journey.fieldOrder), `${screen.id} fields must follow the policy field order`);
    assert(screen.warning === journey.warning, `${screen.id} must use the policy warning`);
    assert(screen.statusLine === journeyUi.candidatePayment.editor.statusLine && screen.confirmation === journeyUi.candidatePayment.editor.confirmation && screen.scopeCopy === journeyUi.candidatePayment.editor.scopeCopy, `${screen.id} must use the policy status line, confirmation and scope copy`);
    const labels = (screen.footer?.actions || []).map(action => action.label);
    const allowed = new Set([...journey.actions, 'Cancel', 'Recheck']);
    assert(labels.every(label => allowed.has(label)), `${screen.id} footer must use only locked protected-shift-pay actions`);
    if (screen.mode === 'ADD') assert(JSON.stringify(labels) === JSON.stringify(journeyUi.candidatePayment.editor.addActions), `${screen.id} add-mode actions must match policy`);
    if (screen.mode === 'CHANGE') assert(JSON.stringify(labels) === JSON.stringify(journeyUi.candidatePayment.editor.changeActions) && screen.comparison, `${screen.id} change-mode actions and comparison must match policy`);
    if (screen.mode === 'STALE') assert(screen.staleNotice === journeyUi.staleState.notice && JSON.stringify(labels) === JSON.stringify(journeyUi.staleState.actions) && (screen.fields || []).every(field => field.readOnly === true || field.editable === false), `${screen.id} stale state must save nothing and offer only Cancel and Recheck`);
    if (screen.canvas === 'mobile') assert(screen.mode === 'ADD', 'The 390 px protected-shift-pay render must be the add editor');
  }
  assert(editorScreens.some(item => item.canvas === 'mobile') && editorScreens.filter(item => item.mode === 'ADD').length === 2 && editorScreens.some(item => item.mode === 'CHANGE') && editorScreens.some(item => item.mode === 'STALE'), 'Protected shift pay must render add (desktop and 390 px), change and stale states');
  const stopScreen = policy.screens.find(item => item.id === 'protected-shift-stop');
  assert(stopScreen?.title === journeyUi.candidatePayment.stop.title && stopScreen?.note === journeyUi.candidatePayment.stop.note && stopScreen?.confirmation === journeyUi.candidatePayment.stop.confirmation && JSON.stringify((stopScreen?.actions || []).map(action => action.label)) === JSON.stringify(journeyUi.candidatePayment.stop.actions) && stopScreen?.warning === journey.warning, 'Stop protected pay must use the policy title, note, confirmation, warning and actions');
  const reconcileScreen = policy.screens.find(item => item.id === 'reconcile-approved-hours');
  assert(reconcileScreen?.title === journeyUi.reconciliation.title, 'Reconcile approved hours must use the policy title');
  assert(JSON.stringify((reconcileScreen?.truthRows || []).map(row => row.label)) === JSON.stringify(journey.truthOrder), 'Reconcile approved hours truth rows must follow the policy truth order');
  assert(JSON.stringify((reconcileScreen?.position || []).map(row => row.label)) === JSON.stringify(journey.positionOrder), 'Reconcile approved hours position must follow the policy position order');
  assert(reconcileScreen?.position?.some(row => row.value === journey.waitStatus), 'Reconcile approved hours must show the policy Wait status');
  const reconcileLabels = (reconcileScreen?.footer?.actions || []).map(action => action.label);
  assert(journeyUi.reconciliation.actions.every(label => reconcileLabels.includes(label)) && JSON.stringify((reconcileScreen?.options || []).map(option => option.label)) === JSON.stringify(journeyUi.reconciliation.actions), 'Reconcile approved hours must offer exactly the policy reconciliation actions');
  const notWorkedScreen = policy.screens.find(item => item.id === 'record-not-worked');
  assert(notWorkedScreen?.warning === journeyUi.recordNotWorked.warning && notWorkedScreen?.confirmation === journeyUi.recordNotWorked.confirmation, 'Record shift as not worked must use the policy warning and confirmation');
  assert(journey.didNotWorkConfirmation === notWorkedScreen?.confirmation && journey.didNotWorkWarning === notWorkedScreen?.warning, 'The locked did-not-work confirmation and warning must equal the rendered record-not-worked copy');
  const renderedFamilyActions = new Set();
  for (const screen of [...editorScreens, reconcileScreen, notWorkedScreen]) {
    for (const action of screen?.footer?.actions || []) renderedFamilyActions.add(action.label);
  }
  for (const action of stopScreen?.actions || []) renderedFamilyActions.add(action.label);
  renderedFamilyActions.delete('Cancel');
  renderedFamilyActions.delete('Recheck');
  assert(JSON.stringify([...renderedFamilyActions].sort()) === JSON.stringify([...journey.actions].sort()), `The locked protected-shift-pay action list must equal the union of rendered footer actions; locked=${JSON.stringify(journey.actions)} rendered=${JSON.stringify([...renderedFamilyActions])}`);
  const healthRosterScreen = policy.screens.find(item => item.id === 'healthroster-finalise');
  assert(
    JSON.stringify((healthRosterScreen?.columns || []).map(column => column.label)) === JSON.stringify(policy.lockedSourceProfilePolicy.healthRosterWeeklySelfBill.columns),
    'The HealthRoster finalise fixture columns must match the locked source-profile policy'
  );
  assert(
    (healthRosterScreen?.rows || []).some(row => row?.status?.text === policy.lockedSourceProfilePolicy.healthRosterWeeklySelfBill.sourceStatus.rowNotFinalised && String(row?.actual || '').toLowerCase().includes('no confirmed hours')),
    'The HealthRoster fixture must show a non-finalised row with no confirmed system hours'
  );
  const nhspReadyScreen = policy.screens.find(item => item.id === 'nhsp-ready-finalise');
  assert(
    JSON.stringify((nhspReadyScreen?.columns || []).map(column => column.label)) === JSON.stringify(policy.lockedSourceProfilePolicy.nhspFinalBackingReport.columns),
    'The NHSP ready fixture columns must match the locked backing-report policy'
  );
  assert(
    (nhspReadyScreen?.rows || []).some(row => row.movement === policy.lockedSourceProfilePolicy.nhspFinalBackingReport.movementText.negative),
    'The NHSP ready fixture must show a physical full reversal'
  );
  for (const screen of policy.screens.filter(item => item.kind === 'office-alert-popover')) {
    assert(screen.channel === 'WEEKLY_SOURCE', `${screen.id} must use the independent Weekly source alert channel`);
    assert(screen.headerControl === officeAlerts.headerLabel && screen.accessibleName === officeAlerts.accessibleName && screen.heading === officeAlerts.panelHeading, `${screen.id} must use the policy-owned Weekly source labels`);
    assert(!JSON.stringify(screen).toLowerCase().includes('banking'), `${screen.id} must contain no Banking alert copy or state`);
  }
  const clientSettings = policy.screens.find(item => item.id === 'client-settings');
  assert(clientSettings?.preserveExistingFields === true, 'Client settings fixture must preserve existing fields');
  assert(JSON.stringify((clientSettings?.tabs || []).map(tab => tab.label)) === JSON.stringify(placement.clientTabOrder), 'Client settings fixture tabs must match the current Client settings tabs');
  const invoicingSettings = clientSettings?.tabs?.find(tab => tab.label === 'Invoicing');
  const invoicingSections = (invoicingSettings?.sections || []).map(section => section.title);
  assert(invoicingSections.indexOf('Invoice rules') >= 0 && invoicingSections.indexOf('Expenses from the weekly source file') === invoicingSections.indexOf('Invoice rules') + 1, 'Source-expense settings must immediately follow Invoice rules');
  const invoiceRuleLabels = (invoicingSettings?.sections?.find(section => section.title === 'Invoice rules')?.fields || []).map(field => field.label);
  assert(invoiceRuleLabels.includes('Keep Candidate expenses on a separate Timesheet') && invoiceRuleLabels.includes('Expense Invoice Email'), 'Existing Candidate expense controls must remain in Invoice rules');
  const expectedScreenColumnKeys = ['select', 'candidate', 'client', 'issues', 'candidateAsked', 'managerInformed', 'status', 'oldest', 'actions'];
  const querySelection = policy?.interactionPolicies?.querySelection;
  assert(querySelection?.outreachHeaderCheckbox?.position === 'far-left cell of the sticky collapsed-group column header', 'Group selection must use the far-left sticky header checkbox');
  assert(querySelection?.expandedShiftHeaderCheckbox?.position === 'far-left cell of the sticky expanded-shift column header inside its candidate group', 'Expanded shift selection must use its own far-left sticky header checkbox');
  assert(JSON.stringify(querySelection?.selectionHeaderButtonsForbidden) === JSON.stringify(['Select all', 'Unselect all', 'Select all shifts', 'Unselect all shifts']), 'Separate select-all and unselect-all buttons must be forbidden');
  for (const screenId of ['import-query-review', 'query-progress']) {
    const groupedScreen = policy.screens.find(item => item.id === screenId);
    assert(JSON.stringify((groupedScreen?.columns || []).map(column => column.key)) === JSON.stringify(expectedScreenColumnKeys), `${screenId} must use the fixed grouped column order`);
    assert(groupedScreen?.columns?.[0]?.headerCheckboxScope === 'outreach-groups' && ['checked', 'unchecked', 'indeterminate'].includes(groupedScreen?.columns?.[0]?.headerCheckboxState), `${screenId} must put the group selector in the far-left header cell`);
    const toolbarLabels = (groupedScreen?.toolbar?.actions || []).map(action => action.label);
    assert(!querySelection.selectionHeaderButtonsForbidden.some(label => toolbarLabels.includes(label)), `${screenId} must not render separate select-all or unselect-all buttons`);
  }
  for (const screen of policy.screens) {
    const multiSelectColumn = (screen.columns || []).find(column => column.key === 'select');
    if (multiSelectColumn) {
      assert(screen.columns[0] === multiSelectColumn && multiSelectColumn.headerCheckboxState, `${screen.id} must place its multi-select checkbox in the far-left header cell`);
    }
    const visibleButtons = [
      ...(screen.toolbar?.actions || []),
      ...(screen.footer?.actions || []),
      ...(screen.actions || [])
    ].map(action => action.label);
    assert(!querySelection.selectionHeaderButtonsForbidden.some(label => visibleButtons.includes(label)), `${screen.id} must not contain a separate select-all or unselect-all button`);
  }
  const progressScreen = policy.screens.find(item => item.id === 'query-progress');
  const progressStatuses = (progressScreen?.rows || []).map(row => String(row?.status?.text || row?.status || ''));
  assert(!progressStatuses.includes('Ready to reconcile'), 'Ready to reconcile must not be rendered as a query status');
  const missingTimesheetGroup = (progressScreen?.rows || []).find(row => row?.children?.some(child => child?.candidateHours === 'Timesheet not submitted'));
  assert(missingTimesheetGroup?.status?.text === 'Waiting for Timesheet' && missingTimesheetGroup?.managerInformed === false, 'T12 missing-Timesheet group must remain waiting without manager membership');
  assert(JSON.stringify(missingTimesheetGroup?.actions) === JSON.stringify(['Remind missing timesheet', 'View details']), 'T12 missing-Timesheet group must offer only Remind candidate and View details');
  const missingTimesheetShift = missingTimesheetGroup?.children?.find(child => child?.candidateHours === 'Timesheet not submitted');
  assert(missingTimesheetShift?.nextAction?.text === 'Needs Office action', 'T12 missing-Timesheet next action must remain a separate Needs Office action indicator');
  const candidateDisplay = policy.interactionPolicies.candidateTimesheetDisplay;
  const candidateTimesheet = policy.screens.find(item => item.id === 'worker-timesheet-approved-hours');
  assert(candidateTimesheet?.route === 'SOURCE_AUTHORITY' && candidateTimesheet?.approvedTitle === candidateDisplay.approvedHoursLabel, 'MyTMS approved-hours label must come from the controlling policy');
  assert(candidateTimesheet?.expenses?.action === 'Add additional expense Timesheet', 'A source-authority MyTMS record must offer only the separate additional expense Timesheet');
  const ordinaryTimesheet = policy.screens.find(item => item.id === 'worker-timesheet-ordinary');
  assert(ordinaryTimesheet?.route === 'ORDINARY' && ordinaryTimesheet?.expenses?.action === 'Add expense' && (ordinaryTimesheet?.captureVariants || []).some(variant => variant.file === '23-worker-timesheet-expenses.png'), 'The ordinary non-source MyTMS Timesheet fixture must preserve same-record expense upload unchanged');
  const noSubmission = policy.screens.find(item => item.id === 'worker-timesheet-no-submission');
  assert(noSubmission?.readOnly === true && noSubmission?.differenceNote === candidateDisplay.noSubmissionState.submissionCopy && (noSubmission?.truthRows || []).length === 0 && (noSubmission?.approvedRows || []).length > 0 && noSubmission?.approvedTitle === candidateDisplay.approvedHoursLabel && !Object.hasOwn(noSubmission, 'expenses'), 'The no-submission MyTMS fixture must be read-only, claim no submission and show only Approved hours to be paid');
  assert(candidateDisplay.noSubmissionState.screen === noSubmission?.file, 'The no-submission policy must point at its rendered screen');
  const sourceFixedExpense = policy.screens.find(item => item.id === 'worker-timesheet-source-fixed-expense');
  assert(sourceFixedExpense && !Object.hasOwn(sourceFixedExpense, 'expenses'), 'The source-fixed-expense MyTMS fixture must hide every expense entry');
  for (const screen of policy.screens.filter(item => item.kind === 'candidate-timesheet-history')) {
    const candidateVisible = JSON.stringify(screen).toLowerCase();
    for (const term of ['£', 'gross', 'remittance', 'recovery', 'exceptional', 'protected', 'reconcil', 'invoice', 'rate', 'client system', 'source hours', 'payment history', 'submitted these hours']) {
      const hit = term === '£' ? candidateVisible.includes(term) : new RegExp(`\\b${term}`).test(candidateVisible);
      assert(!hit, `${screen.id} contains forbidden candidate wording: ${term}`);
    }
  }
  const policyText = JSON.stringify(policy);
  for (const marker of ['CLOSED_PENDING', '"renderControls"', 'DESIGN_PENDING', 'releaseGateRef', 'implementationGate', '"releaseGates"', 'GATE_CLOSED', 'Exceptional payment']) {
    assert(!policyText.includes(marker), `Modal policy must not carry the inactive runtime gate or obsolete label marker: ${marker}`);
  }
  const weeklyUx = policy.weeklyTimesheetExperiencePolicy;
  assert(weeklyUx?.referenceSetting?.default === 'Off', 'Weekly HealthRoster reference-required-before-pay must default Off');
  assert(weeklyUx?.mytms?.moneyForbidden === true && weeklyUx?.mytms?.ordinaryUnaffected === true, 'MyTMS ordinary behavior and money prohibition must remain fixed');
  // Detailed Simple Timesheet and Bulk Authorise route screens are owned by
  // 18_TIMESHEET_AUTHORISE_UI_POLICY.json. The core-policy renderer must not
  // duplicate or contradict that single controlling definition.
  const forbiddenVisibleTerms = ['manifest', 'fingerprint', 'generation', 'rpc', 'tsfin', 'workbench', 'database', 'row identity', 'enum', 'payload', 'source authority pointer', 'exceptional payment', 'release gate', 'design pending'];
  for (const screen of policy.screens) {
    const visibleMarkup = renderScreen(policy, screen).toLowerCase();
    for (const term of forbiddenVisibleTerms) {
      assert(!visibleMarkup.includes(term), `${screen.id} contains forbidden visible technical wording: ${term}`);
    }
  }
  fs.mkdirSync(outputDir, { recursive: true });
  fs.mkdirSync(tempDir, { recursive: true });
  const { chromium } = loadPlaywright();
  const browser = await chromium.launch({ headless: true, executablePath: process.env.CLOUDTMS_CHROME || 'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe' });
  try {
    const screens = policy.screens;
    for (const screen of screens) {
      const canvas = policy.canvases[screen.canvas];
      if (!canvas) throw new Error(`Canvas ${screen.canvas} not found`);
      const htmlPath = path.join(tempDir, `${screen.id}.html`);
      fs.writeFileSync(htmlPath, htmlDocument(policy, screen), 'utf8');
      const page = await browser.newPage({ viewport: canvas, deviceScaleFactor: 1 });
      await page.goto(`file:///${htmlPath.replaceAll('\\', '/')}`, { waitUntil: 'load' });
      await page.screenshot({ path: path.join(outputDir, screen.file), type: 'png', fullPage: false });
      for (const variant of screen.captureVariants || []) {
        const target = page.locator(variant.scrollSelector);
        assert(await target.count() === 1, `Capture variant ${variant.file} scroll selector must match exactly one element`);
        await target.evaluate((element, scrollTop) => { element.scrollTop = scrollTop; }, variant.scrollTop);
        await page.screenshot({ path: path.join(outputDir, variant.file), type: 'png', fullPage: false });
      }
      await page.close();
    }
  } finally {
    await browser.close();
  }
  const imageCount = policy.screens.reduce((count, screen) => count + 1 + (screen.captureVariants || []).length, 0);
  process.stdout.write(`Rendered ${imageCount} policy-governed images from ${policy.screens.length} screens to ${outputDir}\n`);
}

main().catch(err => { console.error(err); process.exitCode = 1; });

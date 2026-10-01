// Review-only generic renderer: all scenario copy and data come from policy JSON.
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const assert = require('node:assert/strict');
const { chromium } = require(process.env.CLOUDTMS_PLAYWRIGHT_MODULE || 'C:/Users/KierArthur/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright');
const base = __dirname;
const policyBytes = fs.readFileSync(path.join(base, 'journeys.policy.json'));
const policy = JSON.parse(policyBytes);
const sha = value => crypto.createHash('sha256').update(value).digest('hex');
const e = value => String(value ?? '').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;');
function table(screen) {
  if (!screen.rows) return '';
  return `${screen.subtabs?`<div class="subtabs">${screen.subtabs.map(tab=>`<button${tab.disabled?' disabled':''} class="${tab.selected?'primary':''}">${e(tab.label)}</button>`).join('')}</div>`:''}<table><thead><tr>${screen.columns.map(c=>`<th>${c==='Select'?'<input type="checkbox" checked aria-label="Select all eligible reports">':e(c)}</th>`).join('')}</tr></thead><tbody>${screen.rows.map(row=>`<tr>${row.map((cell,i)=>`<td data-label="${e(screen.columns[i])}">${screen.columns[i]==='Action'?`<button>${e(cell)}</button>`:e(cell)}</td>`).join('')}</tr>`).join('')}</tbody></table>`;
}
function screenMarkup(screen, journey) {
  return `<section class="modal" data-step="${e(screen.step)}"><header><span class="step">${e(screen.step)}</span><h2>${e(screen.title)}</h2><span class="location">${e(screen.tab)}</span></header><nav>${(journey.signed?policy.signedTabs:policy.sourceTabs).map(tab=>`<span class="${tab===screen.tab?'selected':''}">${e(tab)}</span>`).join('')}</nav><main><div class="context">${e(screen.context)}</div>${screen.fields?`<div class="fields">${screen.fields.map(([label,value])=>`<label>${e(label)}<span>${e(value)}</span></label>`).join('')}</div>`:''}${table(screen)}${screen.confirmation?`<label class="confirmation"><input type="checkbox" checked> ${e(screen.confirmation)}</label>`:''}${screen.note?`<p class="note">${e(screen.note)}</p>`:''}${screen.buttons?`<footer>${screen.buttons.map((b,i)=>`<button class="${i===0?'primary':''}">${e(b)}</button>`).join('')}</footer>`:''}</main></section>`;
}
function documentFor(journey) {
  const t = policy.theme;
  return `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>${e(journey.title)}</title><style>
  *{box-sizing:border-box}body{margin:0;padding:22px;background:${t.background};color:${t.text};font:14px/1.45 'Segoe UI',Arial,sans-serif}h1{font-size:23px;margin:0 0 6px}h2{font-size:17px;margin:0;font-weight:650}.review-label{color:${t.warning};font-size:12px;letter-spacing:.06em}.flow{display:flex;gap:9px;align-items:stretch;margin:16px 0 9px}.flow span{flex:1;padding:10px 12px;border:1px solid ${t.border};border-radius:8px;background:${t.surface};font-weight:600}.flow b{align-self:center;color:${t.muted}}.branch{color:${t.muted};margin:0 0 20px;max-width:1150px}.modal{border:1px solid ${t.border};border-radius:13px;background:${t.surface};overflow:hidden;margin:0 0 18px}header{display:flex;align-items:center;gap:10px;padding:12px 16px;background:linear-gradient(120deg,${t.raised},${t.surface})}.step{background:${t.accent};border-radius:6px;padding:2px 8px;font-weight:700}.location{margin-left:auto;color:${t.muted};font-size:12px}nav{display:flex;gap:19px;padding:0 16px;border-bottom:1px solid ${t.border}}nav span{padding:9px 3px;color:${t.muted};font-weight:600}nav .selected{color:${t.text};border-bottom:2px solid ${t.accent}}main{padding:12px 16px 14px}.context{color:${t.muted};font-size:12px;margin-bottom:11px}.fields{display:flex;gap:12px;margin-bottom:12px}.fields label{flex:1;color:${t.muted};font-size:12px}.fields label span{display:block;border:1px solid ${t.border};background:${t.background};padding:9px;border-radius:6px;color:${t.text};font-size:14px;margin-top:4px}table{border-collapse:collapse;width:100%;table-layout:fixed;text-align:left}th{font-size:11px;text-transform:uppercase;letter-spacing:.035em;color:${t.muted};background:${t.raised};padding:9px 10px}td{padding:8px 10px;border-bottom:1px solid #25364b;vertical-align:middle;overflow-wrap:anywhere}th:last-child,td:last-child{width:144px}button{font:600 12px/1.3 'Segoe UI',Arial,sans-serif;color:${t.text};background:#16283e;border:1px solid #435d7c;border-radius:6px;padding:7px 12px;min-height:31px;white-space:normal;max-width:100%}button.primary{background:#5a4ee5;border-color:#8a80ff}footer{display:flex;flex-wrap:wrap;gap:8px;margin-top:12px}.note{margin:10px 0 0;color:${t.muted};font-size:13px}.confirmation{display:block;background:#29291f;border:1px solid #7c6a38;padding:10px;border-radius:7px;margin-top:12px}.rule{font-size:11px;color:${t.muted};margin-top:10px}input{accent-color:${t.accent}}@media(max-width:850px){body{padding:12px}.flow{flex-direction:column}.flow b{transform:rotate(90deg)}.fields{flex-direction:column}table,tbody,tr,td{display:block;width:100%}thead{display:none}tr{padding:8px 0;border-bottom:1px solid ${t.border}}td,td:last-child{width:100%;border:0;display:grid;grid-template-columns:140px 1fr;gap:8px;padding:5px}td:before{content:attr(data-label);color:${t.muted};font-size:12px}td button{justify-self:start}nav{gap:12px}h1{font-size:20px}}
  .subtabs{display:flex;gap:8px;margin-bottom:12px}button:disabled{opacity:.45}@media(pointer:coarse){button{min-height:44px}}
  </style></head><body><div class="review-label">${e(policy.status)}</div><h1>${e(journey.title)}</h1><div class="flow">${journey.flow.map((item,i)=>`${i?'<b aria-hidden="true">→</b>':''}<span>${e(item)}</span>`).join('')}</div><p class="branch">${e(journey.branch)}</p>${journey.screens.map(s=>screenMarkup(s,journey)).join('')}<p class="rule">Policy ${e(policy.version)} · ${journey.rules.map(e).join(' / ')} · Successive screen states, not simultaneous dialogs</p></body></html>`;
}
async function main() {
  assert.equal(policy.journeys.length,5);
  for (const journey of policy.journeys) {
    assert(journey.rules.length && journey.flow.length===4);
    for(const screen of journey.screens) {
      assert(screen.rows.every(row=>row.length===screen.columns.length),`${journey.id}: column mismatch`);
      assert(!JSON.stringify(screen).includes('Manage questions'));
    }
    if (journey.signed) {
      assert(journey.screens.every(s=>!['Finalise','History'].includes(s.tab)));
      assert(!journey.screens.flatMap(s=>s.buttons||[]).some(b=>/Accept system|Protect|Finalise/i.test(b)));
    } else if(journey.id!=='05-query-detail') assert.equal(journey.screens.at(-1).tab,'History');
  }
  const out=path.join(base,'screens'); fs.mkdirSync(out,{recursive:true});
  const browser=await chromium.launch({headless:true,executablePath:process.env.CLOUDTMS_CHROME||'C:/Program Files/Google/Chrome/Application/chrome.exe'});
  const evidence={policyVersion:policy.version,policySha256:sha(policyBytes),kind:'LOCAL_POLICY_MOCKUPS_NOT_RUNTIME_PROOF',screens:[]};
  try {
    for(const journey of policy.journeys) {
      const html=documentFor(journey); fs.writeFileSync(path.join(out,`${journey.id}.html`),html);
      const page=await browser.newPage({viewport:{width:1360,height:900},deviceScaleFactor:1});
      await page.setContent(html); await page.evaluate(()=>document.fonts.ready);
      const png=path.join(out,`${journey.id}.png`);
      const desktop=await page.evaluate(()=>({width:innerWidth,contentWidth:document.documentElement.scrollWidth,rows:[...document.querySelectorAll('tbody tr')].map(r=>Math.round(r.getBoundingClientRect().height))}));
      assert(desktop.contentWidth<=desktop.width,`${journey.id}: desktop overflow`);
      await page.screenshot({path:png,fullPage:true});
      await page.setViewportSize({width:760,height:900});
      const narrow=await page.evaluate(()=>({width:innerWidth,contentWidth:document.documentElement.scrollWidth}));
      assert(narrow.contentWidth<=narrow.width,`${journey.id}: narrow overflow`);
      evidence.screens.push({id:journey.id,sha256:sha(fs.readFileSync(png)),desktop,narrow});
      await page.close();
    }
  } finally {await browser.close();}
  fs.writeFileSync(path.join(base,'evidence.json'),JSON.stringify(evidence,null,2)+'\n');
  console.log(JSON.stringify(evidence,null,2));
}
main().catch(error=>{console.error(error);process.exitCode=1;});

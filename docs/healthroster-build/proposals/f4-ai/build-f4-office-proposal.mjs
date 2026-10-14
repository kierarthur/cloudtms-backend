import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url));
const input=process.argv[2];
assert.ok(input,'explicit read-only Office main.js required');
const bytes=fs.readFileSync(input), native=bytes.toString('utf8').replace(/\r\n/g,'\n');
let next=native; const hunks=[];
function change(anchor,replacement) {
  assert.equal(next.split(anchor).length,2,'Office anchor changed '+anchor.slice(0,60));
  const index=native.indexOf(anchor);assert.ok(index>=0,'anchor belongs to original');
  hunks.push({line:native.slice(0,index).split('\n').length,anchor,replacement});
  next=next.replace(anchor,replacement);
}
change(`    amount_ex_vat: a.amount_ex_vat
  })).filter`, `    amount_ex_vat: a.amount_ex_vat,
    ...(invoiceModalGetInvoiceData(modalCtx).weekly_source_invoice?.is_weekly_source_invoice === true
      && Object.prototype.hasOwnProperty.call(a,'vat_amount') ? {vat_amount:a.vat_amount} : {})
  })).filter`);
change(`    const vat = ex * (vatRatePct / 100);
    subtotal_ex_vat += ex;`, `    const source = invData.weekly_source_invoice;
    const helper = window.CloudTmsSourceInvoiceAdjustments;
    const vat = source?.is_weekly_source_invoice === true
      ? (Object.prototype.hasOwnProperty.call(a,'vat_amount') ? Number(a.vat_amount)
        : Number(helper.defaultVat(a.amount_ex_vat,helper.context(source).default_vat_rate_pct)))
      : ex * (vatRatePct / 100);
    subtotal_ex_vat += ex;`);
change(`  const { invoice } = invoiceModalGetInvoiceData(parentCtx);

  // Ensure showModal`, `  const sourceInvoiceData = invoiceModalGetInvoiceData(parentCtx);
  const { invoice } = sourceInvoiceData;
  if (sourceInvoiceData.weekly_source_invoice?.is_weekly_source_invoice === true) {
    return window.CloudTmsSourceInvoiceAdjustments?.open(parentCtx,{
      invoiceData:sourceInvoiceData,canEdit:invoiceModalIsEditable(invoice,parentCtx,'lines'),
      showModal,document,newToken:invoiceModalNewClientToken,
      close:()=>document.getElementById('btnCloseModal')?.click(),rerender
    });
  }

  // Ensure showModal`);
change(`        \` : ''}
      </div>

      \${isEditing && hasPendingEdits ?`, `        \` : ''}
        \${isEditing && weekly_source_invoice?.is_weekly_source_invoice === true
          && window.CloudTmsSourceInvoiceAdjustments?.context(weekly_source_invoice) ? \`
          <button type="button" class="btn btn-sm btn-outline-secondary" data-action="inv-add-adjustment" \${addDisabledAttr}>+ Add adjustment</button>
        \` : ''}
      </div>

      \${isEditing && hasPendingEdits ?`);
change(`        <tbody>\${sourceBody || \`<tr><td colspan="\${isEditing ? 7 : 6}" class="text-muted">No source lines remain on this invoice.</td></tr>\`}</tbody>
      </table>
    </div>\`;`, `        <tbody>\${sourceBody || \`<tr><td colspan="\${isEditing ? 7 : 6}" class="text-muted">No source lines remain on this invoice.</td></tr>\`}</tbody>
      </table>
      \${window.CloudTmsSourceInvoiceAdjustments?.rows(items,modalCtx.invoiceEdits?.add_adjustments,
        removedSet,weeklySource,isEditing,esc,fmtMoneyLocal) || ''}
    </div>\`;`);
const sha=value=>crypto.createHash('sha256').update(value).digest('hex');
fs.writeFileSync(path.join(here,'OFFICE_HUNKS.json'),JSON.stringify({
  input:{path:input,sha256:sha(bytes)},status:'PROPOSAL_UNMOUNTED',
  requiredScriptBeforeMain:'js/weekly-source/source-invoice-adjustments.js',hunks
},null,2)+'\n');
let offset=0;
const patch=hunks.sort((a,b)=>a.line-b.line).map(h=>{
  const old=h.anchor.split('\n'),replacement=h.replacement.split('\n');
  const at=h.line+offset;offset+=replacement.length-old.length;
  return `@@ -${h.line},${old.length} +${at},${replacement.length} @@\n${old.map(line=>'-'+line).join('\n')}\n${replacement.map(line=>'+'+line).join('\n')}\n`;
}).join('');
fs.writeFileSync(path.join(here,'OFFICE_ROOT.diff'),'--- a/js/main.js\n+++ b/js/main.js\n'+patch);
const names=['invoiceModalComputePreviewTotals','invoiceModalSaveEdits','openInvoiceAddAdjustmentModal','renderInvoiceModalContent','renderInvoiceLinesTable'];
const functions=names.map(name=>{
  const start=next.search(new RegExp(`^(?:async )?function ${name}\\(`,'m'));
  assert.ok(start>=0,name);const following=next.slice(start+1).search(/\n(?:async )?function /);
  return {name,source:next.slice(start,following<0?next.length:start+1+following)};
});
fs.writeFileSync(path.join(here,'OFFICE_COMPLETE_AFFECTED_FUNCTIONS.js'),functions.map(f=>f.source).join('\n\n')+'\n');
console.log(JSON.stringify({status:'SOURCE_GENERATED',hunks:hunks.length,functions:functions.length,input_sha256:sha(bytes)}));

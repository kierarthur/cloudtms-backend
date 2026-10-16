import {WeeklySourceParserError} from './errors.js';
import {parseSourceMoneyCell} from './money.js';
import {parseReportLineVatCell,parseReportLineTotalCell} from './source-invoice-money.js';
import {assertRuleSetReportLayout} from './rule-set-validation.js';
import {boundedCellEvidence,cellText,isBlankCell,parseDateCell,parseTimeCell,serialiseIssue,stableSortByPhysicalRow} from './shared.js';

function literal(cell,label) {
  if(!cell||isBlankCell(cell)||['FORMULA','ERROR','BOOLEAN','DATE'].includes(cell.kind))throw new WeeklySourceParserError('SELF_BILL_REPORT_LITERAL_REQUIRED',`${label} must be a literal value.`);
  return cell.kind==='NUMBER'?String(cell.rawToken):cellText(cell);
}
function date(value){return parseDateCell({kind:'STRING',text:value});}
function invoiceDate(cell){const value=literal(cell,'Invoice date'),match=/^(\d{2})\/(\d{2})\/(\d{2})$/.exec(value);return match?date(`20${match[3]}-${match[2]}-${match[1]}`):{ok:false};}
function requestedOn(cell) {
  const value=literal(cell,'Requested On');
  if(/^\d{4}-\d{2}-\d{2}$/.test(value)){const d=date(value);return d.ok?`${d.value}T00:00:00`:null;}
  let m=/^(\d{1,2})-(\d{1,2})-(\d{4}) (\d{1,2}):(\d{2})$/.exec(value);
  if(m){const d=date(`${m[3]}-${m[2].padStart(2,'0')}-${m[1].padStart(2,'0')}`),h=Number(m[4]),min=Number(m[5]);return d.ok&&h<24&&min<60?`${d.value}T${m[4].padStart(2,'0')}:${m[5]}:00`:null;}
  m=/^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) (\d{1,2}) (\d{4}) (\d{1,2}):(\d{2})(AM|PM)$/i.exec(value);
  if(m){const months=['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'],month=String(months.indexOf(m[1].toLowerCase())+1).padStart(2,'0'),d=date(`${m[3]}-${month}-${m[2].padStart(2,'0')}`),h=Number(m[4]),min=Number(m[5]);if(!d.ok||h<1||h>12||min>59)return null;return `${d.value}T${String(h%12+(m[6].toUpperCase()==='PM'?12:0)).padStart(2,'0')}:${m[5]}:00`;}
  return null;
}
const component=(evidence,cell,parsed)=>({...boundedCellEvidence(cell,evidence.sourceKind,128),parseState:'PARSED',parsedPence:parsed.pence,...(parsed.normalisation?{normalisation:parsed.normalisation}:{}),...(parsed.rounding?{rounding:parsed.rounding}:{})});
export function normaliseAcaciumReportEvidence(evidence,profile,options={}) {
  const {sheet,columns,metadata}=profile,rows=[],fatalErrors=[],warnings=[],seen=new Map(),numbers=new Set(),dates=new Set();
  if(options.ruleValues)assertRuleSetReportLayout(options.ruleValues,metadata.layout);
  for(let physicalRow=2;physicalRow<=(sheet.lastPhysicalRow??1);physicalRow++){
    if(!(sheet.rows.find(r=>r.physicalRow===physicalRow)?.cells??[]).some(c=>!isBlankCell(c)))continue;
    const cell=key=>sheet.getCell(physicalRow,columns[key]);
    try {
      const requestId=literal(cell('requestId'),'Request Id'),workerName=literal(cell('workerName'),'Staff');
      if(requestId.length>160||!requestId.trim())throw new WeeklySourceParserError('SELF_BILL_REPORT_REQUEST_INVALID','Request Id is missing or too long.');
      if(seen.has(requestId))throw new WeeklySourceParserError('WEEKLY_SOURCE_SELF_BILL_REPORT_DUPLICATE_REQUEST','A report contains a repeated Request Id.');
      seen.set(requestId,physicalRow);
      const workDate=parseDateCell(cell('date'),{date1904:evidence.date1904}),start=parseTimeCell(cell('actualStart')),end=parseTimeCell(cell('actualEnd')),issued=invoiceDate(cell('invoiceDate'));
      const requested=requestedOn(cell('requestedOn')),reportNumber=literal(cell('reportNumber'),'Invoice number').toUpperCase();
      if(!workDate.ok||!start.ok||!end.ok||!issued.ok||!requested)throw new WeeklySourceParserError('SELF_BILL_REPORT_DATE_TIME_INVALID','A report date or time is invalid.');
      if(!/^XYOXLR\d{6}$/.test(reportNumber)||reportNumber.slice(6)!==issued.value.slice(8,10)+issued.value.slice(5,7)+issued.value.slice(2,4))throw new WeeklySourceParserError('SELF_BILL_REPORT_NUMBER_INVALID','The report number and invoice date do not agree.');
      numbers.add(reportNumber);dates.add(issued.value);
      const net=parseSourceMoneyCell(cell('net')),vat=metadata.layout==='A'?parseReportLineVatCell(cell('vat')):parseSourceMoneyCell(cell('vat')),gross=metadata.layout==='A'?parseReportLineTotalCell(cell('gross')):parseSourceMoneyCell(cell('gross'));
      if(BigInt(net.pence)+BigInt(vat.pence)!==BigInt(gross.pence))warnings.push(serialiseIssue('SELF_BILL_REPORT_ARITHMETIC_WARNING',physicalRow,null,'Report net plus VAT differs from its total.'));
      const overnight=end.minutes<=start.minutes;
      rows.push({worksheetIndex:sheet.index,physicalRow,sourceRowId:`worksheet:${sheet.index}:row:${physicalRow}`,rowKind:'SOURCE_REPORT_LINE',sourcePosition:'INVOICE_REPORT_ONLY',requestId,workerName,date:workDate.value,reportNumber,invoiceDate:issued.value,requestedOn:requested,
        reportTimes:{start:start.value,end:end.value,overnight},
        pricingEvidence:{state:'PARSED',authority:'SELF_BILL_REPORT_INVOICE_ONLY',netPence:net.pence,vatPence:vat.pence,grossPence:gross.pence,components:{net:component(evidence,cell('net'),net),vat:component(evidence,cell('vat'),vat),gross:component(evidence,cell('gross'),gross)}}});
    }catch(error){fatalErrors.push(serialiseIssue(error.code||'SELF_BILL_REPORT_ROW_INVALID',physicalRow,null,error instanceof WeeklySourceParserError?error.message:'A report row could not be read.'));}
  }
  if(numbers.size!==1||dates.size!==1)fatalErrors.push(serialiseIssue('SELF_BILL_REPORT_SINGLE_INVOICE_REQUIRED',null,null,'Every line must belong to the same report number and invoice date.'));
  if(!rows.length)fatalErrors.push(serialiseIssue('SELF_BILL_REPORT_NO_DATA_ROWS',null,null,'The report contains no valid invoice lines.'));
  const total=key=>rows.reduce((sum,row)=>sum+BigInt(row.pricingEvidence[key]),0n).toString();
  return {rows:stableSortByPhysicalRow(rows),presentationRows:[],scope:{client:options.expectedClient?.trim()||null,backingReportNumber:[...numbers][0]||null,invoiceDate:[...dates][0]||null,reportLayout:metadata.layout},
    report:{reportNumber:[...numbers][0]||null,invoiceDate:[...dates][0]||null,layout:metadata.layout,lineCount:rows.length,netPence:total('netPence'),vatPence:total('vatPence'),grossPence:total('grossPence')},trailers:null,
    rowCounts:{total:rows.length,reportLines:rows.length,negativeLines:rows.filter(r=>BigInt(r.pricingEvidence.netPence)<0n).length},warnings,fatalErrors};
}

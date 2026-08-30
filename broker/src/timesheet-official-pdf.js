import QRCode from 'qrcode';
import { PDFDocument, StandardFonts, rgb } from 'pdf-lib';

const PAGE_WIDTH_MM = 297;
const PAGE_HEIGHT_MM = 210;
const MM_TO_PT = 72 / 25.4;
const CORPORATE_NAVY = rgb(14 / 255, 47 / 255, 103 / 255);
const CORPORATE_PALE_BLUE = rgb(238 / 255, 244 / 255, 252 / 255);
const CORPORATE_MIST = rgb(248 / 255, 250 / 255, 253 / 255);
const CORPORATE_BLUE = rgb(35 / 255, 91 / 255, 158 / 255);
const CORPORATE_INK = rgb(18 / 255, 27 / 255, 41 / 255);
const CORPORATE_WHITE = rgb(1, 1, 1);
const FONT_MINIMUMS = new WeakMap();
const WEEKDAY_NAMES = Object.freeze([
  'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'
]);

const LAYOUTS = Object.freeze([
  Object.freeze({
    name: 'NORMAL',
    margin: 7,
    baseFont: 7.2,
    smallFont: 6.4,
    headerFont: 8,
    rowHeight: 5.0,
    scheduleHeaderHeight: 7,
    scheduleTotalHeight: 6.5,
    detailsHeight: 26,
    declarationMinimumHeight: 31,
    signatureLineZoneHeight: 5.5,
    signatureOverlayHeight: 14,
    additionalRowHeight: 4.8,
    additionalHeaderHeight: 9,
    lineHeight: 3.8,
    gap: 2.2
  }),
  Object.freeze({
    name: 'COMPACT',
    margin: 6,
    baseFont: 6.4,
    smallFont: 5.9,
    headerFont: 7.2,
    rowHeight: 4.1,
    scheduleHeaderHeight: 6.2,
    scheduleTotalHeight: 5.8,
    detailsHeight: 23,
    declarationMinimumHeight: 27,
    signatureLineZoneHeight: 5,
    signatureOverlayHeight: 12,
    additionalRowHeight: 4.0,
    additionalHeaderHeight: 8,
    lineHeight: 3.2,
    gap: 1.7
  }),
  Object.freeze({
    name: 'ULTRA',
    margin: 5,
    baseFont: 6.0,
    smallFont: 5.5,
    headerFont: 6.4,
    rowHeight: 3.45,
    scheduleHeaderHeight: 5.5,
    scheduleTotalHeight: 5.2,
    detailsHeight: 21,
    declarationMinimumHeight: 24,
    signatureLineZoneHeight: 4.6,
    signatureOverlayHeight: 10.5,
    additionalRowHeight: 3.45,
    additionalHeaderHeight: 7,
    lineHeight: 2.75,
    gap: 1.2
  })
]);

function timesheetError(code, detail) {
  return Object.assign(new Error(code), { code, detail });
}

function safeText(value) {
  return value == null ? '' : String(value);
}

function yFromTop(mm) {
  return (PAGE_HEIGHT_MM - Number(mm || 0)) * MM_TO_PT;
}

function dateFromYmd(ymd) {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(safeText(ymd).slice(0, 10));
  if (!match) return null;
  const date = new Date(Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3])));
  if (Number.isNaN(date.getTime())
    || date.getUTCFullYear() !== Number(match[1])
    || date.getUTCMonth() !== Number(match[2]) - 1
    || date.getUTCDate() !== Number(match[3])) return null;
  return date;
}

function formatDmy(ymd) {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(safeText(ymd).slice(0, 10));
  return match ? `${match[3]}/${match[2]}/${match[1]}` : '';
}

function normaliseLines(value) {
  if (Array.isArray(value)) return value.map(safeText).map(line => line.trim()).filter(Boolean);
  if (value && typeof value === 'object' && Array.isArray(value.lines)) {
    return value.lines.map(safeText).map(line => line.trim()).filter(Boolean);
  }
  const text = safeText(value && typeof value === 'object' ? value.bodyText || value.text : value).trim();
  return text ? text.split(/\r?\n/).map(line => line.trim()).filter(Boolean) : [];
}

function countWrappedLines(lines, charactersPerLine) {
  return lines.reduce((count, line) => {
    const words = safeText(line).split(/\s+/).filter(Boolean);
    if (!words.length) return count;
    let lineCount = 1;
    let used = 0;
    for (const word of words) {
      const next = used ? used + 1 + word.length : word.length;
      if (used && next > charactersPerLine) {
        lineCount += 1;
        used = word.length;
      } else {
        used = next;
      }
    }
    return count + lineCount;
  }, 0);
}

function configuredWordingStyle(block, layout, minimumFontSize = 5.5) {
  const configuredFont = Number(block?.font_size);
  const configuredLineHeight = Number(block?.line_height_mm);
  const normalFont = Number.isFinite(configuredFont) && configuredFont > 0
    ? configuredFont
    : layout.smallFont;
  const normalLineHeight = Number.isFinite(configuredLineHeight) && configuredLineHeight > 0
    ? configuredLineHeight
    : layout.lineHeight;
  const fontSize = layout.name === 'NORMAL'
    ? Math.max(minimumFontSize, normalFont)
    : Math.max(minimumFontSize, Math.min(normalFont, layout.smallFont));
  const lineHeight = layout.name === 'NORMAL'
    ? normalLineHeight
    : Math.max(fontSize * 0.36, Math.min(normalLineHeight, layout.lineHeight));
  return Object.freeze({ fontSize, lineHeight });
}

function scheduleLineCount(model) {
  const days = Array.isArray(model?.week_period?.days) ? model.week_period.days : [];
  return days.reduce((count, day) =>
    count + Math.max(1, Array.isArray(day?.shift_lines) ? day.shift_lines.length : 0), 0);
}

function scheduleShifts(model) {
  const days = Array.isArray(model?.week_period?.days) ? model.week_period.days : [];
  return days.flatMap(day => Array.isArray(day?.shift_lines) ? day.shift_lines : []);
}

function estimateLayoutHeight(model, layout) {
  const wording = model.wording || {};
  const minimumFontSize = Number(model?.layout?.minimum_font_size || 5.5);
  const headerStyle = configuredWordingStyle(wording.header, layout, minimumFontSize);
  const footerStyle = configuredWordingStyle(wording.footer, layout, minimumFontSize);
  const workerStyle = configuredWordingStyle(
    wording.temporary_worker_declaration, layout, minimumFontSize
  );
  const clientStyle = configuredWordingStyle(
    wording.client_declaration, layout, minimumFontSize
  );
  const headerLines = normaliseLines(wording.header);
  const footerLines = normaliseLines(wording.footer);
  const headerWrappedLineCount = countWrappedLines(headerLines, Math.max(
    60, Math.floor(190 * (layout.smallFont / headerStyle.fontSize))
  ));
  const footerWrappedLineCount = countWrappedLines(footerLines, Math.max(
    60, Math.floor(190 * (layout.smallFont / footerStyle.fontSize))
  ));
  const workerDeclaration = normaliseLines(wording.temporary_worker_declaration);
  const clientDeclaration = normaliseLines(wording.client_declaration);
  const additional = model.additional_units_section || {};
  const additionalRows = additional.visible === true && Array.isArray(additional.rows)
    ? additional.rows
    : [];
  const minimumBlankRows = additionalRows.length
    ? Math.max(0, Number(additional.minimum_blank_space_rows || 0))
    : 0;
  const workerCharacters = Math.max(40, Math.floor(
    (layout.name === 'ULTRA' ? 112 : 96) * (layout.smallFont / workerStyle.fontSize)
  ));
  const clientCharacters = Math.max(40, Math.floor(
    (layout.name === 'ULTRA' ? 112 : 96) * (layout.smallFont / clientStyle.fontSize)
  ));
  const workerDeclarationHeight =
    countWrappedLines(workerDeclaration, workerCharacters) * workerStyle.lineHeight;
  const clientDeclarationHeight =
    countWrappedLines(clientDeclaration, clientCharacters) * clientStyle.lineHeight;
  const declarationHeight = Math.max(
    layout.declarationMinimumHeight,
    5 + Math.max(workerDeclarationHeight, clientDeclarationHeight)
      + layout.signatureLineZoneHeight
  );
  const headerBlock = 12 + layout.detailsHeight
    + (headerWrappedLineCount
      ? headerWrappedLineCount * headerStyle.lineHeight + layout.gap
      : 0);
  const schedule = layout.scheduleHeaderHeight
    + scheduleLineCount(model) * layout.rowHeight
    + layout.scheduleTotalHeight;
  const additionalHeight = additionalRows.length
    ? layout.additionalHeaderHeight
      + (additionalRows.length + minimumBlankRows) * layout.additionalRowHeight
      + layout.gap
    : 0;
  const footerHeight = footerWrappedLineCount
    ? footerWrappedLineCount * footerStyle.lineHeight + layout.gap
    : 0;
  return headerBlock + schedule + additionalHeight + declarationHeight + footerHeight
    + layout.gap * 5;
}

function measureLayoutHeightWithFonts(model, layout, regular) {
  const width = PAGE_WIDTH_MM - layout.margin * 2;
  const minimumFontSize = Number(model?.layout?.minimum_font_size || 5.5);
  const headerStyle = configuredWordingStyle(model?.wording?.header, layout, minimumFontSize);
  const footerStyle = configuredWordingStyle(model?.wording?.footer, layout, minimumFontSize);
  const headerCount = wrapText(
    regular, normaliseLines(model?.wording?.header), headerStyle.fontSize, width
  ).length;
  const footerCount = wrapText(
    regular, normaliseLines(model?.wording?.footer), footerStyle.fontSize, width
  ).length;
  const declarationGap = 5;
  const declarationWidth = (width - declarationGap) / 2;
  const declarationHeight = Math.max(
    layout.declarationMinimumHeight,
    ...[
      model?.wording?.temporary_worker_declaration,
      model?.wording?.client_declaration
    ].map(block => {
      const style = configuredWordingStyle(block, layout, minimumFontSize);
      return 5 + wrapText(
        regular, normaliseLines(block), style.fontSize, declarationWidth - 4
      ).length * style.lineHeight + layout.signatureLineZoneHeight;
    })
  );
  const additional = model?.additional_units_section || {};
  const additionalRows = additional.visible === true && Array.isArray(additional.rows)
    ? additional.rows : [];
  const minimumBlankRows = additionalRows.length
    ? Math.max(
      0,
      Number(additional.minimum_blank_space_rows || 0),
      Number(model?.layout?.minimum_additional_blank_rows || 0)
    )
    : 0;
  const required = 12
    + officialDetailsHeight(model, layout) + layout.gap
    + (headerCount ? headerCount * headerStyle.lineHeight + layout.gap : 0)
    + layout.scheduleHeaderHeight
    + scheduleLineCount(model) * layout.rowHeight
    + layout.scheduleTotalHeight + layout.gap
    + (additionalRows.length
      ? layout.additionalHeaderHeight
        + (additionalRows.length + minimumBlankRows) * layout.additionalRowHeight
        + layout.gap
      : 0)
    + declarationHeight
    + (footerCount ? footerCount * footerStyle.lineHeight + layout.gap : 0);
  return Object.freeze({ required, declarationHeight, minimumBlankRows });
}

function preflightLayoutHorizontalCapacity(model, layout, fonts) {
  const regular = fonts.regular;
  const bold = fonts.bold || regular;
  const margin = layout.margin;
  const width = PAGE_WIDTH_MM - margin * 2;
  const details = officialDetailsGeometry(
    model.form_variant, margin, width, paperReturnQrPanelRequested(model)
  );
  const ending = model.week_period || {};
  const timesheetNumber = safeText(
    model.timesheet_number || officialTimesheetNumber(model.timesheet_id)
  );
  const heading = `TIMESHEET   Time Sheet No. ${timesheetNumber}   Week ending (${
    safeText(ending.end_weekday_name)
  }): ${formatDmy(ending.end_date)}`;

  fitText(bold, safeText(model?.branding?.agency_name || 'ARMS'), 86, 9);
  fitText(bold, heading, 155, 8.5, 6);
  fitText(bold, 'Temporary Worker Details', details.workerWidth - 2, layout.headerFont);
  fitText(bold, 'Client Details', details.clientWidth - 2, layout.headerFont);

  const worker = model.worker || model.candidate || {};
  for (const value of [worker.surname, worker.first_name, worker.job_profile_title]) {
    fitText(regular, safeText(value), details.workerWidth - 33, layout.baseFont);
  }
  const client = model.client || {};
  for (const value of [
    client.name || client.hospital_display || client.hospital,
    client.site_ward || client.hospital_ward_display || client.ward_display
  ]) {
    fitText(regular, safeText(value), details.clientWidth - 41, layout.baseFont);
  }

  const columns = [
    ['Day', 14], ['Date', 22], ['Shift Start', 18], ['Shift End', 18],
    ['Break Start', 18], ['Break End', 18], ['Paid hrs', 18],
    ['Paid hrs (words)', 52], ['Band', 14], ['Booking Ref', 0]
  ];
  columns[columns.length - 1][1] = width
    - columns.slice(0, -1).reduce((sum, column) => sum + column[1], 0);
  for (const [label, columnWidth] of columns) {
    fitText(bold, label, columnWidth - 1.6, layout.smallFont);
  }
  for (const day of model?.week_period?.days || []) {
    fitText(
      bold,
      safeText(day.weekday_abbreviation || day.weekday_name).slice(0, 3),
      columns[0][1] - 1.6,
      layout.baseFont
    );
    fitText(regular, formatDmy(day.date), columns[1][1] - 1.6, layout.baseFont);
    for (const shift of day.shift_lines || []) {
      const [breakStart, breakEnd] = shiftBreakDisplay(shift);
      const paidMinutes = Number(shift.paid_minutes || 0);
      const values = [
        safeText(shift.display_start_local),
        safeText(shift.display_end_local),
        breakStart,
        breakEnd,
        decimalHours(paidMinutes),
        formatOfficialTimesheetHoursWords(paidMinutes),
        safeText(shift.band || model.band),
        safeText(shift.booking_reference)
      ];
      values.forEach((value, index) => fitText(
        regular,
        value,
        columns[index + 2][1] - 1.6,
        index === 5 ? layout.smallFont : layout.baseFont
      ));
    }
  }
  fitText(
    bold,
    'Total overall hours claimed (excluding breaks):',
    150,
    layout.baseFont
  );
  const totalMinutes = Number(model?.totals?.paid_minutes || 0);
  fitText(
    bold,
    `${decimalHours(totalMinutes)}  (${formatOfficialTimesheetHoursWords(totalMinutes)})`,
    86,
    layout.baseFont
  );

  const additional = model?.additional_units_section || {};
  const additionalRows = additional.visible === true && Array.isArray(additional.rows)
    ? additional.rows : [];
  if (additionalRows.length) {
    fitText(
      bold,
      safeText(additional.title || 'Additional rates / units'),
      width - 4,
      layout.baseFont
    );
    const widths = [95, 34, 30, width - 159];
    ['Rate Type', 'Date', 'Quantity', 'Unit'].forEach((value, index) =>
      fitText(bold, value, widths[index] - 2, layout.smallFont));
    additionalRows.forEach(row => {
      [row.rate_type, row.date ? formatDmy(row.date) : '', row.quantity, row.unit]
        .forEach((value, index) =>
          fitText(regular, safeText(value), widths[index] - 2, layout.baseFont));
    });
  }

  const declarationWidth = (width - 5) / 2;
  for (const block of [
    model?.wording?.temporary_worker_declaration,
    model?.wording?.client_declaration
  ]) {
    const titleFont = block?.title_bold === false ? regular : bold;
    fitText(
      titleFont,
      safeText(block?.title || 'Declaration'),
      declarationWidth - 2,
      layout.headerFont
    );
  }
  fitText(regular, 'Signature', declarationWidth - 32, layout.smallFont);
  fitText(regular, 'Date 30/12/2099', 22, layout.smallFont);
}

export async function selectOfficialTimesheetOnePageLayout(model, fonts = null) {
  let regular = fonts?.regular;
  let bold = fonts?.bold;
  if (!regular || !bold) {
    const measurementPdf = await PDFDocument.create({ updateMetadata: false });
    if (!regular) regular = await measurementPdf.embedFont(StandardFonts.Helvetica);
    if (!bold) bold = await measurementPdf.embedFont(StandardFonts.HelveticaBold);
  }
  FONT_MINIMUMS.set(regular, Number(model?.layout?.minimum_font_size || 5.5));
  FONT_MINIMUMS.set(bold, Number(model?.layout?.minimum_font_size || 5.5));
  const allowed = new Set(
    Array.isArray(model?.layout?.allowed_modes)
      ? model.layout.allowed_modes.map(value => safeText(value).toUpperCase())
      : LAYOUTS.map(layout => layout.name)
  );
  const minimumFontSize = Number(model?.layout?.minimum_font_size || 0);
  const minimumRowHeight = Number(model?.layout?.minimum_row_height_mm || 0);
  const minimumSignatureHeight = Number(model?.layout?.minimum_signature_height_mm || 0);
  for (const layout of LAYOUTS) {
    if (!allowed.has(layout.name)) continue;
    if (minimumFontSize && layout.smallFont < minimumFontSize) continue;
    if (minimumRowHeight && layout.rowHeight < minimumRowHeight) continue;
    if (minimumSignatureHeight
      && layout.signatureOverlayHeight < minimumSignatureHeight) continue;
    const available = PAGE_HEIGHT_MM - layout.margin * 2;
    const measured = measureLayoutHeightWithFonts(model, layout, regular);
    if (measured.required <= available) {
      try {
        preflightLayoutHorizontalCapacity(model, layout, { regular, bold });
        return Object.freeze({
          ...layout,
          requiredHeightMm: measured.required,
          measuredDeclarationHeightMm: measured.declarationHeight,
          measuredMinimumBlankRows: measured.minimumBlankRows
        });
      } catch (error) {
        if (error?.code !== 'TIMESHEET_ONE_PAGE_CAPACITY_EXCEEDED') throw error;
      }
    }
  }
  throw timesheetError('TIMESHEET_ONE_PAGE_CAPACITY_EXCEEDED', {
    schedule_lines: scheduleLineCount(model),
    additional_rows: Array.isArray(model?.additional_units_section?.rows)
      ? model.additional_units_section.rows.length
      : 0
  });
}

function twoDigitWords(number) {
  const ones = ['zero','one','two','three','four','five','six','seven','eight','nine'];
  const teens = ['ten','eleven','twelve','thirteen','fourteen','fifteen','sixteen','seventeen','eighteen','nineteen'];
  const tens = ['','','twenty','thirty','forty','fifty','sixty','seventy','eighty','ninety'];
  const value = Math.max(0, Math.trunc(number));
  if (value < 10) return ones[value];
  if (value < 20) return teens[value - 10];
  const unit = value % 10;
  return unit ? `${tens[Math.floor(value / 10)]} ${ones[unit]}` : tens[Math.floor(value / 10)];
}

function wholeNumberWords(number) {
  const value = Math.max(0, Math.trunc(Number(number) || 0));
  if (value < 100) return twoDigitWords(value);
  if (value < 1000) {
    const remainder = value % 100;
    return `${twoDigitWords(Math.floor(value / 100))} hundred${
      remainder ? ` and ${twoDigitWords(remainder)}` : ''
    }`;
  }
  throw timesheetError('TIMESHEET_HOURS_WORDING_CAPACITY_EXCEEDED', { hours: value });
}

export function formatOfficialTimesheetHoursWords(totalMinutes) {
  const minutes = Math.round(Number(totalMinutes));
  if (!Number.isFinite(minutes) || minutes <= 0) return '';
  const hours = Math.floor(minutes / 60);
  const remainder = minutes % 60;
  const parts = [];
  if (hours) {
    parts.push(`${wholeNumberWords(hours)} ${hours === 1 ? 'hour' : 'hours'}`);
  }
  if (remainder) {
    parts.push(`${twoDigitWords(remainder)} ${remainder === 1 ? 'minute' : 'minutes'}`);
  }
  const wording = parts.join(' and ');
  return wording ? `${wording.charAt(0).toUpperCase()}${wording.slice(1)}` : '';
}

function paperReturnQrPanelRequested(model) {
  return model?.layout?.paper_return_qr_panel === true;
}

function officialDetailsHeight(model, layout) {
  return paperReturnQrPanelRequested(model)
    ? Math.max(layout.detailsHeight, 48)
    : layout.detailsHeight;
}

function officialDetailsGeometry(formVariant, margin, width, paperReturnQrPanel = false) {
  const gap = 3;
  if (safeText(formVariant).toUpperCase() === 'QR_UNSIGNED' || paperReturnQrPanel) {
    const centreWidth = paperReturnQrPanel ? 48 : 28;
    const sideWidth = (width - centreWidth - gap * 2) / 2;
    return Object.freeze({
      variant: paperReturnQrPanel ? 'PAPER_RETURN_QR_THREE_PANEL' : 'QR_THREE_PANEL',
      gap,
      workerX: margin,
      workerWidth: sideWidth,
      clientX: margin + sideWidth + gap + centreWidth + gap,
      clientWidth: sideWidth,
      centreBox: Object.freeze({
        x: margin + sideWidth + gap,
        width: centreWidth
      })
    });
  }
  const sideWidth = width / 2;
  return Object.freeze({
    variant: 'ELECTRONIC_TWO_PANEL',
    gap: 0,
    workerX: margin,
    workerWidth: sideWidth,
    clientX: margin + sideWidth,
    clientWidth: sideWidth,
    centreBox: null
  });
}

export function officialTimesheetNumber(timesheetId) {
  let hash = 0x811c9dc5;
  for (const character of safeText(timesheetId)) {
    hash ^= character.charCodeAt(0);
    hash = Math.imul(hash, 0x01000193);
  }
  return String((hash >>> 0) % 100000000).padStart(8, '0');
}

export function buildOfficialWeekPeriod(weekEndingDate) {
  const end = dateFromYmd(weekEndingDate);
  if (!end) throw timesheetError('TIMESHEET_WEEK_ENDING_DATE_MISSING');
  const days = [];
  for (let offset = 6; offset >= 0; offset -= 1) {
    const date = new Date(end);
    date.setUTCDate(end.getUTCDate() - offset);
    const ymd = `${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, '0')}-${String(date.getUTCDate()).padStart(2, '0')}`;
    days.push(Object.freeze({
      row_key: `day:${ymd}`,
      display_order: days.length + 1,
      date: ymd,
      weekday_index: date.getUTCDay(),
      weekday_name: WEEKDAY_NAMES[date.getUTCDay()],
      weekday_abbreviation: WEEKDAY_NAMES[date.getUTCDay()].slice(0, 3),
      shift_lines: []
    }));
  }
  return Object.freeze({
    start_date: days[0].date,
    end_date: days[6].date,
    end_weekday_index: end.getUTCDay(),
    end_weekday_name: WEEKDAY_NAMES[end.getUTCDay()],
    start_weekday_name: days[0].weekday_name,
    source: 'TIMESHEET_WEEK_ENDING_DATE',
    configured_week_ending_weekday: end.getUTCDay(),
    days
  });
}

function drawLine(page, x1, top1, x2, top2, thickness = 0.35, color = CORPORATE_INK) {
  page.drawLine({
    start: { x: x1 * MM_TO_PT, y: yFromTop(top1) },
    end: { x: x2 * MM_TO_PT, y: yFromTop(top2) },
    thickness,
    color
  });
}

function fillBox(page, x, top, width, height, color) {
  page.drawRectangle({
    x: x * MM_TO_PT,
    y: yFromTop(top + height),
    width: width * MM_TO_PT,
    height: height * MM_TO_PT,
    color
  });
}

function drawBox(page, x, top, width, height, thickness = 0.4) {
  page.drawRectangle({
    x: x * MM_TO_PT,
    y: yFromTop(top + height),
    width: width * MM_TO_PT,
    height: height * MM_TO_PT,
    borderWidth: thickness,
    borderColor: CORPORATE_NAVY
  });
}

function fitText(font, value, maxWidthMm, preferredSize, minimumSize = 5.5) {
  const text = safeText(value);
  const enforcedMinimum = Math.max(
    Number(minimumSize || 0),
    Number(FONT_MINIMUMS.get(font) || 0)
  );
  let size = preferredSize;
  while (size > enforcedMinimum
    && font.widthOfTextAtSize(text, size) > maxWidthMm * MM_TO_PT) {
    size -= 0.2;
  }
  size = Math.max(size, enforcedMinimum);
  if (font.widthOfTextAtSize(text, size) > maxWidthMm * MM_TO_PT + 0.01) {
    throw timesheetError('TIMESHEET_ONE_PAGE_CAPACITY_EXCEEDED', {
      section: 'horizontal_text_fit',
      minimum_font_size: enforcedMinimum,
      available_width_mm: maxWidthMm
    });
  }
  return size;
}

function drawText(
  page, font, value, x, top, size, maxWidthMm = null, color = CORPORATE_INK
) {
  const text = safeText(value);
  const actualSize = maxWidthMm == null ? size : fitText(font, text, maxWidthMm, size);
  page.drawText(text, {
    x: x * MM_TO_PT,
    y: yFromTop(top) - actualSize,
    size: actualSize,
    font,
    color,
    maxWidth: maxWidthMm == null ? undefined : maxWidthMm * MM_TO_PT
  });
}

function drawCenteredText(page, font, value, x, top, width, size, color = CORPORATE_INK) {
  const text = safeText(value);
  const actual = fitText(font, text, width - 2, size);
  const textWidth = font.widthOfTextAtSize(text, actual) / MM_TO_PT;
  drawText(page, font, text, x + Math.max(1, (width - textWidth) / 2), top, actual, null, color);
}

function drawAlignedText(
  page, font, value, x, top, width, size, align = 'left', color = CORPORATE_INK
) {
  const text = safeText(value);
  const actual = fitText(font, text, width - 2, size);
  const textWidth = font.widthOfTextAtSize(text, actual) / MM_TO_PT;
  const mode = safeText(align).trim().toLowerCase();
  const offset = mode === 'right'
    ? Math.max(1, width - textWidth - 1)
    : mode === 'center'
      ? Math.max(1, (width - textWidth) / 2)
      : 1;
  drawText(page, font, text, x + offset, top, actual, null, color);
}

function wrapText(font, value, size, maxWidthMm) {
  const paragraphs = Array.isArray(value) ? value : normaliseLines(value);
  const output = [];
  const maximumWidth = maxWidthMm * MM_TO_PT;
  const splitOversizedToken = token => {
    const chunks = [];
    let chunk = '';
    for (const character of Array.from(token)) {
      const candidate = `${chunk}${character}`;
      if (chunk && font.widthOfTextAtSize(candidate, size) > maximumWidth) {
        chunks.push(chunk);
        chunk = character;
      } else {
        chunk = candidate;
      }
      if (font.widthOfTextAtSize(chunk, size) > maximumWidth + 0.01) {
        throw timesheetError('TIMESHEET_ONE_PAGE_CAPACITY_EXCEEDED', {
          section: 'wrapped_text_horizontal_fit',
          minimum_font_size: Number(FONT_MINIMUMS.get(font) || 0),
          available_width_mm: maxWidthMm
        });
      }
    }
    if (chunk) chunks.push(chunk);
    return chunks;
  };
  for (const paragraph of paragraphs) {
    const words = safeText(paragraph).split(/\s+/).filter(Boolean);
    let current = '';
    for (const word of words) {
      if (font.widthOfTextAtSize(word, size) > maximumWidth) {
        if (current) {
          output.push(current);
          current = '';
        }
        const chunks = splitOversizedToken(word);
        output.push(...chunks.slice(0, -1));
        current = chunks[chunks.length - 1] || '';
        continue;
      }
      const candidate = current ? `${current} ${word}` : word;
      if (current && font.widthOfTextAtSize(candidate, size) > maximumWidth) {
        output.push(current);
        current = word;
      } else {
        current = candidate;
      }
    }
    if (current) output.push(current);
  }
  return output;
}

function dataUrlBytes(value) {
  const match = /^data:([^;,]+);base64,(.+)$/i.exec(safeText(value));
  if (!match) return null;
  const binary = atob(match[2]);
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) bytes[index] = binary.charCodeAt(index);
  return { mediaType: match[1].toLowerCase(), bytes };
}

async function embedImage(pdf, identity) {
  const decoded = dataUrlBytes(identity?.data_url);
  if (!decoded) return null;
  if (decoded.mediaType === 'image/png') return pdf.embedPng(decoded.bytes);
  if (decoded.mediaType === 'image/jpeg') return pdf.embedJpg(decoded.bytes);
  throw timesheetError('RENDER_ASSET_MEDIA_UNSUPPORTED', decoded.mediaType);
}

export function fitImageContain(nativeWidth, nativeHeight, boxWidthMm, boxHeightMm) {
  const width = Number(nativeWidth);
  const height = Number(nativeHeight);
  const boxWidth = Number(boxWidthMm);
  const boxHeight = Number(boxHeightMm);
  if (![width, height, boxWidth, boxHeight].every(value => Number.isFinite(value) && value > 0)) {
    throw timesheetError('RENDER_ASSET_DIMENSIONS_INVALID');
  }
  const scale = Math.min(boxWidth / width, boxHeight / height);
  return Object.freeze({
    width_mm: width * scale,
    height_mm: height * scale,
    offset_x_mm: (boxWidth - width * scale) / 2,
    offset_y_mm: (boxHeight - height * scale) / 2,
    aspect_ratio_preserved: true
  });
}

function drawEmbeddedImage(page, image, box) {
  if (!image) return null;
  const fit = fitImageContain(image.width, image.height, box.width, box.height);
  page.drawImage(image, {
    x: (box.x + fit.offset_x_mm) * MM_TO_PT,
    y: yFromTop(box.top + box.height) + fit.offset_y_mm * MM_TO_PT,
    width: fit.width_mm * MM_TO_PT,
    height: fit.height_mm * MM_TO_PT
  });
  return fit;
}

function shiftBreakDisplay(shift) {
  const mode = safeText(shift.break_display_mode).toUpperCase();
  if (mode === 'EXPLICIT_INTERVAL') {
    return [safeText(shift.break_start_local), safeText(shift.break_end_local)];
  }
  if (mode === 'MINUTES_ONLY' && Number(shift.break_minutes) > 0) {
    return [`${Math.round(Number(shift.break_minutes))}mins`, ''];
  }
  return ['', ''];
}

function decimalHours(minutes) {
  return (Math.round((Number(minutes || 0) / 60) * 100) / 100).toFixed(2);
}

async function drawQrModules(page, value, box) {
  const text = safeText(value).trim();
  if (!text) throw timesheetError('TIMESHEET_QR_PAYLOAD_MISSING');
  const qr = QRCode.create(text, { errorCorrectionLevel: 'L' });
  const count = qr.modules.size;
  const quiet = 4;
  const modules = count + quiet * 2;
  const moduleSize = Math.min(box.width, box.height) / modules;
  const originX = box.x + (box.width - moduleSize * modules) / 2 + quiet * moduleSize;
  const originTop = box.top + (box.height - moduleSize * modules) / 2 + quiet * moduleSize;
  page.drawRectangle({
    x: box.x * MM_TO_PT,
    y: yFromTop(box.top + box.height),
    width: box.width * MM_TO_PT,
    height: box.height * MM_TO_PT,
    color: rgb(1, 1, 1)
  });
  for (let row = 0; row < count; row += 1) {
    for (let column = 0; column < count; column += 1) {
      if (!qr.modules.get(row, column)) continue;
      page.drawRectangle({
        x: (originX + column * moduleSize) * MM_TO_PT,
        y: yFromTop(originTop + (row + 1) * moduleSize),
        width: moduleSize * MM_TO_PT,
        height: moduleSize * MM_TO_PT,
        color: rgb(0, 0, 0)
      });
    }
  }
}

function drawWordingBlock(page, font, lines, x, top, width, size, lineHeight) {
  let cursor = top;
  for (const line of lines) {
    const wrapped = wrapText(font, [line], size, width);
    for (const part of wrapped) {
      drawText(page, font, part, x, cursor, size, width);
      cursor += lineHeight;
    }
  }
  return cursor;
}

export async function renderOfficialTimesheetPdfBytes(model, assets = {}) {
  const pdf = await PDFDocument.create({ updateMetadata: false });
  const regular = await pdf.embedFont(StandardFonts.Helvetica);
  const bold = await pdf.embedFont(StandardFonts.HelveticaBold);
  const minimumFontSize = Number(model?.layout?.minimum_font_size || 5.5);
  FONT_MINIMUMS.set(regular, minimumFontSize);
  FONT_MINIMUMS.set(bold, minimumFontSize);
  const layout = await selectOfficialTimesheetOnePageLayout(model, { regular, bold });
  const page = pdf.addPage([PAGE_WIDTH_MM * MM_TO_PT, PAGE_HEIGHT_MM * MM_TO_PT]);
  const logo = await embedImage(pdf, assets.logo || model?.branding?.logo);
  const candidateSignature = await embedImage(pdf, assets.candidate_signature || model?.signatures?.candidate);
  const authoriserSignature = await embedImage(pdf, assets.authoriser_signature || model?.signatures?.authoriser);

  const margin = layout.margin;
  const width = PAGE_WIDTH_MM - margin * 2;
  const right = margin + width;
  const headerWordingStyle = configuredWordingStyle(
    model?.wording?.header, layout, minimumFontSize
  );
  const footerWordingStyle = configuredWordingStyle(
    model?.wording?.footer, layout, minimumFontSize
  );
  const headerLines = normaliseLines(model?.wording?.header);
  const headerWrappedLines = wrapText(
    regular, headerLines, headerWordingStyle.fontSize, width
  );
  const footerLines = normaliseLines(model?.wording?.footer);
  const footerWrappedLines = wrapText(
    regular, footerLines, footerWordingStyle.fontSize, width
  );
  const footerHeight = footerWrappedLines.length
    ? footerWrappedLines.length * footerWordingStyle.lineHeight + layout.gap
    : 0;
  const declarationGap = 5;
  const declarationWidth = (width - declarationGap) / 2;
  const declarationBoxes = [
    {
      x: margin,
      signatureRole: 'CANDIDATE',
      title: safeText(model?.wording?.temporary_worker_declaration?.title || 'Temporary Worker Declaration'),
      wording: model?.wording?.temporary_worker_declaration,
      lines: normaliseLines(model?.wording?.temporary_worker_declaration),
      signature: candidateSignature,
      date: model?.signatures?.candidate?.signed_date
    },
    {
      x: margin + declarationWidth + declarationGap,
      signatureRole: 'AUTHORISER',
      title: safeText(model?.wording?.client_declaration?.title || 'Client Declaration'),
      wording: model?.wording?.client_declaration,
      lines: normaliseLines(model?.wording?.client_declaration),
      signature: authoriserSignature,
      date: model?.signatures?.authoriser?.signed_date
    }
  ].map(box => {
    const wordingStyle = configuredWordingStyle(box.wording, layout, minimumFontSize);
    const bodyLines = wrapText(
      regular, box.lines, wordingStyle.fontSize, declarationWidth - 4
    );
    return { ...box, wordingStyle, bodyLines };
  });
  const declarationHeight = Math.max(
    layout.declarationMinimumHeight,
    ...declarationBoxes.map(box =>
      5 + box.bodyLines.length * box.wordingStyle.lineHeight
        + layout.signatureLineZoneHeight)
  );
  const additional = model.additional_units_section || {};
  const additionalRows = additional.visible === true && Array.isArray(additional.rows)
    ? additional.rows
    : [];
  const blankRows = additionalRows.length
    ? Math.max(
      0,
      Number(additional.minimum_blank_space_rows || 0),
      Number(model?.layout?.minimum_additional_blank_rows || 0)
    )
    : 0;
  const lineCount = scheduleLineCount(model);
  const headerWordingHeight = headerWrappedLines.length
    ? headerWrappedLines.length * headerWordingStyle.lineHeight + layout.gap
    : 0;
  const additionalBaseHeight = additionalRows.length
    ? layout.additionalHeaderHeight
      + (additionalRows.length + blankRows) * layout.additionalRowHeight
      + layout.gap
    : 0;
  const baseContentHeight = 12
    + officialDetailsHeight(model, layout) + layout.gap
    + headerWordingHeight
    + layout.scheduleHeaderHeight + lineCount * layout.rowHeight
    + layout.scheduleTotalHeight + layout.gap
    + additionalBaseHeight
    + declarationHeight + footerHeight;
  const availableContentHeight = PAGE_HEIGHT_MM - margin * 2;
  if (baseContentHeight > availableContentHeight + 0.01) {
    throw timesheetError('TIMESHEET_ONE_PAGE_CAPACITY_EXCEEDED', {
      section: 'page_flow',
      required_height_mm: baseContentHeight,
      available_height_mm: availableContentHeight
    });
  }

  let flexibleHeight = Math.max(0, availableContentHeight - baseContentHeight);
  const detailsBonus = Math.min(3.2, flexibleHeight * 0.16);
  flexibleHeight -= detailsBonus;
  const scheduleHeaderBonus = Math.min(1.2, flexibleHeight * 0.06);
  flexibleHeight -= scheduleHeaderBonus;
  const scheduleTotalBonus = Math.min(1.4, flexibleHeight * 0.07);
  flexibleHeight -= scheduleTotalBonus;
  const additionalFlexUnits = additionalRows.length
    ? (additionalRows.length + blankRows) * 1.35
    : 0;
  const flexibleUnitHeight = flexibleHeight / Math.max(1, lineCount + additionalFlexUnits);
  const detailsHeight = officialDetailsHeight(model, layout) + detailsBonus;
  const scheduleHeaderHeight = layout.scheduleHeaderHeight + scheduleHeaderBonus;
  const scheduleTotalHeight = layout.scheduleTotalHeight + scheduleTotalBonus;
  const scheduleRowHeight = layout.rowHeight + flexibleUnitHeight;
  const additionalRowHeight = layout.additionalRowHeight + flexibleUnitHeight * 1.35;
  const additionalHeight = additionalRows.length
    ? layout.additionalHeaderHeight
      + (additionalRows.length + blankRows) * additionalRowHeight
    : 0;
  const declarationTop = PAGE_HEIGHT_MM - margin - footerHeight - declarationHeight;
  let top = margin;

  fillBox(page, margin, top, width, 0.8, CORPORATE_BLUE);
  let logoFit = null;
  const signatureFits = [];
  const signaturePlacements = [];
  if (logo) logoFit = drawEmbeddedImage(page, logo, { x: margin + 0.5, top: top + 0.8, width: 10.5, height: 10.5 });
  drawText(page, bold, safeText(model?.branding?.agency_name || 'ARMS'),
    margin + 13, top + 2.3, 9, 86, CORPORATE_NAVY);
  const ending = model.week_period;
  const timesheetNumber = safeText(model.timesheet_number || officialTimesheetNumber(model.timesheet_id));
  const heading = `TIMESHEET   Time Sheet No. ${timesheetNumber}   Week ending (${safeText(ending.end_weekday_name)}): ${formatDmy(ending.end_date)}`;
  const headingSize = fitText(bold, heading, 155, 8.5, 6);
  const headingWidth = bold.widthOfTextAtSize(heading, headingSize) / MM_TO_PT;
  drawText(page, bold, heading, right - headingWidth, top + 2.5, headingSize, null, CORPORATE_NAVY);
  top += 12;

  const detailsGeometry = officialDetailsGeometry(
    model.form_variant, margin, width, paperReturnQrPanelRequested(model)
  );
  const detailsTop = top;
  fillBox(page, detailsGeometry.workerX, detailsTop, detailsGeometry.workerWidth, 6, CORPORATE_NAVY);
  fillBox(page, detailsGeometry.clientX, detailsTop, detailsGeometry.clientWidth, 6, CORPORATE_NAVY);
  fillBox(page, detailsGeometry.workerX, detailsTop + 6, detailsGeometry.workerWidth, detailsHeight - 6, CORPORATE_MIST);
  fillBox(page, detailsGeometry.clientX, detailsTop + 6, detailsGeometry.clientWidth, detailsHeight - 6, CORPORATE_MIST);
  drawBox(page, detailsGeometry.workerX, detailsTop, detailsGeometry.workerWidth, detailsHeight);
  drawBox(page, detailsGeometry.clientX, detailsTop, detailsGeometry.clientWidth, detailsHeight);
  drawCenteredText(page, bold, 'Temporary Worker Details',
    detailsGeometry.workerX, detailsTop + 2, detailsGeometry.workerWidth, layout.headerFont, CORPORATE_WHITE);
  drawCenteredText(page, bold, 'Client Details',
    detailsGeometry.clientX, detailsTop + 2, detailsGeometry.clientWidth, layout.headerFont, CORPORATE_WHITE);
  const worker = model.worker || model.candidate || {};
  const client = model.client || {};
  const workerValueX = detailsGeometry.workerX + 31;
  const paperReturnPanel = paperReturnQrPanelRequested(model);
  const workerRows = paperReturnPanel ? [11, 25, 39] : [8, 14, 20];
  const clientRows = paperReturnPanel ? [15, 33] : [9, 17];
  drawText(page, bold, 'Surname:', detailsGeometry.workerX + 2, detailsTop + workerRows[0], layout.smallFont);
  drawText(page, regular, safeText(worker.surname), workerValueX, detailsTop + workerRows[0],
    layout.baseFont, detailsGeometry.workerWidth - 33);
  drawText(page, bold, 'First name:', detailsGeometry.workerX + 2, detailsTop + workerRows[1], layout.smallFont);
  drawText(page, regular, safeText(worker.first_name), workerValueX, detailsTop + workerRows[1],
    layout.baseFont, detailsGeometry.workerWidth - 33);
  drawText(page, bold, 'Job Profile Title:', detailsGeometry.workerX + 2, detailsTop + workerRows[2], layout.smallFont);
  drawText(page, regular, safeText(worker.job_profile_title), workerValueX, detailsTop + workerRows[2],
    layout.baseFont, detailsGeometry.workerWidth - 33);
  const clientValueX = detailsGeometry.clientX + 39;
  drawText(page, bold, 'Client Name / Hospital:', detailsGeometry.clientX + 2, detailsTop + clientRows[0], layout.smallFont);
  drawText(page, regular, safeText(client.name || client.hospital_display || client.hospital),
    clientValueX, detailsTop + clientRows[0], layout.baseFont, detailsGeometry.clientWidth - 41);
  drawText(page, bold, 'Site / Ward:', detailsGeometry.clientX + 2, detailsTop + clientRows[1], layout.smallFont);
  drawText(page, regular, safeText(client.site_ward || client.hospital_ward_display || client.ward_display),
    clientValueX, detailsTop + clientRows[1], layout.baseFont, detailsGeometry.clientWidth - 41);

  if (detailsGeometry.centreBox) {
    const qrBox = {
      x: detailsGeometry.centreBox.x,
      top: detailsTop,
      width: detailsGeometry.centreBox.width,
      height: detailsHeight
    };
    await drawQrModules(page, assets.qr_text, qrBox);
  }
  top += detailsHeight + layout.gap;

  if (headerLines.length) {
    top = drawWordingBlock(page, regular, headerLines, margin, top, width,
      headerWordingStyle.fontSize, headerWordingStyle.lineHeight);
    top += layout.gap;
  }

  const scheduleTop = top;
  const columns = [
    ['Day', 14], ['Date', 22], ['Shift Start', 18], ['Shift End', 18],
    ['Break Start', 18], ['Break End', 18], ['Paid hrs', 18],
    ['Paid hrs (words)', 52], ['Band', 14], ['Booking Ref', 0]
  ];
  const fixedWidth = columns.slice(0, -1).reduce((sum, column) => sum + column[1], 0);
  columns[columns.length - 1][1] = width - fixedWidth;
  const bodyHeight = lineCount * scheduleRowHeight;
  const scheduleHeight = scheduleHeaderHeight + bodyHeight + scheduleTotalHeight;
  let shadeTop = scheduleTop + scheduleHeaderHeight;
  model.week_period.days.forEach((day, dayIndex) => {
    const shadeHeight = Math.max(1, Array.isArray(day.shift_lines) ? day.shift_lines.length : 0)
      * scheduleRowHeight;
    if (dayIndex % 2 === 1) fillBox(page, margin, shadeTop, width, shadeHeight, CORPORATE_MIST);
    shadeTop += shadeHeight;
  });
  fillBox(page, margin, scheduleTop, width, scheduleHeaderHeight, CORPORATE_NAVY);
  drawBox(page, margin, scheduleTop, width, scheduleHeight);
  let columnX = margin;
  const columnStarts = [];
  for (const [, columnWidth] of columns) {
    columnStarts.push(columnX);
    columnX += columnWidth;
  }
  drawLine(page, margin, scheduleTop + scheduleHeaderHeight, right, scheduleTop + scheduleHeaderHeight);
  for (let index = 1; index < columns.length; index += 1) {
    drawLine(page, columnStarts[index], scheduleTop, columnStarts[index], scheduleTop + scheduleHeaderHeight + bodyHeight);
  }
  columns.forEach((column, index) => {
    drawText(page, bold, column[0], columnStarts[index] + 0.8,
      scheduleTop + 1.6, layout.smallFont, column[1] - 1.6, CORPORATE_WHITE);
  });

  let rowTop = scheduleTop + scheduleHeaderHeight;
  const days = model.week_period.days;
  for (const day of days) {
    const shifts = Array.isArray(day.shift_lines) ? day.shift_lines : [];
    const dayLineCount = Math.max(1, shifts.length);
    const dayHeight = dayLineCount * scheduleRowHeight;
    const rowTextTop = rowTop + Math.max(1.1, (scheduleRowHeight - layout.baseFont * 0.36) / 2);
    drawText(page, bold, safeText(day.weekday_abbreviation || day.weekday_name).slice(0, 3), columnStarts[0] + 0.8, rowTextTop, layout.baseFont, columns[0][1] - 1.6, CORPORATE_NAVY);
    drawText(page, regular, formatDmy(day.date), columnStarts[1] + 0.8, rowTextTop, layout.baseFont, columns[1][1] - 1.6);
    for (let index = 1; index < dayLineCount; index += 1) {
      drawLine(page, columnStarts[2], rowTop + index * scheduleRowHeight, right, rowTop + index * scheduleRowHeight, 0.22);
    }
    shifts.forEach((shift, index) => {
      const lineTop = rowTop + index * scheduleRowHeight
        + Math.max(1.05, (scheduleRowHeight - layout.baseFont * 0.36) / 2);
      const [breakStart, breakEnd] = shiftBreakDisplay(shift);
      const paidMinutes = Number(shift.paid_minutes || 0);
      const values = [
        safeText(shift.display_start_local),
        safeText(shift.display_end_local),
        breakStart,
        breakEnd,
        decimalHours(paidMinutes),
        formatOfficialTimesheetHoursWords(paidMinutes),
        safeText(shift.band || model.band),
        safeText(shift.booking_reference)
      ];
      values.forEach((value, valueIndex) => {
        const columnIndex = valueIndex + 2;
        drawText(page, regular, value, columnStarts[columnIndex] + 0.8, lineTop,
          valueIndex === 5 ? layout.smallFont : layout.baseFont,
          columns[columnIndex][1] - 1.6);
      });
    });
    rowTop += dayHeight;
    drawLine(page, margin, rowTop, right, rowTop, 0.3);
  }

  const totalMinutes = Number(model?.totals?.paid_minutes || 0);
  const totalTop = scheduleTop + scheduleHeaderHeight + bodyHeight;
  fillBox(page, margin, totalTop, width, scheduleTotalHeight, CORPORATE_PALE_BLUE);
  drawLine(page, margin, totalTop, right, totalTop, 0.35, CORPORATE_NAVY);
  drawText(page, bold, 'Total overall hours claimed (excluding breaks):', margin + 2, totalTop + 1.3, layout.baseFont, 150);
  drawText(page, bold, `${decimalHours(totalMinutes)}  (${formatOfficialTimesheetHoursWords(totalMinutes)})`,
    right - 88, totalTop + 1.3, layout.baseFont, 86);
  top = scheduleTop + scheduleHeight + layout.gap;

  if (additionalRows.length) {
    const titleDivider = top + layout.additionalHeaderHeight / 2;
    const headerDivider = top + layout.additionalHeaderHeight;
    fillBox(page, margin, top, width, titleDivider - top, CORPORATE_NAVY);
    fillBox(page, margin, titleDivider, width, headerDivider - titleDivider, CORPORATE_PALE_BLUE);
    additionalRows.forEach((row, index) => {
      if (index % 2 === 1) {
        fillBox(page, margin, headerDivider + index * additionalRowHeight,
          width, additionalRowHeight, CORPORATE_MIST);
      }
    });
    drawBox(page, margin, top, width, additionalHeight);
    drawText(page, bold, safeText(additional.title || 'Additional rates / units'),
      margin + 2, top + 1, layout.baseFont, width - 4, CORPORATE_WHITE);
    drawLine(page, margin, titleDivider, right, titleDivider, 0.25, CORPORATE_NAVY);
    drawLine(page, margin, headerDivider, right, headerDivider, 0.35);
    const addWidths = [95, 34, 30, width - 159];
    const addLabels = ['Rate Type', 'Date', 'Quantity', 'Unit'];
    let addX = margin;
    const addStarts = [];
    addWidths.forEach((columnWidth, index) => {
      addStarts.push(addX);
      if (index) drawLine(page, addX, titleDivider, addX, top + additionalHeight, 0.3);
      drawText(page, bold, addLabels[index], addX + 1, titleDivider + 0.7, layout.smallFont, columnWidth - 2);
      addX += columnWidth;
    });
    additionalRows.forEach((row, index) => {
      const lineTop = headerDivider + index * additionalRowHeight
        + Math.max(0.8, (additionalRowHeight - layout.baseFont * 0.36) / 2);
      const values = [
        row.rate_type,
        row.date ? formatDmy(row.date) : '',
        row.quantity,
        row.unit
      ];
      values.forEach((value, valueIndex) =>
        drawText(page, regular, value, addStarts[valueIndex] + 1, lineTop, layout.baseFont, addWidths[valueIndex] - 2));
    });
    top += additionalHeight + layout.gap;
  }

  const unusedVerticalGap = Math.max(0, declarationTop - top);
  if (top > declarationTop + 0.01) {
    throw timesheetError('TIMESHEET_ONE_PAGE_CAPACITY_EXCEEDED', {
      section: 'declarations',
      required_height_mm: declarationHeight,
      available_height_mm: Math.max(0, PAGE_HEIGHT_MM - margin - footerHeight - top)
    });
  }
  top = declarationTop;
  for (const box of declarationBoxes) {
    const wordingBlock = box.wording;
    const wordingStyle = box.wordingStyle;
    const titleFont = wordingBlock?.title_bold === false ? regular : bold;
    fillBox(page, box.x, top, declarationWidth, 5, CORPORATE_NAVY);
    drawBox(page, box.x, top, declarationWidth, declarationHeight);
    drawAlignedText(page, titleFont, box.title, box.x, top + 1.5, declarationWidth,
      layout.headerFont, wordingBlock?.title_align || 'center', CORPORATE_WHITE);
    const bodyLines = box.bodyLines;
    const maximumBodyLines = Math.floor(
      ((declarationHeight - 5 - layout.signatureLineZoneHeight) + 1e-9)
        / wordingStyle.lineHeight
    );
    if (bodyLines.length > maximumBodyLines) {
      throw timesheetError('TIMESHEET_ONE_PAGE_CAPACITY_EXCEEDED', {
        section: box.title,
        required_lines: bodyLines.length,
        available_lines: maximumBodyLines
      });
    }
    bodyLines.forEach((line, index) =>
      drawText(page, regular, line, box.x + 2,
        top + 5 + index * wordingStyle.lineHeight,
        wordingStyle.fontSize, declarationWidth - 4));
    const signatureLineTop = top + declarationHeight - 3;
    const signatureTop = signatureLineTop - layout.signatureOverlayHeight + 1.1;
    drawLine(page, box.x + 2, signatureLineTop, box.x + declarationWidth - 28, signatureLineTop);
    drawText(page, regular, 'Signature', box.x + 2, top + declarationHeight - 2.5, layout.smallFont);
    drawLine(page, box.x + declarationWidth - 24, signatureLineTop, box.x + declarationWidth - 2, signatureLineTop);
    if (box.date) {
      drawText(page, regular, formatDmy(box.date), box.x + declarationWidth - 24, top + declarationHeight - 6.2, layout.smallFont, 22);
    }
    drawText(page, regular, 'Date', box.x + declarationWidth - 24, top + declarationHeight - 2.5, layout.smallFont, 22);
    const signatureAllowed = model.form_variant === 'ELECTRONIC_SIGNED'
      || (model.form_variant === 'ELECTRONIC_MANAGER_REVIEW' && box.signatureRole === 'CANDIDATE');
    if (signatureAllowed && box.signature) {
      signaturePlacements.push({
        image: box.signature,
        box: {
        x: box.x + 2,
        top: signatureTop,
        width: declarationWidth - 32,
        height: layout.signatureOverlayHeight
        }
      });
    }
  }

  if (footerLines.length) {
    drawLine(page, margin, PAGE_HEIGHT_MM - margin - footerHeight + 0.8,
      right, PAGE_HEIGHT_MM - margin - footerHeight + 0.8, 0.45, CORPORATE_NAVY);
    drawWordingBlock(page, regular, footerLines, margin, PAGE_HEIGHT_MM - margin - footerHeight + 2,
      width, footerWordingStyle.fontSize, footerWordingStyle.lineHeight);
  }

  // Signatures are the final page objects. They behave like ink placed on a
  // completed form, so no date, label, rule, declaration text or border can be
  // painted over them.
  for (const placement of signaturePlacements) {
    signatureFits.push(drawEmbeddedImage(page, placement.image, placement.box));
  }

  const bytes = new Uint8Array(await pdf.save({ useObjectStreams: false }));
  const parsed = await PDFDocument.load(bytes, { ignoreEncryption: false, updateMetadata: false });
  const pageCount = parsed.getPageCount();
  if (pageCount !== 1) {
    throw timesheetError('TIMESHEET_ONE_PAGE_CAPACITY_EXCEEDED', { page_count: pageCount });
  }
  return Object.freeze({
    pdf_bytes: bytes,
    page_count: pageCount,
    layout_mode: layout.name,
    render_receipt: Object.freeze({
      schema_version: 'TIMESHEET_OFFICIAL_RENDER_RECEIPT_V2',
      visual_style: 'ARMS_CORPORATE_NAVY_V2',
      layout_contract_version: safeText(model.layout_contract_version),
      layout_mode: layout.name,
      schedule_line_count: scheduleLineCount(model),
      additional_unit_row_count: additionalRows.length,
      break_display_row_count: scheduleShifts(model)
        .filter(shift => shiftBreakDisplay(shift).some(Boolean)).length,
      band_display_row_count: scheduleShifts(model)
        .filter(shift => safeText(shift?.band || model?.band).trim()).length,
      logo_embedded: Boolean(logo),
      all_embedded_images_aspect_ratio_preserved: [logoFit, ...signatureFits]
        .filter(Boolean).every(fit => fit.aspect_ratio_preserved === true),
      declaration_height_mm: declarationHeight,
      declaration_top_mm: declarationTop,
      schedule_row_height_mm: scheduleRowHeight,
      additional_unit_row_height_mm: additionalRows.length ? additionalRowHeight : 0,
      unused_vertical_gap_mm: unusedVerticalGap,
      page_fill_verified: unusedVerticalGap <= 0.01,
      additional_units_above_declarations: true,
      detail_layout_variant: detailsGeometry.variant,
      centre_box_rendered: Boolean(detailsGeometry.centreBox),
      details_height_mm: detailsHeight,
      centre_box_width_mm: Number(detailsGeometry.centreBox?.width || 0),
      worker_value_columns_aligned: true,
      client_value_columns_aligned: true,
      signature_overlay_independent_of_text_flow: true,
      signature_images_drawn_last: true,
      signature_line_zone_height_mm: layout.signatureLineZoneHeight,
      signature_overlay_height_mm: layout.signatureOverlayHeight,
      one_page_verified: true
    })
  });
}

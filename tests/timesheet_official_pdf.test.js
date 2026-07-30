import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { PDFDocument } from 'pdf-lib';
import {
  buildTsq1Payload,
  buildTsq1String
} from '../broker/src/timesheet-qr-payload.js';
import {
  buildOfficialWeekPeriod,
  fitImageContain,
  formatOfficialTimesheetHoursWords,
  officialTimesheetNumber,
  renderOfficialTimesheetPdfBytes,
  selectOfficialTimesheetOnePageLayout
} from '../broker/src/timesheet-official-pdf.js';
import {
  validateFrozenHealthRosterModel,
  validateFrozenTimesheetPresentationModel
} from '../broker/src/invoice-presentation-contract.js';

const TIMESHEET_ID = '00000000-0000-4000-8000-000000000101';

function fixture(overrides = {}) {
  const weekPeriod = buildOfficialWeekPeriod('2026-07-11');
  weekPeriod.days[1].shift_lines.push({
    row_key: 'segment:1',
    display_order: 1,
    segment_id: '00000000-0000-4000-8000-000000000201',
    date: '2026-07-06',
    worked_start_utc: '2026-07-06T07:00:00Z',
    worked_end_utc: '2026-07-06T15:30:00Z',
    display_start_local: '08:00',
    display_end_local: '16:30',
    break_start_local: '',
    break_end_local: '',
    break_minutes: 30,
    break_display_mode: 'MINUTES_ONLY',
    bucket_hours: { DAY: 8 },
    paid_minutes: 480,
    band: '5',
    booking_reference: '0726049648',
    reference_required: true,
    reference_source: 'SEGMENT',
    reference_row_key: 'segment:1'
  });
  return {
    schema_version: 'TIMESHEET_RENDER_MODEL_V2',
    template_version: 'timesheet-professional-v2',
    layout_contract_version: 'TIMESHEET_ONE_PAGE_LANDSCAPE_V2',
    timesheet_id: TIMESHEET_ID,
    document_revision: 2,
    timesheet_number: officialTimesheetNumber(TIMESHEET_ID),
    sheet_scope: 'WEEKLY',
    form_variant: 'ELECTRONIC_UNSIGNED',
    submission_mode: 'ELECTRONIC',
    locale: 'en-GB',
    time_zone: 'Europe/London',
    week_period: weekPeriod,
    worker: {
      first_name: 'Sarah',
      surname: 'Dumbuya',
      job_profile_title: 'Registered Nurse'
    },
    client: {
      name: 'Example NHS Foundation Trust',
      hospital: 'Example NHS Foundation Trust',
      site_ward: 'Ward 1'
    },
    band: '5',
    branding: { agency_name: 'Arthur Rai Medical Services', logo: {} },
    wording: {
      header: { lines: ['Please complete all relevant fields.'] },
      footer: { lines: ['Arthur Rai Medical Services'] },
      temporary_worker_declaration: {
        title: 'Temporary Worker Declaration',
        lines: ['I declare that the hours and units recorded are complete and accurate.']
      },
      client_declaration: {
        title: 'Client Declaration',
        lines: ['I confirm that the recorded work was completed and is authorised.']
      }
    },
    additional_units_section: {
      schema_version: 'TIMESHEET_ADDITIONAL_UNITS_V1',
      visible: false,
      title: 'Additional rates / units',
      column_labels: {
        rate_type: 'Rate Type',
        date: 'Date',
        quantity: 'Quantity',
        unit: 'Unit'
      },
      minimum_blank_space_rows: 1,
      rows: []
    },
    totals: { paid_minutes: 480 },
    authorisation: { authorised: false, authorised_at_utc: null },
    signatures: { candidate: {}, authoriser: {} },
    qr: { required: false, signed: false, status: null, payload: null, token: null },
    layout: {
      one_page_required: true,
      allowed_modes: ['NORMAL', 'COMPACT', 'ULTRA'],
      second_page_allowed: false,
      minimum_font_size: 5.5,
      minimum_row_height_mm: 3.45,
      minimum_signature_height_mm: 7,
      minimum_additional_blank_rows: 1
    },
    ...overrides
  };
}

test('official week period supports every configured ending weekday', () => {
  const endings = [
    ['2026-07-05', 'Monday', 'Sunday'],
    ['2026-07-06', 'Tuesday', 'Monday'],
    ['2026-07-07', 'Wednesday', 'Tuesday'],
    ['2026-07-08', 'Thursday', 'Wednesday'],
    ['2026-07-09', 'Friday', 'Thursday'],
    ['2026-07-10', 'Saturday', 'Friday'],
    ['2026-07-11', 'Sunday', 'Saturday']
  ];
  for (const [ending, firstName, endingName] of endings) {
    const period = buildOfficialWeekPeriod(ending);
    assert.equal(period.days.length, 7);
    assert.equal(period.days[0].weekday_name, firstName);
    assert.equal(period.end_weekday_name, endingName);
    assert.equal(period.days[6].date, ending);
  }
});

test('official hours wording uses complete grammatical sentence-case English', () => {
  assert.equal(formatOfficialTimesheetHoursWords(1), 'One minute');
  assert.equal(formatOfficialTimesheetHoursWords(30), 'Thirty minutes');
  assert.equal(formatOfficialTimesheetHoursWords(60), 'One hour');
  assert.equal(formatOfficialTimesheetHoursWords(65), 'One hour and five minutes');
  assert.equal(formatOfficialTimesheetHoursWords(121), 'Two hours and one minute');
  assert.equal(formatOfficialTimesheetHoursWords(735), 'Twelve hours and fifteen minutes');
  assert.equal(formatOfficialTimesheetHoursWords(750), 'Twelve hours and thirty minutes');
  assert.equal(formatOfficialTimesheetHoursWords(6720), 'One hundred and twelve hours');
});

test('logos and signatures are fitted without changing their aspect ratio', () => {
  const landscape = fitImageContain(400, 100, 32, 10);
  const portrait = fitImageContain(100, 400, 30, 8);
  assert.equal(landscape.width_mm / landscape.height_mm, 4);
  assert.equal(portrait.width_mm / portrait.height_mm, 0.25);
  assert.ok(landscape.width_mm <= 32 && landscape.height_mm <= 10);
  assert.ok(portrait.width_mm <= 30 && portrait.height_mm <= 8);
  assert.equal(landscape.aspect_ratio_preserved, true);
  assert.equal(portrait.aspect_ratio_preserved, true);
});

test('TSQ1 extraction preserves the established token field and signature input', async () => {
  const payload = buildTsq1Payload({ qr_token: 'test-token' });
  assert.deepEqual(payload, { v: 1, tok: 'test-token' });
  const secret = 'test-only-secret';
  const text = await buildTsq1String(payload, { QR_SIGNING_SECRET: secret });
  const [, payloadEncoded, signatureEncoded] = text.split('.');
  const expected = createHmac('sha256', secret)
    .update(`TSQ1.${payloadEncoded}`)
    .digest('base64url');
  assert.equal(signatureEncoded, expected);
});

test('V2 contract validates the Saturday-ending Sunday-to-Saturday form', () => {
  const model = fixture();
  assert.equal(validateFrozenTimesheetPresentationModel(model), model);
  assert.equal(model.week_period.days[0].date, '2026-07-05');
  assert.equal(model.week_period.days[6].date, '2026-07-11');
});

test('official renderer emits exactly one A4 landscape page with additional units', async () => {
  const model = fixture({
    additional_units_section: {
      ...fixture().additional_units_section,
      visible: true,
      rows: [
        {
          row_key: 'additional:mileage:2026-07-06',
          display_order: 1,
          code: 'MILEAGE',
          rate_type: 'Mileage',
          date: '2026-07-06',
          quantity: 12.5,
          unit: 'miles',
          frequency: 'PER_DAY'
        },
        {
          row_key: 'additional:on-call:weekly',
          display_order: 2,
          code: 'ON_CALL',
          rate_type: 'On call',
          date: null,
          quantity: 1,
          unit: 'unit',
          frequency: 'WEEKLY'
        }
      ]
    }
  });
  validateFrozenTimesheetPresentationModel(model);
  const rendered = await renderOfficialTimesheetPdfBytes(model, {
    logo: {
      data_url: 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Y9ZP8sAAAAASUVORK5CYII='
    }
  });
  assert.equal(rendered.page_count, 1);
  assert.equal(rendered.render_receipt.additional_unit_row_count, 2);
  assert.equal(rendered.render_receipt.visual_style, 'ARMS_CORPORATE_NAVY_V2');
  assert.equal(rendered.render_receipt.break_display_row_count, 1);
  assert.equal(rendered.render_receipt.band_display_row_count, 1);
  assert.equal(rendered.render_receipt.logo_embedded, true);
  assert.equal(rendered.render_receipt.all_embedded_images_aspect_ratio_preserved, true);
  assert.ok(rendered.render_receipt.declaration_height_mm <= 31);
  assert.ok(rendered.render_receipt.declaration_top_mm > 150);
  assert.ok(rendered.render_receipt.schedule_row_height_mm > 5);
  assert.ok(rendered.render_receipt.additional_unit_row_height_mm > 4.8);
  assert.ok(rendered.render_receipt.unused_vertical_gap_mm <= 0.01);
  assert.equal(rendered.render_receipt.page_fill_verified, true);
  assert.equal(rendered.render_receipt.additional_units_above_declarations, true);
  assert.equal(rendered.render_receipt.detail_layout_variant, 'ELECTRONIC_TWO_PANEL');
  assert.equal(rendered.render_receipt.centre_box_rendered, false);
  assert.equal(rendered.render_receipt.worker_value_columns_aligned, true);
  assert.equal(rendered.render_receipt.client_value_columns_aligned, true);
  assert.equal(rendered.render_receipt.signature_overlay_independent_of_text_flow, true);
  assert.equal(rendered.render_receipt.signature_images_drawn_last, true);
  const pdf = await PDFDocument.load(rendered.pdf_bytes);
  assert.equal(pdf.getPageCount(), 1);
  const page = pdf.getPage(0);
  assert.ok(page.getWidth() > page.getHeight());
});

test('declaration capacity accepts an exact nine-line readable fit', async () => {
  const declaration = {
    title: 'Declaration',
    font_size: 7.5,
    line_height_mm: 3.4,
    lines: Array.from({ length: 9 }, (_, index) => 'Declaration statement ' + (index + 1) + '.')
  };
  const model = fixture({
    wording: {
      ...fixture().wording,
      temporary_worker_declaration: declaration,
      client_declaration: declaration
    }
  });
  const rendered = await renderOfficialTimesheetPdfBytes(model);
  assert.equal(rendered.page_count, 1);
  assert.equal(rendered.layout_mode, 'NORMAL');
  assert.ok(rendered.render_receipt.declaration_height_mm >= 41);
  assert.equal(rendered.render_receipt.signature_line_zone_height_mm, 5.5);
});
test('signature dates are drawn above their date lines', () => {
  const source = readFileSync(
    new URL('../broker/src/timesheet-official-pdf.js', import.meta.url), 'utf8'
  );
  assert.match(source, /drawLine\(page, box\.x \+ declarationWidth - 24, signatureLineTop,/);
  assert.match(source, /`Date \$\{formatDmy\(box\.date\)\}`[^\n]+top \+ declarationHeight - 6\.2/);
});

test('signature images are the final visual layer inside each declaration box', () => {
  const source = readFileSync(
    new URL('../broker/src/timesheet-official-pdf.js', import.meta.url), 'utf8'
  );
  assert.match(
    source,
    /drawText\(page, regular, `Date \$\{formatDmy\(box\.date\)\}`[\s\S]*if \(model\.form_variant === 'ELECTRONIC_SIGNED' && box\.signature\) \{[\s\S]*signatureFits\.push\(drawEmbeddedImage/
  );
  assert.doesNotMatch(
    source,
    /signatureFits\.push\(drawEmbeddedImage[\s\S]*\}\)\);[\s\S]*draw(?:Line|Text)\(page,[^\n]+signatureLineTop/
  );
});

test('QR forms retain the centre QR panel while electronic forms expand both detail boxes', async () => {
  const electronic = await renderOfficialTimesheetPdfBytes(fixture());
  assert.equal(electronic.render_receipt.detail_layout_variant, 'ELECTRONIC_TWO_PANEL');
  assert.equal(electronic.render_receipt.centre_box_rendered, false);

  const qr = await renderOfficialTimesheetPdfBytes(fixture({
    form_variant: 'QR_UNSIGNED',
    submission_mode: 'MANUAL'
  }), {
    qr_text: 'TSQ1.eyJ2IjoxLCJ0b2siOiJ0ZXN0In0.signature'
  });
  assert.equal(qr.render_receipt.detail_layout_variant, 'QR_THREE_PANEL');
  assert.equal(qr.render_receipt.centre_box_rendered, true);
});

test('dense supported form selects a readable compact mode without omitting rows', async () => {
  const model = fixture();
  for (const day of model.week_period.days) {
    day.shift_lines.length = 0;
    for (let index = 0; index < 2; index += 1) {
      day.shift_lines.push({
        row_key: `segment:${day.date}:${index}`,
        display_order: index + 1,
        segment_id: `segment:${day.date}:${index}`,
        date: day.date,
        display_start_local: index ? '18:00' : '08:00',
        display_end_local: index ? '22:00' : '16:00',
        break_minutes: 0,
        break_display_mode: 'NONE',
        paid_minutes: index ? 240 : 480,
        band: '5',
        booking_reference: `REF-${day.display_order}-${index}`,
        reference_required: true,
        reference_source: 'SEGMENT',
        reference_row_key: `segment:${day.date}:${index}`
      });
    }
  }
  model.totals.paid_minutes = 7 * 720;
  model.additional_units_section.visible = true;
  model.additional_units_section.rows = Array.from({ length: 4 }, (_, index) => ({
    row_key: `additional:${index}`,
    display_order: index + 1,
    code: `UNIT_${index}`,
    rate_type: `Additional unit ${index + 1}`,
    date: index % 2 ? null : model.week_period.days[index].date,
    quantity: index + 0.5,
    unit: 'unit',
    frequency: index % 2 ? 'WEEKLY' : 'PER_DAY'
  }));
  validateFrozenTimesheetPresentationModel(model);
  const layout = await selectOfficialTimesheetOnePageLayout(model);
  assert.ok(['COMPACT', 'ULTRA'].includes(layout.name));
  const rendered = await renderOfficialTimesheetPdfBytes(model);
  assert.equal(rendered.page_count, 1);
  assert.equal(rendered.render_receipt.schedule_line_count, 14);
  assert.equal(rendered.render_receipt.additional_unit_row_count, 4);
  assert.ok(rendered.render_receipt.unused_vertical_gap_mm <= 0.01);
  assert.equal(rendered.render_receipt.page_fill_verified, true);
});

test('impossible one-page fixture fails instead of truncating or spilling', async () => {
  const model = fixture();
  model.additional_units_section.visible = true;
  model.additional_units_section.rows = Array.from({ length: 80 }, (_, index) => ({
    row_key: `additional:${index}`,
    display_order: index + 1,
    code: `UNIT_${index}`,
    rate_type: `Additional unit ${index + 1}`,
    date: null,
    quantity: index + 1,
    unit: 'unit',
    frequency: 'WEEKLY'
  }));
  await assert.rejects(
    selectOfficialTimesheetOnePageLayout(model),
    error => error?.code === 'TIMESHEET_ONE_PAGE_CAPACITY_EXCEEDED'
  );
});

test('layout selection preflights horizontal cells and retries a smaller readable mode', async () => {
  const model = fixture();
  model.week_period.days[1].shift_lines[0].booking_reference = 'R'.repeat(64);
  const layout = await selectOfficialTimesheetOnePageLayout(model);
  assert.ok(['COMPACT', 'ULTRA'].includes(layout.name));
  const rendered = await renderOfficialTimesheetPdfBytes(model);
  assert.equal(rendered.page_count, 1);
  assert.equal(rendered.layout_mode, layout.name);
});

test('wrapped wording preserves an unbroken long token without overflow or truncation', async () => {
  const model = fixture({
    wording: {
      ...fixture().wording,
      header: {
        lines: [`POLICY-${'ABCDEFGHIJ'.repeat(35)}`]
      }
    }
  });
  const rendered = await renderOfficialTimesheetPdfBytes(model);
  assert.equal(rendered.page_count, 1);
  assert.equal(rendered.render_receipt.page_fill_verified, true);
});

test('HealthRoster V2 rejects ambiguous references and accepts one exact match', () => {
  const model = {
    schema_version: 'HEALTHROSTER_PRESENTATION_V2',
    rows: [{
      worker: 'Worker',
      assignment: 'Assignment',
      shift_date: '2026-07-06',
      shift_times: '08:00–16:00',
      site: 'Hospital',
      ward: 'Ward 1',
      booking_reference: '0726049648',
      units_hours: '8.00',
      validation_state: 'MATCHED',
      source_identity: 'external-row-1',
      reference_match_state: 'EXACT',
      reference_match_count: 1
    }]
  };
  assert.equal(validateFrozenHealthRosterModel(model), model);
  assert.throws(
    () => validateFrozenHealthRosterModel({
      ...model,
      rows: [{ ...model.rows[0], reference_match_state: 'AMBIGUOUS', reference_match_count: 2 }]
    }),
    error => error?.code === 'HEALTHROSTER_REFERENCE_MATCH_AMBIGUOUS'
  );
});

import { mkdir, writeFile } from 'node:fs/promises';
import { candidateAppBackendInternals } from '../../broker/src/candidate-app-backend.js';
import { buildTsq2PagePayload, buildTsq2String } from '../../broker/src/timesheet-qr-payload.js';

const outputDirectory = new URL('./pdf/', import.meta.url);
await mkdir(outputDirectory, { recursive: true });

const workflow = {
  id: '11111111-2222-4333-8444-555555555555',
  generation: 3,
  week_ending_date: '2026-08-30',
  immutable_submission_json: JSON.stringify({
    expense_submission: {
      total_mileage: 38,
      mileage_journeys: [
        { post_code_from: 'CO4 5AA', post_code_to: 'IP4 5PD', number_of_miles: 18 },
        { post_code_from: 'IP4 5PD', post_code_to: 'CO4 5AA', number_of_miles: 20 }
      ]
    }
  })
};

const branding = { agency_name: 'Arthur Rai Medical Services Limited', logo: null };
const presentation = {
  worker: { first_name: 'Kier', surname: 'Arthur' },
  client: { name: 'Arthur Rai Medical Services' }
};

const manifestSha256 = '7'.repeat(64);
const pageQrText = await buildTsq2String(await buildTsq2PagePayload({
  workflow_id: workflow.id,
  timesheet_id: '11111111-2222-4333-8444-555555555556',
  workflow_generation: workflow.generation,
  paper_return_manifest_sha256: manifestSha256,
  ordinal: 3,
  page_key: 'MILEAGE_FORM::1',
  page_kind: 'M',
  category_code: 'M',
  category_occurrence: 1
}), { QR_SIGNING_SECRET: 'visual-proof-only-qr-signing-secret' });

const qrBytes = await candidateAppBackendInternals.mileageClaimFormBytes(
  {}, workflow, branding, presentation, pageQrText, 'Mileage form'
);
const standaloneBytes = await candidateAppBackendInternals.mileageClaimFormBytes(
  {}, workflow, branding, presentation, null, 'Mileage form'
);

await writeFile(new URL('mileage-qr-pack.pdf', outputDirectory), qrBytes);
await writeFile(new URL('mileage-standalone.pdf', outputDirectory), standaloneBytes);

console.log(JSON.stringify({
  qr_pdf: new URL('mileage-qr-pack.pdf', outputDirectory).pathname,
  standalone_pdf: new URL('mileage-standalone.pdf', outputDirectory).pathname,
  qr_bytes: qrBytes.byteLength,
  standalone_bytes: standaloneBytes.byteLength
}));

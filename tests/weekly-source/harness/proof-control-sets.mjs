// Plan 6.2 carries one release-blocking control set that the pack publishes as prose
// instead of an annex CSV: R1-R44, the rollback-contained PostgreSQL 17.11 proofs of
// `proof/32_PENDING_PUBLICATION_OWNER_SPECIFICATION_20260917.md` section 12.
//
// The harness therefore derives those ids from the pinned proof file rather than carrying
// an unpinned transcription of them. The file itself is pinned by SHA-256 in
// `controlling-ledger-manifest.json`, so a pack edit is detected before the ids are used.

const ROW_PATTERN = /^\|\s*(R\d{1,3})\s*\|\s*(.*?)\s*\|\s*(.*?)\s*\|\s*$/;

function fail(code, message, details = {}) {
  const error = new Error(message);
  error.name = 'WeeklySourceProofControlSetError';
  error.code = code;
  error.details = details;
  throw error;
}

/**
 * Extract the `## <section>.` table of a pack proof document.
 * Returns the raw lines between that heading and the next `## ` heading.
 */
export function proofSectionLines(text, section) {
  const lines = String(text ?? '').split('\n');
  const start = lines.findIndex((line) => line.startsWith(`## ${section}.`));
  if (start < 0) fail('PROOF_CONTROL_SECTION_MISSING', `The pinned proof document has no section ${section}.`);
  const rest = lines.slice(start + 1);
  const end = rest.findIndex((line) => line.startsWith('## '));
  return end < 0 ? rest : rest.slice(0, end);
}

/**
 * Parse the R-series required-test table out of proof/32 section 12.
 *
 * Every row keeps its exact scenario and required-result text so a failure message can
 * quote the controlling authority instead of paraphrasing a money rule. `implementationGate`
 * records the pack's own classification of R40 ("Implementation gate (not a database test)").
 * It is informational only: R40 still requires executed evidence like every other id.
 */
export function parseProofRSeries(text, { section = '12', requiredCount = 44 } = {}) {
  const rows = [];
  for (const line of proofSectionLines(text, section)) {
    const match = ROW_PATTERN.exec(line);
    if (!match) continue;
    const [, id, scenario, requiredResult] = match;
    if (!scenario || !requiredResult) {
      fail('PROOF_CONTROL_ROW_INCOMPLETE', `Proof control row ${id} has no scenario or required result.`);
    }
    rows.push({
      proof_id: id,
      authority: `proof/32 section ${section}`,
      scenario,
      required_result: requiredResult,
      implementation_gate: /^Implementation gate \(not a database test\)/.test(scenario),
    });
  }
  if (rows.length !== requiredCount) {
    fail('PROOF_CONTROL_COUNT_INVALID', `Proof section ${section} yielded ${rows.length} rows; expected ${requiredCount}.`, {
      actual: rows.length,
      expected: requiredCount,
    });
  }
  for (let index = 0; index < rows.length; index += 1) {
    const expected = `R${index + 1}`;
    if (rows[index].proof_id !== expected) {
      fail('PROOF_CONTROL_ID_SEQUENCE_INVALID', `Proof control row ${index + 1} is ${rows[index].proof_id}; expected ${expected}.`);
    }
  }
  return rows;
}

export { ROW_PATTERN as PROOF_CONTROL_ROW_PATTERN };

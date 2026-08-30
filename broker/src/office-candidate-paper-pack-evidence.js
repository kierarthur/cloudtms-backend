export function resolveOfficeCandidatePaperPackEvidenceKey(env, rawRows, timesheetId) {
  const environment = String(env?.CANDIDATE_APP_ENVIRONMENT || '')
    .trim()
    .toLowerCase();
  const expectedTimesheetId = String(timesheetId || '').trim().toLowerCase();
  if (!/^[a-z0-9][a-z0-9_-]{0,31}$/.test(environment)
      || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(expectedTimesheetId)) {
    return null;
  }

  const parseObject = (value) => {
    if (value && typeof value === 'object' && !Array.isArray(value)) return value;
    if (typeof value !== 'string' || !value.trim()) return {};
    try {
      const parsed = JSON.parse(value);
      return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {};
    } catch {
      return {};
    }
  };
  const parseArray = (value) => {
    if (Array.isArray(value)) return value;
    if (typeof value !== 'string' || !value.trim()) return [];
    try {
      const parsed = JSON.parse(value);
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  };
  const cleanKey = (value) => String(value || '').trim().replace(/^\/+/, '');
  const sha256 = /^[0-9a-f]{64}$/;
  const expectedPrefix = `candidate-app/${environment}/`;

  const exact = (Array.isArray(rawRows) ? rawRows : []).filter((row) => {
    if (String(row?.context_id || '').trim().toLowerCase() !== expectedTimesheetId) return false;
    const scope = parseObject(row?.payment_scope_json);
    if (String(scope.candidate_mail_authority || '').trim().toUpperCase() !== 'CANDIDATE_PAPER_V1'
        || scope.candidate_paper_pack_ready !== true
        || scope.mail_held_until_pdf_rendered !== false
        || String(scope.mail_hold_reason || '').trim()
        || scope.candidate_paper_generation_retired === true) return false;

    const key = cleanKey(scope.candidate_complete_pack_storage_key);
    const digest = String(scope.candidate_complete_pack_sha256 || '').trim().toLowerCase();
    const sizeBytes = Number(scope.candidate_complete_pack_size_bytes);
    const pageCount = Number(scope.candidate_complete_pack_page_count);
    if (!key.startsWith(expectedPrefix) || !key.endsWith('.pdf')
        || !sha256.test(digest)
        || !Number.isSafeInteger(sizeBytes) || sizeBytes < 1
        || !Number.isSafeInteger(pageCount) || pageCount < 1) return false;

    const attachments = parseArray(row?.attachments);
    if (attachments.length !== 1) return false;
    const attachment = attachments[0] || {};
    return cleanKey(attachment.r2_key) === key
      && String(attachment.sha256 || '').trim().toLowerCase() === digest
      && Number(attachment.size_bytes) === sizeBytes
      && Number(attachment.page_count) === pageCount
      && String(attachment.content_type || '').trim().toLowerCase() === 'application/pdf';
  });

  if (exact.length !== 1) return null;
  return cleanKey(parseObject(exact[0].payment_scope_json).candidate_complete_pack_storage_key);
}

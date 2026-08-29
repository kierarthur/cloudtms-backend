const norm = (value) => String(value ?? '').trim().toLowerCase().replace(/\s+/g, ' ');

const includesAny = (value, aliases) => aliases.some((alias) => value === alias || value.includes(alias));

function normalizedRow(row) {
  return (Array.isArray(row) ? row : []).map(norm);
}

function findNhspHeader(rows) {
  const source = Array.isArray(rows) ? rows : [];
  const scanLimit = Math.min(source.length, 30);

  for (let groupIndex = 0; groupIndex < scanLimit; groupIndex += 1) {
    const group = normalizedRow(source[groupIndex]);
    if (!group.some((cell) => cell === 'contract') || !group.some((cell) => cell === 'actual')) continue;

    for (let subIndex = groupIndex + 1; subIndex <= Math.min(groupIndex + 3, scanLimit - 1); subIndex += 1) {
      const sub = normalizedRow(source[subIndex]);
      const startCount = sub.filter((cell) => cell === 'start').length;
      const endCount = sub.filter((cell) => cell === 'end' || cell === 'finish').length;
      const breakCount = sub.filter((cell) => cell.includes('break')).length;
      const totalCount = sub.filter((cell) => cell === 'total' || cell.includes('hours worked')).length;
      if (startCount < 2 || endCount < 2 || breakCount < 2 || totalCount < 2) continue;

      const identityRows = source.slice(groupIndex, subIndex + 1).flatMap(normalizedRow);
      const hasDate = identityRows.some((cell) => cell === 'date' || cell.includes('shift date'));
      const hasReference = identityRows.some((cell) => cell === 'ref' || cell.includes('reference'));
      const hasStaff = identityRows.some((cell) => includesAny(cell, ['staff', 'worker', 'name']));
      const hasTrust = identityRows.some((cell) => includesAny(cell, ['trust', 'hospital', 'client']));
      const hasWard = identityRows.some((cell) => includesAny(cell, ['ward', 'unit', 'department', 'dept']));
      const hasAssignment = identityRows.some((cell) => cell.includes('assign'));
      if (hasDate && hasReference && hasStaff && hasTrust && hasWard && hasAssignment) {
        return { groupIndex, subIndex };
      }
    }
  }

  return null;
}

function findHealthRosterHeader(rows) {
  const source = Array.isArray(rows) ? rows : [];
  const scanLimit = Math.min(source.length, 30);

  for (let index = 0; index < scanLimit; index += 1) {
    const row = normalizedRow(source[index]);
    const hasDate = row.some((cell) => cell === 'date' || cell.includes('shift date'));
    const hasStaff = row.some((cell) => includesAny(cell, ['staff', 'worker', 'name']));
    const hasStart = row.some((cell) => cell === 'start' || cell.includes('start time'));
    const hasEnd = row.some((cell) => cell === 'end' || cell === 'finish' || cell.includes('end time'));
    const hasDailyIdentity = row.some((cell) => includesAny(cell, ['request', 'unit', 'ward', 'grade', 'assignment']));
    if (hasDate && hasStaff && hasStart && hasEnd && hasDailyIdentity) return { headerIndex: index };
  }

  return null;
}

export function detectDailyRosterFormatFromRows(rows) {
  const nhspHeader = findNhspHeader(rows);
  const healthRosterHeader = findHealthRosterHeader(rows);
  const matches = [];
  if (nhspHeader) matches.push('NHSP');
  if (healthRosterHeader) matches.push('HEALTHROSTER');

  return {
    format: matches.length === 1 ? matches[0] : null,
    matches,
    nhspHeader,
    healthRosterHeader,
    errorCode: matches.length === 1
      ? null
      : (matches.length > 1 ? 'DAILY_ROSTER_FORMAT_AMBIGUOUS' : 'DAILY_ROSTER_FORMAT_UNSUPPORTED')
  };
}

function nthMatchingIndex(row, matcher, nth) {
  let seen = 0;
  for (let index = 0; index < row.length; index += 1) {
    if (!matcher(norm(row[index]))) continue;
    seen += 1;
    if (seen === nth) return index;
  }
  return -1;
}

export function resolveNhspActualColumnIndexes(rows) {
  const detection = detectDailyRosterFormatFromRows(rows);
  if (detection.format !== 'NHSP' || !detection.nhspHeader) {
    throw new Error(detection.errorCode || 'DAILY_ROSTER_FORMAT_UNSUPPORTED');
  }

  const { groupIndex, subIndex } = detection.nhspHeader;
  const groupRow = Array.isArray(rows[groupIndex]) ? rows[groupIndex] : [];
  const subRow = Array.isArray(rows[subIndex]) ? rows[subIndex] : [];
  const width = Math.max(groupRow.length, subRow.length);
  const groupAt = [];
  let activeGroup = '';

  for (let index = 0; index < width; index += 1) {
    const value = norm(groupRow[index]);
    if (value === 'contract' || value === 'actual') activeGroup = value;
    groupAt[index] = activeGroup;
  }

  const actualIndex = (matcher) => {
    for (let index = 0; index < width; index += 1) {
      if (groupAt[index] === 'actual' && matcher(norm(subRow[index]))) return index;
    }
    return -1;
  };

  const indexes = {
    start: actualIndex((cell) => cell === 'start'),
    end: actualIndex((cell) => cell === 'end' || cell === 'finish'),
    break: actualIndex((cell) => cell.includes('break')),
    total: actualIndex((cell) => cell === 'total' || cell.includes('hours worked'))
  };

  // Some HTML exports do not preserve merged-cell group spans. In that case,
  // the second matching subheader remains the canonical Actual column.
  if (indexes.start < 0) indexes.start = nthMatchingIndex(subRow, (cell) => cell === 'start', 2);
  if (indexes.end < 0) indexes.end = nthMatchingIndex(subRow, (cell) => cell === 'end' || cell === 'finish', 2);
  if (indexes.break < 0) indexes.break = nthMatchingIndex(subRow, (cell) => cell.includes('break'), 2);
  if (indexes.total < 0) indexes.total = nthMatchingIndex(subRow, (cell) => cell === 'total' || cell.includes('hours worked'), 2);

  if (Object.values(indexes).some((index) => index < 0)) {
    throw new Error('NHSP_DAILY_ACTUAL_COLUMNS_MISSING');
  }

  return { ...indexes, groupIndex, subIndex };
}

export function normalizeNhspDailyBreakMinutes(value) {
  if (value == null || String(value).trim() === '') return 0;
  const minutes = Number(value);
  if (!Number.isFinite(minutes) || minutes < 0) throw new Error('NHSP_DAILY_BREAK_INVALID');
  return minutes;
}

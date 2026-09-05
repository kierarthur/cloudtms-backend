const ROUTE_LABELS = Object.freeze({
  WEEKLY_NHSP: 'Weekly NHSP',
  WEEKLY_NHSP_ADJUSTMENT: 'Weekly NHSP Adjustment',
  WEEKLY_HEALTHROSTER: 'Weekly HealthRoster',
  WEEKLY_ELECTRONIC: 'Weekly Electronic',
  DAILY_ELECTRONIC: 'Daily Electronic',
  WEEKLY_MANUAL: 'Weekly Manual',
  DAILY_MANUAL: 'Daily Manual',
  UNKNOWN: 'Unknown'
});

function friendlyRouteType(value) {
  const normalized = String(value == null ? '' : value)
    .trim()
    .replace(/-/g, '_')
    .toUpperCase();
  if (!normalized) return '';
  if (ROUTE_LABELS[normalized]) return ROUTE_LABELS[normalized];
  return normalized
    .replace(/_/g, ' ')
    .toLowerCase()
    .replace(/\b\w/g, (character) => character.toUpperCase())
    .replace(/\bNhsp\b/g, 'NHSP')
    .replace(/\bHr\b/g, 'HR')
    .replace(/\bNhs\b/g, 'NHS')
    .replace(/\bVat\b/g, 'VAT');
}

export function displayedTimesheetRouteLabel(row) {
  for (const value of [row?.display_route_label, row?.route_display]) {
    const label = String(value == null ? '' : value).trim();
    if (label) return label;
  }
  return friendlyRouteType(row?.route_type);
}

export function createTimesheetRouteComparator(direction = 'asc') {
  const collator = new Intl.Collator('en-GB', { sensitivity: 'base', numeric: true });
  const descending = String(direction || '').trim().toLowerCase() === 'desc';
  return (left, right) => {
    const leftLabel = displayedTimesheetRouteLabel(left);
    const rightLabel = displayedTimesheetRouteLabel(right);
    if (!leftLabel && rightLabel) return 1;
    if (leftLabel && !rightLabel) return -1;
    const labelCompared = collator.compare(leftLabel, rightLabel);
    if (labelCompared) return descending ? -labelCompared : labelCompared;

    const candidateCompared = collator.compare(
      String(left?.candidate_name || ''),
      String(right?.candidate_name || '')
    );
    if (candidateCompared) return candidateCompared;

    const weekCompared = collator.compare(
      String(right?.week_ending_date || ''),
      String(left?.week_ending_date || '')
    );
    if (weekCompared) return weekCompared;

    const idCompared = collator.compare(
      String(left?.timesheet_id || left?.contract_week_id || left?.id || ''),
      String(right?.timesheet_id || right?.contract_week_id || right?.id || '')
    );
    if (idCompared) return idCompared;

    return Number(left?.__timesheet_display_sort_ordinal || 0)
      - Number(right?.__timesheet_display_sort_ordinal || 0);
  };
}

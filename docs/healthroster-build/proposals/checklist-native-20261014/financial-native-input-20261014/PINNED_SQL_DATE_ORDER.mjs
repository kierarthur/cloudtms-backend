import path from 'node:path';
export function sqlDateKey(filename) {
  const base = path.basename(filename);
  let m = base.match(/^(20\d{2})(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])(.*)$/);
  if (m && validDate(+m[1], +m[2], +m[3])) return `${m[1]}${m[2]}${m[3]}${m[4]}`;
  m = base.match(/^(0[1-9]|[12]\d|3[01])(0[1-9]|1[0-2])(20\d{2})(.*)$/);
  if (m && validDate(+m[3], +m[2], +m[1])) return `${m[3]}${m[2]}${m[1]}${m[4]}`;
  return `ZZZZZZZZ_${base}`;
}

function validDate(year, month, day) {
  const d = new Date(Date.UTC(year, month - 1, day));
  return d.getUTCFullYear() === year && d.getUTCMonth() === month - 1 && d.getUTCDate() === day;
}


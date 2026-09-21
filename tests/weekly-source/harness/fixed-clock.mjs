function parseUtc(value, label) {
  const match = typeof value === 'string'
    ? /^(\d{4})-(\d{2})-(\d{2})T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,3})?Z$/.exec(value)
    : null;
  if (!match) {
    throw new TypeError(`${label} must be an ISO 8601 UTC timestamp ending in Z`);
  }
  const epochMs = Date.parse(value);
  const date = new Date(epochMs);
  if (!Number.isFinite(epochMs)
    || date.getUTCFullYear() !== Number(match[1])
    || date.getUTCMonth() !== Number(match[2]) - 1
    || date.getUTCDate() !== Number(match[3])) {
    throw new TypeError(`${label} is not a real UTC instant`);
  }
  return epochMs;
}

function canonicalUtc(epochMs) {
  return new Date(epochMs).toISOString();
}

export class FixedClock {
  #epochMs;
  #history;

  constructor(initialUtc) {
    this.#epochMs = parseUtc(initialUtc, 'initialUtc');
    this.#history = [canonicalUtc(this.#epochMs)];
  }

  nowUtc() {
    return canonicalUtc(this.#epochMs);
  }

  nowEpochMs() {
    return this.#epochMs;
  }

  now() {
    return new Date(this.#epochMs);
  }

  advanceTo(targetUtc) {
    const target = parseUtc(targetUtc, 'targetUtc');
    if (target < this.#epochMs) throw new RangeError('Fixed clock cannot move backwards');
    this.#epochMs = target;
    this.#history.push(canonicalUtc(target));
    return this.nowUtc();
  }

  advanceBy({ milliseconds = 0, seconds = 0, minutes = 0, hours = 0, days = 0 } = {}) {
    const values = [milliseconds, seconds, minutes, hours, days];
    if (values.some((value) => !Number.isSafeInteger(value) || value < 0)) {
      throw new TypeError('Every fixed-clock advance component must be a non-negative safe integer');
    }
    const delta = milliseconds + (seconds * 1000) + (minutes * 60000) + (hours * 3600000) + (days * 86400000);
    if (!Number.isSafeInteger(delta)) throw new RangeError('Fixed-clock advance is too large');
    return this.advanceTo(canonicalUtc(this.#epochMs + delta));
  }

  history() {
    return Object.freeze([...this.#history]);
  }
}

export function createFixedClock(initialUtc) {
  return new FixedClock(initialUtc);
}

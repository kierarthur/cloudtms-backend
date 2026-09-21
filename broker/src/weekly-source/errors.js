export class WeeklySourceParserError extends Error {
  constructor(code, message, details = {}) {
    super(message);
    this.name = 'WeeklySourceParserError';
    this.code = code;
    this.details = details;
  }
}

export function failParser(code, message, details = {}) {
  throw new WeeklySourceParserError(code, message, details);
}


import jpeg from 'jpeg-js';
import jsQR from 'jsqr';

const MAX_QR_SCAN_PIXELS = 20_000_000;
const MAX_QR_RESULTS = 8;

function paperImageError(code) {
  return Object.assign(new Error(code), { code });
}

function clamp(value, minimum, maximum) {
  return Math.max(minimum, Math.min(maximum, value));
}

function maskLocatedCode(rgba, width, height, location) {
  const points = [
    location?.topLeftCorner,
    location?.topRightCorner,
    location?.bottomLeftCorner,
    location?.bottomRightCorner
  ].filter(Boolean);
  if (!points.length) return false;
  const padding = 8;
  const left = clamp(Math.floor(Math.min(...points.map((point) => point.x))) - padding, 0, width - 1);
  const right = clamp(Math.ceil(Math.max(...points.map((point) => point.x))) + padding, 0, width - 1);
  const top = clamp(Math.floor(Math.min(...points.map((point) => point.y))) - padding, 0, height - 1);
  const bottom = clamp(Math.ceil(Math.max(...points.map((point) => point.y))) + padding, 0, height - 1);
  if (right <= left || bottom <= top) return false;
  for (let y = top; y <= bottom; y += 1) {
    for (let x = left; x <= right; x += 1) {
      const offset = (y * width + x) * 4;
      rgba[offset] = 255;
      rgba[offset + 1] = 255;
      rgba[offset + 2] = 255;
      rgba[offset + 3] = 255;
    }
  }
  return true;
}

export function decodeCandidatePaperQrTextsFromJpeg(bytes) {
  let decoded;
  try {
    decoded = jpeg.decode(bytes, {
      useTArray: true,
      formatAsRGBA: true,
      tolerantDecoding: false,
      maxResolutionInMP: 20,
      maxMemoryUsageInMB: 128
    });
  } catch {
    throw paperImageError('CANDIDATE_PAPER_IMAGE_UNREADABLE');
  }
  const width = Number(decoded?.width);
  const height = Number(decoded?.height);
  if (!Number.isSafeInteger(width) || !Number.isSafeInteger(height)
      || width < 1 || height < 1 || width * height > MAX_QR_SCAN_PIXELS
      || !decoded?.data || decoded.data.length !== width * height * 4) {
    throw paperImageError('CANDIDATE_PAPER_IMAGE_UNREADABLE');
  }
  const working = new Uint8ClampedArray(decoded.data);
  const found = [];
  const seen = new Set();
  for (let attempt = 0; attempt < MAX_QR_RESULTS; attempt += 1) {
    const result = jsQR(working, width, height, {
      inversionAttempts: 'attemptBoth'
    });
    if (!result?.data) break;
    const value = String(result.data).trim();
    if (value && !seen.has(value)) {
      seen.add(value);
      found.push(value);
    }
    if (!maskLocatedCode(working, width, height, result.location)) break;
  }
  return Object.freeze({
    width,
    height,
    qr_texts: Object.freeze(found)
  });
}

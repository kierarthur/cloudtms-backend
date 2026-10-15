import { registerHooks, createRequire } from 'node:module';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
if (!process.env.CLOUDTMS_TEST_DEPENDENCY_ROOT) throw new Error('Set CLOUDTMS_TEST_DEPENDENCY_ROOT to the approved existing backend dependency root for local tests');
const dependencies = createRequire(path.join(path.resolve(process.env.CLOUDTMS_TEST_DEPENDENCY_ROOT), 'package.json'));
const resolved = new Map(['pdf-lib', 'qrcode'].map(name => [name, pathToFileURL(dependencies.resolve(name)).href]));
registerHooks({ resolve(specifier, context, nextResolve) {
  return nextResolve(resolved.get(specifier) || specifier, context);
} });

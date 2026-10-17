import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { expect } from 'file:///D:/Development/GitHub/TEST-Frontend/node_modules/@playwright/test/index.mjs';
import fs from 'node:fs';
import path from 'node:path';
import {ownedDirectory,check} from './policy.mjs';
import {credentials} from './credentials-child.mjs';
import {admission} from './admission.mjs';

const authFile=path.join(ownedDirectory(process.env.HEALTH_TEST_AUTH_OWNED_OUT),'storage-state.json');

export async function authenticate({page,network}){
  let {email,password}=credentials();

  if (!email || !password) {
    throw new Error('AUTH_APPROVED_ENV_UNAVAILABLE');
  }

  await page.goto('/');

  const preferredEmailInput = page.getByTestId('login-email');
  const preferredPasswordInput = page.getByTestId('login-password');
  const preferredSubmitButton = page.getByTestId('login-submit');

  const emailInput = (await preferredEmailInput.count()) ? preferredEmailInput : page.locator('#loginEmail');
  const passwordInput = (await preferredPasswordInput.count()) ? preferredPasswordInput : page.locator('#loginPassword');
  const submitButton = (await preferredSubmitButton.count()) ? preferredSubmitButton : page.locator('#loginForm button[type="submit"]');

  if (await emailInput.isVisible().catch(() => false)) {
    await emailInput.fill(email);
    await passwordInput.fill(password);
    await submitButton.click();
    await page.waitForFunction(() => {
      const login = document.getElementById('loginOverlay');
      const tfa = document.getElementById('tfaOverlay');
      return (login && getComputedStyle(login).display === 'none') ||
        (tfa && getComputedStyle(tfa).display !== 'none');
    }, null, { timeout: 30_000 });
  }

  const tfaOverlay = page.locator('#tfaOverlay');
  if (await tfaOverlay.isVisible().catch(() => false)) {
    throw new Error('AUTH_2FA_REQUIRED');
  }

  await expect(page.locator('#loginOverlay')).toBeHidden({ timeout: 30_000 });
  email=null;password=null;
  const admitted=await admission(page);
  network.recordAdmission(admitted);
  check(admitted.meStatus===200&&admitted.actorUuid&&admitted.admin,'AUTH_ME_ADMIN_ADMISSION_HELD');
  check(admitted.workspaceStatus===200&&admitted.contract&&admitted.queries,'AUTH_QUERY_WORKSPACE_ADMISSION_HELD');
  network.assertAdmission();
  check(!fs.existsSync(authFile),'AUTH_FRESH_STATE_REQUIRED');
  mkdirSync(dirname(authFile), { recursive: true });
  await page.context().storageState({ path: authFile });
}

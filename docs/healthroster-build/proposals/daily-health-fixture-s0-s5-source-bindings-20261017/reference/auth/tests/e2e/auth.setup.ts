import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { test as setup, expect } from '@playwright/test';

const authFile = 'tests/e2e/.auth/user.json';

setup('authenticate', async ({ page }) => {
  const email = process.env.E2E_USER_EMAIL;
  const password = process.env.E2E_USER_PASSWORD;

  if (!email || !password) {
    throw new Error('E2E_USER_EMAIL and E2E_USER_PASSWORD must be set to run e2e auth setup.');
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
    throw new Error('Login requires 2FA; this e2e harness intentionally does not bypass production 2FA.');
  }

  await expect(page.locator('#loginOverlay')).toBeHidden({ timeout: 30_000 });
  mkdirSync(dirname(authFile), { recursive: true });
  await page.context().storageState({ path: authFile });
});

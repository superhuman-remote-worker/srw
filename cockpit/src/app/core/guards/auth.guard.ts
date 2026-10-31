import {effect, EffectRef, inject, Injector, runInInjectionContext} from '@angular/core';
import {CanActivateFn} from '@angular/router';
import {SessionService} from '../services/session.service';
import {UserService} from '../services/user.service';

/**
 * Route guard.
 *
 * The cookie BFF means we have no synchronous "authenticated?" signal — we
 * only know once `/auth/me` returned. The APP_INITIALIZER awaits the first
 * `/auth/me` before bootstrapping; by the time a guard runs, `currentUser`
 * is set on success or the BFF login redirect has already been kicked off.
 *
 * If for whatever reason `currentUser` is still null when the guard runs
 * (race between component activation and a delayed initializer resolution),
 * we wait one effect tick for the next non-null emission, then fall back to
 * sending the user to login.
 */
export const authGuard: CanActivateFn = () => {
  const session = inject(SessionService);
  const userService = inject(UserService);
  const injector = inject(Injector);

  if (userService.currentUser()) return true;
  // The API did not answer the bootstrap read: the shell shows a Retry /
  // Log out card. Sending the browser to /auth/login on an unreachable API
  // would only trade that card for the browser's own error page.
  if (userService.loadState() === 'failed') return false;

  return new Promise<boolean>((done) => {
    runInInjectionContext(injector, () => {
      let ref: EffectRef | null = null;
      let resolved = false;
      ref = effect(() => {
        if (userService.currentUser()) {
          if (resolved) return;
          resolved = true;
          ref?.destroy();
          done(true);
        }
      });
      // If no currentUser materialises within a short window, redirect to
      // the BFF login. This is a defensive path — the APP_INITIALIZER
      // should already have handled the 401 case.
      setTimeout(() => {
        if (resolved) return;
        resolved = true;
        ref?.destroy();
        if (userService.loadState() !== 'failed') session.login();
        done(false);
      }, 1500);
    });
  });
};

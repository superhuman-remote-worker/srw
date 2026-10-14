import {effect, EffectRef, inject, Injector, runInInjectionContext} from '@angular/core';
import {CanActivateFn, Router, UrlTree} from '@angular/router';
import {SessionService} from '../services/session.service';
import {UserService} from '../services/user.service';

/**
 * Route guard for admin-only pages (Settings → Models, Settings → Cloud storage).
 *
 * Runs after `authGuard` has ensured a session exists. If the user is not
 * yet loaded (F5 reload before `GET /auth/me` resolves), the guard waits
 * for the next non-null `currentUser()` emission before deciding. Non-
 * admins are redirected to the root path rather than leaving the router on
 * the admin URL with a server-side 403.
 */
export const adminGuard: CanActivateFn = () => {
  const session = inject(SessionService);
  const userService = inject(UserService);
  const router = inject(Router);
  const injector = inject(Injector);

  const resolve = (isAdmin: boolean): true | UrlTree =>
    isAdmin ? true : router.parseUrl('/');

  const user = userService.currentUser();
  if (user) return resolve(user.is_admin === true);

  return new Promise<true | UrlTree>((done) => {
    runInInjectionContext(injector, () => {
      let ref: EffectRef | null = null;
      let resolved = false;
      ref = effect(() => {
        const u = userService.currentUser();
        if (!u) return;
        if (resolved) return;
        resolved = true;
        ref?.destroy();
        done(resolve(u.is_admin === true));
      });
      setTimeout(() => {
        if (resolved) return;
        resolved = true;
        ref?.destroy();
        session.login();
        done(router.parseUrl('/'));
      }, 1500);
    });
  });
};

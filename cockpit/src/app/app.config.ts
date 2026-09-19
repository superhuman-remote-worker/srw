import {APP_INITIALIZER, ApplicationConfig, isDevMode, provideBrowserGlobalErrorListeners} from '@angular/core';
import {provideRouter, withViewTransitions} from '@angular/router';
import {provideServiceWorker} from '@angular/service-worker';
import {HttpClient, provideHttpClient, withInterceptors} from '@angular/common/http';
import {registerLocaleData} from '@angular/common';
import localeDe from '@angular/common/locales/de';
import {firstValueFrom} from 'rxjs';
import {authInterceptor} from './core/interceptors/auth.interceptor';
import {viewAsInterceptor} from './core/interceptors/view-as.interceptor';
import {MARKED_EXTENSIONS, MARKED_OPTIONS, SANITIZE, provideMarkdown} from 'ngx-markdown';
import {citationExtension} from './core/markdown/citation-extension';
import {mathExtension} from './core/markdown/math-extension';
import {externalImageExtension} from './core/markdown/external-image-extension';
import {markdownLinkExtension} from './core/markdown/link-extension';
import {sanitizeMarkdownHtml} from './core/markdown/markdown-sanitizer';
import {SessionService} from './core/services/session.service';
import {UserService} from './core/services/user.service';
import {SettingsService} from './core/services/settings.service';
import {I18nService, SUPPORTED_LANGS, DEFAULT_LANG} from './core/services/i18n.service';
import {TranslocoHttpLoader} from './core/services/transloco-loader';
import {provideTransloco} from '@jsverse/transloco';
import {provideTranslocoLocale} from '@jsverse/transloco-locale';
import {User} from './core/models/api.model';
import {environment} from './core/environment';

import {routes} from './app.routes';
import {provideClientHydration, withEventReplay} from '@angular/platform-browser';

registerLocaleData(localeDe, 'de-DE');

/**
 * Bootstrap auth via the cookie BFF.
 *
 * GET /auth/me — if the browser already has a valid `srw_session` cookie,
 * the orchestrator returns the user payload. We pre-populate UserService
 * before any component renders, so guards can decide synchronously and
 * components never see a null-then-populated flash.
 *
 * On 401 (no cookie or expired), we redirect to /auth/login on the API
 * origin; the orchestrator generates PKCE state and bounces on to
 * Keycloak. After successful auth the user lands back on the original
 * route via /auth/callback's `return_to` redirect.
 */
function authBootstrap(
  http: HttpClient,
  session: SessionService,
  userService: UserService,
  i18n: I18nService,
  settings: SettingsService,
): () => Promise<void> {
  return async () => {
    i18n.applyInitialLanguage();
    try {
      const resp = await firstValueFrom(
        http.get<{ user: User }>(`${environment.apiUrl}/auth/me`),
      );
      if (resp?.user) {
        userService.currentUser.set(resp.user);
        session.authenticated.set(true);
        userService.loadUsers();
        settings.loadPreferences();
      }
    } catch {
      // 401 — interceptor already kicked off the BFF login redirect. Swallow
      // so app bootstrap resolves (the page is about to unload anyway).
    }
  };
}

export const appConfig: ApplicationConfig = {
  providers: [
    provideBrowserGlobalErrorListeners(),
    provideRouter(routes, withViewTransitions()),
    provideClientHydration(withEventReplay()),
    // No withFetch(): Angular's FetchBackend emits no UploadProgress events at
    // all (@angular/common/fesm2022/_module-chunk.mjs — the package's only
    // HttpEventType.UploadProgress emission lives in HttpXhrBackend), so
    // attachment upload progress is impossible on it and `reportProgress: true`
    // is a silent no-op there. withXhr() does not exist in Angular 21, so the
    // only way to get the XHR backend is to stop asking for fetch.
    //
    // Safe here because nothing ever runs this config in Node: angular.json
    // sets `ssr: false` and declares no `server` entry, so the build emits no
    // server bundle and prerenders nothing (dist/cockpit/prerendered-routes.json
    // is `{"routes": {}}`; index.html ships an empty <app-root> with no `ngh`
    // hydration markers). If SSR is ever switched on, app.config.server.ts's
    // provideServerRendering() supplies XhrFactory → ServerXhr, which is exactly
    // what an XHR-backed HttpClient needs off-browser.
    // knowledge-base/knowledge/features/session_attachment_send_flow.md §9.2
    provideHttpClient(withInterceptors([authInterceptor, viewAsInterceptor])),
    provideTransloco({
      config: {
        availableLangs: [...SUPPORTED_LANGS],
        defaultLang: DEFAULT_LANG,
        fallbackLang: DEFAULT_LANG,
        reRenderOnLangChange: true,
        prodMode: !isDevMode(),
      },
      loader: TranslocoHttpLoader,
    }),
    provideTranslocoLocale({
      langToLocaleMapping: {
        en: 'en-US',
        'de-DE': 'de-DE',
      },
    }),
    {
      provide: APP_INITIALIZER,
      useFactory: authBootstrap,
      deps: [HttpClient, SessionService, UserService, I18nService, SettingsService],
      multi: true,
    },
    provideMarkdown({
      markedOptions: {
        provide: MARKED_OPTIONS,
        useValue: {
          gfm: true,
          breaks: true,
        },
      },
      markedExtensions: [
        {
          // Images are URL-review cards; never emit a live remote <img>.
          provide: MARKED_EXTENSIONS,
          multi: true,
          useValue: externalImageExtension(),
        },
        {
          // No agent-written link may navigate this document: external ones
          // open in a new tab, workspace paths become inert in-app controls.
          provide: MARKED_EXTENSIONS,
          multi: true,
          useValue: markdownLinkExtension(),
        },
        {
          provide: MARKED_EXTENSIONS,
          multi: true,
          useValue: citationExtension(),
        },
        {
          // Protect LaTeX math from markdown mangling before the KaTeX pass.
          provide: MARKED_EXTENSIONS,
          multi: true,
          useValue: mathExtension(),
        },
      ],
      sanitize: {
        provide: SANITIZE,
        useValue: sanitizeMarkdownHtml,
      },
    }),
    provideServiceWorker('ngsw-worker.js', {
      // Only register in production builds. Dev mode reloads frequently and
      // a stale SW would shadow code changes.
      enabled: !isDevMode() && environment.serviceWorkerEnabled,
      // Register immediately: Chrome decides install-as-app vs. bookmark
      // shortcut based on whether a SW controls the page at install time, so
      // a deferred registration loses the standalone-window install.
      registrationStrategy: 'registerImmediately',
    }),
  ]
};

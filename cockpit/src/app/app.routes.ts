import {Routes} from '@angular/router';
import {ChatPageComponent} from './views/chat/chat-page.component';
import {authGuard} from './core/guards/auth.guard';
import {adminGuard} from './core/guards/admin.guard';
import {projectAccessGuard} from './core/guards/project-access.guard';

const loadSettings = () =>
  import('./views/settings/settings.component').then((m) => m.SettingsComponent);

// Expert authoring is only needed when creating or editing an Expert.
const loadExpertEditor = () =>
  import('./views/experts/expert-editor.component').then((m) => m.ExpertEditorComponent);

const loadSkillEditor = () =>
  import('./views/skills/skill-editor.component').then((m) => m.SkillEditorComponent);

export const routes: Routes = [
  // Instant landing (knowledge-base/knowledge/features/instant_landing_session.md): the root is a
  // fresh draft chat — open composer, nothing created until the first send.
  // (Replaces the sessions-list redirect left by the builder removal, see
  // knowledge-base/knowledge/features/builder_to_sessions_consolidation.md.)
  { path: '', component: ChatPageComponent, canActivate: [authGuard], data: { draft: true } },
    {
      path: 'sessions',
      loadComponent: () =>
        import('./views/sessions/sessions-page.component').then((m) => m.SessionsPageComponent),
      canActivate: [authGuard],
    },
    {
      path: 'sessions/new',
      loadComponent: () =>
        import('./views/session-create/session-create.component').then((m) => m.SessionCreateComponent),
      canActivate: [authGuard],
    },
    {
      path: 'sessions/:threadId/canvas',
      loadComponent: () =>
        import('./views/canvas/canvas-popout-page.component').then((m) => m.CanvasPopoutPageComponent),
      canActivate: [authGuard],
      data: {canvasPopout: true},
    },
    // Eager like '': ChatPageComponent is the landing page, so a lazy wrapper would save nothing.
    {path: 'sessions/:threadId', component: ChatPageComponent, canActivate: [authGuard]},
    {path: 'chat', redirectTo: 'sessions'},
  {
    path: 'jobs',
    loadComponent: () =>
      import('./views/jobs/jobs-page.component').then((m) => m.JobsPageComponent),
    canActivate: [authGuard],
  },
  {
    path: 'jobs/new',
    loadComponent: () =>
      import('./views/create/create-page.component').then((m) => m.CreatePageComponent),
    canActivate: [authGuard],
  },
  {
    path: 'jobs/review',
    loadComponent: () =>
      import('./views/job-review/job-review-page.component').then((m) => m.JobReviewPageComponent),
    canActivate: [authGuard],
  },
  {
    path: 'inbox',
    loadComponent: () =>
      import('./views/inbox/inbox-page.component').then((m) => m.InboxPageComponent),
    canActivate: [authGuard],
  },
  {
    path: 'projects',
    loadComponent: () =>
      import('./views/projects/project-list.component').then((m) => m.ProjectListPageComponent),
    canActivate: [authGuard],
  },
  {
    path: 'projects/:id',
    loadComponent: () =>
      import('./views/project-detail/project-detail.component').then((m) => m.ProjectDetailPageComponent),
    canActivate: [authGuard, projectAccessGuard],
  },
  {
    path: 'projects/:id/officer/conference',
    loadComponent: () =>
      import('./views/project-detail/conference-launcher.component').then((m) => m.ConferenceLauncherComponent),
    canActivate: [authGuard, projectAccessGuard],
  },
  {
    path: 'datasources',
    loadComponent: () =>
      import('./views/datasources/datasources-page.component').then(m => m.DatasourcesPageComponent),
    canActivate: [authGuard],
  },
  {
    path: 'contacts',
    loadComponent: () =>
      import('./views/contacts/contacts-page.component').then(m => m.ContactsPageComponent),
    canActivate: [authGuard],
  },
  {
    path: 'experts',
    loadComponent: () =>
      import('./views/experts/experts-page.component').then((m) => m.ExpertsPageComponent),
    canActivate: [authGuard],
  },
  { path: 'experts/new', loadComponent: loadExpertEditor, canActivate: [authGuard] },
  { path: 'experts/:id/edit', loadComponent: loadExpertEditor, canActivate: [authGuard] },
  {
    path: 'skills',
    loadComponent: () =>
      import('./views/skills/skills-page.component').then((m) => m.SkillsPageComponent),
    canActivate: [authGuard],
  },
  { path: 'skills/new', loadComponent: loadSkillEditor, canActivate: [authGuard] },
  { path: 'skills/:id/edit', loadComponent: loadSkillEditor, canActivate: [authGuard] },
  // Slice A3: workspace templates, the fifth Customize tab. Lazy, like the
  // other Customize editors, so the chat bundle doesn't carry them.
  {
    path: 'workspaces',
    loadComponent: () =>
      import('./views/workspaces/workspace-templates-page.component').then((m) => m.WorkspaceTemplatesPageComponent),
    canActivate: [authGuard],
  },
  {
    path: 'workspaces/new',
    loadComponent: () =>
      import('./views/workspaces/workspace-template-editor.component').then((m) => m.WorkspaceTemplateEditorComponent),
    canActivate: [authGuard],
  },
  {
    path: 'workspaces/:uid',
    loadComponent: () =>
      import('./views/workspaces/workspace-template-editor.component').then((m) => m.WorkspaceTemplateEditorComponent),
    canActivate: [authGuard],
  },
  // Automations loads on demand: the schedule editor is the only screen that
  // needs cronstrue + cron-parser, and both are CommonJS, so keeping the route
  // eager taxed every page load with a cron library it would never call.
  {
    path: 'automations',
    loadComponent: () =>
      import('./views/automations/automations-page.component').then(m => m.AutomationsPageComponent),
    canActivate: [authGuard],
  },
  // Settings is one page per section, all rendered by SettingsComponent from
  // the route's `section` (navigation_fixed_rail.md §5). Loaded on demand,
  // keeping account and subscription controls out of the initial chat bundle
  // as with the other settings/admin pages. The rail lists the sections;
  // /settings itself is only the door, so old links land on General.
  {path: 'settings', pathMatch: 'full', redirectTo: 'settings/general'},
  {path: 'settings/general', loadComponent: loadSettings, canActivate: [authGuard], data: {section: 'general'}},
  {path: 'settings/defaults', loadComponent: loadSettings, canActivate: [authGuard], data: {section: 'defaults'}},
  {path: 'settings/provider-keys', loadComponent: loadSettings, canActivate: [authGuard], data: {section: 'provider-keys'}},
  {path: 'settings/notifications', loadComponent: loadSettings, canActivate: [authGuard], data: {section: 'notifications'}},
  {path: 'settings/mcp', loadComponent: loadSettings, canActivate: [authGuard], data: {section: 'mcp'}},
  {
    path: 'settings/api-keys',
    loadComponent: () =>
      import('./views/settings/api-keys/api-keys-page.component').then(
        (m) => m.ApiKeysPageComponent,
      ),
    canActivate: [authGuard],
  },
  // The connector driver capability matrix (connector_drivers.md, D2): read
  // by any signed-in user, linked from the Connectors page too.
  {
    path: 'settings/connector-drivers',
    loadComponent: () =>
      import('./views/connector-drivers/connector-drivers-page.component').then(
        (m) => m.ConnectorDriversPageComponent,
      ),
    canActivate: [authGuard],
  },
  // SSH key management also loads on demand; its key-generation instructions
  // are only needed when this page is opened.
  {
    path: 'settings/ssh-keys',
    loadComponent: () =>
      import('./views/settings/ssh-keys/ssh-keys-page.component').then(
        (m) => m.SshKeysPageComponent,
      ),
    canActivate: [authGuard],
  },
  // Admin and the workbench load on demand. They are large (the config, usage
  // and grants screens alone are most of a megabyte of source, and the
  // workbench pulls the graph timeline), and no ordinary session ever opens
  // them — keeping them in the initial bundle taxed every page load to serve a
  // handful of admin visits, and pushed the build past its initial-bundle
  // budget.
  // Admin is the Administration group of Settings, not a separate area: the
  // settings rail lists these routes under its own heading for admins
  // (navigation_fixed_rail.md F4). The parent is componentless — the rail owns
  // the sub-navigation now — and exists to carry the guards once for every
  // child, asserted in app.routes.spec.ts so a future edit can't drop them
  // silently. Each page stays on loadComponent (see the comment above).
  // Subscriptions and cloud storage are Settings sections that only admins
  // can use; they live here so they inherit adminGuard instead of relying on
  // a template @if.
  {
    path: 'admin',
    canActivate: [authGuard, adminGuard],
    children: [
      {path: '', pathMatch: 'full', redirectTo: 'models'},
      {
        path: 'models',
        loadComponent: () =>
          import('./views/admin/models/admin-models.component').then(m => m.AdminModelsComponent),
      },
      {
        path: 'users',
        loadComponent: () =>
          import('./views/admin/users/admin-users.component').then(m => m.AdminUsersComponent),
      },
      {
        path: 'config',
        loadComponent: () =>
          import('./views/admin/config/admin-config.component').then(m => m.AdminConfigComponent),
      },
      {
        path: 'grants',
        loadComponent: () =>
          import('./views/admin/grants/admin-grants.component').then(m => m.AdminGrantsComponent),
      },
      {
        path: 'usage',
        loadComponent: () =>
          import('./views/admin/usage/admin-usage.component').then(m => m.AdminUsageComponent),
      },
      {
        path: 'capacity',
        loadComponent: () =>
          import('./views/admin/capacity/admin-capacity.component').then(m => m.AdminCapacityComponent),
      },
      {path: 'subscriptions', loadComponent: loadSettings, data: {section: 'subscriptions'}},
      {path: 'cloud', loadComponent: loadSettings, data: {section: 'cloud'}},
    ],
  },
  // The page was 'admin/llm' until the catalog grew past chat models — it now
  // holds TTS, speech-to-text, vision and embedding entries too, so the name
  // described a third of its contents. Both former paths still resolve;
  // 'admin/llm' in particular is in the wild via the readiness-gate banners.
  { path: 'admin/providers', redirectTo: 'admin/models' },
  { path: 'admin/llm', redirectTo: 'admin/models' },
  {
    path: 'workbench',
    loadComponent: () =>
      import('./workbench/pages/workbench.component').then(m => m.WorkbenchPageComponent),
    canActivate: [authGuard],
  },

  // Redirects for old bookmarks. Jobs absorbed the standalone Create + Review
  // surfaces, so their old top-level paths now redirect into /jobs/*. /debug
  // was renamed to /workbench — the surface is a customizable panel workspace,
  // not a troubleshooting console.
  { path: 'debug', redirectTo: 'workbench' },
  { path: 'sudo', redirectTo: 'inbox' },
  { path: 'create', redirectTo: 'jobs/new' },
  { path: 'review', redirectTo: 'jobs/review' },

  // Catch old email links: /jobs/{jobId}/messages/{threadId}
  // redirectTo can't transform path params to query params, so use a redirect component
  {
    path: 'jobs/:jobId/messages/:threadId',
    loadComponent: () =>
      import('./core/routing/message-redirect/message-redirect.component').then(
        (m) => m.MessageRedirectComponent,
      ),
  },

  { path: '**', redirectTo: '' },
];

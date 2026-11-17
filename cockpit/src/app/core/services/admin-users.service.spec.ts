import {describe, expect, it, vi} from 'vitest';
import {Injector, runInInjectionContext, signal} from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {TestBed} from '@angular/core/testing';
import {of, throwError} from 'rxjs';
import {AdminUsersService} from './admin-users.service';
import {NotificationService} from './notification.service';
import {McpServerQuestionsSetting, User} from '../models/api.model';
import {AdminUsersComponent} from '../../views/admin/users/admin-users.component';
import en from '../../../assets/i18n/en.json';
import de from '../../../assets/i18n/de-DE.json';

function createService(mockHttp?: any) {
  const http = mockHttp ?? {
    get: vi.fn().mockReturnValue(of([])),
    put: vi.fn().mockReturnValue(of({})),
    post: vi.fn().mockReturnValue(of({})),
    patch: vi.fn().mockReturnValue(of({})),
    delete: vi.fn().mockReturnValue(of({})),
  };

  const injector = Injector.create({
    providers: [{provide: HttpClient, useValue: http}],
  });

  const service = runInInjectionContext(injector, () => new AdminUsersService());
  return {service, http};
}

describe('AdminUsersService', () => {
  it('loads users (incl. approval status) from /admin/users', () => {
    const rows: Partial<User>[] = [
      {id: '1', display_name: 'A', is_approved: true},
      {id: '2', display_name: 'B', is_approved: false},
    ];
    const http = {
      get: vi.fn().mockReturnValue(of(rows)),
      put: vi.fn(),
      post: vi.fn(),
      patch: vi.fn(),
      delete: vi.fn(),
    };
    const {service} = createService(http);
    service.loadUsers();
    expect(http.get).toHaveBeenCalledWith(
      expect.stringContaining('/admin/users'),
    );
    expect(service.users()).toHaveLength(2);
    expect(service.users()[1].is_approved).toBe(false);
  });

  it('PATCHes a flag and updates the row in place (no full reload)', () => {
    const http = {
      get: vi.fn().mockReturnValue(of([])),
      patch: vi.fn().mockReturnValue(of({status: 'updated'})),
      put: vi.fn(),
      post: vi.fn(),
      delete: vi.fn(),
    };
    const {service} = createService(http);
    service.users.set([
      {id: 'u-1', display_name: 'A', is_approved: true},
      {id: 'u-2', display_name: 'B', is_approved: true},
    ] as User[]);
    service.patchUser('u-1', {is_approved: false}).subscribe();
    expect(http.patch).toHaveBeenCalledWith(
      expect.stringContaining('/admin/users/u-1'),
      {is_approved: false},
    );
    // No full reload: the changed row is patched in place so the list does not
    // re-fetch and visibly "jump"; only the targeted row changes.
    expect(http.get).not.toHaveBeenCalled();
    expect(service.users()[0].is_approved).toBe(false);
    expect(service.users()[1].is_approved).toBe(true);
  });

  it('POSTs bulk approval to /admin/users/approve with user_ids and reloads', () => {
    const http = {
      get: vi.fn().mockReturnValue(of([])),
      post: vi.fn().mockReturnValue(
        of({
          approved_count: 2,
          results: [
            {id: 'a', status: 'approved'},
            {id: 'b', status: 'approved'},
          ],
        }),
      ),
      put: vi.fn(),
      patch: vi.fn(),
      delete: vi.fn(),
    };
    const {service} = createService(http);
    let got: any = null;
    service.approveUsers(['a', 'b']).subscribe((r) => (got = r));
    expect(http.post).toHaveBeenCalledWith(
      expect.stringContaining('/admin/users/approve'),
      {user_ids: ['a', 'b']},
    );
    expect(got.approved_count).toBe(2);
    expect(http.get).toHaveBeenCalled(); // tap reloads the list
  });
});

describe('AdminUsersService — MCP server questions default', () => {
  const PATH = '/admin/system-settings/mcp_server_questions';

  it('reads the default from its system setting', () => {
    const row = {default: 'accept', updated_at: '2026-11-17T10:00:00Z', updated_by: 'admin'};
    const {service, http} = createService({get: vi.fn().mockReturnValue(of(row)), put: vi.fn()});
    expect(service.mcpServerQuestions()).toBeNull();
    service.loadMcpServerQuestions();
    expect(http.get).toHaveBeenCalledWith(expect.stringMatching(new RegExp(`${PATH}$`)));
    expect(service.mcpServerQuestions()).toEqual(row);
  });

  it('keeps what it knew when the read fails (the endpoint is admin-only)', () => {
    const {service} = createService({
      get: vi.fn().mockReturnValue(throwError(() => ({status: 403}))),
      put: vi.fn(),
    });
    service.loadMcpServerQuestions();
    expect(service.mcpServerQuestions()).toBeNull();
  });

  it('PUTs a new default and keeps the answer', () => {
    const row = {default: 'decline', updated_at: '2026-11-17T11:00:00Z', updated_by: 'root'};
    const http = {get: vi.fn(), put: vi.fn().mockReturnValue(of(row))};
    const {service} = createService(http);
    let answered: McpServerQuestionsSetting | null = null;
    service.setMcpServerQuestionsDefault('decline').subscribe((r) => (answered = r));
    expect(http.put).toHaveBeenCalledWith(expect.stringMatching(new RegExp(`${PATH}$`)), {
      default: 'decline',
    });
    expect(answered).toEqual(row);
    expect(service.mcpServerQuestions()).toEqual(row);
  });
});

describe('AdminUsersComponent — MCP server questions default', () => {
  function create(put: ReturnType<typeof vi.fn>) {
    const admin = {
      users: signal([]),
      vmWorkspaces: signal({enabled: true, updated_at: null, updated_by: null}),
      mcpServerQuestions: signal<McpServerQuestionsSetting | null>({
        default: 'decline',
        updated_at: null,
        updated_by: null,
      }),
      loadUsers: vi.fn(),
      loadVmWorkspaces: vi.fn(),
      loadMcpServerQuestions: vi.fn(),
      setMcpServerQuestionsDefault: put,
    };
    // TestBed's injector: the page's effect needs a change-detection scheduler.
    TestBed.resetTestingModule();
    TestBed.configureTestingModule({
      providers: [
        {provide: AdminUsersService, useValue: admin},
        {provide: NotificationService, useValue: {adminUserRegistered: signal(null)}},
      ],
    });
    const component = TestBed.runInInjectionContext(() => new AdminUsersComponent());
    return {admin, component};
  }

  it('loads the default with the page and saves a new choice', () => {
    const put = vi.fn().mockReturnValue(of({default: 'accept', updated_at: null, updated_by: null}));
    const {admin, component} = create(put);
    component.ngOnInit();
    expect(admin.loadMcpServerQuestions).toHaveBeenCalled();
    expect(component.mcpQuestionsDefault()).toBe('decline');
    component.setMcpQuestionsDefault('accept');
    expect(put).toHaveBeenCalledWith('accept');
    expect(component.mcpQuestionsFailed()).toBe(false);
  });

  it('springs back to the stored choice when the save is refused', () => {
    const put = vi.fn().mockReturnValue(throwError(() => ({status: 500})));
    const {admin, component} = create(put);
    component.setMcpQuestionsDefault('accept');
    expect(component.mcpQuestionsDefault()).toBe('decline');
    expect(component.mcpQuestionsFailed()).toBe(true);
    expect(admin.loadMcpServerQuestions).toHaveBeenCalled();
  });

  it('has every string in both languages', () => {
    expect(Object.keys(de.admin.mcpQuestions).sort()).toEqual(
      Object.keys(en.admin.mcpQuestions).sort(),
    );
  });
});

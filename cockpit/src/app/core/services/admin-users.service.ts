import {inject, Injectable, signal} from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {catchError, Observable, of, tap} from 'rxjs';
import {
  McpServerQuestionsChoice,
  McpServerQuestionsSetting,
  User,
  VmWorkspacesSetting,
} from '../models/api.model';
import {environment} from '../environment';

export interface AdminUserPatch {
  // Admin status is owned by the Keycloak `admin` realm role and is not
  // settable via the app (it would be reconciled away on the user's next
  // request). The cockpit users page shows it read-only.
  can_use_vm?: boolean;
  is_approved?: boolean;
}

/** Result of POST /admin/users/approve (bulk admission). */
export interface AdminApproveResult {
  approved_count: number;
  results: {id: string; status: string}[];
}

/**
 * REST client for the admin user management + VM kill-switch surface.
 * Every call is gated by `is_admin=TRUE` server-side; the client does not
 * re-check.
 */
@Injectable({providedIn: 'root'})
export class AdminUsersService {
  private readonly http = inject(HttpClient);
  private readonly baseUrl = environment.apiUrl;

  readonly users = signal<User[]>([]);
  readonly vmWorkspaces = signal<VmWorkspacesSetting>({
    enabled: true,
    updated_at: null,
    updated_by: null,
  });
  /** The installation default for MCP server questions; null until read
   *  (or when the caller may not read it: the endpoint is admin-only). */
  readonly mcpServerQuestions = signal<McpServerQuestionsSetting | null>(null);

  loadUsers(): void {
    this.http
      .get<User[]>(`${this.baseUrl}/admin/users`)
      .pipe(catchError(() => of([] as User[])))
      .subscribe((rows) => this.users.set(rows));
  }

  patchUser(userId: string, body: AdminUserPatch): Observable<{status: string}> {
    return this.http
      .patch<{status: string}>(`${this.baseUrl}/admin/users/${userId}`, body)
      // Update just the changed row in place instead of refetching the whole
      // list. A full reload (loadUsers) replaces the array and re-renders every
      // row, which is what made the list visibly "jump" on a single toggle.
      // Callers (e.g. toggleVm) reload on error to fall back to server truth.
      .pipe(
        tap(() =>
          this.users.update((rows) =>
            rows.map((u) => (u.id === userId ? {...u, ...body} : u)),
          ),
        ),
      );
  }

  /**
   * Bulk-approve pending users in a single server-side transaction. Reloads
   * the list on success so the Status column reflects the new state.
   */
  approveUsers(userIds: string[]): Observable<AdminApproveResult> {
    return this.http
      .post<AdminApproveResult>(`${this.baseUrl}/admin/users/approve`, {
        user_ids: userIds,
      })
      .pipe(tap(() => this.loadUsers()));
  }

  loadVmWorkspaces(): void {
    this.http
      .get<VmWorkspacesSetting>(`${this.baseUrl}/admin/system-settings/vm_workspaces`)
      .pipe(
        catchError(() =>
          of<VmWorkspacesSetting>({enabled: true, updated_at: null, updated_by: null}),
        ),
      )
      .subscribe((row) => this.vmWorkspaces.set(row));
  }

  setVmWorkspacesEnabled(enabled: boolean): Observable<VmWorkspacesSetting> {
    return this.http
      .put<VmWorkspacesSetting>(
        `${this.baseUrl}/admin/system-settings/vm_workspaces`,
        {enabled},
      )
      .pipe(tap((row) => this.vmWorkspaces.set(row)));
  }

  /** Read the MCP server-questions default. A failed read leaves what is
   *  known (null at first), never a guess. */
  loadMcpServerQuestions(): void {
    this.http
      .get<McpServerQuestionsSetting>(
        `${this.baseUrl}/admin/system-settings/mcp_server_questions`,
      )
      .pipe(catchError(() => of(null)))
      .subscribe((row) => {
        if (row) this.mcpServerQuestions.set(row);
      });
  }

  setMcpServerQuestionsDefault(
    value: McpServerQuestionsChoice,
  ): Observable<McpServerQuestionsSetting> {
    return this.http
      .put<McpServerQuestionsSetting>(
        `${this.baseUrl}/admin/system-settings/mcp_server_questions`,
        {default: value},
      )
      .pipe(tap((row) => this.mcpServerQuestions.set(row)));
  }
}

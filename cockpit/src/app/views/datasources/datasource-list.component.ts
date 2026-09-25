import {Component, computed, DestroyRef, inject, OnInit, signal} from '@angular/core';
import {takeUntilDestroyed} from '@angular/core/rxjs-interop';
import {forkJoin, timer} from 'rxjs';
import {TranslocoPipe, TranslocoService} from '@jsverse/transloco';
import {ApiService} from '../../core/services/api.service';
import {
    CredentialFileEntry,
    Datasource,
    DatasourceCatalogFilters,
    DatasourceConfig,
    DatasourceCreateRequest,
    DatasourceIndexStatus,
    DatasourceTestResult,
    DatasourceType,
    DatasourceUpdateRequest,
    DatasourceScopeMode,
    EmailAccessTier,
    LinkableDatasourceProject,
    RepositoryForge,
} from '../../core/models/api.model';
import {AppButtonComponent} from '../../ui/button';
import {AppIconButtonComponent} from '../../ui/icon-button';
import {AppBadgeComponent, type BadgeTone} from '../../ui/badge';
import {AppChipComponent} from '../../ui/chip';
import {AppInputComponent} from '../../ui/input';
import {AppSelectComponent} from '../../ui/select';
import {AppTextareaComponent} from '../../ui/textarea';
import {AppIconComponent} from '../../ui/icon';
import {AppSpinnerComponent} from '../../ui/spinner';
import {AppFormFieldComponent} from '../../ui/form-field';
import {AppDialogComponent} from '../../ui/dialog';
import {AppConfirmNameDialogComponent} from '../../ui/confirm-name-dialog';
import {CapabilitiesService} from '../../core/services/capabilities.service';
import {AppMenuComponent, AppMenuItemComponent, AppMenuTriggerDirective} from '../../ui/menu';
import {ViewportService} from '../../core/services/viewport.service';
import {UserService} from '../../core/services/user.service';
import {ActivatedRoute} from '@angular/router';

type McpTransport = 'http' | 'sse' | 'stdio';
type KeyValueRow = {key: string; value: string};

/**
 * Datasource management panel with full CRUD, type filtering, and connection testing.
 */
@Component({
  selector: 'app-datasource-list',
  standalone: true,
  imports: [
    TranslocoPipe,
    AppButtonComponent,
    AppIconButtonComponent,
    AppBadgeComponent,
    AppChipComponent,
    AppInputComponent,
    AppSelectComponent,
    AppTextareaComponent,
    AppIconComponent,
    AppSpinnerComponent,
    AppFormFieldComponent,
    AppDialogComponent,
    AppConfirmNameDialogComponent,
    AppMenuComponent,
    AppMenuItemComponent,
    AppMenuTriggerDirective,
  ],
  template: `
    <div class="ds-container" [class.form-open]="showForm()">
      <!-- Header -->
      <div class="header-bar">
        <span class="title">{{ 'datasources.title' | transloco }}</span>
        <div class="filter-chips">
          @for (filter of typeFilters; track filter.value) {
            <app-chip
              size="sm"
              [selected]="typeFilter() === filter.value"
              (clicked)="onTypeFilter(filter.value)"
            >
              {{ filter.labelKey | transloco }}
            </app-chip>
          }
        </div>
        <div class="header-actions">
          <app-button
            variant="primary"
            size="sm"
            [disabled]="showForm()"
            (clicked)="openCreateForm()"
          >
            <app-icon size="sm">add</app-icon> {{ 'datasources.new' | transloco }}
          </app-button>
          <app-icon-button
            variant="ghost"
            size="sm"
            [ariaLabel]="'datasources.refresh' | transloco"
            [disabled]="isLoading()"
            (clicked)="refresh()"
          >
            <app-icon size="sm">refresh</app-icon>
          </app-icon-button>
        </div>
      </div>

      @if (capabilities.datasourceScopeAutoAttachAvailable()) {
        <div class="catalog-filter-bar">
          <app-input
            size="sm"
            [value]="catalogSearch()"
            (valueChange)="onCatalogSearch($event)"
            [placeholder]="'datasources.catalog.search' | transloco"
          />
          <div class="catalog-project-filter">
            <app-input
              size="sm"
              [value]="catalogProjectSearch()"
              (valueChange)="onCatalogProjectSearch($event)"
              [placeholder]="'datasources.catalog.projectSearch' | transloco"
              [disabled]="catalogProjectsLoading()"
            />
            <app-select
              size="sm"
              [value]="catalogProjectId()"
              (changed)="onCatalogProjectFilter($event)"
              [disabled]="catalogProjectsLoading() || catalogProjectsError()"
            >
              <option value="">{{ 'datasources.catalog.anyProject' | transloco }}</option>
              @for (project of catalogProjects(); track project.id) {
                <option [value]="project.id">{{ project.name }}</option>
              }
            </app-select>
            @if (catalogProjectsLoading()) {
              <app-spinner size="sm" />
            } @else if (catalogProjectsNextCursor()) {
              <app-button size="sm" variant="ghost" (clicked)="loadMoreCatalogProjects()">
                {{ 'datasources.catalog.moreProjects' | transloco }}
              </app-button>
            }
          </div>
          <app-select size="sm" [value]="scopeFilter()" (changed)="onScopeFilter($event)">
            <option value="all">{{ 'datasources.catalog.anyAvailability' | transloco }}</option>
            <option value="scope-all">{{ 'datasources.catalog.everywhere' | transloco }}</option>
            <option value="projects">{{ 'datasources.catalog.projectScoped' | transloco }}</option>
            <option value="unavailable">{{ 'datasources.catalog.unavailable' | transloco }}</option>
          </app-select>
          <app-select size="sm" [value]="automaticFilter()" (changed)="onAutomaticFilter($event)">
            <option value="all">{{ 'datasources.catalog.anyDefault' | transloco }}</option>
            <option value="automatic">{{ 'datasources.catalog.automatic' | transloco }}</option>
            <option value="manual">{{ 'datasources.catalog.manual' | transloco }}</option>
          </app-select>
          <app-select size="sm" [value]="ownershipFilter()" (changed)="onOwnershipFilter($event)">
            <option value="all">{{ 'datasources.catalog.mineAndShared' | transloco }}</option>
            <option value="mine">{{ 'datasources.catalog.mine' | transloco }}</option>
            <option value="shared">{{ 'datasources.catalog.shared' | transloco }}</option>
          </app-select>
          <app-select size="sm" [value]="visibilityFilter()" (changed)="onVisibilityFilter($event)">
            <option value="all">{{ 'datasources.catalog.anyVisibility' | transloco }}</option>
            <option value="private">{{ 'datasources.catalog.private' | transloco }}</option>
            <option value="public">{{ 'datasources.catalog.public' | transloco }}</option>
          </app-select>
        </div>
      }

      <!-- Messages -->
      @if (successMessage()) {
        <div class="msg success-msg">
          <span>{{ successMessage() }}</span>
          <app-button variant="ghost" size="sm" (clicked)="successMessage.set(null)">
            {{ 'datasources.dismiss' | transloco }}
          </app-button>
        </div>
      }
      @if (errorMessage()) {
        <div class="msg error-msg">
          <span>{{ errorMessage() }}</span>
          <app-button variant="ghost" size="sm" (clicked)="errorMessage.set(null)">
            {{ 'datasources.dismiss' | transloco }}
          </app-button>
        </div>
      }

      <!-- Create/Edit Form -->
      @if (showForm()) {
        <div class="form-panel">
          <div class="form-header">
            <span>{{ (editingId() ? 'datasources.form.editTitle' : 'datasources.form.newTitle') | transloco }}</span>
            <app-icon-button
              variant="ghost"
              size="sm"
              [ariaLabel]="'datasources.form.close' | transloco"
              (clicked)="closeForm()"
            >
              <app-icon size="sm">close</app-icon>
            </app-icon-button>
          </div>

          <div class="form-body">
            <div class="form-row">
              <app-form-field class="flex-1" [label]="'datasources.form.nameLabel' | transloco" [required]="true">
                <app-input
                  size="sm"
                  [value]="formData.name"
                  (valueChange)="formData.name = $event"
                  [placeholder]="'datasources.form.namePlaceholder' | transloco"
                  [disabled]="isSaving()"
                />
              </app-form-field>
              <app-form-field [label]="'datasources.form.typeLabel' | transloco" [required]="true">
                <app-select
                  size="sm"
                  [value]="formData.type"
                  (changed)="onTypeSelect($event)"
                  [disabled]="isSaving() || !!editingId()"
                >
                  <optgroup [label]="'datasources.form.typeGroupCli' | transloco">
                    <option value="credentials">{{ 'datasources.form.optCredentials' | transloco }}</option>
                    <option value="generic">{{ 'datasources.form.optGeneric' | transloco }}</option>
                  </optgroup>
                  <optgroup [label]="'datasources.form.typeGroupKnowledge' | transloco">
                    <option value="repository">{{ 'datasources.form.optRepository' | transloco }}</option>
                    <option value="kb">{{ 'datasources.form.optKb' | transloco }}</option>
                  </optgroup>
                  <optgroup [label]="'datasources.form.typeGroupManaged' | transloco">
                    <option value="postgresql">{{ 'datasources.form.optPostgresql' | transloco }}</option>
                    <option value="neo4j">{{ 'datasources.form.optNeo4j' | transloco }}</option>
                    <option value="mongodb">{{ 'datasources.form.optMongodb' | transloco }}</option>
                    <option value="webdav">{{ 'datasources.form.optWebdav' | transloco }}</option>
                    <option value="email">{{ 'datasources.form.optEmail' | transloco }}</option>
                  </optgroup>
                  <optgroup [label]="'datasources.form.typeGroupCredentialFiles' | transloco">
                    <option value="kubeconfig">{{ 'datasources.form.optKubeconfig' | transloco }}</option>
                    <option value="ssh_key">{{ 'datasources.form.optSshKey' | transloco }}</option>
                    <option value="generic_file">{{ 'datasources.form.optGenericFile' | transloco }}</option>
                  </optgroup>
                  <optgroup [label]="'datasources.form.typeGroupMcp' | transloco">
                    <option value="mcp">{{ 'datasources.form.optMcp' | transloco }}</option>
                  </optgroup>
                </app-select>
              </app-form-field>
            </div>

            @if (formData.type === 'mcp') {
              <app-form-field
                [label]="'datasources.form.mcpTransportLabel' | transloco"
                [required]="true"
              >
                <app-select
                  size="sm"
                  [value]="formData.mcpTransport"
                  (changed)="onMcpTransportSelect($event)"
                  [disabled]="isSaving()"
                >
                  <option value="http">{{ 'datasources.form.mcpTransportHttp' | transloco }}</option>
                  <option value="sse">{{ 'datasources.form.mcpTransportSse' | transloco }}</option>
                  <option value="stdio">{{ 'datasources.form.mcpTransportStdio' | transloco }}</option>
                </app-select>
              </app-form-field>
            }

            <!-- Connection URL (required for non-generic, non-credential-file types) -->
            @if (hasConnectionUrl() && !isEnvType()) {
              <app-form-field
                [label]="(isGitBackedType() ? 'datasources.form.repoUrlLabel' : 'datasources.form.connectionUrlLabel') | transloco"
                [required]="true"
              >
                <app-input
                  size="sm"
                  class="mono"
                  [value]="formData.connection_url"
                  (valueChange)="onConnectionUrlChange($event)"
                  [placeholder]="urlPlaceholder()"
                  [disabled]="isSaving()"
                />
              </app-form-field>
            }

            <!-- Generic: optional connection URL -->
            @if (isEnvType()) {
              <app-form-field [label]="'datasources.form.connectionUrlLabel' | transloco" [optional]="'datasources.form.optional' | transloco">
                <app-input
                  size="sm"
                  class="mono"
                  [value]="formData.connection_url"
                  (valueChange)="onConnectionUrlChange($event)"
                  [placeholder]="'datasources.form.genericUrlPlaceholder' | transloco"
                  [disabled]="isSaving()"
                />
              </app-form-field>
            }

            <app-form-field
              [label]="'datasources.form.descriptionLabel' | transloco"
              [required]="formData.type === 'generic'"
            >
              <app-textarea
                size="sm"
                [value]="formData.description"
                (valueChange)="formData.description = $event"
                [placeholder]="(formData.type === 'generic' ? 'datasources.form.descriptionPlaceholderGeneric' : 'datasources.form.descriptionPlaceholderManaged') | transloco"
                [rows]="formData.type === 'generic' ? 3 : 2"
                [disabled]="isSaving()"
              />
            </app-form-field>

            @if (formData.type === 'mcp' && formData.mcpTransport !== 'stdio') {
              <app-form-field
                [label]="'datasources.form.mcpTokenLabel' | transloco"
                [optional]="'datasources.form.optional' | transloco"
              >
                <app-input
                  size="sm"
                  type="password"
                  class="mono"
                  [value]="formData.mcpToken"
                  (valueChange)="formData.mcpToken = $event"
                  [placeholder]="'datasources.form.mcpTokenPlaceholder' | transloco"
                  [disabled]="isSaving()"
                />
              </app-form-field>
              <app-form-field
                [label]="'datasources.form.mcpHeadersLabel' | transloco"
                [optional]="'datasources.form.optional' | transloco"
              >
                <div class="env-vars-editor">
                  @for (header of formData.mcpHeaders; track $index) {
                    <div class="env-var-row">
                      <app-input
                        size="sm"
                        class="mono env-key"
                        [value]="header.key"
                        (valueChange)="header.key = $event"
                        [placeholder]="'datasources.form.mcpHeaderKeyPlaceholder' | transloco"
                        [disabled]="isSaving()"
                      />
                      <span class="env-eq">:</span>
                      <app-input
                        size="sm"
                        type="password"
                        class="mono env-val"
                        [value]="header.value"
                        (valueChange)="header.value = $event"
                        [placeholder]="'datasources.form.mcpHeaderValuePlaceholder' | transloco"
                        [disabled]="isSaving()"
                      />
                      <app-icon-button
                        variant="danger"
                        size="sm"
                        [ariaLabel]="'datasources.form.envRemoveTooltip' | transloco"
                        [tooltip]="'datasources.form.envRemoveTooltip' | transloco"
                        [disabled]="isSaving()"
                        (clicked)="removeMcpHeader($index)"
                      >
                        <app-icon size="sm">close</app-icon>
                      </app-icon-button>
                    </div>
                  }
                  <app-button
                    variant="ghost"
                    size="sm"
                    class="btn-add-env"
                    [disabled]="isSaving()"
                    (clicked)="addMcpHeader()"
                  >
                    <app-icon size="sm">add</app-icon>
                    {{ 'datasources.form.mcpHeaderAdd' | transloco }}
                  </app-button>
                </div>
              </app-form-field>
              @if (editingId()) {
                <div class="credential-retain-hint">
                  {{ 'datasources.form.mcpCredentialsRetainHint' | transloco }}
                </div>
              }
            }

            @if (formData.type === 'mcp' && formData.mcpTransport === 'stdio') {
              <app-form-field
                [label]="'datasources.form.mcpCommandLabel' | transloco"
                [required]="true"
              >
                <app-input
                  size="sm"
                  class="mono"
                  [value]="formData.mcpCommand"
                  (valueChange)="formData.mcpCommand = $event"
                  [placeholder]="'datasources.form.mcpCommandPlaceholder' | transloco"
                  [disabled]="isSaving()"
                />
              </app-form-field>
              <app-form-field
                [label]="'datasources.form.mcpArgsLabel' | transloco"
                [optional]="'datasources.form.optional' | transloco"
              >
                <app-textarea
                  size="sm"
                  class="mono"
                  [value]="formData.mcpArgs"
                  (valueChange)="formData.mcpArgs = $event"
                  [placeholder]="'datasources.form.mcpArgsPlaceholder' | transloco"
                  [rows]="3"
                  [disabled]="isSaving()"
                />
              </app-form-field>
              <app-form-field
                [label]="'datasources.form.mcpEnvLabel' | transloco"
                [optional]="'datasources.form.optional' | transloco"
              >
                <div class="env-vars-editor">
                  @for (envVar of formData.mcpEnv; track $index) {
                    <div class="env-var-row">
                      <app-input
                        size="sm"
                        class="mono env-key"
                        [value]="envVar.key"
                        (valueChange)="envVar.key = $event"
                        [placeholder]="'datasources.form.envKeyPlaceholder' | transloco"
                        [disabled]="isSaving()"
                      />
                      <span class="env-eq">=</span>
                      <app-input
                        size="sm"
                        type="password"
                        class="mono env-val"
                        [value]="envVar.value"
                        (valueChange)="envVar.value = $event"
                        [placeholder]="'datasources.form.envValuePlaceholder' | transloco"
                        [disabled]="isSaving()"
                      />
                      <app-icon-button
                        variant="danger"
                        size="sm"
                        [ariaLabel]="'datasources.form.envRemoveTooltip' | transloco"
                        [tooltip]="'datasources.form.envRemoveTooltip' | transloco"
                        [disabled]="isSaving()"
                        (clicked)="removeMcpEnv($index)"
                      >
                        <app-icon size="sm">close</app-icon>
                      </app-icon-button>
                    </div>
                  }
                  <app-button
                    variant="ghost"
                    size="sm"
                    class="btn-add-env"
                    [disabled]="isSaving()"
                    (clicked)="addMcpEnv()"
                  >
                    <app-icon size="sm">add</app-icon>
                    {{ 'datasources.form.envAdd' | transloco }}
                  </app-button>
                </div>
              </app-form-field>
              @if (editingId()) {
                <div class="credential-retain-hint">
                  {{ 'datasources.form.mcpCredentialsRetainHint' | transloco }}
                </div>
              }
              <div class="form-hint">
                {{ 'datasources.form.mcpStdioWarning' | transloco }}
              </div>
            }

            <!-- Generic: CLI hint -->
            @if (isEnvType()) {
              <app-form-field [label]="'datasources.form.cliHintLabel' | transloco" [optional]="'datasources.form.optional' | transloco">
                <app-input
                  size="sm"
                  class="mono"
                  [value]="formData.cli_hint"
                  (valueChange)="formData.cli_hint = $event"
                  [placeholder]="'datasources.form.cliHintPlaceholder' | transloco"
                  [disabled]="isSaving()"
                />
              </app-form-field>
            }

            <!-- Generic: Environment Variables -->
            @if (isEnvType()) {
              <app-form-field [label]="'datasources.form.envVarsLabel' | transloco" [hint]="(formData.type === 'credentials' ? 'datasources.form.credentialsEnvHint' : 'datasources.form.envHint') | transloco">
                <div class="env-vars-editor">
                  @for (envVar of envVars; track $index) {
                    <div class="env-var-row">
                      <app-input
                        size="sm"
                        class="mono env-key"
                        [value]="envVar.key"
                        (valueChange)="envVar.key = $event"
                        [placeholder]="'datasources.form.envKeyPlaceholder' | transloco"
                        [disabled]="isSaving()"
                      />
                      <span class="env-eq">=</span>
                      <app-input
                        size="sm"
                        type="password"
                        class="mono env-val"
                        [value]="envVar.value"
                        (valueChange)="envVar.value = $event"
                        [placeholder]="'datasources.form.envValuePlaceholder' | transloco"
                        [disabled]="isSaving()"
                      />
                      <app-icon-button
                        variant="danger"
                        size="sm"
                        [ariaLabel]="'datasources.form.envRemoveTooltip' | transloco"
                        [tooltip]="'datasources.form.envRemoveTooltip' | transloco"
                        [disabled]="isSaving()"
                        (clicked)="removeEnvVar($index)"
                      >
                        <app-icon size="sm">close</app-icon>
                      </app-icon-button>
                    </div>
                  }
                  <app-button
                    variant="ghost"
                    size="sm"
                    class="btn-add-env"
                    [disabled]="isSaving()"
                    (clicked)="addEnvVar()"
                  >
                    <app-icon size="sm">add</app-icon> {{ 'datasources.form.envAdd' | transloco }}
                  </app-button>
                </div>
              </app-form-field>
            }

            <!-- Git-backed datasource: default branch -->
            @if (isGitBackedType()) {
              <app-form-field [label]="'datasources.form.defaultBranchLabel' | transloco" [optional]="'datasources.form.optional' | transloco">
                <app-input
                  size="sm"
                  [value]="formData.default_branch"
                  (valueChange)="formData.default_branch = $event"
                  [placeholder]="'datasources.form.defaultBranchPlaceholder' | transloco"
                  [disabled]="isSaving()"
                />
              </app-form-field>
            }

            <!-- OKF Knowledge Base: repository-relative note root -->
            @if (formData.type === 'kb') {
              <app-form-field
                [label]="'datasources.form.okfRootLabel' | transloco"
                [hint]="'datasources.form.okfRootHint' | transloco"
                [optional]="'datasources.form.optional' | transloco"
              >
                <app-input
                  size="sm"
                  class="mono"
                  [value]="formData.root_path"
                  (valueChange)="formData.root_path = $event"
                  [placeholder]="'datasources.form.okfRootPlaceholder' | transloco"
                  [disabled]="isSaving()"
                />
              </app-form-field>
            }

            <!-- Repository: forge. github.com/gitlab.com default automatically;
                 any other host is left blank (required) since a self-hosted
                 Gitea and GitLab can't be told apart by URL alone. -->
            @if (formData.type === 'repository') {
              <app-form-field [label]="'datasources.form.forgeLabel' | transloco" [required]="true">
                <app-select
                  size="sm"
                  [value]="formData.forge"
                  (changed)="onForgeSelect($event)"
                  [disabled]="isSaving()"
                >
                  <option value="">{{ 'datasources.form.forgePlaceholder' | transloco }}</option>
                  <option value="github">{{ 'datasources.form.forgeGithub' | transloco }}</option>
                  <option value="gitea">{{ 'datasources.form.forgeGitea' | transloco }}</option>
                  <option value="gitlab">{{ 'datasources.form.forgeGitlab' | transloco }}</option>
                </app-select>
              </app-form-field>
            }

            <!-- Git-backed datasource: auth method -->
            @if (isGitBackedType()) {
              <app-form-field [label]="'datasources.form.authMethodLabel' | transloco" [required]="true">
                <app-select
                  size="sm"
                  [value]="gitAuthMethod"
                  (changed)="onGitAuthMethodChange($event)"
                  [disabled]="isSaving()"
                >
                  <option value="token">{{ 'datasources.form.authToken' | transloco }}</option>
                  <option value="ssh">{{ 'datasources.form.authSsh' | transloco }}</option>
                </app-select>
              </app-form-field>
              @if (gitAuthMethod === 'token') {
                <app-form-field [label]="'datasources.form.tokenLabel' | transloco">
                  <app-input
                    size="sm"
                    type="password"
                    class="mono"
                    [value]="formCredentials.password"
                    (valueChange)="formCredentials.password = $event"
                    [placeholder]="'datasources.form.tokenPlaceholder' | transloco"
                    [disabled]="isSaving()"
                  />
                </app-form-field>
                @if (formData.type === 'repository' && formData.forge === 'github') {
                  <div class="form-hint">{{ 'datasources.form.forgeTokenHintGithub' | transloco }}</div>
                }
                @if (formData.type === 'repository' && formData.forge === 'gitlab') {
                  <div class="form-hint">{{ 'datasources.form.forgeTokenHintGitlab' | transloco }}</div>
                }
                @if (formData.type === 'repository' && formData.forge === 'gitea') {
                  <div class="trust-notice">
                    <app-icon size="sm">warning</app-icon>
                    <div>{{ 'datasources.form.forgeTokenHintGitea' | transloco }}</div>
                  </div>
                }
              }
              @if (gitAuthMethod === 'ssh') {
                <app-form-field [label]="'datasources.form.sshKeyLabel' | transloco">
                  <app-textarea
                    size="sm"
                    class="mono"
                    [value]="gitSshKey"
                    (valueChange)="gitSshKey = $event"
                    placeholder="-----BEGIN OPENSSH PRIVATE KEY-----&#10;...&#10;-----END OPENSSH PRIVATE KEY-----"
                    [rows]="5"
                    [disabled]="isSaving() || isGeneratingKey()"
                  />
                </app-form-field>
                <div class="ssh-key-actions">
                  @if (!showGenerateConfirm()) {
                    <app-button
                      size="sm"
                      variant="secondary"
                      type="button"
                      [disabled]="isSaving() || isGeneratingKey()"
                      (clicked)="onGenerateSshKeyClick()"
                    >
                      @if (isGeneratingKey()) {
                        <app-spinner size="sm" />
                        {{ 'datasources.form.sshGenerateBusy' | transloco }}
                      } @else {
                        {{ 'datasources.form.sshGenerate' | transloco }}
                      }
                    </app-button>
                  } @else {
                    <span class="confirm-text">
                      {{ 'datasources.form.sshGenerateConfirm' | transloco }}
                    </span>
                    <app-button
                      size="sm"
                      variant="primary"
                      type="button"
                      (clicked)="confirmGenerateSshKey()"
                    >
                      {{ 'datasources.form.sshGenerateReplace' | transloco }}
                    </app-button>
                    <app-button
                      size="sm"
                      variant="secondary"
                      type="button"
                      (clicked)="cancelGenerateSshKey()"
                    >
                      {{ 'datasources.form.cancel' | transloco }}
                    </app-button>
                  }
                </div>
              }
              @if (editingId()) {
                <div class="credential-retain-hint">
                  {{ 'datasources.form.credentialsRetainHint' | transloco }}
                </div>
              }
              @if (patScopeHintKey(); as scopeHint) {
                <div class="form-hint">{{ scopeHint | transloco }}</div>
              }
              <div class="form-hint">
                {{ (formData.type === 'kb' ? 'datasources.form.kbHint' : 'datasources.form.repoHint') | transloco }}
              </div>
            }

            <!-- Managed connectors: credentials -->
            @if (formData.type === 'neo4j' || formData.type === 'webdav') {
              <div class="form-row">
                <app-form-field class="flex-1" [label]="'datasources.form.usernameLabel' | transloco">
                  <app-input
                    size="sm"
                    [value]="formCredentials.username"
                    (valueChange)="formCredentials.username = $event"
                    [placeholder]="'datasources.form.usernamePlaceholder' | transloco"
                    [disabled]="isSaving()"
                  />
                </app-form-field>
                <app-form-field class="flex-1" [label]="'datasources.form.passwordLabel' | transloco">
                  <app-input
                    size="sm"
                    type="password"
                    [value]="formCredentials.password"
                    (valueChange)="formCredentials.password = $event"
                    [placeholder]="'datasources.form.passwordPlaceholder' | transloco"
                    [disabled]="isSaving()"
                  />
                </app-form-field>
              </div>
            }

            <!-- Email (IMAP/SMTP): provider preset, credentials, tier + scoping -->
            @if (formData.type === 'email') {
              <app-form-field [label]="'datasources.form.emailProviderLabel' | transloco">
                <app-select
                  size="sm"
                  [value]="emailForm.provider"
                  (changed)="onEmailProviderSelect($event)"
                  [disabled]="isSaving()"
                >
                  <option value="custom">{{ 'datasources.form.emailProviderCustom' | transloco }}</option>
                  <option value="gmail">{{ 'datasources.form.emailProviderGmail' | transloco }}</option>
                  <option value="fastmail">{{ 'datasources.form.emailProviderFastmail' | transloco }}</option>
                  <option value="icloud">{{ 'datasources.form.emailProviderIcloud' | transloco }}</option>
                  <option value="yahoo">{{ 'datasources.form.emailProviderYahoo' | transloco }}</option>
                  <option value="mailbox">{{ 'datasources.form.emailProviderMailbox' | transloco }}</option>
                  <option value="gmx">{{ 'datasources.form.emailProviderGmx' | transloco }}</option>
                </app-select>
              </app-form-field>
              @if (emailForm.provider === 'fastmail') {
                <div class="form-hint">{{ 'datasources.form.emailProviderFastmailHint' | transloco }}</div>
              }
              @if (emailForm.provider === 'icloud') {
                <div class="form-hint">{{ 'datasources.form.emailProviderIcloudHint' | transloco }}</div>
              }
              <div class="form-hint">{{ 'datasources.form.emailOauthNotice' | transloco }}</div>

              <div class="form-row">
                <app-form-field class="flex-1" [label]="'datasources.form.usernameLabel' | transloco">
                  <app-input
                    size="sm"
                    [value]="formCredentials.username"
                    (valueChange)="formCredentials.username = $event"
                    [placeholder]="'datasources.form.emailUsernamePlaceholder' | transloco"
                    [disabled]="isSaving()"
                  />
                </app-form-field>
                <app-form-field
                  class="flex-1"
                  [label]="'datasources.form.emailPasswordLabel' | transloco"
                  [hint]="'datasources.form.emailAppPasswordHint' | transloco"
                >
                  <app-input
                    size="sm"
                    type="password"
                    [value]="formCredentials.password"
                    (valueChange)="formCredentials.password = $event"
                    [placeholder]="'datasources.form.passwordPlaceholder' | transloco"
                    [disabled]="isSaving()"
                  />
                </app-form-field>
              </div>
              @if (editingId()) {
                <div class="credential-retain-hint">
                  {{ 'datasources.form.credentialsRetainHint' | transloco }}
                </div>
              }

              <div class="form-row">
                <app-form-field class="flex-1" [label]="'datasources.form.emailImapHostLabel' | transloco" [required]="true">
                  <app-input
                    size="sm"
                    class="mono"
                    [value]="emailForm.imap_host"
                    (valueChange)="emailForm.imap_host = $event"
                    placeholder="imap.example.com"
                    [disabled]="isSaving()"
                  />
                </app-form-field>
                <app-form-field [label]="'datasources.form.emailImapPortLabel' | transloco">
                  <app-input
                    size="sm"
                    class="mono"
                    [value]="emailForm.imap_port"
                    (valueChange)="emailForm.imap_port = $event"
                    placeholder="993"
                    [disabled]="isSaving()"
                  />
                </app-form-field>
                <app-form-field [label]="'datasources.form.emailSecurityLabel' | transloco">
                  <app-select
                    size="sm"
                    [value]="emailForm.imap_security"
                    (changed)="onEmailSecurityChange('imap', $event)"
                    [disabled]="isSaving()"
                  >
                    <option value="ssl">{{ 'datasources.form.emailSecuritySsl' | transloco }}</option>
                    <option value="starttls">{{ 'datasources.form.emailSecurityStarttls' | transloco }}</option>
                  </app-select>
                </app-form-field>
              </div>

              <app-form-field
                [label]="'datasources.form.emailAccessLabel' | transloco"
                [hint]="emailAccessHintKey() | transloco"
              >
                <app-select
                  size="sm"
                  [value]="emailForm.access"
                  (changed)="onEmailAccessChange($event)"
                  [disabled]="isSaving()"
                >
                  <option value="read">{{ 'datasources.form.emailAccessRead' | transloco }}</option>
                  <option value="read_write">{{ 'datasources.form.emailAccessReadWrite' | transloco }}</option>
                  <option value="draft">{{ 'datasources.form.emailAccessDraft' | transloco }}</option>
                  <option value="send">{{ 'datasources.form.emailAccessSend' | transloco }}</option>
                </app-select>
              </app-form-field>

              @if (emailForm.access === 'send') {
                <div class="form-row">
                  <app-form-field class="flex-1" [label]="'datasources.form.emailSmtpHostLabel' | transloco" [required]="true">
                    <app-input
                      size="sm"
                      class="mono"
                      [value]="emailForm.smtp_host"
                      (valueChange)="emailForm.smtp_host = $event"
                      placeholder="smtp.example.com"
                      [disabled]="isSaving()"
                    />
                  </app-form-field>
                  <app-form-field [label]="'datasources.form.emailSmtpPortLabel' | transloco">
                    <app-input
                      size="sm"
                      class="mono"
                      [value]="emailForm.smtp_port"
                      (valueChange)="emailForm.smtp_port = $event"
                      placeholder="587"
                      [disabled]="isSaving()"
                    />
                  </app-form-field>
                  <app-form-field [label]="'datasources.form.emailSecurityLabel' | transloco">
                    <app-select
                      size="sm"
                      [value]="emailForm.smtp_security"
                      (changed)="onEmailSecurityChange('smtp', $event)"
                      [disabled]="isSaving()"
                    >
                      <option value="ssl">{{ 'datasources.form.emailSecuritySsl' | transloco }}</option>
                      <option value="starttls">{{ 'datasources.form.emailSecurityStarttls' | transloco }}</option>
                    </app-select>
                  </app-form-field>
                </div>
              }

              <app-form-field
                [label]="'datasources.form.emailFoldersLabel' | transloco"
                [hint]="'datasources.form.emailFoldersHint' | transloco"
                [optional]="'datasources.form.optional' | transloco"
              >
                <app-input
                  size="sm"
                  class="mono"
                  [value]="emailForm.folders"
                  (valueChange)="emailForm.folders = $event"
                  [placeholder]="'datasources.form.emailFoldersPlaceholder' | transloco"
                  [disabled]="isSaving()"
                />
              </app-form-field>
              @if (emailForm.access === 'send' && !emailForm.folders.trim()) {
                <div class="trust-notice">
                  <app-icon size="sm">warning</app-icon>
                  <div>{{ 'datasources.form.emailSendFoldersWarning' | transloco }}</div>
                </div>
              }

              <div class="form-row">
                <app-form-field
                  class="flex-1"
                  [label]="'datasources.form.emailDraftsFolderLabel' | transloco"
                  [optional]="'datasources.form.optional' | transloco"
                >
                  <app-input
                    size="sm"
                    class="mono"
                    [value]="emailForm.drafts_folder"
                    (valueChange)="emailForm.drafts_folder = $event"
                    placeholder="Drafts"
                    [disabled]="isSaving()"
                  />
                </app-form-field>
                <app-form-field
                  class="flex-1"
                  [label]="'datasources.form.emailFromAddressLabel' | transloco"
                  [optional]="'datasources.form.optional' | transloco"
                >
                  <app-input
                    size="sm"
                    class="mono"
                    [value]="emailForm.from_address"
                    (valueChange)="emailForm.from_address = $event"
                    placeholder="user@example.com"
                    [disabled]="isSaving()"
                  />
                </app-form-field>
              </div>

              <app-form-field
                [label]="'datasources.form.emailRecipientsLabel' | transloco"
                [hint]="'datasources.form.emailRecipientsHint' | transloco"
                [optional]="'datasources.form.optional' | transloco"
              >
                <app-input
                  size="sm"
                  class="mono"
                  [value]="emailForm.recipient_allowlist"
                  (valueChange)="emailForm.recipient_allowlist = $event"
                  [placeholder]="'datasources.form.emailRecipientsPlaceholder' | transloco"
                  [disabled]="isSaving()"
                />
              </app-form-field>

              @if (emailForm.access === 'send') {
                <app-form-field
                  [label]="'datasources.form.emailUnattendedSendLabel' | transloco"
                  [hint]="'datasources.form.emailUnattendedSendHint' | transloco"
                >
                  <label class="toggle-label">
                    <input
                      type="checkbox"
                      [checked]="emailForm.unattended_send"
                      (change)="emailForm.unattended_send = $any($event.target).checked"
                      [disabled]="isSaving()"
                    >
                    {{ 'datasources.form.emailUnattendedSendToggle' | transloco }}
                  </label>
                </app-form-field>
              }
            }

            <!-- Kubeconfig: paste/upload YAML -->
            @if (formData.type === 'kubeconfig') {
              <app-form-field [label]="'datasources.form.kubeconfigLabel' | transloco" [required]="!editingId()">
                <app-textarea
                  size="sm"
                  class="mono"
                  [value]="kubeconfigContent"
                  (valueChange)="kubeconfigContent = $event"
                  [placeholder]="'datasources.form.kubeconfigPlaceholder' | transloco"
                  [rows]="8"
                  [disabled]="isSaving()"
                />
              </app-form-field>
              <div class="cred-file-actions">
                <input #kubeFileInput type="file" hidden accept=".yaml,.yml,.conf,application/x-yaml,text/yaml" (change)="onUploadFile($event, 'kubeconfig')" />
                <app-button size="sm" variant="secondary" type="button" [disabled]="isSaving()" (clicked)="kubeFileInput.click()">
                  <app-icon size="sm">attach_file</app-icon> {{ 'datasources.form.uploadFile' | transloco }}
                </app-button>
              </div>
              <div class="form-hint">{{ 'datasources.form.kubeconfigHint' | transloco }}</div>
              <div class="trust-notice">
                <app-icon size="sm">shield</app-icon>
                <div>
                  <strong>{{ 'datasources.form.trustNoticeTitle' | transloco }}.</strong>
                  {{ 'datasources.form.trustNotice' | transloco }}
                </div>
              </div>
            }

            <!-- SSH key: paste or generate -->
            @if (formData.type === 'ssh_key') {
              <app-form-field [label]="'datasources.form.sshKeyContentLabel' | transloco" [required]="!editingId()">
                <app-textarea
                  size="sm"
                  class="mono"
                  [value]="gitSshKey"
                  (valueChange)="gitSshKey = $event"
                  [placeholder]="'datasources.form.sshKeyContentPlaceholder' | transloco"
                  [rows]="6"
                  [disabled]="isSaving() || isGeneratingKey()"
                />
              </app-form-field>
              <div class="cred-file-actions ssh-key-actions">
                <input #sshFileInput type="file" hidden (change)="onUploadFile($event, 'ssh_key')" />
                <app-button size="sm" variant="secondary" type="button" [disabled]="isSaving() || isGeneratingKey()" (clicked)="sshFileInput.click()">
                  <app-icon size="sm">attach_file</app-icon> {{ 'datasources.form.uploadFile' | transloco }}
                </app-button>
                @if (!showGenerateConfirm()) {
                  <app-button
                    size="sm"
                    variant="secondary"
                    type="button"
                    [disabled]="isSaving() || isGeneratingKey()"
                    (clicked)="onGenerateSshKeyClick()"
                  >
                    @if (isGeneratingKey()) {
                      <app-spinner size="sm" />
                      {{ 'datasources.form.sshGenerateBusy' | transloco }}
                    } @else {
                      {{ 'datasources.form.sshGenerate' | transloco }}
                    }
                  </app-button>
                } @else {
                  <span class="confirm-text">{{ 'datasources.form.sshGenerateConfirm' | transloco }}</span>
                  <app-button size="sm" variant="primary" type="button" (clicked)="confirmGenerateSshKey()">
                    {{ 'datasources.form.sshGenerateReplace' | transloco }}
                  </app-button>
                  <app-button size="sm" variant="secondary" type="button" (clicked)="cancelGenerateSshKey()">
                    {{ 'datasources.form.cancel' | transloco }}
                  </app-button>
                }
              </div>
              <div class="form-hint">{{ 'datasources.form.sshKeyHint' | transloco }}</div>
              <div class="trust-notice">
                <app-icon size="sm">shield</app-icon>
                <div>
                  <strong>{{ 'datasources.form.trustNoticeTitle' | transloco }}.</strong>
                  {{ 'datasources.form.trustNotice' | transloco }}
                </div>
              </div>
            }

            <!-- Generic file: repeatable editor -->
            @if (formData.type === 'generic_file') {
              <app-form-field [label]="'datasources.form.credFilesLabel' | transloco">
                <div class="generic-files-editor">
                  @for (gf of genericFiles; track $index) {
                    <div class="generic-file-card">
                      <div class="form-row">
                        <app-form-field class="flex-1" [label]="'datasources.form.credFileNameLabel' | transloco">
                          <app-input
                            size="sm"
                            [value]="gf.name"
                            (valueChange)="gf.name = $event"
                            [placeholder]="'datasources.form.credFileNamePlaceholder' | transloco"
                            [disabled]="isSaving()"
                          />
                        </app-form-field>
                        <app-form-field class="flex-1" [label]="'datasources.form.credFileTargetPathLabel' | transloco" [required]="true">
                          <app-input
                            size="sm"
                            class="mono"
                            [value]="gf.target_path"
                            (valueChange)="gf.target_path = $event"
                            [placeholder]="'datasources.form.credFileTargetPathPlaceholder' | transloco"
                            [disabled]="isSaving()"
                          />
                        </app-form-field>
                      </div>
                      <app-form-field [label]="'datasources.form.credFileContentsLabel' | transloco" [required]="true">
                        <app-textarea
                          size="sm"
                          class="mono"
                          [value]="gf.contents"
                          (valueChange)="gf.contents = $event"
                          [placeholder]="'datasources.form.credFileContentsPlaceholder' | transloco"
                          [rows]="5"
                          [disabled]="isSaving()"
                        />
                      </app-form-field>
                      <div class="form-row">
                        <app-form-field [label]="'datasources.form.credFileModeLabel' | transloco">
                          <app-input
                            size="sm"
                            class="mono"
                            [value]="gf.mode"
                            (valueChange)="gf.mode = $event"
                            [placeholder]="'datasources.form.credFileModePlaceholder' | transloco"
                            [disabled]="isSaving()"
                          />
                        </app-form-field>
                        <app-form-field class="flex-1" [label]="'datasources.form.credFileEnvVarLabel' | transloco" [optional]="'datasources.form.optional' | transloco">
                          <app-input
                            size="sm"
                            class="mono"
                            [value]="gf.env_var"
                            (valueChange)="gf.env_var = $event"
                            [placeholder]="'datasources.form.credFileEnvVarPlaceholder' | transloco"
                            [disabled]="isSaving()"
                          />
                        </app-form-field>
                        <div class="generic-file-card-actions">
                          <input #gfFileInput type="file" hidden (change)="onUploadFile($event, { file: $index })" />
                          <app-button size="sm" variant="ghost" type="button" [disabled]="isSaving()" (clicked)="gfFileInput.click()">
                            <app-icon size="sm">attach_file</app-icon> {{ 'datasources.form.uploadFile' | transloco }}
                          </app-button>
                          <app-icon-button
                            variant="danger"
                            size="sm"
                            [ariaLabel]="'datasources.form.credFileRemoveTooltip' | transloco"
                            [tooltip]="'datasources.form.credFileRemoveTooltip' | transloco"
                            [disabled]="isSaving()"
                            (clicked)="removeGenericFile($index)"
                          >
                            <app-icon size="sm">close</app-icon>
                          </app-icon-button>
                        </div>
                      </div>
                    </div>
                  }
                  <app-button
                    size="sm"
                    variant="ghost"
                    class="btn-add-env"
                    [disabled]="isSaving() || genericFiles.length >= 5"
                    (clicked)="addGenericFile()"
                  >
                    <app-icon size="sm">add</app-icon> {{ 'datasources.form.credFilesAdd' | transloco }}
                  </app-button>
                </div>
              </app-form-field>
              <div class="form-hint">{{ 'datasources.form.credFileGenericHint' | transloco }}</div>
              <div class="trust-notice">
                <app-icon size="sm">shield</app-icon>
                <div>
                  <strong>{{ 'datasources.form.trustNoticeTitle' | transloco }}.</strong>
                  {{ 'datasources.form.trustNotice' | transloco }}
                </div>
              </div>
            }

            <!-- Policy controls are rollout-gated. Older orchestrators keep
                 legacy connector CRUD without receiving unknown fields. -->
            @if (capabilities.datasourceScopeAutoAttachAvailable()) {
              <section class="availability-section" aria-labelledby="ds-availability-title">
              <div class="availability-heading" id="ds-availability-title">
                {{ 'datasources.form.availabilityTitle' | transloco }}
              </div>
              @if (isNativeProjectConnector()) {
                <div class="native-policy">
                  <app-icon size="sm">lock</app-icon>
                  <span>
                    <strong>{{ 'datasources.form.nativeProjectPolicy' | transloco }}</strong>
                    <small>{{ 'datasources.form.nativeProjectPolicyHint' | transloco }}</small>
                  </span>
                </div>
              } @else {
                <label class="availability-option">
                  <input
                    type="radio"
                    name="ds-scope-mode"
                    [checked]="formData.scope_mode === 'all'"
                    (change)="onScopeModeChange('all')"
                    [disabled]="isSaving() || scopeTargetsLoading()"
                  >
                  <span>
                    <strong>{{ 'datasources.form.scopeEverywhere' | transloco }}</strong>
                    <small>{{ 'datasources.form.scopeEverywhereHint' | transloco }}</small>
                  </span>
                </label>
                <label class="availability-option">
                  <input
                    type="radio"
                    name="ds-scope-mode"
                    [checked]="formData.scope_mode === 'projects'"
                    (change)="onScopeModeChange('projects')"
                    [disabled]="isSaving() || scopeTargetsLoading()"
                  >
                  <span>
                    <strong>{{ 'datasources.form.scopeProjects' | transloco }}</strong>
                    <small>{{ 'datasources.form.scopeProjectsHint' | transloco }}</small>
                  </span>
                </label>

                @if (scopeTargetsLoading()) {
                  <div class="scope-load-state"><app-spinner size="sm" /> {{ 'datasources.form.projectsLoading' | transloco }}</div>
                } @else if (scopeTargetsError()) {
                  <div class="scope-error" role="alert">
                    <span>{{ 'datasources.form.projectsLoadFailed' | transloco }}</span>
                    <app-button size="sm" variant="secondary" (clicked)="loadScopeTargets(true)">
                      {{ 'datasources.form.retry' | transloco }}
                    </app-button>
                  </div>
                } @else if (formData.scope_mode === 'projects') {
                  <div class="project-picker">
                    <app-input
                      size="sm"
                      [value]="projectSearch()"
                      (valueChange)="onProjectSearch($event)"
                      [placeholder]="'datasources.form.projectsSearch' | transloco"
                      [disabled]="isSaving()"
                    />
                    @if (selectedScopeTargets().length > 0) {
                      <div class="selected-project-chips">
                        @for (project of selectedScopeTargets(); track project.id) {
                          <app-chip
                            size="sm"
                            [selectable]="false"
                            [disabled]="isSaving()"
                            [ariaLabel]="'datasources.form.removeSelectedProject' | transloco: {name: project.name}"
                            (clicked)="toggleScopeProject(project)"
                          >
                            {{ project.name }} <app-icon size="sm">close</app-icon>
                          </app-chip>
                        }
                      </div>
                    }
                    <div class="project-options">
                      @for (project of scopeTargets(); track project.id) {
                        <label class="project-option" [class.retained-only]="project.retained_only">
                          <input
                            type="checkbox"
                            [checked]="formProjectIds().has(project.id)"
                            (change)="toggleScopeProject(project)"
                            [disabled]="isSaving() || (!project.addable && !formProjectIds().has(project.id))"
                          >
                          <span>{{ project.name }}</span>
                          @if (project.retained_only) {
                            <small>{{ 'datasources.form.projectRetainedOnly' | transloco }}</small>
                          }
                        </label>
                      }
                      @if (scopeTargets().length === 0) {
                        <span class="scope-empty">{{ 'datasources.form.projectsEmpty' | transloco }}</span>
                      }
                    </div>
                    @if (scopeTargetsNextCursor()) {
                      <app-button size="sm" variant="ghost" (clicked)="loadMoreScopeTargets()">
                        {{ 'datasources.catalog.loadMore' | transloco }}
                      </app-button>
                    }
                    <span class="scope-count">
                      {{ 'datasources.form.projectsSelected' | transloco: {count: formProjectIds().size} }}
                    </span>
                  </div>
                }

                <label class="auto-attach-option">
                  <input
                    type="checkbox"
                    [checked]="formData.auto_attach"
                    (change)="formData.auto_attach = $any($event.target).checked"
                    [disabled]="isSaving() || scopeTargetsLoading()"
                  >
                  <span>
                    <strong>{{ 'datasources.form.autoAttach' | transloco }}</strong>
                    <small>{{ 'datasources.form.autoAttachHint' | transloco }}</small>
                  </span>
                </label>
                @if (formData.auto_attach && formData.scope_mode) {
                  <div class="impact-summary">
                    {{ (formData.scope_mode === 'all'
                      ? 'datasources.form.autoImpactAll'
                      : 'datasources.form.autoImpactProjects')
                      | transloco: {count: formProjectIds().size} }}
                  </div>
                }
              }
              </section>
            }

            <!-- Visibility (publish) — rendered only with the public_datasources
                 capability; the server enforces regardless. Email is never
                 publishable (the server rejects is_global for mailboxes). -->
            @if (capabilities.canPublishDatasources() && formData.type === 'email') {
              <div class="credential-retain-hint">
                {{ 'datasources.form.emailNotPublishableHint' | transloco }}
              </div>
            }
            @if (capabilities.canPublishDatasources() && formData.type !== 'email' && formData.type !== 'credentials') {
              <div class="form-row">
                <app-form-field
                  [label]="'datasources.form.visibilityLabel' | transloco"
                  [hint]="formData.is_global
                    ? ((formData.type === 'kb'
                        ? 'datasources.form.visibilityKbHint'
                        : 'datasources.form.visibilityCredentialHint') | transloco)
                    : ''"
                >
                  <div class="visibility-controls">
                    <label class="visibility-toggle">
                      <input
                        type="checkbox"
                        [checked]="formData.is_global"
                        (change)="formData.is_global = $any($event.target).checked"
                        [disabled]="isSaving()"
                      >
                      {{ 'datasources.form.visibilityPublic' | transloco }}
                    </label>
                    @if (formData.is_global) {
                      <div class="access-radio">
                        <label>
                          <input type="radio" name="ds-access"
                            [checked]="formData.read_only"
                            (change)="formData.read_only = true"
                            [disabled]="isSaving()">
                          {{ 'datasources.form.accessReadOnly' | transloco }}
                        </label>
                        <label>
                          <input type="radio" name="ds-access"
                            [checked]="!formData.read_only"
                            (change)="formData.read_only = false"
                            [disabled]="isSaving() || formData.type === 'kb'">
                          {{ 'datasources.form.accessReadWrite' | transloco }}
                        </label>
                      </div>
                    }
                  </div>
                </app-form-field>
              </div>
            }
          </div>

          <div class="form-footer-bar">
            <div class="form-actions">
              @if (!isEnvType() && formData.type !== 'repository' && !isCredentialFileType()) {
                <app-button
                  variant="secondary"
                  size="sm"
                  [loading]="isTesting()"
                  [disabled]="!canTestFromForm()"
                  (clicked)="testFromForm()"
                >
                  @if (isTesting()) {
                    {{ 'datasources.form.testing' | transloco }}
                  } @else {
                    <app-icon size="sm">cable</app-icon> {{ 'datasources.form.test' | transloco }}
                  }
                </app-button>
              }
              <app-button
                variant="secondary"
                size="sm"
                [disabled]="isSaving()"
                (clicked)="closeForm()"
              >
                {{ 'datasources.form.cancel' | transloco }}
              </app-button>
              <app-button
                variant="primary"
                size="sm"
                [loading]="isSaving()"
                [disabled]="!canSave()"
                (clicked)="saveForm()"
              >
                @if (isSaving()) {
                  {{ 'datasources.form.saving' | transloco }}
                } @else {
                  {{ (editingId() ? 'datasources.form.update' : 'datasources.form.create') | transloco }}
                }
              </app-button>
            </div>

            @if (formTestResult()) {
              <div
                class="test-result"
                [class.test-ok]="formTestResult()!.status === 'ok'"
                [class.test-error]="formTestResult()!.status === 'error'"
              >
                <app-icon size="sm">{{ formTestResult()!.status === 'ok' ? 'check_circle' : 'error' }}</app-icon>
                {{ formTestResult()!.message }}
              </div>
            }
          </div>
        </div>
      }

      <!-- Loading State -->
      @if (isLoading() && filteredDatasources().length === 0 && !showForm()) {
        <div class="center-state">
          <app-spinner size="lg" tone="accent" />
          <span>{{ 'datasources.table.loading' | transloco }}</span>
        </div>
      }

      <!-- Empty State -->
      @if (!isLoading() && filteredDatasources().length === 0 && !showForm()) {
        <div class="center-state">
          <app-icon size="inherit" class="empty-icon">database</app-icon>
          <span>{{ 'datasources.table.empty' | transloco }}</span>
          <span class="hint">{{ 'datasources.table.emptyHint' | transloco }}</span>
        </div>
      }

      <!-- Table -->
      @if (filteredDatasources().length > 0) {
        <div class="table-container">
          <div class="table-inner">
            <table class="ds-table app-table">
              <thead>
                <tr>
                  <th>{{ 'datasources.table.colType' | transloco }}</th>
                  <th>{{ 'datasources.table.colName' | transloco }}</th>
                  <th class="col-url">{{ 'datasources.table.colUrl' | transloco }}</th>
                  <th class="col-scope">{{ 'datasources.table.colVisibility' | transloco }}</th>
                  @if (capabilities.datasourceScopeAutoAttachAvailable()) {
                    <th class="col-availability">{{ 'datasources.table.colAvailability' | transloco }}</th>
                  }
                  <th>{{ 'datasources.table.colActions' | transloco }}</th>
                </tr>
              </thead>
              <tbody>
                @for (ds of filteredDatasources(); track ds.id) {
                  <tr>
                    <td>
                      <app-badge [tone]="dsTypeTone(ds.type)" size="sm">
                        <app-icon size="sm">{{ getTypeIcon(ds.type) }}</app-icon>
                        {{ 'datasources.filter.' + ds.type | transloco }}
                      </app-badge>
                    </td>
                    <td class="name-cell">
                      <span class="ds-name">{{ ds.name }}</span>
                      @if (ds.description) {
                        <span class="ds-desc">{{ ds.description }}</span>
                      }
                      @if (ds.type === 'kb' && indexStatuses()[ds.id]; as indexStatus) {
                        <div class="index-status-row">
                          <app-badge [tone]="indexStatusTone(indexStatus.status)" size="xs">
                            {{ indexStatusLabel(indexStatus) }}
                          </app-badge>
                          @if (indexStatus.status === 'indexing' && indexStatus.notes_total) {
                            <span
                              class="index-progress"
                              [title]="
                                'datasources.table.indexProgressTitle'
                                  | transloco
                                    : {
                                        done: indexStatus.notes_done ?? 0,
                                        total: indexStatus.notes_total
                                      }
                              "
                            >
                              <span class="index-progress-count">
                                {{ indexStatus.notes_done ?? 0 }}/{{ indexStatus.notes_total }}
                              </span>
                              <span class="index-progress-track">
                                <span
                                  class="index-progress-fill"
                                  [style.width.%]="indexProgressPercent(indexStatus)"
                                ></span>
                              </span>
                            </span>
                          }
                          @if (indexStatus.last_success_at) {
                            <span
                              class="index-status-detail"
                              [title]="indexStatus.last_success_at"
                            >
                              {{ 'datasources.table.lastSuccess' | transloco }}:
                              {{ formatIndexDate(indexStatus.last_success_at) }}
                            </span>
                          }
                          @if (indexStatus.last_error) {
                            <span
                              class="index-status-error"
                              [title]="redactIndexError(indexStatus.last_error)"
                            >
                              {{ redactIndexError(indexStatus.last_error) }}
                            </span>
                          }
                        </div>
                      }
                      @if (viewport.isMobile()) {
                        <app-badge class="ds-scope-inline" [tone]="scopeTone(ds)" size="xs">
                          {{ scopeLabelKey(ds) | transloco }}
                        </app-badge>
                        @if (ds.is_global && ds.read_only === false) {
                          <app-badge class="ds-scope-inline" tone="warning" size="xs">
                            {{ 'datasources.table.badgeRw' | transloco }}
                          </app-badge>
                        }
                        @if (capabilities.datasourceScopeAutoAttachAvailable()) {
                          <app-badge class="ds-scope-inline" [tone]="availabilityTone(ds)" size="xs">
                            {{ availabilityLabel(ds) }}
                          </app-badge>
                          @if (isOwnedByCurrentUser(ds) && ds.auto_attach) {
                            <app-badge class="ds-scope-inline" tone="accent" size="xs">
                              {{ 'datasources.table.badgeAuto' | transloco }}
                            </app-badge>
                          }
                        }
                      }
                    </td>
                    <td class="url-cell mono col-url">{{ ds.connection_url ? maskUrl(ds.connection_url) : '—' }}</td>
                    <td class="col-scope">
                      <app-badge [tone]="scopeTone(ds)" size="xs">
                        {{ scopeLabelKey(ds) | transloco }}
                      </app-badge>
                      @if (ds.is_global && ds.read_only === false) {
                        <app-badge tone="warning" size="xs">
                          {{ 'datasources.table.badgeRw' | transloco }}
                        </app-badge>
                      }
                    </td>
                    @if (capabilities.datasourceScopeAutoAttachAvailable()) {
                      <td class="col-availability">
                        <app-badge [tone]="availabilityTone(ds)" size="xs">
                          {{ availabilityLabel(ds) }}
                        </app-badge>
                        @if (isOwnedByCurrentUser(ds) && ds.auto_attach) {
                          <app-badge tone="accent" size="xs">
                            {{ 'datasources.table.badgeAuto' | transloco }}
                          </app-badge>
                        }
                      </td>
                    }
                    <td class="actions-cell">
                      @if (canManage(ds) && viewport.isMobile()) {
                        <!-- Mobile: collapse the row actions into a ⋯ overflow menu so the
                             cell is just the kebab and the table fits without h-scroll
                             (mirrors the Jobs list). -->
                        <app-icon-button
                          variant="ghost"
                          size="sm"
                          [ariaLabel]="'datasources.table.moreActions' | transloco"
                          [loading]="testingIds().has(ds.id) || reindexingIds().has(ds.id)"
                          [appMenuTrigger]="rowMenu"
                          menuPlacement="bottom-end"
                        >
                          <app-icon size="sm">more_vert</app-icon>
                        </app-icon-button>
                        <app-menu #rowMenu>
                          <app-menu-item (activated)="testDatasource(ds.id)">
                            {{ 'datasources.table.testTooltip' | transloco }}
                          </app-menu-item>
                          @if (ds.type === 'kb') {
                            <app-menu-item (activated)="reindexDatasource(ds)">
                              {{ 'datasources.table.reindexTooltip' | transloco }}
                            </app-menu-item>
                          }
                          @if (ds.type === 'kb') {
                            <app-menu-item (activated)="reindexDatasource(ds, true)">
                              {{ 'datasources.table.fullReindexTooltip' | transloco }}
                            </app-menu-item>
                          }
                          <app-menu-item (activated)="openEditForm(ds)">
                            {{ 'datasources.table.editTooltip' | transloco }}
                          </app-menu-item>
                          <app-menu-item tone="danger" (activated)="deleteDatasource(ds)">
                            {{ 'datasources.table.deleteTooltip' | transloco }}
                          </app-menu-item>
                        </app-menu>
                      } @else if (canManage(ds)) {
                        <app-icon-button
                          variant="ghost"
                          size="sm"
                          [ariaLabel]="'datasources.table.testTooltip' | transloco"
                          [tooltip]="'datasources.table.testTooltip' | transloco"
                          [loading]="testingIds().has(ds.id)"
                          (clicked)="testDatasource(ds.id)"
                        >
                          <app-icon size="sm">cable</app-icon>
                        </app-icon-button>
                        @if (ds.type === 'kb') {
                          <app-icon-button
                            variant="ghost"
                            size="sm"
                            [ariaLabel]="'datasources.table.reindexTooltip' | transloco"
                            [tooltip]="'datasources.table.reindexTooltip' | transloco"
                            [loading]="reindexingIds().has(ds.id)"
                            (clicked)="reindexDatasource(ds)"
                          >
                            <app-icon size="sm">sync</app-icon>
                          </app-icon-button>
                          <app-icon-button
                            variant="ghost"
                            size="sm"
                            [ariaLabel]="'datasources.table.fullReindexTooltip' | transloco"
                            [tooltip]="'datasources.table.fullReindexTooltip' | transloco"
                            [disabled]="reindexingIds().has(ds.id)"
                            (clicked)="reindexDatasource(ds, true)"
                          >
                            <app-icon size="sm">restart_alt</app-icon>
                          </app-icon-button>
                        }
                        <app-icon-button
                          variant="ghost"
                          size="sm"
                          [ariaLabel]="'datasources.table.editTooltip' | transloco"
                          [tooltip]="'datasources.table.editTooltip' | transloco"
                          (clicked)="openEditForm(ds)"
                        >
                          <app-icon size="sm">edit</app-icon>
                        </app-icon-button>
                        <app-icon-button
                          variant="danger"
                          size="sm"
                          [ariaLabel]="'datasources.table.deleteTooltip' | transloco"
                          [tooltip]="'datasources.table.deleteTooltip' | transloco"
                          (clicked)="deleteDatasource(ds)"
                        >
                          <app-icon size="sm">delete</app-icon>
                        </app-icon-button>
                      }

                      @if (canManage(ds) && testResults()[ds.id]; as result) {
                        <span
                          class="inline-test"
                          [class.test-ok]="result.status === 'ok'"
                          [class.test-error]="result.status === 'error'"
                          title="{{ result.message }}"
                        >
                          <app-icon size="sm">{{ result.status === 'ok' ? 'check_circle' : 'error' }}</app-icon>
                        </span>
                      }
                    </td>
                  </tr>
                }
              </tbody>
            </table>
            @if (capabilities.datasourceScopeAutoAttachAvailable() && catalogNextCursor()) {
              <div class="load-more-row">
                <app-button variant="secondary" size="sm" [loading]="isLoadingMore()" (clicked)="loadMoreCatalog()">
                  {{ 'datasources.catalog.loadMore' | transloco }}
                </app-button>
              </div>
            }
          </div>
        </div>
      }
    </div>

    <!-- Publish confirmation (two tiers: warn / type-the-name) -->
    <app-confirm-name-dialog
      [open]="showPublishConfirm()"
      [title]="'datasources.publishDialog.title' | transloco"
      [message]="(publishConfirmName() !== null
        ? 'datasources.publishDialog.rwMessage'
        : 'datasources.publishDialog.message') | transloco"
      [requiredName]="publishConfirmName()"
      [namePrompt]="'datasources.publishDialog.namePrompt' | transloco"
      [confirmLabel]="'datasources.publishDialog.confirm' | transloco"
      [cancelLabel]="'datasources.publishDialog.cancel' | transloco"
      (confirmed)="onPublishConfirmed()"
      (dismissed)="showPublishConfirm.set(false)"
    />

    <!-- Generated SSH public-key dialog -->
    <app-dialog
      [open]="showPublicKeyDialog()"
      size="md"
      [title]="'datasources.sshKeyDialog.title' | transloco"
      (closed)="closePublicKeyDialog()"
    >
      <p class="public-key-intro">
        {{ 'datasources.sshKeyDialog.intro' | transloco }}
      </p>
      <div class="public-key-wrap">
        <pre class="public-key-block">{{ generatedPublicKey() }}</pre>
        <app-icon-button
          class="public-key-copy"
          size="sm"
          variant="ghost"
          [ariaLabel]="
            (publicKeyCopied()
              ? 'datasources.sshKeyDialog.copied'
              : 'datasources.sshKeyDialog.copy'
            ) | transloco
          "
          [tooltip]="
            (publicKeyCopied()
              ? 'datasources.sshKeyDialog.copied'
              : 'datasources.sshKeyDialog.copy'
            ) | transloco
          "
          (clicked)="copyPublicKey()"
        >
          <app-icon size="sm">{{
            publicKeyCopied() ? 'check' : 'content_copy'
          }}</app-icon>
        </app-icon-button>
      </div>
      <p class="public-key-hint">
        {{ 'datasources.sshKeyDialog.privateNote' | transloco }}
      </p>
      <ng-container appDialogActions>
        <app-button
          size="sm"
          [variant]="publicKeyCopied() ? 'success' : 'primary'"
          type="button"
          (clicked)="copyPublicKey()"
        >
          {{
            (publicKeyCopied()
              ? 'datasources.sshKeyDialog.copied'
              : 'datasources.sshKeyDialog.copy'
            ) | transloco
          }}
        </app-button>
        <app-button
          size="sm"
          variant="secondary"
          type="button"
          (clicked)="closePublicKeyDialog()"
        >
          {{ 'datasources.sshKeyDialog.done' | transloco }}
        </app-button>
      </ng-container>
    </app-dialog>
  `,
  styles: [
    `
      :host {
        display: block;
        height: 100%;
        overflow: hidden;
      }

      .ds-container {
        display: flex;
        flex-direction: column;
        height: 100%;
        background: var(--panel-bg, var(--panel-bg));
      }

      /* Header */
      .header-bar {
        display: flex;
        align-items: center;
        gap: 12px;
        padding: 10px 12px;
        background: var(--panel-header-bg);
        border-bottom: 1px solid var(--border-color, var(--surface-0));
        flex-shrink: 0;
        flex-wrap: wrap;
      }

      .title {
        font-weight: 600;
        color: var(--text-primary, var(--text-primary));
      }

      /* Thirteen type chips never fit beside the title, so they take their
         own row (order + full basis) and the actions stay on the title row —
         the same shape the Jobs header uses. */
      .filter-chips {
        display: flex;
        gap: 4px;
        flex-wrap: wrap;
        order: 1;
        flex-basis: 100%;
        min-width: 0;
      }

      .header-actions {
        display: flex;
        gap: 6px;
        margin-left: auto;
        align-items: center;
      }

      .catalog-filter-bar {
        display: grid;
        grid-template-columns: minmax(220px, 1fr) minmax(300px, 1.2fr) repeat(4, minmax(130px, auto));
        gap: 8px;
        padding: 8px 12px;
        border-bottom: 1px solid var(--border-color, var(--surface-0));
        background: var(--panel-header-bg);
        flex-shrink: 0;
      }

      .catalog-project-filter {
        display: flex;
        align-items: center;
        gap: 6px;
        min-width: 0;
      }

      .catalog-project-filter app-input,
      .catalog-project-filter app-select { min-width: 0; flex: 1 1 0; }

      /* Messages */
      .msg {
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 8px 12px;
        margin: 8px 12px 0;
        border-radius: var(--radius-control);
        font-size: 12px;
        flex-shrink: 0;
      }

      .success-msg {
        background: var(--success-tint);
        border: 1px solid var(--success-tint);
        color: var(--success);
      }

      .error-msg {
        background: var(--danger-tint);
        border: 1px solid var(--danger-tint);
        color: var(--danger);
      }

      /* Form Panel
         The panel is a self-contained scroll region rather than a flex-shrink:0
         block: :host and .page-content both clip, so a form taller than the
         viewport (OKF/repository types with the project picker expanded) used to
         be unreachable — no scrollbar anywhere. flex:0 1 auto + min-height:0 lets
         the panel take only the space left over and hand the overflow to
         .form-body, keeping the title and the Test/Cancel/Create bar pinned. */
      .form-panel {
        margin: 8px 12px 0;
        border: 1px solid var(--border-color, var(--surface-1));
        border-radius: var(--radius-surface);
        background: rgba(0, 0, 0, 0.2);
        display: flex;
        flex-direction: column;
        flex: 0 1 auto;
        min-height: 0;
      }

      .form-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 10px 14px;
        border-bottom: 1px solid var(--border-color, var(--surface-0));
        font-weight: 600;
        font-size: 13px;
        color: var(--text-primary, var(--text-primary));
        flex-shrink: 0;
      }

      .form-body {
        padding: 14px;
        overflow-y: auto;
        min-height: 0;
      }

      .form-row {
        display: flex;
        gap: 12px;
        overflow: hidden;
      }

      .flex-1 {
        flex: 1;
      }

      app-input.mono,
      app-textarea.mono {
        font-family: 'JetBrains Mono', monospace;
      }

      .form-row > app-form-field:not(.flex-1) {
        min-width: 0;
        max-width: 160px;
      }

      /* Pinned action bar — stays visible while .form-body scrolls. */
      .form-footer-bar {
        flex-shrink: 0;
        padding: 10px 14px;
        border-top: 1px solid var(--border-color, var(--surface-0));
        border-radius: 0 0 var(--radius-surface) var(--radius-surface);
        background: var(--panel-header-bg);
      }

      .form-footer-bar .form-actions {
        justify-content: flex-end;
      }

      .hint-inline {
        font-weight: 400;
        color: var(--text-muted);
        font-size: 11px;
      }

      .form-hint {
        margin-top: 6px;
        padding: 8px 10px;
        background: var(--info-tint);
        border: 1px solid var(--info-tint);
        border-radius: var(--radius-tag);
        font-size: 11px;
        color: var(--text-secondary, var(--text-secondary));
        line-height: 1.5;
      }

      .credential-retain-hint {
        margin-top: 4px;
        font-size: 11px;
        color: var(--text-muted);
      }

      /* Env var editor */
      .env-vars-editor {
        display: flex;
        flex-direction: column;
        gap: 6px;
      }

      .env-var-row {
        display: flex;
        align-items: center;
        gap: 6px;
      }

      app-input.env-key {
        flex: 0 0 200px;
      }

      app-input.env-key ::ng-deep input {
        text-transform: uppercase;
      }

      .env-eq {
        color: var(--text-muted);
        font-family: 'JetBrains Mono', monospace;
        font-size: 12px;
      }

      app-input.env-val {
        flex: 1;
      }

      .btn-add-env {
        align-self: flex-start;
      }

      .toggle-label {
        display: flex;
        align-items: center;
        gap: 6px;
        font-size: 12px;
        color: var(--text-primary, var(--text-primary));
        cursor: pointer;
      }

      .form-actions {
        display: flex;
        gap: 8px;
        align-items: center;
      }

      /* SSH key generation */
      .ssh-key-actions {
        display: flex;
        align-items: center;
        gap: 8px;
        margin-top: 6px;
        flex-wrap: wrap;
      }

      .ssh-key-actions .confirm-text {
        font-size: 12px;
        color: var(--warning, var(--text-primary));
      }

      /* Credential-file actions row (upload, generate) */
      .cred-file-actions {
        display: flex;
        align-items: center;
        gap: 8px;
        margin-top: 6px;
        flex-wrap: wrap;
      }

      /* Posture-1 trust notice — warning-tinted, slightly more prominent
         than .form-hint because it carries security-relevant copy. */
      .trust-notice {
        display: flex;
        align-items: flex-start;
        gap: 8px;
        margin-top: 8px;
        padding: 10px 12px;
        background: var(--warning-tint, var(--info-tint));
        border: 1px solid var(--warning-border, var(--info-tint));
        border-radius: var(--radius-tag);
        font-size: 12px;
        line-height: 1.5;
        color: var(--text-primary, var(--text-primary));
      }

      .trust-notice app-icon {
        flex-shrink: 0;
        margin-top: 1px;
        color: var(--warning, var(--text-primary));
      }

      /* Visibility (publish) section */
      .visibility-controls {
        display: flex;
        flex-direction: column;
        gap: 8px;
      }

      .visibility-toggle,
      .access-radio label {
        display: inline-flex;
        align-items: center;
        gap: 8px;
        cursor: pointer;
        font-size: 13px;
        color: var(--text-primary);
      }

      .access-radio {
        display: flex;
        gap: 20px;
      }

      .availability-section {
        display: flex;
        flex-direction: column;
        gap: 10px;
        margin: 12px 0;
        padding: 12px;
        border: 1px solid var(--border-color, var(--surface-1));
        border-radius: var(--radius-surface);
        background: var(--surface-0, transparent);
      }

      .availability-heading {
        font-size: 12px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        color: var(--text-muted);
      }

      .availability-option,
      .auto-attach-option {
        display: flex;
        align-items: flex-start;
        gap: 9px;
        cursor: pointer;
        color: var(--text-primary);
      }

      .availability-option span,
      .auto-attach-option span,
      .native-policy span {
        display: flex;
        flex-direction: column;
        gap: 2px;
      }

      .availability-option strong,
      .auto-attach-option strong {
        font-size: 13px;
        font-weight: 500;
      }

      .availability-option small,
      .auto-attach-option small,
      .native-policy small,
      .scope-count,
      .scope-empty,
      .project-option small {
        font-size: 11px;
        color: var(--text-muted);
      }

      .project-picker {
        display: flex;
        flex-direction: column;
        gap: 8px;
        margin-left: 24px;
      }

      .project-options {
        max-height: 180px;
        overflow: auto;
        display: flex;
        flex-direction: column;
        gap: 3px;
      }

      .project-option {
        display: flex;
        align-items: center;
        gap: 8px;
        padding: 6px 8px;
        border-radius: var(--radius-control);
        font-size: 12px;
        cursor: pointer;
      }

      .project-option:hover { background: rgba(255, 255, 255, 0.03); }
      .project-option.retained-only { color: var(--warning); }
      .project-option small { margin-left: auto; }

      .selected-project-chips {
        display: flex;
        flex-wrap: wrap;
        gap: 5px;
      }

      .selected-project-chips app-chip app-icon {
        margin-left: 3px;
      }

      .native-policy {
        display: flex;
        align-items: flex-start;
        gap: 9px;
        padding: 8px 10px;
        border-radius: var(--radius-control);
        background: var(--info-tint);
        color: var(--text-primary);
        font-size: 13px;
      }

      .scope-load-state,
      .scope-error {
        display: flex;
        align-items: center;
        gap: 8px;
        padding: 8px 10px;
        font-size: 12px;
      }

      .scope-error {
        justify-content: space-between;
        color: var(--danger);
        background: var(--danger-tint);
        border-radius: var(--radius-control);
      }

      .impact-summary {
        margin-left: 24px;
        padding: 7px 9px;
        border-radius: var(--radius-control);
        background: var(--info-tint);
        color: var(--text-secondary);
        font-size: 11px;
      }

      /* Repeatable file editor for generic_file type */
      .generic-files-editor {
        display: flex;
        flex-direction: column;
        gap: 10px;
      }

      .generic-file-card {
        padding: 10px;
        border: 1px solid var(--border-color, var(--surface-0));
        border-radius: var(--radius-tag);
        background: var(--surface-1, transparent);
        display: flex;
        flex-direction: column;
        gap: 8px;
      }

      .generic-file-card-actions {
        display: flex;
        align-items: center;
        gap: 6px;
        align-self: flex-end;
      }

      .public-key-intro,
      .public-key-hint {
        margin: 0 0 10px;
        font-size: 13px;
        color: var(--text-primary, var(--text-primary));
      }

      .public-key-hint {
        margin-top: 10px;
        margin-bottom: 0;
        color: var(--text-muted);
      }

      .public-key-wrap {
        position: relative;
      }

      .public-key-block {
        font-family: 'JetBrains Mono', monospace;
        font-size: 12px;
        line-height: 1.4;
        padding: 10px 36px 10px 12px;
        margin: 0;
        background: rgba(0, 0, 0, 0.25);
        border: 1px solid var(--border-color, var(--surface-1));
        border-radius: var(--radius-tag);
        white-space: pre-wrap;
        word-break: break-all;
      }

      .public-key-copy {
        position: absolute;
        top: 4px;
        right: 4px;
      }

      .test-result {
        display: flex;
        align-items: center;
        gap: 6px;
        padding: 8px 12px;
        margin-top: 8px;
        border-radius: var(--radius-control);
        font-size: 12px;
      }

      .test-ok {
        background: var(--success-tint);
        color: var(--success);
      }

      .test-error {
        background: var(--danger-tint);
        color: var(--danger);
      }

      /* Center States */
      .center-state {
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        gap: 10px;
        padding: 40px;
        flex: 1;
        color: var(--text-muted);
        font-size: 13px;
      }

      .empty-icon {
        font-size: 48px !important;
        opacity: 0.4;
      }

      .hint {
        font-size: 11px;
        opacity: 0.7;
      }

      /* Table
         Padding lives on .table-inner, not here: .table-container is the flex
         item that gets squeezed to nothing when an open form claims the column,
         and padding on the item itself can't be squeezed — it would leave a 16px
         strip of clipped table header pinned to the bottom edge. Inside the
         scroll box it just gets clipped along with the rows. Same reason the
         load-more row sits inside rather than beside it. */
      .table-container {
        flex: 1;
        overflow: auto;
        min-height: 0;
      }

      .table-inner {
        padding: 8px;
      }

      .load-more-row {
        display: flex;
        justify-content: center;
        padding: 8px 12px 4px;
      }

      app-badge app-icon {
        margin-right: 4px;
      }

      /* Name cell */
      .name-cell {
        max-width: 200px;
      }

      .ds-name {
        display: block;
        font-weight: 500;
      }

      .ds-desc {
        display: block;
        font-size: 10px;
        color: var(--text-muted);
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
        max-width: 200px;
      }

      .index-status-row {
        display: flex;
        align-items: center;
        gap: 6px;
        margin-top: 5px;
        max-width: 360px;
      }

      .index-status-detail,
      .index-status-error {
        min-width: 0;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        font-size: 10px;
      }

      .index-status-detail {
        color: var(--text-muted);
      }

      .index-status-error {
        color: var(--danger);
      }

      .index-progress {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        font-size: 10px;
        color: var(--text-muted);
      }

      .index-progress-count {
        font-variant-numeric: tabular-nums;
        white-space: nowrap;
      }

      .index-progress-track {
        position: relative;
        width: 64px;
        height: 4px;
        border-radius: var(--radius-tag);
        background: var(--surface-3, rgba(127, 127, 127, 0.25));
        overflow: hidden;
      }

      .index-progress-fill {
        position: absolute;
        inset: 0 auto 0 0;
        height: 100%;
        border-radius: var(--radius-tag);
        background: var(--info, #3b82f6);
        transition: width 0.3s ease;
      }

      /* URL cell */
      .url-cell {
        font-family: 'JetBrains Mono', monospace;
        font-size: 11px;
        color: var(--text-secondary, var(--text-secondary));
        max-width: 220px;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
      }

      .mono {
        font-family: 'JetBrains Mono', monospace;
      }

      /* Action Buttons */
      .actions-cell {
        white-space: nowrap;
        text-align: right;
      }

      .actions-cell app-icon-button,
      .actions-cell .inline-test {
        vertical-align: middle;
      }

      .actions-cell app-icon-button + app-icon-button {
        margin-left: 2px;
      }

      .inline-test {
        margin-left: 4px;
      }

      .inline-test.test-ok { color: var(--success); }
      .inline-test.test-error { color: var(--danger); }

      /* ===== Mobile (≤768px) =====
         This page never had a responsive pass. Mirror the Jobs header (a single
         horizontally-scrollable chip strip with a right edge-fade) and shrink the
         table so it fits without horizontal scroll (the row actions collapse into a
         ⋯ overflow menu in the template). 768px matches ViewportService.isMobile(). */
      @media (max-width: 768px) {
        .catalog-filter-bar {
          grid-template-columns: 1fr 1fr;
        }

        /* The browse controls cost ~215px of an 800px phone viewport (a 165px
           3-row filter grid + the 32px chip strip), which came straight out of
           the form's scroll window. They filter a list the open form has already
           squeezed to nothing, so drop them while authoring and give the height
           to the fields. Everything returns when the form closes. */
        .form-open .catalog-filter-bar,
        .form-open .filter-chips {
          display: none;
        }
        /* Filter chips: scroll sideways instead of overflowing 599px and getting
           clipped by :host{overflow:hidden}. order:1 + flex-basis:100% drops the
           strip onto its own row below the title + actions. */
        .filter-chips {
          order: 1;
          flex-basis: 100%;
          min-width: 0;
          flex-wrap: nowrap;
          overflow-x: auto;
          -webkit-overflow-scrolling: touch;
          scrollbar-width: none;
          padding-bottom: 2px;
          -webkit-mask-image: linear-gradient(to right, #000 calc(100% - 24px), transparent);
          mask-image: linear-gradient(to right, #000 calc(100% - 24px), transparent);
        }

        .filter-chips::-webkit-scrollbar {
          display: none;
        }

        .filter-chips app-chip {
          flex-shrink: 0;
        }

        /* Trailing space so the last chip clears the fade when scrolled fully right. */
        .filter-chips app-chip:last-child {
          margin-right: 28px;
        }

        /* Compact the chips for the scroll strip (the global mobile rule gives
           selectable chips a chunky 44px target). Scoped local to this strip. */
        .filter-chips ::ng-deep .app-chip__btn[data-selectable] {
          min-height: 0;
          height: 30px;
          padding: 0 7px;
          font-size: 10px;
        }

        /* Drop the low-value (masked + truncated) URL column, and the Scope column
           (scope is folded into the name cell as a badge below) so Type / Name +
           the kebab fit without horizontal scroll. URL stays editable in the form. */
        .col-url,
        .col-scope,
        .col-availability {
          display: none;
        }

        /* Fixed layout so the three remaining columns share the phone width:
           under auto layout the type badge alone took 183px and pushed the
           actions column (the kebab) off-screen (measured 461px in 390px). */
        .ds-table {
          table-layout: fixed;
        }

        .ds-table th:first-child,
        .ds-table td:first-child {
          width: 32%;
        }

        .ds-table th:last-child,
        .ds-table td:last-child {
          width: 48px;
          padding-inline: 4px;
        }

        /* A lone kebab column: the "Actions" label doesn't fit 48px and clipped
           at the screen edge (same trick as the jobs table; the text stays in
           the accessibility tree). */
        .ds-table th:last-child {
          font-size: 0;
        }

        /* The type badge wraps to a second line in its narrower column
           (same treatment the project cards give their badges). */
        .ds-table td:first-child app-badge {
          white-space: normal;
          height: auto;
          min-height: 20px;
          padding-block: 2px;
          line-height: 1.25;
        }

        /* Scope shown inline under the name on mobile (its own column is hidden). */
        .ds-scope-inline {
          margin-top: 4px;
        }

        /* Env-var editor: let the key shrink so the value field isn't squeezed
           to ~68px next to the fixed 200px key. */
        app-input.env-key {
          flex: 1 1 0;
          min-width: 0;
        }

        /* Generic-file card rows wrap so the Upload / remove actions drop to their
           own line instead of crushing the Env-var field. */
        .generic-file-card .form-row {
          flex-wrap: wrap;
        }
      }
    `,
  ],
})
export class DatasourceListComponent implements OnInit {
  private readonly api = inject(ApiService);
  private readonly transloco = inject(TranslocoService);
  private readonly userService = inject(UserService);
  private readonly route = inject(ActivatedRoute, {optional: true});
  protected readonly viewport = inject(ViewportService);
  protected readonly capabilities = inject(CapabilitiesService);
  private readonly destroyRef = inject(DestroyRef);
  /** Poll cadence for a KB that is still indexing. */
  private readonly INDEX_POLL_MS = 5000;

  // State signals
  readonly datasources = signal<Datasource[]>([]);
  readonly isLoading = signal(false);
  readonly typeFilter = signal<string>('all');
  readonly catalogSearch = signal('');
  readonly scopeFilter = signal<'all' | 'scope-all' | 'projects' | 'unavailable'>('all');
  readonly automaticFilter = signal<'all' | 'automatic' | 'manual'>('all');
  readonly ownershipFilter = signal<'all' | 'mine' | 'shared'>('all');
  readonly visibilityFilter = signal<'all' | 'private' | 'public'>('all');
  readonly catalogProjectId = signal('');
  readonly catalogProjectSearch = signal('');
  readonly catalogProjects = signal<LinkableDatasourceProject[]>([]);
  readonly catalogProjectsLoading = signal(false);
  readonly catalogProjectsError = signal(false);
  readonly catalogProjectsNextCursor = signal<string | null>(null);
  readonly catalogNextCursor = signal<string | null>(null);
  readonly isLoadingMore = signal(false);
  private catalogRequestSerial = 0;
  private catalogSearchTimer: ReturnType<typeof setTimeout> | null = null;
  private catalogProjectSearchTimer: ReturnType<typeof setTimeout> | null = null;
  private catalogProjectsRequestSerial = 0;
  readonly showForm = signal(false);
  readonly editingId = signal<string | null>(null);
  /** Original datasource in edit mode — publishConfirmTier compares against it. */
  private editingOriginal: Datasource | null = null;
  readonly showPublishConfirm = signal(false);
  /** Name to type in the confirm dialog; null = warn tier (no input). */
  readonly publishConfirmName = signal<string | null>(null);
  readonly testResults = signal<Record<string, DatasourceTestResult>>({});
  readonly testingIds = signal<Set<string>>(new Set());
  readonly indexStatuses = signal<Record<string, DatasourceIndexStatus>>({});
  readonly reindexingIds = signal<Set<string>>(new Set());
  readonly isTesting = signal(false);
  readonly isSaving = signal(false);
  readonly formTestResult = signal<DatasourceTestResult | null>(null);
  readonly successMessage = signal<string | null>(null);
  readonly errorMessage = signal<string | null>(null);

  // Shared availability-policy form state.
  readonly scopeTargets = signal<LinkableDatasourceProject[]>([]);
  readonly scopeTargetsLoading = signal(false);
  readonly scopeTargetsError = signal(false);
  readonly scopeTargetsNextCursor = signal<string | null>(null);
  readonly projectSearch = signal('');
  readonly formProjectIds = signal<Set<string>>(new Set());
  readonly selectedScopeTargets = computed<LinkableDatasourceProject[]>(() => {
    const targets = new Map(this.scopeTargets().map(project => [project.id, project]));
    return [...this.formProjectIds()]
      .map(id => targets.get(id) ?? {
        id,
        // A selected link can fall outside the current search/page. Its UUID
        // is still an authorized management value returned by the catalog and
        // remains removable without silently dropping it from the full set.
        name: id,
        user_role: null,
        addable: false,
        retained_only: true,
        linked: true,
      })
      .sort((left, right) => left.name.localeCompare(right.name));
  });
  private scopeTargetsRequestSerial = 0;
  private contextScopeTargetRequestSerial = 0;
  private projectSearchTimer: ReturnType<typeof setTimeout> | null = null;
  private scopeSelectionInitialized = false;

  // SSH key generation state
  readonly isGeneratingKey = signal(false);
  readonly showGenerateConfirm = signal(false);
  readonly showPublicKeyDialog = signal(false);
  readonly generatedPublicKey = signal<string>('');
  readonly publicKeyCopied = signal(false);
  private copiedResetTimer: ReturnType<typeof setTimeout> | null = null;

  // Filter options
  readonly typeFilters = [
    { labelKey: 'datasources.filter.all', value: 'all' },
    { labelKey: 'datasources.filter.credentials', value: 'credentials' },
    { labelKey: 'datasources.filter.generic', value: 'generic' },
    { labelKey: 'datasources.filter.repository', value: 'repository' },
    { labelKey: 'datasources.filter.kb', value: 'kb' },
    { labelKey: 'datasources.filter.postgresql', value: 'postgresql' },
    { labelKey: 'datasources.filter.neo4j', value: 'neo4j' },
    { labelKey: 'datasources.filter.mongodb', value: 'mongodb' },
    { labelKey: 'datasources.filter.webdav', value: 'webdav' },
    { labelKey: 'datasources.filter.email', value: 'email' },
    { labelKey: 'datasources.filter.mcp', value: 'mcp' },
    { labelKey: 'datasources.filter.kubeconfig', value: 'kubeconfig' },
    { labelKey: 'datasources.filter.ssh_key', value: 'ssh_key' },
    { labelKey: 'datasources.filter.generic_file', value: 'generic_file' },
  ];

  // Types whose credentials are materialized as files on the agent (rather
  // than env vars or live connections). No connection URL, no test button.
  readonly credentialFileTypes: DatasourceType[] = ['kubeconfig', 'ssh_key', 'generic_file'];

  isEnvType(): boolean {
    return this.formData.type === 'generic' || this.formData.type === 'credentials';
  }

  isCredentialFileType(type: DatasourceType | string = this.formData.type): boolean {
    return this.credentialFileTypes.includes(type as DatasourceType);
  }

  /** True for types that connect to something with a URL (everything except
   *  credential-files and email, whose endpoints live in credentials.imap/smtp). */
  hasConnectionUrl(): boolean {
    return (
      !this.isCredentialFileType() &&
      this.formData.type !== 'email' &&
      !(this.formData.type === 'mcp' && this.formData.mcpTransport === 'stdio')
    );
  }

  isGitBackedType(type: DatasourceType | string = this.formData.type): boolean {
    return type === 'repository' || type === 'kb';
  }

  /** Which token-permission disclaimer applies, if any.

   *  A PAT with too few permissions fails at first use — inside an agent run,
   *  as a forge error nobody is watching for — so the form says up front what
   *  to grant. Empty when no token is being collected (SSH auth, or a type
   *  that has no repository behind it). */
  patScopeHintKey(): string {
    if (!this.isGitBackedType() || this.gitAuthMethod !== 'token') return '';
    return this.formData.type === 'kb'
      ? 'datasources.form.patScopesKb'
      : 'datasources.form.patScopesRepo';
  }

  /** A redacted REST URL represents an existing usable secret-bearing URL,
   * even when the sanitized display value is empty. */
  private hasConnectionUrlForSave(): boolean {
    return !!this.formData.connection_url.trim() || !!(
      this.editingId() && this.editingOriginal?.connection_url_redacted &&
      !this.connectionUrlDirty
    );
  }

  // Computed filtered list
  readonly filteredDatasources = computed(() => {
    const filter = this.typeFilter();
    const all = this.datasources();
    if (filter === 'all') return all;
    return all.filter((ds) => ds.type === filter);
  });

  // URL placeholder based on type
  readonly urlPlaceholder = computed(() => {
    const placeholders: Record<string, string> = {
      generic: 'e.g. postgresql://host:5432/db or https://api.example.com',
      repository: 'https://github.com/org/repo.git',
      kb: 'https://github.com/org/knowledge-base.git',
      postgresql: 'postgres://user:pass@host:5432/dbname',
      neo4j: 'bolt://host:7687',
      mongodb: 'mongodb://user:pass@host:27017/dbname',
      webdav: 'http://host:8800/remote.php/dav/files/user/',
      mcp: 'https://mcp.example.com/mcp',
    };
    return placeholders[this.formData.type] || '';
  });

  // Credential-file form state. Reused for editing too: when the user opens
  // an edit form, the contents stay blank (credentials never come back from
  // the API), so the user re-pastes if they want to replace the stored value.
  kubeconfigContent = '';
  genericFiles: Array<{
    name: string;
    target_path: string;
    contents: string;
    mode: string;
    env_var: string;
  }> = [];

  // Whether the form can be saved (method, not computed, because formData is a plain object)
  canSave(): boolean {
    if (!this.formData.name) return false;
    if (this.capabilities.datasourceScopeAutoAttachAvailable()) {
      if (this.scopeTargetsLoading() || this.scopeTargetsError()) return false;
      if (this.formData.scope_mode === null) return false;
      if (this.formData.scope_mode === 'projects' && this.formProjectIds().size === 0) {
        return false;
      }
    }
    if (this.formData.type === 'credentials') {
      return this.editingId() !== null || this.envVars.some(row => row.key.trim() && row.value);
    }
    if (this.formData.type === 'generic') {
      return !!(this.formData.description);
    }
    if (this.formData.type === 'kubeconfig') {
      // On edit, leaving contents blank means "keep existing".
      return this.editingId() !== null || !!this.kubeconfigContent.trim();
    }
    if (this.formData.type === 'ssh_key') {
      return this.editingId() !== null || !!this.gitSshKey.trim();
    }
    if (this.formData.type === 'generic_file') {
      if (this.editingId() !== null) return true;
      // At least one file with both target_path and contents.
      return this.genericFiles.some(
        (f) => f.target_path.trim() && f.contents.length > 0,
      );
    }
    if (this.formData.type === 'email') {
      // Editing without touching credentials means "keep existing" — the
      // stored imap/smtp endpoints live inside the (redacted) credentials.
      const wantsCredentialUpdate =
        !!this.formCredentials.username || !!this.formCredentials.password;
      if (this.editingId() !== null && !wantsCredentialUpdate) return true;
      return !!(
        this.formCredentials.username &&
        this.formCredentials.password &&
        this.emailForm.imap_host.trim() &&
        (this.emailForm.access !== 'send' || this.emailForm.smtp_host.trim())
      );
    }
    if (this.formData.type === 'mcp') {
      // Existing credentials are redacted by the API; an untouched edit keeps
      // the stored connection and remains testable.
      if (this.editingId() !== null && !this.mcpCredentialsWereEntered()) {
        return true;
      }
      if (this.formData.mcpTransport === 'stdio') {
        return !!this.formData.mcpCommand.trim();
      }
      return !!this.formData.connection_url.trim();
    }
    if (this.formData.type === 'kb' && this.isNativeProjectConnector()) {
      // The native project vault is not a remote repository and deliberately
      // has no connection URL. Its ordinary metadata/root-path fields remain
      // editable while the policy block stays project-managed.
      return true;
    }
    if (this.formData.type === 'repository') {
      // forge is required — self-hosted hosts don't default it, so an empty
      // selection must block save rather than let the server 400.
      return this.hasConnectionUrlForSave() && !!this.formData.forge;
    }
    return this.hasConnectionUrlForSave();
  }

  canTestFromForm(): boolean {
    return this.canSave();
  }

  // Form data (mutable object, not a signal, matching job-create pattern)
  formData: {
    name: string;
    type: DatasourceType;
    connection_url: string;
    description: string;
    cli_hint: string;
    default_branch: string;
    root_path: string;
    /** Repository only; '' means "not yet chosen" (blocks canSave()). */
    forge: RepositoryForge | '';
    mcpTransport: McpTransport;
    mcpToken: string;
    mcpHeaders: KeyValueRow[];
    mcpCommand: string;
    mcpArgs: string;
    mcpEnv: KeyValueRow[];
    is_global: boolean;
    read_only: boolean;
    scope_mode: DatasourceScopeMode | null;
    auto_attach: boolean;
    policy_revision: number | null;
  } = {
    name: '',
    type: 'generic',
    connection_url: '',
    description: '',
    cli_hint: '',
    default_branch: '',
    root_path: '',
    forge: '',
    mcpTransport: 'http',
    mcpToken: '',
    mcpHeaders: [],
    mcpCommand: '',
    mcpArgs: '',
    mcpEnv: [],
    is_global: false,
    read_only: true,
    scope_mode: null,
    auto_attach: false,
    policy_revision: null,
  };

  formCredentials: { username: string; password: string } = {
    username: '',
    password: '',
  };

  // Email (IMAP/SMTP) form state. Endpoints are credentials (redacted on
  // read-back, "leave blank to keep"); tier/scoping is non-secret config
  // and round-trips through ds.config on edit.
  emailForm: {
    provider: string;
    imap_host: string;
    imap_port: string;
    imap_security: 'ssl' | 'starttls';
    smtp_host: string;
    smtp_port: string;
    smtp_security: 'ssl' | 'starttls';
    access: EmailAccessTier;
    folders: string;
    drafts_folder: string;
    from_address: string;
    recipient_allowlist: string;
    unattended_send: boolean;
  } = this.defaultEmailForm();

  /** Host/port/security presets for the app-password-capable providers
   *  (knowledge-base/knowledge/features/email_datasource.md "Provider reality"). */
  private readonly emailProviderPresets: Record<
    string,
    {
      imap_host: string;
      imap_port: string;
      imap_security: 'ssl' | 'starttls';
      smtp_host: string;
      smtp_port: string;
      smtp_security: 'ssl' | 'starttls';
    }
  > = {
    gmail: {
      imap_host: 'imap.gmail.com', imap_port: '993', imap_security: 'ssl',
      smtp_host: 'smtp.gmail.com', smtp_port: '587', smtp_security: 'starttls',
    },
    fastmail: {
      imap_host: 'imap.fastmail.com', imap_port: '993', imap_security: 'ssl',
      smtp_host: 'smtp.fastmail.com', smtp_port: '587', smtp_security: 'starttls',
    },
    icloud: {
      imap_host: 'imap.mail.me.com', imap_port: '993', imap_security: 'ssl',
      smtp_host: 'smtp.mail.me.com', smtp_port: '587', smtp_security: 'starttls',
    },
    yahoo: {
      imap_host: 'imap.mail.yahoo.com', imap_port: '993', imap_security: 'ssl',
      smtp_host: 'smtp.mail.yahoo.com', smtp_port: '465', smtp_security: 'ssl',
    },
    mailbox: {
      imap_host: 'imap.mailbox.org', imap_port: '993', imap_security: 'ssl',
      smtp_host: 'smtp.mailbox.org', smtp_port: '587', smtp_security: 'starttls',
    },
    gmx: {
      imap_host: 'imap.gmx.net', imap_port: '993', imap_security: 'ssl',
      smtp_host: 'mail.gmx.net', smtp_port: '465', smtp_security: 'ssl',
    },
  };

  private defaultEmailForm(): DatasourceListComponent['emailForm'] {
    return {
      provider: 'custom',
      imap_host: '',
      imap_port: '993',
      imap_security: 'ssl',
      smtp_host: '',
      smtp_port: '587',
      smtp_security: 'starttls',
      access: 'draft',
      folders: '',
      drafts_folder: 'Drafts',
      from_address: '',
      recipient_allowlist: '',
      unattended_send: false,
    };
  }

  onEmailProviderSelect(value: string | null): void {
    this.emailForm.provider = value || 'custom';
    const preset = this.emailProviderPresets[this.emailForm.provider];
    if (preset) {
      this.emailForm.imap_host = preset.imap_host;
      this.emailForm.imap_port = preset.imap_port;
      this.emailForm.imap_security = preset.imap_security;
      this.emailForm.smtp_host = preset.smtp_host;
      this.emailForm.smtp_port = preset.smtp_port;
      this.emailForm.smtp_security = preset.smtp_security;
    }
  }

  onEmailAccessChange(value: string | null): void {
    if (value === 'read' || value === 'read_write' || value === 'draft' || value === 'send') {
      this.emailForm.access = value;
    }
  }

  onEmailSecurityChange(which: 'imap' | 'smtp', value: string | null): void {
    const security = value === 'starttls' ? 'starttls' : 'ssl';
    if (which === 'imap') this.emailForm.imap_security = security;
    else this.emailForm.smtp_security = security;
  }

  /** One-line description for the currently selected access tier. */
  emailAccessHintKey(): string {
    const suffixes: Record<EmailAccessTier, string> = {
      read: 'emailAccessHintRead',
      read_write: 'emailAccessHintReadWrite',
      draft: 'emailAccessHintDraft',
      send: 'emailAccessHintSend',
    };
    return `datasources.form.${suffixes[this.emailForm.access]}`;
  }

  // Repository auth form state
  gitAuthMethod: 'token' | 'ssh' = 'token';
  gitSshKey = '';

  // Generic env var editor
  envVars: { key: string; value: string }[] = [];
  private mcpTransportDirty = false;
  /** True once the user has explicitly picked a forge via onForgeSelect in
   *  this form session. Mirrors mcpTransportDirty: stops onConnectionUrlChange
   *  from re-inferring over a deliberate choice, and is reset on open/close. */
  private forgeDirty = false;
  private connectionUrlDirty = false;

  ngOnInit(): void {
    // Capabilities are fetched asynchronously. Start in fail-closed legacy
    // mode, then reload from the v1 catalog once the deployment explicitly
    // confirms support. Replay makes an already-resolved capability use the
    // right source on the initial refresh without causing a duplicate call.
    let initialising = true;
    this.capabilities.datasourceScopeAutoAttachAvailability$
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe((available) => {
        if (!initialising) {
          this.refresh();
          if (available) this.loadCatalogProjects(true);
          else this.resetCatalogProjectFilter();
        }
      });
    initialising = false;
    this.refresh();
    if (this.capabilities.datasourceScopeAutoAttachAvailable()) {
      this.loadCatalogProjects(true);
    }
    // Live-refresh KB index status while any KB is still converging. The tick is
    // a cheap no-op when nothing is indexing; an HTTP GET only fires for rows
    // actually pending/indexing, and stops once they reach a terminal status
    // (ready/partial/failed).
    timer(this.INDEX_POLL_MS, this.INDEX_POLL_MS)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe(() => this.pollActiveIndexStatuses());
  }

  refresh(): void {
    const serial = ++this.catalogRequestSerial;
    this.isLoading.set(true);
    if (!this.capabilities.datasourceScopeAutoAttachAvailable()) {
      this.catalogNextCursor.set(null);
      this.isLoadingMore.set(false);
      const type = this.typeFilter() === 'all'
        ? undefined
        : this.typeFilter() as DatasourceType;
      this.api.getDatasources(undefined, type).subscribe({
        next: (rows) => {
          if (serial !== this.catalogRequestSerial) return;
          this.applyCatalogRows(rows, false);
          this.isLoading.set(false);
        },
        error: () => {
          if (serial !== this.catalogRequestSerial) return;
          this.isLoading.set(false);
          this.errorMessage.set(this.transloco.translate('datasources.catalog.loadFailed'));
        },
      });
      return;
    }
    this.api.getDatasourceCatalog(this.catalogFilters()).subscribe({
      next: (response) => {
        if (serial !== this.catalogRequestSerial) return;
        this.applyCatalogRows(response.items, false);
        this.catalogNextCursor.set(response.next_cursor);
        this.isLoading.set(false);
      },
      error: () => {
        if (serial !== this.catalogRequestSerial) return;
        this.isLoading.set(false);
        this.errorMessage.set(this.transloco.translate('datasources.catalog.loadFailed'));
      },
    });
  }

  loadMoreCatalog(): void {
    if (!this.capabilities.datasourceScopeAutoAttachAvailable()) return;
    const cursor = this.catalogNextCursor();
    if (!cursor || this.isLoadingMore()) return;
    const serial = ++this.catalogRequestSerial;
    this.isLoadingMore.set(true);
    this.api.getDatasourceCatalog({...this.catalogFilters(), cursor}).subscribe({
      next: (response) => {
        if (serial !== this.catalogRequestSerial) return;
        this.applyCatalogRows(response.items, true);
        this.catalogNextCursor.set(response.next_cursor);
        this.isLoadingMore.set(false);
      },
      error: () => {
        if (serial !== this.catalogRequestSerial) return;
        this.isLoadingMore.set(false);
        this.errorMessage.set(this.transloco.translate('datasources.catalog.loadMoreFailed'));
      },
    });
  }

  private catalogFilters(): DatasourceCatalogFilters {
    const filters: DatasourceCatalogFilters = {limit: 50};
    const q = this.catalogSearch().trim();
    if (q) filters.q = q;
    if (this.catalogProjectId()) filters.project_id = this.catalogProjectId();
    if (this.typeFilter() !== 'all') filters.type = this.typeFilter() as DatasourceType;
    if (this.scopeFilter() === 'scope-all') filters.availability = 'all';
    if (this.scopeFilter() === 'projects') filters.availability = 'projects';
    if (this.scopeFilter() === 'unavailable') filters.availability = 'unavailable';
    if (this.automaticFilter() !== 'all') {
      filters.auto_attach = this.automaticFilter() === 'automatic';
    }
    const ownership = this.ownershipFilter();
    if (ownership !== 'all') filters.ownership = ownership;
    const visibility = this.visibilityFilter();
    if (visibility !== 'all') filters.visibility = visibility;
    return filters;
  }

  private applyCatalogRows(rows: Datasource[], append: boolean): void {
    const merged = append
      ? [...this.datasources(), ...rows.filter(row => !this.datasources().some(old => old.id === row.id))]
      : rows;
    this.datasources.set(merged);
    if (!append) this.indexStatuses.set({});
    for (const ds of rows) {
      if (ds.type === 'kb') this.loadIndexStatus(ds.id);
    }
  }

  onTypeFilter(value: string): void {
    this.typeFilter.set(value);
    this.refresh();
  }

  onCatalogSearch(value: string): void {
    this.catalogSearch.set(value);
    if (this.catalogSearchTimer) clearTimeout(this.catalogSearchTimer);
    this.catalogSearchTimer = setTimeout(() => this.refresh(), 250);
  }

  onCatalogProjectFilter(value: string | null): void {
    this.catalogProjectId.set(value ?? '');
    this.refresh();
  }

  onCatalogProjectSearch(value: string): void {
    this.catalogProjectSearch.set(value);
    if (this.catalogProjectSearchTimer) clearTimeout(this.catalogProjectSearchTimer);
    this.catalogProjectSearchTimer = setTimeout(() => this.loadCatalogProjects(true), 250);
  }

  loadCatalogProjects(reset = true): void {
    if (!this.capabilities.datasourceScopeAutoAttachAvailable()) return;
    const cursor = reset ? null : this.catalogProjectsNextCursor();
    if (!reset && !cursor) return;
    const serial = ++this.catalogProjectsRequestSerial;
    this.catalogProjectsLoading.set(true);
    this.catalogProjectsError.set(false);
    this.api.getLinkableDatasourceProjects({
      q: this.catalogProjectSearch().trim() || undefined,
      cursor: cursor ?? undefined,
      limit: 50,
    }).subscribe({
      next: (response) => {
        if (serial !== this.catalogProjectsRequestSerial) return;
        const previous = reset ? [] : this.catalogProjects();
        const selected = this.catalogProjects().find(
          project => project.id === this.catalogProjectId(),
        );
        const incoming = response.items;
        this.catalogProjects.set([
          ...(selected && !incoming.some(project => project.id === selected.id)
            ? [selected]
            : []),
          ...previous.filter(
            project => !incoming.some(item => item.id === project.id) && project.id !== selected?.id,
          ),
          ...incoming,
        ]);
        this.catalogProjectsNextCursor.set(response.next_cursor);
        this.catalogProjectsLoading.set(false);
      },
      error: () => {
        if (serial !== this.catalogProjectsRequestSerial) return;
        this.catalogProjectsLoading.set(false);
        this.catalogProjectsError.set(true);
      },
    });
  }

  loadMoreCatalogProjects(): void {
    this.loadCatalogProjects(false);
  }

  private resetCatalogProjectFilter(): void {
    this.catalogProjectsRequestSerial += 1;
    this.catalogProjectId.set('');
    this.catalogProjectSearch.set('');
    this.catalogProjects.set([]);
    this.catalogProjectsLoading.set(false);
    this.catalogProjectsError.set(false);
    this.catalogProjectsNextCursor.set(null);
  }

  onScopeFilter(value: string | null): void {
    if (value === 'scope-all' || value === 'projects' || value === 'unavailable') {
      this.scopeFilter.set(value);
    } else {
      this.scopeFilter.set('all');
    }
    this.refresh();
  }

  onAutomaticFilter(value: string | null): void {
    this.automaticFilter.set(value === 'automatic' || value === 'manual' ? value : 'all');
    this.refresh();
  }

  onOwnershipFilter(value: string | null): void {
    this.ownershipFilter.set(value === 'mine' || value === 'shared' ? value : 'all');
    this.refresh();
  }

  onVisibilityFilter(value: string | null): void {
    this.visibilityFilter.set(value === 'private' || value === 'public' ? value : 'all');
    this.refresh();
  }

  // ===== Form Methods =====

  openCreateForm(): void {
    this.resetFormData();
    this.editingId.set(null);
    this.showForm.set(true);
    this.formTestResult.set(null);
    this.cancelGenerateSshKey();
    this.closePublicKeyDialog();
    if (this.capabilities.datasourceScopeAutoAttachAvailable()) {
      this.loadScopeTargets(true);
      this.loadContextScopeTarget();
    }
  }

  openEditForm(ds: Datasource): void {
    this.editingOriginal = ds;
    this.formData = {
      name: ds.name,
      type: ds.type,
      connection_url: ds.connection_url || '',
      description: ds.description || '',
      cli_hint: ds.cli_hint || '',
      default_branch: ds.default_branch || '',
      root_path: ds.config?.root_path || '',
      // Legacy repository connectors created before the forge field existed
      // have empty config; fall back to inferring from the stored URL so
      // github.com/gitlab.com connectors don't open blank with Save disabled.
      forge: ds.config?.forge || this.inferForgeFromUrl(ds.connection_url || ''),
      // MCP credentials, including transport, are redacted by REST. Default
      // the editor to remote HTTP and preserve the stored credential object
      // unless the user enters replacement connection details.
      mcpTransport: 'http',
      mcpToken: '',
      mcpHeaders: [],
      mcpCommand: '',
      mcpArgs: '',
      mcpEnv: [],
      is_global: ds.is_global ?? false,
      read_only: ds.read_only ?? true,
      scope_mode: ds.scope_mode ?? 'all',
      auto_attach: ds.auto_attach ?? false,
      policy_revision: ds.policy_revision ?? 1,
    };
    this.connectionUrlDirty = false;
    this.formProjectIds.set(new Set(ds.project_ids ?? []));
    this.scopeSelectionInitialized = (ds.project_ids?.length ?? 0) > 0;
    // F3 (knowledge-base/knowledge/multi_tenancy.md): credentials never come back from the
    // API. The user re-enters them only if they want to change the stored
    // value — submitting blank fields leaves the secret untouched.
    this.formCredentials = { username: '', password: '' };
    // Email: the non-secret tier/scoping config round-trips; the imap/smtp
    // endpoints live inside the redacted credentials and stay blank.
    this.emailForm = this.defaultEmailForm();
    if (ds.type === 'email') {
      const cfg = ds.config ?? {};
      this.emailForm.access = cfg.access ?? 'draft';
      this.emailForm.folders = (cfg.folders ?? []).join(', ');
      this.emailForm.drafts_folder = cfg.drafts_folder ?? 'Drafts';
      this.emailForm.from_address = cfg.from_address ?? '';
      this.emailForm.recipient_allowlist = (cfg.recipient_allowlist ?? []).join(', ');
      this.emailForm.unattended_send = cfg.unattended_send ?? false;
    }
    // Repository auth: pick the right tab based on cli_hint so the user
    // sees the same auth method they'd previously configured, but the
    // SSH key field stays blank for the same "leave blank to keep" reason.
    if (this.isGitBackedType(ds.type)) {
      this.gitAuthMethod = ds.cli_hint?.includes('ssh') ? 'ssh' : 'token';
      this.gitSshKey = '';
    } else {
      this.gitAuthMethod = 'token';
      this.gitSshKey = '';
    }
    // ENV names can round-trip; blank values preserve the saved credentials.
    this.envVars = ds.type === 'credentials'
      ? (ds.env_var_names ?? []).map(key => ({key, value: ''}))
      : [];
    // Credential-file types: contents never come back from the API (F3
    // redaction), so the textareas stay blank. The user re-pastes only
    // if they want to replace the stored value.
    this.kubeconfigContent = '';
    this.genericFiles = [];
    this.mcpTransportDirty = false;
    this.forgeDirty = false;
    this.editingId.set(ds.id);
    this.showForm.set(true);
    this.formTestResult.set(null);
    this.cancelGenerateSshKey();
    this.closePublicKeyDialog();
    // Native project knowledge policy is server-owned and already complete in
    // the management row. Ordinary connector edits load addable/retained
    // targets before their controls or Save become available.
    if (
      this.capabilities.datasourceScopeAutoAttachAvailable() &&
      !this.isNativeProjectConnector()
    ) {
      this.loadScopeTargets(true);
    }
  }

  closeForm(): void {
    this.showForm.set(false);
    this.editingId.set(null);
    this.formTestResult.set(null);
    this.showPublishConfirm.set(false);
    this.cancelGenerateSshKey();
    this.closePublicKeyDialog();
    this.scopeTargetsRequestSerial += 1;
    this.scopeTargetsLoading.set(false);
    this.scopeTargetsError.set(false);
    this.resetFormData();
  }

  onScopeModeChange(mode: DatasourceScopeMode): void {
    this.formData.scope_mode = mode;
  }

  isNativeProjectConnector(ds: Datasource | null = this.editingOriginal): boolean {
    return !!ds?.config?.native_project_id;
  }

  onProjectSearch(value: string): void {
    this.projectSearch.set(value);
    if (this.projectSearchTimer) clearTimeout(this.projectSearchTimer);
    this.projectSearchTimer = setTimeout(() => this.loadScopeTargets(true), 250);
  }

  loadScopeTargets(_reset = false): void {
    if (!this.capabilities.datasourceScopeAutoAttachAvailable()) {
      this.scopeTargetsRequestSerial += 1;
      this.scopeTargetsLoading.set(false);
      this.scopeTargetsError.set(false);
      return;
    }
    if (!this.showForm()) return;
    const serial = ++this.scopeTargetsRequestSerial;
    this.scopeTargetsLoading.set(true);
    this.scopeTargetsError.set(false);
    this.api.getLinkableDatasourceProjects({
      datasourceId: this.editingId() ?? undefined,
      q: this.projectSearch().trim() || undefined,
      limit: 50,
    }).subscribe({
      next: (response) => {
        if (serial !== this.scopeTargetsRequestSerial) return;
        const selectedItems = response.selected_items ?? [];
        this.scopeTargets.set([
          ...selectedItems,
          ...response.items.filter(item => !selectedItems.some(selected => selected.id === item.id)),
        ]);
        this.scopeTargetsNextCursor.set(response.next_cursor);
        if (this.editingId() && !this.scopeSelectionInitialized) {
          const linkedSource = selectedItems.length > 0 ? selectedItems : response.items;
          const linked = linkedSource.filter(item => item.linked || item.selected).map(item => item.id);
          this.formProjectIds.set(new Set(linked));
          this.scopeSelectionInitialized = true;
        } else if (!this.editingId() && !this.scopeSelectionInitialized) {
          const contextProjectId = this.route?.snapshot.queryParamMap.get('project');
          if (contextProjectId && response.items.some(item => item.id === contextProjectId && item.addable)) {
            this.formProjectIds.set(new Set([contextProjectId]));
            this.formData.scope_mode = 'projects';
            this.scopeSelectionInitialized = true;
          } else if (!contextProjectId) {
            this.scopeSelectionInitialized = true;
          }
        }
        this.scopeTargetsLoading.set(false);
      },
      error: () => {
        if (serial !== this.scopeTargetsRequestSerial) return;
        this.scopeTargetsLoading.set(false);
        this.scopeTargetsError.set(true);
      },
    });
  }

  loadMoreScopeTargets(): void {
    const cursor = this.scopeTargetsNextCursor();
    if (!cursor || this.scopeTargetsLoading()) return;
    const serial = ++this.scopeTargetsRequestSerial;
    this.scopeTargetsLoading.set(true);
    this.scopeTargetsError.set(false);
    this.api.getLinkableDatasourceProjects({
      datasourceId: this.editingId() ?? undefined,
      q: this.projectSearch().trim() || undefined,
      cursor,
      limit: 50,
    }).subscribe({
      next: (response) => {
        if (serial !== this.scopeTargetsRequestSerial) return;
        const current = this.scopeTargets();
        const incoming = [...(response.selected_items ?? []), ...response.items];
        this.scopeTargets.set([
          ...current,
          ...incoming.filter(item => !current.some(old => old.id === item.id)),
        ]);
        this.scopeTargetsNextCursor.set(response.next_cursor);
        this.scopeTargetsLoading.set(false);
      },
      error: () => {
        if (serial !== this.scopeTargetsRequestSerial) return;
        this.scopeTargetsLoading.set(false);
        this.scopeTargetsError.set(true);
      },
    });
  }

  /** Resolve a route-provided project independently of the first catalog
   * page. GET project + members is enough to prove the same owner/admin
   * authority enforced by connector creation, without widening backend APIs. */
  private loadContextScopeTarget(): void {
    const projectId = this.route?.snapshot.queryParamMap.get('project');
    if (!projectId || this.editingId() || !this.showForm()) return;
    const serial = ++this.contextScopeTargetRequestSerial;
    forkJoin({
      project: this.api.getProject(projectId),
      members: this.api.getProjectMembers(projectId),
    }).subscribe(({project, members}) => {
      if (
        serial !== this.contextScopeTargetRequestSerial ||
        !this.showForm() || this.editingId() ||
        this.route?.snapshot.queryParamMap.get('project') !== projectId
      ) return;
      const user = this.userService.currentUser();
      const role = project?.user_role ?? members.find(member => member.user_id === user?.id)?.role;
      const addable = !!project && (!!user?.is_admin || role === 'owner');
      if (project && addable) {
        const target: LinkableDatasourceProject = {
          id: project.id,
          name: project.name,
          is_default: project.is_default,
          user_role: role ?? null,
          addable: true,
          retained_only: false,
          linked: false,
        };
        this.scopeTargets.update(current => current.some(item => item.id === target.id)
          ? current.map(item => item.id === target.id ? target : item)
          : [target, ...current]);
        this.formProjectIds.set(new Set([projectId]));
        this.formData.scope_mode = 'projects';
      }
      this.scopeSelectionInitialized = true;
    });
  }

  toggleScopeProject(project: LinkableDatasourceProject): void {
    const current = this.formProjectIds();
    const next = new Set(current);
    if (next.has(project.id)) {
      const wasLinked = this.editingOriginal?.project_ids?.includes(project.id) ?? false;
      if (
        wasLinked &&
        !confirm(this.transloco.translate('datasources.form.removeProjectWarning', {name: project.name}))
      ) return;
      next.delete(project.id);
    } else if (project.addable) {
      next.add(project.id);
    }
    this.formProjectIds.set(next);
  }

  // ===== SSH Key Generation =====

  /**
   * Entry point for the Generate button. Asks for confirmation if the
   * textarea already has content (so a hand-pasted key in progress isn't
   * silently wiped); otherwise generates immediately.
   */
  onGenerateSshKeyClick(): void {
    if (this.isGeneratingKey()) return;
    if (this.gitSshKey.trim()) {
      this.showGenerateConfirm.set(true);
      return;
    }
    void this.runGenerateSshKey();
  }

  confirmGenerateSshKey(): void {
    this.showGenerateConfirm.set(false);
    void this.runGenerateSshKey();
  }

  cancelGenerateSshKey(): void {
    this.showGenerateConfirm.set(false);
  }

  closePublicKeyDialog(): void {
    this.showPublicKeyDialog.set(false);
    this.publicKeyCopied.set(false);
    if (this.copiedResetTimer) {
      clearTimeout(this.copiedResetTimer);
      this.copiedResetTimer = null;
    }
  }

  copyPublicKey(): void {
    const text = this.generatedPublicKey();
    if (!text) return;
    const finish = () => {
      this.publicKeyCopied.set(true);
      if (this.copiedResetTimer) clearTimeout(this.copiedResetTimer);
      this.copiedResetTimer = setTimeout(() => this.publicKeyCopied.set(false), 2500);
    };
    if (typeof navigator !== 'undefined' && navigator.clipboard?.writeText) {
      navigator.clipboard.writeText(text).then(finish).catch((err) => {
        console.error('Clipboard write failed:', err);
      });
    } else {
      // Fallback for environments without the async clipboard API
      try {
        const ta = document.createElement('textarea');
        ta.value = text;
        ta.style.position = 'fixed';
        ta.style.opacity = '0';
        document.body.appendChild(ta);
        ta.select();
        document.execCommand('copy');
        document.body.removeChild(ta);
        finish();
      } catch (err) {
        console.error('Clipboard fallback failed:', err);
      }
    }
  }

  private runGenerateSshKey(): Promise<void> {
    this.isGeneratingKey.set(true);
    const comment = this.buildSshKeyComment();
    return new Promise((resolve) => {
      this.api.generateSshKey(comment).subscribe((result) => {
        this.isGeneratingKey.set(false);
        if (!result) {
          this.errorMessage.set(
            this.transloco.translate('datasources.sshKeyDialog.generateFailed'),
          );
          resolve();
          return;
        }
        this.gitSshKey = result.private_key;
        this.generatedPublicKey.set(result.public_key);
        this.publicKeyCopied.set(false);
        this.showPublicKeyDialog.set(true);
        resolve();
      });
    });
  }

  private buildSshKeyComment(): string {
    const name = (this.formData.name || '').trim();
    return name ? `${name} (srw)` : 'srw';
  }

  onTypeChange(): void {
    // Reset URL placeholder trigger
    this.formTestResult.set(null);
    // Entering the URL before switching to 'repository' left forge blank
    // forever, since onConnectionUrlChange only infers for that type. Catch
    // up now — unless the user already made an explicit choice.
    if (this.formData.type === 'repository' && !this.forgeDirty) {
      this.formData.forge = this.inferForgeFromUrl(this.formData.connection_url);
    }
  }

  onTypeSelect(value: DatasourceType | null): void {
    if (value) {
      this.formData.type = value;
      // kb datasources are read-only by architecture (OKF org-vault policy).
      if (value === 'kb') this.formData.read_only = true;
      // Mailboxes are private-only; the server rejects is_global for email.
      if (value === 'email') this.formData.is_global = false;
      this.onTypeChange();
    }
  }

  onMcpTransportSelect(value: string | null): void {
    if (value === 'http' || value === 'sse' || value === 'stdio') {
      this.mcpTransportDirty =
        this.mcpTransportDirty || this.formData.mcpTransport !== value;
      this.formData.mcpTransport = value;
      this.formTestResult.set(null);
    }
  }

  onGitAuthMethodChange(value: string | null): void {
    this.gitAuthMethod = value === 'ssh' ? 'ssh' : 'token';
  }

  /** Connection-URL input handler (shared by every non-generic type). For
   *  repository connectors this also (re)derives `forge` from the host:
   *  github.com/gitlab.com default automatically; every other host — every
   *  self-hosted Gitea or GitLab included — is left blank so `canSave()`
   *  blocks submission until the user picks one explicitly, rather than
   *  letting the server 400 (self-hosted Gitea and GitLab are
   *  indistinguishable by URL alone; see orchestrator/main.py
   *  `_normalize_repository_config`).
   *
   *  Skipped once forgeDirty (the user explicitly picked one via
   *  onForgeSelect): re-inferring on every keystroke would silently
   *  overwrite a deliberate choice and re-disable Save. */
  onConnectionUrlChange(value: string = this.formData.connection_url): void {
    this.connectionUrlDirty = !this.editingOriginal ||
      value !== (this.editingOriginal.connection_url ?? '');
    this.formData.connection_url = value;
    if (this.formData.type === 'repository' && !this.forgeDirty) {
      this.formData.forge = this.inferForgeFromUrl(value);
    }
  }

  private inferForgeFromUrl(url: string): RepositoryForge | '' {
    let host = '';
    try {
      host = new URL(url).hostname.toLowerCase();
    } catch {
      return '';
    }
    if (host === 'github.com' || host === 'www.github.com') return 'github';
    if (host === 'gitlab.com' || host === 'www.gitlab.com') return 'gitlab';
    return '';
  }

  onForgeSelect(value: string | null): void {
    const next =
      value === 'github' || value === 'gitea' || value === 'gitlab' ? value : '';
    this.forgeDirty = this.forgeDirty || this.formData.forge !== next;
    this.formData.forge = next;
  }

  /**
   * Visibility badge for a datasource. Three real states:
   *  - is_global → shared with all users, agents run against its stored
   *    credentials ("Global"); only admins/system-seeding can create these
   *  - job_id    → scoped to a single job ("Job")
   *  - otherwise → owner-only, attachable via the picker ("Private")
   *
   * NOTE: this badge previously keyed off `job_id` alone, so an ordinary
   * private datasource (is_global=false, job_id=null) rendered "Global" —
   * a display-only mislabel, not an actual visibility bug.
   */
  scopeLabelKey(ds: Datasource): string {
    if (ds.is_global) return 'datasources.table.scopeGlobal';
    if (ds.job_id) return 'datasources.table.scopeJob';
    return 'datasources.table.scopePrivate';
  }

  scopeTone(ds: Datasource): BadgeTone {
    if (ds.is_global) return 'accent';
    if (ds.job_id) return 'neutral';
    return 'info';
  }

  private projectCount(ds: Datasource): number {
    return ds.project_count ?? ds.project_ids?.length ?? 0;
  }

  availabilityLabel(ds: Datasource): string {
    if ((ds.scope_mode ?? 'all') === 'all') {
      return this.transloco.translate('datasources.table.availabilityAll');
    }
    const count = this.projectCount(ds);
    if (count === 0) return this.transloco.translate('datasources.table.availabilityNone');
    // Shared rows expose only caller-visible associations. A numeric subset
    // would look like the connector's complete scope, so reserve counts for
    // creators/admins and use the non-enumerating label for other callers.
    const canSeeCompleteScope = this.isOwnedByCurrentUser(ds) || !!this.userService.currentUser()?.is_admin;
    if (!canSeeCompleteScope || (ds.project_count === undefined && ds.project_ids === undefined)) {
      return this.transloco.translate('datasources.table.availabilityScoped');
    }
    return this.transloco.translate('datasources.table.availabilityProjects', {count});
  }

  availabilityTone(ds: Datasource): BadgeTone {
    if ((ds.scope_mode ?? 'all') === 'all') return 'info';
    return this.projectCount(ds) === 0 ? 'warning' : 'neutral';
  }

  isOwnedByCurrentUser(ds: Datasource): boolean {
    return !!ds.created_by && ds.created_by === this.userService.currentUser()?.id;
  }

  dsTypeTone(type: DatasourceType | string): BadgeTone {
    switch (type) {
      case 'generic':
        return 'accent';
      case 'repository':
      case 'postgresql':
      case 'mcp':
        return 'info';
      case 'kb':
        return 'accent';
      case 'neo4j':
        return 'success';
      case 'webdav':
      case 'email':
        return 'warning';
      case 'mongodb':
        return 'neutral';
      case 'kubeconfig':
      case 'ssh_key':
      case 'generic_file':
        // Credential-file types share a tone so the table groups them visually.
        return 'warning';
      default:
        return 'neutral';
    }
  }

  /** Which confirmation the pending save needs.
   *  'name' — read-write publish or RO→RW flip (typed-name gate)
   *  'warn' — newly public, read-only (plain warning)
   *  null   — private saves, unpublish, RW→RO (exposure-reducing) */
  publishConfirmTier(): 'name' | 'warn' | null {
    if (!this.formData.is_global) return null;
    const prev = this.editingId() ? this.editingOriginal : null;
    const wasPublic = prev?.is_global === true;
    const wasRw = wasPublic && prev?.read_only === false;
    const isRw = !this.formData.read_only && this.formData.type !== 'kb';
    if (isRw && !wasRw) return 'name';
    if (!wasPublic) return 'warn';
    return null;
  }

  saveForm(): void {
    if (!this.formData.name) return;
    const tier = this.publishConfirmTier();
    if (tier) {
      this.publishConfirmName.set(tier === 'name' ? this.formData.name : null);
      this.showPublishConfirm.set(true);
      return;
    }
    this.doSave();
  }

  onPublishConfirmed(): void {
    this.showPublishConfirm.set(false);
    this.doSave();
  }

  private connectionUrlForPayload(): string | undefined {
    if (
      this.editingId() && this.editingOriginal?.connection_url_redacted &&
      !this.connectionUrlDirty
    ) {
      return undefined;
    }
    if (
      this.formData.type === 'mcp' &&
      this.formData.mcpTransport === 'stdio'
    ) {
      return undefined;
    }
    return this.formData.connection_url || undefined;
  }

  /** One source of truth for the create and create-then-test policy payload. */
  private createAvailabilityPolicy(): Partial<Pick<
    DatasourceCreateRequest,
    'scope_mode' | 'project_ids' | 'auto_attach'
  >> {
    if (!this.capabilities.datasourceScopeAutoAttachAvailable()) return {};
    return {
      scope_mode: this.formData.scope_mode ?? undefined,
      project_ids: this.formData.scope_mode === 'projects'
        ? [...this.formProjectIds()]
        : [],
      auto_attach: this.formData.auto_attach,
    };
  }

  doSave(): void {
    if (!this.formData.name) return;

    // Warn when creating a repository without credentials
    if (this.formData.type === 'repository' && this.buildCredentials()?.['read_only']) {
      const confirmed = confirm(this.transloco.translate('datasources.messages.repoNoAuthConfirm'));
      if (!confirmed) return;
    }

    this.isSaving.set(true);
    this.clearMessages();

    const editId = this.editingId();
    const creds = this.buildCredentials();

    if (editId) {
      const previousProjectIds = new Set(this.editingOriginal?.project_ids ?? []);
      const currentProjectIds = this.formProjectIds();
      const projectSetChanged =
        previousProjectIds.size !== currentProjectIds.size ||
        [...previousProjectIds].some(id => !currentProjectIds.has(id));
      const update: DatasourceUpdateRequest = {
        name: this.formData.name,
        description: this.formData.description || undefined,
        credentials: creds,
        cli_hint: this.formData.cli_hint || undefined,
        default_branch: this.formData.default_branch || undefined,
        config: this.buildTypeConfig(),
        is_global: this.formData.is_global,
        read_only: this.formData.is_global
          ? (this.formData.type === 'kb' ? true : this.formData.read_only)
          : undefined,
      };
      const connectionUrl = this.connectionUrlForPayload();
      if (connectionUrl !== undefined) update.connection_url = connectionUrl;
      if (
        this.capabilities.datasourceScopeAutoAttachAvailable() &&
        !this.isNativeProjectConnector()
      ) {
        update.scope_mode = this.formData.scope_mode ?? undefined;
        update.auto_attach = this.formData.auto_attach;
        update.policy_revision = this.formData.policy_revision ?? undefined;
        // In all-mode omission preserves existing sharing/settings links. A
        // project-scoped update is a full-set policy edit.
        update.project_ids = this.formData.scope_mode === 'projects' || projectSetChanged
          ? [...currentProjectIds]
          : undefined;
      }

      this.api.updateDatasource(editId, update).subscribe({
        next: (result) => {
          this.isSaving.set(false);
          if (result) {
            this.successMessage.set(this.transloco.translate('datasources.messages.updated'));
            this.closeForm();
            this.refresh();
          } else {
            this.errorMessage.set(this.transloco.translate('datasources.messages.updateFailed'));
          }
        },
        error: (err) => {
          this.isSaving.set(false);
          // Surface the server detail (e.g. the publish-capability 403)
          // instead of the generic message — mirrors the create path.
          const detail = err?.error?.detail;
          this.errorMessage.set(
            detail || this.transloco.translate('datasources.messages.updateError'),
          );
        },
      });
    } else {
      const create: DatasourceCreateRequest = {
        name: this.formData.name,
        type: this.formData.type,
        connection_url: this.connectionUrlForPayload(),
        description: this.formData.description || undefined,
        credentials: creds,
        cli_hint: this.formData.cli_hint || undefined,
        default_branch: this.formData.default_branch || undefined,
        config: this.buildTypeConfig(),
        is_global: this.formData.is_global,
        read_only: this.formData.is_global
          ? (this.formData.type === 'kb' ? true : this.formData.read_only)
          : undefined,
        ...this.createAvailabilityPolicy(),
      };

      this.api.createDatasource(create).subscribe({
        next: (result) => {
          this.isSaving.set(false);
          this.successMessage.set(this.transloco.translate('datasources.messages.created', {name: result.name}));
          this.closeForm();
          this.refresh();
        },
        error: (err) => {
          this.isSaving.set(false);
          const detail = err?.error?.detail;
          this.errorMessage.set(detail || this.transloco.translate('datasources.messages.createFailed'));
        },
      });
    }
  }

  testFromForm(): void {
    // For forms we need to save first to test, or test an existing one
    const editId = this.editingId();
    if (editId) {
      this.isTesting.set(true);
      this.formTestResult.set(null);
      this.api.testDatasource(editId).subscribe({
        next: (result) => {
          this.isTesting.set(false);
          this.formTestResult.set(result);
        },
        error: () => {
          this.isTesting.set(false);
          this.formTestResult.set({ status: 'error', message: this.transloco.translate('datasources.messages.testFailed') });
        },
      });
    } else {
      // For new datasources, save first then test (email carries its
      // endpoints in credentials instead of a connection URL)
      if (!this.formData.name) return;
      if (!this.canTestFromForm()) return;
      this.isSaving.set(true);
      this.formTestResult.set(null);

      const create: DatasourceCreateRequest = {
        name: this.formData.name,
        type: this.formData.type,
        connection_url: this.connectionUrlForPayload(),
        description: this.formData.description || undefined,
        credentials: this.buildCredentials(),
        default_branch: this.formData.default_branch || undefined,
        config: this.buildTypeConfig(),
        ...this.createAvailabilityPolicy(),
      };

      this.api.createDatasource(create).subscribe({
        next: (created) => {
          this.isSaving.set(false);
          if (created) {
            const createdPolicy: Datasource = {
              ...created,
              scope_mode: created.scope_mode ?? create.scope_mode,
              auto_attach: created.auto_attach ?? create.auto_attach,
              policy_revision: created.policy_revision ?? 1,
              project_ids: created.project_ids ?? create.project_ids ?? [],
            };
            this.editingOriginal = createdPolicy;
            this.formData.scope_mode = createdPolicy.scope_mode ?? this.formData.scope_mode;
            this.formData.auto_attach = createdPolicy.auto_attach ?? this.formData.auto_attach;
            this.formData.policy_revision = createdPolicy.policy_revision ?? 1;
            this.formProjectIds.set(new Set(createdPolicy.project_ids ?? []));
            this.editingId.set(created.id);
            this.refresh();
            // Now test
            this.isTesting.set(true);
            this.api.testDatasource(created.id).subscribe({
              next: (result) => {
                this.isTesting.set(false);
                this.formTestResult.set(result);
              },
              error: () => {
                this.isTesting.set(false);
                this.formTestResult.set({ status: 'error', message: this.transloco.translate('datasources.messages.testRequestFailed') });
              },
            });
          } else {
            this.errorMessage.set(this.transloco.translate('datasources.messages.createForTestFailed'));
          }
        },
        error: () => {
          this.isSaving.set(false);
          this.errorMessage.set(this.transloco.translate('datasources.messages.createError'));
        },
      });
    }
  }

  // ===== Table Actions =====

  testDatasource(id: string): void {
    this.testingIds.update((s) => new Set(s).add(id));
    this.api.testDatasource(id).subscribe({
      next: (result) => {
        this.testingIds.update((s) => {
          const next = new Set(s);
          next.delete(id);
          return next;
        });
        if (result) {
          this.testResults.update((r) => ({ ...r, [id]: result }));
        }
      },
      error: () => {
        this.testingIds.update((s) => {
          const next = new Set(s);
          next.delete(id);
          return next;
        });
        this.testResults.update((r) => ({
          ...r,
          [id]: { status: 'error' as const, message: this.transloco.translate('datasources.messages.testFailed') },
        }));
      },
    });
  }

  reindexDatasource(ds: Datasource, full = false): void {
    if (
      ds.type !== 'kb' ||
      !this.canManage(ds) ||
      this.reindexingIds().has(ds.id)
    ) {
      return;
    }
    if (
      full &&
      !confirm(this.transloco.translate('datasources.table.fullReindexConfirm'))
    ) {
      return;
    }

    this.reindexingIds.update((ids) => new Set(ids).add(ds.id));
    this.clearMessages();
    this.api.reindexDatasource(ds.id, full).subscribe((result) => {
      this.reindexingIds.update((ids) => {
        const next = new Set(ids);
        next.delete(ds.id);
        return next;
      });
      if (result) {
        this.successMessage.set(
          this.transloco.translate('datasources.messages.reindexed', {
            name: ds.name,
            status: result.status,
          }),
        );
        this.loadIndexStatus(ds.id);
      } else {
        this.errorMessage.set(
          this.transloco.translate('datasources.messages.reindexFailed'),
        );
      }
    });
  }

  deleteDatasource(ds: Datasource): void {
    this.clearMessages();
    this.api.deleteDatasource(ds.id).subscribe({
      next: (result) => {
        if (result) {
          this.successMessage.set(this.transloco.translate('datasources.messages.deleted', {name: ds.name}));
          this.refresh();
        } else {
          this.errorMessage.set(this.transloco.translate('datasources.messages.deleteFailed'));
        }
      },
      error: () => {
        this.errorMessage.set(this.transloco.translate('datasources.messages.deleteError'));
      },
    });
  }

  // ===== Helpers =====

  getTypeIcon(type: DatasourceType | string): string {
    const icons: Record<string, string> = {
      credentials: 'key',
      generic: 'settings_input_component',
      repository: 'code',
      kb: 'menu_book',
      postgresql: 'database',
      neo4j: 'hub',
      mongodb: 'eco',
      webdav: 'cloud',
      email: 'mail',
      kubeconfig: 'rocket_launch',
      ssh_key: 'key',
      generic_file: 'description',
    };
    return icons[type] || 'storage';
  }

  maskUrl(url: string): string {
    try {
      return url.replace(/:([^/:@]+)@/, ':***@');
    } catch {
      return url;
    }
  }

  canManage(ds: Datasource): boolean {
    const user = this.userService.currentUser();
    return !!user && (user.is_admin === true || ds.created_by === user.id);
  }

  indexStatusTone(status: DatasourceIndexStatus['status']): BadgeTone {
    switch (status) {
      case 'ready':
        return 'success';
      case 'indexing':
        return 'info';
      case 'partial':
        return 'warning';
      case 'failed':
        return 'danger';
      default:
        return 'neutral';
    }
  }

  indexStatusLabel(status: DatasourceIndexStatus): string {
    if (status.status === 'ready') {
      const sha = (status.indexed_commit || status.source_head || '').slice(0, 8);
      return this.transloco.translate('datasources.table.indexReady', {
        sha: sha || '—',
      });
    }
    return this.transloco.translate(
      `datasources.table.index${status.status[0].toUpperCase()}${status.status.slice(1)}`,
    );
  }

  formatIndexDate(value: string): string {
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
  }

  /** Percent complete for the current indexing run (clamped 0–100). */
  indexProgressPercent(status: DatasourceIndexStatus): number {
    const total = status.notes_total ?? 0;
    if (total <= 0) return 0;
    const done = Math.min(status.notes_done ?? 0, total);
    return Math.round((done / total) * 100);
  }

  redactIndexError(value: string): string {
    return value
      .replace(/(https?:\/\/)[^/@\s]+@/gi, '$1***@')
      .replace(/\b(token|password|secret)=([^\s&]+)/gi, '$1=***')
      .slice(0, 240);
  }

  /** Re-fetch index status for KB rows still pending/indexing (poll tick). */
  private pollActiveIndexStatuses(): void {
    const statuses = this.indexStatuses();
    for (const ds of this.datasources()) {
      if (ds.type !== 'kb') continue;
      const state = statuses[ds.id]?.status;
      if (state === 'pending' || state === 'indexing') {
        this.loadIndexStatus(ds.id);
      }
    }
  }

  private loadIndexStatus(id: string): void {
    this.api.getDatasourceIndexStatus(id).subscribe((status) => {
      if (!status) return;
      this.indexStatuses.update((statuses) => ({...statuses, [id]: status}));
    });
  }

  // Env var editor methods
  addEnvVar(): void {
    this.envVars.push({ key: '', value: '' });
  }

  removeEnvVar(index: number): void {
    this.envVars.splice(index, 1);
  }

  addMcpHeader(): void {
    this.formData.mcpHeaders.push({key: '', value: ''});
  }

  removeMcpHeader(index: number): void {
    this.formData.mcpHeaders.splice(index, 1);
  }

  addMcpEnv(): void {
    this.formData.mcpEnv.push({key: '', value: ''});
  }

  removeMcpEnv(index: number): void {
    this.formData.mcpEnv.splice(index, 1);
  }

  /** Non-secret, type-specific config for the create/update/test payloads.
   *  `undefined` for types without config so the column stays untouched. */
  private buildTypeConfig(): DatasourceConfig | undefined {
    if (this.formData.type === 'kb') {
      return {root_path: this.formData.root_path.trim()};
    }
    if (this.formData.type === 'repository') {
      // canSave() already requires forge to be non-blank before this can be
      // reached from the UI; the `{}` fallback only guards a defensive call.
      return this.formData.forge ? {forge: this.formData.forge} : {};
    }
    if (this.formData.type === 'email') {
      return {
        access: this.emailForm.access,
        folders: this.parseListInput(this.emailForm.folders),
        drafts_folder: this.emailForm.drafts_folder.trim() || 'Drafts',
        from_address: this.emailForm.from_address.trim(),
        recipient_allowlist: this.parseListInput(this.emailForm.recipient_allowlist),
        unattended_send: this.emailForm.unattended_send,
      };
    }
    return undefined;
  }

  /** Split a comma-separated text input into trimmed, non-empty entries. */
  private parseListInput(value: string): string[] {
    return value
      .split(',')
      .map((entry) => entry.trim())
      .filter(Boolean);
  }

  private buildCredentials(): Record<string, unknown> | undefined {
    // F3: when editing, blank credentials mean "leave existing alone"
    // (the API never returns credentials, so the form can't show them
    // back). Returning undefined skips the credentials column in the
    // PUT body so the orchestrator preserves the stored secret.
    const isEditing = this.editingId() !== null;
    if (this.isEnvType()) {
      const envVarsObj: Record<string, string> = {};
      for (const ev of this.envVars) {
        if (ev.key.trim() && (this.formData.type !== 'credentials' || ev.value !== '')) {
          envVarsObj[ev.key.trim()] = ev.value;
        }
      }
      if (Object.keys(envVarsObj).length === 0) return undefined;
      return { env_vars: envVarsObj };
    }
    if (this.formData.type === 'mcp') {
      if (isEditing && !this.mcpCredentialsWereEntered()) {
        return undefined;
      }
      const credentials: Record<string, unknown> = {
        transport: this.formData.mcpTransport,
      };
      if (this.formData.mcpTransport === 'stdio') {
        credentials['command'] = this.formData.mcpCommand.trim();
        credentials['args'] = this.formData.mcpArgs
          .split('\n')
          .map((arg) => arg.trim())
          .filter(Boolean);
        credentials['env'] = this.kvRowsToObject(this.formData.mcpEnv);
        return credentials;
      }

      if (this.formData.mcpToken) {
        credentials['auth'] = {
          type: 'bearer',
          token: this.formData.mcpToken,
        };
        return credentials;
      }
      const headers = this.kvRowsToObject(this.formData.mcpHeaders);
      if (Object.keys(headers).length > 0) {
        credentials['auth'] = {type: 'headers', headers};
      }
      return credentials;
    }
    if (this.isGitBackedType()) {
      if (this.gitAuthMethod === 'ssh') {
        if (!this.gitSshKey) {
          return isEditing || this.formData.type === 'kb'
            ? undefined
            : {read_only: true};
        }
        return { auth_method: 'ssh', ssh_key: this.gitSshKey };
      } else {
        if (!this.formCredentials.password) {
          return isEditing || this.formData.type === 'kb'
            ? undefined
            : {read_only: true};
        }
        return { auth_method: 'token', token: this.formCredentials.password };
      }
    }
    if (this.formData.type === 'kubeconfig') {
      const contents = this.kubeconfigContent;
      if (!contents.trim()) return isEditing ? undefined : { files: [] };
      const file: CredentialFileEntry = { contents };
      return { files: [file] };
    }
    if (this.formData.type === 'ssh_key') {
      const contents = this.gitSshKey;
      if (!contents.trim()) return isEditing ? undefined : { files: [] };
      const file: CredentialFileEntry = { contents };
      return { files: [file] };
    }
    if (this.formData.type === 'email') {
      // F3 again: blank username+password on edit means "keep the stored
      // credentials" (which carry the imap/smtp endpoints too).
      if (isEditing && !this.formCredentials.username && !this.formCredentials.password) {
        return undefined;
      }
      const creds: Record<string, unknown> = {
        backend: 'imap_smtp',
        username: this.formCredentials.username,
        password: this.formCredentials.password,
        imap: {
          host: this.emailForm.imap_host.trim(),
          port: Number(this.emailForm.imap_port) || 993,
          security: this.emailForm.imap_security,
        },
      };
      // SMTP is only needed (and only validated) at the send tier.
      if (this.emailForm.access === 'send') {
        creds['smtp'] = {
          host: this.emailForm.smtp_host.trim(),
          port: Number(this.emailForm.smtp_port) || 587,
          security: this.emailForm.smtp_security,
        };
      }
      return creds;
    }
    if (this.formData.type === 'generic_file') {
      const entries: CredentialFileEntry[] = [];
      for (const f of this.genericFiles) {
        if (!f.target_path.trim() || f.contents.length === 0) continue;
        const entry: CredentialFileEntry = {
          contents: f.contents,
          target_path: f.target_path.trim(),
        };
        if (f.name.trim()) entry.name = f.name.trim();
        if (f.mode.trim()) entry.mode = f.mode.trim();
        if (f.env_var.trim()) entry.env_var = f.env_var.trim();
        entries.push(entry);
      }
      if (entries.length === 0) return isEditing ? undefined : { files: [] };
      return { files: entries };
    }
    if (!this.formCredentials.username && !this.formCredentials.password) return undefined;
    return {
      username: this.formCredentials.username || undefined,
      password: this.formCredentials.password || undefined,
    };
  }

  private kvRowsToObject(rows: KeyValueRow[]): Record<string, string> {
    const values: Record<string, string> = {};
    for (const row of rows) {
      const key = row.key.trim();
      if (key) values[key] = row.value;
    }
    return values;
  }

  private mcpCredentialsWereEntered(): boolean {
    return (
      this.mcpTransportDirty ||
      !!this.formData.mcpToken ||
      !!this.formData.mcpCommand.trim() ||
      !!this.formData.mcpArgs.trim() ||
      this.formData.mcpHeaders.some((row) => !!row.key.trim()) ||
      this.formData.mcpEnv.some((row) => !!row.key.trim())
    );
  }

  private resetFormData(): void {
    this.formData = {
      name: '',
      type: 'generic',
      connection_url: '',
      description: '',
      cli_hint: '',
      default_branch: '',
      root_path: '',
      forge: '',
      mcpTransport: 'http',
      mcpToken: '',
      mcpHeaders: [],
      mcpCommand: '',
      mcpArgs: '',
      mcpEnv: [],
      is_global: false,
      read_only: true,
      scope_mode: null,
      auto_attach: false,
      policy_revision: null,
    };
    this.editingOriginal = null;
    this.formCredentials = { username: '', password: '' };
    this.emailForm = this.defaultEmailForm();
    this.gitAuthMethod = 'token';
    this.gitSshKey = '';
    this.envVars = [];
    this.mcpTransportDirty = false;
    this.forgeDirty = false;
    this.connectionUrlDirty = false;
    this.kubeconfigContent = '';
    this.genericFiles = [];
    this.formProjectIds.set(new Set());
    this.scopeSelectionInitialized = false;
    this.scopeTargets.set([]);
    this.scopeTargetsNextCursor.set(null);
    this.projectSearch.set('');
    this.contextScopeTargetRequestSerial += 1;
  }

  // ===== Generic file editor =====

  addGenericFile(): void {
    if (this.genericFiles.length >= 5) return;
    this.genericFiles.push({
      name: '',
      target_path: '',
      contents: '',
      mode: '0600',
      env_var: '',
    });
  }

  removeGenericFile(index: number): void {
    this.genericFiles.splice(index, 1);
  }

  // ===== File upload (reads selected file into a textarea client-side) =====

  /** Read a user-selected file into the target text variable. No multipart upload. */
  onUploadFile(evt: Event, target: 'kubeconfig' | 'ssh_key' | { file: number }): void {
    const input = evt.target as HTMLInputElement;
    const file = input.files?.[0];
    if (!file) return;
    // Hard-cap at 64KB to match the backend validator; surface a clear error otherwise.
    if (file.size > 64 * 1024) {
      this.errorMessage.set(
        this.transloco.translate('datasources.messages.fileTooLarge') ||
          'File exceeds 64 KB',
      );
      input.value = '';
      return;
    }
    const reader = new FileReader();
    reader.onload = () => {
      const text = typeof reader.result === 'string' ? reader.result : '';
      if (target === 'kubeconfig') {
        this.kubeconfigContent = text;
      } else if (target === 'ssh_key') {
        this.gitSshKey = text;
      } else if (typeof target === 'object' && 'file' in target) {
        const idx = target.file;
        if (this.genericFiles[idx]) {
          this.genericFiles[idx].contents = text;
          if (!this.genericFiles[idx].name) {
            this.genericFiles[idx].name = file.name;
          }
        }
      }
      input.value = '';
    };
    reader.onerror = () => {
      this.errorMessage.set('Failed to read file');
      input.value = '';
    };
    reader.readAsText(file);
  }

  private clearMessages(): void {
    this.successMessage.set(null);
    this.errorMessage.set(null);
  }
}

import {Component, computed, inject, input, output, signal} from '@angular/core';
import {FormsModule} from '@angular/forms';
import {TranslocoPipe} from '@jsverse/transloco';
import {AppIconComponent} from '../../ui/icon';
import {AppTooltipDirective} from '../../ui/tooltip';
import {pinResolvedValue, readConfigPath, resolveMatrixForModel, SettingsMode} from './agent-settings.types';
import {PinOnInteractDirective} from './pin-on-interact.directive';
import {reasoningOptionsForModel} from './reasoning-options';
import {ModelService} from '../../core/services/model.service';
import {liftLegacyTiers} from '../experts/expert-config';

const GIB_BYTES = 1024 ** 3;
const VM_SIZE_UNITS: Record<string, number> = {
  Ki: 1024, Mi: 1024 ** 2, Gi: GIB_BYTES, Ti: 1024 ** 4,
  K: 1000, M: 1000 ** 2, G: 1000 ** 3, T: 1000 ** 4,
};

function vmQuantityInGiB(value: unknown): number | null {
  if (typeof value !== 'string') return null;
  const match = /^(\d+)(Ki|Mi|Gi|Ti|K|M|G|T)$/.exec(value.trim());
  if (!match) return null;
  const gib = Number(match[1]) * VM_SIZE_UNITS[match[2]] / GIB_BYTES;
  return Number.isFinite(gib) && gib > 0 ? gib : null;
}

function validVmSizeGiB(value: number | null): value is number {
  return value !== null && Number.isSafeInteger(value) && value > 0;
}

/**
 * Advanced settings tab: collapsible accordion sections for power-user settings.
 * One level of accordion only (no nesting).
 */
@Component({
  selector: 'app-advanced-accordion',
  standalone: true,
    imports: [FormsModule, TranslocoPipe, AppIconComponent, AppTooltipDirective, PinOnInteractDirective],
  template: `
    <div class="advanced-container">
      <!-- Inference Parameters -->
      <div class="accordion-section" [class.expanded]="expanded().has('inference')">
        <button type="button" class="accordion-header" (click)="toggleSection('inference')">
          <app-icon size="md" class="accordion-icon">{{ expanded().has('inference') ? 'expand_less' : 'expand_more' }}</app-icon>
          {{ 'advanced.sections.inference' | transloco }}
          @if (inferenceModifiedCount() > 0) {
            <span class="modified-badge">{{ inferenceModifiedCount() }}</span>
          }
        </button>
        @if (expanded().has('inference')) {
          <div class="accordion-body">
            <!-- One model runs the whole job since U1, so there is ONE
                 inference section (the per-phase strategic/tactical pair is
                 gone). Job mode owns the reasoning level here; in session
                 mode the Reasoning select lives in the Settings tab's MODEL
                 group (model-group.component.ts), the single session-mode
                 writer of llm.reasoning_level. -->
            @if (mode() === 'job') {
              <div class="field-row" [class.modified]="reasoning() !== null">
                <label class="field-label">{{ 'advanced.labels.reasoning' | transloco }}</label>
                <div class="field-control">
                  <select class="form-input"
                    [ngModel]="reasoning() ?? resolvedReasoning()"
                    appPinOnInteract (pin)="pinValue(reasoning, resolvedReasoning())"
                    (ngModelChange)="onReasoningChange($event)"
                    [disabled]="disabled()">
                    @for (opt of reasoningOptions(); track opt.value) {
                      @if (opt.value === null) {
                        <option [ngValue]="null">{{ opt.label }}</option>
                      } @else {
                        <option [value]="opt.value">{{ opt.label }}</option>
                      }
                    }
                  </select>
                  @if (reasoning() !== null) {
                    <button type="button" class="reset-btn" (click)="reasoning.set(null); emitChange()"><app-icon size="xs">close</app-icon></button>
                  }
                </div>
              </div>
            }
            <div class="field-row" [class.modified]="temperature() !== null">
              <label class="field-label">
                {{ 'advanced.labels.temperature' | transloco: {value: effectiveTemp()} }}
              </label>
              <div class="slider-row">
                <span class="slider-label">0</span>
                <input type="range" class="form-range" min="0" max="2" step="0.1"
                  [ngModel]="temperature() ?? resolvedTemp()"
                  (ngModelChange)="onTempChange($event)"
                  [disabled]="disabled()">
                <span class="slider-label">2</span>
                @if (temperature() !== null) {
                  <button type="button" class="reset-btn" (click)="temperature.set(null); emitChange()"><app-icon size="xs">close</app-icon></button>
                }
              </div>
            </div>
            <div class="field-row toggle-row" [class.modified]="multimodal() !== null">
              <label class="toggle-label">
                <input type="checkbox"
                  [checked]="multimodal() ?? resolvedMultimodal()"
                  (change)="onMultimodalChange($event)"
                  [disabled]="disabled()">
                <span>{{ 'advanced.labels.multimodal' | transloco }}</span>
              </label>
              @if (multimodal() !== null) {
                <button type="button" class="reset-btn" (click)="multimodal.set(null); emitChange()"><app-icon size="xs">close</app-icon></button>
              }
            </div>

            <!-- Shared inference params -->
            <div class="shared-params">
              <div class="field-row" [class.modified]="topP() !== null">
                <label class="field-label">{{ 'advanced.labels.topP' | transloco }}</label>
                <div class="field-control">
                  <input type="number" class="form-input compact-input" min="0" max="1" step="0.05"
                    [ngModel]="topP() ?? resolvedTopP()"
                    (ngModelChange)="onTopPChange($event)"
                    [disabled]="disabled()"
                    [placeholder]="'advanced.hints.autoPlaceholder' | transloco">
                  @if (topP() !== null) {
                    <button type="button" class="reset-btn" (click)="topP.set(null); emitChange()"><app-icon size="xs">close</app-icon></button>
                  }
                </div>
              </div>
              <div class="field-row" [class.modified]="topK() !== null">
                <label class="field-label">{{ 'advanced.labels.topK' | transloco }}</label>
                <div class="field-control">
                  <input type="number" class="form-input compact-input" min="0" step="1"
                    [ngModel]="topK() ?? resolvedTopK()"
                    (ngModelChange)="onTopKChange($event)"
                    [disabled]="disabled()"
                    [placeholder]="'advanced.hints.autoPlaceholder' | transloco">
                  @if (topK() !== null) {
                    <button type="button" class="reset-btn" (click)="topK.set(null); emitChange()"><app-icon size="xs">close</app-icon></button>
                  }
                </div>
              </div>
              <div class="field-row" [class.modified]="maxOutputTokens() !== null">
                <label class="field-label">{{ 'advanced.labels.maxOutputTokens' | transloco }}</label>
                <div class="field-control">
                  <input type="number" class="form-input compact-input" min="1" step="100"
                    [ngModel]="maxOutputTokens() ?? resolvedMaxOutputTokens()"
                    (ngModelChange)="onMaxOutputTokensChange($event)"
                    [disabled]="disabled()"
                    [placeholder]="'advanced.hints.autoPlaceholder' | transloco">
                  @if (maxOutputTokens() !== null) {
                    <button type="button" class="reset-btn" (click)="maxOutputTokens.set(null); emitChange()"><app-icon size="xs">close</app-icon></button>
                  }
                </div>
              </div>
              <div class="field-row toggle-row" [class.modified]="parallelToolCalls() !== null">
                <label class="toggle-label">
                  <input type="checkbox"
                    [checked]="parallelToolCalls() ?? resolvedParallelToolCalls()"
                    (change)="onParallelToolCallsChange($event)"
                    [disabled]="disabled()">
                  <span>{{ 'advanced.labels.parallelToolCalls' | transloco }}</span>
                </label>
                <app-icon size="xs" class="info-icon" tabindex="0"
                  [appTooltip]="'advanced.hints.parallelToolCalls' | transloco"
                  [attr.aria-label]="'advanced.hints.parallelToolCalls' | transloco">info</app-icon>
                @if (parallelToolCalls() !== null) {
                  <button type="button" class="reset-btn" (click)="parallelToolCalls.set(null); emitChange()"><app-icon size="xs">close</app-icon></button>
                }
              </div>
            </div>
          </div>
        }
      </div>

      <!-- Limits & Safety -->
      <div class="accordion-section" [class.expanded]="expanded().has('limits')">
        <button type="button" class="accordion-header" (click)="toggleSection('limits')">
          <app-icon size="md" class="accordion-icon">{{ expanded().has('limits') ? 'expand_less' : 'expand_more' }}</app-icon>
          {{ 'advanced.sections.limits' | transloco }}
        </button>
        @if (expanded().has('limits')) {
          <div class="accordion-body">
            <div class="field-row" [class.modified]="messageCountThreshold() !== null">
              <label class="field-label">{{ 'advanced.labels.messageCountThreshold' | transloco }}</label>
              <div class="field-control">
                <input type="number" class="form-input compact-input" min="1"
                  [ngModel]="messageCountThreshold() ?? resolvedMessageCountThreshold()"
                  (ngModelChange)="messageCountThreshold.set($event); emitChange()"
                  [disabled]="disabled()">
                @if (messageCountThreshold() !== null) {
                  <button type="button" class="reset-btn" (click)="messageCountThreshold.set(null); emitChange()"><app-icon size="xs">close</app-icon></button>
                }
              </div>
            </div>
            <div class="field-row" [class.modified]="toolRetryCount() !== null">
              <label class="field-label">{{ 'advanced.labels.toolRetryCount' | transloco }}</label>
              <div class="field-control">
                <input type="number" class="form-input compact-input" min="0" max="10"
                  [ngModel]="toolRetryCount() ?? resolvedToolRetryCount()"
                  (ngModelChange)="toolRetryCount.set($event); emitChange()"
                  [disabled]="disabled()">
                @if (toolRetryCount() !== null) {
                  <button type="button" class="reset-btn" (click)="toolRetryCount.set(null); emitChange()"><app-icon size="xs">close</app-icon></button>
                }
              </div>
            </div>
            <div class="field-row" [class.modified]="progressStallThreshold() !== null">
              <label class="field-label">{{ 'advanced.labels.progressStallThreshold' | transloco }}</label>
              <div class="field-control">
                <input type="number" class="form-input compact-input" min="1"
                  [ngModel]="progressStallThreshold() ?? resolvedProgressStallThreshold()"
                  (ngModelChange)="progressStallThreshold.set($event); emitChange()"
                  [disabled]="disabled()">
                @if (progressStallThreshold() !== null) {
                  <button type="button" class="reset-btn" (click)="progressStallThreshold.set(null); emitChange()"><app-icon size="xs">close</app-icon></button>
                }
              </div>
            </div>
            <div class="field-row" [class.modified]="maxToolCallsPerPhase() !== null">
              <label class="field-label">{{ 'advanced.labels.maxToolCallsPerPhase' | transloco }}</label>
              <div class="field-control">
                <input type="number" class="form-input compact-input" min="1"
                  [ngModel]="maxToolCallsPerPhase() ?? resolvedMaxToolCallsPerPhase()"
                  (ngModelChange)="maxToolCallsPerPhase.set($event); emitChange()"
                  [disabled]="disabled()">
                @if (maxToolCallsPerPhase() !== null) {
                  <button type="button" class="reset-btn" (click)="maxToolCallsPerPhase.set(null); emitChange()"><app-icon size="xs">close</app-icon></button>
                }
              </div>
            </div>
          </div>
        }
      </div>

      <!-- Memory Tuning -->
      <div class="accordion-section" [class.expanded]="expanded().has('memory')">
        <button type="button" class="accordion-header" (click)="toggleSection('memory')">
          <app-icon size="md" class="accordion-icon">{{ expanded().has('memory') ? 'expand_less' : 'expand_more' }}</app-icon>
          {{ 'advanced.sections.memory' | transloco }}
        </button>
        @if (expanded().has('memory')) {
          <div class="accordion-body">
            <div class="field-row toggle-row" [class.modified]="memoryEnabled() !== null">
              <label class="toggle-label">
                <input type="checkbox"
                  [checked]="memoryEnabled() ?? resolvedMemoryEnabled()"
                  (change)="onMemoryEnabledChange($event)"
                  [disabled]="disabled()">
                <span>{{ 'advanced.labels.memoryEnabled' | transloco }}</span>
              </label>
              @if (memoryEnabled() !== null) {
                <button type="button" class="reset-btn" (click)="memoryEnabled.set(null); emitChange()"><app-icon size="xs">close</app-icon></button>
              }
            </div>
            <div class="field-row" [class.modified]="memoryBudget() !== null">
              <label class="field-label">{{ 'advanced.labels.budgetTokens' | transloco }}</label>
              <div class="field-control">
                <input type="number" class="form-input compact-input" min="0" step="1000"
                  [ngModel]="memoryBudget() ?? resolvedMemoryBudget()"
                  (ngModelChange)="memoryBudget.set($event); emitChange()"
                  [disabled]="disabled()">
                @if (memoryBudget() !== null) {
                  <button type="button" class="reset-btn" (click)="memoryBudget.set(null); emitChange()"><app-icon size="xs">close</app-icon></button>
                }
              </div>
            </div>
          </div>
        }
      </div>

      <!-- Context Management -->
      <div class="accordion-section" [class.expanded]="expanded().has('context')">
        <button type="button" class="accordion-header" (click)="toggleSection('context')">
          <app-icon size="md" class="accordion-icon">{{ expanded().has('context') ? 'expand_less' : 'expand_more' }}</app-icon>
          {{ 'advanced.sections.context' | transloco }}
        </button>
        @if (expanded().has('context')) {
          <div class="accordion-body">
            <div class="field-row toggle-row" [class.modified]="compactOnArchive() !== null">
              <label class="toggle-label">
                <input type="checkbox"
                  [checked]="compactOnArchive() ?? resolvedCompactOnArchive()"
                  (change)="onCompactOnArchiveChange($event)"
                  [disabled]="disabled()">
                <span>{{ 'advanced.labels.compactOnArchive' | transloco }}</span>
              </label>
              @if (compactOnArchive() !== null) {
                <button type="button" class="reset-btn" (click)="compactOnArchive.set(null); emitChange()"><app-icon size="xs">close</app-icon></button>
              }
            </div>
            <div class="field-row" [class.modified]="keepRecentToolResults() !== null">
              <label class="field-label">{{ 'advanced.labels.keepRecentToolResults' | transloco }}</label>
              <div class="field-control">
                <input type="number" class="form-input compact-input" min="0"
                  [ngModel]="keepRecentToolResults() ?? resolvedKeepRecentToolResults()"
                  (ngModelChange)="keepRecentToolResults.set($event); emitChange()"
                  [disabled]="disabled()">
                @if (keepRecentToolResults() !== null) {
                  <button type="button" class="reset-btn" (click)="keepRecentToolResults.set(null); emitChange()"><app-icon size="xs">close</app-icon></button>
                }
              </div>
            </div>
            <div class="field-row" [class.modified]="keepRecentMessages() !== null">
              <label class="field-label">{{ 'advanced.labels.keepRecentMessages' | transloco }}</label>
              <div class="field-control">
                <input type="number" class="form-input compact-input" min="0"
                  [ngModel]="keepRecentMessages() ?? resolvedKeepRecentMessages()"
                  (ngModelChange)="keepRecentMessages.set($event); emitChange()"
                  [disabled]="disabled()">
                @if (keepRecentMessages() !== null) {
                  <button type="button" class="reset-btn" (click)="keepRecentMessages.set(null); emitChange()"><app-icon size="xs">close</app-icon></button>
                }
              </div>
            </div>
          </div>
        }
      </div>

      <!-- Workspace -->
      <div class="accordion-section" [class.expanded]="expanded().has('workspace')">
        <button type="button" class="accordion-header" (click)="toggleSection('workspace')">
          <app-icon size="md" class="accordion-icon">{{ expanded().has('workspace') ? 'expand_less' : 'expand_more' }}</app-icon>
          {{ 'advanced.sections.workspace' | transloco }}
        </button>
        @if (expanded().has('workspace')) {
          <div class="accordion-body">
            <!-- The backend selector itself is a level-1 control, in the
                 Settings tab's EXECUTION group. This section keeps the tuning
                 that hangs off it and explains the greying below. -->
            @if (isLiteBackend()) {
              <p class="field-hint lite-hint">{{ (isNoneBackend() ? 'advanced.hints.noneBackend' : 'advanced.hints.virtualBackend') | transloco }}</p>
            }
            @if (effectiveBackend() === 'vm') {
              <div class="field-row" [class.modified]="vmCpuCores() !== null">
                <label class="field-label">{{ 'advanced.labels.vmCpuCores' | transloco }}</label>
                <div class="field-control">
                  <input type="number" class="form-input compact-input" min="1" max="16" step="1"
                    [ngModel]="vmCpuCores() ?? resolvedVmCpuCores()"
                    (ngModelChange)="vmCpuCores.set($event); emitChange()"
                    [disabled]="disabled()">
                  @if (vmCpuCores() !== null) {
                    <button type="button" class="reset-btn" (click)="vmCpuCores.set(null); emitChange()"><app-icon size="xs">close</app-icon></button>
                  }
                </div>
              </div>
              <div class="field-row" [class.modified]="vmMemory() !== null">
                <label class="field-label">{{ 'advanced.labels.vmMemory' | transloco }}</label>
                <div class="field-control">
                  <input type="number" class="form-input compact-input" step="any" placeholder="16"
                    [ngModel]="vmMemory() ?? resolvedVmMemory()"
                    (ngModelChange)="vmMemory.set($event); emitChange()"
                    [attr.aria-invalid]="vmMemoryInvalid()"
                    [disabled]="disabled()">
                  @if (vmMemory() !== null) {
                    <button type="button" class="reset-btn" (click)="vmMemory.set(null); emitChange()"><app-icon size="xs">close</app-icon></button>
                  }
                </div>
                @if (vmMemoryInvalid()) {
                  <span class="field-hint size-error" role="alert">{{ 'advanced.hints.vmSizeWholeGiB' | transloco }}</span>
                }
              </div>
              <div class="field-row" [class.modified]="vmDiskSize() !== null">
                <label class="field-label">{{ 'advanced.labels.vmDiskSize' | transloco }}</label>
                <div class="field-control">
                  <input type="number" class="form-input compact-input" step="any" placeholder="20"
                    [ngModel]="vmDiskSize() ?? resolvedVmDiskSize()"
                    (ngModelChange)="vmDiskSize.set($event); emitChange()"
                    [attr.aria-invalid]="vmDiskSizeInvalid()"
                    [disabled]="disabled()">
                  @if (vmDiskSize() !== null) {
                    <button type="button" class="reset-btn" (click)="vmDiskSize.set(null); emitChange()"><app-icon size="xs">close</app-icon></button>
                  }
                </div>
                @if (vmDiskSizeInvalid()) {
                  <span class="field-hint size-error" role="alert">{{ 'advanced.hints.vmSizeWholeGiB' | transloco }}</span>
                }
              </div>
            }
            <div class="field-row" [class.modified]="maxReadWords() !== null">
              <label class="field-label">{{ 'advanced.labels.maxReadWords' | transloco }}</label>
              <div class="field-control">
                <input type="number" class="form-input compact-input" min="0" step="1000"
                  [ngModel]="maxReadWords() ?? resolvedMaxReadWords()"
                  (ngModelChange)="maxReadWords.set($event); emitChange()"
                  [disabled]="disabled() || isNoneBackend()">
                @if (maxReadWords() !== null) {
                  <button type="button" class="reset-btn" (click)="maxReadWords.set(null); emitChange()"><app-icon size="xs">close</app-icon></button>
                }
              </div>
            </div>
            <div class="field-row" [class.modified]="maxWriteWords() !== null">
              <label class="field-label">{{ 'advanced.labels.maxWriteWords' | transloco }}</label>
              <div class="field-control">
                <input type="number" class="form-input compact-input" min="0" step="1000"
                  [ngModel]="maxWriteWords() ?? resolvedMaxWriteWords()"
                  (ngModelChange)="maxWriteWords.set($event); emitChange()"
                  [disabled]="disabled() || isNoneBackend()">
                @if (maxWriteWords() !== null) {
                  <button type="button" class="reset-btn" (click)="maxWriteWords.set(null); emitChange()"><app-icon size="xs">close</app-icon></button>
                }
              </div>
            </div>
            <div class="field-row toggle-row" [class.modified]="gitVersioning() !== null">
              <label class="toggle-label">
                <input type="checkbox"
                  [checked]="gitVersioning() ?? resolvedGitVersioning()"
                  (change)="onGitVersioningChange($event)"
                  [disabled]="disabled() || isLiteBackend()">
                <span>{{ 'advanced.labels.gitVersioning' | transloco }}</span>
              </label>
              @if (gitVersioning() !== null) {
                <button type="button" class="reset-btn" (click)="gitVersioning.set(null); emitChange()"><app-icon size="xs">close</app-icon></button>
              }
            </div>
          </div>
        }
      </div>

      <!-- Shell -->
      <div class="accordion-section" [class.expanded]="expanded().has('shell')">
        <button type="button" class="accordion-header" (click)="toggleSection('shell')">
          <app-icon size="md" class="accordion-icon">{{ expanded().has('shell') ? 'expand_less' : 'expand_more' }}</app-icon>
          {{ 'advanced.sections.shell' | transloco }}
        </button>
        @if (expanded().has('shell')) {
          <div class="accordion-body">
            <div class="field-row" [class.modified]="shellMode() !== null">
              <label class="field-label">{{ 'advanced.labels.mode' | transloco }}</label>
              <div class="field-control">
                <select class="form-input"
                  [ngModel]="shellMode() ?? resolvedShellMode()"
                  appPinOnInteract (pin)="pinValue(shellMode, resolvedShellMode())"
                  (ngModelChange)="shellMode.set($event); emitChange()"
                  [disabled]="disabled() || isLiteBackend()">
                  <option value="stateless">{{ 'advanced.options.stateless' | transloco }}</option>
                  <option value="persistent">{{ 'advanced.options.persistent' | transloco }}</option>
                </select>
                @if (shellMode() !== null) {
                  <button type="button" class="reset-btn" (click)="shellMode.set(null); emitChange()"><app-icon size="xs">close</app-icon></button>
                }
              </div>
            </div>
            <div class="field-row toggle-row" [class.modified]="shellSandbox() !== null">
              <label class="toggle-label">
                <input type="checkbox"
                  [checked]="shellSandbox() ?? resolvedShellSandbox()"
                  (change)="onShellSandboxChange($event)"
                  [disabled]="disabled() || isLiteBackend()">
                <span>{{ 'advanced.labels.sandbox' | transloco }}</span>
              </label>
              @if (shellSandbox() !== null) {
                <button type="button" class="reset-btn" (click)="shellSandbox.set(null); emitChange()"><app-icon size="xs">close</app-icon></button>
              }
            </div>
            <div class="field-row" [class.modified]="shellTimeout() !== null">
              <label class="field-label">{{ 'advanced.labels.defaultTimeout' | transloco }}</label>
              <div class="field-control">
                <input type="number" class="form-input compact-input" min="1"
                  [ngModel]="shellTimeout() ?? resolvedShellTimeout()"
                  (ngModelChange)="shellTimeout.set($event); emitChange()"
                  [disabled]="disabled() || isLiteBackend()">
                @if (shellTimeout() !== null) {
                  <button type="button" class="reset-btn" (click)="shellTimeout.set(null); emitChange()"><app-icon size="xs">close</app-icon></button>
                }
              </div>
            </div>
            <div class="field-row" [class.modified]="sudoAction() !== null">
              <label class="field-label">{{ 'advanced.labels.sudoAction' | transloco }}</label>
              <div class="field-control">
                <select class="form-input"
                  [ngModel]="sudoAction() ?? resolvedSudoAction()"
                  appPinOnInteract (pin)="pinValue(sudoAction, resolvedSudoAction())"
                  (ngModelChange)="sudoAction.set($event); emitChange()"
                  [disabled]="disabled() || isLiteBackend()">
                  <option value="freeze">{{ 'advanced.options.sudoFreeze' | transloco }}</option>
                  <option value="block">{{ 'advanced.options.sudoBlock' | transloco }}</option>
                  <option value="allow">{{ 'advanced.options.sudoAllow' | transloco }}</option>
                </select>
                @if (sudoAction() !== null) {
                  <button type="button" class="reset-btn" (click)="sudoAction.set(null); emitChange()"><app-icon size="xs">close</app-icon></button>
                }
              </div>
            </div>
          </div>
        }
      </div>

      <!-- Research -->
      <div class="accordion-section" [class.expanded]="expanded().has('research')">
        <button type="button" class="accordion-header" (click)="toggleSection('research')">
          <app-icon size="md" class="accordion-icon">{{ expanded().has('research') ? 'expand_less' : 'expand_more' }}</app-icon>
          {{ 'advanced.sections.research' | transloco }}
        </button>
        @if (expanded().has('research')) {
          <div class="accordion-body">
            <div class="field-row toggle-row" [class.modified]="proxyEnabled() !== null">
              <label class="toggle-label">
                <input type="checkbox"
                  [checked]="proxyEnabled() ?? resolvedProxyEnabled()"
                  (change)="onProxyEnabledChange($event)"
                  [disabled]="disabled()">
                <span>{{ 'advanced.labels.proxyEnabled' | transloco }}</span>
              </label>
              @if (proxyEnabled() !== null) {
                <button type="button" class="reset-btn" (click)="proxyEnabled.set(null); emitChange()"><app-icon size="xs">close</app-icon></button>
              }
            </div>
          </div>
        }
      </div>

      <!-- Auxiliary LLM -->
      <div class="accordion-section" [class.expanded]="expanded().has('auxiliary')">
        <button type="button" class="accordion-header" (click)="toggleSection('auxiliary')">
          <app-icon size="md" class="accordion-icon">{{ expanded().has('auxiliary') ? 'expand_less' : 'expand_more' }}</app-icon>
          {{ 'advanced.sections.auxiliary' | transloco }}
        </button>
        @if (expanded().has('auxiliary')) {
          <div class="accordion-body">
            <div class="field-row toggle-row" [class.modified]="auxEnabled() !== null">
              <label class="toggle-label">
                <input type="checkbox"
                  [checked]="auxEnabled() ?? resolvedAuxEnabled()"
                  (change)="onAuxEnabledChange($event)"
                  [disabled]="disabled()">
                <span>{{ 'advanced.labels.auxEnabled' | transloco }}</span>
              </label>
              @if (auxEnabled() !== null) {
                <button type="button" class="reset-btn" (click)="auxEnabled.set(null); emitChange()"><app-icon size="xs">close</app-icon></button>
              }
            </div>
            @if (effectiveAuxEnabled()) {
              <div class="field-row" [class.modified]="auxModel() !== null">
                <label class="field-label">{{ 'advanced.labels.model' | transloco }}</label>
                <div class="field-control">
                  <input type="text" class="form-input"
                    [ngModel]="auxModel() ?? resolvedAuxModel()"
                    (ngModelChange)="auxModel.set($event); emitChange()"
                    [disabled]="disabled()"
                    placeholder="RedHatAI/gemma-4-31B-it-FP8-Dynamic">
                  @if (auxModel() !== null) {
                    <button type="button" class="reset-btn" (click)="auxModel.set(null); emitChange()"><app-icon size="xs">close</app-icon></button>
                  }
                </div>
              </div>
              <div class="field-row" [class.modified]="auxTemperature() !== null">
                <label class="field-label">
                  {{ 'advanced.labels.temperature' | transloco: {value: effectiveAuxTemp()} }}
                </label>
                <div class="slider-row">
                  <span class="slider-label">0</span>
                  <input type="range" class="form-range" min="0" max="2" step="0.1"
                    [ngModel]="auxTemperature() ?? resolvedAuxTemperature()"
                    (ngModelChange)="auxTemperature.set(clampTemp($event)); emitChange()"
                    [disabled]="disabled()">
                  <span class="slider-label">2</span>
                  @if (auxTemperature() !== null) {
                    <button type="button" class="reset-btn" (click)="auxTemperature.set(null); emitChange()"><app-icon size="xs">close</app-icon></button>
                  }
                </div>
              </div>
            }
          </div>
        }
      </div>

      <!-- Session-specific: idle timeout -->
      @if (mode() === 'session') {
        <div class="accordion-section" [class.expanded]="expanded().has('session')">
          <button type="button" class="accordion-header" (click)="toggleSection('session')">
            <app-icon size="md" class="accordion-icon">{{ expanded().has('session') ? 'expand_less' : 'expand_more' }}</app-icon>
            {{ 'advanced.sections.session' | transloco }}
          </button>
          @if (expanded().has('session')) {
            <div class="accordion-body">
              <div class="field-row" [class.modified]="idleTimeout() !== null">
                <label class="field-label">{{ 'advanced.labels.idleTimeout' | transloco }}</label>
                <div class="field-control">
                  <input type="number" class="form-input compact-input" min="0"
                    [ngModel]="idleTimeout() ?? resolvedIdleTimeout()"
                    (ngModelChange)="idleTimeout.set($event); emitChange()"
                    [disabled]="disabled()">
                  @if (idleTimeout() !== null) {
                    <button type="button" class="reset-btn" (click)="idleTimeout.set(null); emitChange()"><app-icon size="xs">close</app-icon></button>
                  }
                </div>
                <span class="field-hint">{{ 'advanced.hints.idleDisabled' | transloco }}</span>
              </div>
            </div>
          }
        </div>
      }

    </div>
  `,
  styles: [`
    .advanced-container {
      display: flex;
      flex-direction: column;
      gap: 2px;
    }
    .accordion-section {
      border: 1px solid var(--border-color, var(--surface-0));
      border-radius: var(--radius-control);
      overflow: hidden;
    }
    .accordion-section.expanded {
      border-color: var(--border-color, var(--surface-1));
    }
    .accordion-header {
      display: flex;
      align-items: center;
      gap: 8px;
      width: 100%;
      padding: 10px 12px;
      border: none;
      background: var(--surface-0, var(--surface-0));
      color: var(--text-primary, var(--text-primary));
      font-size: 13px;
      font-weight: 500;
      cursor: pointer;
      text-align: left;
      transition: background 0.15s;
    }
    .accordion-header:hover {
      background: rgba(255, 255, 255, 0.05);
    }
    .accordion-icon {
      color: var(--text-muted);
    }
    .modified-badge {
      margin-left: auto;
      padding: 1px 6px;
      border-radius: var(--radius-pill);
      background: color-mix(in srgb, var(--accent-color) 20%, transparent);
      color: var(--accent-color, var(--accent-color));
      font-size: 11px;
      font-weight: 600;
    }
    .accordion-body {
      padding: 12px 14px;
      background: var(--panel-bg, var(--panel-bg));
    }
    .shared-params {
      padding-top: 8px;
      border-top: 1px solid var(--border-color, var(--surface-0));
    }
    .field-row {
      margin-bottom: 10px;
      padding-left: 8px;
      border-left: 2px solid transparent;
      transition: border-color 0.15s;
    }
    .field-row.modified {
      border-left-color: var(--accent-color, var(--accent-color));
    }
    .field-label {
      display: block;
      font-size: 12px;
      font-weight: 500;
      color: var(--text-primary, var(--text-primary));
      margin-bottom: 4px;
    }
    .field-control {
      display: flex;
      align-items: center;
      gap: 6px;
    }
    .field-hint {
      display: block;
      font-size: 11px;
      color: var(--text-muted);
      margin-top: 2px;
    }
    .size-error { color: var(--danger); }
    .info-icon {
      color: var(--text-muted);
      cursor: help;
      margin-left: 2px;
    }
    .info-icon:hover, .info-icon:focus-visible {
      color: var(--text-secondary);
    }
    .lite-hint {
      margin: 4px 0 8px;
      padding: 6px 9px;
      line-height: 1.4;
      border-left: 2px solid var(--accent-color, var(--accent-color));
      background: var(--surface-1, rgba(127, 127, 127, 0.08));
      border-radius: var(--radius-tag);
    }
    .form-input {
      flex: 1;
      padding: 7px 10px;
      border: 1px solid var(--border-color, var(--surface-1));
      border-radius: var(--radius-control);
      background: var(--surface-0, var(--surface-0));
      color: var(--text-primary, var(--text-primary));
      font-family: inherit;
      font-size: 13px;
    }
    .form-input:focus {
      outline: none;
      border-color: var(--accent-color, var(--accent-color));
    }
    .form-input:disabled {
      opacity: 0.6;
      cursor: not-allowed;
    }
    .compact-input {
      max-width: 120px;
    }
    .slider-row {
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .slider-label {
      font-size: 11px;
      color: var(--text-muted);
      min-width: 12px;
      text-align: center;
    }
    .form-range {
      flex: 1;
      accent-color: var(--accent-color, var(--accent-color));
    }
    .toggle-row {
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .toggle-label {
      display: flex;
      align-items: center;
      gap: 6px;
      font-size: 13px;
      color: var(--text-primary, var(--text-primary));
      cursor: pointer;
    }
    .toggle-label input[type="checkbox"] {
      accent-color: var(--accent-color, var(--accent-color));
    }
    .reset-btn {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 20px;
      height: 20px;
      border: none;
      border-radius: 50%;
      background: rgba(255, 255, 255, 0.08);
      color: var(--text-muted);
      cursor: pointer;
      flex-shrink: 0;
    }
    .reset-btn:hover {
      background: var(--danger-tint);
      color: var(--danger);
    }
  `],
})
export class AdvancedAccordionComponent {
  config = input<Record<string, unknown>>({});
  mode = input<SettingsMode>('job');
  disabled = input(false);
  /** Raw settings_matrix for model-family-aware defaults. */
  settingsMatrix = input<Record<string, Record<string, unknown>>>({});
  /** The model chosen in the Settings tab's MODEL group (null = inherit the
   *  config default) — the ONE model since U1; drives the family-aware
   *  defaults and the reasoning options. */
  modelOverride = input<string | null>(null);
  /** The workspace backend chosen in the Settings tab's EXECUTION group, or
   *  null when it is unset there. The selector is a level-1 control and lives
   *  in ExecutionGroupComponent; this section reads it to grey the tools a lite
   *  tier cannot run and to scope what its own `getOverrides()` emits. */
  backendOverride = input<string | null>(null);

  change = output<void>();

  // Accordion state
  readonly expanded = signal<Set<string>>(new Set());

  // --- Inference params (one model, one section) ---
  /** Job mode only — session mode's reasoning is owned by ModelGroupComponent. */
  readonly reasoning = signal<string | null>(null);
  readonly temperature = signal<number | null>(null);
  readonly multimodal = signal<boolean | null>(null);
  readonly topP = signal<number | null>(null);
  readonly topK = signal<number | null>(null);
  readonly maxOutputTokens = signal<number | null>(null);
  readonly parallelToolCalls = signal<boolean | null>(null);

  // --- Limits ---
  readonly messageCountThreshold = signal<number | null>(null);
  readonly toolRetryCount = signal<number | null>(null);
  readonly progressStallThreshold = signal<number | null>(null);
  readonly maxToolCallsPerPhase = signal<number | null>(null);

  // --- Memory ---
  readonly memoryEnabled = signal<boolean | null>(null);
  readonly memoryBudget = signal<number | null>(null);

  // --- Context ---
  readonly compactOnArchive = signal<boolean | null>(null);
  readonly keepRecentToolResults = signal<number | null>(null);
  readonly keepRecentMessages = signal<number | null>(null);

  // --- Workspace ---
  private readonly modelService = inject(ModelService);
  readonly vmCpuCores = signal<number | null>(null);
  readonly vmMemory = signal<number | null>(null);
  readonly vmDiskSize = signal<number | null>(null);
  readonly maxReadWords = signal<number | null>(null);
  readonly maxWriteWords = signal<number | null>(null);
  readonly gitVersioning = signal<boolean | null>(null);

  // --- Shell ---
  readonly shellMode = signal<string | null>(null);
  readonly shellSandbox = signal<boolean | null>(null);
  readonly shellTimeout = signal<number | null>(null);
  readonly sudoAction = signal<string | null>(null);

  // --- Research ---
  readonly proxyEnabled = signal<boolean | null>(null);

  // --- Auxiliary ---
  readonly auxEnabled = signal<boolean | null>(null);
  readonly auxModel = signal<string | null>(null);
  readonly auxTemperature = signal<number | null>(null);

  // --- Session-specific ---
  readonly idleTimeout = signal<number | null>(null);
  // ===== Resolved defaults =====
  // Resolution order: matrix for the effective model → base config → hardcoded.
  // The server sends raw config (no matrix baked in) + the raw settings_matrix.
  // The client resolves matrix values for the effective model (user override or config default).

  // The config input may be a merged dict that still carries a pre-U1
  // expert's per-phase tiers as stored; read it through the compat mapping so
  // a legacy `llm.strategic` pin is THE model (and its params ride along).
  private readonly normalizedConfig = computed(() => liftLegacyTiers(this.config(), {merged: true}));

  private r(path: string): unknown { return readConfigPath(this.normalizedConfig(), path); }

  /** Look up a settings_matrix value for a model. Returns null if key not found. */
  private mv(model: string | null, key: string): unknown {
    if (!model) return null;
    const resolved = resolveMatrixForModel(this.settingsMatrix(), model);
    return resolved[key] ?? null;
  }

  // Effective model: user override falls back to the config default.
  private readonly effectiveModel = computed(() =>
    this.modelOverride() ?? (this.r('llm.model') as string) ?? null);

  readonly resolvedReasoning = computed(() => this.r('llm.reasoning_level') as string | null);
  readonly resolvedTemp = computed(() =>
    (this.mv(this.effectiveModel(), 'temperature') ?? this.r('llm.temperature') ?? 0) as number);
  readonly resolvedMultimodal = computed(() =>
    (this.mv(this.effectiveModel(), 'multimodal') ?? this.r('llm.multimodal') ?? false) as boolean);
  readonly resolvedTopP = computed(() =>
    (this.mv(this.effectiveModel(), 'top_p') ?? this.r('llm.top_p')) as number | null);
  readonly resolvedTopK = computed(() =>
    (this.mv(this.effectiveModel(), 'top_k') ?? this.r('llm.top_k')) as number | null);
  readonly resolvedMaxOutputTokens = computed(() => this.r('llm.max_output_tokens') as number | null);
  readonly resolvedParallelToolCalls = computed(() =>
    (this.mv(this.effectiveModel(), 'parallel_tool_calls') ?? this.r('llm.parallel_tool_calls') ?? false) as boolean);

  readonly resolvedMessageCountThreshold = computed(() => (this.r('limits.message_count_threshold') ?? 300) as number);
  readonly resolvedToolRetryCount = computed(() => (this.r('limits.tool_retry_count') ?? 3) as number);
  readonly resolvedProgressStallThreshold = computed(() => (this.r('limits.progress_stall_threshold') ?? 30) as number);
  readonly resolvedMaxToolCallsPerPhase = computed(() => (this.r('limits.max_tool_calls_per_phase') ?? 200) as number);

  readonly resolvedMemoryEnabled = computed(() => (this.r('memory.enabled') ?? true) as boolean);
  readonly resolvedMemoryBudget = computed(() => (this.r('memory.budget_tokens') ?? 10000) as number);

  readonly resolvedCompactOnArchive = computed(() => (this.r('context_management.compact_on_archive') ?? true) as boolean);
  readonly resolvedKeepRecentToolResults = computed(() => (this.r('context_management.keep_recent_tool_results') ?? 150) as number);
  readonly resolvedKeepRecentMessages = computed(() => (this.r('context_management.keep_recent_messages') ?? 10) as number);

  readonly resolvedWorkspaceBackend = computed(() => (this.r('workspace.backend') ?? 'sandbox') as string);
  readonly resolvedVmCpuCores = computed(() => (this.r('workspace.vm.cpu_cores') ?? 8) as number);
  readonly resolvedVmMemory = computed(() => vmQuantityInGiB(this.r('workspace.vm.memory') ?? '16Gi'));
  // Empty means "the deployment's controller default" — the orchestrator omits
  // disk_size from the VM create payload unless a job sets it.
  readonly resolvedVmDiskSize = computed(() => vmQuantityInGiB(this.r('workspace.vm.disk_size')));
  readonly vmMemoryInvalid = computed(() => this.vmMemory() === null
    ? this.resolvedVmMemory() === null : !validVmSizeGiB(this.vmMemory()));
  readonly vmDiskSizeInvalid = computed(() => this.vmDiskSize() === null
    ? this.r('workspace.vm.disk_size') != null && this.r('workspace.vm.disk_size') !== '' && this.resolvedVmDiskSize() === null
    : !validVmSizeGiB(this.vmDiskSize()));
  readonly vmSizingValid = computed(() => this.effectiveBackend() !== 'vm'
    || (!this.vmMemoryInvalid() && !this.vmDiskSizeInvalid()));
  readonly resolvedMaxReadWords = computed(() => (this.r('workspace.max_read_words') ?? 25000) as number);
  readonly resolvedMaxWriteWords = computed(() => (this.r('workspace.max_write_words') ?? 10000) as number);
  readonly resolvedGitVersioning = computed(() => {
    const v = this.r('workspace.git_versioning');
    return (v ?? (this.mode() === 'job')) as boolean;
  });

  // Effective backend = the Execution group's override → resolved config
  // default. The lite tiers (`virtual`/`none`) run with no workspace container,
  // so shell/browser/git tools are gated off server-side
  // (no_workspace_agent_mode.md §7) and the form greys the matching controls.
  // `none` additionally has no file tools.
  readonly effectiveBackend = computed(() => this.backendOverride() ?? this.resolvedWorkspaceBackend());
  readonly isLiteBackend = computed(() => {
    const b = this.effectiveBackend();
    return b === 'virtual' || b === 'none';
  });
  readonly isNoneBackend = computed(() => this.effectiveBackend() === 'none');

  readonly resolvedShellMode = computed(() => (this.r('shell.mode') ?? 'stateless') as string);
  readonly resolvedShellSandbox = computed(() => (this.r('shell.sandbox') ?? true) as boolean);
  readonly resolvedShellTimeout = computed(() => (this.r('shell.default_timeout') ?? 120) as number);
  readonly resolvedSudoAction = computed(() => (this.r('shell.sudo_action') ?? 'freeze') as string);

  readonly resolvedProxyEnabled = computed(() => (this.r('research.proxy.enabled') ?? false) as boolean);

  readonly resolvedAuxEnabled = computed(() => (this.r('auxiliary.enabled') ?? true) as boolean);
  readonly resolvedAuxModel = computed(() => (this.r('auxiliary.model') ?? 'RedHatAI/gemma-4-31B-it-FP8-Dynamic') as string);
  readonly resolvedAuxTemperature = computed(() => (this.r('auxiliary.temperature') ?? 0) as number);

  readonly resolvedIdleTimeout = computed(() => (this.r('interactive.idle_timeout_minutes') ?? 30) as number);
  // ===== Computed helpers =====
  readonly effectiveAuxEnabled = computed(() => this.auxEnabled() ?? this.resolvedAuxEnabled());
  readonly effectiveTemp = computed(() => this.temperature() ?? this.resolvedTemp());
  readonly effectiveAuxTemp = computed(() => this.auxTemperature() ?? this.resolvedAuxTemperature());

  readonly reasoningOptions = computed(() =>
    reasoningOptionsForModel(this.effectiveModel(), this.modelService.reasoningByModel()),
  );
  readonly inferenceModifiedCount = computed(() => {
    let c = 0;
    if (this.reasoning() !== null) c++;
    if (this.temperature() !== null) c++;
    if (this.multimodal() !== null) c++;
    if (this.topP() !== null) c++;
    if (this.topK() !== null) c++;
    if (this.maxOutputTokens() !== null) c++;
    if (this.parallelToolCalls() !== null) c++;
    return c;
  });

  // ===== Event handlers =====
  toggleSection(section: string): void {
    const next = new Set(this.expanded());
    if (next.has(section)) {
      next.delete(section);
    } else {
      next.add(section);
    }
    this.expanded.set(next);
  }

  emitChange(): void { this.change.emit(); }

  /** Commit a displayed-but-inherited value on deliberate interaction.
   *  See PinOnInteractDirective — a <select> emits no change event when the
   *  option already showing is re-picked, so without this the resolved default
   *  is the one value the form cannot express. */
  pinValue<T>(target: {(): T | null; set(value: T | null): void}, resolved: T): void {
    if (pinResolvedValue(target, resolved)) this.emitChange();
  }

  clampTemp(value: number): number {
    return Math.round(Math.min(2, Math.max(0, value)) * 10) / 10;
  }

  onReasoningChange(v: string | null): void { this.reasoning.set(v); this.emitChange(); }
  onTempChange(v: number): void { this.temperature.set(this.clampTemp(v)); this.emitChange(); }
  onMultimodalChange(e: Event): void { this.multimodal.set((e.target as HTMLInputElement).checked); this.emitChange(); }
  onTopPChange(v: number | null): void { this.topP.set(v); this.emitChange(); }
  onTopKChange(v: number | null): void { this.topK.set(v); this.emitChange(); }
  onMaxOutputTokensChange(v: number | null): void { this.maxOutputTokens.set(v); this.emitChange(); }
  onParallelToolCallsChange(e: Event): void { this.parallelToolCalls.set((e.target as HTMLInputElement).checked); this.emitChange(); }
  onMemoryEnabledChange(e: Event): void { this.memoryEnabled.set((e.target as HTMLInputElement).checked); this.emitChange(); }
  onCompactOnArchiveChange(e: Event): void { this.compactOnArchive.set((e.target as HTMLInputElement).checked); this.emitChange(); }
  onGitVersioningChange(e: Event): void { this.gitVersioning.set((e.target as HTMLInputElement).checked); this.emitChange(); }
  onShellSandboxChange(e: Event): void { this.shellSandbox.set((e.target as HTMLInputElement).checked); this.emitChange(); }
  onProxyEnabledChange(e: Event): void { this.proxyEnabled.set((e.target as HTMLInputElement).checked); this.emitChange(); }
  onAuxEnabledChange(e: Event): void { this.auxEnabled.set((e.target as HTMLInputElement).checked); this.emitChange(); }
  /** Build the advanced settings config_override fragment. */
  getOverrides(): Record<string, unknown> {
    const o: Record<string, unknown> = {};
    const llm: Record<string, unknown> = {};

    // One model, one llm block (U1): the inference params ride on llm.* —
    // never on a per-phase tier. llm.reasoning_level is owned by
    // ModelGroupComponent in session mode.
    if (this.mode() === 'job' && this.reasoning() !== null) llm['reasoning_level'] = this.reasoning();
    if (this.temperature() !== null) llm['temperature'] = this.temperature();
    if (this.multimodal() !== null) llm['multimodal'] = this.multimodal();

    if (this.topP() !== null) llm['top_p'] = this.topP();
    if (this.topK() !== null) llm['top_k'] = this.topK();
    if (this.maxOutputTokens() !== null) llm['max_output_tokens'] = this.maxOutputTokens();
    if (this.parallelToolCalls() !== null) llm['parallel_tool_calls'] = this.parallelToolCalls();
    if (Object.keys(llm).length) o['llm'] = llm;

    // Limits
    const lim: Record<string, unknown> = {};
    if (this.messageCountThreshold() !== null) lim['message_count_threshold'] = this.messageCountThreshold();
    if (this.toolRetryCount() !== null) lim['tool_retry_count'] = this.toolRetryCount();
    if (this.progressStallThreshold() !== null) lim['progress_stall_threshold'] = this.progressStallThreshold();
    if (this.maxToolCallsPerPhase() !== null) lim['max_tool_calls_per_phase'] = this.maxToolCallsPerPhase();
    if (Object.keys(lim).length) o['limits'] = lim;

    // Memory
    const mem: Record<string, unknown> = {};
    if (this.memoryEnabled() !== null) mem['enabled'] = this.memoryEnabled();
    if (this.memoryBudget() !== null) mem['budget_tokens'] = this.memoryBudget();
    if (Object.keys(mem).length) o['memory'] = { ...(o['memory'] as any ?? {}), ...mem };

    // Context
    const ctx: Record<string, unknown> = {};
    if (this.compactOnArchive() !== null) ctx['compact_on_archive'] = this.compactOnArchive();
    if (this.keepRecentToolResults() !== null) ctx['keep_recent_tool_results'] = this.keepRecentToolResults();
    if (this.keepRecentMessages() !== null) ctx['keep_recent_messages'] = this.keepRecentMessages();
    if (Object.keys(ctx).length) o['context_management'] = ctx;

    // Workspace. Lite tiers (virtual/none) run with no container, so the gated
    // tool categories' settings aren't emitted (no_workspace_agent_mode.md §7):
    // git is forced off for any lite tier, file-size limits drop for `none`
    // (no file tools), and the whole shell/browser fragments are skipped.
    // `backend` itself is emitted by ExecutionGroupComponent, which owns the
    // selector; the host deep-merges the two `workspace` fragments.
    const lite = this.isLiteBackend();
    const ws: Record<string, unknown> = {};
    if (this.effectiveBackend() === 'vm') {
      const vm: Record<string, unknown> = {};
      if (this.vmCpuCores() !== null) vm['cpu_cores'] = this.vmCpuCores();
      if (validVmSizeGiB(this.vmMemory())) vm['memory'] = `${this.vmMemory()}Gi`;
      if (validVmSizeGiB(this.vmDiskSize())) vm['disk_size'] = `${this.vmDiskSize()}Gi`;
      if (Object.keys(vm).length) ws['vm'] = vm;
    }
    if (!this.isNoneBackend()) {
      if (this.maxReadWords() !== null) ws['max_read_words'] = this.maxReadWords();
      if (this.maxWriteWords() !== null) ws['max_write_words'] = this.maxWriteWords();
    }
    if (!lite && this.gitVersioning() !== null) ws['git_versioning'] = this.gitVersioning();
    if (Object.keys(ws).length) o['workspace'] = ws;

    // Shell — no shell tools on lite tiers, so skip the fragment entirely.
    if (!lite) {
      const sh: Record<string, unknown> = {};
      if (this.shellMode() !== null) sh['mode'] = this.shellMode();
      if (this.shellSandbox() !== null) sh['sandbox'] = this.shellSandbox();
      if (this.shellTimeout() !== null) sh['default_timeout'] = this.shellTimeout();
      if (this.sudoAction() !== null) sh['sudo_action'] = this.sudoAction();
      if (Object.keys(sh).length) o['shell'] = sh;
    }

    // Research — the proxy toggle applies on every tier (web_search egress).
    // The browser fragment that used to live here emitted `browser.headless` /
    // `browser.use_vision`, knobs of the removed `browse_website` sub-agent;
    // the live browser config is `browser.snapshot.*` / `browser.security.*`
    // and vision is derived from the model's multimodal flag.
    if (this.proxyEnabled() !== null) o['research'] = { proxy: { enabled: this.proxyEnabled() } };

    // Auxiliary
    const aux: Record<string, unknown> = {};
    if (this.auxEnabled() !== null) aux['enabled'] = this.auxEnabled();
    if (this.auxModel() !== null) aux['model'] = this.auxModel();
    if (this.auxTemperature() !== null) aux['temperature'] = this.auxTemperature();
    if (Object.keys(aux).length) o['auxiliary'] = aux;

    // Session-specific
    if (this.mode() === 'session') {
      const inter: Record<string, unknown> = {};
      if (this.idleTimeout() !== null) inter['idle_timeout_minutes'] = this.idleTimeout();
      if (Object.keys(inter).length) o['interactive'] = { ...(o['interactive'] as any ?? {}), ...inter };
    }

    return o;
  }

  /** Prefill from expert config. Job mode pins the config's inference values
   *  (as it always has); session mode's inference fields stay inherited. A
   *  pre-U1 fragment's `llm.strategic` block is read through the compat
   *  mapping — its params ride into `llm.*`, never into a phase tier. */
  prefillFromConfig(config: Record<string, unknown>): void {
    if (this.mode() !== 'job') return;
    const lifted = liftLegacyTiers(config, {merged: true});
    const llm = lifted['llm'] as Record<string, unknown> | undefined;

    this.reasoning.set((llm?.['reasoning_level'] as string) ?? null);
    this.temperature.set((llm?.['temperature'] as number) ?? null);
    this.multimodal.set((llm?.['multimodal'] as boolean) ?? null);
  }

  resetAll(): void {
    this.reasoning.set(null);
    this.temperature.set(null);
    this.multimodal.set(null);
    this.topP.set(null);
    this.topK.set(null);
    this.maxOutputTokens.set(null);
    this.parallelToolCalls.set(null);
    this.messageCountThreshold.set(null);
    this.toolRetryCount.set(null);
    this.progressStallThreshold.set(null);
    this.maxToolCallsPerPhase.set(null);
    this.memoryEnabled.set(null);
    this.memoryBudget.set(null);
    this.compactOnArchive.set(null);
    this.keepRecentToolResults.set(null);
    this.keepRecentMessages.set(null);
    this.vmCpuCores.set(null);
    this.vmMemory.set(null);
    this.vmDiskSize.set(null);
    this.maxReadWords.set(null);
    this.maxWriteWords.set(null);
    this.gitVersioning.set(null);
    this.shellMode.set(null);
    this.shellSandbox.set(null);
    this.shellTimeout.set(null);
    this.sudoAction.set(null);
    this.proxyEnabled.set(null);
    this.auxEnabled.set(null);
    this.auxModel.set(null);
    this.auxTemperature.set(null);
    this.idleTimeout.set(null);
    this.expanded.set(new Set());
  }
}

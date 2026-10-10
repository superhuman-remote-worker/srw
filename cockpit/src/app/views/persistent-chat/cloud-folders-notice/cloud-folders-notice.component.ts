import { ChangeDetectionStrategy, Component, input, output } from '@angular/core';
import { TranslocoPipe } from '@jsverse/transloco';
import { AppIconComponent } from '../../../ui/icon';
import { CloudFolderBinding, CloudFolderProblem } from '../../../core/util/cloud-mount-status';
import { accessLevelLabelKey } from '../../../core/models/platform-connector';

/**
 * This session's cloud folders, each with its binding state (main cloud
 * slices 3 to 6): every attached cloud connector binds on its own, so each
 * one says what it is, the level it is bound at, and whether it is mounted.
 * One that did not mount, or was left out, is shown in the warning tone and
 * says why; nothing takes its place. A protected session's folder links to
 * the review banner above it.
 *
 * A protected session whose cloud layer did not come up runs with no cloud
 * folder at all (decision 42): one line says the protected cloud is
 * unavailable, and why.
 *
 * Without `folders` (an older caller) it lists only the folders that are not
 * available (connector drivers D7). Its own component, like the cloud review
 * banner, so no rules land in persistent-chat.component.scss.
 */
@Component({
  selector: 'app-cloud-folders-notice',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [TranslocoPipe, AppIconComponent],
  template: `
    @if (folders().length || problems().length || agentOutdated()) {
      <div
        class="cfn"
        [class.cfn--ok]="!problems().length && !agentOutdated()"
        role="status"
        data-testid="cloud-folders-notice"
        [attr.aria-label]="'chat.cloudFolders.regionLabel' | transloco"
      >
        <app-icon size="sm" class="cfn__icon" aria-hidden="true">{{
          problems().length || agentOutdated() ? 'cloud_off' : 'cloud_done'
        }}</app-icon>
        <div class="cfn__body">
          @if (protectedCloud() && problems().length) {
            <p class="cfn__title" data-testid="cloud-folders-protected">
              {{ 'chat.cloudFolders.protectedTitle' | transloco }}
              <span class="cfn__sep" aria-hidden="true">·</span>
              {{ 'chat.cloudFolders.reason.' + problems()[0].reason | transloco }}
            </p>
            <p class="cfn__meta">{{ 'chat.cloudFolders.protectedMeta' | transloco }}</p>
          } @else if (folders().length) {
            <p class="cfn__title">
              {{ (problems().length ? 'chat.cloudFolders.title' : 'chat.cloudFolders.regionLabel') | transloco }}
            </p>
            <ul class="cfn__list">
              @for (folder of folders(); track $index) {
                <li
                  class="cfn__folder"
                  [class.cfn__folder--down]="folder.state === 'unavailable' || folder.state === 'excluded'"
                  data-cloud-folder
                  [attr.data-state]="folder.state"
                >
                  <!-- Pieces are joined by text, not by whitespace between
                       elements (stripped at compile): the line reads the
                       same to a screen reader as it looks. -->
                  @if (folder.path) {
                    <span class="cfn__name">{{ folder.path }}</span>
                    @if (kindKey(folder); as kind) {
                      {{ ' · ' }}<span class="cfn__kind">{{ kind | transloco }}</span>
                    }
                  } @else if (folder.title) {
                    <span class="cfn__label">{{ folder.title }}</span>
                  } @else if (folder.kind) {
                    <span class="cfn__label">{{ 'chat.cloudFolders.notAttachedKind.' + folder.kind | transloco }}</span>
                  } @else {
                    <span class="cfn__label">{{ 'chat.cloudFolders.notAttached' | transloco }}</span>
                  }
                  @if (levelKey(folder); as level) {
                    {{ ' ' }}<span class="cfn__level" data-level>{{ level | transloco }}</span>
                  }
                  {{ ' · ' }}
                  @if (folder.reason) {
                    <span data-reason>{{ 'chat.cloudFolders.reason.' + folder.reason | transloco }}</span>
                  } @else {
                    <span data-folder-state>{{ 'chat.cloudFolders.state.' + folder.state | transloco }}</span>
                  }
                  @if (folder.level === 'protected' && folder.state !== 'unavailable') {
                    {{ ' ' }}<button type="button" class="cfn__review" data-review (click)="showReview.emit()">
                      {{ 'chat.cloudFolders.review' | transloco }}
                    </button>
                  }
                </li>
              }
            </ul>
          } @else if (problems().length) {
            <p class="cfn__title">{{ 'chat.cloudFolders.title' | transloco }}</p>
            <ul class="cfn__list">
              @for (problem of problems(); track $index) {
                <li>
                  @if (problem.path) {
                    <span class="cfn__name">{{ problem.path }}</span>
                  } @else if (problem.kind) {
                    <span class="cfn__name">{{
                      'chat.cloudFolders.notAttachedKind.' + problem.kind | transloco
                    }}</span>
                  } @else {
                    <span class="cfn__name">{{ 'chat.cloudFolders.notAttached' | transloco }}</span>
                  }
                  <span class="cfn__sep" aria-hidden="true">·</span>
                  {{ 'chat.cloudFolders.reason.' + problem.reason | transloco }}
                </li>
              }
            </ul>
          }
          @if (agentOutdated()) {
            <p class="cfn__meta">{{ 'chat.cloudFolders.agentOutdated' | transloco }}</p>
          }
        </div>
      </div>
    }
  `,
  styleUrl: './cloud-folders-notice.component.scss',
})
export class CloudFoldersNoticeComponent {
  problems = input<CloudFolderProblem[]>([]);
  /** Every attached cloud folder with its binding state; empty for a
   *  caller that passes only the problems. */
  folders = input<CloudFolderBinding[]>([]);
  /** A protected session runs without its cloud (decision 42). */
  protectedCloud = input<boolean>(false);
  agentOutdated = input<boolean>(false);
  /** The protected folder's link to its review: the host points at the
   *  review banner, the one place the review opens from. */
  showReview = output<void>();

  /** What a mounted folder is ("Project folder", "My cloud"), or null. */
  kindKey(folder: CloudFolderBinding): string | null {
    return folder.kind ? `chat.cloudFolders.kind.${folder.kind}` : null;
  }

  levelKey(folder: CloudFolderBinding): string | null {
    return accessLevelLabelKey(folder.level);
  }
}

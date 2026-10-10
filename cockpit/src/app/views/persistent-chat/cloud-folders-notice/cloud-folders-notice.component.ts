import { ChangeDetectionStrategy, Component, input } from '@angular/core';
import { TranslocoPipe } from '@jsverse/transloco';
import { AppIconComponent } from '../../../ui/icon';
import { CloudFolderProblem } from '../../../core/util/cloud-mount-status';

/**
 * Cloud folders of this session that are not available (connector drivers
 * D7). A folder that does not mount never blocks the workspace; this is where
 * the user learns which one and why. Hidden when every folder came up.
 *
 * A protected session whose cloud layer did not come up runs with no cloud
 * folder at all (decision 42): one line says the protected cloud is
 * unavailable, and why.
 *
 * Main-cloud slice 3 replaces it with each connector's binding state in the
 * session header. Its own component, like the cloud review banner, so no
 * rules land in persistent-chat.component.scss.
 */
@Component({
  selector: 'app-cloud-folders-notice',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [TranslocoPipe, AppIconComponent],
  template: `
    @if (problems().length || agentOutdated()) {
      <div
        class="cfn"
        role="status"
        data-testid="cloud-folders-notice"
        [attr.aria-label]="'chat.cloudFolders.regionLabel' | transloco"
      >
        <app-icon size="sm" class="cfn__icon" aria-hidden="true">cloud_off</app-icon>
        <div class="cfn__body">
          @if (protectedCloud() && problems().length) {
            <p class="cfn__title" data-testid="cloud-folders-protected">
              {{ 'chat.cloudFolders.protectedTitle' | transloco }}
              <span class="cfn__sep" aria-hidden="true">·</span>
              {{ 'chat.cloudFolders.reason.' + problems()[0].reason | transloco }}
            </p>
            <p class="cfn__meta">{{ 'chat.cloudFolders.protectedMeta' | transloco }}</p>
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
  /** A protected session runs without its cloud (decision 42). */
  protectedCloud = input<boolean>(false);
  agentOutdated = input<boolean>(false);
}

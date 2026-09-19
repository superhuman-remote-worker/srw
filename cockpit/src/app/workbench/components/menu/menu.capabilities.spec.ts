import {TestBed} from '@angular/core/testing';
import {beforeEach, describe, expect, it} from 'vitest';
import {environment} from '../../../core/environment';
import {LayoutService} from '../../services/layout.service';
import {MenuComponent} from './menu.component';

describe('workbench tool links', () => {
  beforeEach(() => TestBed.resetTestingModule());
  it.each([false, true])('keeps Git and only advertises supported admin tools (%s)', enabled => {
    const previous = environment.adminToolsEnabled;
    environment.adminToolsEnabled = enabled;
    try {
      TestBed.configureTestingModule({imports: [MenuComponent], providers: [{provide: LayoutService, useValue: {}}]});
      const fixture = TestBed.createComponent(MenuComponent);
      fixture.componentInstance.isOpen.set(true);
      fixture.detectChanges();
      const links = [...fixture.nativeElement.querySelectorAll('.item-label')].map((el: any) => el.textContent.trim());
      expect(links).toContain('Gitea');
      for (const label of ['Neo4j Browser', 'PostgreSQL', 'Dozzle']) expect(links.includes(label)).toBe(enabled);
      fixture.destroy();
    } finally { environment.adminToolsEnabled = previous; }
  });
});

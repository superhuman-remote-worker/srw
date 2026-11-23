import {ɵresolveComponentResources} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {TranslocoTestingModule} from '@jsverse/transloco';
import {beforeAll, describe, expect, it} from 'vitest';
import en from '../../../assets/i18n/en.json';
import {ConnectorFilterBarComponent} from './connector-filter-bar.component';

/**
 * Signal inputs cannot be bound in this pipeline (no ngtsc for vitest), so
 * they are stubbed on the instance, as in editor-header.component.spec.ts.
 */
function mount(inputs: Record<string, unknown>) {
  TestBed.configureTestingModule({
    imports: [
      ConnectorFilterBarComponent,
      TranslocoTestingModule.forRoot({
        langs: {en},
        translocoConfig: {availableLangs: ['en'], defaultLang: 'en'},
      }),
    ],
  });
  const fixture = TestBed.createComponent(ConnectorFilterBarComponent);
  const defaults = {chips: [], selectedType: 'all', searchable: true, search: '', tokens: [], panelOpen: false};
  for (const [name, value] of Object.entries({...defaults, ...inputs})) {
    Object.defineProperty(fixture.componentInstance, name, {value: () => value});
  }
  fixture.detectChanges();
  return {fixture, el: fixture.nativeElement as HTMLElement};
}

describe('ConnectorFilterBarComponent', () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  it('shows a chip per type with its count, the selected one pressed', () => {
    const {el} = mount({
      chips: [
        {value: 'all', count: 3},
        {value: 'postgresql', count: 2},
        {value: 'kb', count: 1},
      ],
      selectedType: 'postgresql',
    });
    const chips = Array.from(el.querySelectorAll<HTMLButtonElement>('[data-type-chip]'));
    expect(chips.map((chip) => chip.getAttribute('data-type-chip'))).toEqual(['all', 'postgresql', 'kb']);
    expect(chips.map((chip) => chip.getAttribute('aria-pressed'))).toEqual(['false', 'true', 'false']);
    expect(chips[1].querySelector('.cf-chip__count')?.textContent).toBe('2');
  });

  it('says what the user picked, and which token they want gone', () => {
    const {fixture, el} = mount({
      chips: [{value: 'all', count: 1}],
      tokens: [{id: 'kept', labelKey: 'datasources.catalog.token.keptHidden'}],
    });
    const picked: string[] = [];
    const removed: string[] = [];
    let cleared = 0;
    fixture.componentInstance.typeSelect.subscribe((value) => picked.push(value));
    fixture.componentInstance.removeToken.subscribe((id) => removed.push(id));
    fixture.componentInstance.clearAll.subscribe(() => cleared++);

    el.querySelector<HTMLButtonElement>('[data-type-chip="all"]')!.click();
    el.querySelector<HTMLButtonElement>('[data-token="kept"]')!.click();
    el.querySelector<HTMLButtonElement>('[data-filters-clear]')!.click();

    expect(picked).toEqual(['all']);
    expect(removed).toEqual(['kept']);
    expect(cleared).toBe(1);
    // The Filters button counts the tokens.
    expect(el.querySelector('.cf__badge')?.textContent).toBe('1');
  });

  it('has no search or Filters button without the server-side catalog', () => {
    const {el} = mount({searchable: false});
    expect(el.querySelector('app-input')).toBeNull();
    expect(el.querySelector('[data-filters-toggle]')).toBeNull();
  });
});

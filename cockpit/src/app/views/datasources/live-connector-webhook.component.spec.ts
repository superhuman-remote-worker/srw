import {CUSTOM_ELEMENTS_SCHEMA, signal, ɵresolveComponentResources} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {TranslocoPipe, TranslocoTestingModule} from '@jsverse/transloco';
import {afterEach, beforeAll, beforeEach, describe, expect, it, vi} from 'vitest';
import {environment} from '../../core/environment';
import {LiveConnectorWebhookComponent, liveConnectorWebhookUrl} from './live-connector-webhook.component';
import en from '../../../assets/i18n/en.json';

const originalApiUrl = environment.apiUrl;
describe('live connector webhook', () => {
  beforeAll(async () => { await ɵresolveComponentResources(() => Promise.resolve('')); });
  beforeEach(() => {
    TestBed.configureTestingModule({imports: [LiveConnectorWebhookComponent,
      TranslocoTestingModule.forRoot({langs: {en}, translocoConfig: {availableLangs: ['en'], defaultLang: 'en'}, preloadLangs: true})]});
    TestBed.overrideComponent(LiveConnectorWebhookComponent, {
      set: {imports: [TranslocoPipe], schemas: [CUSTOM_ELEMENTS_SCHEMA]},
    });
  });
  afterEach(() => { environment.apiUrl = originalApiUrl; vi.unstubAllGlobals(); });

  it.each(['/api', 'https://api.example.test/api'])('uses the API origin for %s', (apiUrl) => {
    environment.apiUrl = apiUrl;
    const origin = apiUrl.startsWith('https:') ? 'https://api.example.test' : window.location.origin;
    expect(liveConnectorWebhookUrl('connector-1')).toBe(`${origin}/api/live/webhooks/connector-1`);
  });

  it('shows a read-only URL with OpenAI signing-secret instructions and copies it', async () => {
    environment.apiUrl = 'https://srw.example.test/api';
    const writeText = vi.fn().mockResolvedValue(undefined);
    vi.stubGlobal('navigator', {clipboard: {writeText}});
    const fixture = TestBed.createComponent(LiveConnectorWebhookComponent);
    Object.defineProperty(fixture.componentInstance, 'connectorId', {value: signal('phone-1')});
    fixture.detectChanges();
    const host = fixture.nativeElement as HTMLElement;
    const field = host.querySelector('app-form-field') as unknown as {label: string; hint: string};
    expect(field.label).toBe('Webhook URL for OpenAI');
    expect(field.hint).toContain('whsec_');
    const input = host.querySelector('app-input') as unknown as {value: string; readOnly: boolean};
    expect(input.value).toBe('https://srw.example.test/api/live/webhooks/phone-1');
    expect(input.readOnly).toBe(true);
    await fixture.componentInstance.copy();
    expect(writeText).toHaveBeenCalledWith(input.value);
    expect(fixture.componentInstance.copied()).toBe(true);
  });

  it('reports clipboard failure without marking the URL copied', async () => {
    vi.stubGlobal('navigator', {clipboard: {writeText: vi.fn().mockRejectedValue(new Error('denied'))}});
    const fixture = TestBed.createComponent(LiveConnectorWebhookComponent);
    Object.defineProperty(fixture.componentInstance, 'connectorId', {value: signal('phone-1')});
    await fixture.componentInstance.copy();
    expect(fixture.componentInstance.copyFailed()).toBe(true);
    expect(fixture.componentInstance.copied()).toBe(false);
  });
});

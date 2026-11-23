import {describe, expect, it} from 'vitest';
import de from '../assets/i18n/de-DE.json';
import en from '../assets/i18n/en.json';

// The chat-pool stall copy is fixed wording; a key in the wrong block would
// render as its raw dotted path, which the EN/DE parity check cannot see.
function at(tree: unknown, path: string): unknown {
  return path.split('.').reduce<any>((node, part) => node?.[part], tree);
}

const COPY: Record<string, {en: string; de: string}> = {
  'chat.queue.unavailable': {
    en: 'Chat is temporarily unavailable; an administrator has been notified. Your message is saved.',
    de: 'Der Chat ist vorübergehend nicht verfügbar; ein Administrator wurde benachrichtigt. Ihre Nachricht ist gespeichert.',
  },
  'admin.capacity.stalled': {
    en: 'Chat is down: {{count}} message(s) have waited {{age}} and no executor is taking any. Check the srw-agent-stateless pods.',
    de: 'Der Chat ist ausgefallen: {{count}} Nachricht(en) warten seit {{age}}, und kein Executor nimmt sie an. Prüfen Sie die srw-agent-stateless-Pods.',
  },
  'notifications.category.chat_executors_unavailable': {en: 'Chat down', de: 'Chat ausgefallen'},
  'notifications.actions.openAdminCapacity': {en: 'Open capacity', de: 'Kapazität öffnen'},
};

describe('chat-pool stall copy', () => {
  for (const [key, copy] of Object.entries(COPY)) {
    it(`${key} resolves to the exact EN and DE strings`, () => {
      expect(at(en, key)).toBe(copy.en);
      expect(at(de, key)).toBe(copy.de);
    });
  }
});

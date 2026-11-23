import {afterEach, beforeAll, beforeEach, describe, expect, it, vi} from 'vitest';
import {CUSTOM_ELEMENTS_SCHEMA, ɵresolveComponentResources} from '@angular/core';
import {ComponentFixture, TestBed} from '@angular/core/testing';
import {TranslocoPipe, TranslocoTestingModule} from '@jsverse/transloco';
import {McpQuestionCardComponent, questionCardKind} from './mcp-question-card.component';
import {
  McpQuestion,
  McpQuestionAnswer,
  McpQuestionUiState,
} from '../../../core/models/mcp-question.model';
// The real catalogue, so these specs also prove the keys exist.
import en from '../../../../assets/i18n/en.json';
import de from '../../../../assets/i18n/de-DE.json';

/*
 * The ui primitives stay unknown elements: the vitest JIT harness wires
 * neither their signal inputs nor their outputs (see
 * generic-connector-form.component.spec.ts). Their bindings land as DOM
 * properties, which these specs read; a button's `(clicked)` is then a DOM
 * event listener, fired with `dispatchEvent`, and typing goes through the
 * card's own `setText`/`pick`/`toggle`, the methods the primitives call.
 */

const CONFIRM: McpQuestion = {
  id: 'call-1',
  approvalId: 'ask-1',
  server: 'Civitai',
  serverSlug: 'civitai',
  tool: 'mcp__civitai__generate',
  mode: 'form',
  message: 'Spend 12 Buzz to run imageGen?',
  schema: {type: 'object', properties: {}},
  url: null,
  confirmation: {confirm: true},
};

const FORM: McpQuestion = {
  ...CONFIRM,
  approvalId: 'ask-2',
  message: 'Describe the image.',
  confirmation: null,
  schema: {
    type: 'object',
    properties: {
      prompt: {type: 'string', title: 'Prompt', description: 'What to draw', maxLength: 50},
      size: {type: 'string', enum: ['s', 'm'], enumNames: ['Small', 'Medium'], default: 'm'},
      count: {type: 'integer', minimum: 1, maximum: 4, default: 1},
      nsfw: {type: 'boolean', title: 'Allow mature content'},
      tags: {type: 'array', items: {enum: ['a', 'b']}},
    },
    required: ['prompt', 'size'],
  },
};

const LINK: McpQuestion = {
  ...CONFIRM,
  approvalId: 'ask-3',
  tool: null,
  mode: 'url',
  message: 'Sign in to Civitai to continue.',
  schema: null,
  url: 'https://civitai.com/login?next=%2Fbuzz',
  confirmation: null,
};

interface Rendered {
  fixture: ComponentFixture<McpQuestionCardComponent>;
  host: HTMLElement;
  card: McpQuestionCardComponent;
  answers: McpQuestionAnswer[];
  click: (action: string) => void;
  text: () => string;
}

function render(question: McpQuestion, state: McpQuestionUiState = {}): Rendered {
  const fixture = TestBed.createComponent(McpQuestionCardComponent);
  const answers: McpQuestionAnswer[] = [];
  fixture.componentInstance.answered.subscribe((answer) => answers.push(answer));
  fixture.componentRef.setInput('question', question);
  fixture.componentRef.setInput('state', state);
  fixture.detectChanges();
  const host = fixture.nativeElement as HTMLElement;
  return {
    fixture,
    host,
    card: fixture.componentInstance,
    answers,
    click: (action) => {
      const button = host.querySelector(`[data-action="${action}"]`);
      if (!button) throw new Error(`no ${action} button`);
      button.dispatchEvent(new Event('clicked'));
      fixture.detectChanges();
    },
    text: () => (host.textContent ?? '').replace(/\s+/g, ' ').trim(),
  };
}

/** A bound DOM property of an inert primitive element. */
function prop(element: Element | null, name: string): unknown {
  return (element as unknown as Record<string, unknown> | null)?.[name];
}

function actions(host: HTMLElement): string[] {
  return [...host.querySelectorAll('[data-action]')].map((b) => b.getAttribute('data-action')!);
}

describe('McpQuestionCardComponent', () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [
        McpQuestionCardComponent,
        TranslocoTestingModule.forRoot({
          langs: {en},
          translocoConfig: {availableLangs: ['en'], defaultLang: 'en'},
          preloadLangs: true,
        }),
      ],
    });
    TestBed.overrideComponent(McpQuestionCardComponent, {
      set: {
        imports: [TranslocoPipe],
        schemas: [CUSTOM_ELEMENTS_SCHEMA],
        styleUrl: undefined,
        styles: [''],
      },
    });
  });

  afterEach(() => {
    TestBed.resetTestingModule();
    vi.restoreAllMocks();
  });

  it('picks the card by mode and confirmation', () => {
    expect(questionCardKind(CONFIRM)).toBe('confirm');
    expect(questionCardKind(FORM)).toBe('form');
    expect(questionCardKind(LINK)).toBe('url');
  });

  it('has every string in both languages', () => {
    const enKeys = Object.keys(en.chat.question).sort();
    expect(Object.keys(de.chat.question).sort()).toEqual(enKeys);
  });

  describe('a yes/no confirmation', () => {
    it('names the server and the tool, says the fixed line, and shows no fields', () => {
      const {host, text} = render(CONFIRM);
      expect(text()).toContain('Civitai is asking during generate');
      expect(text()).toContain('Spend 12 Buzz to run imageGen?');
      expect(host.querySelector('[data-testid="question-secret-warning"]')?.textContent).toContain(
        'Never enter passwords or keys here.',
      );
      expect(host.querySelector('app-form-field')).toBeNull();
      expect(actions(host)).toEqual(['accept', 'always-accept', 'decline', 'dismiss']);
    });

    it('says only the server when the tool is unknown', () => {
      const {text} = render({...CONFIRM, tool: null});
      expect(text()).toContain('Civitai is asking');
      expect(text()).not.toContain('during');
    });

    it("shows the server's message as text, never as HTML", () => {
      const {host} = render({...CONFIRM, message: '<b>bold</b><img src=x onerror=alert(1)>'});
      const message = host.querySelector('[data-testid="question-message"]')!;
      expect(message.querySelector('b, img')).toBeNull();
      expect(message.textContent).toBe('<b>bold</b><img src=x onerror=alert(1)>');
    });

    it('answers Accept and Always accept with the confirmation, Decline and Dismiss bare', () => {
      const {click, answers} = render(CONFIRM);
      click('accept');
      click('always-accept');
      click('decline');
      click('dismiss');
      expect(answers).toEqual([
        {decision: 'approve', content: {confirm: true}},
        {decision: 'approve', content: {confirm: true}, alwaysAccept: true},
        {decision: 'deny'},
        {decision: 'cancel'},
      ]);
    });

    it('locks while an answer is on its way, and says when it landed', () => {
      const sending = render(CONFIRM, {busy: true});
      expect(prop(sending.host.querySelector('[data-action="accept"]'), 'disabled')).toBe(true);
      sending.click('accept');
      expect(sending.answers).toEqual([]);

      const sent = render(CONFIRM, {sent: true});
      expect(sent.host.querySelector('[data-testid="question-sent"]')?.textContent).toContain(
        'Answer sent',
      );
    });

    it("shows the server's refusal on the card", () => {
      const {host} = render(CONFIRM, {error: 'count must be at most 4'});
      const error = host.querySelector('[data-testid="question-error"]')!;
      expect(error.getAttribute('role')).toBe('alert');
      expect(error.textContent).toBe('count must be at most 4');
      // Still answerable.
      expect(prop(host.querySelector('[data-action="accept"]'), 'disabled')).toBe(false);
    });
  });

  describe('a form', () => {
    it('renders each field with its label, hint and required mark, defaults prefilled', () => {
      const {host} = render(FORM);
      const fields = [...host.querySelectorAll('app-form-field')];
      expect(fields.map((f) => f.getAttribute('data-field'))).toEqual([
        'prompt',
        'size',
        'count',
        'nsfw',
        'tags',
      ]);
      const [prompt, size, count, nsfw, tags] = fields;
      expect(prop(prompt, 'label')).toBe('Prompt');
      expect(prop(prompt, 'hint')).toBe('What to draw');
      expect(prop(prompt, 'required')).toBe(true);
      expect(prop(prompt.querySelector('app-input'), 'maxLength')).toBe(50);
      // Never a password manager's fill target.
      expect(prompt.querySelector('app-input')?.getAttribute('autocomplete')).toBe('off');
      expect(prop(size, 'required')).toBe(true);
      expect(prop(size.querySelector('app-select'), 'value')).toBe('1');
      expect([...size.querySelectorAll('option')].map((o) => o.textContent?.trim())).toEqual([
        'Choose…',
        'Small',
        'Medium',
      ]);
      expect(prop(count, 'optional')).toBe('optional');
      const countInput = count.querySelector('app-input');
      expect(prop(countInput, 'type')).toBe('number');
      expect(prop(countInput, 'value')).toBe('1');
      expect([prop(countInput, 'min'), prop(countInput, 'max'), prop(countInput, 'step')]).toEqual([
        1, 4, 1,
      ]);
      // A boolean's label is its checkbox's own.
      expect(nsfw.querySelector('app-checkbox')?.textContent?.trim()).toBe('Allow mature content');
      expect(tags.querySelectorAll('app-checkbox')).toHaveLength(2);
      expect(actions(host)).toEqual(['submit', 'decline', 'dismiss']);
    });

    it('refuses to send a missing required field, and says so at the field', () => {
      const {host, click, answers} = render(FORM);
      click('submit');
      expect(answers).toEqual([]);
      const prompt = host.querySelector('[data-field="prompt"]');
      expect(prop(prompt, 'error')).toBe('Required.');
      // Typing clears the field's error.
      const card = render(FORM);
      card.click('submit');
      card.card.setText('prompt', 'fox');
      card.fixture.detectChanges();
      expect(prop(card.host.querySelector('[data-field="prompt"]'), 'error')).toBe('');
    });

    it('submits the defaults and what was typed, optional empty fields left out', () => {
      const {card, click, answers, fixture} = render(FORM);
      card.setText('prompt', 'a red fox');
      card.toggle('nsfw', true);
      card.toggleOption('tags', 1, true);
      fixture.detectChanges();
      click('submit');
      expect(answers).toEqual([
        {
          decision: 'approve',
          content: {prompt: 'a red fox', size: 'm', count: 1, nsfw: true, tags: ['b']},
        },
      ]);
    });

    it('keeps what was typed when the same question is re-announced', () => {
      const {card, fixture, answers} = render(FORM);
      card.setText('prompt', 'kept');
      card.pick('size', '0');
      fixture.componentRef.setInput('question', {...FORM, message: 'Again'});
      fixture.detectChanges();
      card.submit();
      // A new request starts over, from the defaults.
      fixture.componentRef.setInput('question', {...FORM, approvalId: 'ask-9'});
      fixture.detectChanges();
      card.setText('prompt', 'fresh');
      card.submit();
      expect(answers).toEqual([
        {decision: 'approve', content: {prompt: 'kept', size: 's', count: 1, nsfw: false}},
        {decision: 'approve', content: {prompt: 'fresh', size: 'm', count: 1, nsfw: false}},
      ]);
    });
  });

  describe('a link', () => {
    it('shows the full address with its host highlighted, and never fetches it', () => {
      const open = vi.spyOn(window, 'open').mockReturnValue(null);
      const {host, text} = render(LINK);
      expect(host.querySelector('[data-testid="question-url"]')?.textContent?.replace(/\s+/g, '')).toBe(
        'https://civitai.com/login?next=%2Fbuzz',
      );
      expect(host.querySelector('[data-testid="question-url-host"]')?.textContent).toBe('civitai.com');
      // No anchor, image, frame or prefetch hint carries the address.
      expect(host.querySelector('a, img, iframe, link')).toBeNull();
      expect(host.innerHTML).not.toContain('href=');
      expect(text()).toContain('Sign in to Civitai to continue.');
      expect(host.querySelector('[data-testid="question-url-lookalike"]')).toBeNull();
      expect(actions(host)).toEqual(['open-link', 'done', 'decline', 'dismiss']);
      expect(open).not.toHaveBeenCalled();
    });

    it('opens it only on a click, in a new tab that cannot reach back', () => {
      const open = vi.spyOn(window, 'open').mockReturnValue(null);
      const {click, answers} = render(LINK);
      expect(open).not.toHaveBeenCalled();
      click('open-link');
      expect(open).toHaveBeenCalledTimes(1);
      expect(open).toHaveBeenCalledWith(
        'https://civitai.com/login?next=%2Fbuzz',
        '_blank',
        'noopener,noreferrer',
      );
      // Opening is not an answer.
      expect(answers).toEqual([]);
      click('done');
      click('decline');
      click('dismiss');
      expect(answers).toEqual([{decision: 'approve'}, {decision: 'deny'}, {decision: 'cancel'}]);
    });

    it('warns of a look-alike host', () => {
      const {host} = render({...LINK, url: 'https://xn--civta-3ya.com/login'});
      expect(host.querySelector('[data-testid="question-url-lookalike"]')?.textContent).toContain(
        'look-alike characters',
      );
    });

    it('never opens an address that is not a web address', () => {
      const open = vi.spyOn(window, 'open').mockReturnValue(null);
      const {host, card} = render({...LINK, url: 'javascript:alert(1)'});
      expect(prop(host.querySelector('[data-action="open-link"]'), 'disabled')).toBe(true);
      expect(host.querySelector('[data-testid="question-url-unopenable"]')).not.toBeNull();
      card.openLink();
      expect(open).not.toHaveBeenCalled();
    });
  });
});

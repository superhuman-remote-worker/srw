import {describe, expect, it} from 'vitest';
import {
  McpQuestionSchema,
  collectQuestionAnswer,
  initialQuestionValues,
  questionDecisionBody,
  questionErrorDetail,
  questionFields,
  questionOutcome,
  questionToolLabel,
  questionUrlParts,
  toLocalDateTimeInput,
  toMcpQuestion,
  toMcpQuestions,
} from './mcp-question.model';

const WIRE = {
  id: 'call-1',
  approval_id: 'ask-1',
  server: 'Civitai',
  server_slug: 'civitai',
  tool: 'mcp__civitai__generate',
  mode: 'form',
  message: 'Spend 12 Buzz to run imageGen?',
  schema: {type: 'object', properties: {}},
  url: null,
  confirmation: {confirm: true},
};

describe('toMcpQuestion', () => {
  it('maps the wire entry to camelCase', () => {
    expect(toMcpQuestion(WIRE)).toEqual({
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
    });
  });

  it('keeps a null tool, a url question and a missing schema', () => {
    const q = toMcpQuestion({
      ...WIRE,
      tool: null,
      mode: 'url',
      schema: null,
      url: 'https://civitai.com/login',
      confirmation: null,
    })!;
    expect(q.tool).toBeNull();
    expect(q.mode).toBe('url');
    expect(q.schema).toBeNull();
    expect(q.url).toBe('https://civitai.com/login');
    expect(q.confirmation).toBeNull();
  });

  it('drops an entry it cannot answer (no tool call or request id)', () => {
    expect(toMcpQuestion({...WIRE, approval_id: ''})).toBeNull();
    expect(toMcpQuestion({...WIRE, id: undefined})).toBeNull();
    expect(toMcpQuestion(null)).toBeNull();
  });

  it('lists one question per call, the later row winning', () => {
    const list = toMcpQuestions([
      WIRE,
      {...WIRE, id: 'call-2', approval_id: 'ask-2'},
      {...WIRE, approval_id: 'ask-1b'},
      {nonsense: true},
    ]);
    expect(list.map((q) => [q.id, q.approvalId])).toEqual([
      ['call-2', 'ask-2'],
      ['call-1', 'ask-1b'],
    ]);
    expect(toMcpQuestions(undefined)).toEqual([]);
  });
});

describe('questionOutcome', () => {
  it('reads approved and denied, and everything else as not answered', () => {
    expect(questionOutcome('approved')).toBe('accepted');
    expect(questionOutcome('denied')).toBe('declined');
    for (const decision of ['expired', 'interrupted', 'unavailable', undefined]) {
      expect(questionOutcome(decision)).toBe('unanswered');
    }
  });
});

describe('questionDecisionBody', () => {
  it('sends the content of an acceptance, and always_accept only when chosen', () => {
    expect(questionDecisionBody({decision: 'approve', content: {confirm: true}})).toEqual({
      decision: 'approve',
      content: {confirm: true},
    });
    expect(
      questionDecisionBody({decision: 'approve', content: {confirm: true}, alwaysAccept: true}),
    ).toEqual({decision: 'approve', content: {confirm: true}, always_accept: true});
  });

  it('omits the content of a URL question\'s Done', () => {
    expect(questionDecisionBody({decision: 'approve'})).toEqual({decision: 'approve'});
  });

  it('sends a bare decline and dismiss', () => {
    expect(questionDecisionBody({decision: 'deny'})).toEqual({decision: 'deny'});
    expect(questionDecisionBody({decision: 'cancel'})).toEqual({decision: 'cancel'});
  });
});

describe('questionErrorDetail', () => {
  it('reads a string detail, a validation list, or nothing', () => {
    expect(questionErrorDetail({detail: 'count must be at most 4'})).toBe('count must be at most 4');
    expect(questionErrorDetail({detail: [{msg: 'a'}, {msg: 'b'}]})).toBe('a b');
    expect(questionErrorDetail({detail: ''})).toBeNull();
    expect(questionErrorDetail(null)).toBeNull();
  });
});

describe('questionToolLabel', () => {
  it('strips the agent\'s mcp__<server>__ prefix', () => {
    expect(questionToolLabel('mcp__civitai__generate', 'civitai')).toBe('generate');
    expect(questionToolLabel('mcp__my_server__list_items', 'my_server')).toBe('list_items');
    expect(questionToolLabel('mcp__civitai__generate')).toBe('generate');
    expect(questionToolLabel('run_command')).toBe('run_command');
    expect(questionToolLabel(null)).toBeNull();
  });
});

describe('questionUrlParts', () => {
  it('splits the address around its host', () => {
    expect(questionUrlParts('https://civitai.com/login?next=/buzz#top')).toEqual({
      href: 'https://civitai.com/login?next=/buzz#top',
      before: 'https://',
      host: 'civitai.com',
      after: '/login?next=/buzz#top',
      lookAlike: false,
      openable: true,
    });
  });

  it('highlights the real host behind a user@ prefix', () => {
    const parts = questionUrlParts('https://civitai.com@evil.example/pay')!;
    expect(parts.before).toBe('https://civitai.com@');
    expect(parts.host).toBe('evil.example');
    expect(parts.openable).toBe(true);
  });

  it('flags a punycode host, typed or converted from Unicode', () => {
    expect(questionUrlParts('https://xn--civta-3ya.com/')!.lookAlike).toBe(true);
    const unicode = questionUrlParts('https://cіvitai.com/')!; // Cyrillic і
    expect(unicode.host.startsWith('xn--')).toBe(true);
    expect(unicode.lookAlike).toBe(true);
  });

  it('never opens anything but a web address', () => {
    expect(questionUrlParts('javascript:alert(1)')!.openable).toBe(false);
    expect(questionUrlParts('data:text/html,hi')!.openable).toBe(false);
    expect(questionUrlParts('not a url')!.openable).toBe(false);
    expect(questionUrlParts(null)).toBeNull();
  });
});

const FORM: McpQuestionSchema = {
  type: 'object',
  properties: {
    prompt: {type: 'string', title: 'Prompt', description: 'What to draw', minLength: 3, maxLength: 20},
    email: {type: 'string', format: 'email'},
    site: {type: 'string', format: 'uri'},
    day: {type: 'string', format: 'date'},
    at: {type: 'string', format: 'date-time'},
    size: {type: 'string', enum: ['s', 'm', 'l'], enumNames: ['Small', 'Medium', 'Large'], default: 'm'},
    style: {type: 'string', oneOf: [{const: 'photo', title: 'Photo'}, {const: 'anime', title: 'Anime'}]},
    count: {type: 'integer', minimum: 1, maximum: 4, default: 1},
    strength: {type: 'number', minimum: 0, maximum: 1},
    nsfw: {type: 'boolean', default: false},
    tags: {
      type: 'array',
      items: {anyOf: [{const: 'a', title: 'Alpha'}, {const: 'b', title: 'Beta'}, {const: 'c', title: 'Gamma'}]},
      minItems: 1,
      maxItems: 2,
      default: ['b'],
    },
    extras: {type: 'array', items: {enum: ['x', 'y']}},
  },
  required: ['prompt', 'size', 'count', 'tags'],
};

describe('questionFields', () => {
  it('reads every supported kind, in property order, with labels and bounds', () => {
    const fields = questionFields(FORM);
    expect(fields.map((f) => [f.key, f.kind, f.required])).toEqual([
      ['prompt', 'text', true],
      ['email', 'email', false],
      ['site', 'url', false],
      ['day', 'date', false],
      ['at', 'datetime', false],
      ['size', 'select', true],
      ['style', 'select', false],
      ['count', 'integer', true],
      ['strength', 'number', false],
      ['nsfw', 'boolean', false],
      ['tags', 'multi', true],
      ['extras', 'multi', false],
    ]);
    const [prompt] = fields;
    expect(prompt.label).toBe('Prompt');
    expect(prompt.description).toBe('What to draw');
    expect([prompt.minLength, prompt.maxLength]).toEqual([3, 20]);
    // The key stands in for a missing title.
    expect(fields[1].label).toBe('email');
    expect(fields[5].options).toEqual([
      {value: 's', label: 'Small'},
      {value: 'm', label: 'Medium'},
      {value: 'l', label: 'Large'},
    ]);
    expect(fields[6].options.map((o) => o.label)).toEqual(['Photo', 'Anime']);
    expect(fields[10].options.map((o) => o.value)).toEqual(['a', 'b', 'c']);
    expect(fields[11].options.map((o) => o.label)).toEqual(['x', 'y']);
  });

  it('is empty for a form without properties', () => {
    expect(questionFields(null)).toEqual([]);
    expect(questionFields({type: 'object'})).toEqual([]);
  });
});

describe('initialQuestionValues + collectQuestionAnswer', () => {
  const fields = questionFields(FORM);

  it('prefills the defaults', () => {
    const values = initialQuestionValues(fields);
    expect(values['size']).toBe('1');
    expect(values['count']).toBe('1');
    expect(values['nsfw']).toBe(false);
    expect(values['tags']).toEqual([1]);
    expect(values['prompt']).toBe('');
  });

  it('builds the content from the defaults and what was typed, leaving optional empty fields out', () => {
    const values = {...initialQuestionValues(fields), prompt: 'a red fox'};
    const {content, errors} = collectQuestionAnswer(fields, values);
    expect(errors).toEqual({});
    expect(content).toEqual({prompt: 'a red fox', size: 'm', count: 1, nsfw: false, tags: ['b']});
  });

  it('sends each kind as its own type', () => {
    const values = {
      ...initialQuestionValues(fields),
      prompt: 'fox',
      email: ' me@example.com ',
      site: 'https://example.com',
      day: '2026-11-17',
      at: '2026-11-17T10:30',
      style: '1',
      count: '3',
      strength: '0.5',
      nsfw: true,
      tags: [0, 2],
      extras: [1],
    };
    const {content, errors} = collectQuestionAnswer(fields, values);
    expect(errors).toEqual({});
    expect(content).toEqual({
      prompt: 'fox',
      email: 'me@example.com',
      site: 'https://example.com',
      day: '2026-11-17',
      at: new Date('2026-11-17T10:30').toISOString(),
      size: 'm',
      style: 'anime',
      count: 3,
      strength: 0.5,
      nsfw: true,
      tags: ['a', 'c'],
      extras: ['y'],
    });
  });

  it('refuses a missing required value and every broken bound before sending', () => {
    const values = {
      ...initialQuestionValues(fields),
      prompt: 'ab',
      email: 'nope',
      site: 'example.com',
      day: '17.11.2026',
      size: '',
      count: '9',
      strength: '1.5',
      tags: [0, 1, 2],
    };
    const {errors} = collectQuestionAnswer(fields, values);
    expect(errors).toEqual({
      prompt: {key: 'minLength', params: {min: 3}},
      email: {key: 'email'},
      site: {key: 'url'},
      day: {key: 'date'},
      size: {key: 'required'},
      count: {key: 'maximum', params: {max: 4}},
      strength: {key: 'maximum', params: {max: 1}},
      tags: {key: 'maxItems', params: {max: 2}},
    });
  });

  it('checks integers, lower bounds, lengths and empty multi-selects', () => {
    const values = {
      ...initialQuestionValues(fields),
      prompt: 'x'.repeat(21),
      count: '1.5',
      strength: '-1',
      tags: [],
    };
    const {errors} = collectQuestionAnswer(fields, values);
    expect(errors['prompt']).toEqual({key: 'maxLength', params: {max: 20}});
    expect(errors['count']).toEqual({key: 'integer'});
    expect(errors['strength']).toEqual({key: 'minimum', params: {min: 0}});
    expect(errors['tags']).toEqual({key: 'minItems', params: {min: 1}});
    expect(
      collectQuestionAnswer(fields, {...values, prompt: '   ', count: 'abc'}).errors,
    ).toMatchObject({prompt: {key: 'required'}, count: {key: 'number'}});
  });

  it('counts characters, not UTF-16 units', () => {
    const [prompt] = questionFields({properties: {p: {type: 'string', maxLength: 3}}});
    expect(collectQuestionAnswer([prompt], {p: '🦊🦊🦊'}).errors).toEqual({});
  });
});

describe('toLocalDateTimeInput', () => {
  it('turns an RFC 3339 default into the input\'s local value', () => {
    const iso = '2026-11-17T09:05:00Z';
    const local = toLocalDateTimeInput(iso);
    expect(local).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/);
    expect(new Date(local).toISOString()).toBe(new Date(iso).toISOString());
    expect(toLocalDateTimeInput('garbage')).toBe('');
    expect(toLocalDateTimeInput(undefined)).toBe('');
  });
});

describe('toMcpQuestion field order', () => {
  it('shows the fields in the order the server asked, which JSONB loses', () => {
    const q = toMcpQuestion({
      id: 'call-1',
      approval_id: 'row-1',
      server: 'Civitai',
      mode: 'form',
      message: 'Which size?',
      // A database round trip sorted the keys: note before size.
      schema: {
        type: 'object',
        properties: {note: {type: 'string'}, size: {type: 'string', enum: ['s', 'l']}},
      },
      field_order: ['size', 'note'],
    });
    expect(questionFields(q!.schema).map((field) => field.key)).toEqual(['size', 'note']);
  });

  it('keeps the schema as sent without an order, and lists unnamed fields last', () => {
    const schema = {type: 'object', properties: {b: {type: 'string'}, a: {type: 'string'}}};
    const plain = toMcpQuestion({id: 'c', approval_id: 'r', mode: 'form', schema});
    expect(questionFields(plain!.schema).map((field) => field.key)).toEqual(['b', 'a']);
    const partial = toMcpQuestion({
      id: 'c', approval_id: 'r', mode: 'form', schema, field_order: ['a', 'gone'],
    });
    expect(questionFields(partial!.schema).map((field) => field.key)).toEqual(['a', 'b']);
  });
});

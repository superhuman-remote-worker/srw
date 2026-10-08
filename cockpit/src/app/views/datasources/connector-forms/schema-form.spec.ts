import {describe, expect, it} from 'vitest';
import {
  ConnectorDriver,
  ConnectorDriverMatrix,
  JsonSchema,
} from '../../../core/models/connector-driver.model';
import {
  ChoiceState,
  buildFormModel,
  connectorFormError,
  errorAnchor,
  fieldsOf,
  formValue,
  initialFormState,
  parseSchema,
  renderedPointers,
  setStateAt,
  stateAt,
  toValue,
} from './schema-form';
import fixture from '../../../core/models/fixtures/connector-drivers.json';

const matrix = fixture as unknown as ConnectorDriverMatrix;
const builtin = (name: string): ConnectorDriver =>
  matrix.drivers.find((driver) => driver.name === name)!;

/** A driver around a config schema and slots, as an image author writes it. */
function driver(config: JsonSchema, slots: ConnectorDriver['credential_slots'] = []): ConnectorDriver {
  return {
    ...builtin('srw.generic/v1'),
    name: 'community.example/v1',
    legacy_type: null,
    legacy_connection_url: 'forbidden',
    config_schema: config,
    credential_slots: slots,
  };
}

const TARGET: JsonSchema = {
  type: 'object',
  properties: {
    target: {
      oneOf: [
        {
          title: 'Database',
          type: 'object',
          properties: {kind: {const: 'db'}, host: {type: 'string'}, port: {type: 'integer'}},
        },
        {
          type: 'object',
          properties: {kind: {const: 'bucket'}, bucket: {type: 'string'}},
        },
      ],
    },
  },
};

describe('every built-in spec', () => {
  it.each(matrix.drivers.map((d) => [d.name, d] as const))(
    '%s builds a form whose blank create sends nothing it was not given',
    (_name, spec) => {
      const model = buildFormModel(spec);
      const state = initialFormState(model);
      const value = formValue(model, state, false);
      expect(value.config).toBeUndefined();
      expect(value.credentials).toBeUndefined();
      // Every config property and every slot key is a rendered field.
      const pointers = renderedPointers(model, state);
      for (const key of Object.keys(spec.config_schema.properties ?? {})) {
        expect(pointers).toContain(`/config/${key}`);
      }
      for (const slot of spec.credential_slots) {
        for (const key of Object.keys(slot.schema.properties ?? {})) {
          expect(pointers).toContain(`/credentials/${key}`);
        }
      }
    },
  );

  it('asks a create for what the specs require, and an edit for nothing', () => {
    const model = buildFormModel(builtin('srw.email/v1'));
    const reasons = (editing: boolean) =>
      formValue(model, initialFormState(model), editing).problems.map((p) => `${p.pointer}:${p.reason}`);
    expect(reasons(false)).toEqual([
      '/credentials/username:required',
      '/credentials/password:required',
      '/credentials/imap:required',
    ]);
    expect(reasons(true)).toEqual([]);
  });

  it('renders file secrets with the file widget and SSH keys as multiline files', () => {
    const files = buildFormModel(builtin('srw.kubeconfig/v1')).slots[0].node;
    const item = fieldsOf(files)[0].item!;
    const contents = fieldsOf(item).find((f) => f.key === 'contents')!;
    expect(contents.widget).toBe('file');
    expect(contents.secret).toBe(true);
    const repo = buildFormModel(builtin('srw.repository/v1'));
    expect(fieldsOf(repo.slots[1].node)[0].widget).toBe('file');
    expect(fieldsOf(repo.config).find((f) => f.key === 'known_hosts')!.widget).toBe('textarea');
  });

  it('starts a list at its minimum and edits a map as rows', () => {
    const model = buildFormModel(builtin('srw.generic-file/v1'));
    const state = initialFormState(model);
    expect(stateAt(state, 'slots/files/files')).toHaveLength(1);
    const env = buildFormModel(builtin('srw.generic/v1'));
    expect(fieldsOf(env.slots[0].node)[0].kind).toBe('map');
  });

  it('prefills config on an edit, never a secret', () => {
    const model = buildFormModel(builtin('srw.email/v1'));
    const state = initialFormState(model, {config: {access: 'send', folders: ['AI']}});
    const value = formValue(model, state, true);
    expect(value.config).toEqual({access: 'send', folders: ['AI']});
    expect(value.credentials).toBeUndefined();
  });

  it('keeps a redacted connection URL blank, so an edit leaves it alone', () => {
    const model = buildFormModel(builtin('srw.postgresql/v1'));
    expect(model.connectionUrl).toBe('optional');
    const state = initialFormState(model, {connection_url: 'postgresql://***', connection_url_redacted: true});
    expect(formValue(model, state, true).connection_url).toBeUndefined();
  });
});

describe('oneOf with a const discriminator', () => {
  it('labels branches by title or const and sends the discriminator', () => {
    const model = buildFormModel(driver(TARGET));
    const choice = fieldsOf(model.config)[0];
    expect(choice.kind).toBe('choice');
    expect(choice.discriminator).toBe('kind');
    expect(choice.branches.map((b) => b.label)).toEqual(['Database', 'bucket']);

    const state = initialFormState(model);
    setStateAt(state, 'config/target/values/0/host', 'db.internal');
    expect(formValue(model, state, false).config).toEqual({target: {host: 'db.internal', kind: 'db'}});

    setStateAt(state, 'config/target/branch', 1);
    expect(formValue(model, state, false).config).toEqual({target: {kind: 'bucket'}});
    // Switching back keeps what was typed in the first branch.
    setStateAt(state, 'config/target/branch', 0);
    expect(formValue(model, state, false).config).toEqual({target: {host: 'db.internal', kind: 'db'}});
  });

  it('opens an edit on the stored branch', () => {
    const model = buildFormModel(driver(TARGET));
    const state = initialFormState(model, {config: {target: {kind: 'bucket', bucket: 'b1'}}});
    expect((stateAt(state, 'config/target') as ChoiceState).branch).toBe(1);
    expect(formValue(model, state, true).config).toEqual({target: {kind: 'bucket', bucket: 'b1'}});
  });

  it('picks branches by position when nothing discriminates them', () => {
    const env = buildFormModel(builtin('srw.env/v1'));
    const values = fieldsOf(env.config)[0].entry!;
    expect(values.kind).toBe('choice');
    expect(values.discriminator).toBeNull();
    expect(values.branches.map((b) => b.label)).toEqual(['string', 'credential']);
  });
});

describe('writeOnly secrets', () => {
  const slots: ConnectorDriver['credential_slots'] = [
    {
      name: 'api',
      kind: 'secret_string',
      required: true,
      rotatable: false,
      access_levels: [],
      delivery: null,
      update: 'keep_if_blank',
      schema: {
        type: 'object',
        required: ['key'],
        properties: {key: {type: 'string', writeOnly: true}, user: {type: 'string'}},
      },
    },
  ];

  it('are masked inputs', () => {
    const model = buildFormModel(driver({type: 'object', maxProperties: 0}, slots));
    expect(fieldsOf(model.slots[0].node)[0].widget).toBe('password');
  });

  it('blank keeps the stored value on an edit and is required on a create', () => {
    const model = buildFormModel(driver({type: 'object', maxProperties: 0}, slots));
    const state = initialFormState(model);
    expect(formValue(model, state, true)).toEqual({problems: []});
    expect(formValue(model, state, false).problems).toEqual([
      {pointer: '/credentials/key', reason: 'required'},
    ]);

    // Changing the non-secret part of a slot on an edit keeps the secret.
    setStateAt(state, 'slots/api/user', 'svc');
    expect(formValue(model, state, true)).toEqual({credentials: {user: 'svc'}, problems: []});

    setStateAt(state, 'slots/api/key', 's3cret');
    expect(formValue(model, state, true).credentials).toEqual({key: 's3cret', user: 'svc'});
  });
});

describe('UI hints', () => {
  const HINTED: JsonSchema = {
    type: 'object',
    properties: {
      notes: {type: 'string', 'x-srw-multiline': true},
      host: {type: 'string', 'x-srw-order': 1, 'x-srw-group': 'Server'},
      port: {type: 'integer', 'x-srw-order': 2, 'x-srw-group': 'Server'},
      cert: {type: 'string', 'x-srw-widget': 'file', 'x-srw-group': 'TLS'},
      extra: {type: 'object', 'x-srw-widget': 'json'},
      name: {type: 'string', 'x-srw-order': 0},
    },
  };

  it('order and group the fields', () => {
    const node = parseSchema(HINTED);
    expect(node.groups.map((g) => [g.name, g.fields.map((f) => f.key)])).toEqual([
      [null, ['name', 'notes', 'extra']],
      ['Server', ['host', 'port']],
      ['TLS', ['cert']],
    ]);
  });

  it('pick the widgets', () => {
    const fields = Object.fromEntries(fieldsOf(parseSchema(HINTED)).map((f) => [f.key, f]));
    expect(fields['notes'].widget).toBe('textarea');
    expect(fields['cert'].widget).toBe('file');
    expect(fields['extra'].kind).toBe('json');
    expect(fields['port'].kind).toBe('number');
  });
});

describe('local checks', () => {
  it('flag a bad map key, a bad number and bad JSON; the API decides the rest', () => {
    const model = buildFormModel(
      driver({
        type: 'object',
        properties: {
          port: {type: 'integer'},
          extra: {type: 'object', 'x-srw-widget': 'json'},
          env: {type: 'object', propertyNames: {pattern: '^[A-Z_]+$'}, additionalProperties: {type: 'string'}},
        },
      }),
    );
    const state = initialFormState(model);
    setStateAt(state, 'config/port', 'eighty');
    setStateAt(state, 'config/extra', '{nope');
    setStateAt(state, 'config/env', [{key: 'bad-key', value: 'x'}, {key: 'GOOD', value: 'y'}]);
    expect(formValue(model, state, false).problems).toEqual([
      {pointer: '/config/port', reason: 'not_a_number'},
      {pointer: '/config/extra', reason: 'invalid_json'},
      {pointer: '/config/env/bad-key', reason: 'pattern'},
    ]);
  });

  it('drop a blank value, keep a set boolean', () => {
    const node = parseSchema({type: 'object', properties: {a: {type: 'string'}, b: {type: 'boolean'}}});
    expect(toValue(node, {a: '', b: false})).toBeUndefined();
    expect(toValue(node, {a: '', b: true})).toEqual({b: true});
  });
});

describe('the API refusal', () => {
  it('reads a plain detail, a driver error envelope and FastAPI validation', () => {
    expect(connectorFormError({error: {detail: 'Repository connectors require a URL'}})).toEqual({
      message: 'Repository connectors require a URL',
      field: null,
    });
    expect(connectorFormError({error: {detail: {message: 'bad host', field: '/config/host'}}})).toEqual({
      message: 'bad host',
      field: '/config/host',
    });
    expect(
      connectorFormError({error: {detail: [{loc: ['body', 'config', 'port'], msg: 'not an int'}]}}),
    ).toEqual({message: 'not an int', field: '/config/port'});
    expect(connectorFormError({})).toBeNull();
  });

  it('anchors at the deepest rendered field, else above the form', () => {
    const rendered = new Set(['/config/imap', '/config/imap/host']);
    expect(errorAnchor('/config/imap/host', rendered)).toBe('/config/imap/host');
    expect(errorAnchor('/config/imap/port', rendered)).toBe('/config/imap');
    expect(errorAnchor('/credentials/x', rendered)).toBeNull();
    expect(errorAnchor(null, rendered)).toBeNull();
  });
});

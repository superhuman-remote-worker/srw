import {beforeAll, beforeEach, describe, expect, it, vi} from 'vitest';
import {CUSTOM_ELEMENTS_SCHEMA, ɵresolveComponentResources} from '@angular/core';
import {NgTemplateOutlet} from '@angular/common';
import {ComponentFixture, TestBed} from '@angular/core/testing';
import {TranslocoPipe, TranslocoTestingModule} from '@jsverse/transloco';
import {GenericConnectorFormComponent} from './generic-connector-form.component';
import {
  ConnectorDriver,
  ConnectorDriverMatrix,
  JsonSchema,
} from '../../../core/models/connector-driver.model';
import {ConnectorFormError, ExistingConnector, FieldState, GenericFormValue} from './schema-form';
import fixture from '../../../core/models/fixtures/connector-drivers.json';
// The rows the managed MCP servers add where the chart installs them.
import managedFixture from '../../../core/models/fixtures/connector-drivers-managed.json';
// The real catalogue, so these specs also prove the keys exist.
import en from '../../../../assets/i18n/en.json';

/*
 * The ui primitives are left as unknown elements: this repo's vitest JIT
 * harness wires neither their signal inputs nor their outputs (see
 * helm-managed-badge.component.spec.ts). Their bindings land as DOM
 * properties, which these specs read; typing goes through the component's
 * own `set`/`pick`, the methods the primitives' outputs call. The native
 * file and checkbox inputs are driven through the DOM.
 */

const matrix = fixture as unknown as ConnectorDriverMatrix;
const managed = managedFixture as unknown as ConnectorDriverMatrix;
const builtin = (name: string): ConnectorDriver =>
  matrix.drivers.find((driver) => driver.name === name)!;

function imageDriver(
  config: JsonSchema,
  slots: ConnectorDriver['credential_slots'] = [],
): ConnectorDriver {
  return {
    ...builtin('srw.generic/v1'),
    name: 'community.example/v1',
    title: 'Example',
    legacy_type: null,
    legacy_connection_url: 'forbidden',
    config_schema: config,
    credential_slots: slots,
  };
}

const SECRET_SLOT: ConnectorDriver['credential_slots'][number] = {
  name: 'api',
  kind: 'secret_string',
  required: true,
  rotatable: false,
  access_levels: [],
  delivery: null,
  update: 'keep_if_blank',
  schema: {type: 'object', properties: {key: {type: 'string', writeOnly: true}}},
};

interface Form {
  set(path: string, value: FieldState, pointer: string): void;
  pick(path: string, value: string | null, pointer: string): void;
}

interface Rendered {
  fixture: ComponentFixture<GenericConnectorFormComponent>;
  host: HTMLElement;
  form: Form;
  last: () => GenericFormValue;
  /** Type into a field as its primitive's valueChange would, then render. */
  type: (path: string, pointer: string, text: string) => void;
}

function render(
  driver: ConnectorDriver,
  options: {editing?: boolean; existing?: ExistingConnector | null; error?: ConnectorFormError | null} = {},
): Rendered {
  const fixture = TestBed.createComponent(GenericConnectorFormComponent);
  const values: GenericFormValue[] = [];
  fixture.componentInstance.valueChange.subscribe((value) => values.push(value));
  fixture.componentRef.setInput('driver', driver);
  fixture.componentRef.setInput('editing', options.editing ?? false);
  fixture.componentRef.setInput('existing', options.existing ?? null);
  fixture.componentRef.setInput('error', options.error ?? null);
  fixture.detectChanges();
  const form = fixture.componentInstance as unknown as Form;
  return {
    fixture,
    host: fixture.nativeElement,
    form,
    last: () => values[values.length - 1],
    type: (path, pointer, text) => {
      form.set(path, text, pointer);
      fixture.detectChanges();
    },
  };
}

function field(host: HTMLElement, pointer: string): HTMLElement {
  const element = host.querySelector<HTMLElement>(`[data-pointer="${pointer}"]`);
  if (!element) throw new Error(`no field ${pointer}`);
  return element;
}

/** A bound DOM property of an inert primitive element. */
function prop(element: Element | null, name: string): unknown {
  return (element as unknown as Record<string, unknown> | null)?.[name];
}

describe('GenericConnectorFormComponent', () => {
  beforeAll(async () => {
    // The declared imports still list the primitives (styleUrl); resolve
    // them before TestBed compiles, as helm-managed-badge.component.spec.ts.
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  beforeEach(async () => {
    TestBed.configureTestingModule({
      imports: [
        GenericConnectorFormComponent,
        TranslocoTestingModule.forRoot({
          langs: {en},
          translocoConfig: {availableLangs: ['en'], defaultLang: 'en'},
          preloadLangs: true,
        }),
      ],
    });
    TestBed.overrideComponent(GenericConnectorFormComponent, {
      set: {imports: [NgTemplateOutlet, TranslocoPipe], schemas: [CUSTOM_ELEMENTS_SCHEMA]},
    });
  });

  it.each(matrix.drivers.map((driver) => [driver.name, driver] as const))(
    'renders the built-in %s from its spec alone',
    (_name, driver) => {
      const {host, last} = render(driver);
      expect(host.querySelector('.generic-form')?.getAttribute('data-driver')).toBe(driver.name);
      for (const [key, schema] of Object.entries(driver.config_schema.properties ?? {})) {
        // A readOnly property mirrors the connector row; SRW sets it.
        if (schema.readOnly) {
          expect(host.querySelector(`[data-pointer="/config/${key}"]`)).toBeNull();
        }
        else expect(field(host, `/config/${key}`)).toBeTruthy();
      }
      for (const slot of driver.credential_slots) {
        expect(host.querySelector(`[data-slot="${slot.name}"]`)).toBeTruthy();
        for (const [key, schema] of Object.entries(slot.schema.properties ?? {})) {
          if (schema.const !== undefined) continue;
          expect(field(host, `/credentials/${key}`)).toBeTruthy();
        }
      }
      expect(!!host.querySelector('[data-pointer="/connection_url"]')).toBe(
        driver.legacy_connection_url !== 'forbidden',
      );
      expect(last().credentials).toBeUndefined();
    },
  );

  describe('a managed MCP server (no bespoke form: this is its form)', () => {
    const gitea = managed.drivers.find((driver) => driver.name === 'srw.gitea-mcp/v1')!;

    it('offers exactly its access levels, asks for its URL and token, and no connection URL', () => {
      const {host} = render(gitea);
      const options = [...field(host, '/config/access').querySelectorAll('app-select option')];
      // The first option leaves it unset: the driver's default level applies.
      expect(options.map((o) => o.textContent?.trim())).toEqual([
        en.datasources.generic.unset,
        ...gitea.access_levels.map((level) => level.id),
      ]);
      expect(gitea.access_levels.map((level) => level.id)).toEqual(['ReadOnly', 'ReadWrite']);
      expect(field(host, '/config/url')).toBeTruthy();
      expect(host.querySelector('[data-pointer="/config/host"]')).toBeNull();
      expect(host.querySelector('[data-pointer="/connection_url"]')).toBeNull();
      expect(prop(field(host, '/credentials/token').querySelector('app-input'), 'type')).toBe('password');
    });

    it('submits the URL, the chosen level and the token', () => {
      const {type, last} = render(gitea);
      expect(last().problems).toEqual([{pointer: '/credentials/token', reason: 'required'}]);
      // Once the config is in use, its required URL is too (a blank config
      // is the API's to refuse, as for every driver).
      type('config/access', '/config/access', '0');
      expect(last().problems).toContainEqual({pointer: '/config/url', reason: 'required'});
      type('config/url', '/config/url', 'https://gitea.example.com/path');
      expect(last().problems).toContainEqual({pointer: '/config/url', reason: 'pattern'});
      type('config/url', '/config/url', 'https://gitea.example.com');
      type('slots/token/token', '/credentials/token', 'gitea-token');
      expect(last()).toEqual({
        config: {url: 'https://gitea.example.com', access: 'ReadOnly'},
        credentials: {token: 'gitea-token'},
        problems: [],
      });
    });
  });

  it('submits a typed built-in: a Neo4j URL and login', () => {
    const {host, type, last} = render(builtin('srw.neo4j/v1'));
    type('connection_url', '/connection_url', 'bolt://graph:7687');
    // A slot's keys sit at the top of the credentials object.
    type('slots/login/username', '/credentials/username', 'neo');
    expect(last()).toMatchObject({
      connection_url: 'bolt://graph:7687',
      credentials: {username: 'neo'},
    });
    expect(prop(field(host, '/connection_url').querySelector('app-input'), 'value')).toBe(
      'bolt://graph:7687',
    );
  });

  describe('oneOf with a const discriminator', () => {
    const TARGET = imageDriver({
      type: 'object',
      properties: {
        target: {
          oneOf: [
            {title: 'Database', type: 'object', properties: {kind: {const: 'db'}, host: {type: 'string'}}},
            {type: 'object', properties: {kind: {const: 'bucket'}, bucket: {type: 'string'}}},
          ],
        },
      },
    });

    it('offers the branches as variants and sends the chosen const', () => {
      const {host, fixture, form, type, last} = render(TARGET);
      const options = [...field(host, '/config/target').querySelectorAll('app-select option')];
      expect(options.map((o) => o.textContent?.trim())).toEqual(['Database', 'bucket']);

      type('config/target/values/0/host', '/config/target/host', 'db.internal');
      expect(last().config).toEqual({target: {host: 'db.internal', kind: 'db'}});

      form.pick('config/target', '1', '/config/target');
      fixture.detectChanges();
      expect(host.querySelector('[data-pointer="/config/target/host"]')).toBeNull();
      expect(field(host, '/config/target/bucket')).toBeTruthy();
      type('config/target/values/1/bucket', '/config/target/bucket', 'b1');
      expect(last().config).toEqual({target: {bucket: 'b1', kind: 'bucket'}});
    });
  });

  describe('writeOnly secrets', () => {
    const SECRET_ONLY = imageDriver({type: 'object', maxProperties: 0}, [SECRET_SLOT]);

    it('mask, show the keep placeholder on an edit and send nothing while blank', () => {
      const {host, type, last} = render(SECRET_ONLY, {editing: true});
      const input = field(host, '/credentials/key').querySelector('app-input');
      expect(prop(input, 'type')).toBe('password');
      expect(prop(input, 'placeholder')).toBe(en.datasources.generic.keepStored);
      expect(host.querySelector('[data-hint="replace-all"]')?.textContent?.trim()).toBe(
        en.datasources.generic.replaceAll,
      );
      // Not required on an edit: blank keeps.
      expect(field(host, '/credentials/key').querySelector('.gf-required')).toBeNull();
      expect(last()).toEqual({problems: []});

      type('slots/api/key', '/credentials/key', 'rotated');
      expect(last().credentials).toEqual({key: 'rotated'});
    });

    it('are required on a create; the problem shows once the field is touched', () => {
      const {host, type, last} = render(SECRET_ONLY);
      expect(prop(field(host, '/credentials/key').querySelector('app-input'), 'placeholder')).toBe('');
      expect(last().problems).toEqual([{pointer: '/credentials/key', reason: 'required'}]);
      expect(host.querySelector('[data-slot="api"] legend .gf-required')).toBeTruthy();
      expect(field(host, '/credentials/key').querySelector('.gf-error')).toBeNull();

      type('slots/api/key', '/credentials/key', 'x');
      type('slots/api/key', '/credentials/key', '');
      expect(field(host, '/credentials/key').querySelector('.gf-error')?.textContent?.trim()).toBe(
        en.datasources.generic.problem.required,
      );
    });

    it('ask again on an edit for every secret once one credential is typed', () => {
      // A Neo4j edit that changes the username would otherwise store no password.
      const {host, type, last} = render(builtin('srw.neo4j/v1'), {editing: true});
      expect(host.querySelector('[data-hint="replace-all"]')).toBeTruthy();
      expect(last().problems).toEqual([]);
      type('slots/login/username', '/credentials/username', 'neo');
      expect(last().problems).toEqual([{pointer: '/credentials/password', reason: 'required'}]);
      // Shown though the password was never touched: it holds back Save.
      expect(field(host, '/credentials/password').querySelector('.gf-error')?.textContent?.trim()).toBe(
        en.datasources.generic.problem.required,
      );
      type('slots/login/password', '/credentials/password', 'pw');
      expect(last()).toEqual({credentials: {username: 'neo', password: 'pw'}, problems: []});
    });
  });

  describe('file inputs', () => {
    it('load a file into the field', async () => {
      const {host, fixture, last} = render(builtin('srw.kubeconfig/v1'));
      const contents = field(host, '/credentials/files/0/contents');
      const upload = contents.querySelector<HTMLInputElement>('input[type="file"]')!;
      const file = new File(['apiVersion: v1\nkind: Config\n'], 'config', {type: 'text/plain'});
      Object.defineProperty(upload, 'files', {value: [file]});
      upload.dispatchEvent(new Event('change'));
      await vi.waitFor(() => {
        fixture.detectChanges();
        expect(last().credentials).toEqual({files: [{contents: 'apiVersion: v1\nkind: Config\n'}]});
      });
      expect(prop(contents.querySelector('app-textarea'), 'value')).toBe(
        'apiVersion: v1\nkind: Config\n',
      );
    });

    it('add list items up to the maximum', () => {
      const {host, fixture} = render(builtin('srw.generic-file/v1'));
      const list = field(host, '/credentials/files');
      const add = () => list.querySelector(':scope > app-button.gf-add')!;
      for (let i = 0; i < 4; i++) {
        add().dispatchEvent(new Event('clicked'));
        fixture.detectChanges();
      }
      expect(list.querySelectorAll(':scope > .gf-row')).toHaveLength(5);
      expect(prop(add(), 'disabled')).toBe(true);
    });
  });

  describe('hints', () => {
    const HINTED = imageDriver({
      type: 'object',
      properties: {
        notes: {type: 'string', 'x-srw-multiline': true},
        port: {type: 'integer', 'x-srw-order': 2, 'x-srw-group': 'Server'},
        host: {type: 'string', 'x-srw-order': 1, 'x-srw-group': 'Server'},
        cert: {type: 'string', 'x-srw-widget': 'file'},
        verify: {type: 'boolean'},
        name: {type: 'string', 'x-srw-order': 0},
      },
    });

    it('order, group and pick widgets', () => {
      const {host} = render(HINTED);
      const pointers = [...host.querySelectorAll('[data-section="config"] [data-pointer]')].map((el) =>
        el.getAttribute('data-pointer'),
      );
      expect(pointers).toEqual([
        '/config/name',
        '/config/notes',
        '/config/cert',
        '/config/verify',
        '/config/host',
        '/config/port',
      ]);
      const server = host.querySelector('[data-group="Server"]')!;
      expect(server.querySelector('.gf-group-title')?.textContent?.trim()).toBe('Server');
      expect(field(host, '/config/notes').querySelector('app-textarea')).toBeTruthy();
      expect(field(host, '/config/notes').querySelector('input[type="file"]')).toBeNull();
      expect(field(host, '/config/cert').querySelector('input[type="file"]')).toBeTruthy();
      expect(field(host, '/config/port').querySelector('app-input')?.getAttribute('type')).toBe('number');
    });

    it('drives a boolean through its native checkbox', () => {
      const {host, fixture, last} = render(HINTED);
      const box = field(host, '/config/verify').querySelector<HTMLInputElement>('input[type="checkbox"]')!;
      box.checked = true;
      box.dispatchEvent(new Event('change'));
      fixture.detectChanges();
      expect(last().config).toEqual({verify: true});
    });
  });

  describe('the API refusal', () => {
    const HOSTED = imageDriver({type: 'object', properties: {host: {type: 'string'}}});

    it('shows at the field its pointer names', () => {
      const {host} = render(HOSTED, {error: {message: 'host is not reachable', field: '/config/host'}});
      expect(field(host, '/config/host').querySelector('.gf-error')?.textContent).toContain(
        'host is not reachable',
      );
      expect(host.querySelector('.gf-form-error')).toBeNull();
    });

    it('shows above the form without a field', () => {
      const {host} = render(HOSTED, {error: {message: 'Connector name already exists', field: null}});
      expect(host.querySelector('.gf-form-error')?.textContent).toContain('Connector name already exists');
    });
  });
});

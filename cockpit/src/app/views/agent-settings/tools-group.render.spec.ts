import {beforeAll, describe, expect, it} from 'vitest';
import {signal, ɵresolveComponentResources} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {TranslocoTestingModule} from '@jsverse/transloco';
import en from '../../../assets/i18n/en.json';
import type {
  SessionToolCategory,
  SessionToolGroupsResponse,
} from '../../core/services/api.service';
import {ToolsGroupComponent} from './tools-group.component';

/**
 * What the user actually SEES.
 *
 * The three-state control is the deliverable, and a control that resolves its
 * copy through transloco can pass every logic test while rendering
 * `agentSettings.toolCategories.mcp.label` on screen — AOT does not catch a
 * missing key, and neither does a spec that only reads component state. So
 * these mount the component, run change detection, and assert on RESOLVED
 * text: the reason sentence, the provenance headline, the labels, and the
 * absence of any raw key.
 */

function cat(over: Partial<SessionToolCategory> = {}): SessionToolCategory {
  return {state: 'on', settable: true, reason: null, decided_by: 'base', tools: [], ...over};
}

function response(over: Partial<SessionToolGroupsResponse> = {}): SessionToolGroupsResponse {
  return {
    thread_id: 't1',
    source: 'resolved',
    origin: 'agent',
    observed_at: '2026-08-03T10:00:00Z',
    prediction_reason: null,
    degraded_reason: null,
    enumerate_only: {shell: ['cancel_command', 'run_command', 'shell_execute', 'shell_read']},
    tool_groups: {},
    categories: {},
    ...over,
  };
}

/**
 * Mount with `mode` and `resolved` replaced directly.
 *
 * Signal inputs cannot be set through `setInput()` in this pipeline — the repo
 * does not run components through ngtsc for vitest, so `ɵcmp.inputs` is empty
 * and every binding throws NG0303. Same workaround, and same reason, as
 * model-group.component.spec.ts.
 */
function mount(options: {
  mode?: string;
  resolved?: SessionToolGroupsResponse | null;
  gatedCapabilities?: Record<string, unknown> | null;
  readsResolvedToolset?: boolean;
  enumerateOnly?: Record<string, string[]> | null;
  config?: Record<string, unknown> | (() => Record<string, unknown>);
  delegationCapScope?: 'worker' | 'session';
} = {}) {
  TestBed.configureTestingModule({
    imports: [
      ToolsGroupComponent,
      TranslocoTestingModule.forRoot({
        langs: {en},
        translocoConfig: {availableLangs: ['en'], defaultLang: 'en'},
      }),
    ],
  });
  const fixture = TestBed.createComponent(ToolsGroupComponent);
  const instance = fixture.componentInstance;
  Object.defineProperty(instance, 'mode', {value: () => options.mode ?? 'session'});
  Object.defineProperty(instance, 'resolved', {value: () => options.resolved ?? null});
  Object.defineProperty(instance, 'gatedCapabilities', {
    value: () => options.gatedCapabilities ?? null,
  });
  Object.defineProperty(instance, 'readsResolvedToolset', {
    value: () => options.readsResolvedToolset ?? true,
  });
  Object.defineProperty(instance, 'enumerateOnly', {
    value: () => options.enumerateOnly ?? null,
  });
  if (options.config) {
    const config = options.config;
    Object.defineProperty(instance, 'config', {
      value: typeof config === 'function' ? config : () => config,
    });
  }
  if (options.delegationCapScope) {
    Object.defineProperty(instance, 'delegationCapScope', {
      value: () => options.delegationCapScope,
    });
  }
  fixture.detectChanges();
  return fixture;
}

function text(fixture: {nativeElement: unknown}): string {
  return ((fixture.nativeElement as HTMLElement).textContent ?? '').replace(/\s+/g, ' ');
}

function rows(fixture: {nativeElement: unknown}): HTMLElement[] {
  return Array.from((fixture.nativeElement as HTMLElement).querySelectorAll('.tool-toggle'));
}

function rowFor(fixture: {nativeElement: unknown}, label: string): HTMLElement {
  const found = rows(fixture).find((row) => (row.textContent ?? '').includes(label));
  if (!found) throw new Error(`no row containing ${label!}; saw: ${text(fixture)}`);
  return found;
}

describe('ToolsGroupComponent rendering', () => {
  beforeAll(async () => {
    // The component declares no external templateUrl/styleUrls, but the shared
    // icon component in its imports is resolved the same way the other mounted
    // specs in this folder do it.
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  it('renders EVERY category the answer returns, not a curated four', () => {
    // The live pane showed 4 of 25 because that was all its transport carried.
    const fixture = mount({
      mode: 'live',
      resolved: response({
        categories: {
          canvas: cat(),
          job_control: cat({state: 'off'}),
          job_inspection: cat({state: 'off'}),
          orchestrator: cat({state: 'off'}),
          shell: cat({state: 'unavailable', settable: false, reason: 'requires the shell_tools capability grant'}),
          knowledge: cat({state: 'off'}),
          sql: cat({state: 'unavailable', settable: false, reason: 'a postgresql datasource decides'}),
        },
      }),
    });
    expect(rows(fixture)).toHaveLength(7);
    const body = text(fixture);
    expect(body).toContain('Canvas');
    expect(body).toContain('Job Control');
    expect(body).toContain('Job Inspection');
    expect(body).toContain('SRW Projects');
    expect(body).toContain('Shell');
    expect(body).toContain('Knowledge');
    expect(body).toContain('SQL');
  });

  it("renders the server's reason sentence, verbatim, next to the control", () => {
    const fixture = mount({
      resolved: response({
        categories: {
          shell: cat({
            state: 'unavailable',
            settable: false,
            reason: 'requires the shell_tools capability grant',
          }),
        },
      }),
    });
    const row = rowFor(fixture, 'Shell');
    expect(row.querySelector('.tool-toggle-reason')?.textContent?.trim()).toBe(
      'requires the shell_tools capability grant',
    );
  });

  it('an unavailable row has NO checkbox — three states, not a box that lies', () => {
    const fixture = mount({
      resolved: response({
        categories: {
          shell: cat({state: 'unavailable', settable: false, reason: 'this workspace tier has no shell'}),
          canvas: cat({state: 'on'}),
        },
      }),
    });
    const shell = rowFor(fixture, 'Shell');
    expect(shell.querySelector('input[type="checkbox"]')).toBeNull();
    expect(shell.querySelector('.tool-state-blocked')).not.toBeNull();
    expect(shell.classList.contains('unavailable')).toBe(true);

    const canvas = rowFor(fixture, 'Canvas');
    const box = canvas.querySelector('input[type="checkbox"]') as HTMLInputElement;
    expect(box).not.toBeNull();
    expect(box.checked).toBe(true);
  });

  it('an `off` row renders an unchecked, live checkbox', () => {
    const fixture = mount({resolved: response({categories: {knowledge: cat({state: 'off'})}})});
    const box = rowFor(fixture, 'Knowledge').querySelector(
      'input[type="checkbox"]',
    ) as HTMLInputElement;
    expect(box.checked).toBe(false);
    expect(box.disabled).toBe(false);
  });

  it('a measurement says so, in words, above the rows', () => {
    const fixture = mount({resolved: response({categories: {canvas: cat()}})});
    const banner = (fixture.nativeElement as HTMLElement).querySelector('.toolset-provenance');
    expect(banner?.getAttribute('data-trust')).toBe('measured');
    expect(banner?.textContent).toContain('Live from the running agent');
  });

  it('a PREDICTION says so, and carries the server sentence — never rendered as fact', () => {
    const fixture = mount({
      resolved: response({
        origin: 'prediction',
        observed_at: null,
        prediction_reason: 'no agent exists for an unsaved session',
        categories: {canvas: cat({tools: ['get_canvas']})},
      }),
    });
    const banner = (fixture.nativeElement as HTMLElement).querySelector('.toolset-provenance');
    expect(banner?.getAttribute('data-trust')).toBe('predicted');
    expect(banner?.textContent).toContain('Predicted, not measured');
    expect(banner?.textContent).toContain('no agent exists for an unsaved session');
    // And the per-row count is labelled a forecast too, not a fact.
    expect(rowFor(fixture, 'Canvas').textContent).toContain('1 predicted');
  });

  it('agent_partial reads as a measurement and shows what it is missing', () => {
    // The common path against the deployed fleet image. Rendering it as a
    // forecast would tell the user their live session is a guess.
    const fixture = mount({
      resolved: response({
        origin: 'agent_partial',
        observed_at: null,
        degraded_reason: 'this agent image predates GET /session/toolset',
        categories: {canvas: cat({tools: ['get_canvas', 'set_canvas']})},
      }),
    });
    const banner = (fixture.nativeElement as HTMLElement).querySelector('.toolset-provenance');
    expect(banner?.getAttribute('data-trust')).toBe('measured_partial');
    expect(banner?.textContent).toContain('Live from the running agent');
    expect(banner?.textContent).toContain('partial answer');
    expect(banner?.textContent).toContain('predates GET /session/toolset');
    expect(rowFor(fixture, 'Canvas').textContent).toContain('2 bound');
  });

  it('a creation form with no answer still renders its static list, labelled', () => {
    // The creation forms get their baseline from `prefillFromConfig`, so the
    // static fallback is as honest there as it ever was.
    const fixture = mount({mode: 'session', resolved: null});
    const banner = (fixture.nativeElement as HTMLElement).querySelector('.toolset-provenance');
    expect(banner?.getAttribute('data-trust')).toBe('unknown');
    expect(banner?.textContent).toContain('The resolved toolset could not be read');
    expect(rows(fixture).length).toBeGreaterThan(0);
  });

  it('JOB mode renders the answer it is given, not its six static rows', () => {
    // Job create shipped with no `resolved` binding at all, so it rendered
    // JOB_TOOL_CATEGORIES — six rows, two states, no grant gating — while
    // sessions got twenty-five and three states. Passing the answer has to be
    // enough; if `mode === 'job'` still narrowed anywhere, this fails.
    const categories: Record<string, SessionToolCategory> = {};
    for (const key of [
      'research', 'shell', 'core', 'knowledge', 'git', 'evaluation',
      'delegation', 'workspace', 'citation', 'communication', 'browser_direct',
    ]) {
      categories[key] = cat({state: 'off'});
    }
    const fixture = mount({mode: 'job', resolved: response({categories})});

    expect(rows(fixture)).toHaveLength(11);
    const body = text(fixture);
    // Assert on the METADATA description, not the label: an unknown key still
    // renders a humanised label via `humanize()`, so "Core" appearing proves
    // nothing about which catalogue supplied it. The description only appears
    // when the row resolved real metadata, and the "not recognised" fallback
    // string must be absent.
    expect(body).toContain('Planning, progress and completion');
    expect(body).toContain('Approve or return worker jobs with feedback');
    expect(body).not.toContain('does not recognise');
    expect(body).not.toContain('agentSettings.');
  });

  it('JOB mode greys a grant-blocked row instead of offering a dead checkbox', () => {
    const fixture = mount({
      mode: 'job',
      resolved: response({categories: {shell: cat({state: 'off'})}}),
      gatedCapabilities: {shell_tools: false},
    });
    const row = rowFor(fixture, 'Shell');
    expect(row.classList.contains('unavailable')).toBe(true);
    expect(row.querySelector('input[type="checkbox"]')).toBeNull();
  });

  it('the LIVE pane with no answer renders no rows at all — not twelve ticked ones', () => {
    // The degraded live path: an older orchestrator 404s, the network fails,
    // or the 8s deadline trips on a read that probes an agent pod. The live
    // pane has no config to fall back on — a stock session's config_override
    // has no `tools` key — so the static list rendered twelve categories ALL
    // TICKED, six of which ship `[]` in session_base, and every switch was
    // dead because the pane's dispatch is keyed off the resolved answer.
    // Six false assertions and twelve dead toggles: this task's own two
    // headline defects, on the fallback path.
    const fixture = mount({mode: 'live', resolved: null});
    expect(rows(fixture)).toHaveLength(0);
    expect(
      (fixture.nativeElement as HTMLElement).querySelectorAll('input[type="checkbox"]'),
    ).toHaveLength(0);
    const banner = (fixture.nativeElement as HTMLElement).querySelector('.toolset-provenance');
    expect(banner?.textContent).toContain('The resolved toolset could not be read');
  });

  it('a bound-but-locked category renders CHECKED and locked, never blocked', () => {
    // `product_help` and `session_task` are unconditional persistent-session
    // floors, so every single session has two of these. Drawing a block glyph
    // over tools the agent is actively holding is the same class of lie as the
    // checkbox this control replaces, pointed the other way — and it recolours
    // the "you cannot change this" sentence into "this is off".
    const fixture = mount({
      resolved: response({
        categories: {
          product_help: cat({
            state: 'on',
            settable: false,
            reason: 'granted by the runtime, not by config (persistent-session floor)',
            tools: ['read_product_guide', 'get_product_capabilities'],
          }),
        },
      }),
    });
    const row = rowFor(fixture, 'Product Help');
    expect(row.querySelector('.tool-state-blocked')).toBeNull();
    const box = row.querySelector('input[type="checkbox"]') as HTMLInputElement;
    expect(box).not.toBeNull();
    expect(box.checked).toBe(true);
    expect(box.disabled).toBe(true);
    expect(row.classList.contains('unavailable')).toBe(false);
    expect(row.querySelector('.tool-lock')).not.toBeNull();
    expect(row.textContent).toContain('2 bound');
    // The sentence is a NOTE, not a denial — different element, muted colour.
    expect(row.querySelector('.tool-toggle-note')?.textContent).toContain(
      'granted by the runtime',
    );
    expect(row.querySelector('.tool-toggle-reason')).toBeNull();
  });

  it('a locked-ON row offers an ADD control, and the checkbox is still un-untickable', () => {
    // The defect a live browser drive caught and every green unit test missed:
    // the machinery to gain shell on a running session was correct and
    // reachable only by calling component methods. The rendered `<input>` is
    // `disabled`, a disabled checkbox fires no `change`, and there is no other
    // route — so the capability existed and no user could get to it.
    //
    // Both halves are asserted HERE, on the DOM, because that is where the
    // defect lived: the checkbox must stay inert (unticking a code-granted
    // category is fiction) AND the additive action must exist as its own
    // control, because a ticked box has no "turn on" gesture to offer.
    const fixture = mount({
      mode: 'live',
      resolved: response({
        categories: {
          shell: cat({
            state: 'on',
            settable: false,
            reason:
              'the runtime binds srw_cloud_status here regardless of config '
              + '(cloud_mount_manager.active), so unticking this group cannot release it',
            tools: ['srw_cloud_status'],
          }),
        },
      }),
    });
    const instance = fixture.componentInstance;
    instance.prefillFromResolved(instance.resolved()!.categories!);
    fixture.detectChanges();

    const row = rowFor(fixture, 'Shell');
    const box = row.querySelector('input[type="checkbox"]') as HTMLInputElement;
    expect(box.checked).toBe(true);
    expect(box.disabled).toBe(true);

    // Half 1 — the off direction is unreachable AND unwritable. A real click
    // on a disabled input is a no-op; a forced `change` (what an extension or
    // a stray dispatchEvent could still deliver) must not produce a fragment.
    box.click();
    box.dispatchEvent(new Event('change'));
    fixture.detectChanges();
    expect(
      (rowFor(fixture, 'Shell').querySelector('input[type="checkbox"]') as HTMLInputElement).checked,
    ).toBe(true);
    expect(instance.getOverrides()).toEqual({});

    // Half 2 — the add control exists, names what it will add, and writes the
    // enumeration the server served.
    const block = (fixture.nativeElement as HTMLElement).querySelector(
      '.tool-additions[data-category="shell"]',
    ) as HTMLElement;
    expect(block).not.toBeNull();
    expect(block.textContent).toContain('config can still add');
    expect(block.textContent).toContain('run_command');
    const button = block.querySelector('button') as HTMLButtonElement;
    expect(button.disabled).toBe(false);
    expect(button.textContent).toContain('Add (4)');

    button.click();
    fixture.detectChanges();

    expect(instance.getOverrides()).toEqual({
      tools: {shell: {only: ['cancel_command', 'run_command', 'shell_execute', 'shell_read']}},
    });
    // ...and the control says the request was made rather than accepting a
    // second click that would diff to nothing.
    const after = (fixture.nativeElement as HTMLElement).querySelector(
      '.tool-additions[data-category="shell"] button',
    ) as HTMLButtonElement;
    expect(after.disabled).toBe(true);
    expect(after.textContent).toContain('Add requested');
  });

  it('a locked-ON row whose category config cannot add offers NO add control', () => {
    // `product_help` and `session_task` are wholly grant:"code" — `true`
    // expands to nothing there, so an Add button would be a write boundary 400
    // dressed as an affordance. Silence is the honest render.
    const fixture = mount({
      mode: 'live',
      resolved: response({
        categories: {
          product_help: cat({
            state: 'on',
            settable: false,
            reason: 'granted by the runtime, not by config (persistent-session floor)',
            tools: ['read_product_guide'],
          }),
        },
      }),
    });
    expect(rowFor(fixture, 'Product Help')).not.toBeNull();
    expect((fixture.nativeElement as HTMLElement).querySelector('.tool-additions')).toBeNull();
  });

  it('a grant-blocked locked row offers no add control either', () => {
    // The client-side grant belt is a refusal in its own right. Offering to add
    // tools the PDP will 422 is the same dead end from the other side.
    const fixture = mount({
      mode: 'live',
      resolved: response({
        categories: {
          shell: cat({state: 'on', settable: false, reason: 'locked', tools: ['srw_cloud_status']}),
        },
      }),
      gatedCapabilities: {shell_tools: false},
    });
    expect((fixture.nativeElement as HTMLElement).querySelector('.tool-additions')).toBeNull();
  });

  it('an ordinary settable row gets no add control — it has a checkbox', () => {
    const fixture = mount({
      mode: 'live',
      resolved: response({categories: {shell: cat({state: 'on', tools: ['run_command']})}}),
    });
    expect((fixture.nativeElement as HTMLElement).querySelector('.tool-additions')).toBeNull();
  });

  it('a locked-and-empty category still renders blocked, with the reason', () => {
    const fixture = mount({
      resolved: response({
        categories: {
          knowledge: cat({
            state: 'unavailable',
            settable: false,
            reason: 'the merged config grants 10 tool(s) here and the agent bound none',
          }),
        },
      }),
    });
    const row = rowFor(fixture, 'Knowledge');
    expect(row.querySelector('input[type="checkbox"]')).toBeNull();
    expect(row.querySelector('.tool-state-blocked')).not.toBeNull();
    expect(row.querySelector('.tool-toggle-reason')?.textContent).toContain('bound none');
  });

  it('a surface that performs no read flies no banner at all', () => {
    // Job create and the expert editor wire no resolved read. An unconditional
    // banner made both permanently report the failure of a request nobody
    // made: "The resolved toolset could not be read".
    const fixture = mount({mode: 'job', resolved: null, readsResolvedToolset: false});
    expect((fixture.nativeElement as HTMLElement).querySelector('.toolset-provenance')).toBeNull();
    expect(rows(fixture).length).toBeGreaterThan(0);
  });

  it('a host-supplied enumeration makes shell tickable without a resolved read', () => {
    // Otherwise the tick emits `true` and the boundary 400s naming a rule the
    // form gives the user no way to satisfy.
    const fixture = mount({
      mode: 'job',
      resolved: null,
      readsResolvedToolset: false,
      enumerateOnly: {shell: ['cancel_command', 'run_command']},
    });
    const instance = fixture.componentInstance;
    instance.prefillFromConfig({tools: {shell: []}});
    fixture.detectChanges();

    const box = rowFor(fixture, 'Shell').querySelector(
      'input[type="checkbox"]',
    ) as HTMLInputElement;
    expect(box.checked).toBe(false);
    box.click();
    fixture.detectChanges();

    expect(instance.getOverrides()).toEqual({
      tools: {shell: {only: ['cancel_command', 'run_command']}},
    });
  });

  it('no rendered string is an unresolved transloco key', () => {
    // A missing key renders as the key, and AOT does not catch it. Every
    // category the endpoint can return is exercised here, including the ones
    // the twelve-row form never showed.
    const categories: Record<string, SessionToolCategory> = {};
    for (const key of [
      'research', 'browser_direct', 'citation', 'shell', 'communication', 'delegation',
      'canvas', 'job_control', 'job_inspection', 'orchestrator', 'agent_catalog', 'workflows', 'catalog_authoring',
      'knowledge', 'git',
      'workspace', 'core', 'session_task', 'product_help', 'evaluation', 'loop',
      'sql', 'mongodb', 'graph', 'webdav', 'email', 'repo', 'mcp', 'unclassified',
    ]) {
      categories[key] = cat({state: 'off'});
    }
    const fixture = mount({resolved: response({categories})});
    expect(rows(fixture)).toHaveLength(29);
    expect(text(fixture)).not.toContain('agentSettings.');
    expect(text(fixture)).not.toContain('grants.');
  });

  it('a category the cockpit has never heard of renders humanised, not as a raw key', () => {
    const fixture = mount({
      resolved: response({categories: {some_future_thing: cat({state: 'off'})}}),
    });
    const body = text(fixture);
    expect(body).toContain('Some Future Thing');
    expect(body).not.toContain('agentSettings.toolCategories.some_future_thing');
  });

  it('the grant gate renders UNAVAILABLE even when the server answered settable', () => {
    // The server's grant lookup fails OPEN on error, so the client-side gate
    // stays as a belt. It must reach the same third state, not a clickable box
    // that greys: a box the user can tick is a promise the PDP will refuse.
    const fixture = mount({
      resolved: response({categories: {shell: cat({state: 'off'})}}),
      gatedCapabilities: {shell_tools: false},
    });
    const row = rowFor(fixture, 'Shell');
    expect(row.classList.contains('disabled')).toBe(true);
    expect(row.classList.contains('unavailable')).toBe(true);
    expect(row.querySelector('input[type="checkbox"]')).toBeNull();
    expect(row.querySelector('.tool-toggle-reason')?.textContent).toBeTruthy();
    expect(row.querySelector('.tool-toggle-reason')?.textContent).not.toContain('grants.');
  });

  it('catalogue authoring reads as a WRITE capability, gated by its own grant', () => {
    // The whole point of splitting these six out of `agent_catalog`: the label
    // must say it writes, and the grant must gate it. If this row ever renders
    // as a tickable box for an ungranted author, they author an expert the PDP
    // refuses at save time — and the box was the promise that misled them.
    const fixture = mount({
      resolved: response({
        categories: {
          agent_catalog: cat({state: 'on', tools: ['list_experts']}),
          catalog_authoring: cat({state: 'off'}),
        },
      }),
      gatedCapabilities: {catalog_authoring: false},
    });

    const authoring = rowFor(fixture, 'Author Experts & Automations');
    expect(authoring.classList.contains('unavailable')).toBe(true);
    expect(authoring.querySelector('input[type="checkbox"]')).toBeNull();
    expect(authoring.querySelector('.tool-toggle-reason')?.textContent).toBeTruthy();

    // The read group next to it is untouched by the authoring grant.
    const reads = rowFor(fixture, 'Experts & Skills');
    expect(reads.classList.contains('unavailable')).toBe(false);
    expect(text(fixture)).not.toContain('agentSettings.');
  });

  it('catalogue authoring is tickable once the grant is held', () => {
    const fixture = mount({
      resolved: response({categories: {catalog_authoring: cat({state: 'off'})}}),
      gatedCapabilities: {catalog_authoring: true},
    });
    const row = rowFor(fixture, 'Author Experts & Automations');
    expect(row.classList.contains('unavailable')).toBe(false);
    expect(row.querySelector('input[type="checkbox"]')).not.toBeNull();
  });

  it('a grant-blocked category is never written, however the row is clicked', () => {
    const fixture = mount({
      resolved: response({categories: {shell: cat({state: 'off'}), canvas: cat({state: 'on'})}}),
      gatedCapabilities: {shell_tools: false},
    });
    fixture.componentInstance.toggleCategory('shell');
    fixture.detectChanges();
    expect(fixture.componentInstance.getOverrides()).toEqual({});
  });

  it('clicking a settable box repaints the row and emits the diff', () => {
    const fixture = mount({
      resolved: response({
        categories: {canvas: cat({state: 'on'}), shell: cat({state: 'off'})},
      }),
    });
    fixture.componentInstance.prefillFromResolved(fixture.componentInstance.resolved()!.categories!);
    fixture.detectChanges();

    const box = rowFor(fixture, 'Shell').querySelector(
      'input[type="checkbox"]',
    ) as HTMLInputElement;
    box.click();
    fixture.detectChanges();

    expect(
      (rowFor(fixture, 'Shell').querySelector('input[type="checkbox"]') as HTMLInputElement)
        .checked,
    ).toBe(true);
    // Shell refuses `true`, so the payload is the enumeration the server served.
    expect(fixture.componentInstance.getOverrides()).toEqual({
      tools: {shell: {only: ['cancel_command', 'run_command', 'shell_execute', 'shell_read']}},
    });
  });
});

describe('ToolsGroupComponent delegation roster', () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  const delegationOn = () => ({
    delegation: cat({state: 'on', tools: ['delegate_agent']}),
  });

  it('lists the roster the expert resolves, default flagged, next to the Delegation row', () => {
    const fixture = mount({
      mode: 'live',
      resolved: response({
        categories: delegationOn(),
        subagents: {
          default: 'explorer',
          roster: [
            {name: 'explorer', description: 'Read-only investigator.', ref: 'subagents/explorer'},
            {name: 'reader', description: null, ref: 'subagents/reader'},
          ],
        },
      }),
    });
    const entries = Array.from(
      (fixture.nativeElement as HTMLElement).querySelectorAll('.roster-entry'),
    ).map((el) => ({
      name: el.querySelector('.roster-name')?.firstChild?.textContent?.trim(),
      isDefault: el.querySelector('.roster-default') !== null,
    }));
    expect(entries).toEqual([
      {name: 'explorer', isDefault: true},
      {name: 'reader', isDefault: false},
    ]);
    expect(text(fixture)).toContain('(default)');
    expect(text(fixture)).toContain('Read-only investigator.');
    expect((fixture.nativeElement as HTMLElement).querySelector('.roster-empty')).toBeNull();
  });

  it('warns when the expert has NO roster — the tick would bind a tool that always errors', () => {
    // The seeded assistant shipped without a roster: a ticked Delegation
    // bound delegate_agent and every call returned "this expert has no
    // roster". Off or on, the warning is what the user needs before ticking.
    const fixture = mount({
      mode: 'live',
      resolved: response({
        categories: {delegation: cat({state: 'off'})},
        subagents: {default: null, roster: []},
      }),
    });
    const warning = (fixture.nativeElement as HTMLElement).querySelector('.roster-empty');
    expect(warning?.textContent).toContain('no subagent roster');
    expect(warning?.textContent).not.toContain('agentSettings.');
  });

  it('claims nothing when the server made no claim (older orchestrator, failed resolve)', () => {
    const fixture = mount({
      mode: 'live',
      resolved: response({categories: delegationOn()}),
    });
    const host = fixture.nativeElement as HTMLElement;
    expect(host.querySelector('.roster-list')).toBeNull();
    expect(host.querySelector('.roster-empty')).toBeNull();
  });
});

describe('ToolsGroupComponent session delegation cap', () => {
  // parallel_subagents.md §6.4 / D2: a session's own cap. The field says what
  // it does, shows an unset cap as inherited instead of as a typed value, and
  // refuses what the orchestrator would refuse.
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  const delegationOn = () => response({
    categories: {delegation: cat({state: 'on', tools: ['delegate_agent']})},
  });

  function capInput(fixture: {nativeElement: unknown}): HTMLInputElement {
    const input = (fixture.nativeElement as HTMLElement).querySelector<HTMLInputElement>(
      '.inline-params input[type="number"]',
    );
    if (!input) throw new Error(`no cap field; saw: ${text(fixture)}`);
    return input;
  }

  function type(
    fixture: {nativeElement: unknown; detectChanges: () => void},
    value: string,
  ): void {
    const input = capInput(fixture);
    input.value = value;
    input.dispatchEvent(new Event('input'));
    fixture.detectChanges();
  }

  it('unset: labelled in plain words, empty, and shown as inherited (default 6)', async () => {
    const fixture = mount({mode: 'live', delegationCapScope: 'session', resolved: delegationOn()});
    await fixture.whenStable();
    const input = capInput(fixture);
    expect(text(fixture)).toContain('Subagents at once');
    expect(text(fixture)).toContain('How many subagents run at the same time. More calls wait their turn.');
    expect(text(fixture)).toContain('Inherited (default 6)');
    expect(text(fixture)).not.toContain('Max concurrent');
    expect(input.value).toBe('');
    expect(input.getAttribute('placeholder')).toBe('6');
    expect(input.getAttribute('min')).toBe('1');
    expect(input.getAttribute('max')).toBe('20');
    expect(fixture.componentInstance.getOverrides()).toEqual({});
  });

  it('inherits the model family value when the config carries one', async () => {
    const fixture = mount({
      mode: 'session',
      delegationCapScope: 'session',
      resolved: delegationOn(),
      config: {delegation: {family_session_max_concurrent: 4}},
    });
    await fixture.whenStable();
    expect(text(fixture)).toContain('Inherited (default 4)');
    expect(capInput(fixture).getAttribute('placeholder')).toBe('4');
  });

  it('a set cap shows as its value, not as inherited, and is not re-written', async () => {
    const fixture = mount({
      mode: 'live',
      delegationCapScope: 'session',
      resolved: delegationOn(),
      config: {delegation: {session_max_concurrent: 3}},
    });
    await fixture.whenStable();
    expect(capInput(fixture).value).toBe('3');
    expect(text(fixture)).not.toContain('Inherited');
    expect(fixture.componentInstance.getOverrides()).toEqual({});
  });

  it('typing a value writes session_max_concurrent and clears the inherited note', async () => {
    const fixture = mount({mode: 'live', delegationCapScope: 'session', resolved: delegationOn()});
    await fixture.whenStable();
    type(fixture, '2');
    expect(fixture.componentInstance.getOverrides()).toEqual({
      delegation: {session_max_concurrent: 2},
    });
    expect(text(fixture)).not.toContain('Inherited');
  });

  it('an out-of-range value shows the rule and is not written', async () => {
    const fixture = mount({mode: 'live', delegationCapScope: 'session', resolved: delegationOn()});
    await fixture.whenStable();
    type(fixture, '25');
    const alert = (fixture.nativeElement as HTMLElement).querySelector('[role="alert"]');
    expect(alert?.textContent).toContain('Enter a whole number from 1 to 20.');
    expect(capInput(fixture).getAttribute('aria-invalid')).toBe('true');
    expect(fixture.componentInstance.getOverrides()).toEqual({});

    type(fixture, '20');
    expect((fixture.nativeElement as HTMLElement).querySelector('[role="alert"]')).toBeNull();
    expect(fixture.componentInstance.getOverrides()).toEqual({
      delegation: {session_max_concurrent: 20},
    });
  });

  it('the live pane offers no reset (pin-only); the create form resets to inherited', async () => {
    const live = mount({mode: 'live', delegationCapScope: 'session', resolved: delegationOn()});
    await live.whenStable();
    type(live, '2');
    expect((live.nativeElement as HTMLElement).querySelector('.inline-params .reset-btn')).toBeNull();
    TestBed.resetTestingModule();

    const create = mount({mode: 'session', delegationCapScope: 'session', resolved: delegationOn()});
    await create.whenStable();
    type(create, '2');
    const reset = (create.nativeElement as HTMLElement).querySelector<HTMLButtonElement>(
      '.inline-params .reset-btn',
    );
    expect(reset).not.toBeNull();
    reset!.click();
    create.detectChanges();
    expect(create.componentInstance.getOverrides()).toEqual({});
    expect(text(create)).toContain('Inherited (default 6)');
  });

  it('switching Delegation off drops a hidden invalid cap from validity', async () => {
    // The field and its error render only while the row is on. An invalid
    // value left behind a switched-off row must not keep Create disabled with
    // nothing on screen saying why.
    const fixture = mount({mode: 'session', delegationCapScope: 'session', resolved: delegationOn()});
    await fixture.whenStable();
    type(fixture, '25');
    expect(fixture.componentInstance.delegationCapValid()).toBe(false);

    const box = (fixture.nativeElement as HTMLElement).querySelector<HTMLInputElement>(
      '.tool-toggle input[type="checkbox"]',
    )!;
    box.dispatchEvent(new Event('change'));
    fixture.detectChanges();
    expect((fixture.nativeElement as HTMLElement).querySelector('.inline-params')).toBeNull();
    expect(fixture.componentInstance.delegationCapValid()).toBe(true);
    expect(fixture.componentInstance.getOverrides()).toEqual({
      tools: {delegation: []},
      delegation: {enabled: false},
    });

    // Back on: the value and its error return together.
    box.dispatchEvent(new Event('change'));
    fixture.detectChanges();
    expect(fixture.componentInstance.delegationCapValid()).toBe(false);
    expect((fixture.nativeElement as HTMLElement).querySelector('[role="alert"]')).not.toBeNull();
  });

  it('an emptied field over a held cap shows the held value, not the default', async () => {
    // Typing 5, the socket confirming 5 into the config, then emptying the
    // field: the bound value stays 5, so ngModel leaves the field empty. Its
    // placeholder is then all it shows, and it must be what the session holds.
    const config = signal<Record<string, unknown>>({});
    const fixture = mount({
      mode: 'live',
      delegationCapScope: 'session',
      resolved: delegationOn(),
      config,
    });
    await fixture.whenStable();
    type(fixture, '5');
    config.set({delegation: {session_max_concurrent: 5}});
    fixture.detectChanges();
    type(fixture, '');
    expect(capInput(fixture).getAttribute('placeholder')).toBe('5');
    expect(text(fixture)).not.toContain('Inherited');
    expect(fixture.componentInstance.getOverrides()).toEqual({});
  });

  it('a job keeps the worker field: max_concurrent, default 4, no inherited note', async () => {
    const fixture = mount({mode: 'job', resolved: delegationOn()});
    await fixture.whenStable();
    expect(text(fixture)).toContain('Max concurrent');
    expect(text(fixture)).not.toContain('Subagents at once');
    expect(capInput(fixture).value).toBe('4');
    type(fixture, '2');
    expect(fixture.componentInstance.getOverrides()).toEqual({delegation: {max_concurrent: 2}});
  });
});

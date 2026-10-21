import { CUSTOM_ELEMENTS_SCHEMA, Pipe, PipeTransform, ɵresolveComponentResources } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { TranslocoService } from '@jsverse/transloco';
import { Observable, of } from 'rxjs';
import { afterEach, beforeAll, beforeEach, describe, it, expect, vi } from 'vitest';
import { Agent, AgentStatus } from '../../core/models/api.model';
import { ApiService } from '../../core/services/api.service';
import { AgentListComponent } from './agent-list.component';

/**
 * Unit tests for AgentListComponent utility functions, mirrored from the
 * component, and (at the end) for how the panel reports a list it could not
 * load.
 */

// Extract utility functions from component for testing
function getStatusIcon(status: AgentStatus): string {
  const icons: Record<AgentStatus, string> = {
    booting: '\u23F3',
    ready: '\u2705',
    working: '\u26A1',
    completed: '\u2714',
    failed: '\u274C',
    offline: '\u26AA',
  };
  return icons[status] || '\u2753';
}

function formatTimestamp(timestamp: string): string {
  const date = new Date(timestamp);
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffSec = Math.floor(diffMs / 1000);

  if (diffSec < 60) {
    return `${diffSec}s ago`;
  }
  if (diffSec < 3600) {
    return `${Math.floor(diffSec / 60)}m ago`;
  }
  if (diffSec < 86400) {
    return `${Math.floor(diffSec / 3600)}h ago`;
  }
  return date.toLocaleDateString();
}

function truncatePrompt(prompt: string | undefined, maxLength: number = 60): string {
  if (!prompt) {
    return '';
  }
  if (prompt.length <= maxLength) {
    return prompt;
  }
  return prompt.slice(0, maxLength) + '...';
}

function formatDate(dateString: string): string {
  const date = new Date(dateString);
  return date.toLocaleDateString('en-GB', {
    day: '2-digit',
    month: 'short',
    hour: '2-digit',
    minute: '2-digit',
  });
}

// Mirror of AgentListComponent.auxTooltip with the translated prefix injected
// (aux Phase 2 badge tooltip). Builds a human string from the compact
// metadata.aux summary the agent reports on its heartbeat.
function auxTooltip(metadata: Record<string, unknown> | undefined, prefix: string): string {
  const aux = (metadata?.['aux'] ?? null) as {
    model?: string;
    consecutive_failures?: number;
    failing_tasks?: Record<string, {last_error_type?: string}>;
  } | null;
  if (!aux) {
    return prefix;
  }

  const parts: string[] = [];
  if (aux.model) {
    parts.push(aux.model);
  }
  if (aux.consecutive_failures) {
    parts.push(`${aux.consecutive_failures} consecutive failures`);
  }

  const taskDetail = Object.entries(aux.failing_tasks ?? {})
    .map(([name, t]) => `${name}: ${t?.last_error_type ?? 'error'}`)
    .join(', ');

  let detail = parts.join(' — ');
  if (taskDetail) {
    detail = detail ? `${detail} (${taskDetail})` : taskDetail;
  }
  return detail ? `${prefix}: ${detail}` : prefix;
}

describe('AgentListComponent utilities', () => {
  describe('getStatusIcon', () => {
    it('should return correct icon for ready status', () => {
      expect(getStatusIcon('ready')).toBe('\u2705');
    });

    it('should return correct icon for working status', () => {
      expect(getStatusIcon('working')).toBe('\u26A1');
    });

    it('should return correct icon for booting status', () => {
      expect(getStatusIcon('booting')).toBe('\u23F3');
    });

    it('should return correct icon for completed status', () => {
      expect(getStatusIcon('completed')).toBe('\u2714');
    });

    it('should return correct icon for failed status', () => {
      expect(getStatusIcon('failed')).toBe('\u274C');
    });

    it('should return correct icon for offline status', () => {
      expect(getStatusIcon('offline')).toBe('\u26AA');
    });
  });

  describe('formatTimestamp', () => {
    it('should format recent timestamps as seconds ago', () => {
      const now = new Date();
      const timestamp = new Date(now.getTime() - 30000).toISOString(); // 30 seconds ago

      const result = formatTimestamp(timestamp);

      expect(result).toMatch(/\d+s ago/);
    });

    it('should format timestamps from minutes ago', () => {
      const now = new Date();
      const timestamp = new Date(now.getTime() - 300000).toISOString(); // 5 minutes ago

      const result = formatTimestamp(timestamp);

      expect(result).toMatch(/\d+m ago/);
    });

    it('should format timestamps from hours ago', () => {
      const now = new Date();
      const timestamp = new Date(now.getTime() - 7200000).toISOString(); // 2 hours ago

      const result = formatTimestamp(timestamp);

      expect(result).toMatch(/\d+h ago/);
    });

    it('should format old timestamps as date', () => {
      const oldDate = new Date();
      oldDate.setDate(oldDate.getDate() - 5);
      const timestamp = oldDate.toISOString();

      const result = formatTimestamp(timestamp);

      // Should be a date format, not "X ago"
      expect(result).not.toContain('ago');
    });
  });

  describe('truncatePrompt', () => {
    it('should not truncate short prompts', () => {
      const shortPrompt = 'Short prompt';

      const result = truncatePrompt(shortPrompt);

      expect(result).toBe(shortPrompt);
    });

    it('should truncate long prompts with ellipsis', () => {
      const longPrompt = 'A'.repeat(100);

      const result = truncatePrompt(longPrompt, 60);

      expect(result.length).toBe(63); // 60 chars + '...'
      expect(result.endsWith('...')).toBe(true);
    });

    it('should not truncate prompts exactly at max length', () => {
      const exactPrompt = 'A'.repeat(60);

      const result = truncatePrompt(exactPrompt, 60);

      expect(result).toBe(exactPrompt);
      expect(result.length).toBe(60);
    });

    it('should use default max length of 60', () => {
      const prompt = 'A'.repeat(70);

      const result = truncatePrompt(prompt);

      expect(result.length).toBe(63); // 60 + '...'
    });
  });

  describe('formatDate', () => {
    it('should format date with day, month, hour and minute', () => {
      const dateString = '2024-03-15T10:30:00Z';

      const result = formatDate(dateString);

      // Should contain day and month
      expect(result).toMatch(/\d{2}/);
      expect(result).toMatch(/Mar/);
    });

    it('should format current date', () => {
      const dateString = new Date().toISOString();

      const result = formatDate(dateString);

      expect(result).toBeTruthy();
      expect(result.length).toBeGreaterThan(0);
    });
  });

  describe('auxTooltip', () => {
    const PREFIX = 'Auxiliary model degraded';

    it('returns the bare prefix when no metadata', () => {
      expect(auxTooltip(undefined, PREFIX)).toBe(PREFIX);
    });

    it('returns the bare prefix when metadata has no aux block', () => {
      expect(auxTooltip({memory_mb: 123}, PREFIX)).toBe(PREFIX);
    });

    it('includes model, failure count and failing task detail', () => {
      const metadata = {
        aux: {
          degraded: true,
          consecutive_failures: 7,
          model: 'gemma-4-moe',
          failing_tasks: {
            memory_extraction: {last_error_type: 'ConnectionError'},
          },
        },
      };

      const result = auxTooltip(metadata, PREFIX);

      expect(result).toBe(
        'Auxiliary model degraded: gemma-4-moe — 7 consecutive failures (memory_extraction: ConnectionError)',
      );
    });

    it('falls back to the prefix when the aux block carries no detail', () => {
      // degraded flag present but no model/count/tasks → nothing to append.
      expect(auxTooltip({aux: {degraded: true}}, PREFIX)).toBe(PREFIX);
    });
  });
});


/** Keys straight through — the template is asserted on structure, not copy. */
@Pipe({ name: 'transloco', standalone: true })
class TranslocoStubPipe implements PipeTransform {
  transform(key: string): string {
    return key;
  }
}

describe('AgentListComponent — loading the fleet', () => {
  const agent = {
    id: 'agent-1',
    status: 'offline',
    config_name: 'assistant',
    hostname: 'srw-agent-1',
    current_job_id: null,
    last_heartbeat: new Date().toISOString(),
  } as unknown as Agent;
  let next: Observable<Agent[] | null>;
  const api = { getAgents: vi.fn(() => next), getJobs: vi.fn(() => of([])) };
  const transloco = { translate: vi.fn((key: string) => key) };

  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  beforeEach(() => {
    api.getAgents.mockClear();
    TestBed.configureTestingModule({
      imports: [AgentListComponent],
      providers: [
        { provide: ApiService, useValue: api },
        { provide: TranslocoService, useValue: transloco },
      ],
    });
    TestBed.overrideComponent(AgentListComponent, {
      set: { imports: [TranslocoStubPipe], schemas: [CUSTOM_ELEMENTS_SCHEMA] },
    });
  });

  afterEach(() => TestBed.resetTestingModule());

  function render() {
    const fixture = TestBed.createComponent(AgentListComponent);
    fixture.detectChanges();
    return fixture;
  }

  it('says the list could not be loaded instead of showing an empty fleet', () => {
    next = of(null);
    const fixture = render();
    const text = (fixture.nativeElement as HTMLElement).textContent ?? '';

    expect(text).toContain('agentList.loadFailed');
    expect(text).not.toContain('agentList.empty');
    expect(fixture.componentInstance.statusIsError()).toBe(true);
    fixture.destroy();
  });

  it('still shows the empty state when the fleet really is empty', () => {
    next = of([]);
    const fixture = render();
    const text = (fixture.nativeElement as HTMLElement).textContent ?? '';

    expect(text).toContain('agentList.empty');
    expect(text).not.toContain('agentList.loadFailed');
    fixture.destroy();
  });

  it('keeps the rows it has when a later refresh fails', () => {
    next = of([agent]);
    const fixture = render();
    next = of(null);
    fixture.componentInstance.refresh();
    fixture.detectChanges();

    expect(fixture.componentInstance.agents()).toEqual([agent]);
    expect(fixture.componentInstance.loadFailed()).toBe(true);
    expect((fixture.nativeElement as HTMLElement).querySelector('.agent-table')).not.toBeNull();
    fixture.destroy();
  });
});

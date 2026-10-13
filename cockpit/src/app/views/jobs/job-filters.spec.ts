import {describe, expect, it} from 'vitest';
import {
  DEFAULT_JOB_FILTERS,
  HUMAN_ORIGINS,
  DEFAULT_PAGE_SIZE,
  JobFilterToken,
  JobListFilters,
  KNOWN_JOB_ORIGINS,
  KNOWN_JOB_STATUSES,
  MAX_PROJECT_FILTERS,
  MAX_SEARCH_LENGTH,
  PAGE_SIZE_OPTIONS,
  activeFilterTokens,
  clearJobFilters,
  isDefaultJobFilters,
  jobFiltersToApiQuery,
  jobFiltersToQueryParams,
  parseJobFilters,
  removeFilterToken,
  setPageSize,
} from './job-filters';

const PROJECT_A = '11111111-2222-4333-8444-555555555555';
const PROJECT_B = 'aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee';

const NO_NAMES: ReadonlyMap<string, string> = new Map();

function filters(overrides: Partial<JobListFilters> = {}): JobListFilters {
  // Derived from DEFAULT_JOB_FILTERS rather than restated: a hardcoded copy
  // silently stops being "the defaults" the moment one of them changes, which
  // is what happened when `origin` gained a non-empty default.
  return {
    ...DEFAULT_JOB_FILTERS,
    status: [...DEFAULT_JOB_FILTERS.status],
    origin: [...DEFAULT_JOB_FILTERS.origin],
    projectIds: [...DEFAULT_JOB_FILTERS.projectIds],
    ...overrides,
  };
}

function tokenOfKind(list: JobFilterToken[], kind: JobFilterToken['kind']): JobFilterToken {
  const found = list.find((token) => token.kind === kind);
  if (!found) throw new Error(`no ${kind} token in [${list.map((t) => t.id).join(', ')}]`);
  return found;
}

describe('constants', () => {
  it('exposes the vocabularies the server accepts', () => {
    expect(KNOWN_JOB_STATUSES).toContain('pending_review');
    expect(KNOWN_JOB_STATUSES).toContain('blocked_undelivered');
    expect(KNOWN_JOB_STATUSES).toHaveLength(11);
    expect(KNOWN_JOB_ORIGINS).toEqual([
      'user',
      'session',
      'automation',
      'telephony',
      'loop',
      'officer',
      'subjob',
      'lifecycle',
      'bench',
    ]);
    expect(PAGE_SIZE_OPTIONS).toEqual([25, 50, 100]);
    expect(DEFAULT_PAGE_SIZE).toBe(25);
  });

  it('has an all-empty default that reports itself as default', () => {
    expect(isDefaultJobFilters(DEFAULT_JOB_FILTERS)).toBe(true);
    expect(DEFAULT_JOB_FILTERS.page).toBe(1);
    expect(DEFAULT_JOB_FILTERS.hasProject).toBeNull();
    expect(DEFAULT_JOB_FILTERS.includeArchivedProjects).toBe(false);
  });

  it('freezes the default so a stray push cannot poison later parses', () => {
    expect(Object.isFrozen(DEFAULT_JOB_FILTERS)).toBe(true);
    expect(Object.isFrozen(DEFAULT_JOB_FILTERS.status)).toBe(true);
  });
});

describe('round-tripping', () => {
  it('survives a fully-populated filter set through query params and back', () => {
    const original = filters({
      status: ['failed', 'paused'],
      origin: ['loop', 'officer'],
      projectIds: [PROJECT_A, PROJECT_B],
      hasProject: null,
      includeArchivedProjects: true,
      search: 'migrate schema',
      page: 4,
      pageSize: 100,
      asOf: '2026-08-21T09:15:00.123456Z',
    });

    expect(parseJobFilters(jobFiltersToQueryParams(original))).toEqual(original);
  });

  it('round-trips the no-project bucket through the `none` alias', () => {
    const original = filters({hasProject: false, status: ['completed']});
    const params = jobFiltersToQueryParams(original);

    expect(params['project_id']).toEqual(['none']);
    expect(params['has_project']).toBeNull();
    expect(parseJobFilters(params)).toEqual(original);
  });

  it('round-trips hasProject=true via the has_project flag', () => {
    const original = filters({hasProject: true});
    const params = jobFiltersToQueryParams(original);

    expect(params['has_project']).toBe('true');
    expect(params['project_id']).toBeNull();
    expect(parseJobFilters(params)).toEqual(original);
  });

  it('treats an empty bag as the defaults', () => {
    expect(parseJobFilters({})).toEqual(DEFAULT_JOB_FILTERS);
  });
});

describe('jobFiltersToQueryParams', () => {
  it('nulls every param when nothing is filtered, so the URL stays bare', () => {
    const params = jobFiltersToQueryParams(DEFAULT_JOB_FILTERS);

    expect(Object.values(params).every((value) => value === null)).toBe(true);
    // Keys are still present: Router only *removes* a param on an explicit
    // null, so omitting the key would strand a stale value under 'merge'.
    expect(Object.keys(params)).toContain('status');
    expect(Object.keys(params)).toContain('page');
  });

  it('omits page and page_size while they sit at their defaults', () => {
    const params = jobFiltersToQueryParams(filters({page: 1, pageSize: DEFAULT_PAGE_SIZE}));
    expect(params['page']).toBeNull();
    expect(params['page_size']).toBeNull();

    const moved = jobFiltersToQueryParams(filters({page: 3, pageSize: 50}));
    expect(moved['page']).toBe('3');
    expect(moved['page_size']).toBe('50');
  });

  it('emits repeated keys for multi-values', () => {
    const params = jobFiltersToQueryParams(filters({status: ['failed', 'paused']}));
    expect(params['status']).toEqual(['failed', 'paused']);
  });

  it('drops a whitespace-only search', () => {
    expect(jobFiltersToQueryParams(filters({search: '   '}))['search']).toBeNull();
  });
});

describe('parseJobFilters accepts both ParamMap shapes', () => {
  it('reads a single string and a string[] for repeatable keys', () => {
    expect(parseJobFilters({status: 'failed'}).status).toEqual(['failed']);
    expect(parseJobFilters({status: ['failed', 'paused']}).status).toEqual(['failed', 'paused']);
    expect(parseJobFilters({project_id: PROJECT_A}).projectIds).toEqual([PROJECT_A]);
  });

  it('takes the first value for a single-valued key, as ParamMap.get would', () => {
    expect(parseJobFilters({page: ['2', '9']}).page).toBe(2);
  });

  it('treats null and undefined values as absent', () => {
    expect(parseJobFilters({status: null, page: undefined, search: null})).toEqual(
      DEFAULT_JOB_FILTERS,
    );
  });
});

describe('parseJobFilters discards invalid values (rule 3)', () => {
  it('drops an unknown status and keeps the rest', () => {
    expect(parseJobFilters({status: ['failed', 'exploded', 'paused']}).status).toEqual([
      'failed',
      'paused',
    ]);
  });

  it('drops an unknown origin and keeps the rest', () => {
    expect(parseJobFilters({origin: ['loop', 'telepathy', 'bench']}).origin).toEqual([
      'loop',
      'bench',
    ]);
  });

  it('falls back to page 1 for a non-numeric page', () => {
    expect(parseJobFilters({page: 'two'}).page).toBe(1);
    expect(parseJobFilters({page: '2.5'}).page).toBe(1);
    expect(parseJobFilters({page: ''}).page).toBe(1);
  });

  it('falls back to page 1 for a page below 1', () => {
    expect(parseJobFilters({page: '0'}).page).toBe(1);
    expect(parseJobFilters({page: '-3'}).page).toBe(1);
  });

  it('falls back to the default for a pageSize outside the options', () => {
    expect(parseJobFilters({page_size: '37'}).pageSize).toBe(DEFAULT_PAGE_SIZE);
    expect(parseJobFilters({page_size: 'lots'}).pageSize).toBe(DEFAULT_PAGE_SIZE);
    expect(parseJobFilters({page_size: '100'}).pageSize).toBe(100);
  });

  it('drops a malformed uuid and keeps the valid ones', () => {
    const parsed = parseJobFilters({project_id: [PROJECT_A, 'not-a-uuid', PROJECT_B]});
    expect(parsed.projectIds).toEqual([PROJECT_A, PROJECT_B]);
  });

  it('drops a malformed as_of watermark', () => {
    expect(parseJobFilters({as_of: 'yesterday'}).asOf).toBeNull();
    // Shaped like ISO but not a real instant.
    expect(parseJobFilters({as_of: '2026-13-45T00:00:00Z'}).asOf).toBeNull();
    // Naive (no Zulu) is not the wire format the API echoes.
    expect(parseJobFilters({as_of: '2026-08-21T09:15:00'}).asOf).toBeNull();
    expect(parseJobFilters({as_of: '2026-08-21T09:15:00Z'}).asOf).toBe('2026-08-21T09:15:00Z');
  });

  it('never throws on hostile input', () => {
    expect(() =>
      parseJobFilters({
        status: ['', ' ', '../etc/passwd'],
        page: 'NaN',
        page_size: '-0',
        project_id: ['%%%'],
        as_of: '<script>',
        has_project: 'maybe',
      }),
    ).not.toThrow();
    expect(parseJobFilters({has_project: 'maybe'}).hasProject).toBeNull();
  });

  it('dedupes and normalises uuid case', () => {
    const parsed = parseJobFilters({project_id: [PROJECT_A, PROJECT_A.toUpperCase()]});
    expect(parsed.projectIds).toEqual([PROJECT_A]);
  });

  it('trims the search and clamps it to the server max length', () => {
    expect(parseJobFilters({search: '  migrate  '}).search).toBe('migrate');
    expect(parseJobFilters({search: 'x'.repeat(500)}).search).toHaveLength(MAX_SEARCH_LENGTH);
  });

  it('caps the project filter list rather than sending a 422-length URL', () => {
    const many = Array.from(
      {length: MAX_PROJECT_FILTERS + 5},
      (_, i) => `${String(i).padStart(8, '0')}-2222-4333-8444-555555555555`,
    );
    expect(parseJobFilters({project_id: many}).projectIds).toHaveLength(MAX_PROJECT_FILTERS);
  });
});

describe('project mutual exclusion (rule 4)', () => {
  it('keeps projectIds and drops the bucket when `none` arrives with real ids', () => {
    const parsed = parseJobFilters({project_id: ['none', PROJECT_A]});
    expect(parsed.projectIds).toEqual([PROJECT_A]);
    expect(parsed.hasProject).toBeNull();
  });

  it('keeps projectIds and drops has_project=false when both arrive', () => {
    const parsed = parseJobFilters({project_id: PROJECT_A, has_project: 'false'});
    expect(parsed.projectIds).toEqual([PROJECT_A]);
    expect(parsed.hasProject).toBeNull();
  });

  it('keeps the bucket when `none` arrives with only a malformed id', () => {
    const parsed = parseJobFilters({project_id: ['none', 'not-a-uuid']});
    expect(parsed.projectIds).toEqual([]);
    expect(parsed.hasProject).toBe(false);
  });

  it('leaves has_project=true alongside ids alone (redundant, not contradictory)', () => {
    const parsed = parseJobFilters({project_id: PROJECT_A, has_project: 'true'});
    expect(parsed.projectIds).toEqual([PROJECT_A]);
    expect(parsed.hasProject).toBe(true);
  });

  it('never serialises both, even from a hand-built violating state', () => {
    const params = jobFiltersToQueryParams(filters({projectIds: [PROJECT_A], hasProject: false}));
    expect(params['project_id']).toEqual([PROJECT_A]);
    expect(params['project_id']).not.toContain('none');
    expect(params['has_project']).toBeNull();
  });

  it('never sends both to the API either', () => {
    const query = jobFiltersToApiQuery(filters({projectIds: [PROJECT_A], hasProject: false}));
    expect(query['project_id']).toEqual([PROJECT_A]);
    expect(query['has_project']).toBeUndefined();
  });
});

describe('jobFiltersToApiQuery (rule 9)', () => {
  it('maps to the REST shape', () => {
    const query = jobFiltersToApiQuery(
      filters({
        status: ['failed'],
        origin: ['loop'],
        projectIds: [PROJECT_A],
        includeArchivedProjects: true,
        search: 'schema',
        page: 3,
        pageSize: 50,
        asOf: '2026-08-21T09:15:00Z',
      }),
    );

    expect(query).toEqual({
      status: ['failed'],
      origin: ['loop'],
      project_id: [PROJECT_A],
      include_archived_projects: true,
      search: 'schema',
      limit: 50,
      offset: 100,
      as_of: '2026-08-21T09:15:00Z',
      include_total: false,
    });
  });

  it('omits everything at its default but always sends limit', () => {
    // limit is NOT omittable: the server default is 100, ours is 25.
    // origin is not omittable either, and for the same kind of reason — the
    // "hide system-created work" default is the cockpit's, not the API's, so
    // it has to be sent explicitly on every request.
    expect(jobFiltersToApiQuery(DEFAULT_JOB_FILTERS)).toEqual({
      limit: DEFAULT_PAGE_SIZE,
      origin: [...HUMAN_ORIGINS],
    });
  });

  it('sends the `none` literal for the no-project bucket', () => {
    expect(jobFiltersToApiQuery(filters({hasProject: false}))['project_id']).toEqual(['none']);
  });

  it('keeps include_total off past page 1 and absent on page 1', () => {
    expect(jobFiltersToApiQuery(filters({page: 1}))['include_total']).toBeUndefined();
    expect(jobFiltersToApiQuery(filters({page: 2}))['include_total']).toBe(false);
    expect(jobFiltersToApiQuery(filters({page: 7}))['include_total']).toBe(false);
  });

  it('computes offset from the 1-based page', () => {
    expect(jobFiltersToApiQuery(filters({page: 1, pageSize: 25}))['offset']).toBeUndefined();
    expect(jobFiltersToApiQuery(filters({page: 2, pageSize: 25}))['offset']).toBe(25);
    expect(jobFiltersToApiQuery(filters({page: 4, pageSize: 100}))['offset']).toBe(300);
  });
});

describe('activeFilterTokens', () => {
  it('emits the archived token while archived projects are hidden (rule 6)', () => {
    const tokens = activeFilterTokens(filters({includeArchivedProjects: false}), NO_NAMES);
    const archived = tokenOfKind(tokens, 'archived');
    expect(archived.labelKey).toBe('jobs.filter.token.archived');
    expect(archived.id).toBe('archived');
  });

  it('drops the archived token once archived projects are shown', () => {
    const tokens = activeFilterTokens(filters({includeArchivedProjects: true}), NO_NAMES);
    expect(tokens.some((token) => token.kind === 'archived')).toBe(false);
  });

  it('is never empty for default filters, because the defaults hide rows', () => {
    // Two things are hidden out of the box — archived projects and
    // system-created work — and each owes the user a visible, removable token.
    const tokens = activeFilterTokens(DEFAULT_JOB_FILTERS, NO_NAMES);
    expect(tokens.map((token) => token.id).sort()).toEqual(['archived', 'systemHidden']);
  });

  it('collapses the default origin pair into one token, not two', () => {
    const tokens = activeFilterTokens(DEFAULT_JOB_FILTERS, NO_NAMES);
    expect(tokens.filter((token) => token.kind === 'origin')).toHaveLength(0);
  });

  it('falls back to per-origin tokens once the selection is not the default', () => {
    const tokens = activeFilterTokens(filters({origin: ['loop', 'bench']}), NO_NAMES);
    expect(tokens.filter((token) => token.kind === 'origin').map((t) => t.value)).toEqual([
      'loop',
      'bench',
    ]);
  });

  it('emits one removable token per active value with stable unique ids', () => {
    const tokens = activeFilterTokens(
      filters({
        status: ['failed', 'paused'],
        origin: ['loop'],
        projectIds: [PROJECT_A],
        search: 'schema',
        includeArchivedProjects: true,
      }),
      NO_NAMES,
    );

    expect(tokens.map((token) => token.id)).toEqual([
      'status:failed',
      'status:paused',
      'origin:loop',
      `project:${PROJECT_A}`,
      'search',
    ]);
    expect(new Set(tokens.map((token) => token.id)).size).toBe(tokens.length);
    expect(tokens.every((token) => token.labelKey.startsWith('jobs.filter.token.'))).toBe(true);
  });

  it('emits a noProject token for the bucket', () => {
    const tokens = activeFilterTokens(filters({hasProject: false}), NO_NAMES);
    expect(tokenOfKind(tokens, 'noProject').labelKey).toBe('jobs.filter.token.noProject');
  });

  it('emits no project token for hasProject=true (it is not a removable chip)', () => {
    const tokens = activeFilterTokens(filters({hasProject: true}), NO_NAMES);
    expect(tokens.some((token) => token.kind === 'noProject')).toBe(false);
  });

  it('uses the display name when the map has one', () => {
    const names = new Map([[PROJECT_A, 'Schema Migration']]);
    const token = tokenOfKind(activeFilterTokens(filters({projectIds: [PROJECT_A]}), names), 'project');
    expect(token.labelParams?.['name']).toBe('Schema Migration');
    expect(token.value).toBe(PROJECT_A);
  });

  it('falls back to a short id, never a full-length uuid (rule 7)', () => {
    const token = tokenOfKind(
      activeFilterTokens(filters({projectIds: [PROJECT_A]}), NO_NAMES),
      'project',
    );
    expect(token.labelParams?.['name']).toBe('11111111');
    expect(token.labelParams?.['name']).not.toBe(PROJECT_A);
  });

  it('falls back to the short id when the name is blank', () => {
    const names = new Map([[PROJECT_A, '   ']]);
    const token = tokenOfKind(activeFilterTokens(filters({projectIds: [PROJECT_A]}), names), 'project');
    expect(token.labelParams?.['name']).toBe('11111111');
  });

  it('does not emit a token for a whitespace-only search', () => {
    // Asserts the absence of a *search* token rather than a total count: the
    // count moves whenever a filter gains a hiding default, and that has
    // nothing to do with what this test is about.
    const tokens = activeFilterTokens(
      filters({search: '  ', includeArchivedProjects: true}),
      NO_NAMES,
    );
    expect(tokens.filter((token) => token.kind === 'search')).toHaveLength(0);
  });
});

describe('removeFilterToken', () => {
  const populated = filters({
    status: ['failed', 'paused'],
    origin: ['loop', 'bench'],
    projectIds: [PROJECT_A, PROJECT_B],
    search: 'schema',
    page: 5,
    pageSize: 50,
    asOf: '2026-08-21T09:15:00Z',
  });

  it('removes only the named status', () => {
    const tokens = activeFilterTokens(populated, NO_NAMES);
    const token = tokens.find((t) => t.id === 'status:failed')!;
    expect(removeFilterToken(populated, token).status).toEqual(['paused']);
  });

  it('removes only the named origin', () => {
    const tokens = activeFilterTokens(populated, NO_NAMES);
    const token = tokens.find((t) => t.id === 'origin:loop')!;
    expect(removeFilterToken(populated, token).origin).toEqual(['bench']);
  });

  it('removes only the named project', () => {
    const tokens = activeFilterTokens(populated, NO_NAMES);
    const token = tokens.find((t) => t.id === `project:${PROJECT_A}`)!;
    expect(removeFilterToken(populated, token).projectIds).toEqual([PROJECT_B]);
  });

  it('clears the search', () => {
    const token = tokenOfKind(activeFilterTokens(populated, NO_NAMES), 'search');
    expect(removeFilterToken(populated, token).search).toBe('');
  });

  it('clears the no-project bucket', () => {
    const state = filters({hasProject: false});
    const token = tokenOfKind(activeFilterTokens(state, NO_NAMES), 'noProject');
    expect(removeFilterToken(state, token).hasProject).toBeNull();
  });

  it('reveals archived projects rather than hiding them further (rule 6)', () => {
    const token = tokenOfKind(activeFilterTokens(populated, NO_NAMES), 'archived');
    expect(removeFilterToken(populated, token).includeArchivedProjects).toBe(true);
  });

  it('resets the page for every token kind (rule 8)', () => {
    const state = filters({
      status: ['failed'],
      origin: ['loop'],
      projectIds: [PROJECT_A],
      search: 'schema',
      page: 9,
    });
    const kinds = activeFilterTokens(state, NO_NAMES).map((token) => token.kind);
    // Every kind except noProject, which cannot coexist with projectIds.
    expect(kinds).toEqual(['status', 'origin', 'project', 'search', 'archived']);

    for (const token of activeFilterTokens(state, NO_NAMES)) {
      expect(removeFilterToken(state, token).page).toBe(1);
    }
    const bucket = filters({hasProject: false, page: 9});
    expect(
      removeFilterToken(bucket, tokenOfKind(activeFilterTokens(bucket, NO_NAMES), 'noProject')).page,
    ).toBe(1);
  });

  it('preserves pageSize and the asOf watermark', () => {
    const token = tokenOfKind(activeFilterTokens(populated, NO_NAMES), 'search');
    const next = removeFilterToken(populated, token);
    expect(next.pageSize).toBe(50);
    expect(next.asOf).toBe('2026-08-21T09:15:00Z');
  });

  it('does not mutate the input', () => {
    const token = tokens0();
    const before = JSON.stringify(populated);
    removeFilterToken(populated, token);
    expect(JSON.stringify(populated)).toBe(before);
  });

  function tokens0(): JobFilterToken {
    return activeFilterTokens(populated, NO_NAMES)[0]!;
  }
});

describe('clearJobFilters', () => {
  const populated = filters({
    status: ['failed'],
    origin: ['loop'],
    projectIds: [PROJECT_A],
    hasProject: null,
    includeArchivedProjects: true,
    search: 'schema',
    page: 6,
    pageSize: 100,
    asOf: '2026-08-21T09:15:00Z',
  });

  it('resets everything except pageSize, including asOf and page (rule 8)', () => {
    const cleared = clearJobFilters(populated);

    expect(cleared).toEqual(filters({pageSize: 100}));
    expect(cleared.pageSize).toBe(100);
    expect(cleared.page).toBe(1);
    expect(cleared.asOf).toBeNull();
    expect(cleared.includeArchivedProjects).toBe(false);
  });

  it('reports the cleared state as default apart from the kept page size', () => {
    expect(isDefaultJobFilters(clearJobFilters(filters({status: ['failed']})))).toBe(true);
    // A non-default page size is still a non-default filter set.
    expect(isDefaultJobFilters(clearJobFilters(populated))).toBe(false);
  });

  it('repairs an unsupported page size on the way through', () => {
    expect(clearJobFilters(filters({pageSize: 37})).pageSize).toBe(DEFAULT_PAGE_SIZE);
  });

  it('does not mutate the input', () => {
    const before = JSON.stringify(populated);
    clearJobFilters(populated);
    expect(JSON.stringify(populated)).toBe(before);
  });
});

describe('setPageSize', () => {
  it('resets to page 1, because offsets do not survive a resize (rule 8)', () => {
    const next = setPageSize(filters({page: 7, pageSize: 25}), 100);
    expect(next.pageSize).toBe(100);
    expect(next.page).toBe(1);
  });

  it('keeps the rest of the filter set', () => {
    const next = setPageSize(filters({status: ['failed'], search: 'schema', page: 3}), 50);
    expect(next.status).toEqual(['failed']);
    expect(next.search).toBe('schema');
  });

  it('falls back to the default for an unsupported size', () => {
    expect(setPageSize(filters(), 37).pageSize).toBe(DEFAULT_PAGE_SIZE);
  });
});

describe('isDefaultJobFilters', () => {
  it('is false as soon as any filter is set', () => {
    expect(isDefaultJobFilters(filters({status: ['failed']}))).toBe(false);
    expect(isDefaultJobFilters(filters({origin: ['loop']}))).toBe(false);
    expect(isDefaultJobFilters(filters({projectIds: [PROJECT_A]}))).toBe(false);
    expect(isDefaultJobFilters(filters({hasProject: false}))).toBe(false);
    expect(isDefaultJobFilters(filters({includeArchivedProjects: true}))).toBe(false);
    expect(isDefaultJobFilters(filters({search: 'schema'}))).toBe(false);
    expect(isDefaultJobFilters(filters({page: 2}))).toBe(false);
    expect(isDefaultJobFilters(filters({pageSize: 100}))).toBe(false);
    expect(isDefaultJobFilters(filters({asOf: '2026-08-21T09:15:00Z'}))).toBe(false);
  });

  it('ignores a whitespace-only search, matching the serialiser', () => {
    expect(isDefaultJobFilters(filters({search: '   '}))).toBe(true);
  });

  it('agrees with an all-null query param bag', () => {
    const candidates = [
      DEFAULT_JOB_FILTERS,
      filters({status: ['failed']}),
      filters({page: 3}),
      filters({search: '  '}),
      filters({asOf: '2026-08-21T09:15:00Z'}),
    ];
    for (const candidate of candidates) {
      const allNull = Object.values(jobFiltersToQueryParams(candidate)).every(
        (value) => value === null,
      );
      expect(isDefaultJobFilters(candidate)).toBe(allNull);
    }
  });
});

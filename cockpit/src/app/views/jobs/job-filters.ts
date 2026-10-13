/**
 * URL codec and filter-token derivation for the jobs list.
 *
 * Extracted from the component deliberately: this is where the bugs live
 * (validation of hand-edited links, the project mutual-exclusion invariant,
 * page-reset discipline) and none of it needs a fixture to test.
 *
 * Three shapes are in play; keep them straight:
 *
 *   JobListFilters   the in-memory state the component holds.
 *   query params     what lands in the address bar. snake_case, because
 *                    `project_id` is forced on us (it is the alias the API
 *                    accepts, see `jobFiltersToQueryParams`) and a URL mixing
 *                    `project_id` with `pageSize` reads like two authors.
 *                    Keys: status, origin, project_id, has_project,
 *                    include_archived_projects, search, page, page_size, as_of.
 *   API query        what `GET /api/jobs` takes: limit/offset rather than
 *                    page/page_size, plus include_total.
 *
 * The URL is a user-editable surface and a link someone bookmarked six months
 * ago is a stale schema. Every parse path here degrades — drop the bad value,
 * keep the rest — and never throws.
 */

export interface JobListFilters {
  status: string[];
  origin: string[];
  projectIds: string[];
  hasProject: boolean | null;
  includeArchivedProjects: boolean;
  search: string;
  page: number;
  pageSize: number;
  asOf: string | null;
}

/** Lifecycle statuses plus server-owned terminal presentation outcomes. */
export const KNOWN_JOB_STATUSES: readonly string[] = Object.freeze([
  'created',
  'pending',
  'processing',
  'completed',
  'failed',
  'cancelled',
  'pending_review',
  'paused',
  'reviewing',
  'waiting',
  'blocked_undelivered',
]);

/** Provenance values, mirroring `GET /api/jobs?origin=`. */
export const KNOWN_JOB_ORIGINS: readonly string[] = Object.freeze([
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

/**
 * Origins a person initiated directly. Everything else — automation, loop,
 * officer, subjob, lifecycle, bench — is work the system started on its own,
 * and is what the jobs list hides by default.
 *
 * The default lives in the cockpit rather than in `/api/jobs`, deliberately:
 * archiving a project is a user saying "put this away", so the API honours it,
 * whereas a critic subjob is real work nobody retired and is only noise *in a
 * table*. Keeping "no filters" meaning "every job you can see" is also what
 * lets an agent reason about the endpoint without reading this file.
 */
export const HUMAN_ORIGINS: readonly string[] = Object.freeze(['user', 'session']);

/**
 * URL sentinel for "every origin".
 *
 * Needed because the default selection is non-empty: without it, an empty
 * origin list serialises to an absent param, an absent param parses back to
 * the default, and removing the "system jobs hidden" token silently re-applies
 * itself. Never sent to the API — the client maps the empty set to no `origin`
 * param at all, which is what the server already means by "no filter".
 */
export const ALL_ORIGINS = 'all';

/** True when the origin selection is exactly the default human pair. */
export function isSystemHiddenSelection(origin: readonly string[]): boolean {
  const unique = new Set(origin);
  return unique.size === HUMAN_ORIGINS.length && HUMAN_ORIGINS.every((o) => unique.has(o));
}

export const PAGE_SIZE_OPTIONS: readonly number[] = Object.freeze([25, 50, 100]);

export const DEFAULT_PAGE_SIZE = 25;

/**
 * Repeated uuids hit nginx's ~4k URL ceiling around 40 values, where the
 * failure mode is a truncated request rather than a clear error. The server
 * 422s past this; we drop the overflow so a pathological link still renders.
 */
export const MAX_PROJECT_FILTERS = 40;

/** `search` is `max_length=200` server-side; over-long input 422s. */
export const MAX_SEARCH_LENGTH = 200;

const EMPTY: readonly string[] = Object.freeze([]);

/**
 * Frozen so an accidental `DEFAULT_JOB_FILTERS.status.push(...)` fails loudly
 * instead of poisoning every later parse. Nothing here hands the frozen arrays
 * out: every function below returns fresh ones.
 */
export const DEFAULT_JOB_FILTERS: JobListFilters = Object.freeze({
  status: EMPTY as string[],
  origin: HUMAN_ORIGINS as string[],
  projectIds: EMPTY as string[],
  hasProject: null,
  includeArchivedProjects: false,
  search: '',
  page: 1,
  pageSize: DEFAULT_PAGE_SIZE,
  asOf: null,
});

type RawParams = Record<string, string | string[] | undefined | null>;

/** Canonical 8-4-4-4-12. Project ids are opaque to us beyond their shape. */
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/** ISO-8601 with an explicit Zulu offset, which is what the API echoes. */
const ISO_ZULU_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z$/;

/** The project-less bucket. The server accepts either spelling. */
const PROJECTLESS_ALIASES: readonly string[] = Object.freeze(['none', 'null']);

/**
 * All values for a key. Angular's ParamMap hands back a bare string for one
 * occurrence and an array for several, so both have to be accepted.
 */
function readAll(params: RawParams, key: string): string[] {
  const raw = params[key];
  if (raw === undefined || raw === null) return [];
  if (Array.isArray(raw)) return raw.filter((value): value is string => typeof value === 'string');
  return typeof raw === 'string' ? [raw] : [];
}

/**
 * The first value for a single-valued key, matching `ParamMap.get()`. A
 * duplicated `?page=2&page=3` resolves the same way the router would.
 */
function readOne(params: RawParams, key: string): string | null {
  const all = readAll(params, key);
  return all.length > 0 ? (all[0] ?? null) : null;
}

/** Dedupe preserving first-occurrence order, so the URL round-trips stably. */
function dedupe(values: readonly string[]): string[] {
  return Array.from(new Set(values));
}

function parseBool(raw: string | null): boolean | null {
  if (raw === 'true') return true;
  if (raw === 'false') return false;
  return null;
}

/** Strict positive integer. Rejects '', '2.5', '1e3', ' 2', 'abc', '-1'. */
function parsePositiveInt(raw: string | null): number | null {
  if (raw === null || !/^\d+$/.test(raw)) return null;
  const value = Number(raw);
  return Number.isSafeInteger(value) && value >= 1 ? value : null;
}

function isProjectless(value: string): boolean {
  return PROJECTLESS_ALIASES.includes(value.toLowerCase());
}

/**
 * Read filters out of a query-param bag, discarding anything invalid.
 *
 * Every rejection is silent by design: a hand-edited or stale link should
 * degrade to a sane list, never blow up the route.
 */
export function parseJobFilters(params: RawParams): JobListFilters {
  const status = dedupe(readAll(params, 'status')).filter((value) =>
    KNOWN_JOB_STATUSES.includes(value),
  );
  // Three states, not two. Absent means the default (hide system-created
  // work); the ALL_ORIGINS sentinel means the user explicitly asked for
  // everything; anything else is a validated selection. Without the sentinel,
  // "show me everything" and "first visit" are both an empty set, and
  // clearing the default filter would immediately re-apply it.
  const rawOrigin = readAll(params, 'origin');
  const origin =
    rawOrigin.length === 0
      ? [...HUMAN_ORIGINS]
      : rawOrigin.includes(ALL_ORIGINS)
        ? []
        : dedupe(rawOrigin).filter((value) => KNOWN_JOB_ORIGINS.includes(value));

  // `project_id` carries two things: real uuids and the `none` bucket alias.
  const rawProjects = dedupe(readAll(params, 'project_id'));
  const wantsProjectless = rawProjects.some(isProjectless);
  const projectIds = rawProjects
    .filter((value) => !isProjectless(value) && UUID_RE.test(value))
    .map((value) => value.toLowerCase())
    .slice(0, MAX_PROJECT_FILTERS);

  // `has_project=false` and `project_id=none` say the same thing.
  const explicit = parseBool(readOne(params, 'has_project'));
  let hasProject: boolean | null = wantsProjectless ? false : explicit;

  // Rule: the no-project bucket cannot coexist with specific projects — the
  // server 422s on the union because the filter set is AND-composed. The
  // explicit ids are the more specific intent, so they win and the bucket
  // flag is dropped. `has_project=true` alongside ids is merely redundant,
  // not contradictory, so it survives.
  if (hasProject === false && projectIds.length > 0) {
    hasProject = null;
  }

  const pageSize = parsePositiveInt(readOne(params, 'page_size'));
  const page = parsePositiveInt(readOne(params, 'page'));
  const asOf = readOne(params, 'as_of');
  const search = readOne(params, 'search') ?? '';

  return {
    status,
    origin,
    projectIds,
    hasProject,
    includeArchivedProjects: parseBool(readOne(params, 'include_archived_projects')) === true,
    search: search.trim().slice(0, MAX_SEARCH_LENGTH),
    page: page ?? DEFAULT_JOB_FILTERS.page,
    pageSize:
      pageSize !== null && PAGE_SIZE_OPTIONS.includes(pageSize) ? pageSize : DEFAULT_PAGE_SIZE,
    asOf: asOf !== null && ISO_ZULU_RE.test(asOf) && !Number.isNaN(Date.parse(asOf)) ? asOf : null,
  };
}

/**
 * Serialise to query params, dropping everything sitting at its default.
 *
 * Every key is always present — `null` for the ones being dropped — because
 * Angular's Router only *removes* a param when it is explicitly null. Omitting
 * the key under `queryParamsHandling: 'merge'` would leave a stale value in
 * the URL instead of clearing it.
 *
 * The payoff is that a URL carries only what is actually filtered, which makes
 * "is anything filtered?" answerable from the query string alone.
 */
export function jobFiltersToQueryParams(filters: JobListFilters): Record<string, string | string[] | null> {
  // Defensive: a caller can hand-build a state violating the invariant that
  // parseJobFilters enforces, and emitting both is a guaranteed 422.
  const projectIds = dedupe(filters.projectIds).slice(0, MAX_PROJECT_FILTERS);
  const projectless = filters.hasProject === false && projectIds.length === 0;

  let projectParam: string[] | null = null;
  if (projectIds.length > 0) {
    projectParam = projectIds;
  } else if (projectless) {
    projectParam = ['none'];
  }

  return {
    status: filters.status.length > 0 ? dedupe(filters.status) : null,
    origin: isSystemHiddenSelection(filters.origin)
      ? null // the default — omitted, so a bare URL is the default view
      : filters.origin.length === 0
        ? ALL_ORIGINS // explicitly every origin, which absence cannot express
        : dedupe(filters.origin),
    project_id: projectParam,
    // The false case rides on `project_id=none` above; only `true` needs the
    // flag, and it must never accompany the bucket alias.
    has_project: filters.hasProject === true ? 'true' : null,
    include_archived_projects: filters.includeArchivedProjects ? 'true' : null,
    search: filters.search.trim().length > 0 ? filters.search.trim() : null,
    page: filters.page !== DEFAULT_JOB_FILTERS.page ? String(filters.page) : null,
    page_size: filters.pageSize !== DEFAULT_PAGE_SIZE ? String(filters.pageSize) : null,
    as_of: filters.asOf,
  };
}

/**
 * True when nothing is set — equivalently, when `jobFiltersToQueryParams`
 * would emit an all-null bag and leave the URL bare.
 */
export function isDefaultJobFilters(filters: JobListFilters): boolean {
  return (
    filters.status.length === 0 &&
    isSystemHiddenSelection(filters.origin) &&
    filters.projectIds.length === 0 &&
    filters.hasProject === null &&
    filters.includeArchivedProjects === DEFAULT_JOB_FILTERS.includeArchivedProjects &&
    filters.search.trim().length === 0 &&
    filters.page === DEFAULT_JOB_FILTERS.page &&
    filters.pageSize === DEFAULT_PAGE_SIZE &&
    filters.asOf === null
  );
}

/**
 * Map to the REST query shape for `GET /api/jobs`.
 *
 * `limit` is always sent: our default page size (25) is not the server's
 * (100), so leaving it off would silently quadruple the page.
 */
export function jobFiltersToApiQuery(filters: JobListFilters): Record<string, unknown> {
  const query: Record<string, unknown> = {};

  const projectIds = dedupe(filters.projectIds).slice(0, MAX_PROJECT_FILTERS);
  if (filters.status.length > 0) query['status'] = dedupe(filters.status);
  if (filters.origin.length > 0) query['origin'] = dedupe(filters.origin);

  if (projectIds.length > 0) {
    query['project_id'] = projectIds;
  } else if (filters.hasProject === false) {
    // The bucket alias already tells the server has_project=false; sending
    // both would be redundant and the pair is what trips the 422.
    query['project_id'] = ['none'];
  } else if (filters.hasProject === true) {
    query['has_project'] = true;
  }

  if (filters.includeArchivedProjects) query['include_archived_projects'] = true;

  const search = filters.search.trim();
  if (search.length > 0) query['search'] = search.slice(0, MAX_SEARCH_LENGTH);

  query['limit'] = filters.pageSize;
  const offset = (filters.page - 1) * filters.pageSize;
  if (offset > 0) query['offset'] = offset;
  if (filters.asOf !== null) query['as_of'] = filters.asOf;
  // The client carries the total from page 1; recomputing the capped count on
  // every page is pure server work for a number we already have.
  if (filters.page > 1) query['include_total'] = false;

  return query;
}

export type JobFilterTokenKind =
  | 'status'
  | 'origin'
  | 'systemHidden'
  | 'project'
  | 'noProject'
  | 'search'
  | 'archived';

export interface JobFilterToken {
  id: string;
  kind: JobFilterTokenKind;
  value?: string;
  labelKey: string;
  labelParams?: Record<string, unknown>;
}

/** Long enough to disambiguate in practice, short enough to read as a stub. */
const SHORT_ID_LENGTH = 8;

function projectLabel(projectId: string, projectNames: ReadonlyMap<string, string>): string {
  const name = projectNames.get(projectId);
  if (name !== undefined && name.trim().length > 0) return name;
  // Never a bare full-length uuid: it is unreadable and it is the thing that
  // makes a filter row look like machine debris.
  return projectId.slice(0, SHORT_ID_LENGTH);
}

/**
 * The removable chips for the filter row.
 *
 * Note the archived token: `includeArchivedProjects === false` is a
 * default-*on* filter that HIDES rows. A hidden row with no visible token is
 * exactly how someone concludes a job was deleted, so it gets a chip even
 * though it is the default.
 */
export function activeFilterTokens(
  filters: JobListFilters,
  projectNames: ReadonlyMap<string, string>,
): JobFilterToken[] {
  const tokens: JobFilterToken[] = [];

  for (const value of dedupe(filters.status)) {
    tokens.push({
      id: `status:${value}`,
      kind: 'status',
      value,
      labelKey: 'jobs.filter.token.status',
      labelParams: {value},
    });
  }

  if (isSystemHiddenSelection(filters.origin)) {
    // One honest token beats two ("Source: user", "Source: session") that
    // describe a default nobody set. Removing it clears the origin filter,
    // which is what "show me everything" means.
    tokens.push({
      id: 'systemHidden',
      kind: 'systemHidden',
      labelKey: 'jobs.filter.token.systemHidden',
    });
  } else {
    for (const value of dedupe(filters.origin)) {
      tokens.push({
        id: `origin:${value}`,
        kind: 'origin',
        value,
        labelKey: 'jobs.filter.token.origin',
        labelParams: {value},
      });
    }
  }

  for (const value of dedupe(filters.projectIds)) {
    tokens.push({
      id: `project:${value}`,
      kind: 'project',
      value,
      labelKey: 'jobs.filter.token.project',
      labelParams: {name: projectLabel(value, projectNames)},
    });
  }

  if (filters.hasProject === false) {
    tokens.push({
      id: 'noProject',
      kind: 'noProject',
      labelKey: 'jobs.filter.token.noProject',
    });
  }

  const search = filters.search.trim();
  if (search.length > 0) {
    tokens.push({
      id: 'search',
      kind: 'search',
      value: search,
      labelKey: 'jobs.filter.token.search',
      labelParams: {term: search},
    });
  }

  if (!filters.includeArchivedProjects) {
    tokens.push({
      id: 'archived',
      kind: 'archived',
      labelKey: 'jobs.filter.token.archived',
    });
  }

  return tokens;
}

/**
 * Drop one token's contribution.
 *
 * Always back to page 1: offset arithmetic under a changed filter set lands
 * on an arbitrary row. `asOf` is deliberately kept — the watermark freezes the
 * creation-time window against concurrent inserts and is orthogonal to which
 * filters apply.
 */
export function removeFilterToken(filters: JobListFilters, token: JobFilterToken): JobListFilters {
  const next: JobListFilters = {
    ...filters,
    status: [...filters.status],
    origin: [...filters.origin],
    projectIds: [...filters.projectIds],
    page: 1,
  };

  switch (token.kind) {
    case 'status':
      next.status = next.status.filter((value) => value !== token.value);
      return next;
    case 'origin':
      next.origin = next.origin.filter((value) => value !== token.value);
      return next;
    case 'systemHidden':
      // Removing the "system jobs hidden" chip means show every origin, not
      // "drop one of the two" — the token stood for the whole default.
      next.origin = [];
      return next;
    case 'project':
      next.projectIds = next.projectIds.filter((value) => value !== token.value);
      return next;
    case 'noProject':
      next.hasProject = null;
      return next;
    case 'search':
      next.search = '';
      return next;
    case 'archived':
      // Removing the "archived hidden" chip reveals them.
      next.includeArchivedProjects = true;
      return next;
  }
}

/**
 * Change the page size, which also lands back on page 1.
 *
 * Additive to the codec contract, but rule "any change resets the page" needs
 * a home for `pageSize` too: page 3 at 25/page and page 3 at 100/page point at
 * completely different rows, so keeping the number is worse than useless. An
 * unsupported size falls back to the default rather than reaching the server.
 */
export function setPageSize(filters: JobListFilters, pageSize: number): JobListFilters {
  return {
    ...filters,
    status: [...filters.status],
    origin: [...filters.origin],
    projectIds: [...filters.projectIds],
    pageSize: PAGE_SIZE_OPTIONS.includes(pageSize) ? pageSize : DEFAULT_PAGE_SIZE,
    page: 1,
  };
}

/**
 * Reset everything except `pageSize`, which is a stated user preference rather
 * than a filter. `asOf` goes too: a cleared list wants a fresh window.
 */
export function clearJobFilters(filters: JobListFilters): JobListFilters {
  return {
    status: [],
    origin: [...HUMAN_ORIGINS],
    projectIds: [],
    hasProject: null,
    includeArchivedProjects: DEFAULT_JOB_FILTERS.includeArchivedProjects,
    search: '',
    page: 1,
    pageSize: PAGE_SIZE_OPTIONS.includes(filters.pageSize) ? filters.pageSize : DEFAULT_PAGE_SIZE,
    asOf: null,
  };
}

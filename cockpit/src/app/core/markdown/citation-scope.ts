import type {AssistantTurn} from '../models/turn.model';

/**
 * Display numbering for the citation chips of one answer (owner ruling D4).
 *
 * The agent writes handles (`[^c1241]`); readers see ¹ ² ³, numbered by
 * first mention across the whole answer, not per rendered block, so an answer
 * that goes text → tool → text does not count from 1 twice. The numbers are
 * assigned here, at render time, and never reach the model's history, so the
 * prompt and its cache are untouched.
 *
 * Order: the answer's text in event order, then its reasoning. A handle that
 * only a tool card shows (a citation created but not used in the text) gets
 * the next number when that card renders. Only citations the chat knows are
 * numbered, so a stray `[5]` in prose does not take a number.
 */
export class CitationScope {
    private numbers = new Map<number, number>();
    private next = 1;
    private numberedFor: ReadonlyMap<number, unknown> | null = null;

    /** @param order citation ids in first-mention order across the answer */
    constructor(readonly order: readonly number[]) {}

    /** The chip number for `cid`, or undefined when the chat doesn't know it. */
    numberOf(cid: number, known: ReadonlyMap<number, unknown>): number | undefined {
        if (this.numberedFor !== known) {
            // The known set changed (a citation arrived): renumber from the
            // answer's order, so numbers stay first-mention.
            this.numberedFor = known;
            this.numbers = new Map();
            this.next = 1;
            for (const c of this.order) {
                if (known.has(c) && !this.numbers.has(c)) {
                    this.numbers.set(c, this.next++);
                }
            }
        }
        if (!known.has(cid)) {
            return undefined;
        }
        let n = this.numbers.get(cid);
        if (n === undefined) {
            n = this.next++;
            this.numbers.set(cid, n);
        }
        return n;
    }
}

// Mirrors citationExtension's tokenizer: a handle `[^c12]` / `[c12]`, or a
// legacy `[12]` not followed by `(`, `[` or `:`.
const MARKER_RE = /\[(?:\^?c)?(\d+)\](?![([:])/g;
const FENCED_CODE_RE = /^(```|~~~)[^\n]*\n[\s\S]*?^\1[^\n]*$/gm;
const INLINE_CODE_RE = /`[^`\n]*`/g;

/** Citation ids in `text`, in first-mention order, skipping code. */
export function citationIdsIn(text: string): number[] {
    const body = (text || '').replace(FENCED_CODE_RE, '').replace(INLINE_CODE_RE, '');
    const seen = new Set<number>();
    for (const m of body.matchAll(MARKER_RE)) {
        seen.add(Number(m[1]));
    }
    return [...seen];
}

const scopes = new WeakMap<readonly unknown[], CitationScope>();

/**
 * The numbering scope of one assistant turn. Cached on the turn's events
 * array, which the reducer replaces whenever the turn changes, so every
 * block of the same render shares one scope.
 */
export function citationScopeFor(turn: AssistantTurn): CitationScope {
    const events = turn.events;
    let scope = scopes.get(events);
    if (!scope) {
        const order: number[] = [];
        const seen = new Set<number>();
        const add = (content: string) => {
            for (const cid of citationIdsIn(content)) {
                if (!seen.has(cid)) {
                    seen.add(cid);
                    order.push(cid);
                }
            }
        };
        for (const e of events) {
            if (e.kind === 'text') add(e.content);
        }
        for (const e of events) {
            if (e.kind === 'thought') add(e.content);
        }
        scope = new CitationScope(order);
        scopes.set(events, scope);
    }
    return scope;
}

const textScopes = new Map<string, CitationScope>();

/**
 * A numbering scope for one standalone text (a compaction summary), numbered
 * by first mention within it. Small cache keyed by the text itself.
 */
export function citationScopeForText(text: string): CitationScope {
    let scope = textScopes.get(text);
    if (!scope) {
        if (textScopes.size >= 64) {
            textScopes.delete(textScopes.keys().next().value as string);
        }
        scope = new CitationScope(citationIdsIn(text));
        textScopes.set(text, scope);
    }
    return scope;
}

/**
 * Text to render for a block that is still streaming: a half-written handle
 * at the very end (`[^c12`) is held back until it closes, so it doesn't flash
 * as literal text before turning into a chip. Finished text is unchanged.
 */
export function holdBackPartialHandle(text: string, streaming: boolean): string {
    if (!streaming || !text) {
        return text;
    }
    return text.replace(/\[(?:\^c?|c)?\d{0,12}$/, '');
}

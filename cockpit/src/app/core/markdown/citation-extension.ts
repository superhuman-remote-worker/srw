import {MarkedExtension} from 'marked';

/**
 * Marked extension that marks the agent's citation handles for
 * `CitationRefDirective`, which turns them into numbered chips with a popover.
 *
 * The cite tools return a handle, `[^c<id>]` (Markdown footnote form), and the
 * agent writes it right after the claim; `<id>` is the citation record's id,
 * not a display number. Sessions from before 2026-10-28 carry the bare id,
 * `[<id>]` (owner ruling D5), so that form is marked too, as `legacy`:
 *
 *   [^c21]  ->  <span class="citation-ref cid-21 handle" data-cid="21">[^c21]</span>
 *   [21]    ->  <span class="citation-ref cid-21 legacy" data-cid="21">[21]</span>
 *
 * This stage is stateless. Its output keeps the literal text, so wherever the
 * directive does not run (the inbox, a KB note) the reader sees the handle as
 * plain text rather than an empty superscript. The directive numbers resolved
 * handles per answer; an unresolved new-form handle is flagged, and an
 * unresolved legacy `[N]` stays plain text, since a stray `[5]` in prose is
 * not a citation.
 *
 * `[c21]` (the caret dropped, as models sometimes write it) counts as a
 * handle. A marker immediately followed by `(`, `[` or `:` is left alone, so
 * Markdown links and definitions (`[1](url)`, `[1][ref]`, `[^c1]: …`) still
 * parse as before.
 */
export function citationExtension(): MarkedExtension {
    return {
        extensions: [
            {
                name: 'citationRef',
                level: 'inline',
                start(src: string) {
                    const i = src.indexOf('[');
                    return i < 0 ? undefined : i;
                },
                tokenizer(src: string) {
                    const match = src.match(/^\[(\^?c)?(\d+)\]/);
                    if (!match) {
                        return undefined;
                    }
                    const after = src.charAt(match[0].length);
                    if (after === '(' || after === '[' || after === ':') {
                        // Real markdown link / reference definition — not a citation.
                        return undefined;
                    }
                    return {
                        type: 'citationRef',
                        raw: match[0],
                        cid: match[2],
                        legacy: !match[1],
                    };
                },
                renderer(token: any) {
                    // The id rides a class (`cid-N`) as well as data-cid: class
                    // survives the markdown HTML sanitizer for sure, so the
                    // directive can always recover the id after it replaces
                    // the visible text. The raw text is digits and brackets
                    // only, so it needs no escaping.
                    const kind = token.legacy ? 'legacy' : 'handle';
                    return `<span class="citation-ref cid-${token.cid} ${kind}" data-cid="${token.cid}">${token.raw}</span>`;
                },
            },
        ],
    };
}

import {Marked} from 'marked';
import {describe, expect, it} from 'vitest';

import {citationExtension} from './citation-extension';

const render = (md: string) => new Marked(citationExtension()).parseInline(md) as string;

describe('citationExtension', () => {
    it('marks a handle, keeping its literal text for views without the resolver', () => {
        expect(render('Claim [^c1241].')).toBe(
            'Claim <span class="citation-ref cid-1241 handle" data-cid="1241">[^c1241]</span>.',
        );
    });

    it('accepts the caret dropped', () => {
        expect(render('[c7]')).toContain('cid-7 handle');
    });

    it('marks a legacy [N] as legacy (old sessions, D5)', () => {
        expect(render('Old [1276]')).toContain('class="citation-ref cid-1276 legacy"');
    });

    it('leaves links and definitions alone', () => {
        expect(render('[1](https://x.example)')).toContain('<a ');
        expect(render('[^c3]: Source')).not.toContain('citation-ref');
    });
});

import type {ThreadCitation} from '../services/persistent-chat.service';

/**
 * What a reader is told about a citation's state, in the chip, the popover
 * and the citations panel (overhaul §6.2). Pending never looks verified:
 * it is neutral and says the source is still being checked. A citation is
 * flagged, never hidden or deleted (owner ruling D7).
 *
 *   pending     → "Being checked"  neutral
 *   verified    → "Verified"       confirmed
 *   failed      → "Unverified"     warning, with the reason
 *   unverified  → "Not checked"    grey (verifier off, down, or no source)
 *   withdrawn   → "Withdrawn"      struck through (the agent stopped using it)
 */
export type CitationState = 'pending' | 'verified' | 'failed' | 'unchecked' | 'withdrawn';

export function citationState(c: Pick<ThreadCitation, 'verification_status' | 'withdrawn_at'>): CitationState {
    if (c.withdrawn_at) return 'withdrawn';
    switch (c.verification_status) {
        case 'verified':
            return 'verified';
        case 'failed':
            return 'failed';
        case 'pending':
            return 'pending';
        default:
            return 'unchecked';
    }
}

/** i18n key for the state's label. */
export function citationStateLabel(state: CitationState): string {
    return `chat.citations.state.${state}`;
}

/** i18n key for a `verification_reason` code, or null for none/unknown. */
export function citationReasonLabel(reason: string | null | undefined): string | null {
    switch (reason) {
        case 'quote_not_found':
        case 'not_supported':
        case 'source_unavailable':
        case 'not_checked':
            return `chat.citations.reason.${reason}`;
        default:
            return null;
    }
}

/** The http(s) URL a citation's source opens at, or null. */
export function citationSourceUrl(c: Pick<ThreadCitation, 'source_identifier'>): string | null {
    const id = c.source_identifier;
    return id && /^https?:\/\//i.test(id) ? id : null;
}

/** "page 4, §3.2" from a locator; empty when it names no place. */
export function citationLocatorText(locator: Record<string, unknown> | null | undefined): string {
    if (!locator) return '';
    const parts: string[] = [];
    for (const [k, v] of Object.entries(locator)) {
        if (v === null || v === undefined || v === '' || k === 'accessed_at' || k === 'title') continue;
        parts.push(k === 'page' ? `p. ${v}` : k === 'section' ? `§ ${v}` : `${k} ${v}`);
    }
    return parts.join(', ');
}

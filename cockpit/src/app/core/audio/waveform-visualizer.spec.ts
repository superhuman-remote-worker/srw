import {describe, expect, it} from 'vitest';
import {barAlpha, barCapacity, barHeight, WAVEFORM} from './waveform-visualizer';

describe('waveform geometry', () => {
    it('draws silence as a dot, never as nothing', () => {
        expect(barHeight(0, 40)).toBe(WAVEFORM.BAR_WIDTH);
        expect(barHeight(1, 40)).toBe(WAVEFORM.BAR_WIDTH);
    });

    it('grows with the voice level and stays inside the canvas', () => {
        expect(barHeight(30, 40)).toBeGreaterThan(barHeight(10, 40));
        expect(barHeight(100, 40)).toBe(36);
        expect(barHeight(250, 40)).toBe(36);
    });

    it('fills the width with bars plus one scrolling in', () => {
        const step = WAVEFORM.BAR_WIDTH + WAVEFORM.BAR_GAP;
        expect(barCapacity(step * 10)).toBe(11);
        expect(barCapacity(0)).toBe(1);
    });

    it('fades old bars towards the left edge and keeps new ones opaque', () => {
        expect(barAlpha(0, 300)).toBeCloseTo(WAVEFORM.FADE_FLOOR);
        expect(barAlpha(300, 300)).toBe(1);
        expect(barAlpha(50, 300)).toBeGreaterThan(barAlpha(10, 300));
    });
});

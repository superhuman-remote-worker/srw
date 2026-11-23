/**
 * Live dictation waveform: thin rounded bars mirrored around the centre line,
 * scrolling in from the right edge, in the canvas's CSS `color`.
 *
 * The canvas is sized to its CSS box × devicePixelRatio (ResizeObserver), so
 * the bars stay crisp and the canvas never imposes an intrinsic width on its
 * flex row. A fixed `width` attribute once pushed the recording strip's ✓ off a
 * phone screen.
 *
 * Before any voice arrives the strip is a row of dots, so it reads as
 * "listening" from the first frame. `destroy()` stops drawing but leaves the
 * last frame painted: the composer dims it as the frozen "transcribing" state.
 */

/** Bar geometry in CSS pixels. */
export const WAVEFORM = {
    BAR_WIDTH: 3,
    BAR_GAP: 3,
    /** A new bar every SAMPLE_MS; the row scrolls one bar per sample. */
    SAMPLE_MS: 80,
    /** Fraction of the width, from the left edge, over which old bars fade out. */
    FADE_SPAN: 0.35,
    /** Opacity of the oldest bar at the left edge. */
    FADE_FLOOR: 0.2,
    /** Visual gain on the 0-100 voice level, so normal speech fills about half the height. */
    GAIN: 1.5,
} as const;

/** One bar's height in CSS px for a 0-100 voice level: never below a dot. */
export function barHeight(level: number, canvasHeight: number): number {
    const v = Math.max(0, Math.min(1, (level / 100) * WAVEFORM.GAIN));
    const usable = Math.max(0, canvasHeight - 4);
    return Math.max(WAVEFORM.BAR_WIDTH, v * usable);
}

/** How many bars fit across a width (plus one that is scrolling in or out). */
export function barCapacity(width: number): number {
    const step = WAVEFORM.BAR_WIDTH + WAVEFORM.BAR_GAP;
    return Math.max(0, Math.ceil(width / step) + 1);
}

/** Opacity for a bar at `x`: faded towards the left edge, full on the right. */
export function barAlpha(x: number, width: number): number {
    const span = width * WAVEFORM.FADE_SPAN;
    if (span <= 0) return 1;
    const t = Math.max(0, Math.min(1, x / span));
    return WAVEFORM.FADE_FLOOR + (1 - WAVEFORM.FADE_FLOOR) * t;
}

export class WaveformVisualizer {
    private readonly ctx: CanvasRenderingContext2D | null;
    /** Voice levels (0-100), oldest first. Pre-filled with silence. */
    private levels: number[] = [];
    private lastSampleAt = 0;
    private frame: number | null = null;
    private resizeObserver: ResizeObserver | null = null;
    private cssWidth = 0;
    private cssHeight = 0;
    private color = 'currentColor';
    private readonly reducedMotion: boolean;

    constructor(
        private readonly canvas: HTMLCanvasElement,
        private readonly level: () => number,
    ) {
        this.ctx = canvas.getContext('2d', {alpha: true});
        this.reducedMotion =
            typeof matchMedia === 'function' && matchMedia('(prefers-reduced-motion: reduce)').matches;
    }

    start(): void {
        if (!this.ctx || this.frame !== null) return;
        this.measure();
        if (typeof ResizeObserver === 'function') {
            this.resizeObserver = new ResizeObserver(() => this.measure());
            this.resizeObserver.observe(this.canvas);
        }
        this.lastSampleAt = performance.now();
        this.frame = requestAnimationFrame(this.tick);
    }

    /** Stop drawing and observing. The last frame stays on the canvas. */
    destroy(): void {
        if (this.frame !== null) {
            cancelAnimationFrame(this.frame);
            this.frame = null;
        }
        this.resizeObserver?.disconnect();
        this.resizeObserver = null;
    }

    private measure(): void {
        const rect = this.canvas.getBoundingClientRect();
        const dpr = Math.max(1, globalThis.devicePixelRatio || 1);
        this.cssWidth = rect.width;
        this.cssHeight = rect.height;
        this.canvas.width = Math.round(rect.width * dpr);
        this.canvas.height = Math.round(rect.height * dpr);
        this.ctx?.setTransform(dpr, 0, 0, dpr, 0, 0);
        this.color = getComputedStyle(this.canvas).color || this.color;
        // Keep the newest levels; pad the left with silence so dots span the width.
        const capacity = barCapacity(this.cssWidth);
        const kept = this.levels.slice(-capacity);
        this.levels = [...new Array(Math.max(0, capacity - kept.length)).fill(0), ...kept];
    }

    private readonly tick = (now: number): void => {
        while (now - this.lastSampleAt >= WAVEFORM.SAMPLE_MS) {
            this.levels.push(this.level());
            this.levels.shift();
            this.lastSampleAt += WAVEFORM.SAMPLE_MS;
        }
        const progress = this.reducedMotion ? 0 : (now - this.lastSampleAt) / WAVEFORM.SAMPLE_MS;
        this.draw(progress);
        this.frame = requestAnimationFrame(this.tick);
    };

    private draw(progress: number): void {
        const ctx = this.ctx;
        if (!ctx) return;
        const w = this.cssWidth;
        const h = this.cssHeight;
        const step = WAVEFORM.BAR_WIDTH + WAVEFORM.BAR_GAP;
        ctx.clearRect(0, 0, w, h);
        ctx.fillStyle = this.color;
        const n = this.levels.length;
        for (let i = n - 1; i >= 0; i--) {
            const x = w - WAVEFORM.BAR_WIDTH - (n - 1 - i + progress) * step;
            if (x < -WAVEFORM.BAR_WIDTH) break;
            const bh = barHeight(this.levels[i], h);
            ctx.globalAlpha = barAlpha(x, w);
            roundedBar(ctx, x, (h - bh) / 2, WAVEFORM.BAR_WIDTH, bh);
        }
        ctx.globalAlpha = 1;
    }
}

function roundedBar(ctx: CanvasRenderingContext2D, x: number, y: number, w: number, h: number): void {
    const r = Math.min(w / 2, h / 2);
    ctx.beginPath();
    if (typeof ctx.roundRect === 'function') {
        ctx.roundRect(x, y, w, h, r);
    } else {
        ctx.rect(x, y, w, h);
    }
    ctx.fill();
}

import {ChangeDetectionStrategy, Component} from '@angular/core';

/**
 * Static SVG explainer: which prompt each agent uses, and where in its loop.
 *
 * Two columns, mirroring the two execution models in the codebase:
 *   - WORKER (jobs)       -> src/agent/graph.py — a LangGraph state machine
 *                            that alternates STRATEGIC (plan/audit) and
 *                            TACTICAL (execute todos) phases. The system prompt
 *                            is the same in both; each phase opens with its
 *                            phase skill (strategic-phase | tactical-phase).
 *   - PERSISTENT (sessions) -> src/persistent_graph.py — a turn-based loop with
 *                            no phases; every turn uses one conversational
 *                            prompt (systemprompt_interactive.txt).
 *
 * Both share persona.txt, run summarization_prompt.txt on compaction, and use
 * memory/curation prompts in the background. The bundled files resolve to a
 * model-family variant (e.g. systemprompt_minimax_m3.txt) via the config
 * matrix — see get_system_prompt() in src/shared/runtime/core/loader.py.
 *
 * Purely presentational (no inputs/state). Colours come from theme tokens so it
 * tracks the active theme. Labels are hardcoded English to match this page
 * (AdminConfigComponent), whose own copy is not translated.
 */
@Component({
  selector: 'app-agent-loop-diagram',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <figure class="diag">
      <svg
        class="diag-svg"
        viewBox="0 0 760 424"
        preserveAspectRatio="xMidYMid meet"
        role="img"
        aria-label="Comparison of the worker agent loop and the persistent agent loop, showing which prompt each uses at each step."
      >
        <defs>
          <marker
            id="srwArrow"
            viewBox="0 0 7 7"
            refX="6"
            refY="3.5"
            markerWidth="7"
            markerHeight="7"
            orient="auto"
          >
            <path d="M0,0 L7,3.5 L0,7 Z" class="diag-arrowhead" />
          </marker>
        </defs>

        <!-- column divider -->
        <line x1="380" y1="16" x2="380" y2="300" class="diag-divider" />

        <!-- ============ LEFT · WORKER (jobs) ============ -->
        <text x="190" y="26" text-anchor="middle" class="diag-title">WORKER · jobs</text>
        <text x="190" y="42" text-anchor="middle" class="diag-sub">batch · alternating phases</text>

        <rect x="120" y="58" width="140" height="34" rx="7" class="diag-node" />
        <text x="190" y="80" text-anchor="middle" class="diag-node-label">initialize</text>

        <line x1="190" y1="92" x2="190" y2="118" class="diag-edge" marker-end="url(#srwArrow)" />

        <!-- the alternating phase loop -->
        <rect x="85" y="122" width="210" height="140" rx="12" class="diag-loop" />
        <text x="282" y="141" text-anchor="middle" class="diag-loop-glyph">⟳</text>

        <rect x="100" y="132" width="180" height="48" rx="7" class="diag-strategic" />
        <text x="190" y="154" text-anchor="middle" class="diag-strategic-label">STRATEGIC</text>
        <text x="190" y="170" text-anchor="middle" class="diag-node-sub">plan · audit · todos</text>

        <text x="190" y="197" text-anchor="middle" class="diag-alt">⇅ alternate each phase</text>

        <rect x="100" y="208" width="180" height="48" rx="7" class="diag-tactical" />
        <text x="190" y="230" text-anchor="middle" class="diag-tactical-label">TACTICAL</text>
        <text x="190" y="246" text-anchor="middle" class="diag-node-sub">execute todos</text>

        <line x1="190" y1="262" x2="190" y2="288" class="diag-edge" marker-end="url(#srwArrow)" />
        <text x="190" y="303" text-anchor="middle" class="diag-cap">goal met? → END · else loop</text>
        <path d="M150,278 L55,278 L55,192 L83,192" class="diag-loopback" marker-end="url(#srwArrow)" />

        <rect x="24" y="316" width="332" height="96" rx="8" class="diag-prompt" />
        <text x="190" y="336" text-anchor="middle" class="diag-prompt-title">PROMPT · EVERY LLM CALL</text>
        <text x="190" y="355" text-anchor="middle" class="diag-prompt-body">
          system · persona
        </text>
        <line x1="48" y1="368" x2="332" y2="368" class="diag-box-divider" />
        <text x="190" y="386" text-anchor="middle" class="diag-instr-title">PHASE SKILLS · AT PHASE START</text>
        <text x="190" y="404" text-anchor="middle" class="diag-instr-body">
          <tspan class="ink-strategic">STRATEGIC</tspan> strategic-phase
          <tspan class="diag-box-sep"> │ </tspan><tspan class="ink-tactical">TACTICAL</tspan> tactical-phase
        </text>

        <!-- ============ RIGHT · PERSISTENT (sessions) ============ -->
        <text x="570" y="26" text-anchor="middle" class="diag-title">PERSISTENT · sessions</text>
        <text x="570" y="42" text-anchor="middle" class="diag-sub">interactive · streaming</text>

        <rect x="500" y="58" width="140" height="34" rx="7" class="diag-node" />
        <text x="570" y="80" text-anchor="middle" class="diag-node-label">user message</text>

        <line x1="570" y1="92" x2="570" y2="118" class="diag-edge" marker-end="url(#srwArrow)" />
        <text x="588" y="112" text-anchor="start" class="diag-cap-side">+ memory / kb</text>

        <!-- the single conversational loop -->
        <rect x="480" y="122" width="180" height="140" rx="10" class="diag-persistent" />
        <text x="645" y="141" text-anchor="middle" class="diag-loop-glyph">⟳</text>
        <text x="570" y="153" text-anchor="middle" class="diag-persistent-label">LLM ⇄ tools</text>
        <text x="570" y="185" text-anchor="middle" class="diag-node-sub">stream tokens</text>
        <text x="570" y="207" text-anchor="middle" class="diag-node-sub">run tools</text>
        <text x="570" y="229" text-anchor="middle" class="diag-node-sub">loop until reply</text>

        <line x1="570" y1="262" x2="570" y2="288" class="diag-edge" marker-end="url(#srwArrow)" />
        <text x="570" y="303" text-anchor="middle" class="diag-cap">reply → wait for next message</text>
        <path d="M610,278 L690,278 L690,192 L662,192" class="diag-loopback" marker-end="url(#srwArrow)" />

        <rect x="404" y="316" width="332" height="96" rx="8" class="diag-prompt" />
        <text x="570" y="336" text-anchor="middle" class="diag-prompt-title">PROMPT · EVERY TURN</text>
        <text x="570" y="355" text-anchor="middle" class="diag-prompt-body">
          system · persona · <tspan class="ink-persistent">interactive</tspan>
        </text>
        <line x1="428" y1="368" x2="712" y2="368" class="diag-box-divider" />
        <text x="570" y="386" text-anchor="middle" class="diag-instr-title">INSTRUCTION FILES · ON TRIGGER</text>
        <text x="570" y="404" text-anchor="middle" class="diag-instr-body">
          <tspan class="diag-box-sep">on setup ›</tspan> design_guide.md
        </text>
      </svg>

      <figcaption class="diag-caption">
        Skills and instruction files inject at their trigger — a phase start,
        before a specific tool (gated until read), or session setup — per
        expert config, separate from the every-call prompt. System prompts
        resolve to a model-family variant (e.g.
        <code>systemprompt_minimax_m3.txt</code>). Both modes also run
        <code>summarization_prompt.txt</code> during context compaction and
        <code>memory</code> / <code>curation</code> prompts in the background.
      </figcaption>
    </figure>
  `,
  styles: [`
    :host { display: block; }
    .diag { margin: 0; }
    .diag-svg {
      display: block;
      width: 100%;
      max-width: 880px;
      height: auto;
      margin: 0 auto;
    }

    /* headings */
    .diag-title { font-size: 13px; font-weight: 600; fill: var(--text-primary); }
    .diag-sub { font-size: 9.5px; fill: var(--text-muted); }

    /* generic nodes */
    .diag-node { fill: var(--app-bg); stroke: var(--border-color); stroke-width: 1; }
    .diag-node-label { font-size: 12px; font-weight: 500; fill: var(--text-primary); }
    .diag-node-sub { font-size: 9px; fill: var(--text-muted); }

    /* worker phase loop */
    .diag-loop { fill: none; stroke: var(--border-color); stroke-width: 1; stroke-dasharray: 4 3; }
    .diag-loop-glyph { font-size: 14px; fill: var(--text-secondary); }
    .diag-strategic { fill: var(--app-bg); stroke: var(--accent-color); stroke-width: 1.5; }
    .diag-strategic-label { font-size: 12px; font-weight: 600; fill: var(--accent-color); }
    .diag-tactical { fill: var(--app-bg); stroke: var(--info); stroke-width: 1.5; }
    .diag-tactical-label { font-size: 12px; font-weight: 600; fill: var(--info); }
    .diag-alt { font-size: 9.5px; fill: var(--text-secondary); }

    /* persistent loop */
    .diag-persistent { fill: var(--app-bg); stroke: var(--success); stroke-width: 1.5; }
    .diag-persistent-label { font-size: 13px; font-weight: 600; fill: var(--text-primary); }

    /* edges + arrows */
    .diag-edge { stroke: var(--text-secondary); stroke-width: 1.4; fill: none; }
    .diag-loopback { stroke: var(--text-muted); stroke-width: 1.2; fill: none; stroke-dasharray: 4 3; }
    .diag-arrowhead { fill: var(--text-secondary); }
    .diag-divider { stroke: var(--border-color); stroke-width: 1; stroke-dasharray: 3 4; }
    .diag-cap { font-size: 9.5px; fill: var(--text-muted); }
    .diag-cap-side { font-size: 9px; fill: var(--text-muted); }

    /* prompt callouts */
    .diag-prompt { fill: var(--app-bg); stroke: var(--border-color); stroke-width: 1; }
    .diag-prompt-title { font-size: 9px; font-weight: 600; fill: var(--text-secondary); letter-spacing: 0.05em; }
    .diag-prompt-body { font-size: 12px; fill: var(--text-secondary); }
    .ink-strategic { fill: var(--accent-color); font-weight: 600; }
    .ink-tactical { fill: var(--info); font-weight: 600; }
    .ink-persistent { fill: var(--success); font-weight: 600; }

    /* instruction-file callout (second row of the prompt box) */
    .diag-box-divider { stroke: var(--border-color); stroke-width: 1; }
    .diag-instr-title { font-size: 9px; font-weight: 600; fill: var(--text-secondary); letter-spacing: 0.05em; }
    .diag-instr-body { font-size: 9.5px; fill: var(--text-secondary); }
    .diag-box-sep { fill: var(--text-muted); }

    /* caption under the figure */
    .diag-caption {
      margin-top: 12px;
      font-size: 12px;
      line-height: 1.5;
      color: var(--text-muted);
    }
    .diag-caption code {
      font-family: var(--font-family-mono, monospace);
      font-size: 11px;
      color: var(--text-secondary);
    }
  `],
})
export class AgentLoopDiagramComponent {}

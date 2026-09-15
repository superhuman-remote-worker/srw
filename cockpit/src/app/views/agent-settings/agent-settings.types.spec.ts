import {describe, it, expect} from 'vitest';
import {
  defaultModelOptionLabel,
  detectModelFamily,
  resolveEffectiveModels,
  resolveMatrixForModel,
} from './agent-settings.types';
import type {EffectiveModels} from '../../core/models/api.model';

describe('detectModelFamily — Muse Spark 1.3', () => {
  it.each(['', 'meta/', 'openrouter/meta/'])(
    'recognizes standard and Contributor IDs with prefix %s', (prefix) => {
      expect(detectModelFamily(`${prefix}Muse-Spark-1.3`)).toBe('muse-spark-1.3');
      expect(detectModelFamily(`${prefix}muse-spark-1.3-contributor`)).toBe('muse-spark-1.3');
      expect(detectModelFamily(`${prefix}muse-spark-1.3-20260902`)).toBe('muse-spark-1.3');
      expect(detectModelFamily(`${prefix}muse-spark-1.3:exacto`)).toBe('muse-spark-1.3');
      expect(detectModelFamily(`${prefix}muse-spark-1.2`)).toBe('default');
      expect(detectModelFamily(`${prefix}muse-spark-1.30`)).toBe('default');
    },
  );
});

describe('detectModelFamily — GLM', () => {
  it.each(['', 'z-ai/', 'openrouter/z-ai/', 'zai-org/'])(
    'distinguishes GLM-5.3 Flash vision settings with prefix %s', (prefix) => {
      expect(detectModelFamily(`${prefix}GLM-5.3-Flash`)).toBe('glm-5.3-flash');
      expect(detectModelFamily(`${prefix}glm-5.3-flash:exacto`)).toBe('glm-5.3-flash');
      expect(detectModelFamily(`${prefix}glm-5.3`)).toBe('glm-5.3');
      expect(detectModelFamily(`${prefix}glm-5.3-20260816`)).toBe('glm-5.3');
      expect(detectModelFamily(`${prefix}glm-4.7-flash`)).toBe('glm');
    },
  );

  it('maps GLM-5.2 IDs to the glm family across transports', () => {
    expect(detectModelFamily('openrouter/z-ai/glm-5.2')).toBe('glm');
    expect(detectModelFamily('z-ai/glm-5.2')).toBe('glm');
    expect(detectModelFamily('glm-5.2')).toBe('glm');
  });
});

describe('detectModelFamily — Claude Opus 5', () => {
  it('maps Opus 5 IDs to the claude-opus-5 family across transports', () => {
    expect(detectModelFamily('claude-opus-5')).toBe('claude-opus-5');
    expect(detectModelFamily('claude-opus-5-20260401')).toBe('claude-opus-5');
    expect(detectModelFamily('openrouter/anthropic/claude-opus-5')).toBe(
      'claude-opus-5',
    );
  });

  it('leaves older Opus rows on the generic family', () => {
    expect(detectModelFamily('claude-opus-4-8')).toBe('claude-opus');
    expect(detectModelFamily('claude-opus-4-5')).toBe('claude-opus');
  });
});

describe('detectModelFamily — Claude Fable', () => {
  it('maps Fable 5 and 5.1 to one family', () => {
    expect(detectModelFamily('claude-fable-5')).toBe('claude-fable');
    expect(detectModelFamily('claude-fable-5-1')).toBe('claude-fable');
    expect(detectModelFamily('openrouter/anthropic/claude-fable-5-1')).toBe(
      'claude-fable',
    );
  });
});

describe('detectModelFamily — GPT-5.6', () => {
  it('maps GPT-5.6 tiers to the gpt-5.6 family, ahead of gpt-5', () => {
    expect(detectModelFamily('gpt-5.6-sol')).toBe('gpt-5.6');
    expect(detectModelFamily('gpt-5.6-terra')).toBe('gpt-5.6');
    expect(detectModelFamily('openai/gpt-5.6-luna')).toBe('gpt-5.6');
    expect(detectModelFamily('codex/gpt-5.6-sol')).toBe('gpt-5.6');
  });

  it('keeps neighbors unaffected', () => {
    expect(detectModelFamily('gpt-5.5')).toBe('gpt-5');
    expect(detectModelFamily('gpt-5.6-codex')).toBe('codex');
  });
});

describe('detectModelFamily — GPT-6', () => {
  it('maps GPT-6 Astra to the gpt-6 family across transports', () => {
    expect(detectModelFamily('gpt-6-astra')).toBe('gpt-6');
    expect(detectModelFamily('openai/gpt-6-astra')).toBe('gpt-6');
    expect(detectModelFamily('codex/gpt-6-astra')).toBe('gpt-6');
    expect(detectModelFamily('openrouter/openai/gpt-6-astra')).toBe('gpt-6');
  });

  it('keeps codex precedence, matching family_of() on the server', () => {
    expect(detectModelFamily('gpt-6-astra-codex')).toBe('codex');
    expect(detectModelFamily('gpt-6-codex-spark')).toBe('codex-spark');
  });

  it('keeps neighbors unaffected', () => {
    expect(detectModelFamily('gpt-5.6-sol')).toBe('gpt-5.6');
    expect(detectModelFamily('gpt-5')).toBe('gpt-5');
  });
});

describe('detectModelFamily — Mistral', () => {
  it('maps Mistral 3 family + specialists across transports', () => {
    expect(detectModelFamily('mistral-large-latest')).toBe('mistral');
    expect(detectModelFamily('mistral-medium-latest')).toBe('mistral');
    expect(detectModelFamily('mistral-small-latest')).toBe('mistral');
    expect(detectModelFamily('codestral-latest')).toBe('mistral');
    expect(detectModelFamily('openrouter/mistralai/mistral-large')).toBe('mistral');
  });
});

describe('resolveMatrixForModel', () => {
  // Flattened settings-matrix shape (family → resolved settings), exactly what
  // the client receives from the backend's _load_settings_matrix output. The
  // per-phase mismatch advisory that used to sit on top of this is gone with
  // the tiers (U1): one model runs the whole job, so there is nothing to
  // compare — the family resolution itself is what the Advanced accordion
  // still reads for temperature/multimodal defaults.
  const M = {
    default: {model_max_context_tokens: 128000, multimodal: false},
    'gpt-5': {model_max_context_tokens: 1050000, multimodal: true},
    gemma: {model_max_context_tokens: 131072, multimodal: true},
  };

  it('merges the family block over the default block', () => {
    expect(resolveMatrixForModel(M, 'gpt-5.5')).toEqual({
      model_max_context_tokens: 1050000,
      multimodal: true,
    });
    expect(resolveMatrixForModel(M, 'RedHatAI/gemma-4-31B-it-FP8-Dynamic')).toEqual({
      model_max_context_tokens: 131072,
      multimodal: true,
    });
  });

  it('falls back to the default block for an unknown family, and to {} without a matrix or model', () => {
    expect(resolveMatrixForModel(M, 'some/unknown-model')).toEqual(M.default);
    expect(resolveMatrixForModel({}, 'gpt-5.5')).toEqual({});
    expect(resolveMatrixForModel(M, '')).toEqual({});
  });
});

describe('resolveEffectiveModels', () => {
  // U1 shape: one `model` slot (+ `subagent`, and `session` kept equal to it);
  // the per-phase strategic/tactical aliases are gone on both sides.
  const expert: EffectiveModels = {
    model: {model: 'gpt-5.5', source: 'expert'},
    subagent: {model: 'gpt-5.5', source: 'expert'},
    session: {model: 'gpt-5.5', source: 'expert'},
  };
  const framework: EffectiveModels = {
    model: {model: 'gemma-4-31b', source: 'system_default'},
    subagent: {model: 'gemma-4-31b', source: 'system_default'},
    session: {model: 'gemma-4-31b', source: 'system_default'},
  };

  it('prefers the selected expert resolution when present', () => {
    expect(resolveEffectiveModels(expert, framework)).toBe(expert);
  });

  it('falls back to the framework default resolution when no expert is selected', () => {
    // Regression: the no-expert create path. Without the fallback the picker's
    // "Default" option drops to the config-literal llm.model (the hardcoded YAML
    // placeholder, RedHatAI/gemma-4-31B-it-FP8-Dynamic) instead of the resolved
    // system chat pin. null (no expert) and undefined (older API) both fall back.
    expect(resolveEffectiveModels(null, framework)).toBe(framework);
    expect(resolveEffectiveModels(undefined, framework)).toBe(framework);
  });

  it('returns null when neither expert nor framework resolution is available', () => {
    expect(resolveEffectiveModels(null, null)).toBeNull();
  });
});

describe('defaultModelOptionLabel', () => {
  it('appends the resolved model to the inherit-marker prefix', () => {
    expect(defaultModelOptionLabel('Base default', 'gemma-4-31b')).toBe('Base default · gemma-4-31b');
    expect(defaultModelOptionLabel('Project default', 'gpt-5.5')).toBe('Project default · gpt-5.5');
  });

  it('shows the bare prefix when no model is resolved yet (null/undefined/empty)', () => {
    // The picker hasn't loaded the framework default yet, or there is no catalog
    // row — fall back to the plain marker instead of a dangling separator.
    expect(defaultModelOptionLabel('Base default', null)).toBe('Base default');
    expect(defaultModelOptionLabel('Base default', undefined)).toBe('Base default');
    expect(defaultModelOptionLabel('Project default', '')).toBe('Project default');
  });
});

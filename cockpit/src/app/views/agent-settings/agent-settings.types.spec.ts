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

describe('detectModelFamily — Kimi K3', () => {
  it.each(['', 'moonshotai/', 'openrouter/moonshotai/'])(
    'claims K3 but leaves K2.x on default with prefix %s', (prefix) => {
      expect(detectModelFamily(`${prefix}kimi-k3`)).toBe('kimi-k3');
      expect(detectModelFamily(`${prefix}Kimi-K3`)).toBe('kimi-k3');
      expect(detectModelFamily(`${prefix}kimi-k3:batch`)).toBe('kimi-k3');
      expect(detectModelFamily(`${prefix}kimi-k3[1m]`)).toBe('kimi-k3');
      expect(detectModelFamily(`${prefix}kimi-k2.6`)).toBe('default');
      expect(detectModelFamily(`${prefix}kimi-k2.7-code`)).toBe('default');
      expect(detectModelFamily(`${prefix}kimi-k30`)).toBe('default');
      expect(detectModelFamily(`${prefix}kimi-k3.5`)).toBe('default');
    },
  );
});

describe('detectModelFamily — Qwen3.8-27B', () => {
  it('claims the 27B but not Qwen3 8B or the Qwen3.8 API rows', () => {
    for (const id of ['Qwen/Qwen3.8-27B', 'Qwen/Qwen3.8-27B-FP8', 'qwen/qwen3.8-27b:free', 'openrouter/qwen/qwen3.8-27b']) {
      expect(detectModelFamily(id)).toBe('qwen3.8-27b');
    }
    expect(detectModelFamily('qwen/qwen3-8b')).toBe('qwen');
    expect(detectModelFamily('qwen/qwen3.8-max-0902')).toBe('qwen3.8-max');
    expect(detectModelFamily('qwen3.8-270b')).toBe('qwen');
  });
});

describe('detectModelFamily — Qwen3.8 Max', () => {
  it('claims Max and Max Prime but not the text-only open 2.4T or Flash', () => {
    for (const id of ['qwen3.8-max', 'qwen/qwen3.8-max-0902', 'openrouter/qwen/qwen3.8-max-prime']) {
      expect(detectModelFamily(id)).toBe('qwen3.8-max');
    }
    expect(detectModelFamily('qwen/qwen3.8-2.4t-a95b')).toBe('qwen');
    expect(detectModelFamily('qwen/qwen3.8-flash')).toBe('qwen');
    expect(detectModelFamily('qwen3.8-maximal')).toBe('qwen');
  });
});

describe('detectModelFamily — Grok 4.7', () => {
  it('claims 4.7 but leaves older Grok rows on default', () => {
    for (const id of ['grok-4.7', 'x-ai/grok-4.7-20260916', 'openrouter/x-ai/grok-4.7', 'grok-4.7-fast']) {
      expect(detectModelFamily(id)).toBe('grok-4.7');
    }
    for (const id of ['grok-4.6', 'grok-4.3', 'grok-build-0.1', 'grok-4.70', 'grok-4.7.1']) {
      expect(detectModelFamily(id)).toBe('default');
    }
  });
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

describe('detectModelFamily — Claude Opus 5.5', () => {
  it('maps Opus 5.5 IDs to their own family in both spellings', () => {
    expect(detectModelFamily('claude-opus-5-5')).toBe('claude-opus-5-5');
    expect(detectModelFamily('claude-opus-5-5-20260922')).toBe('claude-opus-5-5');
    expect(detectModelFamily('openrouter/anthropic/claude-opus-5.5')).toBe(
      'claude-opus-5-5',
    );
  });

  it('keeps dated Opus 5 snapshots on claude-opus-5', () => {
    expect(detectModelFamily('claude-opus-5-20260401')).toBe('claude-opus-5');
  });
});

describe('detectModelFamily — Claude Sonnet 5', () => {
  it('maps Sonnet 5 and 5.5 to one family and keeps 4.x generic', () => {
    expect(detectModelFamily('claude-sonnet-5')).toBe('claude-sonnet-5');
    expect(detectModelFamily('claude-sonnet-5-5')).toBe('claude-sonnet-5');
    expect(detectModelFamily('claude-sonnet-5-5-20260928')).toBe('claude-sonnet-5');
    expect(detectModelFamily('openrouter/anthropic/claude-sonnet-5.5')).toBe('claude-sonnet-5');
    expect(detectModelFamily('claude-sonnet-4-6')).toBe('claude-sonnet');
    expect(detectModelFamily('claude-sonnet-4-5-20250929')).toBe('claude-sonnet');
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

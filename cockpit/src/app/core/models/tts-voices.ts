/**
 * Curated TTS voice catalogs for the Settings → Models → Providers "Add model" voice
 * dropdown.
 *
 * There is no standard "list voices" endpoint in the OpenAI-compatible spec,
 * and our model-orchestrator gateway doesn't proxy Kokoro's
 * `/v1/audio/voices` — so, exactly like the hardcoded Whisper language list,
 * these are snapshots of what each backend actually offers, keyed by a backend
 * detected from the model id. Unrecognized backends fall back to free-text.
 *
 * Kokoro list snapshotted 2026-06-06 from the live kokoro-fastapi
 * `/v1/audio/voices` (67 voices). OpenAI list is the documented set for
 * `tts-1` / `gpt-4o-mini-tts` (OpenAI exposes no list endpoint).
 */

/** Sentinel select value meaning "let me type a voice id by hand". */
export const TTS_VOICE_CUSTOM = '__custom__';

/**
 * Kokoro-82M voices (kokoro-fastapi). Prefix = language + gender:
 * `a*`=American EN, `b*`=British EN, `e*`=Spanish, `f*`=French, `h*`=Hindi,
 * `i*`=Italian, `j*`=Japanese, `p*`=Portuguese, `z*`=Chinese; `*f`=female,
 * `*m`=male; `*_v0*` are legacy variants. (No German voice exists.)
 */
export const KOKORO_VOICES: readonly string[] = [
  'af_alloy', 'af_aoede', 'af_bella', 'af_heart', 'af_jadzia', 'af_jessica',
  'af_kore', 'af_nicole', 'af_nova', 'af_river', 'af_sarah', 'af_sky',
  'af_v0', 'af_v0bella', 'af_v0irulan', 'af_v0nicole', 'af_v0sarah', 'af_v0sky',
  'am_adam', 'am_echo', 'am_eric', 'am_fenrir', 'am_liam', 'am_michael',
  'am_onyx', 'am_puck', 'am_santa', 'am_v0adam', 'am_v0gurney', 'am_v0michael',
  'bf_alice', 'bf_emma', 'bf_lily', 'bf_v0emma', 'bf_v0isabella',
  'bm_daniel', 'bm_fable', 'bm_george', 'bm_lewis', 'bm_v0george', 'bm_v0lewis',
  'ef_dora', 'em_alex', 'em_santa', 'ff_siwis',
  'hf_alpha', 'hf_beta', 'hm_omega', 'hm_psi', 'if_sara', 'im_nicola',
  'jf_alpha', 'jf_gongitsune', 'jf_nezumi', 'jf_tebukuro', 'jm_kumo',
  'pf_dora', 'pm_alex', 'pm_santa',
  'zf_xiaobei', 'zf_xiaoni', 'zf_xiaoxiao', 'zf_xiaoyi',
  'zm_yunjian', 'zm_yunxi', 'zm_yunxia', 'zm_yunyang',
];

/**
 * OpenAI TTS voices (`tts-1`, `tts-1-hd`, `gpt-4o-mini-tts`). Documented
 * constants — OpenAI has no voices endpoint.
 */
export const OPENAI_TTS_VOICES: readonly string[] = [
  'alloy', 'ash', 'ballad', 'coral', 'echo', 'fable',
  'nova', 'onyx', 'sage', 'shimmer', 'verse',
];

/** A recognized TTS backend, or `null` when the model id matches none. */
export type TtsBackend = 'kokoro' | 'openai' | 'elevenlabs' | null;

/**
 * The backend behind a TTS model id, detected from the id. The single source
 * of backend detection — `voicesForModelId` and `voiceLanguageTag` both
 * dispatch on it, so a new backend is added in exactly one place.
 */
export function ttsBackendForModelId(
  modelId: string | null | undefined,
): TtsBackend {
  const id = (modelId ?? '').toLowerCase();
  if (!id) return null;
  if (id.includes('kokoro')) return 'kokoro';
  if (id.includes('tts-1') || id.includes('gpt-4o-mini-tts')) return 'openai';
  // ElevenLabs model ids are `eleven_*` (eleven_multilingual_v2, eleven_v3, …).
  // Its voices are NOT a static catalog — they come live from the deployment
  // account (`GET /api/settings/tts/voices`), so `voicesForModelId` returns []
  // and Settings renders the server-fed picker instead.
  if (id.includes('eleven')) return 'elevenlabs';
  return null;
}

/**
 * One account voice from `GET /api/settings/tts/voices` (server-proxied from
 * ElevenLabs' `/v2/voices`). `labels` carries ElevenLabs' own metadata
 * (`accent`, `gender`, `age`, `description`); `preview_url` is a public CDN
 * mp3 the browser can hotlink to audition without spending characters.
 */
export interface TtsAccountVoice {
  id: string;
  name: string;
  labels: Record<string, string>;
  preview_url: string | null;
}

/** Response of `GET /api/settings/tts/voices`. `voices` is only populated for
 * the `elevenlabs` backend; static-catalog backends return an empty list. */
export interface TtsVoicesResponse {
  backend: TtsBackend;
  voices: TtsAccountVoice[];
}

/**
 * One community Voice Library result from `GET /api/settings/tts/library`
 * (server-proxied from ElevenLabs' `/v1/shared-voices`). Unlike account voices,
 * the library exposes accent/gender/age/language as flat fields; the pair
 * `(public_owner_id, id)` is what "Add to deployment" needs to copy the voice
 * into the account. `free` marks voices free-tier keys can synthesize.
 */
export interface TtsLibraryVoice {
  id: string;
  public_owner_id: string;
  name: string;
  accent: string | null;
  gender: string | null;
  age: string | null;
  language: string | null;
  description: string | null;
  preview_url: string | null;
  free: boolean;
}

/** Optional filters for a Voice Library search — all passed through to
 * ElevenLabs. `search` alone handles the "french english" case. */
export interface TtsLibraryFilters {
  search?: string;
  language?: string;
  accent?: string;
  gender?: string;
  age?: string;
  page?: number;
}

/** Response of `GET /api/settings/tts/library`. `add_enabled` mirrors the
 * `tts.elevenlabs_library_enabled` admin flag — the browser offers "Add to
 * deployment" only when true. `error` carries a readable failure line (the
 * search endpoint never 5xxes). */
export interface TtsLibraryResponse {
  backend: TtsBackend;
  voices: TtsLibraryVoice[];
  has_more: boolean;
  error: string | null;
  add_enabled: boolean;
}

/** Response of the admin `GET|PUT /api/admin/system-settings/tts_library`
 * endpoints — the Voice Library add gate (default off). */
export interface TtsLibrarySetting {
  enabled: boolean;
  updated_at: string | null;
  updated_by: string | null;
}

/**
 * Voices offered by the backend behind `modelId`. Returns `[]` for
 * unrecognized backends — the caller then shows a free-text field instead of
 * a dropdown.
 */
export function voicesForModelId(
  modelId: string | null | undefined,
): readonly string[] {
  switch (ttsBackendForModelId(modelId)) {
    case 'kokoro':
      return KOKORO_VOICES;
    case 'openai':
      return OPENAI_TTS_VOICES;
    default:
      return [];
  }
}

/**
 * Kokoro encodes a voice's language in the FIRST letter of its id
 * (`<lang><gender>_<name>`; second letter is gender). This map is
 * Kokoro-SPECIFIC — its misaki G2P scheme, NOT ISO 639 (note the single
 * letters, and English split into American `a` / British `b`) — so it stays
 * hidden behind `voiceLanguageTag` and never leaks into the UI.
 */
const KOKORO_LANG_BY_PREFIX: Readonly<Record<string, string>> = {
  a: 'EN-US',
  b: 'EN-GB',
  e: 'ES',
  f: 'FR',
  h: 'HI',
  i: 'IT',
  j: 'JA',
  p: 'PT',
  z: 'ZH',
};

/**
 * A short language tag for a voice (e.g. `'EN-US'`, `'ES'`, `'multi'`), or
 * `null` when it can't be derived. Backend-dispatched exactly like
 * `voicesForModelId`, so callers stay backend-agnostic:
 *   - Kokoro: decoded from the id prefix (the only place the Kokoro-ism lives).
 *   - OpenAI: `'multi'` — the voices are multilingual, not per-language.
 *   - Unrecognized backend: `null` — no tag, no claim.
 * Swapping in a vendor backend later (ElevenLabs/Hume/MiniMax) means adding a
 * case here that reads their API language metadata; the UI is unchanged.
 */
export function voiceLanguageTag(
  modelId: string | null | undefined,
  voiceId: string | null | undefined,
): string | null {
  const voice = (voiceId ?? '').toLowerCase();
  if (!voice) return null;
  switch (ttsBackendForModelId(modelId)) {
    case 'kokoro':
      return KOKORO_LANG_BY_PREFIX[voice[0]] ?? null;
    case 'openai':
      return 'multi';
    case 'elevenlabs':
      // Language/accent isn't decodable from the opaque voice_id — it comes
      // from the server `labels` metadata, which Settings renders directly on
      // each option. So no derived tag here, by design.
      return null;
    default:
      return null;
  }
}

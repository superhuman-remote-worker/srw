"""Phone conditions applied to a caller clip after it was rendered.

Every operation takes and returns 16 kHz mono 16-bit audio (a numpy int16
array), which is what SRW's gateway receives from Asterisk (``slin16``):

- ``codec``: G.711 A-law or µ-law (8 kHz, in numpy), G.722 (16 kHz) and
  AMR-NB (8 kHz, the GSM/UMTS mobile codec) through ffmpeg;
- ``loss``: 20 ms packets lost in bursts (a Gilbert-Elliott model), replaced
  by silence (``plc: zero``) or a fading repeat of the last packet;
- ``noise``: street, café or office background generated here (no recorded
  noise, so no licence question), mixed at a signal-to-noise ratio measured
  on the caller's active speech; ``cafe`` can mix real synthetic speech as
  babble instead of the generated murmur;
- ``level`` (gain in dB), ``clip`` (gain, then hard clipping) and ``band``
  (a band limit, as a cheap handset or a mobile leg has).

Random parts are seeded from the condition and the clip, so a clip under a
condition is the same on every run.
"""

from __future__ import annotations

import shutil
import subprocess
import zlib
from collections.abc import Sequence

import numpy as np

from tests.voice_bench.evalset.spec import Condition, Op

RATE = 16_000
FRAME_MS = 20
#: Level the noise is scaled against when a clip has no speech (silence items).
SILENT_REFERENCE_DBFS = -26.0


class ConditionError(RuntimeError):
    pass


# -- conversions -------------------------------------------------------------------


def to_float(pcm: np.ndarray) -> np.ndarray:
    return np.asarray(pcm, dtype=np.float64) / 32768.0


def to_int16(signal: np.ndarray) -> np.ndarray:
    return np.clip(np.round(signal * 32768.0), -32768, 32767).astype(np.int16)


def from_bytes(data: bytes) -> np.ndarray:
    return np.frombuffer(data, dtype="<i2").astype(np.int16)


def resample(signal: np.ndarray, src: int, dst: int) -> np.ndarray:
    """Band-limited resampling by the FFT (integer ratios of 8 and 16 kHz)."""
    if src == dst or len(signal) == 0:
        return signal.astype(np.float64)
    n_out = int(round(len(signal) * dst / src))
    spectrum = np.fft.rfft(signal)
    bins = n_out // 2 + 1
    if bins <= len(spectrum):
        spectrum = spectrum[:bins].copy()
        # Taper the top 5 % of the band so the cut does not ring.
        taper = max(1, bins // 20)
        spectrum[-taper:] *= np.linspace(1.0, 0.0, taper)
    else:
        spectrum = np.concatenate([spectrum, np.zeros(bins - len(spectrum), complex)])
    return np.fft.irfft(spectrum, n_out) * (n_out / len(signal))


def band_limit(
    signal: np.ndarray, low: float, high: float, rate: int = RATE
) -> np.ndarray:
    """Zero-phase 4th-order Butterworth-shaped band pass (magnitude only)."""
    if len(signal) == 0:
        return signal
    freqs = np.fft.rfftfreq(len(signal), 1 / rate)
    gain = np.ones_like(freqs)
    with np.errstate(divide="ignore"):
        if low > 0:
            gain *= 1 / np.sqrt(1 + (low / np.maximum(freqs, 1e-9)) ** 8)
        if high < rate / 2:
            gain *= 1 / np.sqrt(1 + (freqs / high) ** 8)
    return np.fft.irfft(np.fft.rfft(signal) * gain, len(signal))


# -- G.711 (ITU-T G.711, as the classic Sun reference code does it) ---------------------------

_U_SEG_END = np.array([0x3F, 0x7F, 0xFF, 0x1FF, 0x3FF, 0x7FF, 0xFFF, 0x1FFF])


def ulaw_encode(pcm: np.ndarray) -> np.ndarray:
    x = np.asarray(pcm, dtype=np.int32) >> 2  # 14-bit
    mask = np.where(x < 0, 0x7F, 0xFF)
    mag = np.minimum(np.abs(x), 8159) + 0x21
    seg = np.searchsorted(_U_SEG_END, mag, side="left")
    uval = (seg << 4) | ((mag >> (seg + 1)) & 0x0F)
    uval = np.where(seg >= 8, 0x7F, uval)
    return ((uval ^ mask) & 0xFF).astype(np.uint8)


def ulaw_decode(codes: np.ndarray) -> np.ndarray:
    u = (~np.asarray(codes, dtype=np.int32)) & 0xFF
    exponent = (u >> 4) & 0x07
    mantissa = u & 0x0F
    mag = (((mantissa << 3) + 0x84) << exponent) - 0x84
    return np.where(u & 0x80, -mag, mag).astype(np.int16)


_A_SEG_END = np.array([0x1F, 0x3F, 0x7F, 0xFF, 0x1FF, 0x3FF, 0x7FF, 0xFFF])


def alaw_encode(pcm: np.ndarray) -> np.ndarray:
    x = np.asarray(pcm, dtype=np.int32) >> 3  # 13-bit
    mask = np.where(x >= 0, 0xD5, 0x55)
    mag = np.where(x >= 0, x, -x - 1)
    seg = np.searchsorted(_A_SEG_END, mag, side="left")
    shift = np.where(seg < 2, 1, seg)
    aval = (seg << 4) | ((mag >> shift) & 0x0F)
    aval = np.where(seg >= 8, 0x7F, aval)
    return ((aval ^ mask) & 0xFF).astype(np.uint8)


def alaw_decode(codes: np.ndarray) -> np.ndarray:
    a = np.asarray(codes, dtype=np.int32) ^ 0x55
    t = (a & 0x0F) << 4
    seg = (a & 0x70) >> 4
    t = np.where(seg == 0, t + 8, t + 0x108)
    t = np.where(seg > 1, t << np.maximum(seg - 1, 0), t)
    return np.where(a & 0x80, t, -t).astype(np.int16)


def g711(signal16k: np.ndarray, law: str) -> np.ndarray:
    narrow = to_int16(resample(signal16k, RATE, 8000))
    if law == "g711a":
        narrow = alaw_decode(alaw_encode(narrow))
    else:
        narrow = ulaw_decode(ulaw_encode(narrow))
    return resample(to_float(narrow), 8000, RATE)


# -- ffmpeg codecs ----------------------------------------------------------------------


def ffmpeg_available() -> bool:
    return shutil.which("ffmpeg") is not None


def _ffmpeg(args: list[str], data: bytes) -> bytes:
    if not ffmpeg_available():
        raise ConditionError("ffmpeg is needed for G.722 and AMR-NB")
    proc = subprocess.run(
        ["ffmpeg", "-hide_banner", "-loglevel", "error", *args],
        input=data,
        capture_output=True,
        check=False,
        timeout=120,
    )
    if proc.returncode != 0:
        raise ConditionError(proc.stderr.decode(errors="replace").strip()[:300])
    return proc.stdout


def ffmpeg_codec(
    signal16k: np.ndarray, codec: str, bitrate: str | None = None
) -> np.ndarray:
    """Encode and decode through ffmpeg: ``g722`` (16 kHz) or ``amr-nb`` (8 kHz)."""
    if codec == "g722":
        rate, enc, fmt = RATE, ["-c:a", "g722"], "g722"
    elif codec == "amr-nb":
        rate, fmt = 8000, "amr"
        enc = ["-c:a", "libopencore_amrnb", "-b:a", bitrate or "7.95k"]
    else:
        raise ConditionError(f"ffmpeg_codec does not do {codec}")
    pcm = to_int16(resample(signal16k, RATE, rate)).tobytes()
    raw_in = ["-f", "s16le", "-ar", str(rate), "-ac", "1"]
    encoded = _ffmpeg([*raw_in, "-i", "pipe:0", *enc, "-f", fmt, "pipe:1"], pcm)
    decoded = _ffmpeg(["-f", fmt, "-i", "pipe:0", *raw_in, "pipe:1"], encoded)
    out = to_float(from_bytes(decoded))
    out = resample(out, rate, RATE)
    return _fit(out, len(signal16k))


def _fit(signal: np.ndarray, length: int) -> np.ndarray:
    if len(signal) >= length:
        return signal[:length]
    return np.concatenate([signal, np.zeros(length - len(signal))])


# -- packet loss -------------------------------------------------------------------------


def loss_pattern(
    frames: int, rate: float, burst: float, rng: np.random.Generator
) -> np.ndarray:
    """Which 20 ms packets are lost: a two-state Gilbert-Elliott chain whose
    long-run loss is ``rate`` and whose lost runs last ``burst`` packets on
    average."""
    if rate <= 0:
        return np.zeros(frames, dtype=bool)
    to_good = 1.0 / burst
    to_bad = rate * to_good / (1.0 - rate)
    lost = np.zeros(frames, dtype=bool)
    bad = rng.random() < rate
    draws = rng.random(frames)
    for index in range(frames):
        bad = draws[index] >= to_good if bad else draws[index] < to_bad
        lost[index] = bad
    return lost


def packet_loss(
    signal: np.ndarray, rate: float, burst: float, plc: str, rng: np.random.Generator
) -> tuple[np.ndarray, float]:
    """Drop packets; returns the audio and the share actually lost."""
    size = RATE * FRAME_MS // 1000
    frames = len(signal) // size
    lost = loss_pattern(frames, rate, burst, rng)
    out = signal.copy()
    previous = np.zeros(size)
    fade = 1.0
    for index in range(frames):
        piece = slice(index * size, (index + 1) * size)
        if lost[index]:
            if plc == "repeat":
                fade *= 0.6
                out[piece] = previous * fade
            else:
                out[piece] = 0.0
        else:
            previous, fade = signal[piece].copy(), 1.0
    return out, float(lost.mean()) if frames else 0.0


# -- noise -------------------------------------------------------------------------------


def colored(n: int, exponent: float, rng: np.random.Generator) -> np.ndarray:
    """Noise with a 1/f**exponent power spectrum (0 white, 1 pink, 2 brown)."""
    if n == 0:
        return np.zeros(0)
    spectrum = np.fft.rfft(rng.standard_normal(n))
    freqs = np.fft.rfftfreq(n, 1 / RATE)
    freqs[0] = freqs[1] if n > 1 else 1.0
    spectrum /= freqs ** (exponent / 2)
    out = np.fft.irfft(spectrum, n)
    return out / (np.sqrt(np.mean(out**2)) + 1e-12)


def _envelope(n: int, points: np.ndarray, values: np.ndarray) -> np.ndarray:
    return np.interp(np.arange(n), points, values)


def murmur(n: int, rng: np.random.Generator, talkers: int = 6) -> np.ndarray:
    """Speech-shaped murmur: band-limited noise with phrase and syllable
    rhythm, a few talkers at once (a café heard from a table away)."""
    out = np.zeros(n)
    t = np.arange(n) / RATE
    for _ in range(talkers):
        voice = band_limit(colored(n, 1.0, rng), 150, 4000)
        # Phrases of 1–3 s with pauses of 0.3–1 s.
        gate = np.zeros(n)
        at = rng.uniform(0, 1.0)
        while at < n / RATE:
            length = rng.uniform(1.0, 3.0)
            start, end = int(at * RATE), int(min(n / RATE, at + length) * RATE)
            gate[start:end] = 1.0
            at += length + rng.uniform(0.3, 1.0)
        gate = np.convolve(gate, np.ones(800) / 800, mode="same")
        rate = rng.uniform(3.5, 5.5)
        syllables = 0.35 + 0.65 * np.abs(
            np.sin(np.pi * rate * t + rng.uniform(0, np.pi))
        )
        out += voice * gate * syllables * rng.uniform(0.5, 1.0)
    return out


def clinks(n: int, rng: np.random.Generator, per_second: float = 0.4) -> np.ndarray:
    """Cups and cutlery: short decaying tones at random times."""
    out = np.zeros(n)
    count = rng.poisson(per_second * n / RATE)
    for _ in range(count):
        start = int(rng.uniform(0, max(1, n - 4000)))
        length = min(4000, n - start)
        tt = np.arange(length) / RATE
        freq = rng.uniform(2000, 6000)
        out[start : start + length] += (
            np.sin(2 * np.pi * freq * tt) * np.exp(-tt / rng.uniform(0.02, 0.06))
        ) * rng.uniform(1.0, 3.0)
    return out


def street(n: int, rng: np.random.Generator) -> np.ndarray:
    rumble = band_limit(colored(n, 2.0, rng), 20, 300)
    traffic = band_limit(colored(n, 1.0, rng), 80, 3000)
    # Cars passing: bumps of 2–5 s.
    swell = np.full(n, 0.35)
    t = np.arange(n) / RATE
    for _ in range(max(1, rng.poisson(0.25 * n / RATE))):
        centre = rng.uniform(0, n / RATE)
        width = rng.uniform(1.0, 2.5)
        swell += rng.uniform(0.8, 2.0) * np.exp(-(((t - centre) / width) ** 2))
    return rumble * 0.6 + traffic * swell


def office(n: int, rng: np.random.Generator) -> np.ndarray:
    hvac = band_limit(colored(n, 1.0, rng), 30, 600)
    hum = 0.15 * np.sin(2 * np.pi * 100 * np.arange(n) / RATE)
    keys = np.zeros(n)
    at = rng.uniform(0, 2)
    while at < n / RATE:
        burst_end = at + rng.uniform(1.5, 4.0)
        while at < min(burst_end, n / RATE):
            start = int(at * RATE)
            click = band_limit(rng.standard_normal(64), 1000, 7000)
            click *= np.exp(-np.arange(64) / 12)
            end = min(n, start + 64)
            keys[start:end] += click[: end - start] * rng.uniform(2.0, 5.0)
            at += rng.uniform(0.08, 0.25)
        at = burst_end + rng.uniform(1.0, 4.0)
    voices = murmur(n, rng, talkers=2) * 0.35
    return hvac * 0.7 + hum + keys + voices


def babble(
    n: int, sources: Sequence[np.ndarray], rng: np.random.Generator, talkers: int = 6
) -> np.ndarray:
    """Real (synthetic) speech from other clips, several talkers at once."""
    out = np.zeros(n)
    usable = [to_float(s) for s in sources if len(s) > RATE]
    if not usable:
        return murmur(n, rng, talkers)
    for _ in range(talkers):
        track = np.zeros(n)
        at = int(rng.uniform(0, RATE))
        while at < n:
            clip = usable[int(rng.integers(len(usable)))]
            end = min(n, at + len(clip))
            track[at:end] += clip[: end - at]
            at = end + int(rng.uniform(0.2, 0.8) * RATE)
        out += track * rng.uniform(0.6, 1.0)
    return band_limit(out, 100, 5000)


def noise(
    kind: str, n: int, rng: np.random.Generator, sources: Sequence[np.ndarray] = ()
) -> np.ndarray:
    if kind == "street":
        return street(n, rng)
    if kind == "cafe":
        speech = babble(n, sources, rng) if sources else murmur(n, rng)
        return speech + 0.3 * colored(n, 1.0, rng) + clinks(n, rng)
    if kind == "office":
        return office(n, rng)
    if kind == "white":
        return rng.standard_normal(n)
    raise ConditionError(f"unknown noise {kind}")


def active_level(signal: np.ndarray, floor_db: float = 35.0) -> float | None:
    """RMS of the speech frames (within ``floor_db`` of the loudest frame)."""
    size = RATE * FRAME_MS // 1000
    frames = len(signal) // size
    if frames == 0:
        return None
    power = np.mean(signal[: frames * size].reshape(frames, size) ** 2, axis=1)
    if power.max() <= 1e-10:
        return None
    active = power[power >= power.max() * 10 ** (-floor_db / 10)]
    return float(np.sqrt(active.mean()))


def mix_at_snr(signal: np.ndarray, background: np.ndarray, snr_db: float) -> np.ndarray:
    level = active_level(signal)
    if level is None:
        level = 10 ** (SILENT_REFERENCE_DBFS / 20)
    rms = np.sqrt(np.mean(background**2)) + 1e-12
    scale = level / rms * 10 ** (-snr_db / 20)
    return signal + background * scale


# -- applying a condition -----------------------------------------------------------------


def clip_seed(condition_id: str, clip_id: str, extra: int = 0) -> int:
    return zlib.crc32(f"{condition_id}/{clip_id}/{extra}".encode())


def apply_op(
    signal: np.ndarray,
    op: Op,
    rng: np.random.Generator,
    sources: Sequence[np.ndarray] = (),
    info: dict | None = None,
) -> np.ndarray:
    p = op.params
    if op.op == "level":
        return signal * 10 ** (p["db"] / 20)
    if op.op == "clip":
        loud = signal * 10 ** (p["gain_db"] / 20)
        if info is not None:
            info["clipped_share"] = round(float(np.mean(np.abs(loud) >= 0.999)), 4)
        return np.clip(loud, -0.999, 0.999)
    if op.op == "band":
        return band_limit(signal, p["low"], p["high"])
    if op.op == "noise":
        background = noise(p["kind"], len(signal), rng, sources)
        return mix_at_snr(signal, background, p["snr_db"])
    if op.op == "codec":
        if p["name"] in ("g711a", "g711u"):
            return g711(signal, p["name"])
        return ffmpeg_codec(signal, p["name"], p.get("bitrate"))
    if op.op == "loss":
        out, share = packet_loss(
            signal, p["rate"], float(p.get("burst", 1.0)), p.get("plc", "zero"), rng
        )
        if info is not None:
            info["lost_share"] = round(share, 4)
        return out
    raise ConditionError(f"unknown op {op.op}")


def apply(
    pcm: np.ndarray,
    condition: Condition,
    clip_id: str,
    *,
    stage: str | None = None,
    sources: Sequence[np.ndarray] = (),
    info: dict | None = None,
) -> np.ndarray:
    """``pcm`` (16 kHz int16) under ``condition``; ``stage`` limits it to the
    ``acoustic`` or ``line`` operations. ``info`` collects what happened
    (lost share, clipped share)."""
    ops = [o for o in condition.ops if stage is None or o.stage == stage]
    signal = to_float(pcm)
    for index, op in enumerate(ops):
        seed = op.params.get("seed", 0)
        rng = np.random.default_rng(
            clip_seed(condition.id, clip_id, index + 7919 * seed)
        )
        signal = apply_op(signal, op, rng, sources, info)
    return to_int16(signal)

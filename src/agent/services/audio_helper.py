# src/services/audio_helper.py
"""
Audio Helper service for speech-to-text transcription.

Used when the agent encounters audio files (mp3, wav, m4a, etc.) and needs
to extract text content. Uses OpenAI's Whisper API (or compatible endpoint)
for transcription.

Large files (> 25 MB) are transparently chunked via ffmpeg, transcribed
segment by segment, and concatenated. The caller never sees the chunking.

Follows the same pattern as VisionHelper:
- Configurable via environment variables
- Sync wrappers for use with sync tool signatures
- Fire-and-forget archiving to the Postgres audit store
- Module-level singleton via get_audio_helper()
"""

import asyncio
import logging
import math
import os
import re
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from typing import List, Optional, Tuple

from openai import AsyncOpenAI

from shared.speech_transcript import extract_transcript as _extract_transcript

logger = logging.getLogger(__name__)

# Supported audio file extensions
AUDIO_EXTENSIONS = {
    ".mp3",
    ".wav",
    ".m4a",
    ".ogg",
    ".flac",
    ".webm",
    ".mp4",
    ".mpeg",
    ".mpga",
    ".oga",
    ".opus",
}

# Whisper API file size limit (25 MB)
MAX_AUDIO_SIZE_BYTES = 25 * 1024 * 1024

# Target chunk size with headroom (24 MB) to stay safely under the 25 MB limit
_TARGET_CHUNK_BYTES = 24 * 1024 * 1024

# Context carry-over: number of words from previous chunk to pass as prompt
_CONTEXT_CARRYOVER_WORDS = 50

# Default max line length for transcript line splitting
DEFAULT_MAX_LINE_LENGTH = 120


def _run_async(coro):
    """Run an async coroutine synchronously.

    Used to bridge async AudioHelper methods with sync tool signatures.
    Creates a new event loop if none exists (safe for tool context).
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor() as pool:
            future = pool.submit(asyncio.run, coro)
            return future.result()
    else:
        return asyncio.run(coro)


def split_transcript_into_lines(
    text: str,
    max_line_length: int = DEFAULT_MAX_LINE_LENGTH,
) -> List[str]:
    """Split a transcript into readable lines for line-numbered output.

    Splits on natural boundaries:
    1. Existing newlines from Whisper output
    2. Sentence boundaries (. ? !) for lines exceeding max_line_length
    3. Last space before max_line_length as final fallback

    Args:
        text: Raw transcript text
        max_line_length: Maximum characters per line (default: 120)

    Returns:
        List of lines (without trailing newlines)
    """
    if not text or not text.strip():
        return []

    # Split on existing newlines first
    raw_lines = text.split("\n")
    result = []

    for raw_line in raw_lines:
        raw_line = raw_line.strip()
        if not raw_line:
            continue

        if len(raw_line) <= max_line_length:
            result.append(raw_line)
            continue

        # Split long lines at sentence boundaries
        _split_long_line(raw_line, max_line_length, result)

    return result


def _split_long_line(text: str, max_length: int, out: List[str]) -> None:
    """Split a long line at sentence or word boundaries into out list."""
    while len(text) > max_length:
        # Try sentence boundary (. ? ! followed by space)
        best_pos = -1
        for match in re.finditer(r"[.!?]\s", text[:max_length]):
            best_pos = match.end()

        if best_pos > 0:
            out.append(text[:best_pos].strip())
            text = text[best_pos:].strip()
            continue

        # Fall back to last space before max_length
        space_pos = text.rfind(" ", 0, max_length)
        if space_pos > 0:
            out.append(text[:space_pos].strip())
            text = text[space_pos:].strip()
        else:
            # No space at all — hard break
            out.append(text[:max_length])
            text = text[max_length:].strip()

    if text:
        out.append(text)


class AudioHelper:
    """
    Helper service for audio transcription using a dedicated STT model.

    Uses OpenAI's Whisper API (or compatible endpoint) for speech-to-text.
    Files larger than 25 MB are automatically chunked via ffmpeg, transcribed
    segment by segment, and concatenated transparently.

    Configuration is via environment variables (aligned with .env.example):
    - WHISPER_API_KEY: API key for Whisper model (falls back to OPENAI_API_KEY)
    - WHISPER_BASE_URL: Base URL for Whisper API (defaults to OpenAI)
    - WHISPER_MODEL: Model to use (default: whisper-1)
    - WHISPER_TIMEOUT: Request timeout in seconds (default: 300)
    - WHISPER_LANGUAGE: Optional ISO-639-1 language hint (e.g., "en", "de")

    Example:
        ```python
        helper = AudioHelper()

        # Async usage
        transcript = await helper.transcribe(Path("recording.mp3"))

        # Sync usage (for tools)
        transcript = helper.transcribe_sync(Path("recording.mp3"))
        ```
    """

    OPENAI_API_URL = "https://api.openai.com/v1"

    def __init__(self):
        """Initialize the Audio Helper with configuration from environment."""
        from shared.runtime.core.transport_resolution import (
            is_openai_default_endpoint,
        )

        # OPENAI_API_KEY is the fallback only when the endpoint IS OpenAI (a
        # key follows its endpoint).
        self.api_base = os.getenv("WHISPER_BASE_URL", self.OPENAI_API_URL)
        primary_key = (
            os.getenv("OPENAI_API_KEY", "")
            if is_openai_default_endpoint(self.api_base)
            else ""
        )
        self.api_key = os.getenv("WHISPER_API_KEY", primary_key)
        self.model = os.getenv("WHISPER_MODEL", "whisper-1")
        self.timeout = float(os.getenv("WHISPER_TIMEOUT", "300"))
        self.default_language = os.getenv("WHISPER_LANGUAGE")

        if not self.api_key:
            logger.warning(
                "No WHISPER_API_KEY or OPENAI_API_KEY configured - transcription will fail"
            )

        self.client = AsyncOpenAI(
            api_key=self.api_key,
            base_url=self.api_base,
            timeout=self.timeout,
        )

        key_source = (
            "WHISPER_API_KEY" if os.getenv("WHISPER_API_KEY") else "OPENAI_API_KEY"
        )
        base_source = (
            "WHISPER_BASE_URL" if os.getenv("WHISPER_BASE_URL") else "default (OpenAI)"
        )
        logger.info(
            f"AudioHelper initialized: model={self.model}, "
            f"base_url={self.api_base} (from {base_source}), "
            f"api_key from {key_source}, timeout={self.timeout}s"
        )

    # =========================================================================
    # Public API
    # =========================================================================

    async def transcribe(
        self,
        file_path: Path,
        language: Optional[str] = None,
        prompt: Optional[str] = None,
        job_id: Optional[str] = None,
    ) -> str:
        """
        Transcribe an audio file to text.

        Files up to 25 MB are sent directly to the Whisper API. Larger files
        are automatically split into chunks via ffmpeg, transcribed individually
        with context carry-over, and concatenated.

        Args:
            file_path: Path to the audio file
            language: Optional ISO-639-1 language code (e.g., "en", "de")
                     to improve accuracy and speed
            prompt: Optional text to guide the model's style or continue
                   a previous segment
            job_id: Optional job ID for archiving the API call

        Returns:
            Transcribed text content
        """
        file_path = Path(file_path)

        if not file_path.exists():
            return f"[Error: audio file not found: {file_path}]"

        if file_path.suffix.lower() not in AUDIO_EXTENSIONS:
            return f"[Error: unsupported audio format: {file_path.suffix}]"

        file_size = file_path.stat().st_size

        if file_size <= MAX_AUDIO_SIZE_BYTES:
            # Small file — single API call, no ffmpeg needed
            return await self._transcribe_single_chunk(
                file_path=file_path,
                language=language,
                prompt=prompt,
                job_id=job_id,
            )

        # Large file — chunk via ffmpeg, transcribe each, concatenate
        return await self._transcribe_chunked(
            file_path=file_path,
            file_size=file_size,
            language=language,
            prompt=prompt,
            job_id=job_id,
        )

    def transcribe_sync(
        self,
        file_path: Path,
        language: Optional[str] = None,
        prompt: Optional[str] = None,
        job_id: Optional[str] = None,
    ) -> str:
        """Synchronous wrapper for transcribe.

        Use this in sync tool implementations.
        """
        return _run_async(self.transcribe(file_path, language, prompt, job_id))

    # =========================================================================
    # Chunked transcription
    # =========================================================================

    async def _transcribe_chunked(
        self,
        file_path: Path,
        file_size: int,
        language: Optional[str] = None,
        prompt: Optional[str] = None,
        job_id: Optional[str] = None,
    ) -> str:
        """Transcribe a large audio file by splitting into chunks.

        Uses ffmpeg to split the file into segments under 25 MB, transcribes
        each sequentially with context carry-over (last ~50 words of previous
        chunk passed as Whisper prompt), and concatenates results.
        """
        # Check ffmpeg availability
        if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
            return (
                "[Error: ffmpeg is required for audio files larger than 25 MB. "
                "Install with: apt-get install ffmpeg (Ubuntu) or dnf install ffmpeg (Fedora)]"
            )

        temp_dir = None
        try:
            chunk_paths, temp_dir = self._split_audio(file_path, file_size)
            total_chunks = len(chunk_paths)

            logger.info(
                f"Split {file_path.name} ({file_size / 1024 / 1024:.1f} MB) "
                f"into {total_chunks} chunks"
            )

            transcripts = []
            carry_over_prompt = prompt

            for i, chunk_path in enumerate(chunk_paths):
                text = await self._transcribe_single_chunk(
                    file_path=chunk_path,
                    language=language,
                    prompt=carry_over_prompt,
                    job_id=job_id,
                    chunk_index=i,
                    total_chunks=total_chunks,
                    original_file_name=file_path.name,
                )

                if text.startswith("[Error"):
                    # Log but continue with other chunks
                    logger.warning(f"Chunk {i + 1}/{total_chunks} failed: {text}")
                    transcripts.append(f"[chunk {i + 1} transcription failed]")
                else:
                    transcripts.append(text)
                    # Context carry-over: pass last N words as prompt for next chunk
                    words = text.split()
                    if len(words) > _CONTEXT_CARRYOVER_WORDS:
                        carry_over_prompt = " ".join(words[-_CONTEXT_CARRYOVER_WORDS:])
                    else:
                        carry_over_prompt = text

            full_transcript = " ".join(transcripts)
            logger.info(
                f"Chunked transcription complete for {file_path.name}: "
                f"{len(full_transcript)} chars from {total_chunks} chunks"
            )
            return full_transcript

        except Exception as e:
            logger.error(
                f"Error in chunked transcription of {file_path}: {e}", exc_info=True
            )
            return f"[Error transcribing audio: {str(e)}]"
        finally:
            if temp_dir and Path(temp_dir).exists():
                shutil.rmtree(temp_dir, ignore_errors=True)

    def _get_duration(self, file_path: Path) -> float:
        """Get audio duration in seconds via ffprobe.

        Args:
            file_path: Path to the audio file

        Returns:
            Duration in seconds

        Raises:
            RuntimeError: If ffprobe fails
        """
        try:
            result = subprocess.run(
                [
                    "ffprobe",
                    "-v",
                    "error",
                    "-show_entries",
                    "format=duration",
                    "-of",
                    "csv=p=0",
                    str(file_path),
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )

            if result.returncode != 0:
                raise RuntimeError(f"ffprobe failed: {result.stderr.strip()}")

            return float(result.stdout.strip())

        except subprocess.TimeoutExpired:
            raise RuntimeError(f"ffprobe timed out for {file_path}")
        except ValueError as e:
            raise RuntimeError(f"Could not parse duration from ffprobe output: {e}")

    def _split_audio(self, file_path: Path, file_size: int) -> Tuple[List[Path], str]:
        """Split an audio file into chunks under the Whisper API size limit.

        Uses ffmpeg stream copy (no re-encoding) for speed.

        Args:
            file_path: Path to the audio file
            file_size: File size in bytes

        Returns:
            Tuple of (list of chunk file paths, temp directory path)

        Raises:
            RuntimeError: If ffmpeg fails or duration cannot be determined
        """
        num_chunks = math.ceil(file_size / _TARGET_CHUNK_BYTES)
        duration = self._get_duration(file_path)
        segment_duration = duration / num_chunks

        temp_dir = tempfile.mkdtemp(prefix="audio_chunk_")
        suffix = file_path.suffix

        chunk_paths = []
        for i in range(num_chunks):
            start_time = i * segment_duration
            chunk_path = Path(temp_dir) / f"chunk_{i:04d}{suffix}"

            try:
                result = subprocess.run(
                    [
                        "ffmpeg",
                        "-y",
                        "-i",
                        str(file_path),
                        "-ss",
                        str(start_time),
                        "-t",
                        str(segment_duration),
                        "-c",
                        "copy",
                        str(chunk_path),
                    ],
                    capture_output=True,
                    text=True,
                    timeout=120,
                )

                if result.returncode != 0:
                    logger.warning(f"ffmpeg chunk {i} failed: {result.stderr.strip()}")
                    continue

                if chunk_path.exists() and chunk_path.stat().st_size > 0:
                    chunk_paths.append(chunk_path)
                else:
                    logger.warning(f"Chunk {i} produced empty file, skipping")

            except subprocess.TimeoutExpired:
                logger.warning(f"ffmpeg timed out for chunk {i}")
                continue

        if not chunk_paths:
            shutil.rmtree(temp_dir, ignore_errors=True)
            raise RuntimeError("ffmpeg produced no valid chunks")

        return chunk_paths, temp_dir

    # =========================================================================
    # Single chunk transcription
    # =========================================================================

    async def _transcribe_single_chunk(
        self,
        file_path: Path,
        language: Optional[str] = None,
        prompt: Optional[str] = None,
        job_id: Optional[str] = None,
        chunk_index: Optional[int] = None,
        total_chunks: Optional[int] = None,
        original_file_name: Optional[str] = None,
    ) -> str:
        """Transcribe a single audio file (must be ≤ 25 MB).

        This is the core transcription method. Called directly for small files
        or once per chunk for large files.

        Args:
            file_path: Path to audio file (or chunk)
            language: Optional language hint
            prompt: Optional context prompt (used for carry-over between chunks)
            job_id: Optional job ID for archiving
            chunk_index: Chunk number (0-indexed) if part of chunked transcription
            total_chunks: Total number of chunks if part of chunked transcription
            original_file_name: Original file name for archive metadata (for chunks)

        Returns:
            Transcribed text
        """
        try:
            start = time.monotonic()

            kwargs = {"model": self.model}
            effective_language = language or self.default_language
            if effective_language:
                kwargs["language"] = effective_language
            if prompt:
                kwargs["prompt"] = prompt

            with open(file_path, "rb") as audio_file:
                transcript = await self.client.audio.transcriptions.create(
                    file=audio_file,
                    **kwargs,
                )

            latency_ms = int((time.monotonic() - start) * 1000)

            # Endpoints vary: a Transcription object (default JSON format), a
            # bare string, or a `{"text": ...}` blob from non-compliant servers.
            # Normalize all of them so raw JSON never leaks into the transcript.
            text = _extract_transcript(transcript)

            # Archive with chunk metadata if applicable
            archive_file_name = original_file_name or file_path.name
            archive_metadata_extra = {}
            if chunk_index is not None:
                archive_metadata_extra["chunk_index"] = chunk_index
                archive_metadata_extra["total_chunks"] = total_chunks

            self._archive_transcription_call(
                job_id=job_id,
                file_name=archive_file_name,
                file_size=file_path.stat().st_size,
                language=language,
                transcript_text=text,
                latency_ms=latency_ms,
                extra_metadata=archive_metadata_extra,
            )

            chunk_info = (
                f" (chunk {chunk_index + 1}/{total_chunks})"
                if chunk_index is not None
                else ""
            )
            logger.info(
                f"Transcribed {archive_file_name}{chunk_info}: "
                f"{len(text)} chars in {latency_ms}ms"
            )
            return text

        except Exception as e:
            logger.error(f"Error transcribing {file_path}: {e}", exc_info=True)
            return f"[Error transcribing audio: {str(e)}]"

    # =========================================================================
    # Archiving
    # =========================================================================

    def _archive_transcription_call(
        self,
        job_id: Optional[str],
        file_name: str,
        file_size: int,
        language: Optional[str],
        transcript_text: str,
        latency_ms: int,
        extra_metadata: Optional[dict] = None,
    ) -> None:
        """Archive a transcription API call. Fire-and-forget — never raises."""
        if not job_id:
            return

        try:
            from agent.core.archiver import get_archiver
            from langchain_core.messages import HumanMessage as _HM, AIMessage as _AI

            archiver = get_archiver()
            if not archiver:
                return

            human_content = (
                f"Transcribe audio file: {file_name}\n"
                f"[audio: {file_size:,} bytes"
                f"{', language=' + language if language else ''}]"
            )
            messages = [_HM(content=human_content)]
            response = _AI(content=transcript_text)

            aux_meta = {
                "trigger": "transcription",
                "file_name": file_name,
                "file_size": file_size,
                "transcript_length": len(transcript_text),
                "language": language,
            }
            if extra_metadata:
                aux_meta.update(extra_metadata)

            archiver.archive(
                job_id=job_id,
                agent_type="transcription",
                messages=messages,
                response=response,
                model=self.model,
                latency_ms=latency_ms,
                call_type="transcription",
                auxiliary_metadata=aux_meta,
            )
        except Exception as e:
            logger.warning(f"Failed to archive transcription call: {e}")


# Module-level singleton instance (lazy-loaded)
_audio_helper: Optional[AudioHelper] = None


def get_audio_helper() -> AudioHelper:
    """Get or create the AudioHelper singleton instance.

    Use this to get a shared AudioHelper instance rather than
    creating new instances.

    Returns:
        Shared AudioHelper instance
    """
    global _audio_helper
    if _audio_helper is None:
        _audio_helper = AudioHelper()
    return _audio_helper

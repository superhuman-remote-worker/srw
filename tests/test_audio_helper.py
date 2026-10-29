"""Unit tests for AudioHelper — speech-to-text transcription service.

Validates:
- Initialization from environment variables (WHISPER_* with OPENAI fallback)
- Singleton pattern via get_audio_helper()
- Transcription via AsyncOpenAI Whisper API (mocked)
- File validation (existence, format, size)
- Chunked transcription for large files (> 25 MB)
- Context carry-over between chunks
- Transcript line splitting for paged output
- ffprobe duration detection and ffmpeg splitting
- Archiving to the Postgres audit store via LLMArchiver
- Sync wrapper bridging async to sync
"""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agent.services.audio_helper import (
    AudioHelper,
    AUDIO_EXTENSIONS,
    MAX_AUDIO_SIZE_BYTES,
    split_transcript_into_lines,
)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mock_env(monkeypatch):
    """Set up minimal environment for AudioHelper."""
    monkeypatch.setenv("OPENAI_API_KEY", "test-key-123")
    monkeypatch.delenv("WHISPER_API_KEY", raising=False)
    monkeypatch.delenv("WHISPER_BASE_URL", raising=False)
    monkeypatch.delenv("WHISPER_MODEL", raising=False)


@pytest.fixture
def mock_openai_client():
    """Mock AsyncOpenAI client so no real API calls are made."""
    with patch("agent.services.audio_helper.AsyncOpenAI") as mock_cls:
        mock_client = MagicMock()
        mock_cls.return_value = mock_client
        yield mock_client, mock_cls


@pytest.fixture
def audio_helper(mock_env, mock_openai_client):
    """Create an AudioHelper with mocked client."""
    return AudioHelper()


@pytest.fixture
def tmp_audio(tmp_path):
    """Create a temporary .mp3 file with dummy content."""
    audio_file = tmp_path / "test.mp3"
    audio_file.write_bytes(b"\xff\xfb\x90\x00" * 100)  # Fake MP3 header
    return audio_file


# =============================================================================
# Initialization
# =============================================================================


class TestAudioHelperInit:
    """Test AudioHelper initialization from environment."""

    def test_init_with_openai_key(self, mock_env, mock_openai_client):
        """Uses OPENAI_API_KEY when WHISPER_API_KEY not set."""
        helper = AudioHelper()
        assert helper.api_key == "test-key-123"
        assert helper.model == "whisper-1"
        assert helper.api_base == "https://api.openai.com/v1"

    def test_init_with_whisper_key(self, monkeypatch, mock_openai_client):
        """Prefers WHISPER_API_KEY over OPENAI_API_KEY."""
        monkeypatch.setenv("OPENAI_API_KEY", "openai-key")
        monkeypatch.setenv("WHISPER_API_KEY", "whisper-key")

        helper = AudioHelper()
        assert helper.api_key == "whisper-key"

    def test_init_custom_model(self, mock_env, monkeypatch, mock_openai_client):
        """Respects WHISPER_MODEL env var."""
        monkeypatch.setenv("WHISPER_MODEL", "whisper-large-v3")

        helper = AudioHelper()
        assert helper.model == "whisper-large-v3"

    def test_init_custom_base_url(self, mock_env, monkeypatch, mock_openai_client):
        """Respects WHISPER_BASE_URL env var."""
        monkeypatch.setenv("WHISPER_BASE_URL", "http://localhost:8090/v1")

        helper = AudioHelper()
        assert helper.api_base == "http://localhost:8090/v1"

    def test_init_custom_timeout(self, mock_env, monkeypatch, mock_openai_client):
        """Respects WHISPER_TIMEOUT env var."""
        monkeypatch.setenv("WHISPER_TIMEOUT", "600")

        helper = AudioHelper()
        assert helper.timeout == 600.0

    def test_init_language_hint(self, mock_env, monkeypatch, mock_openai_client):
        """Respects WHISPER_LANGUAGE env var."""
        monkeypatch.setenv("WHISPER_LANGUAGE", "de")

        helper = AudioHelper()
        assert helper.default_language == "de"

    def test_init_no_language(self, mock_env, mock_openai_client):
        """default_language is None when WHISPER_LANGUAGE not set."""
        helper = AudioHelper()
        assert helper.default_language is None


# =============================================================================
# Singleton
# =============================================================================


class TestAudioHelperSingleton:
    """Test get_audio_helper singleton pattern."""

    def test_singleton_returns_same_instance(self, mock_env, mock_openai_client):
        """get_audio_helper returns the same instance on repeated calls."""
        import agent.services.audio_helper as mod

        mod._audio_helper = None

        s1 = mod.get_audio_helper()
        s2 = mod.get_audio_helper()
        assert s1 is s2

        # Cleanup
        mod._audio_helper = None

    def test_singleton_follows_the_exported_transport(
        self, mock_env, mock_openai_client, monkeypatch
    ):
        """A pod serves one job or Session after another, each exporting its
        own WHISPER_* block: the cached client must not keep the first one's
        endpoint and key (or the empty key of a caller that had none)."""
        import agent.services.audio_helper as mod

        mod._audio_helper = None
        monkeypatch.setenv("WHISPER_BASE_URL", "https://a.example/v1")
        monkeypatch.setenv("WHISPER_API_KEY", "sk-a")
        first = mod.get_audio_helper()
        assert first.api_key == "sk-a"

        monkeypatch.setenv("WHISPER_BASE_URL", "https://b.example/v1")
        monkeypatch.setenv("WHISPER_API_KEY", "sk-b")
        second = mod.get_audio_helper()
        assert second is not first
        assert (second.api_base, second.api_key) == ("https://b.example/v1", "sk-b")
        assert mod.get_audio_helper() is second

        mod._audio_helper = None


# =============================================================================
# Transcription (small files, single chunk)
# =============================================================================


class TestTranscribe:
    """Test transcribe() method for small files (≤ 25 MB)."""

    @pytest.mark.asyncio
    async def test_transcribe_success(
        self, audio_helper, mock_openai_client, tmp_audio
    ):
        """Successful transcription returns text."""
        mock_client, _ = mock_openai_client
        mock_client.audio.transcriptions.create = AsyncMock(
            return_value="Hello, this is a test transcription."
        )

        result = await audio_helper.transcribe(tmp_audio)

        assert result == "Hello, this is a test transcription."
        mock_client.audio.transcriptions.create.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_transcribe_passes_model(
        self, audio_helper, mock_openai_client, tmp_audio
    ):
        """Passes configured model to API."""
        mock_client, _ = mock_openai_client
        mock_client.audio.transcriptions.create = AsyncMock(return_value="text")

        await audio_helper.transcribe(tmp_audio)

        call_kwargs = mock_client.audio.transcriptions.create.call_args
        assert call_kwargs.kwargs["model"] == "whisper-1"

    @pytest.mark.asyncio
    async def test_transcribe_with_language(
        self, audio_helper, mock_openai_client, tmp_audio
    ):
        """Passes explicit language parameter to API."""
        mock_client, _ = mock_openai_client
        mock_client.audio.transcriptions.create = AsyncMock(return_value="text")

        await audio_helper.transcribe(tmp_audio, language="de")

        call_kwargs = mock_client.audio.transcriptions.create.call_args
        assert call_kwargs.kwargs["language"] == "de"

    @pytest.mark.asyncio
    async def test_transcribe_with_default_language(
        self, mock_env, monkeypatch, mock_openai_client, tmp_audio
    ):
        """Uses WHISPER_LANGUAGE as fallback when no explicit language given."""
        monkeypatch.setenv("WHISPER_LANGUAGE", "fr")
        mock_client, _ = mock_openai_client
        mock_client.audio.transcriptions.create = AsyncMock(return_value="text")

        helper = AudioHelper()
        await helper.transcribe(tmp_audio)

        call_kwargs = mock_client.audio.transcriptions.create.call_args
        assert call_kwargs.kwargs["language"] == "fr"

    @pytest.mark.asyncio
    async def test_transcribe_explicit_language_overrides_default(
        self, mock_env, monkeypatch, mock_openai_client, tmp_audio
    ):
        """Explicit language parameter overrides WHISPER_LANGUAGE."""
        monkeypatch.setenv("WHISPER_LANGUAGE", "fr")
        mock_client, _ = mock_openai_client
        mock_client.audio.transcriptions.create = AsyncMock(return_value="text")

        helper = AudioHelper()
        await helper.transcribe(tmp_audio, language="en")

        call_kwargs = mock_client.audio.transcriptions.create.call_args
        assert call_kwargs.kwargs["language"] == "en"

    @pytest.mark.asyncio
    async def test_transcribe_with_prompt(
        self, audio_helper, mock_openai_client, tmp_audio
    ):
        """Passes prompt parameter to API for style guidance."""
        mock_client, _ = mock_openai_client
        mock_client.audio.transcriptions.create = AsyncMock(return_value="text")

        await audio_helper.transcribe(
            tmp_audio, prompt="Technical discussion about APIs"
        )

        call_kwargs = mock_client.audio.transcriptions.create.call_args
        assert call_kwargs.kwargs["prompt"] == "Technical discussion about APIs"

    @pytest.mark.asyncio
    async def test_transcribe_does_not_force_text_format(
        self, audio_helper, mock_openai_client, tmp_audio
    ):
        """Uses the SDK's default JSON format — never forces response_format=text,
        which leaks `{"text": ...}` verbatim from endpoints that answer in JSON."""
        mock_client, _ = mock_openai_client
        mock_client.audio.transcriptions.create = AsyncMock(return_value="text")

        await audio_helper.transcribe(tmp_audio)

        call_kwargs = mock_client.audio.transcriptions.create.call_args
        assert call_kwargs.kwargs.get("response_format") != "text"

    @pytest.mark.asyncio
    async def test_transcribe_unwraps_json_blob(
        self, audio_helper, mock_openai_client, tmp_audio
    ):
        """A `{"text": ...}` JSON string from a non-compliant endpoint is unwrapped."""
        mock_client, _ = mock_openai_client
        mock_client.audio.transcriptions.create = AsyncMock(
            return_value='{"text": "unwrapped transcript"}'
        )

        result = await audio_helper.transcribe(tmp_audio)

        assert result == "unwrapped transcript"


# =============================================================================
# File validation
# =============================================================================


class TestFileValidation:
    """Test file validation before transcription."""

    @pytest.mark.asyncio
    async def test_file_not_found(self, audio_helper):
        """Returns error for non-existent file."""
        result = await audio_helper.transcribe(Path("/nonexistent/file.mp3"))
        assert "[Error: audio file not found" in result

    @pytest.mark.asyncio
    async def test_unsupported_format(self, audio_helper, tmp_path):
        """Returns error for unsupported file extension."""
        txt_file = tmp_path / "notes.txt"
        txt_file.write_text("not audio")

        result = await audio_helper.transcribe(txt_file)
        assert "[Error: unsupported audio format" in result

    @pytest.mark.asyncio
    async def test_large_file_triggers_chunking(
        self, audio_helper, mock_openai_client, tmp_path
    ):
        """Files over 25 MB trigger chunked transcription (not rejection)."""
        large_file = tmp_path / "huge.mp3"
        large_file.write_bytes(b"\x00" * (MAX_AUDIO_SIZE_BYTES + 1))

        # Without ffmpeg, should get a clear error about needing ffmpeg
        with patch("agent.services.audio_helper.shutil.which", return_value=None):
            result = await audio_helper.transcribe(large_file)

        assert "ffmpeg is required" in result

    @pytest.mark.asyncio
    async def test_file_at_size_limit(self, audio_helper, mock_openai_client, tmp_path):
        """Files exactly at 25 MB go through single-chunk path."""
        mock_client, _ = mock_openai_client
        mock_client.audio.transcriptions.create = AsyncMock(return_value="ok")

        exact_file = tmp_path / "exact.mp3"
        exact_file.write_bytes(b"\x00" * MAX_AUDIO_SIZE_BYTES)

        result = await audio_helper.transcribe(exact_file)
        assert "[Error" not in result


# =============================================================================
# Supported extensions
# =============================================================================


class TestAudioExtensions:
    """Test AUDIO_EXTENSIONS set covers Whisper-supported formats."""

    @pytest.mark.parametrize("ext", [".mp3", ".wav", ".m4a", ".ogg", ".flac", ".opus"])
    def test_common_formats_supported(self, ext):
        """Common audio formats are in the supported set."""
        assert ext in AUDIO_EXTENSIONS

    @pytest.mark.parametrize("ext", [".txt", ".pdf", ".png", ".py", ".json"])
    def test_non_audio_formats_rejected(self, ext):
        """Non-audio formats are not in the supported set."""
        assert ext not in AUDIO_EXTENSIONS


# =============================================================================
# Error handling
# =============================================================================


class TestErrorHandling:
    """Test error handling during transcription."""

    @pytest.mark.asyncio
    async def test_api_error_returns_error_string(
        self, audio_helper, mock_openai_client, tmp_audio
    ):
        """API errors are caught and returned as error strings."""
        mock_client, _ = mock_openai_client
        mock_client.audio.transcriptions.create = AsyncMock(
            side_effect=Exception("API rate limit exceeded")
        )

        result = await audio_helper.transcribe(tmp_audio)
        assert "[Error transcribing audio:" in result
        assert "API rate limit exceeded" in result


# =============================================================================
# Sync wrapper
# =============================================================================


class TestTranscribeSync:
    """Test synchronous wrapper."""

    def test_transcribe_sync(self, audio_helper, mock_openai_client, tmp_audio):
        """transcribe_sync bridges async to sync."""
        mock_client, _ = mock_openai_client
        mock_client.audio.transcriptions.create = AsyncMock(
            return_value="Sync transcription result"
        )

        result = audio_helper.transcribe_sync(tmp_audio)
        assert result == "Sync transcription result"


# =============================================================================
# Archiving
# =============================================================================


class TestArchiving:
    """Test audit-store archiving of transcription calls."""

    @pytest.mark.asyncio
    async def test_archive_called_on_success(
        self, audio_helper, mock_openai_client, tmp_audio
    ):
        """Archiver is called with correct parameters on successful transcription."""
        mock_client, _ = mock_openai_client
        mock_client.audio.transcriptions.create = AsyncMock(
            return_value="Transcribed text here"
        )

        mock_archiver = MagicMock()
        with patch("agent.core.archiver.get_archiver", return_value=mock_archiver):
            await audio_helper.transcribe(tmp_audio, job_id="job-123")

        mock_archiver.archive.assert_called_once()
        call_kwargs = mock_archiver.archive.call_args.kwargs

        assert call_kwargs["job_id"] == "job-123"
        assert call_kwargs["agent_type"] == "transcription"
        assert call_kwargs["model"] == "whisper-1"
        assert call_kwargs["call_type"] == "transcription"
        assert call_kwargs["auxiliary_metadata"]["trigger"] == "transcription"
        assert call_kwargs["auxiliary_metadata"]["file_name"] == "test.mp3"
        assert call_kwargs["auxiliary_metadata"]["transcript_length"] == len(
            "Transcribed text here"
        )

    @pytest.mark.asyncio
    async def test_archive_stores_transcript_text(
        self, audio_helper, mock_openai_client, tmp_audio
    ):
        """Archived response contains the actual transcript, not just a length."""
        mock_client, _ = mock_openai_client
        mock_client.audio.transcriptions.create = AsyncMock(
            return_value="The actual transcript content"
        )

        mock_archiver = MagicMock()
        with patch("agent.core.archiver.get_archiver", return_value=mock_archiver):
            await audio_helper.transcribe(tmp_audio, job_id="job-456")

        call_kwargs = mock_archiver.archive.call_args.kwargs
        response_msg = call_kwargs["response"]
        assert response_msg.content == "The actual transcript content"

    @pytest.mark.asyncio
    async def test_no_archive_without_job_id(
        self, audio_helper, mock_openai_client, tmp_audio
    ):
        """Archiver is not called when job_id is None."""
        mock_client, _ = mock_openai_client
        mock_client.audio.transcriptions.create = AsyncMock(return_value="text")

        with patch("agent.core.archiver.get_archiver") as mock_get:
            await audio_helper.transcribe(tmp_audio)  # No job_id

        mock_get.assert_not_called()

    @pytest.mark.asyncio
    async def test_archive_failure_non_fatal(
        self, audio_helper, mock_openai_client, tmp_audio
    ):
        """Archiver failure doesn't prevent transcription from returning."""
        mock_client, _ = mock_openai_client
        mock_client.audio.transcriptions.create = AsyncMock(return_value="Still works")

        mock_archiver = MagicMock()
        mock_archiver.archive.side_effect = Exception("audit store down")

        with patch("agent.core.archiver.get_archiver", return_value=mock_archiver):
            result = await audio_helper.transcribe(tmp_audio, job_id="job-789")

        assert result == "Still works"


# =============================================================================
# Transcript line splitting
# =============================================================================


class TestSplitTranscriptIntoLines:
    """Test split_transcript_into_lines() function."""

    def test_short_text_single_line(self):
        """Text under max length stays as one line."""
        lines = split_transcript_into_lines("Hello world, this is short.")
        assert lines == ["Hello world, this is short."]

    def test_preserves_existing_newlines(self):
        """Existing newline boundaries are respected."""
        text = "First paragraph.\nSecond paragraph.\nThird."
        lines = split_transcript_into_lines(text)
        assert lines == ["First paragraph.", "Second paragraph.", "Third."]

    def test_long_line_splits_at_sentence(self):
        """Lines exceeding max length split at sentence boundaries."""
        # Create text that's >120 chars with clear sentence boundaries
        text = "This is the first sentence. " * 6  # ~168 chars
        lines = split_transcript_into_lines(text.strip(), max_line_length=120)
        assert len(lines) >= 2
        # Each line should end with a sentence or be the last fragment
        for line in lines[:-1]:
            assert line.rstrip().endswith("sentence.")

    def test_long_line_splits_at_space(self):
        """Lines without sentence boundaries split at last space."""
        # No periods, just a long string of words
        text = "word " * 30  # 150 chars
        lines = split_transcript_into_lines(text.strip(), max_line_length=50)
        assert len(lines) >= 3
        for line in lines:
            assert len(line) <= 50

    def test_empty_text(self):
        """Empty text returns empty list."""
        assert split_transcript_into_lines("") == []
        assert split_transcript_into_lines("   ") == []

    def test_custom_max_length(self):
        """Respects custom max_line_length parameter."""
        text = "A" * 200
        lines = split_transcript_into_lines(text, max_line_length=80)
        for line in lines:
            assert len(line) <= 80

    def test_whitespace_only_lines_skipped(self):
        """Lines that are only whitespace after stripping are removed."""
        text = "Hello.\n   \nWorld."
        lines = split_transcript_into_lines(text)
        assert lines == ["Hello.", "World."]


# =============================================================================
# Duration detection (ffprobe)
# =============================================================================


class TestGetDuration:
    """Test _get_duration() via ffprobe."""

    def test_get_duration_success(self, audio_helper, tmp_audio):
        """Parses duration from ffprobe output."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "120.5\n"

        with patch(
            "agent.services.audio_helper.subprocess.run", return_value=mock_result
        ):
            duration = audio_helper._get_duration(tmp_audio)

        assert duration == 120.5

    def test_get_duration_ffprobe_error(self, audio_helper, tmp_audio):
        """Raises RuntimeError when ffprobe fails."""
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stderr = "not a valid audio file"

        with patch(
            "agent.services.audio_helper.subprocess.run", return_value=mock_result
        ):
            with pytest.raises(RuntimeError, match="ffprobe failed"):
                audio_helper._get_duration(tmp_audio)

    def test_get_duration_timeout(self, audio_helper, tmp_audio):
        """Raises RuntimeError on ffprobe timeout."""
        import subprocess

        with patch(
            "agent.services.audio_helper.subprocess.run",
            side_effect=subprocess.TimeoutExpired(cmd="ffprobe", timeout=30),
        ):
            with pytest.raises(RuntimeError, match="timed out"):
                audio_helper._get_duration(tmp_audio)


# =============================================================================
# Audio splitting (ffmpeg)
# =============================================================================


class TestSplitAudio:
    """Test _split_audio() via ffmpeg."""

    def test_split_creates_correct_number_of_chunks(self, audio_helper, tmp_path):
        """Calculates correct chunk count from file size."""
        audio_file = tmp_path / "big.mp3"
        # 50 MB file → ceil(50/24) = 3 chunks
        file_size = 50 * 1024 * 1024
        audio_file.write_bytes(b"\x00" * 100)  # Small file, size passed separately

        def mock_run(cmd, **kwargs):
            result = MagicMock()
            result.returncode = 0
            result.stdout = ""
            result.stderr = ""
            # Create the output chunk file so it "exists"
            if cmd[0] == "ffmpeg":
                output_path = Path(cmd[-1])
                output_path.write_bytes(b"\x00" * 100)
            return result

        with patch("agent.services.audio_helper.subprocess.run", side_effect=mock_run):
            with patch.object(audio_helper, "_get_duration", return_value=600.0):
                chunks, temp_dir = audio_helper._split_audio(audio_file, file_size)

        assert len(chunks) == 3
        # Cleanup
        import shutil

        shutil.rmtree(temp_dir, ignore_errors=True)

    def test_split_uses_stream_copy(self, audio_helper, tmp_path):
        """ffmpeg is called with -c copy (no re-encoding)."""
        audio_file = tmp_path / "big.mp3"
        file_size = 50 * 1024 * 1024

        ffmpeg_calls = []

        def mock_run(cmd, **kwargs):
            result = MagicMock()
            result.returncode = 0
            if cmd[0] == "ffmpeg":
                ffmpeg_calls.append(cmd)
                output_path = Path(cmd[-1])
                output_path.write_bytes(b"\x00" * 100)
            return result

        with patch("agent.services.audio_helper.subprocess.run", side_effect=mock_run):
            with patch.object(audio_helper, "_get_duration", return_value=600.0):
                chunks, temp_dir = audio_helper._split_audio(audio_file, file_size)

        # Verify -c copy is used
        for cmd in ffmpeg_calls:
            assert "-c" in cmd
            assert "copy" in cmd

        import shutil

        shutil.rmtree(temp_dir, ignore_errors=True)


# =============================================================================
# Chunked transcription
# =============================================================================


class TestChunkedTranscription:
    """Test _transcribe_chunked() for large files."""

    @pytest.mark.asyncio
    async def test_small_file_no_chunking(
        self, audio_helper, mock_openai_client, tmp_audio
    ):
        """Files ≤ 25 MB bypass chunking entirely."""
        mock_client, _ = mock_openai_client
        mock_client.audio.transcriptions.create = AsyncMock(return_value="small file")

        with patch.object(audio_helper, "_split_audio") as mock_split:
            result = await audio_helper.transcribe(tmp_audio)

        mock_split.assert_not_called()
        assert result == "small file"

    @pytest.mark.asyncio
    async def test_large_file_triggers_chunking(
        self, audio_helper, mock_openai_client, tmp_path
    ):
        """Files > 25 MB trigger _split_audio and per-chunk transcription."""
        mock_client, _ = mock_openai_client
        mock_client.audio.transcriptions.create = AsyncMock(
            side_effect=["chunk one text", "chunk two text"]
        )

        large_file = tmp_path / "large.mp3"
        large_file.write_bytes(b"\x00" * (MAX_AUDIO_SIZE_BYTES + 1))

        # Create fake chunk files
        chunk_dir = tmp_path / "chunks"
        chunk_dir.mkdir()
        chunk1 = chunk_dir / "chunk_0000.mp3"
        chunk2 = chunk_dir / "chunk_0001.mp3"
        chunk1.write_bytes(b"\x00" * 100)
        chunk2.write_bytes(b"\x00" * 100)

        with patch.object(
            audio_helper,
            "_split_audio",
            return_value=([chunk1, chunk2], str(chunk_dir)),
        ):
            with patch(
                "agent.services.audio_helper.shutil.which",
                return_value="/usr/bin/ffmpeg",
            ):
                result = await audio_helper.transcribe(large_file)

        assert "chunk one text" in result
        assert "chunk two text" in result

    @pytest.mark.asyncio
    async def test_context_carryover(self, audio_helper, mock_openai_client, tmp_path):
        """Chunk N+1 receives last 50 words of chunk N as prompt."""
        mock_client, _ = mock_openai_client

        # First chunk returns text with known words at the end
        first_text = " ".join(f"word{i}" for i in range(100))
        expected_carryover = " ".join(f"word{i}" for i in range(50, 100))

        call_prompts = []

        async def mock_create(**kwargs):
            call_prompts.append(kwargs.get("prompt"))
            if len(call_prompts) == 1:
                return first_text
            return "second chunk"

        mock_client.audio.transcriptions.create = mock_create

        large_file = tmp_path / "large.mp3"
        large_file.write_bytes(b"\x00" * (MAX_AUDIO_SIZE_BYTES + 1))

        chunk_dir = tmp_path / "chunks"
        chunk_dir.mkdir()
        chunk1 = chunk_dir / "c0.mp3"
        chunk2 = chunk_dir / "c1.mp3"
        chunk1.write_bytes(b"\x00" * 100)
        chunk2.write_bytes(b"\x00" * 100)

        with patch.object(
            audio_helper,
            "_split_audio",
            return_value=([chunk1, chunk2], str(chunk_dir)),
        ):
            with patch(
                "agent.services.audio_helper.shutil.which",
                return_value="/usr/bin/ffmpeg",
            ):
                await audio_helper.transcribe(large_file)

        # First chunk: no carry-over prompt (None)
        assert call_prompts[0] is None
        # Second chunk: last 50 words of first chunk
        assert call_prompts[1] == expected_carryover

    @pytest.mark.asyncio
    async def test_each_chunk_archived(
        self, audio_helper, mock_openai_client, tmp_path
    ):
        """Each chunk gets its own archive call."""
        mock_client, _ = mock_openai_client
        mock_client.audio.transcriptions.create = AsyncMock(
            side_effect=["text one", "text two"]
        )

        large_file = tmp_path / "large.mp3"
        large_file.write_bytes(b"\x00" * (MAX_AUDIO_SIZE_BYTES + 1))

        chunk_dir = tmp_path / "chunks"
        chunk_dir.mkdir()
        chunk1 = chunk_dir / "c0.mp3"
        chunk2 = chunk_dir / "c1.mp3"
        chunk1.write_bytes(b"\x00" * 100)
        chunk2.write_bytes(b"\x00" * 100)

        mock_archiver = MagicMock()
        with patch.object(
            audio_helper,
            "_split_audio",
            return_value=([chunk1, chunk2], str(chunk_dir)),
        ):
            with patch(
                "agent.services.audio_helper.shutil.which",
                return_value="/usr/bin/ffmpeg",
            ):
                with patch(
                    "agent.core.archiver.get_archiver", return_value=mock_archiver
                ):
                    await audio_helper.transcribe(large_file, job_id="job-chunk")

        # Two chunks → two archive calls
        assert mock_archiver.archive.call_count == 2

        # Verify chunk metadata
        first_call = mock_archiver.archive.call_args_list[0].kwargs
        assert first_call["auxiliary_metadata"]["chunk_index"] == 0
        assert first_call["auxiliary_metadata"]["total_chunks"] == 2

        second_call = mock_archiver.archive.call_args_list[1].kwargs
        assert second_call["auxiliary_metadata"]["chunk_index"] == 1

    @pytest.mark.asyncio
    async def test_ffmpeg_not_found_error(self, audio_helper, tmp_path):
        """Clear error message when ffmpeg is not installed."""
        large_file = tmp_path / "large.mp3"
        large_file.write_bytes(b"\x00" * (MAX_AUDIO_SIZE_BYTES + 1))

        with patch("agent.services.audio_helper.shutil.which", return_value=None):
            result = await audio_helper.transcribe(large_file)

        assert "ffmpeg is required" in result
        assert "apt-get install ffmpeg" in result

    @pytest.mark.asyncio
    async def test_temp_cleanup_on_success(
        self, audio_helper, mock_openai_client, tmp_path
    ):
        """Temp directory is cleaned up after successful transcription."""
        mock_client, _ = mock_openai_client
        mock_client.audio.transcriptions.create = AsyncMock(return_value="text")

        large_file = tmp_path / "large.mp3"
        large_file.write_bytes(b"\x00" * (MAX_AUDIO_SIZE_BYTES + 1))

        chunk_dir = tmp_path / "temp_chunks"
        chunk_dir.mkdir()
        chunk1 = chunk_dir / "c0.mp3"
        chunk1.write_bytes(b"\x00" * 100)

        with patch.object(
            audio_helper,
            "_split_audio",
            return_value=([chunk1], str(chunk_dir)),
        ):
            with patch(
                "agent.services.audio_helper.shutil.which",
                return_value="/usr/bin/ffmpeg",
            ):
                with patch("agent.services.audio_helper.shutil.rmtree") as mock_rmtree:
                    await audio_helper.transcribe(large_file)

        mock_rmtree.assert_called_once_with(str(chunk_dir), ignore_errors=True)

    @pytest.mark.asyncio
    async def test_temp_cleanup_on_error(
        self, audio_helper, mock_openai_client, tmp_path
    ):
        """Temp directory is cleaned up even when transcription fails."""
        mock_client, _ = mock_openai_client
        mock_client.audio.transcriptions.create = AsyncMock(
            side_effect=Exception("API error")
        )

        large_file = tmp_path / "large.mp3"
        large_file.write_bytes(b"\x00" * (MAX_AUDIO_SIZE_BYTES + 1))

        chunk_dir = tmp_path / "temp_chunks"
        chunk_dir.mkdir()
        chunk1 = chunk_dir / "c0.mp3"
        chunk1.write_bytes(b"\x00" * 100)

        with patch.object(
            audio_helper,
            "_split_audio",
            return_value=([chunk1], str(chunk_dir)),
        ):
            with patch(
                "agent.services.audio_helper.shutil.which",
                return_value="/usr/bin/ffmpeg",
            ):
                with patch("agent.services.audio_helper.shutil.rmtree") as mock_rmtree:
                    result = await audio_helper.transcribe(large_file)

        # Should still clean up
        mock_rmtree.assert_called_once_with(str(chunk_dir), ignore_errors=True)
        # Chunk failure logged, concatenated result includes failure marker
        assert "chunk 1 transcription failed" in result

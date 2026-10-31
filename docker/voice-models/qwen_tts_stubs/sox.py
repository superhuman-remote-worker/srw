"""Stand-in for an import of Qwen3-TTS's 25 Hz tokenizer, which the 12 Hz models
never use; see qwen-tts-rocm.requirements.txt. Never imported when the real
package is installed (it comes first on the path)."""

#!/usr/bin/env python3
"""
Minimal OpenAI Python SDK example for sub2api transcription endpoint.

Usage:
  export SUB2API_BASE_URL="http://127.0.0.1:18080/v1"
  export SUB2API_API_KEY="sk-..."
  export AUDIO_FILE="/path/to/audio.wav"
  python examples/openai_transcription_via_sub2api.py

Optional:
  export TRANSCRIBE_MODEL="gpt-4o-transcribe"
  export TRANSCRIBE_MODE="nonstream"  # nonstream | stream | both
"""

from __future__ import annotations

import os
import sys

from openai import OpenAI


def _print_nonstream_result(client: OpenAI, audio_file: str, model: str) -> None:
    with open(audio_file, "rb") as f:
        result = client.audio.transcriptions.create(
            model=model,
            file=f,
        )

    # OpenAI SDK returns a typed object with `.text` for transcription responses.
    text = getattr(result, "text", None)
    if text is None:
        print(result)
    else:
        print(f"[nonstream] text: {text}")


def _print_stream_result(client: OpenAI, audio_file: str, model: str) -> None:
    with open(audio_file, "rb") as f:
        events = client.audio.transcriptions.create(
            model=model,
            file=f,
            stream=True,
        )

    final_text = None
    for event in events:
        event_type = getattr(event, "type", "unknown")
        if event_type == "transcript.text.delta":
            delta = getattr(event, "delta", "")
            if delta:
                print(delta, end="", flush=True)
        elif event_type == "transcript.text.done":
            final_text = getattr(event, "text", None)
            usage = getattr(event, "usage", None)
            if usage is not None:
                print(f"\n[stream] usage: {usage}")
        else:
            print(f"\n[stream] event: {event}")

    if final_text:
        print(f"\n[stream] final text: {final_text}")
    else:
        print("\n[stream] done")


def main() -> int:
    base_url = os.getenv("SUB2API_BASE_URL", "http://127.0.0.1:18080/v1")
    api_key = os.getenv("SUB2API_API_KEY")
    audio_file = os.getenv("AUDIO_FILE")
    model = os.getenv("TRANSCRIBE_MODEL", "gpt-4o-transcribe")
    mode = os.getenv("TRANSCRIBE_MODE", "both").strip().lower()

    if not api_key:
        print("Missing SUB2API_API_KEY", file=sys.stderr)
        return 2
    if not audio_file:
        print("Missing AUDIO_FILE", file=sys.stderr)
        return 2
    if not os.path.exists(audio_file):
        print(f"Audio file not found: {audio_file}", file=sys.stderr)
        return 2
    if mode not in {"nonstream", "stream", "both"}:
        print("TRANSCRIBE_MODE must be one of: nonstream, stream, both", file=sys.stderr)
        return 2

    client = OpenAI(api_key=api_key, base_url=base_url)

    if mode in {"nonstream", "both"}:
        _print_nonstream_result(client, audio_file, model)

    if mode in {"stream", "both"}:
        _print_stream_result(client, audio_file, model)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

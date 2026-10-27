"""Parsing of the Job task (``/run/srw/task.json``). Standard library only."""

from __future__ import annotations

import posixpath
from dataclasses import dataclass

from .config import ConfigError, _object, _reject_unknown


@dataclass(frozen=True)
class InputFile:
    url: str
    path: str


@dataclass(frozen=True)
class UploadTarget:
    url: str
    method: str = "PUT"
    headers: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class Task:
    prompt: str
    inputs: tuple[InputFile, ...]
    upload: UploadTarget | None


def _url(value: object, name: str) -> str:
    if (
        not isinstance(value, str)
        or not value.startswith(("http://", "https://"))
        or len(value) <= 8
    ):
        raise ConfigError(f"{name} must be an http(s) URL")
    return value


def _path(value: object, name: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or "\x00" in value
        or value.startswith("/")
    ):
        raise ConfigError(f"{name} must be a relative path inside the workspace")
    if ".." in value.split("/"):
        raise ConfigError(f"{name} must not contain '..'")
    normalized = posixpath.normpath(value)
    if normalized in (".", "") or normalized.startswith("../"):
        raise ConfigError(f"{name} must be a relative path inside the workspace")
    return normalized


def _parse_input(value: object, index: int) -> InputFile:
    name = f"inputs[{index}]"
    obj = _object(value, name)
    _reject_unknown(obj, {"url", "path"}, f"{name}.")
    return InputFile(
        url=_url(obj.get("url"), f"{name}.url"),
        path=_path(obj.get("path"), f"{name}.path"),
    )


def _parse_upload(value: object) -> UploadTarget:
    obj = _object(value, "upload")
    _reject_unknown(obj, {"url", "method", "headers"}, "upload.")
    url = _url(obj.get("url"), "upload.url")
    method = obj.get("method", "PUT")
    if not isinstance(method, str) or method.upper() not in ("PUT", "POST"):
        raise ConfigError("upload.method must be PUT or POST")
    headers = _object(obj.get("headers", {}), "upload.headers")
    for key, val in headers.items():
        if not isinstance(val, str):
            raise ConfigError(f"upload.headers.{key} must be a string")
    return UploadTarget(url=url, method=method.upper(), headers=tuple(headers.items()))


def parse_task(data: object) -> Task:
    obj = _object(data, "task")
    text = obj.get("text")
    if not isinstance(text, str) or not text.strip():
        raise ConfigError("task text is required and must be a non-empty string")
    extra = _object(obj["data"], "task data") if obj.get("data") is not None else {}
    raw_inputs = extra.get("inputs", [])
    if not isinstance(raw_inputs, list):
        raise ConfigError("inputs must be a list")
    inputs = tuple(_parse_input(item, i) for i, item in enumerate(raw_inputs))
    upload = _parse_upload(extra["upload"]) if extra.get("upload") is not None else None
    return Task(prompt=text, inputs=inputs, upload=upload)

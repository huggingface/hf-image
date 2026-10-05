"""Image references: `<registry>/<namespace>/<name>[:<tag>][@<digest>]`."""

from __future__ import annotations

from dataclasses import dataclass

import click


@dataclass(frozen=True)
class ImageRef:
    registry: str
    repo: str
    """`<namespace>/<name>`."""
    tag: str | None = None
    digest: str | None = None

    @classmethod
    def parse(cls, s: str) -> ImageRef:
        rest, at, digest = s.partition("@")
        registry, slash, path = rest.partition("/")
        if not slash:
            raise click.ClickException(f"{s!r}: expected <registry>/<namespace>/<name>")
        if not ("." in registry or ":" in registry or registry == "localhost"):
            raise click.ClickException(f"{s!r} has no registry host (expected e.g. cr.hf.co/<namespace>/<name>)")
        head, colon, tag = path.rpartition(":")
        if colon and "/" not in tag:
            path = head
        else:
            tag = ""
        parts = path.split("/")
        if len(parts) != 2 or not all(parts):
            raise click.ClickException(f"{s!r}: the repository must be <namespace>/<name>")
        if at and (not digest.startswith("sha256:") or len(digest) != 71):
            raise click.ClickException(f"{s!r}: invalid digest")
        return cls(registry.lower(), path, tag or None, digest if at else None)

    def tag_or_latest(self) -> str:
        return self.tag or "latest"

    def tagged(self) -> str:
        """`<registry>/<repo>:<tag>`, the name local images get."""
        return f"{self.registry}/{self.repo}:{self.tag_or_latest()}"

    def __str__(self) -> str:
        tag = f":{self.tag}" if self.tag else ""
        digest = f"@{self.digest}" if self.digest else ""
        return f"{self.registry}/{self.repo}{tag}{digest}"

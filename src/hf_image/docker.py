"""The local Docker daemon: where its containerd lives, and the `docker` CLI for build and run."""

from __future__ import annotations

import itertools
import json
import os
import subprocess
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import click


@dataclass(frozen=True)
class Daemon:
    containerd: str
    """containerd socket of the daemon's image store, as a path on the daemon's host."""
    namespace: str
    snapshotter: str
    platform: str
    """`<os>/<arch>` of the images the daemon runs."""
    rootless: bool

    @classmethod
    def detect(cls) -> Daemon:
        """Requires the containerd image store (Docker 29 default for new installs)."""
        out = output(["info", "--format", "{{json .}}"], "docker info")
        try:
            return cls.parse(json.loads(out))
        except (ValueError, KeyError, TypeError) as e:
            raise click.ClickException("unexpected `docker info` output") from e

    @classmethod
    def parse(cls, info: dict) -> Daemon:
        containerd_store = any(len(kv) > 1 and "snapshotter" in kv[1] for kv in info.get("DriverStatus") or [])
        cd = info.get("Containerd") or {}
        if not (containerd_store and cd.get("Address")):
            raise click.ClickException(
                "Docker is not using the containerd image store; enable it "
                '(`"features": {"containerd-snapshotter": true}` in daemon.json)'
            )
        address = os.environ.get("HF_IMAGE_CONTAINERD_ADDRESS") or cd["Address"]
        return cls(
            containerd=address.removeprefix("unix://"),
            namespace=cd["Namespaces"]["Containers"],
            snapshotter=info.get("Driver") or "",
            platform=f"{info.get('OSType') or 'linux'}/{info.get('Architecture') or ''}",
            rootless=any("name=rootless" in o for o in info.get("SecurityOptions") or []),
        )


@dataclass(frozen=True)
class Builder:
    """The builder `docker buildx build <args>` would use (`--builder`, `BUILDX_BUILDER`, else the current one)."""

    network: str
    """`docker run --network` value that shares BuildKit's network: `host` when BuildKit runs in the
    daemon's network, else `container:<BuildKit container>`."""

    @classmethod
    def detect(cls, args: list[str]) -> Builder:
        named = option(args, "--builder") or os.environ.get("BUILDX_BUILDER")
        inspect = ["buildx", "inspect", "--bootstrap", *([named] if named else [])]
        return cls.parse(output(inspect, "docker buildx inspect"))

    @classmethod
    def parse(cls, text: str) -> Builder:
        """Parses `docker buildx inspect` output."""
        lines = text.splitlines()
        driver = next((line.removeprefix("Driver:").strip() for line in lines if line.startswith("Driver:")), "docker")
        host_network = any(line.startswith("Driver Options:") and 'network="host"' in line for line in lines)
        if driver == "docker" or (driver == "docker-container" and host_network):
            return cls("host")
        if driver == "docker-container":
            nodes = itertools.dropwhile(lambda line: not line.startswith("Nodes:"), lines)
            node = next((line.removeprefix("Name:").strip() for line in nodes if line.startswith("Name:")), None)
            if not node:
                raise click.ClickException("no node in `docker buildx inspect`")
            return cls(f"container:buildx_buildkit_{node}")
        raise click.ClickException(
            f"the {driver} builder driver is not supported: use the docker driver or a docker-container builder"
        )


def option(args: list[str], name: str) -> str | None:
    """The value of `--name value` or `--name=value` in a docker command line."""
    return next((args[i + 1] for i, a in enumerate(args[:-1]) if a == name), None) or next(
        (a.removeprefix(f"{name}=") for a in args if a.startswith(f"{name}=")), None
    )


def full_name(name: str) -> str:
    """The name Docker's image store gives a local image: `app` is `docker.io/library/app:latest`."""
    path, at, digest = name.partition("@")
    first = path.split("/")[0]
    if "/" not in path:
        full = f"docker.io/library/{path}"
    elif "." in first or ":" in first or first == "localhost":
        full = path
    else:
        full = f"docker.io/{path}"
    if at:
        return f"{full}@{digest}"
    return full if ":" in full.rsplit("/", 1)[-1] else f"{full}:latest"


def output(args: list[str], what: str) -> str:
    """Runs `docker <args>`; returns its stdout."""
    try:
        done = subprocess.run(["docker", *args], capture_output=True, text=True)
    except FileNotFoundError as e:
        raise click.ClickException(f"failed to run `{what}` (is Docker installed?)") from e
    if done.returncode != 0:
        raise click.ClickException(f"{what} failed: {done.stderr.strip()}")
    return done.stdout


def quiet(args: list[str]) -> None:
    output(args, f"docker {' '.join(args)}")


def image_exists(name: str) -> bool:
    done = subprocess.run(["docker", "image", "inspect", "--format", "{{.Id}}", name], capture_output=True)
    return done.returncode == 0


def run(args: list[str]) -> int:
    """Runs `docker <args>` with inherited stdio; returns its exit code."""
    return subprocess.call(["docker", *args])


@contextmanager
def anonymous() -> Iterator[dict[str, str]]:
    """An environment where the `docker` CLI sends no registry credentials, on the same context."""
    user = Path(os.environ.get("DOCKER_CONFIG") or Path.home() / ".docker")
    try:
        config = json.loads((user / "config.json").read_text())
    except (OSError, ValueError):
        config = {}
    context = config.get("currentContext") if isinstance(config, dict) else None
    with tempfile.TemporaryDirectory(prefix="hf-image-docker-") as tmp:
        (Path(tmp) / "config.json").write_text(json.dumps({"currentContext": context} if context else {}))
        if (user / "contexts").is_dir():
            (Path(tmp) / "contexts").symlink_to(user / "contexts")
        yield {**os.environ, "DOCKER_CONFIG": tmp}

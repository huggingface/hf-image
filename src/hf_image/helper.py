"""The helper container: the Linux `hf-image-helper` that the CLI runs next to the Docker daemon (in
its VM on macOS, in rootlesskit's namespace for rootless Docker), where containerd's socket and
BuildKit's network are local. The HF token is the first line of its stdin; the next line, or EOF,
stops it. It prints its results as JSON lines."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import click

from ._pin import HELPER_IMAGE
from .docker import Daemon, anonymous, image_exists

CACHE_VOLUME = "hf-image-cache"
"""Named volume holding the Xet caches (`HF_IMAGE_CACHE_VOLUME` overrides)."""
CACHE_DIR = "/cache"
CONTAINERD_SOCKET = "/run/hf-image/containerd.sock"
LAYOUT = "/layout"
"""Where an OCI layout is mounted."""
FORWARDED = ("HF_IMAGE_CONCURRENCY", "HF_IMAGE_CHUNK_CACHE_BYTES", "HF_IMAGE_INSECURE_REGISTRIES", "HF_IMAGE_LOG")
"""Settings the helper inherits, passed by name so values stay off the command line."""


class Failed(Exception):
    """The helper exited with this code, after reporting its error."""

    def __init__(self, code: int):
        super().__init__(f"the helper container exited with code {code}")
        self.code = code


@dataclass(frozen=True)
class Layout:
    """An OCI layout directory on the host, mounted at `LAYOUT`."""

    path: Path
    writable: bool

    @staticmethod
    def owner(daemon: Daemon) -> str | None:
        """`<uid>:<gid>` for what the helper writes, when its root is the host's (rootful Linux)."""
        if sys.platform != "linux" or daemon.rootless:
            return None
        return f"{os.getuid()}:{os.getgid()}"


class Helper:
    """Runs helper containers against one daemon."""

    def __init__(self, daemon: Daemon, network: str = "host"):
        self.image = os.environ.get("HF_IMAGE_HELPER_IMAGE") or HELPER_IMAGE
        self.daemon = daemon
        self.network = network
        """`docker run --network`: BuildKit's network, or the daemon's."""

    def command(self, op: list[str], layout: Layout | None = None) -> list[str]:
        volume = os.environ.get("HF_IMAGE_CACHE_VOLUME") or CACHE_VOLUME
        cmd = ["docker", "run", "--rm", "-i", "--init", "--network", self.network]
        if layout is None:
            cmd += ["-v", f"{self.daemon.containerd}:{CONTAINERD_SOCKET}"]
        else:
            src = layout.path.resolve()
            if "," in str(src):
                raise click.ClickException(f"{src}: an OCI layout path cannot contain a comma")
            mount = f"type=bind,src={src},dst={LAYOUT}"
            cmd += ["--mount", mount if layout.writable else f"{mount},readonly"]
        cmd += ["-v", f"{volume}:{CACHE_DIR}", "-e", f"HF_XET_CACHE={CACHE_DIR}"]
        for name in sorted(os.environ):
            if name in FORWARDED or (name.startswith("HF_XET_") and name != "HF_XET_CACHE"):
                cmd += ["-e", name]
        cmd.append(self.image)
        if layout is None:
            d = self.daemon
            cmd += ["--containerd", CONTAINERD_SOCKET, "--namespace", d.namespace, "--snapshotter", d.snapshotter]
        return cmd + op

    def fetch(self) -> None:
        """Pulls the pinned image without credentials when missing: it is public, and a stale login is refused."""
        if self.image != HELPER_IMAGE or image_exists(self.image):
            return
        click.echo(f"Pulling the helper image {self.image.split('@')[0]}", err=True)
        with anonymous() as env:
            # On failure, `docker run` pulls it with the user's credentials and reports why.
            subprocess.run(["docker", "pull", "-q", self.image], env=env, capture_output=True)

    def start(self, op: list[str], token: str | None, layout: Layout | None = None) -> Running:
        """Starts `op` and hands it the token."""
        try:
            self.fetch()
            proc = subprocess.Popen(
                self.command(op, layout), stdin=subprocess.PIPE, stdout=subprocess.PIPE, encoding="utf-8"
            )
        except FileNotFoundError as e:
            raise click.ClickException("failed to run docker (is Docker installed?)") from e
        running = Running(proc)
        running.send(token or "")
        return running

    def run(self, op: list[str], token: str | None, layout: Layout | None = None) -> dict:
        """Runs `op` to completion; returns its result."""
        with self.start(op, token, layout) as running:
            return running.result(running.wait())


class Running:
    """A helper container at work; leaving its `with` block stops it."""

    def __init__(self, proc: subprocess.Popen):
        self.proc = proc

    def __enter__(self) -> Running:
        return self

    def __exit__(self, *exc) -> None:
        if self.proc.poll() is None:
            self.close()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()

    def send(self, line: str) -> None:
        try:
            self.proc.stdin.write(f"{line}\n")
            self.proc.stdin.flush()
        except BrokenPipeError:
            pass

    def line(self) -> str:
        """The next line the helper prints."""
        line = self.proc.stdout.readline()
        if not line:
            self.exit()
            raise click.ClickException("the helper container stopped early")
        return line.rstrip("\n")

    def stop(self) -> list[str]:
        """Stops the helper; returns the rest of what it printed."""
        self.send("")
        return self.wait()

    def wait(self) -> list[str]:
        lines = [line.rstrip("\n") for line in self.proc.stdout]
        self.exit()
        return lines

    @staticmethod
    def result(lines: list[str]) -> dict:
        try:
            return json.loads(lines[-1])
        except (IndexError, ValueError) as e:
            raise click.ClickException("unexpected helper output") from e

    def close(self) -> None:
        try:
            self.proc.stdin.close()
        except BrokenPipeError:
            pass

    def exit(self) -> None:
        self.close()
        code = self.proc.wait()
        if code == 0:
            return
        if code == 125:
            raise click.ClickException("Docker could not start the helper container: see its error above")
        if code < 0:
            raise click.ClickException("the helper container was killed")
        raise Failed(code)

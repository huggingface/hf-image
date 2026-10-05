"""`hf image`: the commands. The data plane runs in the helper container (see `helper`)."""

from __future__ import annotations

import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import click

from . import docker
from .docker import Builder, Daemon
from .helper import LAYOUT, Failed, Helper, Layout
from .reference import ImageRef

BUILD_FLAGS = ("--push", "--load", "--output", "-o", "--tag")
"""`docker buildx build` flags that `hf image build` sets."""
PASSTHROUGH = {"ignore_unknown_options": True, "allow_interspersed_args": False}


@click.group()
@click.option("--token", help="Hugging Face token (default: HF_TOKEN, then the token `hf auth login` saved).")
@click.version_option(package_name="hf-image")
@click.pass_context
def cli(ctx: click.Context, token: str | None) -> None:
    """Fast build, push, pull and run on the Hugging Face registry."""
    ctx.obj = token


def hf_token(explicit: str | None) -> str | None:
    if explicit:
        return explicit
    from huggingface_hub import get_token

    return get_token()


@cli.command(context_settings={"ignore_unknown_options": True})
@click.option("-t", "--tag", required=True, help="Image to build and push: <registry>/<namespace>/<name>[:<tag>].")
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
@click.pass_obj
def build(token: str | None, tag: str, args: tuple[str, ...]) -> None:
    """Build with `docker buildx build` and push: no gzip, only new chunks are uploaded.

    ARGS go to `docker buildx build` (context, -f, --platform, --build-arg...).
    """
    if own := next((a for a in args if a.split("=")[0] in BUILD_FLAGS), None):
        raise click.ClickException(f"{own} is set by hf image build")
    image = ImageRef.parse(tag)
    start = time.monotonic()
    with ThreadPoolExecutor(2) as pool:
        detecting = pool.submit(Daemon.detect), pool.submit(Builder.detect, list(args))
        daemon, builder = (f.result() for f in detecting)
    serve = ["serve", f"{image.registry}/{image.repo}"]
    with Helper(daemon, builder.network).start(serve, hf_token(token)) as helper:
        endpoint = json.loads(helper.line())
        gw_name = f"{endpoint['addr']}/{endpoint['secret']}/{image.repo}:{image.tag_or_latest()}"
        output = (
            f"type=image,name={gw_name},push=true,registry.insecure=true,"
            "compression=uncompressed,force-compression=true,oci-mediatypes=true"
        )
        code = docker.run(["buildx", "build", *args, "--output", output])
        summary = json.loads("".join(helper.stop()))
    if code != 0:
        raise click.ClickException(f"docker buildx build failed (exit code {code})")
    digest = next((d for r, d in reversed(summary["pushed"]) if r == image.tag_or_latest()), None)
    if digest is None:
        raise click.ClickException("the build pushed no image")
    if docker.image_exists(gw_name):
        docker.quiet(["image", "tag", gw_name, image.tagged()])
        docker.quiet(["image", "rm", gw_name])
    click.echo(
        f"built and pushed {image.tagged()} ({digest[:19]}) in {time.monotonic() - start:.1f}s: "
        f"{summary['blobs_uploaded']} blobs uploaded ({human_bytes(summary['bytes_new'])} new after dedup), "
        f"{summary['blobs_skipped']} already there",
        err=True,
    )
    click.echo(digest)


@cli.command()
@click.argument("image")
@click.option("--from", "source", help="Local image name or `oci:<dir>` (default: the local image named like IMAGE).")
@click.option(
    "--preserve-digests",
    is_flag=True,
    help="Push blobs and manifests as stored: digests match `docker push`, compressed layers don't dedupe.",
)
@click.pass_obj
def push(token: str | None, image: str, source: str | None, preserve_digests: bool) -> None:
    """Push a local image (or an OCI layout) as uncompressed layers through Xet."""
    ref = ImageRef.parse(image)
    daemon = Daemon.detect()
    layout = None
    if source and source.startswith("oci:"):
        layout = Layout(Path(source.removeprefix("oci:")), writable=False)
        source = f"oci:{LAYOUT}"
    else:
        source = docker.full_name(source) if source else ref.tagged()
    op = ["push", str(ref), "--from", source, *(["--preserve-digests"] if preserve_digests else [])]
    click.echo(Helper(daemon).run(op, hf_token(token), layout)["digest"])


@cli.command()
@click.argument("image")
@click.option("--platform", help="<os>/<arch>[/<variant>] (default: the daemon's platform).")
@click.option(
    "-o",
    "--output",
    type=click.Path(file_okay=False, path_type=Path),
    help="Write an OCI layout here instead of the Docker image store.",
)
@click.pass_obj
def pull(token: str | None, image: str, platform: str | None, output: Path | None) -> None:
    """Pull straight from Xet into Docker's containerd store (or an OCI layout with -o)."""
    ref = ImageRef.parse(image)
    daemon = Daemon.detect()
    op = ["pull", str(ref), "--platform", platform or daemon.platform]
    layout = None
    if output is not None:
        output.mkdir(parents=True, exist_ok=True)
        layout = Layout(output, writable=True)
        op += ["--output", LAYOUT]
        if owner := Layout.owner(daemon):
            op += ["--owner", owner]
    click.echo(Helper(daemon).run(op, hf_token(token), layout)["digest"])


@cli.command(context_settings=PASSTHROUGH)
@click.argument("args", nargs=-1, required=True, type=click.UNPROCESSED)
@click.pass_obj
def run(token: str | None, args: tuple[str, ...]) -> None:
    """`docker run`, pulling first (fast) when the local image is missing or stale."""
    image = next((r for a in args if not a.startswith("-") and (r := parse_or_none(a))), None)
    if image is None:
        raise click.ClickException("no <registry>/<namespace>/<name> image in the arguments")
    daemon = Daemon.detect()
    Helper(daemon).run(["pull", str(image), "--platform", daemon.platform, "--if-stale"], hf_token(token))
    sys.stdout.flush()
    sys.stderr.flush()
    os.execvp("docker", ["docker", "run", *args])


def parse_or_none(s: str) -> ImageRef | None:
    try:
        return ImageRef.parse(s)
    except click.ClickException:
        return None


def human_bytes(n: int) -> str:
    v, units = float(n), ["B", "kB", "MB", "GB", "TB"]
    u = 0
    while v >= 1000 and u < len(units) - 1:
        v /= 1000
        u += 1
    return f"{n} B" if u == 0 else f"{v:.1f} {units[u]}"


def main() -> None:
    if sys.platform == "win32":
        sys.exit("error: hf image does not support Windows yet")
    try:
        cli(prog_name="hf image")
    except Failed as e:
        sys.exit(e.code)

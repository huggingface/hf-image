"""The commands end to end, against a fake `docker` that records what it is asked."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from .test_docker import INFO

DIGEST = "sha256:" + "a" * 64
BUILT = "sha256:" + "b" * 64
FAKE_DOCKER = f"""#!{sys.executable}
import json, os, sys
args = sys.argv[1:]
def record(**extra):
    with open(os.environ["FAKE_DOCKER_LOG"], "a") as f:
        f.write(json.dumps({{"args": args, **extra}}) + "\\n")
if args[:1] == ["info"]:
    print({json.dumps(INFO)!r})
elif args[:2] == ["buildx", "inspect"]:
    print("Name: default\\nDriver: docker\\n")
elif args[:2] == ["image", "inspect"]:
    sys.exit(1)
elif args[:1] == ["run"] and "helper:test" in args:
    record(token=sys.stdin.readline().rstrip("\\n"))
    if code := int(os.environ.get("FAKE_HELPER_EXIT", "0")):
        print("error: boom", file=sys.stderr)
        sys.exit(code)
    if "serve" in args:
        print(json.dumps({{"addr": "127.0.0.1:4242", "secret": "s3cr3t"}}), flush=True)
        sys.stdin.readline()
        summary = {{"pushed": [["v1", "{BUILT}"]], "blobs_uploaded": 2, "blobs_skipped": 3, "bytes_new": 1500000}}
        print(json.dumps(summary))
    else:
        print(json.dumps({{"digest": "{DIGEST}"}}))
else:
    record()
"""


@pytest.fixture
def hfi(tmp_path: Path):
    """Runs `hf image <args>`; returns the result and the docker calls."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "docker").write_text(FAKE_DOCKER)
    (bin_dir / "docker").chmod(0o755)
    log = tmp_path / "docker.log"
    env = {k: v for k, v in os.environ.items() if not k.startswith(("HF_IMAGE_", "HF_XET_", "BUILDX_"))}
    env |= {"PATH": f"{bin_dir}:{env['PATH']}", "FAKE_DOCKER_LOG": str(log), "HF_TOKEN": "hf_test"}
    env |= {"HF_IMAGE_HELPER_IMAGE": "helper:test"}

    def run(*args: str, **extra_env: str):
        cli = [sys.executable, "-c", "from hf_image.cli import main; main()", *args]
        done = subprocess.run(cli, env=env | extra_env, capture_output=True, text=True, cwd=tmp_path)
        calls = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
        log.unlink(missing_ok=True)
        return done, calls

    return run


def op(call: dict) -> list[str]:
    """The helper op of a recorded `docker run`, after the image and the store options."""
    args = call["args"][call["args"].index("helper:test") + 1 :]
    return args[6:] if args[:1] == ["--containerd"] else args


def test_pull(hfi):
    done, calls = hfi("pull", "cr.hf.co/a/b:v1")
    assert (done.returncode, done.stdout) == (0, f"{DIGEST}\n"), done.stderr
    (helper,) = calls
    assert op(helper) == ["pull", "cr.hf.co/a/b:v1", "--platform", "linux/aarch64"]
    assert helper["token"] == "hf_test"
    assert not any("hf_test" in a for a in helper["args"])


def test_pull_layout(hfi, tmp_path: Path):
    done, (helper,) = hfi("pull", "cr.hf.co/a/b:v1", "--platform", "linux/amd64", "-o", "out")
    assert done.returncode == 0, done.stderr
    assert (tmp_path / "out").is_dir()
    assert f"type=bind,src={(tmp_path / 'out').resolve()},dst=/layout" in helper["args"]
    owner = ["--owner", f"{os.getuid()}:{os.getgid()}"] if sys.platform == "linux" else []
    assert op(helper) == ["pull", "cr.hf.co/a/b:v1", "--platform", "linux/amd64", "--output", "/layout", *owner]


def test_push(hfi, tmp_path: Path):
    _, (helper,) = hfi("push", "cr.hf.co/a/b:v1")
    assert op(helper) == ["push", "cr.hf.co/a/b:v1", "--from", "cr.hf.co/a/b:v1"]
    _, (helper,) = hfi("push", "cr.hf.co/a/b:v1", "--from", "busybox")
    assert op(helper) == ["push", "cr.hf.co/a/b:v1", "--from", "docker.io/library/busybox:latest"]
    done, (helper,) = hfi("push", "cr.hf.co/a/b", "--from", f"oci:{tmp_path}", "--preserve-digests")
    assert done.stdout == f"{DIGEST}\n"
    assert f"type=bind,src={tmp_path.resolve()},dst=/layout,readonly" in helper["args"]
    assert op(helper) == ["push", "cr.hf.co/a/b", "--from", "oci:/layout", "--preserve-digests"]


def test_run(hfi):
    args = ["--rm", "-it", "-e", "A=b", "cr.hf.co/a/b:v1", "sh", "-c", "echo --token x"]
    done, (helper, run) = hfi("run", *args)
    assert done.returncode == 0, done.stderr
    assert op(helper) == ["pull", "cr.hf.co/a/b:v1", "--platform", "linux/aarch64", "--if-stale"]
    assert run["args"] == ["run", *args]


def test_run_pulls_the_platform_it_runs(hfi):
    _, (helper, _) = hfi("run", "--platform=linux/amd64", "cr.hf.co/a/b:v1", "--platform", "x")
    assert op(helper) == ["pull", "cr.hf.co/a/b:v1", "--platform", "linux/amd64", "--if-stale"]


def test_run_needs_an_image(hfi):
    done, calls = hfi("run", "--rm", "busybox")
    assert done.returncode == 1 and "no <registry>/<namespace>/<name> image" in done.stderr
    assert calls == []


def test_build(hfi):
    done, (serve, build) = hfi("build", "-t", "cr.hf.co/a/b:v1", "--build-arg", "X=1", "ctx")
    assert (done.returncode, done.stdout) == (0, f"{BUILT}\n"), done.stderr
    assert "2 blobs uploaded (1.5 MB new after dedup), 3 already there" in done.stderr
    assert op(serve) == ["serve", "cr.hf.co/a/b"]
    output = (
        "type=image,name=127.0.0.1:4242/s3cr3t/a/b:v1,push=true,registry.insecure=true,"
        "compression=uncompressed,force-compression=true,oci-mediatypes=true"
    )
    assert build["args"] == ["buildx", "build", "--build-arg", "X=1", "ctx", "--output", output]


def test_build_sets_its_own_output(hfi):
    done, calls = hfi("build", "-t", "cr.hf.co/a/b:v1", "--push", "ctx")
    assert done.returncode == 1 and "--push is set by hf image build" in done.stderr
    assert calls == []


def test_helper_exit_code(hfi):
    done, _ = hfi("pull", "cr.hf.co/a/b:v1", FAKE_HELPER_EXIT="3")
    assert done.returncode == 3
    assert done.stderr == "error: boom\n"


def test_windows_is_refused():
    code = "import sys; from hf_image.cli import main; sys.platform = 'win32'; main()"
    done = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert (done.returncode, done.stderr) == (1, "error: hf image does not support Windows yet\n")

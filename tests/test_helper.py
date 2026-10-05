import re
from pathlib import Path

import pytest

from hf_image._pin import HELPER_IMAGE
from hf_image.docker import Daemon
from hf_image.helper import Helper, Layout

DAEMON = Daemon("/run/containerd/containerd.sock", "moby", "overlayfs", "linux/arm64", rootless=False)
ROOT = Path(__file__).parent.parent


@pytest.fixture(autouse=True)
def env(monkeypatch):
    for name in ("HF_IMAGE_HELPER_IMAGE", "HF_IMAGE_CACHE_VOLUME", "HF_IMAGE_LOG", "HF_XET_HIGH_PERFORMANCE"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("HF_IMAGE_HELPER_IMAGE", "helper:test")


def test_store_op():
    """The command line helper/src/main.rs's `cli_command_lines` parses."""
    cmd = Helper(DAEMON).command(["pull", "cr.hf.co/a/b:v1", "--platform", "linux/arm64", "--if-stale"])
    assert cmd == [
        *("docker", "run", "--rm", "-i", "--init", "--network", "host"),
        *("-v", "/run/containerd/containerd.sock:/run/hf-image/containerd.sock"),
        *("-v", "hf-image-cache:/cache", "-e", "HF_XET_CACHE=/cache"),
        "helper:test",
        *("--containerd", "/run/hf-image/containerd.sock", "--namespace", "moby", "--snapshotter", "overlayfs"),
        *("pull", "cr.hf.co/a/b:v1", "--platform", "linux/arm64", "--if-stale"),
    ]


def test_settings_pass_by_name(monkeypatch):
    monkeypatch.setenv("HF_IMAGE_LOG", "debug")
    monkeypatch.setenv("HF_XET_HIGH_PERFORMANCE", "1")
    monkeypatch.setenv("HF_XET_CACHE", "/elsewhere")
    monkeypatch.setenv("HF_IMAGE_CACHE_VOLUME", "my-cache")
    cmd = Helper(DAEMON, "container:buildx_buildkit_b0").command(["serve", "cr.hf.co/a/b"])
    assert cmd[cmd.index("--network") + 1] == "container:buildx_buildkit_b0"
    assert "my-cache:/cache" in cmd
    names = [cmd[i + 1] for i, a in enumerate(cmd) if a == "-e"]
    assert names == ["HF_XET_CACHE=/cache", "HF_IMAGE_LOG", "HF_XET_HIGH_PERFORMANCE"]


def test_layout_ops(tmp_path: Path):
    cmd = Helper(DAEMON).command(["push", "cr.hf.co/a/b:v1", "--from", "oci:/layout"], Layout(tmp_path, False))
    assert f"type=bind,src={tmp_path.resolve()},dst=/layout,readonly" in cmd
    assert "--containerd" not in cmd and not any("containerd.sock" in a for a in cmd)
    cmd = Helper(DAEMON).command(["pull", "cr.hf.co/a/b:v1"], Layout(tmp_path, True))
    assert f"type=bind,src={tmp_path.resolve()},dst=/layout" in cmd


def test_layout_owner(monkeypatch):
    monkeypatch.setattr("sys.platform", "linux")
    assert Layout.owner(DAEMON) is not None
    assert Layout.owner(Daemon(**{**DAEMON.__dict__, "rootless": True})) is None
    monkeypatch.setattr("sys.platform", "darwin")
    assert Layout.owner(DAEMON) is None


def test_versions_and_pin_agree():
    """pyproject.toml, helper/Cargo.toml and the pinned helper image carry the same version."""
    version = re.search(r'^version = "(.+)"$', (ROOT / "pyproject.toml").read_text(), re.M)[1]
    assert re.search(r'^version = "(.+)"$', (ROOT / "helper/Cargo.toml").read_text(), re.M)[1] == version
    assert re.fullmatch(rf"[^@]+:{re.escape(version)}@sha256:[0-9a-f]{{64}}", HELPER_IMAGE), HELPER_IMAGE

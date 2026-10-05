import click
import pytest

from hf_image.docker import Builder, Daemon, full_name

INFO = {
    "Driver": "overlayfs",
    "DriverStatus": [["driver-type", "io.containerd.snapshotter.v1"]],
    "Containerd": {"Address": "unix:///run/containerd/containerd.sock", "Namespaces": {"Containers": "moby"}},
    "OSType": "linux",
    "Architecture": "aarch64",
    "SecurityOptions": ["name=seccomp,profile=builtin", "name=cgroupns"],
}


def test_daemon(monkeypatch):
    monkeypatch.delenv("HF_IMAGE_CONTAINERD_ADDRESS", raising=False)
    d = Daemon.parse(INFO)
    assert d == Daemon("/run/containerd/containerd.sock", "moby", "overlayfs", "linux/aarch64", rootless=False)
    assert Daemon.parse({**INFO, "SecurityOptions": ["name=rootless"]}).rootless
    monkeypatch.setenv("HF_IMAGE_CONTAINERD_ADDRESS", "/elsewhere.sock")
    assert Daemon.parse(INFO).containerd == "/elsewhere.sock"


def test_daemon_needs_the_containerd_store():
    with pytest.raises(click.ClickException, match="containerd image store"):
        Daemon.parse({**INFO, "DriverStatus": [["Backing Filesystem", "extfs"]]})


def test_builder_networks():
    docker = "Name:          lima\nDriver:        docker\n\nNodes:\nName:             lima\nEndpoint:         lima\n"
    assert Builder.parse(docker).network == "host"
    bridged = "Name:          b\nDriver:        docker-container\n\nNodes:\nName:             b0\nStatus:   running\n"
    assert Builder.parse(bridged).network == "container:buildx_buildkit_b0"
    host = 'Name: b\nDriver: docker-container\n\nNodes:\nName: b0\nDriver Options: network="host"\n'
    assert Builder.parse(host).network == "host"
    with pytest.raises(click.ClickException):
        Builder.parse("Name: k\nDriver: kubernetes\n")


def test_local_names():
    assert full_name("app") == "docker.io/library/app:latest"
    assert full_name("acme/app:v1") == "docker.io/acme/app:v1"
    assert full_name("127.0.0.1:5055/acme/app") == "127.0.0.1:5055/acme/app:latest"
    assert full_name("cr.hf.co/acme/app:v1") == "cr.hf.co/acme/app:v1"
    d = "sha256:" + "a" * 64
    assert full_name(f"app@{d}") == f"docker.io/library/app@{d}"

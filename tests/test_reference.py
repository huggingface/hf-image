import click
import pytest

from hf_image.reference import ImageRef


def test_parses_references():
    r = ImageRef.parse("cr.hf.co/acme/app:v1")
    assert (r.registry, r.repo, r.tag) == ("cr.hf.co", "acme/app", "v1")
    r = ImageRef.parse("127.0.0.1:5055/acme/app")
    assert (r.tag, r.tag_or_latest(), str(r)) == (None, "latest", "127.0.0.1:5055/acme/app")
    d = "sha256:" + "a" * 64
    r = ImageRef.parse(f"localhost:5000/a/b:t@{d}")
    assert (r.digest, r.tagged(), str(r)) == (d, "localhost:5000/a/b:t", f"localhost:5000/a/b:t@{d}")
    assert ImageRef.parse("CR.HF.CO/a/b").registry == "cr.hf.co"


@pytest.mark.parametrize("s", ["acme/app:v1", "cr.hf.co/app", "cr.hf.co/a/b/c", "cr.hf.co/a/b@sha256:abc", "cr.hf.co"])
def test_rejects_references(s):
    with pytest.raises(click.ClickException):
        ImageRef.parse(s)

import io
import json
import os
import shutil

import pytest

from hf_image.helper import Running
from hf_image.progress import Progress, human


class Terminal(io.StringIO):
    def isatty(self) -> bool:
        return True


@pytest.fixture
def wide(monkeypatch):
    monkeypatch.setattr(shutil, "get_terminal_size", lambda: os.terminal_size((120, 40)))


def layer(id: str, size: int, state: str) -> dict:
    return {"layer": {"id": id, "size": size, "state": state}}


PULL = {"image": {"name": "cr.hf.co/a/b:v1", "direction": "down"}}


def screen(out: io.StringIO) -> list[str]:
    """The last frame drawn."""
    frame = out.getvalue().rsplit("\x1b[J", 2)[-2].rsplit("F", 1)[-1]
    return [line.removeprefix("\x1b[2K") for line in frame.splitlines()]


def test_a_terminal_shows_a_state_per_layer_and_one_bar(wide):
    out = Terminal()
    p = Progress(out, clock=lambda: 0.0)
    for event in [
        PULL,
        layer("aaa", 1_130_000_000, "downloading"),
        layer("bbb", 719_000_000, "waiting"),
        layer("ccc", 2_560, "done"),
        layer("ddd", 4_096, "waiting"),
        {"transfer": {"bytes": 412_000_000, "total": 1_849_006_656}},
        {"network": {"bytes": 250_000_000, "total": 300_000_000}},
    ]:
        assert p.event(event)
    assert screen(out) == [
        "Pulling cr.hf.co/a/b:v1 · 4 layers, 1.85 GB",
        "  ↓ aaa   1.13 GB  downloading",
        "  · bbb    719 MB  waiting",
        "  · 2 small layers (6.66 kB)",
        "  " + "━" * 8 + "─" * 32 + "   22% · 412 MB / 1.85 GB",
    ]


PUSH = {"image": {"name": "cr.hf.co/a/b:v2", "direction": "up"}}


@pytest.mark.parametrize(
    "image, moved, waited",
    [
        (PULL, "↓ 20 B/s · 8s left", "↓ 13 B/s · 6s left"),
        (PUSH, "25 B/s · 8s left", "17 B/s · 6s left"),
    ],
)
def test_rates_are_means_and_the_time_left_counts_down_from_its_lowest_estimate(wide, image, moved, waited):
    now = [0.0]
    out = Terminal()
    p = Progress(out, clock=lambda: now[0])
    for event in [
        image,
        layer("aaa", 2_000_000, "downloading"),
        {"transfer": {"bytes": 10, "total": 300}},
        {"network": {"bytes": 40, "total": 90}},
    ]:
        p.event(event)
    assert screen(out)[-1].endswith(" 3% · 10 B / 300 B")
    now[0] = 4.0
    p.event({"network": {"bytes": 120, "total": 200}})
    p.event({"transfer": {"bytes": 110, "total": 300}})
    assert screen(out)[-1].endswith(f" 36% · 110 B / 300 B · {moved}")
    now[0] = 6.0
    p.event({"transfer": {"bytes": 112, "total": 300}})
    assert screen(out)[-1].endswith(f" 37% · 112 B / 300 B · {waited}")
    now[0] = 12.0
    p.event({"transfer": {"bytes": 112, "total": 300}})
    assert "left" not in screen(out)[-1]
    p.event({"transfer": {"bytes": 260, "total": 300}})
    assert screen(out)[-1].endswith(" · 2s left")


def test_a_pushs_rate_leaves_out_layers_in_the_registry(wide):
    now = [0.0]
    out = Terminal()
    p = Progress(out, clock=lambda: now[0])
    p.event(PUSH)
    p.event(layer("aaa", 2_000_000, "uploading"))
    p.event(layer("bbb", 3_000_000, "waiting"))
    p.event({"transfer": {"bytes": 1_000_000, "total": 5_000_000}})
    now[0] = 2.0
    p.event(layer("bbb", 3_000_000, "exists"))
    p.event({"transfer": {"bytes": 4_500_000, "total": 5_000_000}})
    assert screen(out)[-1].endswith(" 90% · 4.50 MB / 5.00 MB · 250 kB/s · 2s left")


def test_extracting_shows_its_elapsed_time(wide):
    now = [100.0]
    out = Terminal()
    p = Progress(out, clock=lambda: now[0])
    p.event(PULL)
    p.event(layer("aaa", 2_000_000, "extracting"))
    now[0] = 102.5
    p.event({"transfer": {"bytes": 1, "total": 1}})
    assert screen(out)[1].endswith(" aaa   2.00 MB  extracting 2.5s")


def test_a_pull_summary_says_what_xet_saved(wide):
    out = Terminal()
    p = Progress(out)
    p.event(PULL)
    p.event(layer("aaa", 2_000_000, "done"))
    summary = {
        "name": "cr.hf.co/a/b:v1",
        "secs": 11.8,
        "layers": 2_610_000_000,
        "local": 156_000_000,
        "cached": 640_000_000,
        "wire": 900_000_000,
        "wire_exact": False,
        "wire_secs": 6.0,
    }
    p.event({"summary": summary})
    assert out.getvalue().endswith(
        "\x1b[JPulled cr.hf.co/a/b:v1 in 11.8s\n"
        "  2.61 GB of layers → ≈900 MB downloaded at 150 MB/s\n"
        "  saved: 156 MB already local · 640 MB from the Xet cache · 2.0× compression\n"
    )


def test_a_push_summary_says_what_xet_deduplicated():
    out = io.StringIO()
    p = Progress(out)
    p.event({"image": {"name": "cr.hf.co/a/b:v2", "direction": "up"}})
    p.event({"status": "Finishing the upload"})
    summary = {
        "name": "cr.hf.co/a/b:v2",
        "secs": 4.4,
        "layers": 2_610_000_000,
        "existing": 1_000_000_000,
        "processed": 1_610_000_000,
        "deduplicated": 1_220_000_000,
        "wire": 195_000_000,
        "wire_secs": 3.9,
    }
    p.event({"summary": summary})
    assert out.getvalue() == (
        "Pushing cr.hf.co/a/b:v2\n"
        "Finishing the upload\n"
        "Pushed cr.hf.co/a/b:v2 in 4.4s\n"
        "  2.61 GB of layers → 195 MB uploaded at 50.0 MB/s\n"
        "  saved: 1.00 GB already in the registry · 1.22 GB deduplicated by Xet (75%) · 2.0× compression\n"
    )


def test_a_push_summary_skips_a_few_deduplicated_bytes():
    out = io.StringIO()
    p = Progress(out)
    p.event({"image": {"name": "cr.hf.co/a/b:v2", "direction": "up"}})
    summary = {
        "name": "cr.hf.co/a/b:v2",
        "secs": 9.5,
        "layers": 2_820_000_000,
        "existing": 2_610_000_000,
        "processed": 210_000_000,
        "deduplicated": 2,
        "wire": 210_000_000,
        "wire_secs": 6.4,
    }
    p.event({"summary": summary})
    assert out.getvalue().endswith("  saved: 2.61 GB already in the registry\n")


def test_a_pipe_gets_no_layer_lines():
    out = io.StringIO()
    p = Progress(out)
    p.event(PULL)
    p.event(layer("aaa", 2_000_000, "downloading"))
    p.event({"transfer": {"bytes": 1, "total": 2}})
    assert out.getvalue() == "Pulling cr.hf.co/a/b:v1\n"


def test_lines_fit_narrow_terminals(monkeypatch):
    now = [0.0]
    for columns in (80, 50, 30):
        monkeypatch.setattr(shutil, "get_terminal_size", lambda c=columns: os.terminal_size((c, 40)))
        now[0] = 0.0
        out = Terminal()
        p = Progress(out, clock=lambda: now[0])
        p.event(PULL)
        p.event(layer("a0f760bcd130", 5_816_976_384, "downloading"))
        p.event({"transfer": {"bytes": 1, "total": 2_816_976_384}})
        p.event({"network": {"bytes": 1, "total": 900_000_000}})
        now[0] = 10.0
        p.event({"network": {"bytes": 600_000_000, "total": 900_000_000}})
        p.event({"transfer": {"bytes": 999_999_999, "total": 2_816_976_384}})
        assert screen(out)[-1].endswith(" · 18s left") == (columns >= 80)
        assert all(len(line) < columns for line in screen(out))
        assert ("━" in screen(out)[-1]) == (columns >= 80)


@pytest.mark.parametrize(
    "n, text",
    [
        (0, "0 B"),
        (999, "999 B"),
        (1_000, "1.00 kB"),
        (69_632, "69.6 kB"),
        (999_999_999, "1.00 GB"),
        (2_613_244_416, "2.61 GB"),
    ],
)
def test_sizes_have_three_digits(n, text):
    assert human(n) == text


def test_results_are_not_progress():
    assert not Progress(io.StringIO()).event({"digest": "sha256:abc"})


class Printed:
    """A helper that printed `lines`, and `errors` on stderr, and exited."""

    def __init__(self, lines: list[str], errors: list[str] | None = None):
        self.stdout = io.StringIO("".join(f"{line}\n" for line in lines))
        self.stderr = io.StringIO("".join(f"{line}\n" for line in errors or []))
        self.stdin = io.StringIO()

    def wait(self, timeout=None) -> int:
        return 0


def test_wait_draws_progress_and_returns_the_rest():
    out = io.StringIO()
    lines = [json.dumps(PULL), "not json", json.dumps({"digest": "sha256:abc"})]
    rest = Running(Printed(lines), Progress(out)).wait()
    assert rest == ["not json", '{"digest": "sha256:abc"}']
    assert out.getvalue() == "Pulling cr.hf.co/a/b:v1\n"


def test_wait_prints_the_helpers_stderr():
    out = io.StringIO()
    Running(Printed([json.dumps({"digest": "sha256:abc"})], ["WARN retrying"]), Progress(out)).wait()
    assert out.getvalue() == "WARN retrying\n"


def test_a_log_line_goes_above_the_frame(wide):
    out = Terminal()
    p = Progress(out, clock=lambda: 0.0)
    p.event(PULL)
    p.event(layer("aaa", 1_130_000_000, "downloading"))
    drawn = len(out.getvalue())
    p.log("ERROR range timed out")
    assert out.getvalue()[drawn:] == (
        "\x1b[2F\x1b[JERROR range timed out\n"
        "\x1b[2KPulling cr.hf.co/a/b:v1 · 1 layer, 1.13 GB\n"
        "\x1b[2K  ↓ aaa   1.13 GB  downloading\n"
        "\x1b[J"
    )
    p.event(layer("aaa", 1_130_000_000, "done"))
    assert out.getvalue().endswith(
        "\x1b[2F\x1b[2KPulling cr.hf.co/a/b:v1 · 1 layer, 1.13 GB\n\x1b[2K  ✓ aaa   1.13 GB  done\n\x1b[J"
    )

import io
import json
import os
import shutil

from hf_image.helper import Running
from hf_image.progress import Progress


class Terminal(io.StringIO):
    def isatty(self) -> bool:
        return True


def progress(id: str, status: str, current: int | None = None, total: int | None = None) -> dict:
    p = {"id": id, "status": status}
    if current is not None:
        p |= {"current": current, "total": total}
    return {"progress": p}


def test_a_pipe_gets_one_line_per_step():
    out = io.StringIO()
    p = Progress(out)
    for event in [
        progress("aaa", "Waiting"),
        progress("aaa", "Downloading", 0, 100),
        progress("aaa", "Downloading", 50, 100),
        progress("aaa", "Pull complete"),
        {"status": "Finishing the upload"},
    ]:
        assert p.event(event)
    assert out.getvalue() == "aaa: Waiting\naaa: Downloading\naaa: Pull complete\nFinishing the upload\n"


def test_a_terminal_redraws_every_layer_in_place(monkeypatch):
    monkeypatch.setattr(shutil, "get_terminal_size", lambda: os.terminal_size((120, 40)))
    out = Terminal()
    p = Progress(out)
    p.event(progress("aaa", "Downloading", 500_000, 1_000_000))
    p.event(progress("bbb", "Waiting"))
    p.event(progress("aaa", "Downloading", 1_000_000, 1_000_000))
    screen = out.getvalue()
    assert screen.count("\x1b[2F") == 1
    assert "[" + "=" * 25 + ">" + " " * 24 + "]    500kB/1MB" in screen
    assert "[" + "=" * 50 + "]      1MB/1MB" in screen
    assert screen.endswith("\x1b[2Kbbb: Waiting\n")


def test_bars_fit_narrow_terminals(monkeypatch):
    for columns in (80, 50, 30):
        monkeypatch.setattr(shutil, "get_terminal_size", lambda c=columns: os.terminal_size((c, 40)))
        out = Terminal()
        Progress(out).event(progress("a0f760bcd130", "Download complete", 999_999_999, 5_816_976_384))
        line = out.getvalue().removeprefix("\x1b[2K").rstrip("\n")
        assert len(line) < columns
        assert ("[" in line) == (columns >= 80)


def test_results_are_not_progress():
    assert not Progress(io.StringIO()).event({"digest": "sha256:abc"})


class Printed:
    """A helper that printed `lines` and exited."""

    def __init__(self, lines: list[str]):
        self.stdout = io.StringIO("".join(f"{line}\n" for line in lines))
        self.stdin = io.StringIO()

    def wait(self, timeout=None) -> int:
        return 0


def test_wait_draws_progress_and_returns_the_rest():
    out = io.StringIO()
    lines = [json.dumps(progress("aaa", "Pull complete")), "not json", json.dumps({"digest": "sha256:abc"})]
    rest = Running(Printed(lines)).wait(Progress(out))
    assert rest == ["not json", '{"digest": "sha256:abc"}']
    assert out.getvalue() == "aaa: Pull complete\n"

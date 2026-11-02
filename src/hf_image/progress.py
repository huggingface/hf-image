"""Progress of a pull or push, drawn like `docker pull`: a line per layer, redrawn in place on a
terminal, one line per step otherwise. The helper sends it as JSON lines before its result."""

from __future__ import annotations

import shutil
import sys
from typing import TextIO

BAR = 50
"""Widest bar, as Docker draws it; narrower to fit the terminal."""


class Progress:
    """Draws the helper's progress events."""

    def __init__(self, out: TextIO | None = None):
        self.out = out or sys.stderr
        self.tty = self.out.isatty()
        self.layers: dict[str, dict] = {}
        self.drawn = 0

    def event(self, event: dict) -> bool:
        """Draws `event` if it is progress; returns whether it was."""
        if "status" in event and len(event) == 1:
            self.message(event["status"])
            return True
        p = event.get("progress")
        if not isinstance(p, dict) or "id" not in p:
            return False
        previous = self.layers.get(p["id"], {}).get("status")
        self.layers[p["id"]] = p
        if self.tty:
            self.redraw()
        elif p.get("status") != previous:
            self.write(f"{p['id']}: {p.get('status', '')}\n")
        return True

    def message(self, text: str) -> None:
        if self.tty:
            self.redraw()
            self.write(f"{text}\n")
            self.drawn = 0
            self.layers.clear()
        else:
            self.write(f"{text}\n")

    def redraw(self) -> None:
        width = shutil.get_terminal_size().columns
        lines = [self.line(p, width - 1)[: width - 1] for p in self.layers.values()]
        up = f"\x1b[{self.drawn}F" if self.drawn else ""
        self.write(up + "".join(f"\x1b[2K{line}\n" for line in lines))
        self.drawn = len(lines)

    @staticmethod
    def line(p: dict, width: int) -> str:
        line = f"{p['id']}: {p.get('status', '')}"
        current, total = p.get("current"), p.get("total")
        if current is None:
            return line
        if not total:
            return f"{line:<32}{human(current):>8}"
        sizes = f"{human(current):>8}/{human(total)}"
        bar = min(BAR, width - 32 - 3 - len(sizes))
        if bar < 10:
            return f"{line:<32}{sizes}"
        done = min(bar, bar * current // total)
        return f"{line:<32}[{'=' * done}{'>' if done < bar else ''}{' ' * (bar - done - 1)}] {sizes}"

    def write(self, text: str) -> None:
        self.out.write(text)
        self.out.flush()


def human(n: int) -> str:
    """Bytes as Docker prints them (decimal units)."""
    for unit in ("B", "kB", "MB", "GB"):
        if n < 1000 or unit == "GB":
            return f"{n:.4g}{unit}" if unit != "B" else f"{n}B"
        n /= 1000
    return f"{n}B"

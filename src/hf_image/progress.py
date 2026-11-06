"""Progress of a pull or push, from the helper's JSON lines: a state per layer, one bar for the
layer bytes in the store (pull) or read (push), then a summary of what Xet saved. Redrawn in place
on a terminal; on a pipe, only the first and last lines."""

from __future__ import annotations

import shutil
import sys
import threading
import time
from collections.abc import Callable
from typing import TextIO

SMALL = 1_000_000
"""Layers below this size share one line; savings below it go unsaid."""
BAR = 40
SPINNER = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
STATES = {
    "waiting": ("·", "waiting"),
    "downloading": ("↓", "downloading"),
    "downloaded": ("↓", "downloaded"),
    "extracting": (None, "extracting"),
    "uploading": ("↑", "uploading"),
    "done": ("✓", "done"),
    "local": ("=", "already local"),
    "exists": ("=", "already in the registry"),
}
FINISHED = {"done", "local", "exists"}
SETTLE = 2.0
"""Seconds a stream moves before its mean rate is shown."""


class Progress:
    """Draws the helper's progress events."""

    def __init__(self, out: TextIO | None = None, clock: Callable[[], float] = time.monotonic):
        self.out = out or sys.stderr
        self.tty = self.out.isatty()
        self.clock = clock
        self.name = ""
        self.direction = "down"
        self.layers: dict[str, dict] = {}
        self.since: dict[str, float] = {}
        self.transfer: dict | None = None
        self.network: dict | None = None
        self.first: dict[str, tuple[float, int]] = {}
        """Per stream, the time and the bytes moved at its first event."""
        self.estimate: tuple[float, float] | None = None
        """When the bar last moved, and the seconds left then."""
        self.status = ""
        self.drawn = 0
        self.lock = threading.Lock()

    def event(self, event: dict) -> bool:
        """Draws `event` if it is progress; returns whether it was."""
        with self.lock:
            return self.apply(event)

    def log(self, line: str) -> None:
        """Prints a line of the helper's stderr above the progress."""
        with self.lock:
            if self.tty and self.drawn:
                self.redraw(above=line)
            else:
                self.write(f"{line}\n")

    def apply(self, event: dict) -> bool:
        if isinstance(event.get("image"), dict):
            self.name = event["image"].get("name", "")
            self.direction = event["image"].get("direction", "down")
            if not self.tty:
                self.write(f"{self.verb()} {self.name}\n")
        elif isinstance(event.get("layer"), dict) and "id" in event["layer"]:
            layer = event["layer"]
            if self.layers.get(layer["id"], {}).get("state") != layer.get("state"):
                self.since[layer["id"]] = self.clock()
            self.layers[layer["id"]] = layer
        elif isinstance(event.get("transfer"), dict):
            before, self.transfer = self.transfer or {}, event["transfer"]
            self.first.setdefault("transfer", (self.clock(), self.moved("transfer")))
            if self.transfer.get("bytes") != before.get("bytes") and (left := self.left()):
                if (shown := self.time_left()) is None or left < shown:
                    self.estimate = (self.clock(), left)
        elif isinstance(event.get("network"), dict):
            self.network = event["network"]
            self.first.setdefault("network", (self.clock(), self.moved("network")))
        elif "status" in event and len(event) == 1:
            self.status = event["status"]
            if not self.tty:
                self.write(f"{self.status}\n")
        elif isinstance(event.get("summary"), dict):
            self.transfer, self.network, self.first, self.estimate, self.status = None, None, {}, None, ""
            if self.tty:
                self.redraw()
            self.write("".join(f"{line}\n" for line in self.summary(event["summary"])))
            self.drawn = 0
            return True
        else:
            return False
        if self.tty:
            self.redraw()
        return True

    def verb(self, done: bool = False) -> str:
        if self.direction == "up":
            return "Pushed" if done else "Pushing"
        return "Pulled" if done else "Pulling"

    def redraw(self, above: str | None = None) -> None:
        """Draws the frame over the last one, with `above` printed first."""
        width = shutil.get_terminal_size().columns - 1
        lines = [self.header(), *self.layer_lines()]
        if self.transfer:
            lines.append(self.bar(width))
        if self.status:
            lines.append(f"  {self.status}")
        up = f"\x1b[{self.drawn}F" if self.drawn else ""
        if above is not None:
            up += f"\x1b[J{above}\n"
        self.write(up + "".join(f"\x1b[2K{line[:width]}\n" for line in lines) + "\x1b[J")
        self.drawn = len(lines)

    def header(self) -> str:
        sizes = [layer.get("size", 0) for layer in self.layers.values()]
        count = f"{len(sizes)} {'layer' if len(sizes) == 1 else 'layers'}"
        layers = f" · {count}, {human(sum(sizes))}" if sizes else ""
        return f"{self.verb()} {self.name}{layers}"

    def layer_lines(self) -> list[str]:
        now = self.clock()
        lines, small = [], []
        for layer in self.layers.values():
            if layer.get("size", 0) < SMALL:
                small.append(layer)
                continue
            state = layer.get("state", "")
            icon, text = STATES.get(state, ("?", state))
            if icon is None:
                icon = SPINNER[int(now * 10) % len(SPINNER)]
            if state == "extracting":
                text += f" {now - self.since.get(layer['id'], now):.1f}s"
            lines.append(f"  {icon} {layer['id']}  {human(layer.get('size', 0)):>8}  {text}")
        if small:
            icon = "✓" if all(layer.get("state") in FINISHED for layer in small) else "·"
            size = human(sum(layer.get("size", 0) for layer in small))
            lines.append(f"  {icon} {len(small)} small {'layer' if len(small) == 1 else 'layers'} ({size})")
        return lines

    def bar(self, width: int) -> str:
        t = self.transfer or {}
        done, total = t.get("bytes", 0), max(t.get("total", 0), t.get("bytes", 0))
        parts = [f"{100 * done // total if total else 0:>3}%", f"{human(done)} / {human(total)}"]
        network = self.direction == "down" and self.network
        if rate := self.rate("network" if network else "transfer"):
            parts.append(f"{'↓ ' if network else ''}{human(int(rate))}/s")
        if (left := self.time_left()) and done < total:
            parts.append(f"{left:.0f}s left")
        text = " · ".join(parts)
        size = min(BAR, width - 4 - len(text))
        if size < 10:
            return f"  {text}"
        filled = size * done // total if total else 0
        return f"  {'━' * filled}{'─' * (size - filled)}  {text}"

    def moved(self, stream: str) -> int:
        """Bytes of `stream`; on the bar, less those of layers a push finds in the registry as it goes."""
        if stream == "network":
            return (self.network or {}).get("bytes", 0)
        there = sum(layer.get("size", 0) for layer in self.layers.values() if layer.get("state") == "exists")
        return (self.transfer or {}).get("bytes", 0) - there

    def rate(self, stream: str) -> float | None:
        """The mean rate of `stream` since its first event, once it settled."""
        if stream not in self.first:
            return None
        t0, b0 = self.first[stream]
        elapsed, moved = self.clock() - t0, self.moved(stream) - b0
        return moved / elapsed if elapsed >= SETTLE and moved > 0 else None

    def left(self) -> float | None:
        """The seconds left at the bar's mean rate."""
        t = self.transfer or {}
        if not (rate := self.rate("transfer")):
            return None
        return max(t.get("total", 0) - t.get("bytes", 0), 0) / rate

    def time_left(self) -> float | None:
        """Counted down from the lowest estimate made as the bar moved, until it runs out: layer bytes land
        in bursts, and estimates are closest right after one."""
        if self.estimate is None:
            return None
        at, left = self.estimate
        left -= self.clock() - at
        return left if left >= 1 else None

    def summary(self, s: dict) -> list[str]:
        lines = [f"{self.verb(done=True)} {s.get('name', self.name)} in {s.get('secs', 0):.1f}s"]
        layers, wire, secs = s.get("layers", 0), s.get("wire", 0), s.get("wire_secs", 0)
        if self.direction == "up" and wire:
            rate = f" at {human(int(wire / secs))}/s" if secs else ""
            lines.append(f"  {human(layers)} of layers → {human(wire)} uploaded{rate}")
        elif self.direction == "up":
            lines.append(f"  {human(layers)} of layers, nothing to upload")
        elif wire:
            approx = "" if s.get("wire_exact", True) or s.get("cached", 0) < SMALL else "≈"
            rate = f" at {human(int(wire / secs))}/s" if secs else ""
            lines.append(f"  {human(layers)} of layers → {approx}{human(wire)} downloaded{rate}")
        else:
            lines.append(f"  {human(layers)} of layers, nothing to download")
        saved = []
        if self.direction == "down":
            if s.get("local"):
                saved.append(f"{human(s['local'])} already local")
            if s.get("cached", 0) >= SMALL:
                saved.append(f"{human(s['cached'])} from the Xet cache")
            fetched = layers - s.get("local", 0) - s.get("cached", 0)
            if wire and fetched / wire >= 1.1:
                saved.append(f"{fetched / wire:.1f}× compression")
        else:
            if s.get("existing"):
                saved.append(f"{human(s['existing'])} already in the registry")
            processed, dedup = s.get("processed", 0), s.get("deduplicated", 0)
            if dedup >= SMALL:
                saved.append(f"{human(dedup)} deduplicated by Xet ({100 * dedup // processed}%)")
            if wire and (processed - dedup) / wire >= 1.1:
                saved.append(f"{(processed - dedup) / wire:.1f}× compression")
        if saved:
            lines.append(f"  saved: {' · '.join(saved)}")
        return lines

    def write(self, text: str) -> None:
        self.out.write(text)
        self.out.flush()


def human(n: float) -> str:
    """Bytes in decimal units, three significant digits."""
    for unit in ("B", "kB", "MB", "GB", "TB"):
        if n < 999.5 or unit == "TB":
            break
        n /= 1000
    if unit == "B":
        return f"{int(n)} B"
    return f"{n:.0f} {unit}" if n >= 99.5 else f"{n:.1f} {unit}" if n >= 9.95 else f"{n:.2f} {unit}"

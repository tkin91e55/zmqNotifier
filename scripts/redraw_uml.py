"""
Watch a Mermaid diagram file and regenerate an SVG whenever it changes.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer


def run_mermaid_cli(input_file: Path) -> None:
    """Invoke mermaid-cli to produce an SVG for the given Mermaid source file."""
    output_file = input_file.with_suffix(".svg")
    try:
        subprocess.run(
            ["mmdc", "-i", str(input_file), "-o", str(output_file)],
            check=True,
        )
        print(f"Rendered {input_file.name} -> {output_file.name}")
    except subprocess.CalledProcessError as exc:
        print(f"mermaid-cli failed with exit code {exc.returncode}", file=sys.stderr)


class MermaidRedrawWatcher(FileSystemEventHandler):
    """Trigger mermaid-cli when the watched file changes."""

    def __init__(self, watch_file: Path) -> None:
        self.watch_file = watch_file.resolve()

    def on_modified(self, event):
        if not event.is_directory:
            self._maybe_run(Path(event.src_path))

    def on_created(self, event):
        if not event.is_directory:
            self._maybe_run(Path(event.src_path))

    def on_moved(self, event):
        if not event.is_directory:
            self._maybe_run(Path(event.dest_path))

    def _maybe_run(self, path: Path) -> None:
        if path.resolve() == self.watch_file:
            run_mermaid_cli(self.watch_file)


def parse_args() -> Path:
    parser = argparse.ArgumentParser(
        description="Watch a Mermaid .mmd file and redraw it via mermaid-cli."
    )
    parser.add_argument(
        "file",
        type=Path,
        help="Mermaid source file to watch (e.g. uml.mmd)",
    )
    args = parser.parse_args()
    return args.file


def main() -> None:
    watch_file = parse_args().resolve()

    if not watch_file.exists():
        print(f"File not found: {watch_file}", file=sys.stderr)
        sys.exit(1)

    run_mermaid_cli(watch_file)

    observer = Observer()
    handler = MermaidRedrawWatcher(watch_file)
    observer.schedule(handler, str(watch_file.parent), recursive=False)
    observer.start()
    print(f"Watching {watch_file} for changes. Press Ctrl+C to stop.")

    try:
        while True:
            observer.join(timeout=1)
    except KeyboardInterrupt:
        observer.stop()
    finally:
        observer.join()


if __name__ == "__main__":
    main()

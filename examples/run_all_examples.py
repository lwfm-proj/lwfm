#!/usr/bin/env python3
"""
Run all example scripts in this directory sequentially.

- Discovers files named like: ex<NUMBER>_*.py (e.g., ex0_hello_world.py, ex10_data_showcase.py)
- Sorts them by the numeric index: 0,1,2,...,10
- Executes each script with the current Python interpreter
- Streams output live, summarizes results

Usage examples:
  python run_all_examples.py                # run all examples
  python run_all_examples.py --list         # just list what would run
  python run_all_examples.py --dry-run      # show the plan without running
  python run_all_examples.py --start 3      # run from ex3_* onward
  python run_all_examples.py --end 6        # run through ex6_* only
  python run_all_examples.py --continue-on-error  # don't stop on first failure

Notes:
- Only files matching the regex r"^ex\\d+_.*\\.py$" are considered.
- Other helper scripts like workflow_tree_visualizer.py are ignored.
- Each example is run with cwd set to the examples/ directory so relative paths work.
"""

#pylint: disable = missing-class-docstring, missing-function-docstring, broad-exception-caught

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple

EXAMPLE_PATTERN = re.compile(r"^ex(\d+)_.*\.py$")


@dataclass
class Example:
    index: int
    path: Path

    @property
    def name(self) -> str:
        return self.path.name


def discover_examples(examples_dir: Path) -> List[Example]:
    examples: List[Example] = []
    for entry in sorted(examples_dir.iterdir()):
        if not entry.is_file():
            continue
        m = EXAMPLE_PATTERN.match(entry.name)
        if not m:
            continue
        idx = int(m.group(1))
        examples.append(Example(index=idx, path=entry))
    # Sort by numeric index, then by name to ensure stable order when multiple share same index
    examples.sort(key=lambda e: (e.index, e.name))
    return examples


def run_example(example: Example, cwd: Path) -> int:
    print(f"\n\033[96m▶️  === Running {example.name} (index {example.index}) ===\033[0m",
        flush=True)
    # Use current interpreter
    cmd = [sys.executable, str(example.path)]
    try:
        # Stream output directly to this terminal
        completed = subprocess.run(cmd, cwd=cwd, check=False)
        rc = completed.returncode
    except KeyboardInterrupt:
        print("Interrupted by user.", flush=True)
        return 130
    except Exception as e:
        print(f"Error executing {example.name}: {e}", flush=True)
        return 1
    color = "\033[92m" if rc == 0 else "\033[91m"
    status_icon = "✅" if rc == 0 else "❌"
    print(f"{color}{status_icon} --- Finished {example.name} with exit code {rc} ---\033[0m", flush=True)
    return rc


def parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run all ex#_*.py examples in order.")
    p.add_argument("--start", type=int, default=None,
        help="Minimum example index to include (e.g., 3 to start at ex3_*)")
    p.add_argument("--end", type=int, default=None,
        help="Maximum example index to include (e.g., 6 to stop at ex6_*)")
    p.add_argument("--list", action="store_true",
        help="Only list matching examples without running")
    p.add_argument("--dry-run", action="store_true",
        help="Show which examples would run without executing")
    p.add_argument("--continue-on-error", action="store_true",
        help="Do not stop on first failure; run all and report summary")
    p.add_argument("--fail-fast", dest="fail_fast", action="store_true",
        help="Stop on first failure (default behavior)")
    p.add_argument("--no-fail-fast", dest="fail_fast", action="store_false",
        help="Alias for --continue-on-error")
    p.set_defaults(fail_fast=True)
    return p.parse_args(argv)


def filter_examples(examples: List[Example], start: int | None, end: int | None) -> List[Example]:
    out = []
    for ex in examples:
        if start is not None and ex.index < start:
            continue
        if end is not None and ex.index > end:
            continue
        out.append(ex)
    return out


def main(argv: List[str]) -> int:
    here = Path(__file__).resolve().parent
    examples = discover_examples(here)

    args = parse_args(argv)

    if args.continue_on_error:
        args.fail_fast = False

    examples = filter_examples(examples, args.start, args.end)

    if not examples:
        print("No examples found matching criteria.")
        return 0

    print("Examples to run (in order):")
    for ex in examples:
        print(f"  - ex{ex.index}: {ex.name}")

    if args.list or args.dry_run:
        return 0

    failures: List[Tuple[Example, int]] = []
    for ex in examples:
        rc = run_example(ex, cwd=here)
        if rc != 0:
            failures.append((ex, rc))
            if args.fail_fast:
                print("Fail-fast enabled; stopping.")
                break

    # Summary
    print("\n===== Summary =====")
    total = len(examples)
    failed = len(failures)
    passed = total - failed
    print(f"Total: {total}, Passed: {passed}, Failed: {failed}")
    # Color-coded chart of example results
    print("Results:")
    for ex in examples:
        ok = True
        for f, _rc in failures:
            if f.index == ex.index and f.name == ex.name:
                ok = False
                break
        color = "\033[32m" if ok else "\033[31m"
        status = "PASS" if ok else "FAIL"
        print(f"  {color}{status}\033[0m  ex{ex.index}: {ex.name}")
    if failures:
        print("Failures:")
        for ex, rc in failures:
            print(f"  - {ex.name} (exit {rc})")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

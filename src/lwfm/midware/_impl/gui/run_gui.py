#!/usr/bin/env python3
"""
Launcher for the lwfm GUI under midware/_impl/gui.
Adds the repository's src/ to sys.path and imports app.main.
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.abspath(os.path.join(HERE, "../../../../"))  # repo/src
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

# Fail early with a clear message if Tkinter isn't available
try:
    import importlib
    importlib.import_module("tkinter")
except ImportError:  # pragma: no cover - env specific
    sys.stderr.write(
        "Tkinter (_tkinter) is not available in your Python.\n"
        "Install a Python build with Tk support (e.g., python.org macOS installer),\n"
        "or install Homebrew tcl-tk and use a Python built against it.\n"
    )
    sys.exit(1)

from lwfm.midware._impl.gui.app import main  # type: ignore

if __name__ == "__main__":
    main()

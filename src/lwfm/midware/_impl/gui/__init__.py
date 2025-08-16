"""GUI subpackage: split dialogs into focused modules to keep app.py lean.

This package exposes helper windows used by LwfmGui:
- metasheets: Search metasheets dialog
- events: Events viewer dialog
- workflow: Workflow details dialog (tabs: Overview, Jobs, Data, Graph)
- files: Files list dialog
- status: Status history dialog
- server_log: Live server log viewer

All functions are designed to be called from a Tk root or Toplevel context and
take the parent window as first argument to preserve existing behavior.
"""

from .metasheets import open_metasheets_dialog  # noqa: F401
from .workflow import open_workflow_dialog  # noqa: F401
"""lwfm GUI package (midware/_impl/gui)"""

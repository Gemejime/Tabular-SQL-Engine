"""Project ``cmd`` package preserving Python's stdlib ``cmd`` interface."""

from __future__ import annotations

import importlib.util
import sysconfig
from pathlib import Path

_stdlib_cmd_path = Path(sysconfig.get_path("stdlib")) / "cmd.py"
_spec = importlib.util.spec_from_file_location("_stdlib_cmd", _stdlib_cmd_path)
if _spec is not None and _spec.loader is not None:
    _stdlib_cmd = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_stdlib_cmd)
    for _name, _value in vars(_stdlib_cmd).items():
        if _name not in {
            "__name__",
            "__doc__",
            "__package__",
            "__loader__",
            "__spec__",
            "__file__",
            "__cached__",
            "__path__",
        }:
            globals()[_name] = _value

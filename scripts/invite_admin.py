from __future__ import annotations

"""Backward-compatible wrapper for the MPbM invite operator CLI.

Prefer scripts/mpbm_invites.py in new docs and operator aliases. This file is
kept so older runbooks and shell history do not break during rollout.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from invite_store import main


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

from dataclasses import replace
from typing import Any

from verixa.cli.app import DEFAULT_APP_DEPS, create_app


def build_app(**overrides: Any):
    return create_app(replace(DEFAULT_APP_DEPS, **overrides))

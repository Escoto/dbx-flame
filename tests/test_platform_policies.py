"""Tests for policies.platform — the framework's own, non-configurable checks."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dbx_flame.policies.platform import PlatformPolicy, PlatformPolicyViolation, violate


def test_a_violation_is_logged_and_flushed_before_it_raises():
    ctx = MagicMock()

    with pytest.raises(PlatformPolicyViolation) as failure:
        violate(ctx, PlatformPolicy.UNSTAMPED_FILE, "enrichment", "no stamp", total=2)

    assert failure.value.policy is PlatformPolicy.UNSTAMPED_FILE
    assert str(failure.value) == "unstamped_file: no stamp"
    assert [call[0] for call in ctx.logger.method_calls] == ["error", "flush"]
    assert ctx.logger.error.call_args.kwargs == {
        "name": "unstamped_file",
        "source": "enrichment",
        "description": "no stamp",
        "total": 2,
        "metadata": None,
    }

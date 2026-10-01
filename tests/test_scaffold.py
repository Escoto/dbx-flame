"""Placeholder test — validates the package imports correctly."""

# TaskConfig and Requirements are unused on purpose: here the import *is* the
# assertion, so removing them would drop coverage rather than dead code.
from importlib import metadata

import dbx_flame
from dbx_flame.context.config import (  # noqa: F401
    Origin,
    TaskConfig,
    Verb,
)
from dbx_flame.output.base import Requirements, Writer  # noqa: F401
from dbx_flame.output.registry import VERB_REQUIREMENTS
from dbx_flame.pipelines.base import SourcePipeline
from dbx_flame.policies.base import PolicyResult, Severity
from dbx_flame.typecast.models import CastConfiguration


def test_version():
    """Pinned to the distribution, not to a literal.

    The release workflow bumps pyproject.toml and rewrites __version__ from it, so a
    hardcoded string here would fail on the first release instead of catching the drift
    it exists to catch.
    """
    assert dbx_flame.__version__ == metadata.version("dbx-flame")


def test_enums():
    assert Origin.CSV == "csv"
    assert Verb.COMPLETE_DELTA == "complete_delta"
    assert Severity.FAIL == "fail"


def test_verb_requirements_complete():
    for verb in Verb:
        assert verb in VERB_REQUIREMENTS, f"Missing requirements for verb: {verb}"


def test_cast_configuration_defaults():
    # validation is switched by typing.validate_casts, not by the detached YAML
    cfg = CastConfiguration()
    assert cfg.columns == []


def test_policy_result():
    result = PolicyResult(policy="id_is_null", severity=Severity.WARN)
    assert result.failed_count == 0


def test_protocols_are_runtime_checkable():
    assert hasattr(SourcePipeline, "__protocol_attrs__") or hasattr(
        SourcePipeline, "__abstractmethods__"
    )
    assert hasattr(Writer, "__protocol_attrs__") or hasattr(Writer, "__abstractmethods__")

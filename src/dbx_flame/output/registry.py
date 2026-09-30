"""Verb → Requirements registry, derived from what each writer declares.

Adding a verb means adding a Writer with its own `requires`; nothing here or in the
Start layer needs a matching edit.
"""

from __future__ import annotations

from dbx_flame.context.config import Verb
from dbx_flame.output.base import Requirements, Writer
from dbx_flame.output.delta.append import AppendWriter
from dbx_flame.output.delta.complete_delta import CompleteDeltaWriter
from dbx_flame.output.delta.full import FullWriter
from dbx_flame.output.delta.scd2 import Scd2Writer
from dbx_flame.output.delta.upsert import UpsertWriter

WRITERS = (
    AppendWriter,
    FullWriter,
    UpsertWriter,
    Scd2Writer,
    CompleteDeltaWriter,
)

VERB_REQUIREMENTS: dict[Verb, Requirements] = {w.verb: w.requires for w in WRITERS}

WRITER_BY_VERB: dict[Verb, type[Writer]] = {w.verb: w for w in WRITERS}

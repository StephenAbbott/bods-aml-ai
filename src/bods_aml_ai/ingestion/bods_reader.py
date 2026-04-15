"""Read BODS v0.4 statements from JSON or JSONL files."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Iterator

from bods_aml_ai.ingestion.models import (
    BODSEntityStatement,
    BODSPersonStatement,
    BODSRelationshipStatement,
)

logger = logging.getLogger(__name__)


def classify_statement(data: dict) -> str | None:
    """Determine the BODS statement type from a raw dict.

    Supports both BODS 0.4 (recordType field) and legacy formats
    (statementType field).
    """
    record_type = data.get("recordType")
    if record_type:
        return record_type

    statement_type = data.get("statementType")
    if statement_type == "personStatement":
        return "person"
    elif statement_type == "entityStatement":
        return "entity"
    elif statement_type == "ownershipOrControlStatement":
        return "relationship"

    return None


def parse_statement(
    data: dict,
) -> BODSPersonStatement | BODSEntityStatement | BODSRelationshipStatement | None:
    """Parse a raw dict into a typed BODS statement model."""
    stmt_type = classify_statement(data)
    if stmt_type == "person":
        return BODSPersonStatement.from_dict(data)
    elif stmt_type == "entity":
        return BODSEntityStatement.from_dict(data)
    elif stmt_type == "relationship":
        return BODSRelationshipStatement.from_dict(data)
    else:
        logger.warning("Unknown statement type in: %s", data.get("statementId", "?"))
        return None


def read_bods_file(
    filepath: Path | str,
) -> Iterator[BODSPersonStatement | BODSEntityStatement | BODSRelationshipStatement]:
    """Read BODS statements from a JSON array or JSONL file.

    Supports:
    - JSON file containing an array of statement objects
    - JSONL file with one statement per line
    """
    filepath = Path(filepath)
    logger.info("Reading BODS file: %s", filepath)

    if filepath.suffix == ".jsonl":
        yield from _read_jsonl(filepath)
    else:
        yield from _read_json(filepath)


def _read_json(
    filepath: Path,
) -> Iterator[BODSPersonStatement | BODSEntityStatement | BODSRelationshipStatement]:
    with open(filepath, encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        items = [data]
    else:
        logger.error("Unexpected JSON structure in %s", filepath)
        return

    for item in items:
        stmt = parse_statement(item)
        if stmt:
            yield stmt


def _read_jsonl(
    filepath: Path,
) -> Iterator[BODSPersonStatement | BODSEntityStatement | BODSRelationshipStatement]:
    with open(filepath, encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                stmt = parse_statement(data)
                if stmt:
                    yield stmt
            except json.JSONDecodeError as e:
                logger.warning("Skipping invalid JSON at line %d: %s", line_num, e)

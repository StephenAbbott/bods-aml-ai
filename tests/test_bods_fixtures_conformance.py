"""Conformance tests against the shared bods-fixtures pack.

The pack (https://github.com/StephenAbbott/bods-fixtures) is the canonical
source of truth for BODS v0.4 shape across the adapter ecosystem. Passing
these tests means bods-aml-ai's ingestion + transformation layers agree
with the canonical envelope that other adapters also target. Failures
here indicate either genuine bugs in our handling or a fixture-pack bug
that needs reporting upstream.

AML-specific concerns tested here:
- declared-unknown UBOs (inline `unspecifiedReason`) must not be silently
  dropped, per FATF guidance — anonymity is itself a risk signal.
- circular ownership must terminate and emit supplementary data for each
  leg of the cycle.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from bods_fixtures import list_cases, load

from bods_aml_ai.config import TransformConfig
from bods_aml_ai.pipeline import AMLPipeline

ALL_CASES = list_cases()


def _write_fixture_to_tmp(fixture_statements: list[dict], tmp_path: Path) -> Path:
    """bods-aml-ai's ingestion layer is file-based; adapt the in-memory
    fixture by writing it to a tmp JSON array the pipeline can read."""
    path = tmp_path / "fixture.bods.json"
    path.write_text(json.dumps(fixture_statements))
    return path


def _run_pipeline(fixture_statements: list[dict], tmp_path: Path) -> AMLPipeline:
    input_path = _write_fixture_to_tmp(fixture_statements, tmp_path)
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    pipeline = AMLPipeline(TransformConfig(output_dir=str(output_dir)))
    pipeline.process_file(input_path)
    pipeline.finalize()
    return pipeline


def _read_ndjson(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


@pytest.mark.parametrize("name", ALL_CASES)
def test_pipeline_does_not_raise(name, tmp_path):
    """Every canonical fixture must pass through ingestion and all four
    transform stages without raising."""
    fixture = load(name)
    _run_pipeline(fixture.statements, tmp_path)


@pytest.mark.parametrize("name", ALL_CASES)
def test_statement_counts_match_fixture(name, tmp_path):
    """Ingestion must classify every record correctly — if counts diverge,
    something in the fixture is being dropped or misrouted."""
    fixture = load(name)
    pipeline = _run_pipeline(fixture.statements, tmp_path)

    expected = {
        "entity": len(fixture.by_record_type("entity")),
        "person": len(fixture.by_record_type("person")),
        "relationship": len(fixture.by_record_type("relationship")),
    }
    actual = {k: v for k, v in pipeline.statement_counts.items() if k != "total"}
    assert actual == expected, (
        f"{name}: ingestion counts {actual} != fixture counts {expected}"
    )


def test_direct_ownership_produces_consumer_and_company_parties(tmp_path):
    """The baseline UBO fixture must emit at least one CONSUMER (person) and
    one COMPANY (entity) Party row."""
    fixture = load("core/01-direct-ownership")
    _run_pipeline(fixture.statements, tmp_path)

    parties = _read_ndjson(tmp_path / "out" / "party.ndjson")
    party_types = {p.get("type") for p in parties}
    assert "CONSUMER" in party_types, f"no CONSUMER in party types: {party_types}"
    assert "COMPANY" in party_types, f"no COMPANY in party types: {party_types}"


def test_direct_ownership_supplementary_has_ownership_percentage(tmp_path):
    """Ownership share must surface as a bo_ownership_pct_* attribute on the
    owner's PartySupplementaryData row — otherwise the AML model loses the
    most load-bearing BO signal."""
    fixture = load("core/01-direct-ownership")
    _run_pipeline(fixture.statements, tmp_path)

    rows = _read_ndjson(tmp_path / "out" / "party_supplementary_data.ndjson")
    pct_attrs = [
        r for r in rows
        if isinstance(r.get("party_supplementary_data_id"), str)
        and r["party_supplementary_data_id"].startswith("bo_ownership_pct_")
    ]
    assert pct_attrs, (
        "expected at least one bo_ownership_pct_* supplementary row; got "
        f"attributes: {[r.get('party_supplementary_data_id') for r in rows]}"
    )


def test_circular_ownership_produces_supplementary_rows_for_both_edges(tmp_path):
    """A↔B ownership cycle must terminate and emit bo_ownership_pct attrs
    for both legs. A missing leg indicates either graph-walk deduplication
    or silent skip of the reverse edge."""
    fixture = load("edge-cases/10-circular-ownership")
    _run_pipeline(fixture.statements, tmp_path)

    rows = _read_ndjson(tmp_path / "out" / "party_supplementary_data.ndjson")
    pct_attrs = {
        r["party_supplementary_data_id"] for r in rows
        if isinstance(r.get("party_supplementary_data_id"), str)
        and r["party_supplementary_data_id"].startswith("bo_ownership_pct_")
    }
    assert len(pct_attrs) >= 2, (
        f"expected ownership percentage attrs for both legs of the A↔B cycle; "
        f"got {pct_attrs}"
    )


def test_anonymous_interested_party_is_not_silently_dropped(tmp_path):
    """Declared-unknown UBO (inline `unspecifiedReason`) must leave a trace
    in the AML output — either a supplementary row flagging the opacity or
    the reason code surfaced somewhere. FATF treats declared-unknown UBOs
    as a material risk signal; silently dropping them understates risk."""
    fixture = load("edge-cases/11-anonymous-person")
    _run_pipeline(fixture.statements, tmp_path)

    parties = _read_ndjson(tmp_path / "out" / "party.ndjson")
    supplementary = _read_ndjson(tmp_path / "out" / "party_supplementary_data.ndjson")

    # The known entity (Opaque Holdings SA) must still produce a Party row —
    # an unresolvable interestedParty shouldn't wipe out the subject too.
    assert parties, "no Party rows emitted for a fixture with a known entity"

    # The declared-unknown UBO must survive as a supplementary signal —
    # either a generic `bo_has_declared_unknown_owner` flag or a
    # reason-specific `bo_unspecified_reason_{reason}` flag on the subject.
    data_ids = {
        r.get("party_supplementary_data_id") for r in supplementary
    }
    has_unknown_flag = "bo_has_declared_unknown_owner" in data_ids
    has_reason_flag = any(
        isinstance(d, str) and d.startswith("bo_unspecified_reason_")
        for d in data_ids
    )
    assert has_unknown_flag or has_reason_flag, (
        "declared-unknown UBO was silently dropped from AML output. "
        f"supplementary data IDs emitted: {data_ids}. Per FATF, declared-"
        "unknown UBOs are a risk flag in their own right."
    )

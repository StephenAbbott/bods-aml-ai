"""Tests for the end-to-end BODS -> AML AI pipeline."""

import json
import tempfile
from pathlib import Path

import pytest

from bods_aml_ai.config import TransformConfig
from bods_aml_ai.pipeline import AMLPipeline

SAMPLE_INPUT = Path(__file__).parent.parent / "sample_input.json"


class TestPipeline:
    def test_process_sample_file(self):
        pipeline = AMLPipeline(TransformConfig(output_dir=tempfile.mkdtemp()))
        count = pipeline.process_file(SAMPLE_INPUT)
        assert count == 7  # 2 entities + 2 persons + 3 relationships

    def test_statement_counts(self):
        pipeline = AMLPipeline(TransformConfig(output_dir=tempfile.mkdtemp()))
        pipeline.process_file(SAMPLE_INPUT)
        counts = pipeline.statement_counts
        assert counts["person"] == 2
        assert counts["entity"] == 2
        assert counts["relationship"] == 3
        assert counts["total"] == 7

    def test_full_pipeline_produces_output_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = TransformConfig(
                output_dir=tmpdir,
                include_account_party_links=True,
                validity_start_time="2024-06-15T00:00:00Z",
            )
            pipeline = AMLPipeline(config)
            pipeline.process_file(SAMPLE_INPUT)
            counts = pipeline.finalize()

            # Check files exist
            assert (Path(tmpdir) / "party.ndjson").exists()
            assert (Path(tmpdir) / "party_supplementary_data.ndjson").exists()
            assert (Path(tmpdir) / "account_party_link.ndjson").exists()

            # Check party count: 2 entities + 2 persons = 4
            assert counts["party"] == 4

            # Check supplementary data has rows
            assert counts["party_supplementary_data"] > 0

            # Check account party link has rows
            assert counts["account_party_link"] > 0

    def test_pipeline_without_account_links(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = TransformConfig(
                output_dir=tmpdir,
                include_account_party_links=False,
                validity_start_time="2024-06-15T00:00:00Z",
            )
            pipeline = AMLPipeline(config)
            pipeline.process_file(SAMPLE_INPUT)
            counts = pipeline.finalize()

            assert "account_party_link" not in counts
            assert not (Path(tmpdir) / "account_party_link.ndjson").exists()

    def test_output_is_valid_ndjson(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = TransformConfig(
                output_dir=tmpdir,
                validity_start_time="2024-06-15T00:00:00Z",
            )
            pipeline = AMLPipeline(config)
            pipeline.process_file(SAMPLE_INPUT)
            pipeline.finalize()

            # Every line in every output file should be valid JSON
            for ndjson_file in Path(tmpdir).glob("*.ndjson"):
                with open(ndjson_file) as f:
                    for line_num, line in enumerate(f, 1):
                        line = line.strip()
                        if line:
                            try:
                                json.loads(line)
                            except json.JSONDecodeError:
                                pytest.fail(
                                    f"Invalid JSON at {ndjson_file.name}:{line_num}"
                                )

    def test_party_rows_have_required_fields(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = TransformConfig(
                output_dir=tmpdir,
                validity_start_time="2024-06-15T00:00:00Z",
            )
            pipeline = AMLPipeline(config)
            pipeline.process_file(SAMPLE_INPUT)
            pipeline.finalize()

            with open(Path(tmpdir) / "party.ndjson") as f:
                for line in f:
                    row = json.loads(line)
                    # AML AI required fields
                    assert "party_id" in row
                    assert "validity_start_time" in row
                    assert "type" in row
                    assert row["type"] in ("CONSUMER", "COMPANY")

    def test_supplementary_data_has_required_fields(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = TransformConfig(
                output_dir=tmpdir,
                validity_start_time="2024-06-15T00:00:00Z",
            )
            pipeline = AMLPipeline(config)
            pipeline.process_file(SAMPLE_INPUT)
            pipeline.finalize()

            with open(Path(tmpdir) / "party_supplementary_data.ndjson") as f:
                for line in f:
                    row = json.loads(line)
                    assert "party_supplementary_data_id" in row
                    assert "party_id" in row
                    assert "supplementary_data_payload" in row
                    assert isinstance(
                        row["supplementary_data_payload"]["value"], float
                    )

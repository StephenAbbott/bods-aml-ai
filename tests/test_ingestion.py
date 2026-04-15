"""Tests for BODS statement ingestion and parsing."""

import json
import tempfile
from pathlib import Path

from bods_aml_ai.ingestion.bods_reader import classify_statement, read_bods_file
from bods_aml_ai.ingestion.models import (
    BODSEntityStatement,
    BODSInterest,
    BODSPersonStatement,
    BODSRelationshipStatement,
)

SAMPLE_INPUT = Path(__file__).parent.parent / "sample_input.json"


class TestClassifyStatement:
    def test_classify_bods04_person(self):
        assert classify_statement({"recordType": "person"}) == "person"

    def test_classify_bods04_entity(self):
        assert classify_statement({"recordType": "entity"}) == "entity"

    def test_classify_bods04_relationship(self):
        assert classify_statement({"recordType": "relationship"}) == "relationship"

    def test_classify_legacy_person(self):
        assert classify_statement({"statementType": "personStatement"}) == "person"

    def test_classify_legacy_entity(self):
        assert classify_statement({"statementType": "entityStatement"}) == "entity"

    def test_classify_legacy_ooc(self):
        assert classify_statement(
            {"statementType": "ownershipOrControlStatement"}
        ) == "relationship"

    def test_classify_unknown(self):
        assert classify_statement({"foo": "bar"}) is None


class TestReadBodsFile:
    def test_read_sample_json(self):
        statements = list(read_bods_file(SAMPLE_INPUT))
        assert len(statements) == 7

        persons = [s for s in statements if isinstance(s, BODSPersonStatement)]
        entities = [s for s in statements if isinstance(s, BODSEntityStatement)]
        rels = [s for s in statements if isinstance(s, BODSRelationshipStatement)]

        assert len(persons) == 2
        assert len(entities) == 2
        assert len(rels) == 3

    def test_read_jsonl(self):
        """Test reading JSONL format."""
        with open(SAMPLE_INPUT) as f:
            data = json.load(f)

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".jsonl", delete=False
        ) as tmp:
            for item in data:
                tmp.write(json.dumps(item) + "\n")
            tmp_path = tmp.name

        statements = list(read_bods_file(tmp_path))
        assert len(statements) == 7
        Path(tmp_path).unlink()


class TestBODSInterest:
    def test_exact_share(self):
        interest = BODSInterest.from_dict({"share": {"exact": 60.0}})
        assert interest.best_share_percentage() == 60.0

    def test_range_share_midpoint(self):
        interest = BODSInterest.from_dict(
            {"share": {"minimum": 25.0, "maximum": 50.0}}
        )
        assert interest.best_share_percentage() == 37.5

    def test_minimum_only(self):
        interest = BODSInterest.from_dict({"share": {"minimum": 25.0}})
        assert interest.best_share_percentage() == 25.0

    def test_no_share(self):
        interest = BODSInterest.from_dict({"type": "votingRights"})
        assert interest.best_share_percentage() is None


class TestPersonStatement:
    def test_display_name_full(self, sample_person):
        assert sample_person.display_name() == "Jane Smith"

    def test_nationalities(self, sample_person):
        assert sample_person.nationalities == ["GB"]

    def test_birth_date(self, sample_person):
        assert sample_person.birth_date == "1980-05-14"


class TestEntityStatement:
    def test_name(self, sample_entity):
        assert sample_entity.name == "Acme Holdings Ltd"

    def test_jurisdiction(self, sample_entity):
        assert sample_entity.jurisdiction_code == "GB"

    def test_founding_date(self, sample_entity):
        assert sample_entity.founding_date == "2015-03-20"

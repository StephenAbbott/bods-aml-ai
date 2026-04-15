"""Tests for BODS -> AML AI Party transformation."""

from bods_aml_ai.transform.parties import (
    transform_entity_to_party,
    transform_person_to_party,
)


class TestPersonToParty:
    def test_basic_person_fields(self, sample_person):
        row = transform_person_to_party(sample_person)
        assert row["party_id"] == "person-jane-smith-1980"
        assert row["type"] == "CONSUMER"
        assert row["name"] == "Jane Smith"
        assert row["source_system"] == "BODS"

    def test_person_birth_date(self, sample_person):
        row = transform_person_to_party(sample_person)
        assert row["birth_date"] == "1980-05-14"

    def test_person_nationalities(self, sample_person):
        row = transform_person_to_party(sample_person)
        assert row["nationalities"] == [{"region_code": "GB"}]

    def test_person_addresses(self, sample_person):
        row = transform_person_to_party(sample_person)
        assert len(row["addresses"]) == 1
        addr = row["addresses"][0]
        assert addr["address_line"] == "42 Baker Street, London"
        assert addr["post_code"] == "W1U 3BW"
        assert addr["region_code"] == "GB"

    def test_person_validity_timestamp(self, sample_person):
        row = transform_person_to_party(sample_person)
        assert row["validity_start_time"] == "2024-06-15T00:00:00Z"

    def test_person_no_establishment_date(self, sample_person):
        row = transform_person_to_party(sample_person)
        assert "establishment_date" not in row


class TestEntityToParty:
    def test_basic_entity_fields(self, sample_entity):
        row = transform_entity_to_party(sample_entity)
        assert row["party_id"] == "gb-coh-12345678"
        assert row["type"] == "COMPANY"
        assert row["name"] == "Acme Holdings Ltd"
        assert row["source_system"] == "BODS"

    def test_entity_establishment_date(self, sample_entity):
        row = transform_entity_to_party(sample_entity)
        assert row["establishment_date"] == "2015-03-20"

    def test_entity_residencies_from_jurisdiction(self, sample_entity):
        row = transform_entity_to_party(sample_entity)
        assert row["residencies"] == [{"region_code": "GB"}]

    def test_entity_addresses(self, sample_entity):
        row = transform_entity_to_party(sample_entity)
        assert len(row["addresses"]) == 1
        addr = row["addresses"][0]
        assert addr["address_line"] == "10 Downing Street, London"
        assert addr["region_code"] == "GB"

    def test_entity_no_birth_date(self, sample_entity):
        row = transform_entity_to_party(sample_entity)
        assert "birth_date" not in row

"""Tests for BODS -> AML AI PartySupplementaryData transformation."""

from bods_aml_ai.transform.supplementary import transform_relationships_to_supplementary


class TestSupplementaryData:
    def test_generates_ownership_percentage(self, sample_relationships):
        rows = transform_relationships_to_supplementary(sample_relationships)
        pct_rows = [
            r for r in rows
            if r["party_supplementary_data_id"].startswith("bo_ownership_pct_")
        ]
        assert len(pct_rows) >= 1

        # Jane owns 60% of Acme
        jane_acme = [
            r for r in pct_rows
            if r["party_id"] == "person-jane-smith-1980"
            and "gb-coh-12345678" in r["party_supplementary_data_id"]
        ]
        assert len(jane_acme) == 1
        assert jane_acme[0]["supplementary_data_payload"]["value"] == 60.0

    def test_beneficial_owner_flag(self, sample_relationships):
        rows = transform_relationships_to_supplementary(sample_relationships)
        bo_rows = [
            r for r in rows
            if r["party_supplementary_data_id"] == "bo_is_beneficial_owner"
        ]
        # Jane and Hans are both BOs
        bo_party_ids = {r["party_id"] for r in bo_rows}
        assert "person-jane-smith-1980" in bo_party_ids
        assert "person-hans-mueller-1975" in bo_party_ids
        # BVI company is NOT a BO
        assert "vg-fsc-98765" not in bo_party_ids

    def test_direct_indirect_flag(self, sample_relationships):
        rows = transform_relationships_to_supplementary(sample_relationships)
        direct_rows = [
            r for r in rows
            if r["party_supplementary_data_id"] == "bo_is_direct"
        ]
        # All sample relationships are direct
        for row in direct_rows:
            assert row["supplementary_data_payload"]["value"] == 1.0

    def test_subject_aggregate_owner_count(self, sample_relationships):
        rows = transform_relationships_to_supplementary(sample_relationships)
        count_rows = [
            r for r in rows
            if r["party_supplementary_data_id"] == "bo_num_owners"
        ]
        # Acme has 2 owners (Jane + BVI), BVI has 1 owner (Hans)
        acme_count = [r for r in count_rows if r["party_id"] == "gb-coh-12345678"]
        assert len(acme_count) == 1
        assert acme_count[0]["supplementary_data_payload"]["value"] == 2.0

        bvi_count = [r for r in count_rows if r["party_id"] == "vg-fsc-98765"]
        assert len(bvi_count) == 1
        assert bvi_count[0]["supplementary_data_payload"]["value"] == 1.0

    def test_subject_total_ownership(self, sample_relationships):
        rows = transform_relationships_to_supplementary(sample_relationships)
        total_rows = [
            r for r in rows
            if r["party_supplementary_data_id"] == "bo_total_identified_ownership_pct"
        ]
        # Acme: 60% (Jane) + 40% (BVI) = 100%
        acme_total = [r for r in total_rows if r["party_id"] == "gb-coh-12345678"]
        assert len(acme_total) == 1
        assert acme_total[0]["supplementary_data_payload"]["value"] == 100.0

    def test_subject_has_beneficial_owner(self, sample_relationships):
        rows = transform_relationships_to_supplementary(sample_relationships)
        has_bo_rows = [
            r for r in rows
            if r["party_supplementary_data_id"] == "bo_has_beneficial_owner"
        ]
        # Both Acme and BVI have at least one BO
        bo_subjects = {r["party_id"] for r in has_bo_rows}
        assert "gb-coh-12345678" in bo_subjects
        assert "vg-fsc-98765" in bo_subjects

    def test_all_values_are_float(self, sample_relationships):
        rows = transform_relationships_to_supplementary(sample_relationships)
        for row in rows:
            val = row["supplementary_data_payload"]["value"]
            assert isinstance(val, float), (
                f"Value {val} for {row['party_supplementary_data_id']} is not float"
            )

    def test_skips_component_statements(self):
        """Component statements (intermediate chain pieces) should be skipped."""
        from bods_aml_ai.ingestion.models import BODSRelationshipStatement

        component = BODSRelationshipStatement.from_dict({
            "statementId": "comp-1",
            "statementDate": "2024-06-15",
            "recordId": "rel-comp",
            "recordType": "relationship",
            "recordDetails": {
                "isComponent": True,
                "subject": "entity-a",
                "interestedParty": "entity-b",
                "interests": [{"type": "shareholding", "share": {"exact": 50.0}}],
            },
        })
        rows = transform_relationships_to_supplementary([component])
        assert len(rows) == 0

"""Tests for BODS -> AML AI AccountPartyLink transformation."""

from bods_aml_ai.transform.account_party_links import (
    SYNTHETIC_ACCOUNT_PREFIX,
    transform_relationships_to_account_party_links,
)


class TestAccountPartyLinks:
    def test_creates_synthetic_accounts_per_subject(self, sample_relationships):
        rows = transform_relationships_to_account_party_links(sample_relationships)
        account_ids = {r["account_id"] for r in rows}
        # Two subjects: Acme and BVI
        assert len(account_ids) == 2
        assert f"{SYNTHETIC_ACCOUNT_PREFIX}gb-coh-12345678" in account_ids
        assert f"{SYNTHETIC_ACCOUNT_PREFIX}vg-fsc-98765" in account_ids

    def test_subject_is_primary_holder(self, sample_relationships):
        rows = transform_relationships_to_account_party_links(sample_relationships)
        acme_account = f"{SYNTHETIC_ACCOUNT_PREFIX}gb-coh-12345678"
        primary = [
            r for r in rows
            if r["account_id"] == acme_account and r["role"] == "PRIMARY_HOLDER"
        ]
        assert len(primary) == 1
        assert primary[0]["party_id"] == "gb-coh-12345678"

    def test_interested_parties_are_supplementary_holders(self, sample_relationships):
        rows = transform_relationships_to_account_party_links(sample_relationships)
        acme_account = f"{SYNTHETIC_ACCOUNT_PREFIX}gb-coh-12345678"
        supplementary = [
            r for r in rows
            if r["account_id"] == acme_account and r["role"] == "SUPPLEMENTARY_HOLDER"
        ]
        # Acme has 2 owners: Jane and BVI
        assert len(supplementary) == 2
        supp_parties = {r["party_id"] for r in supplementary}
        assert "person-jane-smith-1980" in supp_parties
        assert "vg-fsc-98765" in supp_parties

    def test_all_rows_have_source_system(self, sample_relationships):
        rows = transform_relationships_to_account_party_links(sample_relationships)
        for row in rows:
            assert row["source_system"] == "BODS"

    def test_skips_component_statements(self):
        """Component statements should be skipped."""
        from bods_aml_ai.ingestion.models import BODSRelationshipStatement

        component = BODSRelationshipStatement.from_dict({
            "statementId": "comp-1",
            "statementDate": "2024-06-15",
            "recordType": "relationship",
            "recordDetails": {
                "isComponent": True,
                "subject": "entity-a",
                "interestedParty": "entity-b",
                "interests": [],
            },
        })
        rows = transform_relationships_to_account_party_links([component])
        assert len(rows) == 0

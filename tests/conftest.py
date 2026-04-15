"""Shared test fixtures for bods-aml-ai."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from bods_aml_ai.ingestion.models import (
    BODSEntityStatement,
    BODSPersonStatement,
    BODSRelationshipStatement,
)

SAMPLE_INPUT = Path(__file__).parent.parent / "sample_input.json"


@pytest.fixture
def sample_statements() -> list[dict]:
    """Load sample BODS statements as raw dicts."""
    with open(SAMPLE_INPUT, encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def sample_person() -> BODSPersonStatement:
    """A sample BODS person statement (Jane Smith)."""
    return BODSPersonStatement.from_dict({
        "statementId": "c3d4e5f6-a7b8-9012-cdef-123456789012",
        "statementDate": "2024-06-15",
        "recordId": "person-jane-smith-1980",
        "recordType": "person",
        "recordDetails": {
            "isComponent": False,
            "personType": "knownPerson",
            "names": [
                {
                    "type": "individual",
                    "fullName": "Jane Smith",
                    "givenName": "Jane",
                    "familyName": "Smith",
                }
            ],
            "nationalities": [{"code": "GB", "name": "United Kingdom"}],
            "birthDate": "1980-05-14",
            "addresses": [
                {
                    "type": "registered",
                    "address": "42 Baker Street, London",
                    "postCode": "W1U 3BW",
                    "country": {"code": "GB", "name": "United Kingdom"},
                }
            ],
        },
    })


@pytest.fixture
def sample_entity() -> BODSEntityStatement:
    """A sample BODS entity statement (Acme Holdings Ltd)."""
    return BODSEntityStatement.from_dict({
        "statementId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        "statementDate": "2024-06-15",
        "recordId": "gb-coh-12345678",
        "recordType": "entity",
        "recordDetails": {
            "isComponent": False,
            "entityType": {"type": "registeredEntity"},
            "name": "Acme Holdings Ltd",
            "jurisdiction": {"code": "GB", "name": "United Kingdom"},
            "identifiers": [
                {"id": "12345678", "scheme": "GB-COH", "schemeName": "Companies House"}
            ],
            "addresses": [
                {
                    "type": "registered",
                    "address": "10 Downing Street, London",
                    "postCode": "SW1A 2AA",
                    "country": {"code": "GB", "name": "United Kingdom"},
                }
            ],
            "foundingDate": "2015-03-20",
        },
    })


@pytest.fixture
def sample_relationship() -> BODSRelationshipStatement:
    """A sample BODS relationship (Jane Smith -> 60% of Acme)."""
    return BODSRelationshipStatement.from_dict({
        "statementId": "e5f6a7b8-c9d0-1234-efab-345678901234",
        "statementDate": "2024-06-15",
        "recordId": "rel-jane-acme",
        "recordType": "relationship",
        "recordDetails": {
            "isComponent": False,
            "subject": "gb-coh-12345678",
            "interestedParty": "person-jane-smith-1980",
            "interests": [
                {
                    "type": "shareholding",
                    "directOrIndirect": "direct",
                    "beneficialOwnershipOrControl": True,
                    "share": {"exact": 60.0},
                    "startDate": "2015-03-20",
                }
            ],
        },
    })


@pytest.fixture
def sample_relationships() -> list[BODSRelationshipStatement]:
    """All three sample relationships."""
    raw = [
        {
            "statementId": "e5f6a7b8-c9d0-1234-efab-345678901234",
            "statementDate": "2024-06-15",
            "recordId": "rel-jane-acme",
            "recordType": "relationship",
            "recordDetails": {
                "isComponent": False,
                "subject": "gb-coh-12345678",
                "interestedParty": "person-jane-smith-1980",
                "interests": [
                    {
                        "type": "shareholding",
                        "directOrIndirect": "direct",
                        "beneficialOwnershipOrControl": True,
                        "share": {"exact": 60.0},
                    }
                ],
            },
        },
        {
            "statementId": "f6a7b8c9-d0e1-2345-fabc-456789012345",
            "statementDate": "2024-06-15",
            "recordId": "rel-bvi-acme",
            "recordType": "relationship",
            "recordDetails": {
                "isComponent": False,
                "subject": "gb-coh-12345678",
                "interestedParty": "vg-fsc-98765",
                "interests": [
                    {
                        "type": "shareholding",
                        "directOrIndirect": "direct",
                        "beneficialOwnershipOrControl": False,
                        "share": {"exact": 40.0},
                    }
                ],
            },
        },
        {
            "statementId": "a7b8c9d0-e1f2-3456-abcd-567890123456",
            "statementDate": "2024-06-15",
            "recordId": "rel-hans-bvi",
            "recordType": "relationship",
            "recordDetails": {
                "isComponent": False,
                "subject": "vg-fsc-98765",
                "interestedParty": "person-hans-mueller-1975",
                "interests": [
                    {
                        "type": "shareholding",
                        "directOrIndirect": "direct",
                        "beneficialOwnershipOrControl": True,
                        "share": {"exact": 100.0},
                    }
                ],
            },
        },
    ]
    return [BODSRelationshipStatement.from_dict(r) for r in raw]

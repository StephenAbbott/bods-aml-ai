"""Generate synthetic AccountPartyLink rows from BODS ownership relationships.

Google AML AI's only native relationship mechanism is Party <-> Account via
AccountPartyLink. This module creates synthetic "ownership accounts" to
represent beneficial ownership relationships through this mechanism.

Strategy:
    For each owned entity (subject of an ownership-or-control statement),
    create a synthetic account. Then link:
    - The subject entity as PRIMARY_HOLDER (it "holds" the ownership account)
    - Each interested party (owner/controller) as SUPPLEMENTARY_HOLDER

    This is semantically imperfect — AccountPartyLink was designed for bank
    accounts, not ownership structures — but it allows AML AI's graph-based
    risk scoring to see connections between parties that share ownership
    relationships.

    The synthetic account_id is derived from the subject's recordId to
    ensure deterministic, reproducible output.

This module is OPTIONAL. The Party + PartySupplementaryData tables are
sufficient for basic integration. Use this module when you want AML AI
to consider ownership connections in its party-level risk scoring.
"""

from __future__ import annotations

import logging
from collections import defaultdict

from bods_aml_ai.ingestion.models import BODSRelationshipStatement
from bods_aml_ai.utils.dates import to_timestamp

logger = logging.getLogger(__name__)

SYNTHETIC_ACCOUNT_PREFIX = "bods-ownership-"


def transform_relationships_to_account_party_links(
    relationships: list[BODSRelationshipStatement],
    validity_start_time: str | None = None,
) -> list[dict]:
    """Generate AccountPartyLink rows from BODS ownership relationships.

    For each subject entity, creates:
    - One synthetic account (implicit in the link rows)
    - One PRIMARY_HOLDER link for the subject entity
    - One SUPPLEMENTARY_HOLDER link per interested party

    Args:
        relationships: BODS ownership-or-control statements
        validity_start_time: Fallback timestamp

    Returns:
        List of AccountPartyLink row dicts
    """
    # Group relationships by subject
    subject_relationships: dict[str, list[BODSRelationshipStatement]] = defaultdict(list)

    for rel in relationships:
        if not rel.subject or not rel.interested_party:
            continue
        if rel.is_component:
            continue
        subject_relationships[rel.subject].append(rel)

    rows: list[dict] = []

    for subject_id, rels in subject_relationships.items():
        account_id = f"{SYNTHETIC_ACCOUNT_PREFIX}{subject_id}"

        # Use the earliest relationship date as validity
        validities = [
            to_timestamp(r.publication_date) for r in rels
            if r.publication_date
        ]
        validity = min(validities) if validities else validity_start_time

        # Subject entity is the PRIMARY_HOLDER
        rows.append({
            "account_id": account_id,
            "party_id": subject_id,
            "validity_start_time": validity,
            "role": "PRIMARY_HOLDER",
            "source_system": "BODS",
        })

        # Each interested party is a SUPPLEMENTARY_HOLDER
        seen_ips: set[str] = set()
        for rel in rels:
            ip_id = rel.interested_party
            if ip_id and ip_id not in seen_ips:
                seen_ips.add(ip_id)
                rows.append({
                    "account_id": account_id,
                    "party_id": ip_id,
                    "validity_start_time": validity,
                    "role": "SUPPLEMENTARY_HOLDER",
                    "source_system": "BODS",
                })

    return [_clean_nulls(r) for r in rows]


def _clean_nulls(row: dict) -> dict:
    """Remove None values from a row dict."""
    return {k: v for k, v in row.items() if v is not None}

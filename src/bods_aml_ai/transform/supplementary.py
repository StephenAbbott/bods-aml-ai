"""Transform BODS ownership-or-control statements into AML AI PartySupplementaryData.

This is the critical mapping layer. Google AML AI has NO native concept of
party-to-party relationships, ownership percentages, or control indicators.
The PartySupplementaryData table is the only extensibility mechanism — it
stores per-party numeric (float64) attributes.

Strategy:
    For each ownership-or-control statement, we generate supplementary data
    rows for BOTH the subject (entity being owned) and the interested party
    (the owner/controller). This encodes the relationship as party-level
    attributes rather than a link between two parties.

    Supplementary data IDs generated:
    - bo_ownership_pct_{subject_id}      -> on the interested party: ownership %
    - bo_is_beneficial_owner             -> on the interested party: 1.0 if BO
    - bo_is_direct                       -> on the interested party: 1.0 if direct
    - bo_num_owners                      -> on the subject: count of interested parties
    - bo_total_identified_ownership_pct  -> on the subject: sum of all ownership %s
    - bo_has_beneficial_owner            -> on the subject: 1.0 if any BO identified
    - bo_interest_type_{type}            -> on the interested party: 1.0 per interest type

Constraints:
    - AML AI allows max 100 supplementary data IDs per party
    - Values must be float64 (no strings, no booleans)
    - party_supplementary_data_id + party_id + validity_start_time is the PK

Note on data/label leakage:
    Google warns against encoding information in PartySupplementaryData that
    could leak investigation outcomes. Pure ownership structure data (from
    corporate registries, BODS) is pre-existing factual data, not derived
    from AML investigations, so this concern does not apply here.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import TYPE_CHECKING

from bods_aml_ai.ingestion.models import BODSRelationshipStatement
from bods_aml_ai.utils.dates import to_timestamp

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


def transform_relationships_to_supplementary(
    relationships: list[BODSRelationshipStatement],
    validity_start_time: str | None = None,
) -> list[dict]:
    """Transform all BODS relationship statements into PartySupplementaryData rows.

    Processes relationships in two passes:
    1. Per-relationship: generate interested-party-level attributes
    2. Aggregate: generate subject-level summary attributes

    Args:
        relationships: All BODS ownership-or-control statements
        validity_start_time: Fallback timestamp if statements lack dates

    Returns:
        List of PartySupplementaryData row dicts
    """
    rows: list[dict] = []

    # Track aggregates per subject
    subject_owner_count: dict[str, int] = defaultdict(int)
    subject_ownership_total: dict[str, float] = defaultdict(float)
    subject_has_bo: dict[str, bool] = defaultdict(bool)

    # Pass 1: per-relationship attributes on the interested party
    for rel in relationships:
        # Skip component statements (intermediate chain pieces)
        if rel.is_component:
            continue

        # Declared-unknown UBO (inline unspecifiedRecord) — preserve the
        # reason as an attribute on the subject. Silently dropping these
        # understates opacity risk per FATF guidance.
        if rel.subject and not rel.interested_party and rel.interested_party_reason:
            validity = to_timestamp(rel.publication_date) or validity_start_time
            rows.append(_make_row(
                party_id=rel.subject,
                data_id="bo_has_declared_unknown_owner",
                value=1.0,
                validity=validity,
            ))
            rows.append(_make_row(
                party_id=rel.subject,
                data_id=f"bo_unspecified_reason_{rel.interested_party_reason}",
                value=1.0,
                validity=validity,
            ))
            continue

        if not rel.subject or not rel.interested_party:
            logger.warning(
                "Skipping relationship %s: missing subject or interestedParty",
                rel.statement_id,
            )
            continue

        validity = to_timestamp(rel.publication_date) or validity_start_time
        subject_id = rel.subject
        ip_id = rel.interested_party

        # Track aggregates
        subject_owner_count[subject_id] += 1

        for interest in rel.interests:
            # Ownership percentage
            pct = interest.best_share_percentage()
            if pct is not None:
                rows.append(_make_row(
                    party_id=ip_id,
                    data_id=f"bo_ownership_pct_{subject_id}",
                    value=pct,
                    validity=validity,
                ))
                subject_ownership_total[subject_id] += pct

            # Beneficial ownership flag
            if interest.beneficial_ownership_or_control:
                rows.append(_make_row(
                    party_id=ip_id,
                    data_id="bo_is_beneficial_owner",
                    value=1.0,
                    validity=validity,
                ))
                subject_has_bo[subject_id] = True

            # Direct vs indirect
            if interest.direct_or_indirect:
                rows.append(_make_row(
                    party_id=ip_id,
                    data_id="bo_is_direct",
                    value=1.0 if interest.direct_or_indirect == "direct" else 0.0,
                    validity=validity,
                ))

            # Interest type flags
            if interest.type:
                rows.append(_make_row(
                    party_id=ip_id,
                    data_id=f"bo_interest_type_{interest.type}",
                    value=1.0,
                    validity=validity,
                ))

    # Pass 2: aggregate attributes on the subject entity
    for subject_id, count in subject_owner_count.items():
        # Use the validity from the first relationship mentioning this subject
        validity = None
        for rel in relationships:
            if rel.subject == subject_id:
                validity = to_timestamp(rel.publication_date) or validity_start_time
                break

        rows.append(_make_row(
            party_id=subject_id,
            data_id="bo_num_owners",
            value=float(count),
            validity=validity,
        ))

        if subject_id in subject_ownership_total:
            rows.append(_make_row(
                party_id=subject_id,
                data_id="bo_total_identified_ownership_pct",
                value=subject_ownership_total[subject_id],
                validity=validity,
            ))

        if subject_has_bo.get(subject_id, False):
            rows.append(_make_row(
                party_id=subject_id,
                data_id="bo_has_beneficial_owner",
                value=1.0,
                validity=validity,
            ))

    # Deduplicate: keep latest value per (party_id, data_id)
    rows = _deduplicate_rows(rows)

    return rows


def _make_row(
    party_id: str,
    data_id: str,
    value: float,
    validity: str | None,
) -> dict:
    """Build a single PartySupplementaryData row."""
    row: dict = {
        "party_supplementary_data_id": data_id,
        "party_id": party_id,
        "validity_start_time": validity,
        "supplementary_data_payload": {
            "value": value,
        },
        "source_system": "BODS",
    }
    return {k: v for k, v in row.items() if v is not None}


def _deduplicate_rows(rows: list[dict]) -> list[dict]:
    """Keep the last row per (party_id, data_id, validity_start_time)."""
    seen: dict[tuple, int] = {}
    for idx, row in enumerate(rows):
        key = (
            row.get("party_id"),
            row.get("party_supplementary_data_id"),
            row.get("validity_start_time"),
        )
        seen[key] = idx

    return [rows[idx] for idx in sorted(seen.values())]

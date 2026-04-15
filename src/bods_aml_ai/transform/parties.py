"""Transform BODS person and entity statements into AML AI Party rows.

Google AML AI Party table schema:
- party_id (STRING, REQUIRED): Unique party identifier
- validity_start_time (TIMESTAMP, REQUIRED): When this version became valid
- is_entity_deleted (BOOL, NULLABLE): Soft-delete flag
- type (STRING, REQUIRED): COMPANY or CONSUMER
- name (STRING, NULLABLE): Display name
- addresses (REPEATED STRUCT): address_line, region_code, post_code, etc.
- birth_date (DATE, NULLABLE): For CONSUMER only
- establishment_date (DATE, NULLABLE): For COMPANY only
- nationalities (REPEATED STRUCT): Each has region_code
- residencies (REPEATED STRUCT): Each has region_code
- source_system (STRING, NULLABLE): Data lineage

Mapping decisions:
- BODS personStatement -> Party type=CONSUMER
- BODS entityStatement -> Party type=COMPANY
- party_id derived from BODS recordId (or statementId as fallback)
- validity_start_time from BODS statementDate (or current time)
- BODS addresses mapped to AML AI address structs
- BODS nationalities mapped to AML AI nationalities.region_code
- BODS jurisdiction mapped to AML AI residencies.region_code (for entities)
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from bods_aml_ai.ingestion.models import (
    BODSAddress,
    BODSEntityStatement,
    BODSPersonStatement,
)
from bods_aml_ai.utils.countries import to_region_code
from bods_aml_ai.utils.dates import to_bq_date, to_timestamp

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


def party_id_from_person(person: BODSPersonStatement) -> str:
    """Derive an AML AI party_id from a BODS person statement."""
    return person.record_id or person.statement_id


def party_id_from_entity(entity: BODSEntityStatement) -> str:
    """Derive an AML AI party_id from a BODS entity statement."""
    return entity.record_id or entity.statement_id


def transform_person_to_party(person: BODSPersonStatement) -> dict:
    """Transform a BODS person statement into an AML AI Party row.

    Maps to Party type=CONSUMER.
    """
    pid = party_id_from_person(person)
    validity = to_timestamp(person.publication_date)

    row: dict = {
        "party_id": pid,
        "validity_start_time": validity,
        "type": "CONSUMER",
        "source_system": "BODS",
    }

    # Name
    name = person.display_name()
    if name:
        row["name"] = name

    # Birth date
    if person.birth_date:
        row["birth_date"] = to_bq_date(person.birth_date)

    # Addresses
    addresses = _transform_addresses(person.addresses)
    if addresses:
        row["addresses"] = addresses

    # Nationalities
    nationalities = _transform_nationalities(person.nationalities)
    if nationalities:
        row["nationalities"] = nationalities

    return _clean_nulls(row)


def transform_entity_to_party(entity: BODSEntityStatement) -> dict:
    """Transform a BODS entity statement into an AML AI Party row.

    Maps to Party type=COMPANY.
    """
    pid = party_id_from_entity(entity)
    validity = to_timestamp(entity.publication_date)

    row: dict = {
        "party_id": pid,
        "validity_start_time": validity,
        "type": "COMPANY",
        "source_system": "BODS",
    }

    # Name
    if entity.name:
        row["name"] = entity.name

    # Establishment date (founding date)
    if entity.founding_date:
        row["establishment_date"] = to_bq_date(entity.founding_date)

    # Addresses
    addresses = _transform_addresses(entity.addresses)
    if addresses:
        row["addresses"] = addresses

    # Jurisdiction -> residency (best available mapping)
    if entity.jurisdiction_code:
        region = to_region_code(entity.jurisdiction_code)
        if region:
            row["residencies"] = [{"region_code": region}]

    return _clean_nulls(row)


def _transform_addresses(bods_addresses: list[BODSAddress]) -> list[dict]:
    """Convert BODS addresses to AML AI address structs."""
    result = []
    for addr in bods_addresses:
        aml_addr: dict = {}
        if addr.address:
            aml_addr["address_line"] = addr.address
        if addr.post_code:
            aml_addr["post_code"] = addr.post_code
        if addr.country:
            region = to_region_code(addr.country)
            if region:
                aml_addr["region_code"] = region
        if aml_addr:
            result.append(aml_addr)
    return result


def _transform_nationalities(nationality_codes: list[str]) -> list[dict]:
    """Convert BODS nationality codes to AML AI nationality structs."""
    result = []
    for code in nationality_codes:
        region = to_region_code(code)
        if region:
            result.append({"region_code": region})
    return result


def _clean_nulls(row: dict) -> dict:
    """Remove None values from a row dict."""
    return {k: v for k, v in row.items() if v is not None}

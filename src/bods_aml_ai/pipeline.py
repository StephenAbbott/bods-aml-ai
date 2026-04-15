"""Pipeline orchestrator for BODS -> AML AI transformation.

Reads BODS v0.4 statements, classifies them, transforms each type into
the appropriate AML AI table format, and writes output as NDJSON files
ready for BigQuery ingestion.

Processing order:
1. Read all BODS statements and classify by type
2. Transform person statements -> Party (CONSUMER) rows
3. Transform entity statements -> Party (COMPANY) rows
4. Transform relationship statements -> PartySupplementaryData rows
5. Optionally: Transform relationships -> AccountPartyLink rows
"""

from __future__ import annotations

import logging
from pathlib import Path

from bods_aml_ai.config import TransformConfig
from bods_aml_ai.ingestion.bods_reader import read_bods_file
from bods_aml_ai.ingestion.models import (
    BODSEntityStatement,
    BODSPersonStatement,
    BODSRelationshipStatement,
)
from bods_aml_ai.output.writer import AMLWriter
from bods_aml_ai.transform.account_party_links import (
    transform_relationships_to_account_party_links,
)
from bods_aml_ai.transform.parties import (
    transform_entity_to_party,
    transform_person_to_party,
)
from bods_aml_ai.transform.supplementary import (
    transform_relationships_to_supplementary,
)

logger = logging.getLogger(__name__)


class AMLPipeline:
    """Orchestrates the BODS -> AML AI transformation.

    Usage:
        config = TransformConfig(output_dir="output")
        pipeline = AMLPipeline(config)
        pipeline.process_file("bods_data.json")
        counts = pipeline.finalize()
    """

    def __init__(self, config: TransformConfig):
        self.config = config
        self.writer = AMLWriter(config.output_dir)

        # Collect all statements for cross-referencing
        self._persons: list[BODSPersonStatement] = []
        self._entities: list[BODSEntityStatement] = []
        self._relationships: list[BODSRelationshipStatement] = []

    def process_file(self, filepath: Path | str) -> int:
        """Read and classify all BODS statements from a file.

        Returns the number of statements read.
        """
        filepath = Path(filepath)
        logger.info("Processing BODS file: %s", filepath)

        count = 0
        for stmt in read_bods_file(filepath):
            if isinstance(stmt, BODSPersonStatement):
                self._persons.append(stmt)
            elif isinstance(stmt, BODSEntityStatement):
                self._entities.append(stmt)
            elif isinstance(stmt, BODSRelationshipStatement):
                self._relationships.append(stmt)
            count += 1

        logger.info(
            "Read %d statements: %d persons, %d entities, %d relationships",
            count,
            len(self._persons),
            len(self._entities),
            len(self._relationships),
        )
        return count

    def transform_and_write(self) -> dict[str, int]:
        """Run all transformations and write output.

        Call this after all files have been processed via process_file().
        Returns row counts per table.
        """
        validity = self.config.validity_start_time

        # 1. Person statements -> Party (CONSUMER)
        person_parties = []
        for person in self._persons:
            row = transform_person_to_party(person)
            # Ensure validity_start_time is set
            if not row.get("validity_start_time"):
                row["validity_start_time"] = validity
            person_parties.append(row)

        # 2. Entity statements -> Party (COMPANY)
        entity_parties = []
        for entity in self._entities:
            row = transform_entity_to_party(entity)
            if not row.get("validity_start_time"):
                row["validity_start_time"] = validity
            entity_parties.append(row)

        # Write all Party rows
        all_parties = person_parties + entity_parties
        self.writer.write_rows("party", all_parties)
        logger.info(
            "Generated %d Party rows (%d CONSUMER, %d COMPANY)",
            len(all_parties),
            len(person_parties),
            len(entity_parties),
        )

        # 3. Relationship statements -> PartySupplementaryData
        supplementary_rows = transform_relationships_to_supplementary(
            self._relationships,
            validity_start_time=validity,
        )
        self.writer.write_rows("party_supplementary_data", supplementary_rows)
        logger.info(
            "Generated %d PartySupplementaryData rows from %d relationships",
            len(supplementary_rows),
            len(self._relationships),
        )

        # 4. Optionally: Relationship statements -> AccountPartyLink
        if self.config.include_account_party_links:
            apl_rows = transform_relationships_to_account_party_links(
                self._relationships,
                validity_start_time=validity,
            )
            self.writer.write_rows("account_party_link", apl_rows)
            logger.info(
                "Generated %d AccountPartyLink rows",
                len(apl_rows),
            )

        return self.writer.counts

    def finalize(self) -> dict[str, int]:
        """Run transformation, write output, and close files."""
        counts = self.transform_and_write()
        final_counts = self.writer.finalize()
        return final_counts

    @property
    def statement_counts(self) -> dict[str, int]:
        """Number of BODS statements loaded, by type."""
        return {
            "person": len(self._persons),
            "entity": len(self._entities),
            "relationship": len(self._relationships),
            "total": len(self._persons) + len(self._entities) + len(self._relationships),
        }

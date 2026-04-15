"""Pipeline configuration."""

from __future__ import annotations

from dataclasses import dataclass

from bods_aml_ai.utils.dates import current_date_iso, current_datetime_iso


@dataclass
class TransformConfig:
    """Configuration for the BODS -> AML AI transformation pipeline."""

    output_dir: str = "output"
    include_account_party_links: bool = True
    validity_start_time: str = ""

    def __post_init__(self):
        if not self.validity_start_time:
            self.validity_start_time = current_datetime_iso()

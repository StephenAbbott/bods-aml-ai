"""Date and timestamp utilities for AML AI output."""

from __future__ import annotations

import re
from datetime import date, datetime

from dateutil import parser as dateutil_parser


def current_date_iso() -> str:
    """Return today's date as ISO 8601 string."""
    return date.today().isoformat()


def current_datetime_iso() -> str:
    """Return current UTC datetime as ISO 8601 string."""
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def to_timestamp(date_str: str | None) -> str | None:
    """Convert a date or datetime string to an RFC 3339 timestamp.

    AML AI requires TIMESTAMP fields in RFC 3339 format.
    A bare date like '2024-01-15' becomes '2024-01-15T00:00:00Z'.
    """
    if not date_str or not date_str.strip():
        return None
    date_str = date_str.strip()

    # Already a full timestamp
    if re.match(r"^\d{4}-\d{2}-\d{2}T", date_str):
        if date_str.endswith("Z") or "+" in date_str:
            return date_str
        return date_str + "Z"

    # ISO date: YYYY-MM-DD
    if re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
        return date_str + "T00:00:00Z"

    # Partial date: YYYY-MM -> use first of month
    if re.match(r"^\d{4}-\d{2}$", date_str):
        return date_str + "-01T00:00:00Z"

    # Year only: YYYY -> use Jan 1
    if re.match(r"^\d{4}$", date_str):
        return date_str + "-01-01T00:00:00Z"

    # Try dateutil as fallback
    try:
        parsed = dateutil_parser.parse(date_str, dayfirst=True)
        return parsed.strftime("%Y-%m-%dT%H:%M:%SZ")
    except (ValueError, TypeError):
        return None


def to_bq_date(date_str: str | None) -> str | None:
    """Convert a date string to BigQuery DATE format (YYYY-MM-DD).

    AML AI DATE fields expect YYYY-MM-DD.
    """
    if not date_str or not date_str.strip():
        return None
    date_str = date_str.strip()

    if re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
        return date_str

    # Extract date from timestamp
    match = re.match(r"^(\d{4}-\d{2}-\d{2})T", date_str)
    if match:
        return match.group(1)

    # Partial date: YYYY-MM -> first of month
    if re.match(r"^\d{4}-\d{2}$", date_str):
        return date_str + "-01"

    # Year only
    if re.match(r"^\d{4}$", date_str):
        return date_str + "-01-01"

    try:
        parsed = dateutil_parser.parse(date_str, dayfirst=True)
        return parsed.strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return None

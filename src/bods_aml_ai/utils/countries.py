"""Country code resolution for AML AI region_code fields.

AML AI uses 2-letter CLDR region codes (effectively ISO 3166-1 alpha-2).
BODS uses the same codes, so mapping is mostly pass-through, but we
handle edge cases and validation.
"""

from __future__ import annotations

import logging

import pycountry

logger = logging.getLogger(__name__)

# Common aliases not in pycountry
COUNTRY_ALIASES: dict[str, str] = {
    "uk": "GB",
    "england": "GB",
    "scotland": "GB",
    "wales": "GB",
    "northern ireland": "GB",
    "great britain": "GB",
    "usa": "US",
    "uae": "AE",
    "korea": "KR",
    "south korea": "KR",
    "russia": "RU",
    "czech republic": "CZ",
    "hong kong": "HK",
    "taiwan": "TW",
    "macau": "MO",
    "ivory coast": "CI",
    "vietnam": "VN",
}


def to_region_code(country_input: str | None) -> str | None:
    """Convert a country code or name to a 2-letter CLDR region code.

    AML AI Party.nationalities and Party.residencies require region_code
    as a 2-letter CLDR code.
    """
    if not country_input or not country_input.strip():
        return None

    text = country_input.strip()

    # Already a valid 2-letter code
    if len(text) == 2:
        code = text.upper()
        country = pycountry.countries.get(alpha_2=code)
        if country:
            return code

    # 3-letter code
    if len(text) == 3:
        country = pycountry.countries.get(alpha_3=text.upper())
        if country:
            return country.alpha_2

    # Check aliases
    lower = text.lower()
    if lower in COUNTRY_ALIASES:
        return COUNTRY_ALIASES[lower]

    # Try exact name match
    country = pycountry.countries.get(name=text)
    if country:
        return country.alpha_2

    # Fuzzy search
    try:
        results = pycountry.countries.search_fuzzy(text)
        if results:
            return results[0].alpha_2
    except LookupError:
        pass

    logger.warning("Could not resolve country to region code: %s", country_input)
    return None

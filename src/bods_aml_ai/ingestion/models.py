"""Lightweight models for BODS v0.4 statements.

These models represent the subset of BODS fields needed for
transformation to Google AML AI format. They are not a full
BODS schema implementation — use lib-cove-bods for validation.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class BODSName:
    """A name from a BODS person or entity statement."""

    full_name: str | None = None
    given_name: str | None = None
    family_name: str | None = None
    type: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> BODSName:
        return cls(
            full_name=data.get("fullName"),
            given_name=data.get("givenName"),
            family_name=data.get("familyName"),
            type=data.get("type"),
        )

    def display_name(self) -> str:
        """Best available name string."""
        if self.full_name:
            return self.full_name
        parts = [p for p in [self.given_name, self.family_name] if p]
        return " ".join(parts) if parts else ""


@dataclass
class BODSAddress:
    """An address from a BODS statement."""

    type: str | None = None
    address: str | None = None
    post_code: str | None = None
    country: str | None = None  # ISO 3166-1 alpha-2
    country_name: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> BODSAddress:
        country_obj = data.get("country", {})
        return cls(
            type=data.get("type"),
            address=data.get("address"),
            post_code=data.get("postCode"),
            country=country_obj.get("code") if isinstance(country_obj, dict) else None,
            country_name=country_obj.get("name") if isinstance(country_obj, dict) else None,
        )


@dataclass
class BODSIdentifier:
    """An identifier from a BODS statement."""

    id: str | None = None
    scheme: str | None = None
    scheme_name: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> BODSIdentifier:
        return cls(
            id=data.get("id"),
            scheme=data.get("scheme"),
            scheme_name=data.get("schemeName"),
        )


@dataclass
class BODSInterest:
    """An interest from a BODS ownership-or-control statement."""

    type: str | None = None
    direct_or_indirect: str | None = None
    beneficial_ownership_or_control: bool | None = None
    share_exact: float | None = None
    share_minimum: float | None = None
    share_maximum: float | None = None
    start_date: str | None = None
    end_date: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> BODSInterest:
        share = data.get("share", {})
        return cls(
            type=data.get("type"),
            direct_or_indirect=data.get("directOrIndirect"),
            beneficial_ownership_or_control=data.get("beneficialOwnershipOrControl"),
            share_exact=share.get("exact"),
            share_minimum=share.get("minimum"),
            share_maximum=share.get("maximum"),
            start_date=data.get("startDate"),
            end_date=data.get("endDate"),
        )

    def best_share_percentage(self) -> float | None:
        """Return the best available share percentage."""
        if self.share_exact is not None:
            return self.share_exact
        if self.share_minimum is not None and self.share_maximum is not None:
            return (self.share_minimum + self.share_maximum) / 2
        return self.share_minimum or self.share_maximum


@dataclass
class BODSPersonStatement:
    """A BODS v0.4 person statement."""

    statement_id: str
    record_id: str | None = None
    names: list[BODSName] = field(default_factory=list)
    nationalities: list[str] = field(default_factory=list)  # ISO alpha-2 codes
    birth_date: str | None = None
    addresses: list[BODSAddress] = field(default_factory=list)
    identifiers: list[BODSIdentifier] = field(default_factory=list)
    person_type: str | None = None
    publication_date: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> BODSPersonStatement:
        details = data.get("recordDetails", {})
        return cls(
            statement_id=data.get("statementId", ""),
            record_id=data.get("recordId"),
            names=[BODSName.from_dict(n) for n in details.get("names", [])],
            nationalities=[
                n.get("code", "") for n in details.get("nationalities", [])
                if isinstance(n, dict) and n.get("code")
            ],
            birth_date=details.get("birthDate"),
            addresses=[BODSAddress.from_dict(a) for a in details.get("addresses", [])],
            identifiers=[BODSIdentifier.from_dict(i) for i in details.get("identifiers", [])],
            person_type=details.get("personType"),
            publication_date=data.get("statementDate"),
        )

    def display_name(self) -> str:
        for name in self.names:
            display = name.display_name()
            if display:
                return display
        return ""


@dataclass
class BODSEntityStatement:
    """A BODS v0.4 entity statement."""

    statement_id: str
    record_id: str | None = None
    name: str | None = None
    entity_type: str | None = None
    jurisdiction_code: str | None = None
    jurisdiction_name: str | None = None
    founding_date: str | None = None
    dissolution_date: str | None = None
    addresses: list[BODSAddress] = field(default_factory=list)
    identifiers: list[BODSIdentifier] = field(default_factory=list)
    publication_date: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> BODSEntityStatement:
        details = data.get("recordDetails", {})
        entity_type_obj = details.get("entityType", {})
        jurisdiction = details.get("jurisdiction", {})
        return cls(
            statement_id=data.get("statementId", ""),
            record_id=data.get("recordId"),
            name=details.get("name"),
            entity_type=entity_type_obj.get("type") if isinstance(entity_type_obj, dict) else entity_type_obj,
            jurisdiction_code=jurisdiction.get("code") if isinstance(jurisdiction, dict) else None,
            jurisdiction_name=jurisdiction.get("name") if isinstance(jurisdiction, dict) else None,
            founding_date=details.get("foundingDate"),
            dissolution_date=details.get("dissolutionDate"),
            addresses=[BODSAddress.from_dict(a) for a in details.get("addresses", [])],
            identifiers=[BODSIdentifier.from_dict(i) for i in details.get("identifiers", [])],
            publication_date=data.get("statementDate"),
        )


@dataclass
class BODSRelationshipStatement:
    """A BODS v0.4 ownership-or-control statement."""

    statement_id: str
    record_id: str | None = None
    subject: str | None = None  # recordId of the entity being owned/controlled
    interested_party: str | None = None  # recordId of the owner/controller
    # Populated when interestedParty is an inline unspecifiedRecord
    # (declared-unknown UBO) rather than a recordId string. Per FATF, a
    # declared-unknown UBO is itself a risk signal, not an absence of data.
    interested_party_reason: str | None = None
    interested_party_description: str | None = None
    interests: list[BODSInterest] = field(default_factory=list)
    is_component: bool = False
    component_statement_ids: list[str] = field(default_factory=list)
    publication_date: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> BODSRelationshipStatement:
        details = data.get("recordDetails", {})
        raw_ip = details.get("interestedParty")
        ip_id: str | None = None
        ip_reason: str | None = None
        ip_description: str | None = None
        if isinstance(raw_ip, str):
            ip_id = raw_ip
        elif isinstance(raw_ip, dict):
            ip_reason = raw_ip.get("reason")
            ip_description = raw_ip.get("description")
        return cls(
            statement_id=data.get("statementId", ""),
            record_id=data.get("recordId"),
            subject=details.get("subject"),
            interested_party=ip_id,
            interested_party_reason=ip_reason,
            interested_party_description=ip_description,
            interests=[BODSInterest.from_dict(i) for i in details.get("interests", [])],
            is_component=details.get("isComponent", False),
            component_statement_ids=details.get("componentStatementIDs", []),
            publication_date=data.get("statementDate"),
        )

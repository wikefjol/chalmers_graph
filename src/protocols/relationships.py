"""
Relationship protocol definitions based on Elasticsearch document structure
"""

from typing import Protocol, Optional
from datetime import datetime, date


class AuthoredRelationProtocol(Protocol):
    """Protocol for AUTHORED relationships (Person -> Publication)"""
    person_id: str
    publication_id: str
    order: int
    role_id: str
    role_name_swe: str
    role_name_eng: str
    created_at: Optional[datetime]


class InvolvedInRelationProtocol(Protocol):
    """Protocol for INVOLVED_IN relationships (Person -> Project)"""
    person_id: str
    project_id: str
    organization_id: str
    role_id: int
    role_name_swe: str
    role_name_eng: str
    start_date: Optional[date]
    end_date: Optional[date]
    created_at: Optional[datetime]


class AffiliatedRelationProtocol(Protocol):
    """Protocol for AFFILIATED relationships (Person -> Organization)"""
    person_id: str
    organization_id: str
    role: Optional[str]
    start_date: Optional[date]
    end_date: Optional[date]
    created_at: Optional[datetime]


class PartnerRelationProtocol(Protocol):
    """Protocol for PARTNER relationships (Organization -> Project)"""
    organization_id: str
    project_id: str
    role_id: int
    role_name_swe: str
    role_name_eng: str
    contract_start_date: Optional[date]
    contract_end_date: Optional[date]
    contract_amount: Optional[float]
    contract_currency: Optional[str]
    funder_id: Optional[str]
    created_at: Optional[datetime]


class OutputRelationProtocol(Protocol):
    """Protocol for OUTPUT relationships (Project -> Publication)"""
    project_id: str
    publication_id: str
    created_at: Optional[datetime]


class PublishedInRelationProtocol(Protocol):
    """Protocol for PUBLISHED_IN relationships (Publication -> Serial)"""
    publication_id: str
    serial_id: str
    created_at: Optional[datetime]


class ParentOfRelationProtocol(Protocol):
    """Protocol for PARENT_OF relationships (Organization -> Organization)"""
    parent_organization_id: str
    child_organization_id: str
    level: int
    created_at: Optional[datetime]
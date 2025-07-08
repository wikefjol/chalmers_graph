"""
Protocol definitions for type-safe data structures
"""

from .nodes import (
    PersonProtocol,
    OrganizationProtocol,
    PublicationProtocol,
    ProjectProtocol,
    SerialProtocol,
)

from .relationships import (
    AuthoredRelationProtocol,
    InvolvedInRelationProtocol,
    AffiliatedRelationProtocol,
    PartnerRelationProtocol,
    OutputRelationProtocol,
    PublishedInRelationProtocol,
    ParentOfRelationProtocol,
)

__all__ = [
    # Node protocols
    "PersonProtocol",
    "OrganizationProtocol", 
    "PublicationProtocol",
    "ProjectProtocol",
    "SerialProtocol",
    # Relationship protocols
    "AuthoredRelationProtocol",
    "InvolvedInRelationProtocol",
    "AffiliatedRelationProtocol",
    "PartnerRelationProtocol",
    "OutputRelationProtocol",
    "PublishedInRelationProtocol",
    "ParentOfRelationProtocol",
]
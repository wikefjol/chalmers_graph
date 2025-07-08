"""
Node protocol definitions based on Elasticsearch document structure
"""

from typing import Protocol, Optional, List, Dict, Any
from datetime import datetime


class PersonProtocol(Protocol):
    """Protocol for Person nodes from research-persons-static index"""
    es_id: str
    first_name: str
    last_name: str
    display_name: str
    birth_year: Optional[int]
    is_active: bool
    is_deleted: bool
    has_organization_home: bool
    has_identifiers: bool
    has_publications: bool
    has_projects: bool
    organization_home_count: int
    identifiers_count: int
    identifiers: List[Dict[str, Any]]
    identifier_cid: List[str]
    identifier_cpl_person_id: List[str]
    identifier_orcid: List[str]
    organization_home: List[Dict[str, Any]]
    pdb_categories: List[Dict[str, Any]]
    created_by: str
    created_at: datetime
    sighted_by: str
    sighted_at: datetime
    needs_attention: bool


class OrganizationProtocol(Protocol):
    """Protocol for Organization nodes from research-organizations-static index"""
    es_id: str
    name_swe: str
    name_eng: str
    display_name_swe: str
    display_name_eng: str
    display_path_swe: str
    display_path_eng: str
    display_path_short_swe: str
    display_path_short_eng: str
    possible_display_paths: List[Dict[str, str]]
    city: Optional[str]
    postal_no: Optional[str]
    country: Optional[str]
    geo_lat: Optional[float]
    geo_long: Optional[float]
    level: int
    start_year: int
    end_year: int
    organization_types: List[Dict[str, Any]]
    organization_parents: List[Dict[str, Any]]
    active_organization_parent_ids: List[str]
    has_identifiers: bool
    identifiers_count: int
    identifiers: List[Dict[str, Any]]
    identifier_ldap_code: List[str]
    identifier_cpl_department_id: List[str]
    is_active: bool
    created_by: str
    created_at: datetime
    updated_by: Optional[str]
    updated_at: Optional[datetime]
    validated_by: Optional[str]
    validated_date: Optional[datetime]
    needs_attention: bool


class PublicationProtocol(Protocol):
    """Protocol for Publication nodes from research-publications-static index"""
    es_id: str
    id: int
    title: str
    abstract: Optional[str]
    year: Optional[int]
    publication_type: Optional[str]
    source: Optional[str]
    keywords: List[str]
    categories: List[str]
    identifiers: List[Dict[str, Any]]
    is_draft: bool
    is_deleted: bool
    has_persons: bool
    has_organizations: bool
    persons: List[Dict[str, Any]]  # Contains nested person and organization data
    organizations: List[Dict[str, Any]]
    project: Optional[Dict[str, Any]]
    series: Optional[Dict[str, Any]]
    created_by: str
    created_at: datetime
    updated_by: Optional[str]
    updated_at: Optional[datetime]


class ProjectProtocol(Protocol):
    """Protocol for Project nodes from research-projects-static index"""
    es_id: str
    id: int
    project_title_swe: str
    project_title_eng: str
    project_description_swe: str
    project_description_eng: str
    project_description_swe_html: str
    project_description_eng_html: str
    publish_status: int
    start_date: datetime
    end_date: datetime
    created_date: datetime
    created_by: str
    updated_date: datetime
    updated_by: str
    keywords: List[str]
    categories: List[str]
    contracts: List[Dict[str, Any]]
    identifiers: List[Dict[str, Any]]
    persons: List[Dict[str, Any]]
    organizations: List[Dict[str, Any]]


class SerialProtocol(Protocol):
    """Protocol for Serial nodes from research-serials-static index"""
    es_id: str
    id: str
    title: str
    type: Dict[str, Any]
    start_year: Optional[int]
    end_year: Optional[int]
    publisher: Optional[str]
    country: Optional[str]
    identifiers: List[Dict[str, Any]]
    issn: Optional[str]
    is_open_access: bool
    is_peer_reviewed: bool
    is_deleted: bool
    created_date: datetime
    updated_by: Optional[str]
    updated_date: Optional[datetime]


class OrgUnitProtocol(Protocol):
    """Protocol for OrgUnit nodes from chalmers_organizational_structure.json"""
    es_id: str
    name: str
    display_path: str
    path_parts: List[str]
    path_depth: int
    level: str
    city: Optional[str]
    country: Optional[str]
    org_types: List[str]
    organizations: List[Dict[str, Any]]  # References to organization entries
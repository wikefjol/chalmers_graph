"""
Pytest configuration and shared fixtures
"""

import pytest
import os
import sys
from unittest.mock import Mock, MagicMock
from typing import Dict, Any, List

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from es_client.client import ElasticsearchClient


@pytest.fixture
def mock_es_client():
    """Mock Elasticsearch client for unit tests"""
    client = Mock(spec=ElasticsearchClient)
    client.ping.return_value = True
    client.get_info.return_value = {
        'version': {'number': '6.8.23'},
        'cluster_name': 'test_cluster'
    }
    return client


@pytest.fixture
def sample_person_doc() -> Dict[str, Any]:
    """Sample person document from Elasticsearch"""
    return {
        "Id": "4fadb700-7ea0-4a18-a131-29f745ac18a0",
        "FirstName": "Jiacheng",
        "LastName": "Yin",
        "DisplayName": "Jiacheng Yin",
        "BirthYear": 0,
        "IsActive": False,
        "IsDeleted": False,
        "HasOrganizationHome": False,
        "HasIdentifiers": True,
        "HasPublications": True,
        "HasProjects": False,
        "OrganizationHomeCount": 0,
        "IdentifiersCount": 1,
        "IdentifierCid": [],
        "IdentifierCplPersonId": [],
        "IdentifierOrcid": [],
        "Identifiers": [
            {
                "Id": "a9da2ba9-a0dd-4a6a-a584-c34364c6ee68",
                "Type": {
                    "Id": "ee0f4c8c-0845-499d-b54d-7189440ddca7",
                    "Value": "SCOPUS_AUTHID",
                    "DescriptionSwe": "Author-id i Scopus",
                    "DescriptionEng": "Author Id in Scopus"
                },
                "CreatedAt": "2023-08-09T10:39:30.08",
                "IsActive": True,
                "Value": "58504012200"
            }
        ],
        "OrganizationHome": [],
        "PdbCategories": [],
        "CreatedBy": "import/scopus",
        "CreatedAt": "2023-08-09T10:39:30.08",
        "SightedBy": "import/scopus",
        "SightedAt": "2023-08-09T10:39:30.08",
        "NeedsAttention": True
    }


@pytest.fixture
def sample_organization_doc() -> Dict[str, Any]:
    """Sample organization document from Elasticsearch"""
    return {
        "Id": "12926f37-bbd4-436e-9c5f-6dc6b5809e4d",
        "NameSwe": "Autoneum",
        "NameEng": "Autoneum",
        "DisplayNameSwe": "Autoneum",
        "DisplayNameEng": "Autoneum",
        "DisplayPathSwe": "Autoneum",
        "DisplayPathEng": "Autoneum",
        "DisplayPathShortSwe": "Autoneum",
        "DisplayPathShortEng": "Autoneum",
        "PossibleDisplayPaths": [
            {
                "DisplayPathSwe": "Autoneum",
                "DisplayPathEng": "Autoneum"
            }
        ],
        "City": "Winterthur",
        "Country": "Switzerland",
        "GeoLat": "47.4960383",
        "GeoLong": "8.7022018",
        "Level": 0,
        "StartYear": 0,
        "EndYear": 0,
        "OrganizationTypes": [
            {
                "Id": "7783463b-e852-458d-be30-7aefe0ab7f5c",
                "NameSwe": "Privat",
                "NameEng": "Private",
                "LegacyOrganizationTypeId": 2
            }
        ],
        "OrganizationParents": [],
        "ActiveOrganizationParentIds": [],
        "HasIdentifiers": True,
        "IdentifiersCount": 2,
        "Identifiers": [
            {
                "Id": "a748680e-2c4d-4501-b774-51f250ead3d3",
                "Type": {
                    "Id": "fda04edc-6fce-11ec-90d6-0242ac120003",
                    "Value": "ROR_ID",
                    "DescriptionSwe": "Organisationskoder (ROR ID) från ror.org.",
                    "DescriptionEng": "Organization (ROR ID) codes from ror.org."
                },
                "CreatedBy": "system",
                "CreatedAt": "2023-05-26T11:30:00",
                "IsActive": True,
                "Value": "https://ror.org/04zg05e03"
            }
        ],
        "IdentifierLdapCode": [],
        "IdentifierCplDepartmentId": [],
        "CreatedBy": "import/scopus",
        "CreatedAt": "2019-03-18T05:30:10.853",
        "UpdatedBy": "graner",
        "UpdatedAt": "2021-11-12T14:42:11.663",
        "ValidatedBy": "graner",
        "ValidatedDate": "2021-11-12T14:42:10.897",
        "NeedsAttention": False,
        "IsActive": True
    }


@pytest.fixture
def sample_publication_doc() -> Dict[str, Any]:
    """Sample publication document from Elasticsearch"""
    return {
        "Id": "2eac7013-0ce3-43d1-9a46-191b0c5cc592",
        "Title": "Test Publication Title",
        "Abstract": "This is a test abstract for the publication.",
        "Year": 2023,
        "PublicationType": "Journal Article",
        "Source": "Test Journal",
        "Keywords": ["keyword1", "keyword2"],
        "Categories": ["category1"],
        "IsDraft": False,
        "IsDeleted": False,
        "HasPersons": True,
        "HasOrganizations": False,
        "Persons": [
            {
                "PersonData": {
                    "Id": "1b807d61-badc-457d-a7e2-f71b45042e73",
                    "FirstName": "Yan",
                    "LastName": "Li",
                    "DisplayName": "Yan Li"
                },
                "PersonId": "1b807d61-badc-457d-a7e2-f71b45042e73",
                "Order": 0,
                "Role": {
                    "Id": "b8e4e026-5bf5-428e-8cea-f262e495cf41",
                    "NameSwe": "Författare",
                    "NameEng": "Author"
                }
            }
        ],
        "CreatedBy": "test",
        "CreatedAt": "2023-01-01T00:00:00"
    }


@pytest.fixture
def sample_project_doc() -> Dict[str, Any]:
    """Sample project document from Elasticsearch"""
    return {
        "ID": 5949,
        "ProjectTitleSwe": "Energieffektiva millimetervågssändare",
        "ProjectTitleEng": "Energy efficient millimeter wave transmitters",
        "ProjectDescriptionSwe": "Projektet syftar till att testa...",
        "ProjectDescriptionEng": "Objective and Goal In this project...",
        "PublishStatus": 3,
        "StartDate": "2013-11-25T00:00:00",
        "EndDate": "2017-12-31T00:00:00",
        "CreatedDate": "2015-04-29T11:34:45.67",
        "CreatedBy": "6175",
        "UpdatedDate": "2020-02-07T09:34:41.967",
        "UpdatedBy": "organization/merge",
        "Keywords": ["keyword1", "keyword2"],
        "Categories": ["category1"],
        "Contracts": [],
        "Identifiers": [],
        "Persons": [
            {
                "ID": 28946,
                "ProjectID": 5949,
                "PersonID": "a41ee985-4c7e-4e84-9512-65d79019749c",
                "PersonRoleID": 2,
                "PersonRoleName_en": "Project participant",
                "PersonRoleName_sv": "Projektdeltagare"
            }
        ],
        "Organizations": []
    }


@pytest.fixture
def sample_serial_doc() -> Dict[str, Any]:
    """Sample serial document from Elasticsearch"""
    return {
        "Id": "18d8743d-739a-41ed-9ad8-1a22f2304e62",
        "Title": "Nonwovens Report International",
        "Type": {
            "Id": "4aad0924-7da3-4d99-9f05-ea86f121f932",
            "Value": "JOURNAL",
            "DescriptionSwe": "Tidskrift",
            "DescriptionEng": "Journal"
        },
        "StartYear": 1989,
        "EndYear": 2005,
        "Publisher": "World Textile Publications Ltd",
        "Country": "United Kingdom",
        "Identifiers": [
            {
                "Id": "66fb6c6b-15b2-4b80-b3d0-4c4294d96fd2",
                "Type": {
                    "Value": "SCOPUS_SOURCE_ID",
                    "DescriptionSwe": "Identifierare av källa (tidskrift etc) i Scopus",
                    "DescriptionEng": "Elsevier Scopus Source Identifier"
                },
                "IsActive": True,
                "Value": "16492"
            }
        ],
        "IsOpenAccess": False,
        "IsPeerReviewed": False,
        "IsDeleted": False,
        "CreatedDate": "2017-10-06T09:54:40.2",
        "UpdatedBy": "autobot",
        "UpdatedDate": "2023-10-18T03:03:26.89"
    }


@pytest.fixture 
def mock_es_search_response():
    """Mock Elasticsearch search response"""
    return {
        'hits': {
            'total': 1000,
            'hits': [
                {
                    '_source': {'Id': '1', 'Title': 'Test Doc 1'},
                    '_id': '1'
                },
                {
                    '_source': {'Id': '2', 'Title': 'Test Doc 2'},
                    '_id': '2'
                }
            ]
        },
        '_scroll_id': 'test_scroll_id'
    }
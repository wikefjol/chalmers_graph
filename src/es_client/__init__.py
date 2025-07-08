"""
Elasticsearch integration module
"""

from .client import ElasticsearchClient
from .base_extractor import BaseExtractor, BaseStreamingExtractor
from .extractors import (
    PersonExtractor,
    OrganizationExtractor,
    PublicationExtractor,
    ProjectExtractor,
    SerialExtractor,
)

__all__ = [
    "ElasticsearchClient",
    "PersonExtractor",
    "OrganizationExtractor", 
    "PublicationExtractor",
    "ProjectExtractor",
    "SerialExtractor",
]
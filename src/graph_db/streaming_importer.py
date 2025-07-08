"""
Streaming import pipeline for Neo4j Graph RAG
Handles large datasets without loading all data into memory
"""

import time
import json
from typing import Dict, List, Any, Optional, Generator, Set
from datetime import datetime
from dataclasses import dataclass
from enum import Enum
from elasticsearch.exceptions import ConnectionTimeout, ConnectionError, TransportError

from .connection import Neo4jConnection
from .schema import SchemaManager
from .db_manager import DatabaseManager
from src.es_client.base_extractor import BaseStreamingExtractor


class ImportPhase(Enum):
    """Import phases in dependency order"""
    SETUP = "setup"
    ORGANIZATIONS = "organization"
    PERSONS = "person"
    SERIALS = "serial"
    PROJECTS = "project"
    PUBLICATIONS = "publication"
    RELATIONSHIPS = "relationship"
    VALIDATION = "validation"


@dataclass
class StreamingProgress:
    """Track streaming import progress"""
    phase: ImportPhase
    entity_type: str
    total_items: Optional[int]  # May be unknown for streaming
    processed_items: int
    start_time: float
    current_batch: int = 0
    errors: List[str] = None
    items_per_second: float = 0.0
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []
    
    @property
    def elapsed_time(self) -> float:
        return time.time() - self.start_time
    
    @property
    def current_rate(self) -> float:
        if self.elapsed_time > 0:
            return self.processed_items / self.elapsed_time
        return 0.0
    
    def get_progress_string(self) -> str:
        """Get formatted progress string"""
        rate = self.current_rate
        elapsed = self.elapsed_time
        
        # Format elapsed time
        elapsed_min = int(elapsed // 60)
        elapsed_sec = int(elapsed % 60)
        
        # Build progress string
        parts = [
            f"Batch {self.current_batch}",
            f"{self.processed_items:,} items",
            f"{rate:.0f} items/sec",
            f"Time: {elapsed_min}:{elapsed_sec:02d}"
        ]
        
        # Add percentage if total is known
        if self.total_items:
            percentage = (self.processed_items / self.total_items) * 100
            parts.insert(1, f"{percentage:.1f}%")
            
            # Add ETA
            if rate > 0:
                remaining = self.total_items - self.processed_items
                eta_seconds = remaining / rate
                eta_min = int(eta_seconds // 60)
                eta_sec = int(eta_seconds % 60)
                parts.append(f"ETA: {eta_min}:{eta_sec:02d}")
        
        return " | ".join(parts)


class StreamingImportPipeline:
    """Streaming import pipeline that processes data in batches"""
    
    def __init__(self, connection: Neo4jConnection, batch_size: int = 1000):
        self.connection = connection
        self.batch_size = batch_size
        self.schema_manager = SchemaManager(connection)
        self.db_manager = DatabaseManager(connection)
        self.import_session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.node_id_cache = {}  # Cache for relationship validation
    
    def run_streaming_import(self, extractors: Dict[str, BaseStreamingExtractor], 
                           enable_relationships: bool = True,
                           sample_mode: bool = False,
                           sample_size: int = 1000) -> bool:
        """
        Run streaming import pipeline
        
        Args:
            extractors: Dictionary of entity type to streaming extractor
            enable_relationships: Whether to import relationships
            sample_mode: Whether to limit to sample size
            sample_size: Number of items per type in sample mode
        """
        print("🚀 Starting Streaming Graph RAG Import Pipeline")
        print("=" * 60)
        print(f"📋 Import Session ID: {self.import_session_id}")
        print(f"🔢 Batch Size: {self.batch_size:,}")
        print(f"💾 Memory Mode: Streaming (low memory usage)")
        if sample_mode:
            print(f"🧪 Sample Mode: {sample_size:,} records per type")
        print()
        
        pipeline_start = time.time()
        overall_success = True
        
        # Phase 1: Schema Setup
        print("🏗️ Phase 1: Schema Setup")
        if not self.schema_manager.setup_schema():
            print("❌ Schema setup failed, aborting import")
            return False
        print()
        
        # Phase 2-6: Import nodes in dependency order
        import_order = [
            ('organizations', 'Organization', '🏢'),
            ('persons', 'Person', '👥'),
            ('serials', 'Serial', '📚'),
            ('projects', 'Project', '🔬'),
            ('publications', 'Publication', '📄')
        ]
        
        for extractor_key, node_label, emoji in import_order:
            if extractor_key not in extractors:
                print(f"⚠️ No {extractor_key} extractor provided, skipping")
                continue
            
            print(f"{emoji} Importing {node_label} nodes")
            success = self._import_entity_stream(
                extractor_key,
                node_label,
                extractors[extractor_key],
                sample_mode,
                sample_size
            )
            overall_success &= success
            print()
        
        # Phase 7: Import relationships
        if enable_relationships:
            print("🔗 Phase 7: Importing Relationships")
            success = self._import_relationships_stream(extractors, sample_mode)
            overall_success &= success
            print()
        
        # Phase 8: Validation
        print("🔍 Phase 8: Validation")
        validation_result = self.db_manager.validate_data_consistency()
        if validation_result['status'] != 'clean':
            print("⚠️ Data consistency issues found")
            overall_success = False
        else:
            print("✅ Data validation passed")
        print()
        
        # Final statistics
        print("📊 Final Statistics")
        self.db_manager.print_database_stats()
        
        pipeline_duration = time.time() - pipeline_start
        pipeline_min = int(pipeline_duration // 60)
        pipeline_sec = int(pipeline_duration % 60)
        
        print(f"\n⏱️ Total Import Time: {pipeline_min}:{pipeline_sec:02d}")
        
        if overall_success:
            print("✅ Streaming import pipeline completed successfully!")
        else:
            print("⚠️ Streaming import pipeline completed with warnings/errors")
        
        return overall_success
    
    def _import_entity_stream(self, entity_type: str, node_label: str, 
                            extractor: BaseStreamingExtractor,
                            sample_mode: bool, sample_size: int) -> bool:
        """Import a single entity type using streaming"""
        try:
            # Get total count if available (for progress tracking)
            total_count = None
            if hasattr(extractor.es_client, 'count_documents'):
                total_count = extractor.es_client.count_documents(extractor.index_name)
                if sample_mode and total_count > sample_size:
                    total_count = sample_size
            
            progress = StreamingProgress(
                phase=ImportPhase(entity_type.lower() if entity_type.lower() in [e.value for e in ImportPhase] else 'organization'),
                entity_type=entity_type,
                total_items=total_count,
                processed_items=0,
                start_time=time.time()
            )
            
            print(f"  📊 Estimated total: {total_count:,} documents" if total_count else "  📊 Total count unknown")
            
            items_processed = 0
            
            # Process in batches
            for batch_num, batch in enumerate(extractor.extract_batches(), 1):
                if sample_mode and items_processed >= sample_size:
                    break
                
                # Limit batch if in sample mode
                if sample_mode and items_processed + len(batch) > sample_size:
                    batch = batch[:sample_size - items_processed]
                
                progress.current_batch = batch_num
                
                # Format documents for Neo4j
                formatted_batch = []
                for doc in batch:
                    formatted_doc = self._format_document(entity_type, doc)
                    if formatted_doc:
                        formatted_batch.append(formatted_doc)
                        # Cache ID for relationship processing
                        self._cache_node_id(entity_type, formatted_doc['es_id'])
                
                # Import batch to Neo4j
                if formatted_batch:
                    imported = self._import_nodes_batch(node_label, formatted_batch)
                    progress.processed_items += imported
                    items_processed += len(batch)
                
                # Update progress
                print(f"\r  📈 {progress.get_progress_string()}", end='', flush=True)
                
                if sample_mode and items_processed >= sample_size:
                    break
            
            print()  # New line after progress
            print(f"  ✅ Imported {progress.processed_items:,} {entity_type}")
            return True
            
        except Exception as e:
            print(f"\n  ❌ Error importing {entity_type}: {e}")
            import traceback
            traceback.print_exc()
            return False
    

    def _import_entity_stream(self, entity_type: str, node_label: str, 
                            extractor: BaseStreamingExtractor,
                            sample_mode: bool, sample_size: int) -> bool:
        """Import a single entity type using streaming with retry logic"""
        MAX_RETRIES = 3
        RETRY_DELAY = 5  # seconds
        INITIAL_BATCH_SIZE = extractor.batch_size
        
        for attempt in range(MAX_RETRIES):
            try:
                # Get total count if available (for progress tracking)
                total_count = None
                if hasattr(extractor.es_client, 'count_documents'):
                    total_count = extractor.es_client.count_documents(extractor.index_name)
                    if sample_mode and total_count > sample_size:
                        total_count = sample_size
                
                progress = StreamingProgress(
                    phase=ImportPhase(entity_type.lower() if entity_type.lower() in [e.value for e in ImportPhase] else 'organization'),
                    entity_type=entity_type,
                    total_items=total_count,
                    processed_items=0,
                    start_time=time.time()
                )
                
                print(f"  📊 Estimated total: {total_count:,} documents" if total_count else "  📊 Total count unknown")
                if extractor.batch_size != INITIAL_BATCH_SIZE:
                    print(f"  🔧 Using reduced batch size: {extractor.batch_size} (was {INITIAL_BATCH_SIZE})")
                
                items_processed = 0
                last_successful_batch = 0
                
                # Track items processed across retries with different batch sizes
                if attempt > 0:
                    # On retry, continue from where we left off
                    items_processed = getattr(self, '_last_processed_count', 0)
                    print(f"  🔄 Resuming from {items_processed:,} items processed")
                
                # Process in batches
                for batch_num, batch in enumerate(extractor.extract_batches(), 1):
                    # Skip already processed items when retrying with different batch size
                    if attempt > 0 and items_processed > 0:
                        # Calculate how many batches to skip based on items already processed
                        items_in_batch = len(batch)
                        if items_processed >= items_in_batch:
                            items_processed -= items_in_batch
                            continue
                        elif items_processed > 0:
                            # Partial batch - skip processed items
                            batch = batch[items_processed:]
                            items_processed = 0
                        
                    if sample_mode and items_processed >= sample_size:
                        break
                    
                    # Limit batch if in sample mode
                    if sample_mode and items_processed + len(batch) > sample_size:
                        batch = batch[:sample_size - items_processed]
                    
                    progress.current_batch = batch_num
                    
                    # Format documents for Neo4j
                    formatted_batch = []
                    for doc in batch:
                        formatted_doc = self._format_document(entity_type, doc)
                        if formatted_doc:
                            formatted_batch.append(formatted_doc)
                            # Cache ID for relationship processing
                            self._cache_node_id(entity_type, formatted_doc['es_id'])
                    
                    # Import batch to Neo4j with smaller sub-batches if needed
                    if formatted_batch:
                        # Split large batches to avoid memory issues
                        SUB_BATCH_SIZE = 500
                        for i in range(0, len(formatted_batch), SUB_BATCH_SIZE):
                            sub_batch = formatted_batch[i:i + SUB_BATCH_SIZE]
                            imported = self._import_nodes_batch_with_retry(node_label, sub_batch)
                            progress.processed_items += imported
                        
                        items_processed += len(batch)
                        last_successful_batch = batch_num
                        # Save progress for potential retry
                        self._last_processed_count = progress.processed_items
                    
                    # Update progress
                    print(f"\r  📈 {progress.get_progress_string()}", end='', flush=True)
                    
                    if sample_mode and items_processed >= sample_size:
                        break
                
                print()  # New line after progress
                print(f"  ✅ Imported {progress.processed_items:,} {entity_type}")
                # Clear progress tracking on success
                if hasattr(self, '_last_processed_count'):
                    delattr(self, '_last_processed_count')
                return True
                
            except (ConnectionTimeout, ConnectionError, TransportError) as e:
                print(f"\n  🌐 ES timeout error: {e}")
                
                # Try reducing batch size for ES timeouts
                current_batch_size = extractor.batch_size
                if current_batch_size > 100:  # Don't go below 100
                    new_batch_size = max(100, current_batch_size // 2)
                    print(f"  🔧 Reducing ES batch size: {current_batch_size} → {new_batch_size}")
                    extractor.set_batch_size(new_batch_size)
                    
                    if attempt < MAX_RETRIES - 1:
                        print(f"  ⏳ Retrying with smaller batch size in {RETRY_DELAY} seconds...")
                        time.sleep(RETRY_DELAY)
                        continue
                else:
                    print(f"  ❌ Cannot reduce batch size further (already at {current_batch_size})")
                
                if attempt == MAX_RETRIES - 1:
                    print(f"\n  ❌ ES timeout error importing {entity_type} after {MAX_RETRIES} attempts")
                    return False
                    
            except Exception as e:
                print(f"\n  ⚠️ Attempt {attempt + 1}/{MAX_RETRIES} failed for {entity_type}: {e}")
                if attempt < MAX_RETRIES - 1:
                    print(f"  ⏳ Retrying in {RETRY_DELAY} seconds...")
                    time.sleep(RETRY_DELAY)
                else:
                    print(f"\n  ❌ Error importing {entity_type} after {MAX_RETRIES} attempts: {e}")
                    import traceback
                    traceback.print_exc()
                    return False
                
    def _import_nodes_batch_with_retry(self, node_label: str, nodes: List[Dict[str, Any]]) -> int:
        """Import a batch of nodes to Neo4j with retry logic"""
        if not nodes:
            return 0
        
        MAX_RETRIES = 3
        
        for attempt in range(MAX_RETRIES):
            try:
                query = f"""
                UNWIND $nodes AS node
                MERGE (n:{node_label} {{es_id: node.es_id}})
                SET n = node
                SET n.imported_at = datetime()
                SET n.import_session = $session_id
                RETURN count(n) as processed
                """
                
                with self.connection.get_session() as session:
                    # Use explicit transaction with timeout
                    with session.begin_transaction() as tx:
                        result = tx.run(query, nodes=nodes, session_id=self.import_session_id)
                        processed_count = result.single()["processed"]
                        tx.commit()
                        return processed_count
                        
            except Exception as e:
                if "MemoryPoolOutOfMemoryError" in str(e):
                    # Memory error - split batch and retry
                    if len(nodes) > 1:
                        print(f"\n    ⚠️ Memory error, splitting batch of {len(nodes)} into smaller chunks...")
                        mid = len(nodes) // 2
                        count1 = self._import_nodes_batch_with_retry(node_label, nodes[:mid])
                        count2 = self._import_nodes_batch_with_retry(node_label, nodes[mid:])
                        return count1 + count2
                    else:
                        # Single node still failing - skip it
                        print(f"\n    ❌ Skipping node due to memory constraints: {nodes[0].get('es_id', 'unknown')}")
                        return 0
                elif attempt < MAX_RETRIES - 1:
                    time.sleep(1)  # Brief pause before retry
                    continue
                else:
                    raise
                
    def _import_relationships_stream(self, extractors: Dict[str, BaseStreamingExtractor], 
                                   sample_mode: bool) -> bool:
        """Import relationships using node-centric approach"""
        try:
            print("  🔍 Processing relationships by querying existing nodes...")
            
            # Initialize ES client for lookups
            from es_client.client import ElasticsearchClient
            es_client = ElasticsearchClient()
            
            node_relationship_processor = NodeCentricRelationshipProcessor(
                self.connection, es_client, self.import_session_id
            )
            
            total_relationships = 0
            
            # Process relationships in order of confidence/importance
            relationship_types = [
                ("AFFILIATED", "👥", "Person", "Organization"),
                ("AUTHORED", "📄", "Person", "Publication"), 
                ("INVOLVED_IN", "🔬", "Person", "Project"),
                ("PARTNER", "🏢", "Organization", "Project"),
                ("PUBLISHED_IN", "📚", "Publication", "Serial"),
                ("PART_OF", "🏛️", "Organization", "Organization")
            ]
            
            for rel_type, emoji, source_label, target_label in relationship_types:
                print(f"\n  {emoji} Processing {rel_type} relationships ({source_label} → {target_label})...")
                
                rel_count = node_relationship_processor.process_relationship_type(
                    rel_type, source_label, target_label, sample_mode
                )
                total_relationships += rel_count
                print(f"    ✓ {rel_type}: {rel_count:,} relationships created")
            
            print(f"\n  ✅ Total relationships imported: {total_relationships:,}")
            return True
            
        except Exception as e:
            print(f"\n  ❌ Error importing relationships: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def _process_publication_relationships(self, extractor: BaseStreamingExtractor, sample_mode: bool) -> int:
        """Process relationships from publications"""
        total_count = 0
        
        for batch_num, batch in enumerate(extractor.extract_batches(), 1):
            batch_relationships = []
            
            for doc in batch:
                pub_id = doc.get('Id', '')
                if not pub_id:
                    continue
                
                # AUTHORED relationships (persons)
                persons = doc.get('Persons', [])
                if isinstance(persons, list):
                    for person_data in persons:
                        if isinstance(person_data, dict):
                            # Handle both PersonId and PersonID
                            person_id = person_data.get('PersonId') or person_data.get('PersonID')
                            if person_id:
                                role = person_data.get('Role', {})
                                batch_relationships.append({
                                    'source_id': str(person_id),
                                    'target_id': str(pub_id),
                                    'rel_type': 'AUTHORED',
                                    'properties': {
                                        'order': person_data.get('Order', 0),
                                        'role_name': role.get('NameEng', '') if isinstance(role, dict) else ''
                                    }
                                })
                
                # OUTPUT_OF relationships (project)
                project = doc.get('Project')
                if isinstance(project, dict):
                    project_id = project.get('ProjectId') or project.get('ID')
                    if project_id:
                        batch_relationships.append({
                            'source_id': str(project_id),
                            'target_id': str(pub_id),
                            'rel_type': 'OUTPUT_OF',
                            'properties': {}
                        })
                
                # PUBLISHED_IN relationships (series)
                series = doc.get('Series', [])
                if isinstance(series, list):
                    for series_item in series:
                        if isinstance(series_item, dict):
                            serial_data = series_item.get('SerialItem', {})
                            if isinstance(serial_data, dict):
                                serial_id = serial_data.get('Id')
                                if serial_id:
                                    batch_relationships.append({
                                        'source_id': str(pub_id),
                                        'target_id': str(serial_id),
                                        'rel_type': 'PUBLISHED_IN',
                                        'properties': {
                                            'serial_number': series_item.get('SerialNumber', '')
                                        }
                                    })
            
            if batch_relationships:
                imported = self._import_relationships_batch(batch_relationships)
                total_count += imported
            
            # In sample mode, process more batches to get reasonable relationship counts
            if sample_mode and batch_num >= 50:
                break
        
        return total_count
    
    def _process_project_relationships(self, extractor: BaseStreamingExtractor, sample_mode: bool) -> int:
        """Process relationships from projects"""
        total_count = 0
        
        for batch_num, batch in enumerate(extractor.extract_batches(), 1):
            batch_relationships = []
            
            for doc in batch:
                project_id = str(doc.get('ID', ''))
                if not project_id:
                    continue
                
                # INVOLVES relationships (persons)
                persons = doc.get('Persons', [])
                if isinstance(persons, list):
                    for person_data in persons:
                        if isinstance(person_data, dict):
                            # Use PersonID for projects (uppercase)
                            person_id = person_data.get('PersonID')
                            if person_id:
                                batch_relationships.append({
                                    'source_id': str(person_id),
                                    'target_id': str(project_id),
                                    'rel_type': 'INVOLVES',
                                    'properties': {
                                        'role_name': person_data.get('PersonRoleName_en', '')
                                    }
                                })
                
                # PARTNER relationships (organizations)
                organizations = doc.get('Organizations', [])
                if isinstance(organizations, list):
                    for org_data in organizations:
                        if isinstance(org_data, dict):
                            org_id = org_data.get('OrganizationID')
                            if org_id:
                                batch_relationships.append({
                                    'source_id': str(org_id),
                                    'target_id': str(project_id),
                                    'rel_type': 'PARTNER',
                                    'properties': {
                                        'role_name': org_data.get('OrganizationRoleNameEn', '')
                                    }
                                })
            
            if batch_relationships:
                imported = self._import_relationships_batch(batch_relationships)
                total_count += imported
            
            # In sample mode, process more batches to get reasonable relationship counts
            if sample_mode and batch_num >= 50:
                break
        
        return total_count
    
    def _process_person_relationships(self, extractor: BaseStreamingExtractor, sample_mode: bool) -> int:
        """Process relationships from persons"""
        total_count = 0
        
        for batch_num, batch in enumerate(extractor.extract_batches(), 1):
            batch_relationships = []
            
            for doc in batch:
                person_id = doc.get('Id', '')
                if not person_id:
                    continue
                
                # AFFILIATED relationships (organization_home)
                org_home = doc.get('OrganizationHome', [])
                if isinstance(org_home, list):
                    for org_data in org_home:
                        if isinstance(org_data, dict):
                            org_id = org_data.get('OrganizationId') or org_data.get('organization_id')
                            if org_id:
                                batch_relationships.append({
                                    'source_id': str(person_id),
                                    'target_id': str(org_id),
                                    'rel_type': 'AFFILIATED',
                                    'properties': {
                                        'role': org_data.get('Role', ''),
                                        'start_date': org_data.get('StartDate', ''),
                                        'end_date': org_data.get('EndDate', '')
                                    }
                                })
            
            if batch_relationships:
                imported = self._import_relationships_batch(batch_relationships)
                total_count += imported
            
            # In sample mode, process more batches to get reasonable relationship counts
            if sample_mode and batch_num >= 50:
                break
        
        return total_count
    
    def _process_organization_relationships(self, extractor: BaseStreamingExtractor, sample_mode: bool) -> int:
        """Process relationships from organizations"""
        total_count = 0
        
        for batch_num, batch in enumerate(extractor.extract_batches(), 1):
            batch_relationships = []
            
            for doc in batch:
                org_id = doc.get('Id', '')
                if not org_id:
                    continue
                
                # CHILD_OF relationships (organization_parents)
                org_parents = doc.get('OrganizationParents', [])
                if isinstance(org_parents, list):
                    for parent_data in org_parents:
                        if isinstance(parent_data, dict):
                            parent_id = parent_data.get('OrganizationId') or parent_data.get('organization_id')
                            if parent_id:
                                batch_relationships.append({
                                    'source_id': str(parent_id),
                                    'target_id': str(org_id),
                                    'rel_type': 'PARENT_OF',
                                    'properties': {
                                        'level': parent_data.get('Level', 0)
                                    }
                                })
            
            if batch_relationships:
                imported = self._import_relationships_batch(batch_relationships)
                total_count += imported
            
            # In sample mode, process more batches to get reasonable relationship counts
            if sample_mode and batch_num >= 50:
                break
        
        return total_count
    
    def _import_relationships_batch(self, relationships: List[Dict[str, Any]]) -> int:
        """Import a batch of relationships"""
        if not relationships:
            return 0
        
        # Group by relationship type for efficiency
        by_type = {}
        for rel in relationships:
            rel_type = rel['rel_type']
            if rel_type not in by_type:
                by_type[rel_type] = []
            by_type[rel_type].append(rel)
        
        total_created = 0
        
        with self.connection.get_session() as session:
            for rel_type, rels in by_type.items():
                query = f"""
                UNWIND $rels AS rel
                MATCH (source {{es_id: rel.source_id}})
                MATCH (target {{es_id: rel.target_id}})
                MERGE (source)-[r:{rel_type}]->(target)
                SET r = rel.properties
                SET r.imported_at = datetime()
                SET r.import_session = $session_id
                RETURN count(r) as created
                """
                
                try:
                    result = session.run(query, rels=rels, session_id=self.import_session_id)
                    created = result.single()["created"]
                    total_created += created
                except Exception as e:
                    # Log but don't fail on individual relationship errors
                    print(f"\n    ⚠️ Warning: Failed to create some {rel_type} relationships: {e}")
        
        return total_created
    
    def _cache_node_id(self, entity_type: str, es_id: str):
        """Cache node ID for relationship validation"""
        if entity_type not in self.node_id_cache:
            self.node_id_cache[entity_type] = set()
        self.node_id_cache[entity_type].add(str(es_id))
    
    def _validate_relationship(self, source_type: str, target_type: str, 
                             source_id: str, target_id: str) -> bool:
        """Validate that both nodes exist (using cache for efficiency)"""
        # For streaming, we might not have all IDs cached
        # In production, you might want to use a Bloom filter or query Neo4j
        return True  # For now, attempt all relationships
    
    def _extract_unified_keywords(self, doc: Dict[str, Any], entity_type: str) -> List[str]:
        """Extract and merge all keyword-like fields into ALL CAPS list"""
        all_keywords = []
        
        if entity_type in ['publications', 'projects']:
            # Extract Keywords (simple array of objects with Value field) - mainly for publications
            keywords = doc.get('Keywords', [])
            for keyword in keywords:
                if isinstance(keyword, dict) and 'Value' in keyword:
                    value = keyword['Value'].strip()
                    if value:
                        all_keywords.append(value.upper())
            
            # Extract Categories (complex objects with Swedish and English names)
            categories = doc.get('Categories', [])
            for category in categories:
                if isinstance(category, dict):
                    # Handle different category structures between Publications and Projects
                    if entity_type == 'projects':
                        # Projects have: {CategoryID: "...", Category: {NameSwe: "...", NameEng: "..."}}
                        category_obj = category.get('Category', {})
                    else:
                        # Publications have: {NameSwe: "...", NameEng: "...", Type: {...}}
                        category_obj = category
                    
                    # Extract Swedish name
                    name_swe = category_obj.get('NameSwe', '')
                    if name_swe:
                        all_keywords.append(name_swe.strip().upper())
                    
                    # Extract English name
                    name_eng = category_obj.get('NameEng', '')
                    if name_eng:
                        all_keywords.append(name_eng.strip().upper())
        
        # Remove duplicates and sort
        return sorted(list(set(all_keywords)))
    
    def _format_document(self, doc_type: str, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Format Elasticsearch document for Neo4j import"""
        try:
            if doc_type == 'persons':
                return self._format_person_document(doc)
            elif doc_type == 'organizations':
                return self._format_organization_document(doc)
            elif doc_type == 'publications':
                return self._format_publication_document(doc)
            elif doc_type == 'projects':
                return self._format_project_document(doc)
            elif doc_type == 'serials':
                return self._format_serial_document(doc)
            return None
        except Exception as e:
            print(f"    ⚠️ Warning: Failed to format {doc_type} document {doc.get('Id', doc.get('ID', 'unknown'))}: {e}")
            return None
    
    def _format_person_document(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Format person document with simplified identifiers"""
        # Extract key identifiers as direct properties
        identifiers = doc.get('Identifiers', [])
        cpl_id = ''
        scopus_id = ''
        orcid = ''
        
        for identifier in identifiers:
            if isinstance(identifier, dict):
                id_type = identifier.get('Type', {}).get('Value', '')
                value =  identifier.get('Type', {}).get('Id', '')
                if id_type == 'CPL_PERSONID':
                    cpl_id = value
                elif id_type == 'SCOPUS_AUTHID':
                    scopus_id = value
                elif id_type == 'ORCID':
                    orcid = value
        
        return {
            'es_id': doc.get('Id', ''),
            'first_name': doc.get('FirstName', ''),
            'last_name': doc.get('LastName', ''),
            'display_name': doc.get('DisplayName', ''),
            'birth_year': doc.get('BirthYear', 0),
            # 'is_active': doc.get('IsActive', False),
            # 'has_identifiers': doc.get('HasIdentifiers', False),
            # 'id_cpl': cpl_id,
            # 'id_scopus': scopus_id,
            # 'id_orcid': orcid,
            # 'identifiers_json': json.dumps(identifiers),  # Keep original as backup
        }
    
    def _format_organization_document(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Format organization document"""
        return {
            'es_id': doc.get('Id', ''),
            'name_swe': doc.get('NameSwe', ''),
            'name_eng': doc.get('NameEng', ''),
            'display_name_swe': doc.get('DisplayNameSwe', ''),
            'display_name_eng': doc.get('DisplayNameEng', ''),
            'city': doc.get('City', ''),
            'country': doc.get('Country', ''),
            'geo_lat': float(doc.get('GeoLat', 0)) if doc.get('GeoLat') else 0,
            'geo_long': float(doc.get('GeoLong', 0)) if doc.get('GeoLong') else 0,
            'level': doc.get('Level', 0),
            # 'is_active': doc.get('IsActive', False),
            # 'organization_types_json': json.dumps(doc.get('OrganizationTypes', [])),
        }
    
    def _format_publication_document(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Format publication document, handling nested objects safely"""
        # Extract basic properties only, avoid nested objects
        formatted = {
            'es_id': doc.get('Id', ''),
            'title': doc.get('Title', ''),
            'abstract': doc.get('Abstract', ''),
            'year': doc.get('Year', 0),
            'is_draft': doc.get('IsDraft', False),
            'is_deleted': doc.get('IsDeleted', False),
        }
        
        # Handle potentially complex fields safely
        pub_type = doc.get('PublicationType', '')
        if isinstance(pub_type, dict):
            formatted['publication_type'] = pub_type.get('Value', '')
            formatted['publication_type_json'] = json.dumps(pub_type)
        else:
            formatted['publication_type'] = str(pub_type)
        
        source = doc.get('Source', '')
        if isinstance(source, dict):
            formatted['source'] = source.get('Title', source.get('Value', ''))
            formatted['source_json'] = json.dumps(source)
        else:
            formatted['source'] = str(source)
        
        # Extract unified keywords from Keywords and Categories fields
        keywords = self._extract_unified_keywords(doc, 'publications')
        formatted['keywords'] = keywords
        formatted['keywords_count'] = len(keywords)
        
        # DON'T store relationship data (Persons, Organizations, Series, Project) on nodes
        # These will be processed directly from ES documents during relationship phase
        
        return formatted
    
    def _format_project_document(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Format project document"""
        formatted = {
            'es_id': str(doc.get('ID', '')),
            'title_swe': doc.get('ProjectTitleSwe', ''),
            'title_eng': doc.get('ProjectTitleEng', ''),
            'description_swe': doc.get('ProjectDescriptionSwe', ''),
            'description_eng': doc.get('ProjectDescriptionEng', ''),
            'start_date': doc.get('StartDate', ''),
            'end_date': doc.get('EndDate', ''),
            'publish_status': doc.get('PublishStatus', 0),
        }
        
        # Extract unified keywords from Categories field (projects don't typically have Keywords)
        keywords = self._extract_unified_keywords(doc, 'projects')
        formatted['keywords'] = keywords
        formatted['keywords_count'] = len(keywords)
        
        # DON'T store relationship data (Persons, Organizations) on project nodes
        
        return formatted
    
    def _format_serial_document(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Format serial document"""
        return {
            'es_id': doc.get('Id', ''),
            'title': doc.get('Title', ''),
            'start_year': doc.get('StartYear', 0),
            'end_year': doc.get('EndYear', 0),
            'publisher': doc.get('Publisher', ''),
            'country': doc.get('Country', ''),
            'is_open_access': doc.get('IsOpenAccess', False),
            'is_peer_reviewed': doc.get('IsPeerReviewed', False),
            'is_deleted': doc.get('IsDeleted', False),
        }


class NodeCentricRelationshipProcessor:
    """Process relationships by starting with existing nodes and finding their connections"""
    
    def __init__(self, connection, es_client, import_session_id):
        self.connection = connection
        self.es_client = es_client
        self.import_session_id = import_session_id
    
    def process_relationship_type(self, rel_type: str, source_label: str, target_label: str, 
                                sample_mode: bool) -> int:
        """Process a specific relationship type using node-centric approach"""
        
        if rel_type == "AFFILIATED":
            return self._process_affiliated_relationships(sample_mode)
        elif rel_type == "AUTHORED":
            return self._process_authored_relationships(sample_mode)
        elif rel_type == "INVOLVED_IN":
            return self._process_involved_in_relationships(sample_mode)
        elif rel_type == "PARTNER":
            return self._process_partner_relationships(sample_mode)
        elif rel_type == "PUBLISHED_IN":
            return self._process_published_in_relationships(sample_mode)
        elif rel_type == "PART_OF":
            return self._process_part_of_relationships(sample_mode)
        else:
            print(f"    ⚠️ Unknown relationship type: {rel_type}")
            return 0
    
    def _process_affiliated_relationships(self, sample_mode: bool) -> int:
        """Process AFFILIATED relationships: Person → Organization"""
        total_created = 0
        
        # Get all Person nodes from GraphDB
        with self.connection.get_session() as session:
            result = session.run("MATCH (p:Person) RETURN p.es_id as person_id")
            person_ids = [record['person_id'] for record in result]
        
        if not person_ids:
            print("    No Person nodes found in database")
            return 0
        
        print(f"    Found {len(person_ids):,} Person nodes to process")
        
        # Process in batches for progress reporting
        batch_size = 100
        processed = 0
        
        for i in range(0, len(person_ids), batch_size):
            batch = person_ids[i:i + batch_size]
            batch_relationships = []
            
            for person_id in batch:
                try:
                    # Get person's ES data
                    search_result = self.es_client.search('research-persons-static', {
                        'query': {'term': {'Id': person_id}}
                    })
                    
                    if not search_result['hits']['hits']:
                        continue
                    
                    es_doc = search_result['hits']['hits'][0]['_source']
                    
                    # Extract organization affiliations
                    org_homes = es_doc.get('OrganizationHome', [])
                    if isinstance(org_homes, list):
                        for org_data in org_homes:
                            if isinstance(org_data, dict):
                                org_id = org_data.get('OrganizationId') or org_data.get('organization_id')
                                if org_id:
                                    batch_relationships.append({
                                        'source_id': str(person_id),
                                        'target_id': str(org_id),
                                        'rel_type': 'AFFILIATED',
                                        'properties': {
                                            'role': org_data.get('Role', ''),
                                            'start_year': org_data.get('StartYear', 0),
                                            'end_year': org_data.get('EndYear', 0)
                                        }
                                    })
                
                except Exception as e:
                    # Skip individual person errors
                    continue
            
            # Create relationships for this batch
            if batch_relationships:
                created = self._create_relationships_batch(batch_relationships)
                total_created += created
            
            processed += len(batch)
            
            # Progress reporting
            if processed % 500 == 0 or processed == len(person_ids):
                print(f"    Processed {processed:,} of {len(person_ids):,} persons ({total_created:,} relationships created)")
            
            # Sample mode limit
            if sample_mode and processed >= 100000:
                print(f"    Sample mode: stopped after {processed:,} persons")
                break
        
        return total_created
    
    def _process_authored_relationships(self, sample_mode: bool) -> int:
        """Process AUTHORED relationships: Person → Publication"""
        total_created = 0
        
        # Get all Publication nodes from GraphDB
        with self.connection.get_session() as session:
            result = session.run("MATCH (p:Publication) RETURN p.es_id as pub_id")
            pub_ids = [record['pub_id'] for record in result]
        
        if not pub_ids:
            print("    No Publication nodes found in database")
            return 0
        
        print(f"    Found {len(pub_ids):,} Publication nodes to process")
        
        batch_size = 50  # Smaller batch for publications (they have more relationships)
        processed = 0
        
        for i in range(0, len(pub_ids), batch_size):
            batch = pub_ids[i:i + batch_size]
            batch_relationships = []
            
            for pub_id in batch:
                try:
                    # Get publication's ES data
                    search_result = self.es_client.search('research-publications-static', {
                        'query': {'term': {'Id': pub_id}}
                    })
                    
                    if not search_result['hits']['hits']:
                        continue
                    
                    es_doc = search_result['hits']['hits'][0]['_source']
                    
                    # Extract authors
                    persons = es_doc.get('Persons', [])
                    if isinstance(persons, list):
                        for person_data in persons:
                            if isinstance(person_data, dict):
                                person_id = person_data.get('PersonId') or person_data.get('PersonID')
                                if person_id:
                                    role = person_data.get('Role', {})
                                    batch_relationships.append({
                                        'source_id': str(person_id),
                                        'target_id': str(pub_id),
                                        'rel_type': 'AUTHORED',
                                        'properties': {
                                            'order': person_data.get('Order', 0),
                                            'role_name': role.get('NameEng', '') if isinstance(role, dict) else ''
                                        }
                                    })
                
                except Exception as e:
                    continue
            
            # Create relationships for this batch
            if batch_relationships:
                created = self._create_relationships_batch(batch_relationships)
                total_created += created
            
            processed += len(batch)
            
            # Progress reporting
            if processed % 200 == 0 or processed == len(pub_ids):
                print(f"    Processed {processed:,} of {len(pub_ids):,} publications ({total_created:,} relationships created)")
            
            # Sample mode limit
            if sample_mode and processed >= 500000:
                print(f"    Sample mode: stopped after {processed:,} publications")
                break
        
        return total_created
    
    def _process_involved_in_relationships(self, sample_mode: bool) -> int:
        """Process INVOLVED_IN relationships: Person → Project"""
        total_created = 0
        
        # Get all Project nodes from GraphDB
        with self.connection.get_session() as session:
            result = session.run("MATCH (p:Project) RETURN p.es_id as project_id")
            project_ids = [record['project_id'] for record in result]
        
        if not project_ids:
            print("    No Project nodes found in database")
            return 0
        
        print(f"    Found {len(project_ids):,} Project nodes to process")
        
        batch_size = 50
        processed = 0
        
        for i in range(0, len(project_ids), batch_size):
            batch = project_ids[i:i + batch_size]
            batch_relationships = []
            
            for project_id in batch:
                try:
                    # Get project's ES data
                    search_result = self.es_client.search('research-projects-static', {
                        'query': {'term': {'ID': project_id}}
                    })
                    
                    if not search_result['hits']['hits']:
                        continue
                    
                    es_doc = search_result['hits']['hits'][0]['_source']
                    
                    # Extract persons involved
                    persons = es_doc.get('Persons', [])
                    if isinstance(persons, list):
                        for person_data in persons:
                            if isinstance(person_data, dict):
                                person_id = person_data.get('PersonID')  # Projects use PersonID
                                if person_id:
                                    batch_relationships.append({
                                        'source_id': str(person_id),
                                        'target_id': str(project_id),
                                        'rel_type': 'INVOLVED_IN',
                                        'properties': {
                                            'role_name': person_data.get('PersonRoleName_en', '')
                                        }
                                    })
                
                except Exception as e:
                    continue
            
            # Create relationships for this batch
            if batch_relationships:
                created = self._create_relationships_batch(batch_relationships)
                total_created += created
            
            processed += len(batch)
            
            # Progress reporting
            if processed % 200 == 0 or processed == len(project_ids):
                print(f"    Processed {processed:,} of {len(project_ids):,} projects ({total_created:,} relationships created)")
            
            # Sample mode limit
            if sample_mode and processed >= 500000:
                print(f"    Sample mode: stopped after {processed:,} projects")
                break
        
        return total_created
    
    def _process_partner_relationships(self, sample_mode: bool) -> int:
        """Process PARTNER relationships: Organization → Project"""
        total_created = 0
        
        # Get all Project nodes from GraphDB
        with self.connection.get_session() as session:
            result = session.run("MATCH (p:Project) RETURN p.es_id as project_id")
            project_ids = [record['project_id'] for record in result]
        
        if not project_ids:
            print("    No Project nodes found in database")
            return 0
        
        print(f"    Found {len(project_ids):,} Project nodes to process")
        
        batch_size = 50
        processed = 0
        
        for i in range(0, len(project_ids), batch_size):
            batch = project_ids[i:i + batch_size]
            batch_relationships = []
            
            for project_id in batch:
                try:
                    # Get project's ES data
                    search_result = self.es_client.search('research-projects-static', {
                        'query': {'term': {'ID': project_id}}
                    })
                    
                    if not search_result['hits']['hits']:
                        continue
                    
                    es_doc = search_result['hits']['hits'][0]['_source']
                    
                    # Extract organization partners
                    organizations = es_doc.get('Organizations', [])
                    if isinstance(organizations, list):
                        for org_data in organizations:
                            if isinstance(org_data, dict):
                                org_id = org_data.get('OrganizationID')
                                if org_id:
                                    batch_relationships.append({
                                        'source_id': str(org_id),
                                        'target_id': str(project_id),
                                        'rel_type': 'PARTNER',
                                        'properties': {
                                            'role_name': org_data.get('OrganizationRoleNameEn', '')
                                        }
                                    })
                
                except Exception as e:
                    continue
            
            # Create relationships for this batch
            if batch_relationships:
                created = self._create_relationships_batch(batch_relationships)
                total_created += created
            
            processed += len(batch)
            
            # Progress reporting
            if processed % 200 == 0 or processed == len(project_ids):
                print(f"    Processed {processed:,} of {len(project_ids):,} projects ({total_created:,} relationships created)")
            
            # Sample mode limit
            if sample_mode and processed >= 500000:
                print(f"    Sample mode: stopped after {processed:,} projects")
                break
        
        return total_created
    
    def _process_published_in_relationships(self, sample_mode: bool) -> int:
        """Process PUBLISHED_IN relationships: Publication → Serial"""
        total_created = 0
        
        # Get all Publication nodes from GraphDB
        with self.connection.get_session() as session:
            result = session.run("MATCH (p:Publication) RETURN p.es_id as pub_id")
            pub_ids = [record['pub_id'] for record in result]
        
        if not pub_ids:
            print("    No Publication nodes found in database")
            return 0
        
        print(f"    Found {len(pub_ids):,} Publication nodes to process")
        
        batch_size = 100
        processed = 0
        
        for i in range(0, len(pub_ids), batch_size):
            batch = pub_ids[i:i + batch_size]
            batch_relationships = []
            
            for pub_id in batch:
                try:
                    # Get publication's ES data
                    search_result = self.es_client.search('research-publications-static', {
                        'query': {'term': {'Id': pub_id}}
                    })
                    
                    if not search_result['hits']['hits']:
                        continue
                    
                    es_doc = search_result['hits']['hits'][0]['_source']
                    
                    # Extract series/serials
                    series = es_doc.get('Series', [])
                    if isinstance(series, list):
                        for series_item in series:
                            if isinstance(series_item, dict):
                                serial_data = series_item.get('SerialItem', {})
                                if isinstance(serial_data, dict):
                                    serial_id = serial_data.get('Id')
                                    if serial_id:
                                        batch_relationships.append({
                                            'source_id': str(pub_id),
                                            'target_id': str(serial_id),
                                            'rel_type': 'PUBLISHED_IN',
                                            'properties': {
                                                'serial_number': series_item.get('SerialNumber', '')
                                            }
                                        })
                
                except Exception as e:
                    continue
            
            # Create relationships for this batch
            if batch_relationships:
                created = self._create_relationships_batch(batch_relationships)
                total_created += created
            
            processed += len(batch)
            
            # Progress reporting
            if processed % 500 == 0 or processed == len(pub_ids):
                print(f"    Processed {processed:,} of {len(pub_ids):,} publications ({total_created:,} relationships created)")
            
            # Sample mode limit
            if sample_mode and processed >= 100000:
                print(f"    Sample mode: stopped after {processed:,} publications")
                break
        
        return total_created
    
    def _create_relationships_batch(self, relationships: List[Dict[str, Any]]) -> int:
        """Create a batch of relationships with existence validation"""
        if not relationships:
            return 0
        
        # Group by relationship type for efficiency
        by_type = {}
        for rel in relationships:
            rel_type = rel['rel_type']
            if rel_type not in by_type:
                by_type[rel_type] = []
            by_type[rel_type].append(rel)
        
        total_created = 0
        
        with self.connection.get_session() as session:
            for rel_type, rels in by_type.items():
                # Use MERGE to avoid duplicates and skip if nodes don't exist
                query = f"""
                UNWIND $rels AS rel
                MATCH (source {{es_id: rel.source_id}})
                MATCH (target {{es_id: rel.target_id}})
                MERGE (source)-[r:{rel_type}]->(target)
                SET r = rel.properties
                SET r.imported_at = datetime()
                SET r.import_session = $session_id
                RETURN count(r) as created
                """
                
                try:
                    result = session.run(query, rels=rels, session_id=self.import_session_id)
                    created = result.single()["created"]
                    total_created += created
                except Exception as e:
                    # Log but don't fail on individual relationship errors
                    print(f"      ⚠️ Warning: Failed to create some {rel_type} relationships: {e}")
        
        return total_created
    
    def _process_part_of_relationships(self, sample_mode: bool) -> int:
        """Process PART_OF relationships: Organization → Organization (child part of parent)"""
        total_created = 0
        
        # Get all Organization nodes from GraphDB
        with self.connection.get_session() as session:
            result = session.run("MATCH (o:Organization) RETURN o.es_id as org_id")
            org_ids = [record['org_id'] for record in result]
        
        if not org_ids:
            print("    No Organization nodes found in database")
            return 0
        
        print(f"    Found {len(org_ids):,} Organization nodes to process")
        
        batch_size = 100
        processed = 0
        
        for i in range(0, len(org_ids), batch_size):
            batch = org_ids[i:i + batch_size]
            batch_relationships = []
            
            for org_id in batch:
                try:
                    # Get organization's ES data
                    search_result = self.es_client.search('research-organizations-static', {
                        'query': {'term': {'Id': org_id}}
                    })
                    
                    if not search_result['hits']['hits']:
                        continue
                    
                    es_doc = search_result['hits']['hits'][0]['_source']
                    
                    # Extract organization parents (this org is PART_OF its parents)
                    org_parents = es_doc.get('OrganizationParents', [])
                    if isinstance(org_parents, list):
                        for parent_data in org_parents:
                            if isinstance(parent_data, dict):
                                parent_id = parent_data.get('ParentOrganizationId')
                                if parent_id:
                                    batch_relationships.append({
                                        'source_id': str(org_id),  # Child organization
                                        'target_id': str(parent_id),  # Parent organization
                                        'rel_type': 'PART_OF',
                                        'properties': {
                                            'level': parent_data.get('Level', 0)
                                        }
                                    })
                
                except Exception as e:
                    # Skip individual organization errors
                    continue
            
            # Create relationships for this batch
            if batch_relationships:
                created = self._create_relationships_batch(batch_relationships)
                total_created += created
            
            processed += len(batch)
            
            # Progress reporting
            if processed % 500 == 0 or processed == len(org_ids):
                print(f"    Processed {processed:,} of {len(org_ids):,} organizations ({total_created:,} relationships created)")
            
            # Sample mode limit
            if sample_mode and processed >= 1000:
                print(f"    Sample mode: stopped after {processed:,} organizations")
                break
        
        return total_created
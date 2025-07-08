"""
Phase-based import pipeline for Neo4j Graph RAG
"""

import time
import json
from typing import Dict, List, Any, Optional, Iterator, Tuple
from datetime import datetime
from dataclasses import dataclass
from enum import Enum

from .connection import Neo4jConnection
from .schema import SchemaManager
from .db_manager import DatabaseManager


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
class ImportProgress:
    """Track import progress"""
    phase: ImportPhase
    total_items: int
    processed_items: int
    start_time: float
    current_batch: int = 0
    total_batches: int = 0
    errors: List[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []
    
    @property
    def percentage(self) -> float:
        if self.total_items == 0:
            return 0.0
        return (self.processed_items / self.total_items) * 100
    
    @property
    def elapsed_time(self) -> float:
        return time.time() - self.start_time
    
    @property
    def eta_seconds(self) -> Optional[float]:
        if self.processed_items == 0:
            return None
        rate = self.processed_items / self.elapsed_time
        remaining = self.total_items - self.processed_items
        return remaining / rate if rate > 0 else None


class ImportPipeline:
    """Orchestrates the complete import process"""
    
    def __init__(self, connection: Neo4jConnection, batch_size: int = 1000):
        self.connection = connection
        self.batch_size = batch_size
        self.schema_manager = SchemaManager(connection)
        self.db_manager = DatabaseManager(connection)
        self.progress_callback = None
        self.import_session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    def set_progress_callback(self, callback):
        """Set callback function for progress updates"""
        self.progress_callback = callback
    
    def _update_progress(self, progress: ImportProgress):
        """Update progress and call callback if set"""
        if self.progress_callback:
            self.progress_callback(progress)
        else:
            self._print_progress(progress)
    
    def _print_progress(self, progress: ImportProgress):
        """Default progress printing"""
        eta_str = ""
        if progress.eta_seconds:
            eta_min = int(progress.eta_seconds // 60)
            eta_sec = int(progress.eta_seconds % 60)
            eta_str = f" | ETA: {eta_min}:{eta_sec:02d}"
        
        batch_str = ""
        if progress.total_batches > 0:
            batch_str = f" | Batch: {progress.current_batch}/{progress.total_batches}"
        
        print(f"  📈 {progress.phase.value}: {progress.processed_items:,}/{progress.total_items:,} "
              f"({progress.percentage:.1f}%){batch_str}{eta_str}")
    
    def _create_nodes_batch(self, session, node_type: str, nodes: List[Dict[str, Any]]) -> int:
        """Create a batch of nodes"""
        if not nodes:
            return 0
        
        # Prepare nodes for import
        prepared_nodes = []
        for node in nodes:
            # Convert lists to strings for Neo4j
            prepared_node = {}
            for key, value in node.items():
                if isinstance(value, list):
                    prepared_node[key] = json.dumps(value) if value else "[]"
                elif isinstance(value, dict):
                    prepared_node[key] = json.dumps(value) if value else "{}"
                elif value is None:
                    prepared_node[key] = ""
                else:
                    prepared_node[key] = value
            prepared_nodes.append(prepared_node)
        
        query = f"""
        UNWIND $nodes AS node
        CREATE (n:{node_type})
        SET n = node
        SET n.imported_at = datetime()
        SET n.import_session = $session_id
        RETURN count(n) as created
        """
        
        result = session.run(query, nodes=prepared_nodes, session_id=self.import_session_id)
        return result.single()["created"]
    
    def _create_relationships_batch(self, session, rel_type: str, 
                                  relationships: List[Dict[str, Any]]) -> int:
        """Create a batch of relationships"""
        if not relationships:
            return 0
        
        query = f"""
        UNWIND $rels AS rel
        MATCH (source {{es_id: rel.source_id}})
        MATCH (target {{es_id: rel.target_id}})
        CREATE (source)-[r:{rel_type}]->(target)
        SET r = rel.properties
        SET r.imported_at = datetime()
        SET r.import_session = $session_id
        RETURN count(r) as created
        """
        
        result = session.run(query, rels=relationships, session_id=self.import_session_id)
        return result.single()["created"]
    
    def import_nodes(self, node_type: str, nodes_iterator: Iterator[Dict[str, Any]], 
                    total_count: Optional[int] = None) -> bool:
        """Import nodes in batches"""
        if total_count is None:
            # Convert iterator to list to get count (memory intensive for large datasets)
            nodes = list(nodes_iterator)
            total_count = len(nodes)
            nodes_iterator = iter(nodes)
        
        progress = ImportProgress(
            phase=ImportPhase(node_type.lower()),
            total_items=total_count,
            processed_items=0,
            start_time=time.time(),
            total_batches=(total_count + self.batch_size - 1) // self.batch_size
        )
        
        print(f"📥 Importing {total_count:,} {node_type} nodes...")
        
        try:
            with self.connection.get_session() as session:
                batch = []
                batch_num = 0
                
                for node in nodes_iterator:
                    batch.append(node)
                    
                    if len(batch) >= self.batch_size:
                        batch_num += 1
                        progress.current_batch = batch_num
                        
                        created = self._create_nodes_batch(session, node_type, batch)
                        progress.processed_items += created
                        self._update_progress(progress)
                        
                        batch = []
                
                # Process final batch
                if batch:
                    batch_num += 1
                    progress.current_batch = batch_num
                    created = self._create_nodes_batch(session, node_type, batch)
                    progress.processed_items += created
                    self._update_progress(progress)
            
            print(f"✅ {node_type} import completed: {progress.processed_items:,} nodes")
            return True
            
        except Exception as e:
            progress.errors.append(str(e))
            print(f"❌ {node_type} import failed: {e}")
            return False
    
    def import_relationships(self, rel_type: str, rels_iterator: Iterator[Dict[str, Any]],
                           total_count: Optional[int] = None) -> bool:
        """Import relationships in batches"""
        if total_count is None:
            rels = list(rels_iterator)
            total_count = len(rels)
            rels_iterator = iter(rels)
        
        progress = ImportProgress(
            phase=ImportPhase.RELATIONSHIPS,
            total_items=total_count,
            processed_items=0,
            start_time=time.time(),
            total_batches=(total_count + self.batch_size - 1) // self.batch_size
        )
        
        print(f"🔗 Importing {total_count:,} {rel_type} relationships...")
        
        try:
            with self.connection.get_session() as session:
                batch = []
                batch_num = 0
                
                for rel in rels_iterator:
                    batch.append(rel)
                    
                    if len(batch) >= self.batch_size:
                        batch_num += 1
                        progress.current_batch = batch_num
                        
                        created = self._create_relationships_batch(session, rel_type, batch)
                        progress.processed_items += created
                        self._update_progress(progress)
                        
                        batch = []
                
                # Process final batch
                if batch:
                    batch_num += 1
                    progress.current_batch = batch_num
                    created = self._create_relationships_batch(session, rel_type, batch)
                    progress.processed_items += created
                    self._update_progress(progress)
            
            print(f"✅ {rel_type} relationships import completed: {progress.processed_items:,}")
            return True
            
        except Exception as e:
            progress.errors.append(str(e))
            print(f"❌ {rel_type} relationships import failed: {e}")
            return False
    
    def run_import_pipeline(self, data_extractors: Dict[str, Any], 
                          sample_mode: bool = False, sample_size: int = 1000) -> bool:
        """Run the complete import pipeline"""
        print("🚀 Starting Graph RAG Import Pipeline")
        print("=" * 60)
        print(f"📋 Import Session ID: {self.import_session_id}")
        print(f"🔢 Batch Size: {self.batch_size:,}")
        if sample_mode:
            print(f"🧪 Sample Mode: {sample_size:,} records per type")
        print()
        
        pipeline_start = time.time()
        overall_success = True
        
        # Phase 1: Setup
        print("🏗️ Phase 1: Schema Setup")
        if not self.schema_manager.setup_schema():
            print("❌ Schema setup failed, aborting import")
            return False
        print()
        
        # Phase 2: Organizations (must come first for relationships)
        print("🏢 Phase 2: Organizations")
        if 'organizations' in data_extractors:
            org_data = data_extractors['organizations']
            if sample_mode:
                org_data = list(org_data)[:sample_size]
            success = self.import_nodes("Organization", iter(org_data), len(org_data) if isinstance(org_data, list) else None)
            overall_success &= success
        print()
        
        # Phase 3: Persons
        print("👥 Phase 3: Persons")
        if 'persons' in data_extractors:
            person_data = data_extractors['persons']
            if sample_mode:
                person_data = list(person_data)[:sample_size]
            success = self.import_nodes("Person", iter(person_data), len(person_data) if isinstance(person_data, list) else None)
            overall_success &= success
        print()
        
        # Phase 4: Serials
        print("📚 Phase 4: Serials")
        if 'serials' in data_extractors:
            serial_data = data_extractors['serials']
            if sample_mode:
                serial_data = list(serial_data)[:sample_size]
            success = self.import_nodes("Serial", iter(serial_data), len(serial_data) if isinstance(serial_data, list) else None)
            overall_success &= success
        print()
        
        # Phase 5: Projects
        print("🔬 Phase 5: Projects")
        if 'projects' in data_extractors:
            project_data = data_extractors['projects']
            if sample_mode:
                project_data = list(project_data)[:sample_size]
            success = self.import_nodes("Project", iter(project_data), len(project_data) if isinstance(project_data, list) else None)
            overall_success &= success
        print()
        
        # Phase 6: Publications
        print("🔗 Phase 7: Relationships")
        if 'relationships' in data_extractors:
            for rel_type, rel_data in data_extractors['relationships'].items():
                if rel_data:  # Only process if there are relationships
                    # Don't truncate relationships in sample mode - they're already filtered
                    success = self.import_relationships(rel_type, iter(rel_data), len(rel_data))
                    overall_success &= success
        print()
        
        # Phase 7: Relationships

        
        # Phase 8: Validation
        print("🔍 Phase 8: Validation")
        validation_result = self.db_manager.validate_data_consistency()
        if validation_result['status'] != 'clean':
            print("⚠️ Data consistency issues found")
            overall_success = False
        print()
        
        # Final statistics
        print("📊 Final Statistics")
        self.db_manager.print_database_stats()
        
        pipeline_duration = time.time() - pipeline_start
        pipeline_min = int(pipeline_duration // 60)
        pipeline_sec = int(pipeline_duration % 60)
        
        print(f"\n⏱️ Total Import Time: {pipeline_min}:{pipeline_sec:02d}")
        
        if overall_success:
            print("✅ Import pipeline completed successfully!")
        else:
            print("⚠️ Import pipeline completed with warnings/errors")
        
        return overall_success
    
    def dry_run(self, data_extractors: Dict[str, Any], sample_size: int = 100) -> Dict[str, Any]:
        """Perform a dry run to validate data without importing"""
        print("🧪 Performing Dry Run Validation")
        print("=" * 40)
        
        validation_results = {}
        
        for data_type, data in data_extractors.items():
            if data_type == 'relationships':
                continue
                
            print(f"🔍 Validating {data_type} data...")
            
            # Sample the data
            sample_data = list(data)[:sample_size] if hasattr(data, '__iter__') else []
            
            # Validate structure
            required_fields = self._get_required_fields(data_type)
            missing_fields = []
            type_errors = []
            
            for i, item in enumerate(sample_data):
                for field in required_fields:
                    if field not in item:
                        missing_fields.append(f"Item {i}: missing {field}")
                    elif item[field] is None:
                        missing_fields.append(f"Item {i}: null {field}")
            
            validation_results[data_type] = {
                "sample_size": len(sample_data),
                "missing_fields": missing_fields,
                "type_errors": type_errors,
                "status": "valid" if not (missing_fields or type_errors) else "issues"
            }
            
            status = "✅" if validation_results[data_type]["status"] == "valid" else "⚠️"
            print(f"  {status} {data_type}: {len(sample_data)} samples validated")
        
        return validation_results
    
    def _get_required_fields(self, data_type: str) -> List[str]:
        """Get required fields for each data type"""
        field_map = {
            "organizations": ["es_id", "display_name_eng"],
            "persons": ["es_id", "display_name"],
            "serials": ["es_id", "title"],
            "projects": ["es_id", "title_eng"],
            "publications": ["es_id", "title"]
        }
        return field_map.get(data_type, ["es_id"])
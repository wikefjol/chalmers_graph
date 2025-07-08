"""
Database management with safe operations for Neo4j Graph RAG
"""

import time
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from .connection import Neo4jConnection


class DatabaseManager:
    """Enhanced database management with safety features"""
    
    def __init__(self, connection: Neo4jConnection):
        self.connection = connection
    
    def get_database_stats(self) -> Dict[str, any]:
        """Get comprehensive database statistics"""
        with self.connection.get_session() as session:
            # Node counts by label
            node_result = session.run("""
                MATCH (n)
                RETURN labels(n)[0] as label, count(n) as count
                ORDER BY count DESC
            """)
            nodes = [(record['label'], record['count']) for record in node_result]
            
            # Relationship counts by type
            rel_result = session.run("""
                MATCH ()-[r]->()
                RETURN type(r) as type, count(r) as count
                ORDER BY count DESC
            """)
            relationships = [(record['type'], record['count']) for record in rel_result]
            
            # Total counts
            total_nodes_result = session.run("MATCH (n) RETURN count(n) as total")
            total_nodes = total_nodes_result.single()["total"]
            
            total_rels_result = session.run("MATCH ()-[r]->() RETURN count(r) as total")
            total_relationships = total_rels_result.single()["total"]
            
            # Database size info
            try:
                size_result = session.run("CALL apoc.monitor.store()")
                size_info = dict(size_result.single()) if size_result.peek() else {}
            except:
                size_info = {"note": "Size info requires APOC plugin"}
        
        return {
            "nodes": dict(nodes),
            "relationships": dict(relationships),
            "totals": {
                "nodes": total_nodes,
                "relationships": total_relationships
            },
            "size_info": size_info,
            "timestamp": datetime.now().isoformat()
        }
    
    def print_database_stats(self):
        """Print formatted database statistics"""
        stats = self.get_database_stats()
        
        print("📊 Database Statistics")
        print("=" * 50)
        
        print(f"\n📈 Total: {stats['totals']['nodes']:,} nodes, {stats['totals']['relationships']:,} relationships")
        
        if stats['nodes']:
            print("\n🏷️ Node Types:")
            for label, count in stats['nodes'].items():
                print(f"  {label}: {count:,}")
        
        if stats['relationships']:
            print("\n🔗 Relationship Types:")
            for rel_type, count in stats['relationships'].items():
                print(f"  {rel_type}: {count:,}")
        
        print(f"\n⏰ Generated: {stats['timestamp']}")
    
    def is_database_empty(self) -> bool:
        """Check if database has any nodes"""
        with self.connection.get_session() as session:
            result = session.run("MATCH (n) RETURN count(n) as count LIMIT 1")
            return result.single()["count"] == 0
    
    def backup_database_metadata(self) -> Dict[str, any]:
        """Create a metadata backup for recovery purposes"""
        stats = self.get_database_stats()
        
        # Add schema info
        with self.connection.get_session() as session:
            # Get constraints
            try:
                constraints_result = session.run("SHOW CONSTRAINTS")
                constraints = [dict(record) for record in constraints_result]
            except:
                constraints = []
            
            # Get indexes
            try:
                indexes_result = session.run("SHOW INDEXES")
                indexes = [dict(record) for record in indexes_result]
            except:
                indexes = []
        
        backup = {
            "backup_timestamp": datetime.now().isoformat(),
            "statistics": stats,
            "constraints": constraints,
            "indexes": indexes
        }
        
        return backup
    
    def clear_database_by_label(self, labels: List[str], confirmation: bool = False) -> bool:
        """Clear specific node types"""
        if not confirmation:
            stats = self.get_database_stats()
            total_to_delete = sum(stats['nodes'].get(label, 0) for label in labels)
            
            print(f"⚠️ This will delete {total_to_delete:,} nodes of types: {', '.join(labels)}")
            response = input("Are you sure? (yes/no): ").lower().strip()
            if response != 'yes':
                print("❌ Operation cancelled")
                return False
        
        print(f"🗑️ Clearing node types: {', '.join(labels)}")
        
        try:
            stats = self.get_database_stats()
            
            for label in labels:
                label_count = stats['nodes'].get(label, 0)
                
                if label_count > 50000:
                    print(f"  Large label detected ({label_count:,} nodes) - using batched deletion...")
                    self._clear_label_batched(label)
                else:
                    with self.connection.get_session() as session:
                        query = f"MATCH (n:{label}) DETACH DELETE n"
                        session.run(query)
                    print(f"  ✓ Cleared {label} nodes")
            
            print("✅ Selective clearing completed")
            return True
            
        except Exception as e:
            print(f"❌ Error during clearing: {e}")
            return False
    
    def _clear_label_batched(self, label: str, batch_size: int = 10000) -> bool:
        """Clear a specific label in batches for memory safety"""
        total_deleted = 0
        
        try:
            with self.connection.get_session() as session:
                while True:
                    result = session.run(f"""
                        MATCH (n:{label})
                        WITH n LIMIT {batch_size}
                        DETACH DELETE n
                        RETURN count(n) as deleted
                    """)
                    
                    deleted = result.single()['deleted']
                    total_deleted += deleted
                    
                    if deleted > 0:
                        print(f"    Deleted {deleted:,} {label} nodes (total: {total_deleted:,})")
                    
                    if deleted == 0:
                        break
                    
                    # Small delay for very large deletions
                    if total_deleted % 50000 == 0:
                        time.sleep(0.1)
            
            print(f"  ✓ Cleared {total_deleted:,} {label} nodes")
            return True
            
        except Exception as e:
            print(f"  ❌ Error clearing {label}: {e}")
            return False
    
    def clear_database_by_date(self, node_label: str, date_property: str, 
                             before_date: str, confirmation: bool = False) -> bool:
        """Clear nodes older than specified date"""
        if not confirmation:
            with self.connection.get_session() as session:
                count_query = f"""
                MATCH (n:{node_label}) 
                WHERE n.{date_property} < datetime('{before_date}')
                RETURN count(n) as count
                """
                result = session.run(count_query)
                count = result.single()["count"]
            
            print(f"⚠️ This will delete {count:,} {node_label} nodes before {before_date}")
            response = input("Are you sure? (yes/no): ").lower().strip()
            if response != 'yes':
                print("❌ Operation cancelled")
                return False
        
        print(f"🗑️ Clearing {node_label} nodes before {before_date}")
        
        try:
            with self.connection.get_session() as session:
                delete_query = f"""
                MATCH (n:{node_label}) 
                WHERE n.{date_property} < datetime('{before_date}')
                DETACH DELETE n
                """
                session.run(delete_query)
            
            print("✅ Date-based clearing completed")
            return True
            
        except Exception as e:
            print(f"❌ Error during date-based clearing: {e}")
            return False
    
    def clear_database_safe(self, confirmation: bool = False) -> bool:
        """Safely clear entire database with confirmation and backup"""
        if self.is_database_empty():
            print("ℹ️ Database is already empty")
            return True
        
        # Get current stats
        stats = self.get_database_stats()
        total_nodes = stats['totals']['nodes']
        total_rels = stats['totals']['relationships']
        
        if not confirmation:
            print("⚠️ WARNING: This will delete ALL data in the database!")
            print(f"📊 Current database contains:")
            print(f"   • {total_nodes:,} nodes")
            print(f"   • {total_rels:,} relationships")
            
            # Show breakdown
            if stats['nodes']:
                print("\n🏷️ Node breakdown:")
                for label, count in stats['nodes'].items():
                    print(f"   • {label}: {count:,}")
            
            print(f"\n❗ This action cannot be undone!")
            try:
                response = input("Type 'DELETE ALL' to confirm: ").strip()
                if response != 'DELETE ALL':
                    print("❌ Operation cancelled")
                    return False
            except (EOFError, KeyboardInterrupt):
                print("\n❌ Operation cancelled")
                return False
        else:
            print("🗑️ Force mode: Skipping confirmation prompt")
        
        # Create backup metadata
        backup = self.backup_database_metadata()
        print(f"💾 Created metadata backup at {backup['backup_timestamp']}")
        
        # Always use the nuclear option for most reliable clearing
        print("🗑️ Using nuclear database clearing...")
        return self._clear_database_nuclear()
        
    def _clear_database_nuclear(self) -> bool:
        """Nuclear option: Most aggressive database clearing with multiple fallback strategies"""
        start_time = time.time()
        
        strategies = [
            ("DETACH DELETE (fast)", "MATCH (n) DETACH DELETE n"),
            ("Separate steps", ["MATCH ()-[r]-() DELETE r", "MATCH (n) DELETE n"]),
            ("Small batches", "batched")
        ]
        
        for strategy_name, strategy in strategies:
            print(f"🗑️ Attempting strategy: {strategy_name}")
            try:
                with self.connection.get_session() as session:
                    if strategy == "batched":
                        # Very small batches as last resort
                        total_deleted = 0
                        batch_size = 1000
                        
                        # Delete relationships first
                        while True:
                            result = session.run(f"MATCH ()-[r]-() WITH r LIMIT {batch_size} DELETE r RETURN count(r) as deleted")
                            deleted = result.single()['deleted']
                            if deleted == 0:
                                break
                            print(f"   Deleted {deleted} relationships")
                            time.sleep(0.1)
                        
                        # Delete nodes
                        while True:
                            result = session.run(f"MATCH (n) WITH n LIMIT {batch_size} DELETE n RETURN count(n) as deleted")
                            deleted = result.single()['deleted']
                            total_deleted += deleted
                            if deleted == 0:
                                break
                            print(f"   Deleted {deleted} nodes (total: {total_deleted})")
                            time.sleep(0.1)
                            
                    elif isinstance(strategy, list):
                        # Multiple commands
                        for cmd in strategy:
                            result = session.run(cmd)
                            print(f"   Executed: {cmd}")
                    else:
                        # Single command
                        result = session.run(strategy)
                        print(f"   Executed: {strategy}")
                
                # Verify success
                with self.connection.get_session() as session:
                    result = session.run("MATCH (n) RETURN count(n) as nodes")
                    remaining = result.single()['nodes']
                    
                    if remaining == 0:
                        duration = time.time() - start_time
                        print(f"✅ Database cleared successfully with {strategy_name} in {duration:.2f} seconds")
                        return True
                    else:
                        print(f"⚠️ {remaining} nodes still remain after {strategy_name}")
                        
            except Exception as e:
                print(f"❌ Strategy {strategy_name} failed: {e}")
                continue
        
        print("❌ All clearing strategies failed")
        return False
    
    def _clear_database_simple(self) -> bool:
        """Simple but comprehensive database clearing"""
        start_time = time.time()
        
        try:
            with self.connection.get_session() as session:
                # Step 1: Delete all relationships first
                print("🗑️ Step 1: Deleting all relationships...")
                result = session.run("MATCH ()-[r]-() DELETE r RETURN count(r) as deleted")
                rel_deleted = result.single()['deleted']
                print(f"   Deleted {rel_deleted:,} relationships")
                
                # Step 2: Delete all nodes
                print("🗑️ Step 2: Deleting all nodes...")
                result = session.run("MATCH (n) DELETE n RETURN count(n) as deleted")
                node_deleted = result.single()['deleted']
                print(f"   Deleted {node_deleted:,} nodes")
                
                # Step 3: Verify database is empty
                print("🗑️ Step 3: Verifying database is empty...")
                result = session.run("MATCH (n) RETURN count(n) as nodes")
                remaining_nodes = result.single()['nodes']
                
                result = session.run("MATCH ()-[r]-() RETURN count(r) as rels")
                remaining_rels = result.single()['rels']
                
                if remaining_nodes > 0 or remaining_rels > 0:
                    print(f"⚠️ WARNING: {remaining_nodes} nodes and {remaining_rels} relationships still remain")
                    # Force clear any remaining data
                    session.run("MATCH (n) DETACH DELETE n")
                    print("   Applied force deletion")
                    
                    # Final verification
                    result = session.run("MATCH (n) RETURN count(n) as nodes")
                    final_nodes = result.single()['nodes']
                    
                    result = session.run("MATCH ()-[r]-() RETURN count(r) as rels")
                    final_rels = result.single()['rels']
                    
                    if final_nodes > 0 or final_rels > 0:
                        print(f"❌ ERROR: Database still contains {final_nodes} nodes and {final_rels} relationships after forced clear")
                        return False
                    else:
                        print("   ✅ Force deletion successful")
            
            duration = time.time() - start_time
            print(f"✅ Database cleared successfully in {duration:.2f} seconds")
            return True
            
        except Exception as e:
            print(f"❌ Error during database clearing: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def _clear_database_batched(self, batch_size: int = 5000) -> bool:
        """Memory-safe batched database clearing for large datasets"""
        start_time = time.time()
        total_nodes_deleted = 0
        total_rels_deleted = 0
        
        try:
            with self.connection.get_session() as session:
                # Step 1: Delete all relationships in batches
                print("🗑️ Step 1: Deleting relationships in batches...")
                while True:
                    result = session.run(f"""
                        MATCH ()-[r]-()
                        WITH r LIMIT {batch_size}
                        DELETE r
                        RETURN count(r) as deleted
                    """)
                    deleted = result.single()['deleted']
                    total_rels_deleted += deleted
                    
                    if deleted > 0:
                        print(f"   Deleted relationship batch: {deleted:,} (total: {total_rels_deleted:,})")
                    else:
                        break
                
                # Step 2: Delete all nodes in batches with connection retry
                print("🗑️ Step 2: Deleting nodes in batches...")
                max_retries = 3
                while True:
                    retry_count = 0
                    deleted = 0
                    
                    while retry_count < max_retries:
                        try:
                            result = session.run(f"""
                                MATCH (n) 
                                WITH n LIMIT {batch_size}
                                DELETE n 
                                RETURN count(n) as deleted
                            """)
                            deleted = result.single()['deleted']
                            break  # Success, exit retry loop
                        except Exception as e:
                            retry_count += 1
                            if retry_count < max_retries:
                                print(f"   Retry {retry_count}/{max_retries-1} after error: {e}")
                                time.sleep(2)  # Wait before retry
                            else:
                                raise  # Re-raise if all retries failed
                    
                    total_nodes_deleted += deleted
                    
                    if deleted > 0:
                        print(f"   Deleted node batch: {deleted:,} (total: {total_nodes_deleted:,})")
                        # Small delay to prevent overwhelming the database
                        time.sleep(0.5)
                    else:
                        break
                
                # Step 3: Final verification and cleanup
                print("🗑️ Step 3: Final verification...")
                result = session.run("MATCH (n) RETURN count(n) as nodes")
                remaining_nodes = result.single()['nodes']
                
                result = session.run("MATCH ()-[r]-() RETURN count(r) as rels")
                remaining_rels = result.single()['rels']
                
                if remaining_nodes > 0 or remaining_rels > 0:
                    print(f"⚠️ WARNING: {remaining_nodes} nodes and {remaining_rels} relationships still remain")
                    print("   Applying final cleanup...")
                    session.run("MATCH (n) DETACH DELETE n")
                    
                    # Final verification
                    result = session.run("MATCH (n) RETURN count(n) as nodes")
                    final_nodes = result.single()['nodes']
                    
                    result = session.run("MATCH ()-[r]-() RETURN count(r) as rels")
                    final_rels = result.single()['rels']
                    
                    if final_nodes > 0 or final_rels > 0:
                        print(f"❌ ERROR: Database still contains {final_nodes} nodes and {final_rels} relationships after cleanup")
                        return False
                    else:
                        print("   ✅ Final cleanup successful")
            
            duration = time.time() - start_time
            print(f"✅ Database cleared successfully in {duration:.2f} seconds")
            print(f"📊 Total deleted: {total_nodes_deleted:,} nodes, {total_rels_deleted:,} relationships")
            return True
            
        except Exception as e:
            print(f"❌ Error during batched database clearing: {e}")
            print(f"⚠️ Partially cleared: {total_nodes_deleted:,} nodes, {total_rels_deleted:,} relationships")
            import traceback
            traceback.print_exc()
            return False
    
    def optimize_database(self) -> bool:
        """Optimize database performance after large operations"""
        print("⚡ Optimizing database...")
        
        try:
            with self.connection.get_session() as session:
                # Force garbage collection (if available)
                try:
                    session.run("CALL gds.debug.sysInfo() YIELD key, value WHERE key = 'vmMaxMemory' RETURN key, value")
                    print("  ✓ Memory information checked")
                except:
                    pass
                
                # Force index sampling (if available)
                try:
                    session.run("CALL db.resampleIndex('*')")
                    print("  ✓ Index sampling triggered")
                except:
                    pass
            
            print("✅ Database optimization completed")
            return True
            
        except Exception as e:
            print(f"❌ Error during optimization: {e}")
            return False
    
    def validate_data_consistency(self) -> Dict[str, any]:
        """Validate data consistency and integrity"""
        print("🔍 Validating data consistency...")
        
        issues = []
        
        with self.connection.get_session() as session:
            # Check for nodes without required properties
            checks = [
                ("Person nodes without es_id", "MATCH (p:Person) WHERE p.es_id IS NULL RETURN count(p) as count"),
                ("Organization nodes without es_id", "MATCH (o:Organization) WHERE o.es_id IS NULL RETURN count(o) as count"),
                ("Publication nodes without es_id", "MATCH (p:Publication) WHERE p.es_id IS NULL RETURN count(p) as count"),
                ("Project nodes without es_id", "MATCH (p:Project) WHERE p.es_id IS NULL RETURN count(p) as count"),
                ("Serial nodes without es_id", "MATCH (s:Serial) WHERE s.es_id IS NULL RETURN count(s) as count"),
                
                # Check for orphaned relationships
                ("AUTHORED relationships to missing persons", """
                    MATCH ()-[r:AUTHORED]->()
                    WHERE NOT EXISTS {
                        MATCH (p:Person)-[r]->()
                    }
                    RETURN count(r) as count
                """),
                ("INVOLVED_IN relationships to missing projects", """
                    MATCH ()-[r:INVOLVED_IN]->()
                    WHERE NOT EXISTS {
                        MATCH ()-[r]->(pr:Project)
                    }
                    RETURN count(r) as count
                """),
            ]
            
            for check_name, query in checks:
                try:
                    result = session.run(query)
                    count = result.single()["count"]
                    if count > 0:
                        issues.append({"check": check_name, "count": count})
                        print(f"  ⚠️ {check_name}: {count}")
                    else:
                        print(f"  ✓ {check_name}: OK")
                except Exception as e:
                    issues.append({"check": check_name, "error": str(e)})
                    print(f"  ❌ {check_name}: {e}")
        
        if not issues:
            print("✅ Data consistency validation passed")
        else:
            print(f"⚠️ Found {len(issues)} consistency issues")
        
        return {
            "timestamp": datetime.now().isoformat(),
            "issues": issues,
            "status": "clean" if not issues else "issues_found"
        }
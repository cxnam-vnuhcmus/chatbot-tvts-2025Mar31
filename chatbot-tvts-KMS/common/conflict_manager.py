from typing import List, Dict, Optional
from datetime import datetime
import json
import logging
from openai import OpenAI
import os
from dotenv import load_dotenv
from common.chroma_manager import ChromaManager
from common.models import ConflictResult
from common.data_manager import DatabaseManager
import traceback
from common.openai_conflict_analyzer import OpenAIConflictAnalyzer

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ConflictAnalyzer:
    def __init__(self, api_key: str):
        self.cache = {}
        self.cache_hits = 0
        self.cache_misses = 0
        self.timeout = 30
        self.max_retries = 3
        self.retry_delay = 5
        self.analyzed_pairs = set()
        
        try:
            self.analyzer = OpenAIConflictAnalyzer(api_key=api_key, use_cache=True)
            logger.info("OpenAI Initialization for Conflict Analysis")
        except Exception as e:
            logger.error(f"OpenAI initialization error: {str(e)}")
            logger.warning("Switching to non-conflict analysis mode")
            self.analyzer = None
    
    def create_system_message(self, conflict_type="any"):
        pass
    
    async def check_conflict_async(self, content1: str, content2: str, conflict_type: str = "internal") -> ConflictResult:
        """
        Check for conflicts between two asynchronous contents using OpenAI

        Args:
        content1: First content
        content2: Second content
        conflict_type: Conflict type

        Returns:
        ConflictResult: Conflict analysis result
        """
        try:
            result = await self.analyzer.analyze_conflict_async(content1, content2, conflict_type)
            return result
        except Exception as e:
            logger.error(f"Error parsing asynchronous conflict: {str(e)}")
            return ConflictResult(
                has_conflict=False,
                explanation=f"Lỗi phân tích OpenAI: {str(e)}",
                conflicting_parts=[],
                analyzed_at=datetime.now(),
                chunk_ids=[],
                conflict_type=conflict_type
            )
            
    def analyze_content(self, content: str) -> ConflictResult:
        """Analyzing internal conflicts in a text using OpenAI"""
        try:
            if not content:
                return ConflictResult(
                    has_conflict=False,
                    explanation="Không có nội dung để phân tích",
                    conflicting_parts=[],
                    analyzed_at=datetime.now(),
                    chunk_ids=[],
                    conflict_type="content"
                )

            cache_key = f"content_{hash(content)}"
            if cache_key in self.cache:
                self.cache_hits += 1
                cached_result = self.cache[cache_key]
                return cached_result

            self.cache_misses += 1

            result = self.analyzer.analyze_conflict(content, conflict_type="content")
            
            self.cache[cache_key] = result
            
            return result

        except Exception as e:
            logger.error(f"Error parsing content: {str(e)}")
            return ConflictResult(
                has_conflict=False,
                explanation=f"Lỗi phân tích: {str(e)}",
                conflicting_parts=[],
                analyzed_at=datetime.now(),
                chunk_ids=[],
                conflict_type="content"
            )
    
    def analyze_chunks(self, chunks: List[tuple[str, str]], conflict_type="internal") -> ConflictResult:
        """Analyzing Conflicts Between Chunks Using OpenAI
        
        Args:
            chunks: List of tuple pairs (chunk_id, content)
            conflict_type: Type of conflict analysis to perform ('internal', 'external', or others)
            
        Returns:
            ConflictResult: Analysis result object
        """
        try:
            valid_conflict_types = ['self', 'intra', 'inter', 'content', 'internal', 'external', 'manual']
            if not conflict_type or conflict_type not in valid_conflict_types:
                logger.warning(f"Invalid conflict_type '{conflict_type}', defaulting to 'internal'")
                conflict_type = 'internal' 
                
            if not chunks or len(chunks) < 2:
                return ConflictResult(
                    has_conflict=False,
                    explanation="Cần ít nhất hai chunks để phân tích mâu thuẫn",
                    conflicting_parts=[],
                    analyzed_at=datetime.now(),
                    chunk_ids=[c[0] for c in chunks] if chunks else [],
                    conflict_type=conflict_type
                )

            if len(chunks) == 2:
                sorted_ids = sorted([chunks[0][0], chunks[1][0]])
                pair_key = f"{conflict_type}_{sorted_ids[0]}_{sorted_ids[1]}"
                
                if pair_key in self.cache:
                    self.cache_hits += 1
                    cached_result = self.cache[pair_key]
                    if not cached_result.conflict_type:
                        cached_result.conflict_type = conflict_type
                    return cached_result

            self.cache_misses += 1

            if len(chunks) == 2:
                result = self.analyzer.analyze_conflict(
                    chunks[0][1], 
                    chunks[1][1], 
                    conflict_type=conflict_type
                )
                
                result.chunk_ids = [c[0] for c in chunks]
                result.conflict_type = conflict_type
                
                if len(chunks) == 2:
                    self.cache[pair_key] = result
                    
                return result
            else:
                all_conflicts = []
                for i in range(len(chunks) - 1):
                    for j in range(i + 1, len(chunks)):
                        chunk_pair = [chunks[i], chunks[j]]
                        conflict = self.analyze_chunks(chunk_pair, conflict_type)
                        if conflict.has_conflict:
                            all_conflicts.append(conflict)
                
                if all_conflicts:
                    return ConflictResult(
                        has_conflict=True,
                        explanation="Phát hiện mâu thuẫn giữa các chunks",
                        conflicting_parts=[part for conflict in all_conflicts for part in conflict.conflicting_parts],
                        analyzed_at=datetime.now(),
                        chunk_ids=[c[0] for c in chunks],
                        conflict_type=conflict_type  
                    )
                else:
                    return ConflictResult(
                        has_conflict=False,
                        explanation="Không phát hiện mâu thuẫn giữa các chunks",
                        conflicting_parts=[],
                        analyzed_at=datetime.now(),
                        chunk_ids=[c[0] for c in chunks],
                        conflict_type=conflict_type 
                    )

        except Exception as e:
            logger.error(f"Error parsing chunks: {str(e)}")
            return ConflictResult(
                has_conflict=False,
                explanation=f"Lỗi phân tích: {str(e)}",
                conflicting_parts=[],
                analyzed_at=datetime.now(),
                chunk_ids=[c[0] for c in chunks] if chunks else [],
                conflict_type=conflict_type  
            )
    
class ConflictManager:
    def __init__(self, db_manager: DatabaseManager, chroma_manager: ChromaManager):
        self.db = db_manager
        self.chroma_manager = chroma_manager
        self.analyzer = ConflictAnalyzer(os.getenv('OPENAI_API_KEY'))
        self.analyzed_pairs = set()
        self.chunk_cache = {}
        self.conflict_cache = {}
        self.cache_expiry = 3600

    def _get_cached_conflict(self, chunk1_id: str, chunk2_id: str, conflict_type: str = "any") -> Optional[Dict]:
        """
        Check if chunk pair has been parsed

        Args:
        chunk1_id: ID of first chunk
        chunk2_id: ID of second chunk
        conflict_type: Type of conflict to check

        Returns:
        Optional[Dict]: Cached conflict result if any
        """
        sorted_ids = sorted([chunk1_id, chunk2_id])
        cache_key = f"{conflict_type}_{sorted_ids[0]}_{sorted_ids[1]}"
        
        if cache_key in self.conflict_cache:
            result, timestamp = self.conflict_cache[cache_key]
            if (datetime.now() - timestamp).total_seconds() < self.cache_expiry:
                logger.info(f"Found cached result for chunk pair {cache_key}")
                return result
        
        try:
            query = """
                SELECT explanation, conflicting_parts, detected_at, conflict_type, has_conflict 
                FROM chunk_conflicts 
                WHERE chunk_ids @> ARRAY[%s, %s]::text[] 
                AND conflict_type = %s
                AND resolved = FALSE
                ORDER BY detected_at DESC
                LIMIT 1
            """
            
            cached = self.db.execute_with_retry(
                query,
                (sorted_ids[0], sorted_ids[1], conflict_type),
                fetch=True
            )
            
            if cached and len(cached) > 0:
                logger.info(f"Found DB cached result for chunk pair {cache_key}")
                result = {
                    "has_conflict": cached[0][4] if len(cached[0]) > 4 else True,
                    "explanation": cached[0][0],
                    "conflicting_parts": cached[0][1] or [],
                    "analyzed_at": cached[0][2].isoformat() if cached[0][2] else datetime.now().isoformat(),
                    "chunk_ids": sorted_ids,
                    "conflict_type": cached[0][3]
                }
                
                self.conflict_cache[cache_key] = (result, datetime.now())
                return result
                
        except Exception as e:
            logger.error(f"Error checking DB cache: {str(e)}")
        
        return None
    
    def _cache_key(self, chunk_ids, conflict_type="any"):
        
        sorted_ids = sorted(chunk_ids)
        return f"{conflict_type}_{'-'.join(sorted_ids)}"

    def _get_from_cache(self, key):
        if key in self.conflict_cache:
            result, timestamp = self.conflict_cache[key]
            if (datetime.now() - timestamp).total_seconds() < self.cache_expiry:
                return result
        return None

    def _get_cached_cross_doc_conflict(self, chunk1_id: str, chunk2_id: str, doc1_id: str, doc2_id: str) -> Optional[ConflictResult]:
        """Get conflicting results between documents from cache"""
        try:
            query = """
                SELECT explanation, conflicting_parts, detected_at
                FROM chunk_conflicts 
                WHERE chunk_ids @> ARRAY[%s]::text[] AND chunk_ids @> ARRAY[%s]::text[]
                AND doc_id = %s
                ORDER BY detected_at DESC
                LIMIT 1
            """
            
            cached = self.db.execute_with_retry(
                query,
                (chunk1_id, chunk2_id, doc1_id),
                fetch=True
            )
            
            if cached:
                return ConflictResult(
                    has_conflict=True,  
                    explanation=cached[0][0],
                    conflicting_parts=cached[0][1],
                    analyzed_at=cached[0][2],
                    chunk_ids=[chunk1_id, chunk2_id]
                )
            return None
            
        except Exception as e:
            logger.error(f"Error checking cache: {str(e)}")
            logger.error(traceback.format_exc())
            return None
    
    def _update_cache(self, key, result):
        self.conflict_cache[key] = (result, datetime.now())

    def _is_cache_valid(self, doc_id):
        document = self.db.get_document_by_id(doc_id)
        if not document:
            return False
            
        last_check = document.get('last_conflict_check')
        if not last_check:
            return False
            
        modified_date = document.get('modified_date')
        
        if isinstance(last_check, str):
            try:
                last_check = datetime.fromisoformat(last_check)
            except ValueError:
                logger.warning(f"Could not parse date string: {last_check}")
                return False
        elif isinstance(last_check, datetime):
            pass
        else:
            return False
            
        if not modified_date:
            return True
            
        if isinstance(modified_date, str):
            try:
                modified_date = datetime.fromisoformat(modified_date)
            except ValueError:
                logger.warning(f"Could not parse date string: {modified_date}")
                return False
        
        return last_check > modified_date

    def analyze_conflicts(self, doc_id):
        """
        Analyze conflict types for a document

        Args:
        doc_id (str): ID of the document to analyze

        Returns:
        dict: Analysis results with conflict types
        """
        start_time = datetime.now()
        try:
            logger.info(f"Starting conflict analysis for document {doc_id}")
            
            document = self.db.get_document_by_id(doc_id)
            if not document:
                logger.error(f"Document {doc_id} not found")
                return {'has_conflicts': False}
                
            chunks = self._get_document_chunks(doc_id)
            if not chunks:
                logger.info(f"No chunk found for document {doc_id}")
                return {'has_conflicts': False}
                
            for chunk in chunks:
                if 'metadata' not in chunk:
                    chunk['metadata'] = {}
                chunk['metadata']['doc_id'] = doc_id
                
            logger.info(f"Found {len(chunks)} chunks for document {doc_id}")
            
            content_conflicts = self.analyze_content_conflicts(doc_id, chunks)
            
            internal_conflicts = self.analyze_internal_conflicts(doc_id, chunks)
            
            external_conflicts = self.analyze_external_conflicts(doc_id, chunks)
            
            all_conflicts = content_conflicts + internal_conflicts + external_conflicts
            
            result = {
                'has_conflicts': len(all_conflicts) > 0,
                'total_conflicts': len(all_conflicts),
                'content_conflicts': len(content_conflicts),
                'internal_conflicts': len(internal_conflicts),
                'external_conflicts': len(external_conflicts),
                'conflicts': all_conflicts
            }
            
            self._store_conflict_info(
                doc_id, 
                result['has_conflicts'],
                'Pending Review' if result['has_conflicts'] else 'No Conflict',
                len(all_conflicts),
                all_conflicts
            )
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            logger.info(f"Completed conflict analysis for document {doc_id} in {duration} seconds")
            return result
            
        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            logger.error(f"Contradictory parse error for document {doc_id}: {str(e)}")
            logger.error(traceback.format_exc())
            
            try:
                self.db.update_document_status(doc_id, {
                    'conflict_analysis_status': 'AnalysisFailed',
                    'conflict_status': 'NotAnalyzed'
                })
            except Exception as update_error:
                logger.error(f"Error updating status after error: {str(update_error)}")
            
            logger.info(f"Completed conflict analysis for document {doc_id} in {duration} seconds")
            return {'has_conflicts': False, 'error': str(e)}
    
    def _get_document_chunks(self, doc_id):
        """
        Get document chunks

        Args:
        doc_id (str): Document ID

        Returns:
        list: List of document chunks
        """
        if not hasattr(self, 'chroma_manager') or self.chroma_manager is None:
            logger.warning(f"ChromaManager not available for document {doc_id}")
            return []
            
        try:
            chunks = self.chroma_manager.get_chunks_by_document_id(doc_id)
            if not chunks:
                logger.warning(f"No chunk found for document {doc_id}")
                return []
                
            for chunk in chunks:
                if 'metadata' not in chunk:
                    chunk['metadata'] = {}
                chunk['metadata']['doc_id'] = doc_id
                
            return chunks
        except Exception as e:
            logger.error(f"Error getting chunks for document {doc_id}: {str(e)}")
            return []
    
    def _get_all_documents_except(self, doc_id):
        """
        Get all documents except the one with the given id

        Args:
        doc_id (str): ID of the document to exclude

        Returns:
        list: List of other documents
        """
        try:
            query = """
                SELECT id, document_topic, unit, sender, created_date 
                FROM documents 
                WHERE id != %s AND chunk_status = 'Chunked'
                ORDER BY created_date DESC
            """
            recent_docs = self.db.execute_with_retry(query, (doc_id,), fetch=True)
            
            if not recent_docs:
                return []
                
            result = []
            for row in recent_docs:
                result.append({
                    'id': row[0],
                    'document_topic': row[1] if len(row) > 1 else None,
                    'unit': row[2] if len(row) > 2 else None,
                    'sender': row[3] if len(row) > 3 else None,
                    'created_date': row[4] if len(row) > 4 else None
                })
                
            return result
        except Exception as e:
            logger.error(f"Error in _get_all_documents_except: {str(e)}")
            return []

    def _format_chunk_results(self, results, source_id):
        """
        Format chunk results from Chroma for display and proper handling of disabled status
        
        Args:
            results: Raw results from Chroma query
            source_id: ID of the source document
        
        Returns:
            list: List of formatted chunk objects with proper metadata
        """
        formatted_chunks = []
        try:
            for chunk_id, content, metadata in zip(
                results['ids'],
                results['documents'],
                results['metadatas']
            ):
                is_enabled = metadata.get('is_enabled', True)
                if isinstance(is_enabled, str):
                    is_enabled = is_enabled.lower() == 'true'
                    
                updated_metadata = dict(metadata)
                updated_metadata['is_enabled'] = bool(is_enabled)
                updated_metadata['doc_id'] = source_id
                
                chunk = {
                    'id': chunk_id,
                    'document_topic': metadata.get('document_topic', ''),
                    'chunk_topic': metadata.get('chunk_topic', ''),
                    'paragraph': metadata.get('paragraph', ''),
                    'original_text': metadata.get('original_text', ''), 
                    'qa_content': content,
                    'metadata': updated_metadata,
                    'unit': metadata.get('unit', ''),
                    'source_document': source_id
                }
                formatted_chunks.append(chunk)
        except Exception as format_error:
            logger.error(f"Error formatting chunks result: {str(format_error)}")
            logger.error(traceback.format_exc())
        
        return formatted_chunks
    
    def analyze_content_conflicts(self, chunks: List[Dict]) -> List[ConflictResult]:
        """
        Analyze internal conflicts within individual chunks.
        Skips disabled chunks.

        Args:
        chunks: List of chunks to analyze

        Returns:
        List[ConflictResult]: List of conflicts found
        """
        results = []
        
        doc_id = None
        if chunks and len(chunks) > 0 and 'id' in chunks[0]:
            chunk_id = chunks[0]['id']
            if '_paragraph_' in chunk_id:
                doc_id = chunk_id.split('_paragraph_')[0]
        
        logger.info(f"Starting content conflict analysis for {len(chunks)} chunks, doc_id: {doc_id}")
        
        enabled_chunks = []
        disabled_chunks = []
        for chunk in chunks:
            is_enabled = True
            if 'metadata' in chunk:
                is_enabled = chunk['metadata'].get('is_enabled', True)
                # Handle string values like "true"/"false"
                if isinstance(is_enabled, str):
                    is_enabled = is_enabled.lower() == 'true'
            
            if is_enabled:
                enabled_chunks.append(chunk)
            else:
                disabled_chunks.append(chunk)
                logger.info(f"Skipping disabled chunk {chunk.get('id')} during content conflict analysis")
        
        logger.info(f"Using {len(enabled_chunks)} enabled chunks out of {len(chunks)} total chunks")
        
        for index, chunk in enumerate(enabled_chunks):
            try:
                if not chunk.get('original_text'):
                    logger.info(f"Skipping chunk {chunk.get('id')} with no original text")
                    continue
                    
                chunk_id = chunk.get('id')
                if not chunk_id:
                    logger.info(f"Skipping chunk at index {index} with no ID")
                    continue
                    
                # Check cache for existing analysis
                cache_key = f"content_{chunk_id}"
                cached_result = self._get_from_cache(cache_key)
                
                if cached_result:
                    logger.info(f"Found cache result for chunk {chunk_id}")
                    
                    if not hasattr(cached_result, 'has_conflict'):
                        logger.warning(f"Invalid cache result for chunk {chunk_id}")
                        cached_result = None
                    
                    if cached_result and cached_result.has_conflict:
                        logger.info(f"Detected cached conflict in chunk {chunk_id}")
                        cached_result.chunk_ids = [chunk_id]
                        cached_result.conflict_type = "content"
                        results.append(cached_result)
                        
                        # Record to database if it's not already there
                        if doc_id:
                            try:
                                self.db.store_chunk_conflict(
                                    doc_id,
                                    [chunk_id],
                                    cached_result.explanation,
                                    cached_result.conflicting_parts,
                                    'content'
                                )
                            except Exception as store_error:
                                logger.error(f"Error saving cached conflict to DB: {str(store_error)}")
                    
                    if cached_result:
                        continue
                
                logger.info(f"Analyzing content conflict for chunk {chunk_id}")
                original_text = chunk.get('original_text', '')
                
                if hasattr(self, 'analyzer') and self.analyzer:
                    result = self.analyzer.analyze_content(original_text)
                    
                    result.chunk_ids = [chunk_id]
                    result.conflict_type = "content"
                    
                    self._update_cache(cache_key, result)
                    
                    if result.has_conflict:
                        logger.info(f"Detected new conflict in chunk content {chunk_id}")
                        results.append(result)
                        
                        if doc_id:
                            try:
                                self.db.store_chunk_conflict(
                                    doc_id,
                                    [chunk_id],
                                    result.explanation,
                                    result.conflicting_parts,
                                    'content'
                                )
                                logger.info(f"Saved new conflict to DB for chunk {chunk_id}")
                            except Exception as store_error:
                                logger.error(f"Error saving new conflict to DB: {str(store_error)}")
                else:
                    logger.warning(f"No analyzer found to analyze chunk {chunk_id}")
                        
            except Exception as e:
                error_message = str(e)
                error_message = error_message.replace('%', '%%')
                logger.error(f"Error analyzing content for chunk {index}: {error_message}")
                logger.error(traceback.format_exc())
        
        if doc_id and disabled_chunks:
            try:
                disabled_chunk_ids = [chunk.get('id') for chunk in disabled_chunks if chunk.get('id')]
                if disabled_chunk_ids:
                    for disabled_id in disabled_chunk_ids:
                        try:
                            resolution_note = f"Chunk {disabled_id} was disabled"
                            resolve_query = """
                                UPDATE chunk_conflicts
                                SET 
                                    resolved = TRUE,
                                    resolved_at = CURRENT_TIMESTAMP,
                                    resolution_notes = %s,
                                    updated_at = CURRENT_TIMESTAMP  
                                WHERE chunk_ids @> ARRAY[%s]::varchar[] AND conflict_type = 'content' AND resolved = FALSE
                                RETURNING conflict_id
                            """
                            resolved = self.db.execute_with_retry(
                                resolve_query, 
                                (resolution_note, disabled_id),
                                fetch=True
                            )
                            
                            if resolved:
                                logger.info(f"Automatically resolved {len(resolved)} conflicts for disabled chunk {disabled_id}")
                        except Exception as resolve_error:
                            logger.error(f"Error resolving conflicts for disabled chunk {disabled_id}: {str(resolve_error)}")
            except Exception as disabled_error:
                logger.error(f"Error handling disabled chunks conflicts: {str(disabled_error)}")
        
        logger.info(f"Content conflict analysis complete: {len(results)} conflicts found")
        return results

    def handle_conflicts(self, chunk_id: str, db_manager=None):
        """
        Handle conflicts related to a specific chunk
        
        Args:
            chunk_id (str): ID of the chunk to check
            db_manager (DatabaseManager, optional): Database manager instance
            
        Returns:
            dict: Information about processed conflicts
        """
        try:
            if not db_manager:
                from common.data_manager import DatabaseManager
                db_manager = DatabaseManager()
                
            if not chunk_id:
                return {
                    "status": "error",
                    "message": "No chunk ID provided"
                }
                
            query = """
                SELECT c.id, c.conflict_id, c.doc_id, c.chunk_ids, 
                    c.conflict_type, c.explanation, c.resolved
                FROM chunk_conflicts c
                WHERE c.resolved = false
                    AND chunk_ids @> %s::text[]
            """
            
            results = db_manager.execute_with_retry(query, ([chunk_id],), fetch=True)
            
            if not results:
                return {
                    "status": "success", 
                    "message": "No conflicts found",
                    "conflicts": []
                }
                
            conflicts = []
            for result in results:
                conflict_id = result[1]
                doc_id = result[2]
                chunk_ids = result[3]
                conflict_type = result[4]
                explanation = result[5]
                
                conflicts.append({
                    "conflict_id": conflict_id,
                    "doc_id": doc_id,
                    "chunk_ids": chunk_ids,
                    "conflict_type": conflict_type,
                    "explanation": explanation
                })
                
            return {
                "status": "success",
                "message": f"Found {len(conflicts)} conflicts for chunk {chunk_id}",
                "conflicts": conflicts
            }
            
        except Exception as e:
            logger.error(f"Error handling conflicts for chunk {chunk_id}: {str(e)}")
            logger.error(traceback.format_exc())
            return {
                "status": "error",
                "message": f"Error processing conflicts: {str(e)}"
            }
        
    def _format_conflict_results(self, conflict_info: Dict) -> Dict:
        """
        Reformat conflict analysis results for display on the user interface

        Args:
        conflict_info: Conflict information

        Returns:
        Dict: Formatted conflict information
        """
        try:
            if not conflict_info:
                return {
                    "content_conflicts": [],
                    "internal_conflicts": [],
                    "external_conflicts": []
                }
                
            content_conflicts = conflict_info.get('content_conflicts', [])
            internal_conflicts = conflict_info.get('internal_conflicts', [])
            external_conflicts = conflict_info.get('external_conflicts', [])
            
            formatted_content = []
            for conflict in content_conflicts:
                if isinstance(conflict, dict):
                    if 'explanation' not in conflict:
                        continue
                        
                    if 'chunk_id' not in conflict and 'chunk_ids' in conflict:
                        if conflict['chunk_ids'] and len(conflict['chunk_ids']) > 0:
                            conflict['chunk_id'] = conflict['chunk_ids'][0]
                            
                    if 'conflicting_parts' not in conflict:
                        conflict['conflicting_parts'] = []
                        
                    if 'analyzed_at' not in conflict:
                        conflict['analyzed_at'] = datetime.now().isoformat()
                        
                    if 'has_conflict' not in conflict:
                        conflict['has_conflict'] = True
                        
                    formatted_content.append(conflict)
                    
            return {
                "content_conflicts": formatted_content,
                "internal_conflicts": internal_conflicts,
                "external_conflicts": external_conflicts
            }
            
        except Exception as e:
            logger.error(f"Contradictory result format error: {str(e)}")
            return {
                "content_conflicts": [],
                "internal_conflicts": [],
                "external_conflicts": []
            }

    def analyze_internal_conflicts(self, chunks: List[Dict]) -> List[ConflictResult]:
        """
        Analyze conflicts between chunks in the same document.
        Analyze all possible chunk pairs to ensure no conflicts are missed.
        Skips disabled chunks.

        Args:
        chunks: List of chunks to analyze

        Returns:
        List[ConflictResult]: List of conflicts found
        """
        try:
            if len(chunks) < 2:
                return []
                
            doc_id = chunks[0].get('id', '').split('_paragraph_')[0]
            logger.info(f"Internal conflict analysis for document {doc_id} with {len(chunks)} chunks")
            
            # Filter enabled chunks
            enabled_chunks = []
            for chunk in chunks:
                metadata = chunk.get('metadata', {})
                is_enabled = metadata.get('is_enabled', True)
                if is_enabled:
                    enabled_chunks.append(chunk)
                else:
                    logger.info(f"Skipping disabled chunk {chunk.get('id')} during internal conflict analysis")
            
            logger.info(f"Using {len(enabled_chunks)} enabled chunks out of {len(chunks)} total chunks")
            
            if len(enabled_chunks) < 2:
                logger.info(f"Not enough enabled chunks for internal conflict analysis (need at least 2)")
                return []
            
            results = []
            
            for i in range(len(enabled_chunks)):
                for j in range(i+1, len(enabled_chunks)):
                    chunk1 = enabled_chunks[i]
                    chunk2 = enabled_chunks[j]
                    
                    if not chunk1.get('original_text') or not chunk2.get('original_text'):
                        logger.warning(f"Chunk missing content: {chunk1.get('id')} or {chunk2.get('id')}")
                        continue
                        
                    sorted_ids = sorted([chunk1['id'], chunk2['id']])
                    cache_key = f"internal_{sorted_ids[0]}_{sorted_ids[1]}"
                    
                    cached_result = self._get_from_cache(cache_key)
                    if cached_result:
                        if cached_result.has_conflict:
                            logger.info(f"Detected conflict from cache: {cache_key}")
                            results.append(cached_result)
                        continue
                    
                
                    logger.info(f"Analyze conflict between {chunk1['id']} and {chunk2['id']}")
                    result = self.analyzer.analyze_chunks([
                        (chunk1['id'], chunk1['original_text']),
                        (chunk2['id'], chunk2['original_text'])
                    ], conflict_type="internal")
                    
                    result.conflict_type = "internal"
                    
                    self._update_cache(cache_key, result)
                    
                    if result.has_conflict:
                        logger.info(f"Detected a conflict between {chunk1['id']} and {chunk2['id']}")
                        results.append(result)
                        
            logger.info(f"Detected {len(results)} internal conflicts in document {doc_id}")
            return results
                
        except Exception as e:
            logger.error(f"Internal conflict parse error: {str(e)}")
            logger.error(traceback.format_exc())
            return []
    
    def analyze_external_conflicts(self, doc_id, chunks):
        """
        Analyze conflicts between this document and other documents.
        Optimized to avoid duplicate analysis and track processed pairs.
        
        Args:
            doc_id: ID of the document being analyzed
            chunks: List of chunks from this document
            
        Returns:
            list: List of detected conflicts formatted as dictionaries
        """
        if not chunks:
            return []
        
        logger.info(f"Starting conflict analysis with other docs for document {doc_id}")
        conflicts = []
        
        enabled_chunks = []
        for chunk in chunks:
            metadata = chunk.get('metadata', {})
            is_enabled = metadata.get('is_enabled', True)
            if isinstance(is_enabled, str):
                is_enabled = is_enabled.lower() == 'true'
            if is_enabled:
                enabled_chunks.append(chunk)
            else:
                logger.info(f"Skipping disabled chunk {chunk.get('id')} during external conflict analysis")
        
        logger.info(f"Using {len(enabled_chunks)} enabled chunks out of {len(chunks)} total chunks")
        
        if not enabled_chunks:
            logger.info(f"No enabled chunks to analyze for external conflicts")
            return []
        
        processed_doc_pairs = set()
        processed_chunk_pairs = set()
        
        try:
            document = self.db.get_document_by_id(doc_id)
            if not document:
                logger.warning(f"Document {doc_id} not found")
                return []
                
            duplicate_group_id = document.get('duplicate_group_id')
            related_docs = []
            
            if duplicate_group_id:
                group_docs = self.db.get_documents_in_group(duplicate_group_id)
                for group_doc in group_docs:
                    if group_doc['id'] != doc_id:
                        related_docs.append(group_doc)
                logger.info(f"Found {len(related_docs)} related documents in the same group")
            
            
            if not related_docs:
                other_docs = self._get_all_documents_except(doc_id)
                
                max_docs_to_check = 10 
                related_docs = other_docs[:max_docs_to_check]
                
                logger.info(f"No group found. Using {len(related_docs)} other documents for comparison")
            
            for related_doc in related_docs:
                related_doc_id = related_doc['id']
                
                doc_pair = tuple(sorted([doc_id, related_doc_id]))
                if doc_pair in processed_doc_pairs:
                    logger.info(f"Skipping already processed document pair: {doc_id} and {related_doc_id}")
                    continue
                    
                processed_doc_pairs.add(doc_pair)
                
                try:
                    related_chunks = self._get_document_chunks(related_doc_id)
                    if not related_chunks:
                        logger.info(f"No chunk found for document {related_doc_id}")
                        continue
                        
                    enabled_related_chunks = []
                    for chunk in related_chunks:
                        metadata = chunk.get('metadata', {})
                        is_enabled = metadata.get('is_enabled', True)
                        if isinstance(is_enabled, str):
                            is_enabled = is_enabled.lower() == 'true'
                        if is_enabled:
                            if 'metadata' not in chunk:
                                chunk['metadata'] = {}
                            chunk['metadata']['doc_id'] = related_doc_id
                            enabled_related_chunks.append(chunk)
                        else:
                            logger.info(f"Skipping disabled chunk {chunk.get('id')} from related document {related_doc_id}")
                    
                    if not enabled_related_chunks:
                        logger.info(f"No enabled chunks found for related document {related_doc_id}")
                        continue
                        
                    logger.info(f"Compare document {doc_id} ({len(enabled_chunks)} chunks) with document {related_doc_id} ({len(enabled_related_chunks)} chunks)")
                    
                    chunk_pairs_to_analyze = []
                    
                    for chunk1 in enabled_chunks:
                        chunk1_id = chunk1['id']
                        
                        for chunk2 in enabled_related_chunks:
                            chunk2_id = chunk2['id']
                            
                            pair_key = '_'.join(sorted([chunk1_id, chunk2_id]))
                            
                            if pair_key in processed_chunk_pairs:
                                continue
                                
                            processed_chunk_pairs.add(pair_key)
                            chunk_pairs_to_analyze.append((chunk1, chunk2))
                    
                    logger.info(f"Analyzing {len(chunk_pairs_to_analyze)} unique chunk pairs between documents {doc_id} and {related_doc_id}")
                    
                    for chunk1, chunk2 in chunk_pairs_to_analyze:
                        chunk1_id = chunk1['id']
                        chunk2_id = chunk2['id']
                        logger.info(f"Analyze external conflicts between {chunk1_id} and {chunk2_id}")
                        
                        result = self.analyzer.analyze_chunks([
                            (chunk1_id, chunk1.get('original_text', '')),
                            (chunk2_id, chunk2.get('original_text', ''))
                        ], conflict_type="external")
                        
                        result.conflict_type = "external"
                        
                        if result.has_conflict:
                            logger.info(f"External conflict detected between {chunk1_id} and {chunk2_id}")
                            conflict_data = result.to_dict()
                            
                            conflict_data['document_ids'] = [doc_id, related_doc_id]
                            
                            conflicts.append(conflict_data)
                            
                            try:
                                self.db.store_chunk_conflict(
                                    doc_id, 
                                    [chunk1_id, chunk2_id], 
                                    result.explanation, 
                                    result.conflicting_parts,
                                    'external'
                                )
                                
                                status_update = {
                                    'has_conflicts': True,
                                    'conflict_status': 'Pending Review'
                                }
                                
                                self.db.update_document_status(doc_id, status_update)
                                self.db.update_document_status(related_doc_id, status_update)
                                
                                logger.info(f"Conflict saved for both documents: {doc_id} and {related_doc_id}")
                                
                            except Exception as store_error:
                                logger.error(f"Error saving conflict: {str(store_error)}")
                                logger.error(traceback.format_exc())
                                    
                except Exception as e:
                    logger.error(f"Error while analyzing document {related_doc_id}: {str(e)}")
            
            logger.info(f"Detected {len(conflicts)} external conflicts for document {doc_id}")
            return conflicts
            
        except Exception as e:
            logger.error(f"Error in external conflict analysis: {str(e)}")
            logger.error(traceback.format_exc())
            return []
    
    def _store_conflict_info(self, doc_id, conflict_info):
        """
        Save conflict information to database

        Args:
        doc_id (str): ID of document

        conflict_info (dict): Conflict information

        Returns:
        bool: True on success, False on failure

        """
        try:
            if isinstance(conflict_info, str):
                try:
                    conflict_info = json.loads(conflict_info)
                except json.JSONDecodeError:
                    logger.error(f"Conflict information is not valid JSON: {conflict_info}")
                    conflict_info = {"has_conflicts": False}
            
            has_conflicts = conflict_info.get('has_conflicts', False)
        
            if not isinstance(has_conflicts, bool):
                has_conflicts = bool(has_conflicts)
                
            logger.info(f"Update conflicting information for document {doc_id}: has_conflicts={has_conflicts}")
            
            update_data = {
                'has_conflicts': has_conflicts,  
                'conflict_status': 'Pending Review' if has_conflicts else 'No Conflict',
                'last_conflict_check': datetime.now(),
                'conflict_info': json.dumps(conflict_info)  
            }
            
            try:
                has_analysis_status = self._check_column_exists('documents', 'conflict_analysis_status')
                if has_analysis_status:
                    update_data['conflict_analysis_status'] = 'Analyzed'
            except Exception as e:
                logger.warning(f"Could not check column conflict_analysis_status: {str(e)}")
            
            return self.db.update_document_status(doc_id, update_data)
                
        except Exception as e:
            logger.error(f"Error saving conflicting information for document {doc_id}: {str(e)}")
            logger.error(traceback.format_exc())
            
            try:
                basic_update = {
                    'has_conflicts': has_conflicts if isinstance(has_conflicts, bool) else True,
                    'conflict_status': 'Pending Review' if has_conflicts else 'No Conflict',
                    'last_conflict_check': datetime.now()
                }
                return self.db.update_document_status(doc_id, basic_update)
            except Exception as basic_error:
                logger.error(f"Error while updating basic: {str(basic_error)}")
                return False
   
    def _check_column_exists(self, table_name, column_name):
        """
        Check if a column exists in a table

        Args:
        table_name (str): Table name
        column_name (str): Column name

        Returns:
        bool: True if column exists, False otherwise
        """
        try:
            query = """
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = %s AND column_name = %s
            """
            result = self.db.execute_with_retry(query, (table_name, column_name), fetch=True)
            return bool(result and result[0])
        except Exception as e:
            logger.error(f"Error checking column {column_name} in table {table_name}: {str(e)}")
            return False

    def sync_group_conflicts_by_group(self, group_id):
        """
        Synchronize conflict information for all documents in a duplicate group based on group_id
        
        Args:
            group_id (str): ID of the duplicate group to sync conflicts for
            
        Returns:
            Dict: Status information with conflict count and status
        """
        try:
            if not group_id:
                logger.warning(f"Invalid group ID")
                return {
                    'conflicts': [],
                    'status': "No Conflict"
                }

            logger.info(f"Getting documents in group {group_id}")
            group_docs = self.db.get_documents_in_group(group_id)
            if not group_docs:
                logger.warning(f"No documents found in group {group_id}")
                return {
                    'conflicts': [],
                    'status': "No Conflict"
                }

            all_content_conflicts = []
            all_internal_conflicts = []
            all_external_conflicts = []
            
            processed_content_keys = set()
            processed_internal_keys = set()
            processed_external_keys = set()
            
            all_doc_ids = [d['id'] for d in group_docs]
            
            # Get all enabled chunks for all documents in the group
            enabled_chunks_by_id = {}
            for group_doc_id in all_doc_ids:
                try:
                    chunks = self.chroma_manager.get_chunks_by_document_id(group_doc_id)
                    if chunks:
                        enabled_chunks = []
                        for chunk in chunks:
                            metadata = chunk.get('metadata', {})
                            is_enabled = metadata.get('is_enabled', True)
                            if is_enabled:
                                enabled_chunks.append(chunk['id'])
                        enabled_chunks_by_id[group_doc_id] = set(enabled_chunks)
                except Exception as chunk_error:
                    logger.error(f"Error getting chunks for document {group_doc_id}: {str(chunk_error)}")
                    enabled_chunks_by_id[group_doc_id] = set()
            
            # First retrieve direct conflicts from database, with optimized processing
            try:
                direct_conflicts_query = """
                    SELECT doc_id, conflict_id, chunk_ids, explanation, conflicting_parts, 
                        detected_at, conflict_type, resolved, severity
                    FROM chunk_conflicts 
                    WHERE doc_id IN %s AND resolved = FALSE
                    ORDER BY detected_at DESC
                """
                direct_conflicts = self.db.execute_with_retry(
                    direct_conflicts_query, 
                    (tuple(all_doc_ids),), 
                    fetch=True
                )
                
                if direct_conflicts:
                    logger.info(f"Found {len(direct_conflicts)} direct conflicts from chunk_conflicts table")
                    
                    for conflict in direct_conflicts:
                        doc_id = conflict[0]
                        conflict_id = conflict[1]
                        chunk_ids = conflict[2] if isinstance(conflict[2], list) else [conflict[2]]
                        explanation = conflict[3]
                        conflicting_parts = conflict[4] if isinstance(conflict[4], list) else []
                        detected_at = conflict[5]
                        conflict_type = conflict[6]
                        severity = conflict[8] if len(conflict) > 8 else "medium"
                        
                        if conflict_type == 'content' and len(chunk_ids) == 1:
                            content_key = f"content_{chunk_ids[0]}"
                            if content_key in processed_content_keys:
                                continue
                        elif conflict_type == 'internal':
                            sorted_ids = sorted(chunk_ids)
                            internal_key = f"internal_{'_'.join(sorted_ids)}"
                            if internal_key in processed_internal_keys:
                                continue
                        elif conflict_type == 'external':
                            sorted_ids = sorted(chunk_ids)
                            external_key = f"external_{'_'.join(sorted_ids)}"
                            if external_key in processed_external_keys:
                                continue
                        
                        all_chunks_enabled = True
                        for chunk_id in chunk_ids:
                            chunk_doc_id = chunk_id.split('_paragraph_')[0] if '_paragraph_' in chunk_id else None
                            if chunk_doc_id and chunk_doc_id in enabled_chunks_by_id:
                                if chunk_id not in enabled_chunks_by_id[chunk_doc_id]:
                                    all_chunks_enabled = False
                                    logger.info(f"Skipping conflict {conflict_id} because chunk {chunk_id} is disabled")
                                    break
                        
                        if not all_chunks_enabled:
                            continue
                        
                        conflict_data = {
                            "chunk_ids": chunk_ids,
                            "explanation": explanation or "Detected conflict between chunks",
                            "conflicting_parts": conflicting_parts,
                            "analyzed_at": detected_at.isoformat() if detected_at else datetime.now().isoformat(),
                            "has_conflict": True,
                            "severity": severity
                        }
                        
                        if conflict_type == 'content':
                            if len(chunk_ids) == 1:
                                content_key = f"content_{chunk_ids[0]}"
                                processed_content_keys.add(content_key)
                                conflict_data["chunk_id"] = chunk_ids[0]
                                all_content_conflicts.append(conflict_data)
                        elif conflict_type == 'internal':
                            sorted_ids = sorted(chunk_ids)
                            internal_key = f"internal_{'_'.join(sorted_ids)}"
                            processed_internal_keys.add(internal_key)
                            all_internal_conflicts.append(conflict_data)
                        elif conflict_type == 'external':
                            sorted_ids = sorted(chunk_ids)
                            external_key = f"external_{'_'.join(sorted_ids)}"
                            processed_external_keys.add(external_key)
                            all_external_conflicts.append(conflict_data)
            except Exception as db_error:
                logger.error(f"Error when querying conflicts: {str(db_error)}")
                logger.error(traceback.format_exc())
            
            # Process conflicts from document conflict_info fields
            for doc in group_docs:
                doc_conflict_info = doc.get('conflict_info')
                if doc_conflict_info:
                    if isinstance(doc_conflict_info, str):
                        try:
                            doc_conflict_info = json.loads(doc_conflict_info)
                        except json.JSONDecodeError:
                            logger.warning(f"Invalid JSON in conflict_info of {doc['id']}")
                            continue
                    
                    if not isinstance(doc_conflict_info, dict):
                        continue
                    
                    content_conflicts = doc_conflict_info.get('content_conflicts', [])
                    if content_conflicts:
                        for conflict in content_conflicts:
                            if not isinstance(conflict, dict):
                                continue
                            
                            chunk_id = conflict.get('chunk_id')
                            if not chunk_id and 'chunk_ids' in conflict and conflict['chunk_ids']:
                                chunk_id = conflict['chunk_ids'][0]
                            
                            if chunk_id:
                                content_key = f"content_{chunk_id}"
                                if content_key in processed_content_keys:
                                    continue
                                
                                chunk_doc_id = chunk_id.split('_paragraph_')[0] if '_paragraph_' in chunk_id else None
                                if chunk_doc_id and chunk_doc_id in enabled_chunks_by_id:
                                    if chunk_id not in enabled_chunks_by_id[chunk_doc_id]:
                                        logger.info(f"Skipping content conflict for disabled chunk {chunk_id}")
                                        continue
                                
                                processed_content_keys.add(content_key)
                                if 'chunk_id' not in conflict:
                                    conflict['chunk_id'] = chunk_id
                                all_content_conflicts.append(conflict)
                    
                    # Process internal conflicts
                    internal_conflicts = doc_conflict_info.get('internal_conflicts', [])
                    if internal_conflicts:
                        for conflict in internal_conflicts:
                            if not isinstance(conflict, dict) or 'chunk_ids' not in conflict:
                                continue
                            
                            chunk_ids = conflict['chunk_ids']
                            if not chunk_ids or len(chunk_ids) < 2:
                                continue
                            
                            sorted_ids = sorted(chunk_ids)
                            internal_key = f"internal_{'_'.join(sorted_ids)}"
                            if internal_key in processed_internal_keys:
                                continue
                            
                            all_chunks_enabled = True
                            for chunk_id in chunk_ids:
                                chunk_doc_id = chunk_id.split('_paragraph_')[0] if '_paragraph_' in chunk_id else None
                                if chunk_doc_id and chunk_doc_id in enabled_chunks_by_id:
                                    if chunk_id not in enabled_chunks_by_id[chunk_doc_id]:
                                        all_chunks_enabled = False
                                        logger.info(f"Skipping internal conflict because chunk {chunk_id} is disabled")
                                        break
                            
                            if not all_chunks_enabled:
                                continue
                            
                            processed_internal_keys.add(internal_key)
                            all_internal_conflicts.append(conflict)
                    
                    # Process external conflicts
                    external_conflicts = doc_conflict_info.get('external_conflicts', [])
                    if external_conflicts:
                        for conflict in external_conflicts:
                            if not isinstance(conflict, dict) or 'chunk_ids' not in conflict:
                                continue
                            
                            chunk_ids = conflict['chunk_ids']
                            if not chunk_ids or len(chunk_ids) < 2:
                                continue
                            
                            sorted_ids = sorted(chunk_ids)
                            external_key = f"external_{'_'.join(sorted_ids)}"
                            if external_key in processed_external_keys:
                                continue
                            
                            all_chunks_enabled = True
                            for chunk_id in chunk_ids:
                                chunk_doc_id = chunk_id.split('_paragraph_')[0] if '_paragraph_' in chunk_id else None
                                if chunk_doc_id and chunk_doc_id in enabled_chunks_by_id:
                                    if chunk_id not in enabled_chunks_by_id[chunk_doc_id]:
                                        all_chunks_enabled = False
                                        logger.info(f"Skipping external conflict because chunk {chunk_id} is disabled")
                                        break
                            
                            if not all_chunks_enabled:
                                continue
                            
                            processed_external_keys.add(external_key)
                            all_external_conflicts.append(conflict)

            combined_conflict_info = {
                "content_conflicts": all_content_conflicts,
                "internal_conflicts": all_internal_conflicts,
                "external_conflicts": all_external_conflicts,
                "last_updated": datetime.now().isoformat()
            }
            
            has_conflicts = bool(all_content_conflicts or all_internal_conflicts or all_external_conflicts)
            conflict_status = "Pending Review" if has_conflicts else "No Conflict"
            
            logger.info(f"Collected conflict information: {len(all_content_conflicts)} content conflicts, "
                f"{len(all_internal_conflicts)} internal conflicts, {len(all_external_conflicts)} external conflicts")

            # Update conflict information for all documents in the group
            updated_docs = 0
            for doc in group_docs:
                try:
                    doc_id = doc['id']
                    logger.info(f"Synchronizing conflict information for document {doc_id}")
                    self.db.update_document_status(doc_id, {
                        'has_conflicts': has_conflicts,
                        'conflict_info': json.dumps(combined_conflict_info),
                        'conflict_status': conflict_status,
                        'last_conflict_check': datetime.now().isoformat()
                    })
                    updated_docs += 1
                except Exception as doc_error:
                    logger.error(f"Error updating document {doc['id']} in group: {str(doc_error)}")

            logger.info(f"Updated conflict information for {updated_docs} documents in group {group_id}")
            return {
                'conflicts': all_content_conflicts + all_internal_conflicts + all_external_conflicts,
                'status': conflict_status
            }

        except Exception as e:
            logger.error(f"Error in sync_group_conflicts_by_group: {str(e)}")
            logger.error(traceback.format_exc())
            return {
                'conflicts': [],
                'status': "No Conflict"
            }
        
    # def get_direct_external_conflicts(self, doc_id: str) -> List[Dict]:
    #     """
    #     Get internal conflicts information directly from chunk_conflicts table

    #     Args:
    #     doc_id (str): ID of document to get conflicts

    #     Returns:
    #     List[Dict]: List of internal conflicts
    #     """
    #     try:
    #         query = """
    #             SELECT conflict_id, chunk_ids, explanation, conflicting_parts, detected_at, severity
    #             FROM chunk_conflicts 
    #             WHERE doc_id = %s 
    #             AND conflict_type = 'external'
    #             AND resolved = FALSE
    #             ORDER BY detected_at DESC
    #         """
            
    #         results = self.db.execute_with_retry(query, (doc_id,), fetch=True)
    #         if not results:
    #             return []
            
    #         conflicts = []
    #         for row in results:
    #             conflict_id, chunk_ids, explanation, conflicting_parts, detected_at, severity = row
                
    #             conflict_data = {
    #                 "chunk_ids": chunk_ids,
    #                 "explanation": explanation or "Mâu thuẫn ngoại bộ",
    #                 "conflicting_parts": conflicting_parts or [],
    #                 "detected_at": detected_at.isoformat() if detected_at else datetime.now().isoformat(),
    #                 "has_conflict": True,
    #                 "conflict_type": "external",
    #                 "severity": severity or "medium"
    #             }
                
    #             conflicts.append(conflict_data)
                
    #         return conflicts
        
    #     except Exception as e:
    #         logger.error(f"Error when getting direct external conflict: {str(e)}")
    #         logger.error(traceback.format_exc())
    #         return []

    def _create_empty_result(self, status="success"):
        """Generate empty results when there is no data or an error"""
        return {
            "status": status,
            "has_conflicts": False,
            "content_conflicts": [],
            "internal_conflicts": [],
            "external_conflicts": [],
            "last_updated": datetime.now().isoformat(),
            "message": "Không có dữ liệu để phân tích" if status == "success" else "Lỗi phân tích"
        }

    def resolve_conflict(self, conflict_id: str, resolved_by: str, resolution_notes: str = "") -> bool:
        """Mark resolved a conflict"""
        try:
            parts = conflict_id.split('_paragraph_')
            if len(parts) != 2:
                return False
                
            doc_id = parts[0]
            
            document = self.db.get_document_by_id(doc_id)
            if not document:
                logger.error(f"Document {doc_id} does not exist")
                return False
                
            try:
                check_column_query = """
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name='chunk_conflicts' AND column_name='resolved_by'
                """
                
                column_exists = self.db.execute_with_retry(check_column_query, fetch=True)
                
                if column_exists:
                    query = """
                        UPDATE chunk_conflicts
                        SET 
                            resolved = TRUE,
                            resolved_at = CURRENT_TIMESTAMP,
                            resolution_notes = %s,
                            resolved_by = %s,
                            updated_at = CURRENT_TIMESTAMP  
                        WHERE conflict_id = %s
                        RETURNING id
                    """
                    result = self.db.execute_with_retry(query, (resolution_notes, resolved_by, conflict_id), fetch=True)
                else:
                    query = """
                        UPDATE chunk_conflicts
                        SET 
                            resolved = TRUE,
                            resolved_at = CURRENT_TIMESTAMP,
                            resolution_notes = %s,
                            updated_at = CURRENT_TIMESTAMP  
                        WHERE conflict_id = %s
                        RETURNING id
                    """
                    result = self.db.execute_with_retry(query, (resolution_notes, conflict_id), fetch=True)
            except Exception as db_error:
                logger.warning(f"Error checking table structure: {str(db_error)}, use query instead")
                query = """
                    UPDATE chunk_conflicts
                    SET 
                        resolved = TRUE,
                        resolved_at = CURRENT_TIMESTAMP,
                        resolution_notes = %s,
                        updated_at = CURRENT_TIMESTAMP  
                    WHERE conflict_id = %s
                    RETURNING id
                """
                result = self.db.execute_with_retry(query, (resolution_notes, conflict_id), fetch=True)
            
            if not result:
                self._create_conflict_record(doc_id, [conflict_id], resolved_by, resolution_notes)
                
            conflict_info = document.get('conflict_info')
            if conflict_info:
                if isinstance(conflict_info, str):
                    try:
                        conflict_info = json.loads(conflict_info)
                    except json.JSONDecodeError:
                        conflict_info = {
                            "internal_conflicts": [],
                            "external_conflicts": [],
                            "content_conflicts": []
                        }
                        
                for conflict_type in ["internal_conflicts", "external_conflicts", "content_conflicts"]:
                    if conflict_type in conflict_info:
                        conflict_info[conflict_type] = [
                            c for c in conflict_info[conflict_type] 
                            if not (isinstance(c, dict) and "chunk_ids" in c and conflict_id in c["chunk_ids"])
                        ]
                
                has_conflicts = bool(
                    conflict_info.get("internal_conflicts", []) or 
                    conflict_info.get("external_conflicts", []) or
                    conflict_info.get("content_conflicts", [])
                )
                
                conflict_status = "Pending Review" if has_conflicts else "No Conflict"
                
                self.db.update_document_status(doc_id, {
                    'has_conflicts': has_conflicts,
                    'conflict_info': json.dumps(conflict_info),
                    'conflict_status': conflict_status,
                    'last_conflict_check': datetime.now().isoformat()
                })
                
                duplicate_group_id = document.get('duplicate_group_id')
                if duplicate_group_id:
                    self.sync_group_conflicts(doc_id)
                    
            return True
            
        except Exception as e:
            logger.error(f"Error resolving conflict {conflict_id}: {str(e)}")
            logger.error(traceback.format_exc())
            return False

    def _create_conflict_record(self, doc_id: str, chunk_ids: List[str], 
                            resolved_by: str, resolution_notes: str) -> bool:
        """Create new resolved conflict record"""
        try:
            conflict_id = '_'.join(sorted(chunk_ids))
            
            try:
                check_column_query = """
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name='chunk_conflicts' AND column_name='resolved_by'
                """
                
                column_exists = self.db.execute_with_retry(check_column_query, fetch=True)
                
                if column_exists:
                    query = """
                        INSERT INTO chunk_conflicts (
                            doc_id,
                            conflict_id,
                            chunk_ids,
                            conflict_type, 
                            explanation,
                            conflicting_parts,
                            resolved,
                            resolved_at,
                            resolved_by,
                            resolution_notes
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        RETURNING id
                    """
                    
                    result = self.db.execute_with_retry(
                        query,
                        (
                            doc_id, conflict_id, chunk_ids, 
                            'manual', 'Đánh dấu thủ công bởi người dùng', ['N/A'],
                            True, datetime.now().isoformat(), resolved_by, resolution_notes
                        ),
                        fetch=True
                    )
                else:
                    query = """
                        INSERT INTO chunk_conflicts (
                            doc_id,
                            conflict_id,
                            chunk_ids,
                            conflict_type, 
                            explanation,
                            conflicting_parts,
                            resolved,
                            resolved_at,
                            resolution_notes
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        RETURNING id
                    """
                    
                    result = self.db.execute_with_retry(
                        query,
                        (
                            doc_id, conflict_id, chunk_ids, 
                            'manual', 'Đánh dấu thủ công bởi người dùng', ['N/A'],
                            True, datetime.now().isoformat(), resolution_notes
                        ),
                        fetch=True
                    )
            except Exception as db_error:
                query = """
                    INSERT INTO chunk_conflicts (
                        doc_id,
                        conflict_id,
                        chunk_ids,
                        conflict_type, 
                        explanation,
                        conflicting_parts,
                        resolved,
                        resolved_at,
                        resolution_notes
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    RETURNING id
                """
                
                result = self.db.execute_with_retry(
                    query,
                    (
                        doc_id, conflict_id, chunk_ids, 
                        'manual', 'Đánh dấu thủ công bởi người dùng', ['N/A'],
                        True, datetime.now().isoformat(), resolution_notes
                    ),
                    fetch=True
                )
            
            return bool(result)
            
        except Exception as e:
            logger.error(f"Error creating conflicting record: {str(e)}")
            return False
    
    def count_conflicts(self, conflict_info):
        """
        Count exact number of conflicts from conflict information, including new formats

        Args:
        conflict_info: Dict or JSON string containing conflict information

        Returns:
        Dict: Dictionary containing number of conflict types
        """
        try:
            if isinstance(conflict_info, str):
                try:
                    conflict_info = json.loads(conflict_info)
                except json.JSONDecodeError:
                    logger.error(f"Định dạng JSON không hợp lệ: {conflict_info}")
                    return {'total': 0, 'internal': 0, 'external': 0, 'content': 0}
            
            if not isinstance(conflict_info, dict):
                return {'total': 0, 'internal': 0, 'external': 0, 'content': 0}
                
            content_conflicts = conflict_info.get('content_conflicts', [])
            internal_conflicts = conflict_info.get('internal_conflicts', [])
            external_conflicts = conflict_info.get('external_conflicts', [])
            
            content_count = 0
            for conflict in content_conflicts:
                if isinstance(conflict, dict):
                    if "contradictions" in conflict:
                        content_count += len(conflict.get("contradictions", []))
                        if len(conflict.get("contradictions", [])) == 0 and conflict.get("has_contradiction") == "yes":
                            content_count += 1
                    else:
                        content_count += 1
                else:
                    content_count += 1
            
            internal_count = len(internal_conflicts)
            external_count = len(external_conflicts)
            total_count = content_count + internal_count + external_count
            
            return {
                'total': total_count,
                'content': content_count,
                'internal': internal_count,
                'external': external_count
            }
        except Exception as e:
            logger.error(f"Lỗi khi đếm số lượng mâu thuẫn: {str(e)}")
            return {'total': 0, 'internal': 0, 'external': 0, 'content': 0}

    def analyze_document(self, doc_id: str) -> Dict:
        return self._create_empty_result(status="success")
    
    # def analyze_document(self, doc_id: str) -> Dict:
    #     """
    #     Comprehensive analysis of conflicts in a document.
    #     Improved logging and error handling.
    #     Skips disabled chunks during analysis.

    #     Args:
    #     doc_id: ID of the document to analyze

    #     Returns:
    #     Dict: Results of conflict analysis
    #     """
    #     try:
    #         start_time = datetime.now()
    #         logger.info(f"Starting conflict analysis for document {doc_id}")
            
    #         document = self.db.get_document_by_id(doc_id)
    #         if not document:
    #             logger.warning(f"Document {doc_id} not found, cannot analyze conflicts")
    #             return self._create_empty_result(status="error", message="Document not found")
                
    #         if self._is_cache_valid(doc_id):
    #             conflict_info = document.get('conflict_info')
                
    #             if conflict_info:
    #                 if isinstance(conflict_info, str):
    #                     try:
    #                         conflict_info = json.loads(conflict_info)
    #                         return conflict_info
    #                     except json.JSONDecodeError:
    #                         logger.warning(f"Invalid JSON conflict_info: {conflict_info}")
            
    #         duplicate_group_id = document.get('duplicate_group_id')
    #         if duplicate_group_id:
    #             group_docs = self.db.get_documents_in_group(duplicate_group_id)
    #             if group_docs:
    #                 latest_doc = None
    #                 latest_check_time = None
                    
    #                 for group_doc in group_docs:
    #                     if group_doc['id'] != doc_id and group_doc.get('conflict_info') and group_doc.get('last_conflict_check'):
    #                         try:
    #                             check_time = None
    #                             if isinstance(group_doc['last_conflict_check'], str):
    #                                 check_time = datetime.fromisoformat(group_doc['last_conflict_check'])
    #                             elif isinstance(group_doc['last_conflict_check'], datetime):
    #                                 check_time = group_doc['last_conflict_check']
                                    
    #                             if check_time and (not latest_check_time or check_time > latest_check_time):
    #                                 latest_check_time = check_time
    #                                 latest_doc = group_doc
    #                         except:
    #                             continue
                    
    #                 if latest_doc and latest_doc.get('conflict_info') and latest_check_time:
    #                     if (datetime.now() - latest_check_time).total_seconds() < 3600:
    #                         logger.info(f"Using existing conflict analysis from document {latest_doc['id']} in group")
                            
    #                         conflict_info = latest_doc.get('conflict_info')
    #                         has_conflicts = latest_doc.get('has_conflicts', False)
    #                         conflict_status = latest_doc.get('conflict_status', 'No Conflict')
                            
    #                         self.db.update_document_status(doc_id, {
    #                             'conflict_info': conflict_info,
    #                             'has_conflicts': has_conflicts,
    #                             'conflict_status': conflict_status,
    #                             'last_conflict_check': datetime.now().isoformat(),
    #                             'conflict_analysis_status': 'Analyzed'
    #                         })
                            
    #                         if isinstance(conflict_info, str):
    #                             try:
    #                                 return json.loads(conflict_info)
    #                             except json.JSONDecodeError:
    #                                 logger.warning(f"Invalid JSON from group document: {conflict_info}")
    #                         else:
    #                             return conflict_info
            
    #         chunks = self.chroma_manager.get_chunks_by_document_id(doc_id)
    #         if not chunks:
    #             logger.warning(f"No chunks found for document {doc_id}")
    #             empty_result = self._create_empty_result()
                
    #             self.db.update_document_status(doc_id, {
    #                 'has_conflicts': False,
    #                 'conflict_info': json.dumps(empty_result),
    #                 'conflict_status': 'No Conflict',
    #                 'last_conflict_check': datetime.now().isoformat(),
    #                 'conflict_analysis_status': 'Analyzed'
    #             })
                
    #             return empty_result
                
    #         logger.info(f"Found {len(chunks)} chunks for document {doc_id}")
            
    #         # Filter out disabled chunks
    #         enabled_chunks = []
    #         for chunk in chunks:
    #             metadata = chunk.get('metadata', {})
    #             is_enabled = metadata.get('is_enabled', True)
    #             if is_enabled:
    #                 enabled_chunks.append(chunk)
    #             else:
    #                 logger.info(f"Skipping disabled chunk {chunk.get('id')} during conflict analysis")
                    
    #         logger.info(f"Using {len(enabled_chunks)} enabled chunks out of {len(chunks)} total chunks")
            
    #         if not enabled_chunks:
    #             logger.warning(f"No enabled chunks found for document {doc_id}")
    #             empty_result = self._create_empty_result()
                
    #             self.db.update_document_status(doc_id, {
    #                 'has_conflicts': False,
    #                 'conflict_info': json.dumps(empty_result),
    #                 'conflict_status': 'No Conflict',
    #                 'last_conflict_check': datetime.now().isoformat(),
    #                 'conflict_analysis_status': 'Analyzed'
    #             })
                
    #             return empty_result

    #         logger.info(f"Starting content conflict analysis for document {doc_id}")
    #         content_conflicts = self.analyze_content_conflicts(enabled_chunks)
    #         logger.info(f"Detected {len(content_conflicts)} content conflicts in document {doc_id}")
            
    #         logger.info(f"Starting internal chunk conflict analysis for document {doc_id}")
    #         internal_conflicts = self.analyze_internal_conflicts(enabled_chunks)
    #         logger.info(f"Detected {len(internal_conflicts)} internal conflicts between chunks in document {doc_id}")
            
    #         logger.info(f"Starting conflict analysis with other docs for document {doc_id}")
    #         external_conflicts = self.analyze_external_conflicts(doc_id, enabled_chunks)
    #         logger.info(f"Detected {len(external_conflicts)} conflicts with other docs for document {doc_id}")
            
    #         has_conflicts = bool(content_conflicts or internal_conflicts or external_conflicts)
            
    #         conflict_summary = {
    #             "content": [f"{c.explanation}" for c in content_conflicts if c.has_conflict],
    #             "internal": [f"{c.explanation}" for c in internal_conflicts if c.has_conflict],
    #             "external": []  
    #         }
            
    #         external_conflicts_data = []
    #         if external_conflicts:  
    #             if isinstance(external_conflicts[0], dict):
    #                 external_conflicts_data = external_conflicts
    #                 conflict_summary["external"] = [item.get("explanation", "") for item in external_conflicts if item.get("has_conflict", False)]
    #             else:
    #                 external_conflicts_data = [c.to_dict() for c in external_conflicts]
    #                 conflict_summary["external"] = [c.explanation for c in external_conflicts if c.has_conflict]
            
    #         conflict_info = {
    #             "has_conflicts": has_conflicts,
    #             "content_conflicts": [c.to_dict() for c in content_conflicts],
    #             "internal_conflicts": [c.to_dict() for c in internal_conflicts],
    #             "external_conflicts": external_conflicts_data,  
    #             "conflict_summary": conflict_summary,
    #             "last_updated": datetime.now().isoformat(),
    #             "analysis_time_seconds": (datetime.now() - start_time).total_seconds()
    #         }
            
    #         logger.info(f"Save conflict analysis results for document {doc_id}: has_conflicts={has_conflicts}")
    #         self._store_conflict_info(doc_id, conflict_info)
            
    #         if duplicate_group_id:
    #             logger.info(f"Sync conflicts for duplicate group {duplicate_group_id}")
    #             self.sync_group_conflicts(doc_id)
            
    #         end_time = datetime.now()
    #         duration = (end_time - start_time).total_seconds()
    #         logger.info(f"Completed conflict analysis for document {doc_id} in {duration} seconds")
            
    #         return conflict_info
            
    #     except Exception as e:
    #         logger.error(f"Error in analyze_document {doc_id}: {str(e)}")
    #         logger.error(traceback.format_exc())
            
    #         try:
    #             self.db.update_document_status(doc_id, {
    #                 'conflict_analysis_status': 'AnalysisFailed',
    #                 'conflict_analysis_error': str(e)
    #             })
    #         except Exception as update_error:
    #             logger.error(f"Error updating error status: {str(update_error)}")
                
    #         return self._create_empty_result(status="error")
    
  
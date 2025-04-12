import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Any, Optional, Tuple
import time
from datetime import datetime
import os
from queue import Queue, PriorityQueue
import threading
import json
from common.openai_conflict_analyzer import OpenAIConflictAnalyzer
from common.data_manager import DatabaseManager
from common.chroma_manager import ChromaManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AsyncConflictProcessor:
    """
    Asynchronous Conflict Processor Using OpenAI
    """
    def __init__(self, data_manager: DatabaseManager, chroma_manager: ChromaManager,
             max_workers: int = 3, queue_size: int = 100):
        self.data_manager = data_manager
        self.chroma_manager = chroma_manager
        self.max_workers = max_workers
        
        try:
            api_key = os.getenv('OPENAI_API_KEY')
            model = os.getenv('MODEL')  
            
            self.analyzer = OpenAIConflictAnalyzer(
                api_key=api_key, 
                model=model,
                use_cache=True, 
                max_workers=max_workers
            )
            
            logger.info("OpenAI Analyzer initialized successfully for conflict analysis")
            self.use_analyzer = True
        except Exception as e:
            logger.error(f"Error initializing OpenAI Analyzer: {str(e)}")
            self.use_analyzer = False
            self.analyzer = None
            
        self.task_queue = PriorityQueue(maxsize=queue_size)
        self.results_cache = {}
        self.processing_status = {}
        
        self.processed_pairs = set()  
        self.pair_cache = {}         
        
        self.running = True
        self.worker_thread = threading.Thread(target=self._process_queue, daemon=True)
        self.worker_thread.start()
        
        logger.info(f"AsyncConflictProcessor initialized with {max_workers} workers")

    
    def _manage_cache(self):
        """Manage cache size to prevent memory issues"""
        max_cache_entries = 1000  
        
        if len(self.results_cache) > max_cache_entries:
            sorted_keys = sorted(
                self.results_cache.keys(),
                key=lambda k: self.results_cache[k].get('completed_at', ''),
                reverse=True
            )
            
            keys_to_keep = sorted_keys[:int(max_cache_entries * 0.7)]
            self.results_cache = {k: self.results_cache[k] for k in keys_to_keep}
            logger.info(f"Cleaned results cache, kept {len(keys_to_keep)} entries")
        
        if hasattr(self, 'pair_cache') and len(self.pair_cache) > max_cache_entries:
            keys = list(self.pair_cache.keys())
            keys_to_keep = keys[:int(max_cache_entries * 0.7)]
            self.pair_cache = {k: self.pair_cache[k] for k in keys_to_keep}
            logger.info(f"Cleaned pair cache, kept {len(keys_to_keep)} entries")
        
        if hasattr(self, 'processed_pairs') and len(self.processed_pairs) > max_cache_entries * 2:
            self.processed_pairs.clear()
            logger.info("Cleared processed pairs tracking set")


    
    def get_queue_stats(self):
        """Returns statistics about the queue"""
        return {
            "queue_size": self.task_queue.qsize(),
            "active_tasks": len([k for k, v in self.processing_status.items() if v == "processing"]),
            "completed_tasks": len([k for k, v in self.processing_status.items() if v == "completed"]),
            "failed_tasks": len([k for k, v in self.processing_status.items() if v == "failed"]),
            "cache_size": len(self.results_cache)
        }
        

    def _process_queue(self):
        import random  
        
        cache_check_counter = 0
        cache_check_interval = 20  
        
        while self.running:
            try:
                cache_check_counter += 1
                if cache_check_counter >= cache_check_interval:
                    self._manage_cache()
                    cache_check_counter = 0
                    
                if self.task_queue.empty():
                    time.sleep(1)
                    continue
                    
                priority, task_id, task = self.task_queue.get()
                logger.info(f"Processing task {task_id} (priority: {priority})")
                
                try:
                    self.processing_status[task_id] = "processing"
                    
                    doc_id = task.get('doc_id')
                    task_type = task.get('type', 'document')
                    
                    if task_type == 'document':
                        result = self._analyze_document(doc_id)
                    elif task_type == 'chunk_pair':
                        chunk1 = task.get('chunk1')
                        chunk2 = task.get('chunk2')
                        conflict_type = task.get('conflict_type', 'internal')
                        result = self._analyze_chunk_pair(chunk1, chunk2, conflict_type)
                    elif task_type == 'content':
                        content = task.get('content')
                        result = self._analyze_content(content)
                    else:
                        result = {"error": "Unknown task type"}
                        
                    self.results_cache[task_id] = {
                        "result": result,
                        "completed_at": datetime.now().isoformat(),
                        "status": "completed"
                    }
                    self.processing_status[task_id] = "completed"
                    
                    if 'callback' in task:
                        try:
                            callback_func = task['callback']
                            callback_func(task_id, result)
                        except Exception as callback_error:
                            logger.error(f"Error in callback for task {task_id}: {str(callback_error)}")
                            
                except Exception as e:
                    logger.error(f"Error processing task {task_id}: {str(e)}")
                    self.results_cache[task_id] = {
                        "error": str(e),
                        "completed_at": datetime.now().isoformat(),
                        "status": "failed"
                    }
                    self.processing_status[task_id] = "failed"
                
                finally:
                    self.task_queue.task_done()
                    
            except Exception as e:
                logger.error(f"Error in processing queue: {str(e)}")
                time.sleep(1)
    
    def get_task_status(self, task_id: str) -> Dict:
        """
        Get the status of a task

        Args:
        task_id: ID of the task

        Returns:
        Dict: Status and result of the task
        """
        if task_id in self.results_cache:
            return self.results_cache[task_id]
        elif task_id in self.processing_status:
            return {
                "status": self.processing_status[task_id],
                "message": f"Task {task_id} đang được xử lý"
            }
        else:
            return {
                "status": "not_found",
                "message": f"Không tìm thấy task {task_id}"
            }
    
    def queue_document(self, doc_id: str, priority: int = 5) -> str:
        """
        Add a document to the queue for conflict analysis

        Args:
        doc_id: ID of the document
        priority: Priority (1-10, 1 being the highest)

        Returns:
        str: ID of the task
        """
        task_id = f"doc_{doc_id}_{int(time.time())}"
        task = {
            'type': 'document',
            'doc_id': doc_id
        }
        
        try:
            self.task_queue.put((priority, task_id, task))
            self.processing_status[task_id] = "queued"
            logger.info(f"Added document {doc_id} to queue with ID {task_id}")
            return task_id
        except Exception as e:
            logger.error(f"Error adding document {doc_id} to queue: {str(e)}")
            return None
            
    def queue_content(self, content: str, priority: int = 3) -> str:
        """
        Add a text content to the queue for conflict analysis

        Args:
        content: Content to analyze
        priority: Priority (1-10, 1 being the highest)

        Returns:
        str: ID of the task
        """
        task_id = f"content_{int(time.time())}"
        task = {
            'type': 'content',
            'content': content
        }
        
        try:
            self.task_queue.put((priority, task_id, task))
            self.processing_status[task_id] = "queued"
            logger.info(f"Added content to queue with ID {task_id}")
            return task_id
        except Exception as e:
            logger.error(f"Error adding content to queue: {str(e)}")
            return None
            

    def queue_chunk_pair(self, chunk1: Dict, chunk2: Dict, conflict_type: str = "internal", priority: int = 3) -> str:
        """
        Add a pair of chunks to the queue for conflict analysis with deduplication

        Args:
        chunk1: First chunk
        chunk2: Second chunk
        conflict_type: Conflict type (internal/external)
        priority: Priority (1-10, 1 being highest)

        Returns:
        str: ID of the task
        """
        chunk_ids = sorted([chunk1['id'], chunk2['id']])
        task_id = f"pair_{chunk_ids[0]}_{chunk_ids[1]}_{int(time.time())}"
        
        if not hasattr(self, 'processed_pairs'):
            self.processed_pairs = set()
        
        pair_key = f"{conflict_type}_{chunk_ids[0]}_{chunk_ids[1]}"
        
        if pair_key in self.processed_pairs:
            logger.info(f"Chunk pair {pair_key} already queued or processed recently")
            return f"existing_{pair_key}"
        
        self.processed_pairs.add(pair_key)
        
        task = {
            'type': 'chunk_pair',
            'chunk1': chunk1,
            'chunk2': chunk2,
            'conflict_type': conflict_type,
            'pair_key': pair_key 
        }
        
        try:
            self.task_queue.put((priority, task_id, task))
            self.processing_status[task_id] = "queued"
            logger.info(f"Added chunk pair {pair_key} to queue with ID {task_id}")
            return task_id
        except Exception as e:
            logger.error(f"Error adding chunk pair to queue: {str(e)}")
            self.processed_pairs.remove(pair_key)
            return None
     

    def _analyze_document(self, doc_id: str) -> Dict:
        """
        Analyze conflicts for the entire document 
        
        Args:
        doc_id: ID of the document
        
        Returns:
        Dict: Results of the conflict analysis
        """
        if not self.use_analyzer:
            return {"error": "OpenAI Analyzer is not available"}
            
        try:
            start_time = time.time()
            logger.info(f"Starting conflict analysis for document {doc_id}")
            
            document = self.data_manager.get_document_by_id(doc_id)
            if not document:
                return {"error": f"Document {doc_id} not found"}
                
            chunks = self.chroma_manager.get_chunks_by_document_id(doc_id)
            if not chunks:
                logger.info(f"No chunks found for document {doc_id}")
                return {'has_conflicts': False}
                
            logger.info(f"Found {len(chunks)} chunks for document {doc_id}")
            
            processed_conflict_keys = set()
            
            #1. Conflicts within the same chunk
            content_conflicts = []
            for chunk in chunks:
                if not chunk.get('original_text'):
                    continue
                
                original_text = chunk.get('original_text')
                result = self.analyzer.analyze_conflict(original_text, conflict_type="content")
                
                if result.has_conflict:
                    conflict_key = f"content_{chunk.get('id')}"
                    if conflict_key not in processed_conflict_keys:
                        processed_conflict_keys.add(conflict_key)
                        
                        # Create conflict data with rich information
                        conflict_data = {
                            "chunk_id": chunk.get('id'),
                            "explanation": result.explanation,
                            "conflicting_parts": result.conflicting_parts,
                            "analyzed_at": datetime.now().isoformat(),
                            "contradictions": result.contradictions if hasattr(result, 'contradictions') else []
                        }
                        
                        # Add information about contradiction types if available
                        contradiction_types = []
                        for contradiction in result.contradictions:
                            if "type" in contradiction and contradiction["type"] not in contradiction_types:
                                contradiction_types.append(contradiction["type"])
                        
                        if contradiction_types:
                            conflict_data["contradiction_types"] = contradiction_types
                        
                        content_conflicts.append(conflict_data)
                        
            # 2. internal conflicts
            internal_conflicts = []
            if len(chunks) >= 2:
                for i in range(len(chunks)):
                    for j in range(i + 1, len(chunks)):
                        chunk1 = chunks[i]
                        chunk2 = chunks[j]
                        
                        if not chunk1.get('original_text') or not chunk2.get('original_text'):
                            continue
                            
                        chunk_ids = sorted([chunk1.get('id'), chunk2.get('id')])
                        conflict_key = f"internal_{chunk_ids[0]}_{chunk_ids[1]}"
                        
                        if conflict_key in processed_conflict_keys:
                            continue
                            
                        processed_conflict_keys.add(conflict_key)
                        
                        result = self.analyzer.analyze_conflict(
                            chunk1.get('original_text'),
                            chunk2.get('original_text'),
                            conflict_type="internal"  
                        )
                        
                        if result.has_conflict:
                            conflict_data = {
                                "chunk_ids": [chunk1.get('id'), chunk2.get('id')],
                                "explanation": result.explanation,
                                "conflicting_parts": result.conflicting_parts,
                                "analyzed_at": datetime.now().isoformat(),
                                "contradictions": result.contradictions if hasattr(result, 'contradictions') else []
                            }
                            
                            # Add information about contradiction types if available
                            contradiction_types = []
                            for contradiction in result.contradictions:
                                if "type" in contradiction and contradiction["type"] not in contradiction_types:
                                    contradiction_types.append(contradiction["type"])
                            
                            if contradiction_types:
                                conflict_data["contradiction_types"] = contradiction_types
                                
                            internal_conflicts.append(conflict_data)
                            
            # 3. external conflicts
            external_conflicts = []
            duplicate_group_id = document.get('duplicate_group_id')
            
            if duplicate_group_id:
                group_docs = self.data_manager.get_documents_in_group(duplicate_group_id)
                
                # Track already processed document pairs to avoid redundant analysis
                processed_doc_pairs = set()
                
                for group_doc in group_docs:
                    if group_doc['id'] == doc_id:
                        continue
                        
                    # Generate a unique key for each document pair
                    doc_pair = tuple(sorted([doc_id, group_doc['id']]))
                    if doc_pair in processed_doc_pairs:
                        continue
                    
                    processed_doc_pairs.add(doc_pair)
                    
                    group_chunks = self.chroma_manager.get_chunks_by_document_id(group_doc['id'])
                    if not group_chunks:
                        continue
                    
                    for chunk1 in chunks:
                        for chunk2 in group_chunks:
                            if not chunk1.get('original_text') or not chunk2.get('original_text'):
                                continue
                                
                            chunk_ids = sorted([chunk1.get('id'), chunk2.get('id')])
                            conflict_key = f"external_{chunk_ids[0]}_{chunk_ids[1]}"
                            
                            if conflict_key in processed_conflict_keys:
                                continue
                                
                            processed_conflict_keys.add(conflict_key)
                            
                            result = self.analyzer.analyze_conflict(
                                chunk1.get('original_text'),
                                chunk2.get('original_text'),
                                conflict_type="external"  
                            )
                            
                            if result.has_conflict:
                                conflict_data = {
                                    "chunk_ids": [chunk1.get('id'), chunk2.get('id')],
                                    "documents": [doc_id, group_doc['id']],
                                    "explanation": result.explanation,
                                    "conflicting_parts": result.conflicting_parts,
                                    "analyzed_at": datetime.now().isoformat(),
                                    "contradictions": result.contradictions if hasattr(result, 'contradictions') else []
                                }
                                
                                # Add information about contradiction types if available
                                contradiction_types = []
                                for contradiction in result.contradictions:
                                    if "type" in contradiction and contradiction["type"] not in contradiction_types:
                                        contradiction_types.append(contradiction["type"])
                                
                                if contradiction_types:
                                    conflict_data["contradiction_types"] = contradiction_types
                                    
                                external_conflicts.append(conflict_data)
                                
            # Count direct and indirect conflicts
            direct_conflicts = 0
            indirect_conflicts = 0
            
            for conflicts in [content_conflicts, internal_conflicts, external_conflicts]:
                for conflict in conflicts:
                    for contradiction in conflict.get("contradictions", []):
                        if contradiction.get("type") == "trực tiếp":
                            direct_conflicts += 1
                        elif contradiction.get("type") == "gián tiếp":
                            indirect_conflicts += 1
            
            has_conflicts = bool(content_conflicts or internal_conflicts or external_conflicts)
            conflict_info = {
                "has_conflicts": has_conflicts,
                "content_conflicts": content_conflicts,
                "internal_conflicts": internal_conflicts, 
                "external_conflicts": external_conflicts,
                "last_updated": datetime.now().isoformat(),
                "analysis_duration_seconds": round(time.time() - start_time, 2)
            }
            
            # Add summary of contradiction types
            if direct_conflicts or indirect_conflicts:
                conflict_info["contradiction_types"] = {
                    "direct": direct_conflicts,
                    "indirect": indirect_conflicts,
                    "total": direct_conflicts + indirect_conflicts
                }
            
            self.data_manager.update_document_status(doc_id, {
                'has_conflicts': has_conflicts,
                'conflict_info': json.dumps(conflict_info),
                'conflict_status': "Pending Review" if has_conflicts else "No Conflict",
                'last_conflict_check': datetime.now().isoformat(),
                'conflict_analysis_status': "Analyzed"
            })
            
            logger.info(f"Completed conflict analysis for {doc_id}: {has_conflicts} in {round(time.time() - start_time, 2)}s")
            return conflict_info
            
        except Exception as e:
            logger.error(f"Contradictory parse error for document {doc_id}: {str(e)}")
            return {
                "error": f"Lỗi phân tích: {str(e)}",
                "has_conflicts": False,
                "content_conflicts": [],
                "internal_conflicts": [],
                "external_conflicts": [],
                "last_updated": datetime.now().isoformat()
            }

    def _analyze_chunk_pair(self, chunk1: Dict, chunk2: Dict, conflict_type: str = "internal") -> Dict:
        """
        Analyze conflict between two chunks with improved caching

        Args:
        chunk1: First chunk
        chunk2: Second chunk
        conflict_type: Conflict type (internal/external)

        Returns:
        Dict: Conflict analysis result
        """
        if not self.use_analyzer:
            return {"error": "OpenAI Analyzer không khả dụng"}
            
        try:
            if not chunk1.get('original_text') or not chunk2.get('original_text'):
                return {"error": "Thiếu nội dung chunk", "has_conflicts": False}
            
            chunk_ids = sorted([chunk1.get('id'), chunk2.get('id')])
            cache_key = f"{conflict_type}_{chunk_ids[0]}_{chunk_ids[1]}"
            
            if hasattr(self, 'pair_cache') and cache_key in self.pair_cache:
                logger.info(f"Using cached result for chunk pair {cache_key}")
                return self.pair_cache[cache_key]
                
            result = self.analyzer.analyze_conflict(
                chunk1.get('original_text'),
                chunk2.get('original_text'),
                conflict_type=conflict_type
            )
            
            response = {
                "has_conflicts": result.has_conflict,
                "explanation": result.explanation,
                "conflicting_parts": result.conflicting_parts,
                "chunk_ids": [chunk1.get('id'), chunk2.get('id')],
                "analyzed_at": datetime.now().isoformat(),
                "contradictions": result.contradictions if hasattr(result, 'contradictions') else []
            }
            
            contradiction_types = []
            for contradiction in result.contradictions:
                if "type" in contradiction and contradiction["type"] not in contradiction_types:
                    contradiction_types.append(contradiction["type"])
            
            if contradiction_types:
                response["contradiction_types"] = contradiction_types
            
            if not hasattr(self, 'pair_cache'):
                self.pair_cache = {}
            self.pair_cache[cache_key] = response
            
            return response
            
        except Exception as e:
            logger.error(f"Lỗi phân tích cặp chunk: {str(e)}")
            return {
                "error": f"Lỗi phân tích: {str(e)}",
                "has_conflicts": False,
                "explanation": f"Lỗi phân tích: {str(e)}",
                "conflicting_parts": [],
                "chunk_ids": [chunk1.get('id'), chunk2.get('id')],
                "analyzed_at": datetime.now().isoformat()
            }
    
    def _analyze_content(self, content: str) -> Dict:
        """
        Analyzing contradictions in a text content

        Args:
        content: Content to be analyzed

        Returns:
        Dict: Results of the contradiction analysis
        """
        if not self.use_analyzer:
            return {"error": "OpenAI Analyzer không khả dụng"}
            
        try:
            if not content:
                return {
                    "error": "Nội dung trống",
                    "has_conflicts": False,
                    "explanation": "Không có nội dung để phân tích",
                    "conflicting_parts": [],
                    "analyzed_at": datetime.now().isoformat()
                }
                
            result = self.analyzer.analyze_conflict(content, conflict_type="content")
            
            return {
                "has_conflicts": result.has_conflict,
                "explanation": result.explanation,
                "conflicting_parts": result.conflicting_parts,
                "analyzed_at": datetime.now().isoformat(),
                "contradictions": result.contradictions if hasattr(result, 'contradictions') else []
            }
            
        except Exception as e:
            logger.error(f"Error parsing content: {str(e)}")
            return {
                "error": f"Lỗi phân tích: {str(e)}",
                "has_conflicts": False,
                "explanation": f"Lỗi phân tích: {str(e)}",
                "conflicting_parts": [],
                "analyzed_at": datetime.now().isoformat()
            }
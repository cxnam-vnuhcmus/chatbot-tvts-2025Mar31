import asyncio
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
import threading
from typing import List, Dict, Optional
from common.data_manager import DatabaseManager
from common.chroma_manager import ChromaManager
from common.gpt_processor import GPTProcessor
from common.conflict_manager import ConflictManager
import json
from datetime import datetime
import logging
import os
import traceback

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ConflictProcessor:
    def __init__(self, chroma_manager, data_manager):
        
        self.logger = logging.getLogger(__name__)
        
        self.chroma_manager = ChromaManager()
        self.data_manager = DatabaseManager()
        self.gpt_processor = GPTProcessor()
        
        self.logger.info("Initializing conflict manager with OpenAI")
        self.conflict_manager = ConflictManager(data_manager, chroma_manager)  
        
        self.update_queue = Queue()
        self.conflict_check_queue = Queue()
        
        self.executor = ThreadPoolExecutor(max_workers=3)
        
        self.start_background_workers()

    def start_background_workers(self):
        """Start worker threads"""
        threading.Thread(target=self.process_update_queue, daemon=True).start()
        threading.Thread(target=self.process_conflict_queue, daemon=True).start()

    def process_update_queue(self):
        """Worker processes queue update chunk"""
        while True:
            try:
                task = self.update_queue.get()
                if task:
                    self.process_update_task(task)
                self.update_queue.task_done()
            except Exception as e:
                logger.error(f"Error processing update task: {str(e)}")

    def process_conflict_queue(self):
        """Worker processes queue checking for conflicts"""
        while True:
            try:
                task = self.conflict_check_queue.get()
                if task:
                    self.process_conflict_task(task)
                self.conflict_check_queue.task_done()
            except Exception as e:
                logger.error(f"Error processing conflict task: {str(e)}")

    def process_update_task(self, task):
        """Processing update chunk task"""
        try:
            if not task:
                self.logger.error("Empty update task received")
                return

            doc_id = task.get('doc_id')
            chunk_pairs = task.get('chunk_pairs', [])
            callback = task.get('callback')

            if not doc_id or not chunk_pairs:
                self.logger.error(f"Invalid task data: doc_id={doc_id}, chunks={len(chunk_pairs) if chunk_pairs else 0}")
                if callback:
                    callback("update", {
                        "status": "error",
                        "message": "Dữ liệu task không hợp lệ"
                    })
                return

            
            for pair in chunk_pairs:
                try:
                    success = self.chroma_manager.update_chunk(
                        chunk_id=pair.get('chunk_id'),
                        new_content=pair.get('new_content', '')
                    )
                    if success is not True:  
                        self.logger.error(f"Failed to update chunk {pair.get('chunk_id')}")
                        if callback:
                            callback("update", {
                                "status": "error",
                                "message": f"Không thể cập nhật chunk {pair.get('chunk_id')}"
                            })
                        return
                except Exception as e:
                    self.logger.error(f"Error updating chunk: {str(e)}")
                    if callback:
                        callback("update", {
                            "status": "error",
                            "message": f"Lỗi cập nhật: {str(e)}"
                        })
                    return

            self.logger.info(f"Successfully updated {len(chunk_pairs)} chunks for doc {doc_id}")
            if callback:
                callback("update", {
                    "status": "success",
                    "message": "Cập nhật thành công"
                })

        except Exception as e:
            self.logger.error(f"Error in process_update_task: {str(e)}")
            if callback:
                callback("update", {
                    "status": "error",
                    "message": f"Lỗi xử lý: {str(e)}"
                })

    def process_conflict_task(self, task):
        """Process full conflict checking task"""
        try:
            doc_id = task['doc_id']
            callback = task.get('callback')

            # Check internal and external conflicts
            updated_chunks = self.chroma_manager.get_chunks_by_document_id(doc_id)
            if not updated_chunks:
                logger.warning(f"No chunks found for document {doc_id}")
                if callback:
                    callback("conflict_check", {
                        "status": "error",
                        "message": "Không tìm thấy chunks để kiểm tra"
                    })
                return

            try:
                
                internal_conflicts = self.conflict_manager.analyze_internal_conflicts(updated_chunks)
                
                external_conflicts = []
                doc = self.data_manager.get_document_by_id(doc_id)
                if doc and doc.get('duplicate_group_id'):
                    group_docs = self.data_manager.get_documents_in_group(doc['duplicate_group_id'])
                    for group_doc in group_docs:
                        if group_doc['id'] != doc_id:
                            group_chunks = self.chroma_manager.get_chunks_by_document_id(group_doc['id'])
                            if group_chunks:
                                conflicts = self.conflict_manager.analyze_cross_document_conflicts(
                                    updated_chunks, group_chunks,
                                    doc_id, group_doc['id']
                                )
                                if conflicts:
                                    external_conflicts.extend(conflicts)

                # Update conflict info
                has_conflicts = bool(internal_conflicts or external_conflicts)
                conflict_info = {
                    "analyzed_at": datetime.now().isoformat(),
                    "internal_conflicts": [c.to_dict() for c in internal_conflicts if c.has_conflict],
                    "external_conflicts": [c.to_dict() for c in external_conflicts if c.has_conflict],
                    "message": "Full conflict analysis after content update"
                }

                # Update document status
                self.data_manager.update_document_status(doc_id, {
                    'has_conflicts': has_conflicts,
                    'conflict_info': json.dumps(conflict_info),
                    'conflict_status': 'Pending Review' if has_conflicts else 'No Conflict',
                    'last_conflict_check': datetime.now().isoformat()
                })

                logger.info(f"Completed conflict analysis for document {doc_id}")
                if callback:
                    callback("conflict_check", {
                        "status": "complete",
                        "has_conflicts": has_conflicts
                    })

            except Exception as analysis_error:
                error_message = str(analysis_error)
                # Escape % characters to avoid format specifier issues
                error_message = error_message.replace('%', '%%')
                logger.error(f"Error analyzing conflicts: {error_message}")
                logger.error(traceback.format_exc())
                if callback:
                    callback("conflict_check", {
                        "status": "error",
                        "message": f"Lỗi phân tích xung đột: {error_message}"
                    })

        except Exception as e:
            logger.error(f"Error processing conflict task: {str(e)}")
            if callback:
                callback("conflict_check", {
                    "status": "error",
                    "message": f"Lỗi xử lý task conflict: {str(e)}"
                })
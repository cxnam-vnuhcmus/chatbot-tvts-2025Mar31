import os
import queue
from flask import Flask, request, jsonify
from flask_cors import CORS
from concurrent.futures import ThreadPoolExecutor
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))) 
from datetime import datetime
import pandas as pd
import requests
import traceback
import logging
import threading
import time
import json
from dotenv import load_dotenv
load_dotenv()
from common.data_manager import DatabaseManager
from common.utils import ratio, preprocessing
from common.chroma_manager import ChromaManager 
from common.conflict_manager import ConflictManager

app = Flask(__name__)
CORS(app)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

data_manager = DatabaseManager()
chroma_manager = ChromaManager()

logger.info("Initializing ConflictManager with OpenAI")
conflict_manager = ConflictManager(data_manager, chroma_manager)

scan_queue = queue.Queue()
worker_pool = ThreadPoolExecutor(max_workers = 5)
KMS_PROCESSOR_API = os.getenv('KMS_PROCESSOR_API')


# def analyze_document_similarity(current_doc_id, current_content):
#     """
#     Analyzes the similarity between a given document and all other documents in the system.

#     Args:
#         current_doc_id (str): The unique ID of the document being analyzed.
#         current_content (str): The content of the current document to be compared.

#     Returns:
#         list: A sorted list of dictionaries representing similar documents. Each dictionary contains:
#             - id (str): The unique ID of the similar document.
#             - similarity (float): The similarity score (range: 0.0 to 1.0).
#             - content (str): The content of the similar document.
#             - created_date (str): The creation date of the similar document.

#     Raises:
#         Exception: If there is an error during the similarity analysis process.

#     """
#     try:
#         similar_docs = []
#         processed_current = preprocessing(current_content)
#         all_documents = data_manager.get_all_documents()

#         for _, doc in all_documents.iterrows():
#             if doc['id'] != current_doc_id:
#                 try:
#                     processed_doc = preprocessing(doc['content'])
#                     similarity = ratio(processed_current, processed_doc)
    
#                     if similarity > 0.995:
#                         similar_doc = {
#                             'id': str(doc['id']),
#                             'similarity': float(similarity),
#                             'content': str(doc['content']),
#                             'created_date': doc['created_date']
#                         }
#                         similar_docs.append(similar_doc)
                        
#                 except Exception as e:
#                     logger.error(f"Error comparing with document {doc['id']}: {str(e)}")
#                     continue

#         similar_docs.sort(key=lambda x: x['similarity'], reverse=True)
#         return similar_docs

#     except Exception as e:
#         logger.error(f"Error analyzing similarity: {str(e)}")
#         raise

def analyze_document_similarity(current_doc_id, current_content):
    """
    Analyzes the similarity between a given document and all other documents in the system.

    Args:
        current_doc_id (str): The unique ID of the document being analyzed.
        current_content (str): The content of the current document to be compared.

    Returns:
        list: A sorted list of dictionaries representing similar documents. Each dictionary contains:
            - id (str): The unique ID of the similar document.
            - similarity (float): The similarity score (range: 0.0 to 1.0).
            - content (str): The content of the similar document.
            - created_date (str): The creation date of the similar document.

    Raises:
        Exception: If there is an error during the similarity analysis process.

    """
    try:
        similar_docs = []
        processed_current = preprocessing(current_content)
        all_documents = data_manager.get_all_documents()

        def extract_numbers(text):
            import re
            return re.findall(r'\b\d+[.,]?\d*\b', text)
        
        def has_numeric_differences(text1, text2):
            numbers1 = extract_numbers(text1)
            numbers2 = extract_numbers(text2)
            
            if len(numbers1) != len(numbers2):
                return True
                
            for n1, n2 in zip(sorted(numbers1), sorted(numbers2)):
                if n1 != n2:
                    return True
                    
            return False

        current_numbers = extract_numbers(current_content)

        for _, doc in all_documents.iterrows():
            if doc['id'] != current_doc_id:
                try:
                    processed_doc = preprocessing(doc['content'])
                    similarity = ratio(processed_current, processed_doc)
                    
                    if similarity > 0.99:
                        doc_numbers = extract_numbers(doc['content'])
                        
                        if has_numeric_differences(current_content, doc['content']):
                            similarity = 0.97
    
                    if similarity > 0.995:  
                        similar_doc = {
                            'id': str(doc['id']),
                            'similarity': float(similarity),
                            'content': str(doc['content']),
                            'created_date': doc['created_date']
                        }
                        similar_docs.append(similar_doc)
                        
                except Exception as e:
                    logger.error(f"Error comparing with document {doc['id']}: {str(e)}")
                    continue

        similar_docs.sort(key=lambda x: x['similarity'], reverse=True)
        return similar_docs

    except Exception as e:
        logger.error(f"Error analyzing similarity: {str(e)}")
        raise


def send_to_processor(doc_id, is_duplicate=False, duplicate_info=None, max_retries=3, retry_delay=5):
    """
    Sends a document to the processing service with improved retry logic
    
    Args:
        doc_id (str): The ID of the document to be processed
        is_duplicate (bool): Flag indicating if document is duplicate
        duplicate_info (dict): Additional duplicate information
        max_retries (int): Maximum number of retry attempts
        retry_delay (int): Delay between retries in seconds
        
    Returns:
        bool: Success status
    """
    if is_duplicate:
        logger.info(f"Skip duplicate document processing {doc_id}")
        return True
        
    processor_api = KMS_PROCESSOR_API
    if not processor_api:
        return False
        
    logger.info(f"Send document {doc_id} to processor API: {processor_api}")
    
    for attempt in range(max_retries):
        try:
            doc = data_manager.get_document_by_id(doc_id)
            if not doc:
                logger.info(f"Send document {doc_id} to processor API: {processor_api}")
                return False

            url = f"{processor_api}/process_doc"
            payload = {
                "doc_id": doc_id,
                "duplicate_info": duplicate_info
            }
            
            logger.info(f"Send request to {url} with payload: {json.dumps(payload)}")
            
            response = requests.post(
                url,
                json=payload,
                timeout=15  
            )
            
            logger.info(f"Processor API response: Status={response.status_code}, Content={response.text[:100]}...")
            
            if response.status_code >= 200 and response.status_code < 300:
                logger.info(f"Successfully sent document {doc_id} to processor (Times {attempt + 1}/{max_retries})")
                
                try:
                    data_manager.update_document_status(doc_id, {
                        'processing_status': 'Processing',
                        'scan_status': 'Completed'
                    })
                except Exception as update_error:
                    logger.error(f"Error updating status after sending to processor: {str(update_error)}")
                
                return True
            else:
                logger.warning(f"Processor API returned error code: {response.status_code}, content: {response.text}")
                
        except requests.ConnectionError as e:
            if attempt < max_retries - 1:
                wait_time = retry_delay * (2 ** attempt) 
                logger.info(f"Try again in {wait_time} seconds...")
                time.sleep(wait_time)
            continue
            
        except Exception as e:
            logger.error(traceback.format_exc())
            if attempt < max_retries - 1:
                wait_time = retry_delay * (2 ** attempt) 
                time.sleep(wait_time)
            continue
            
    logger.error(f"Failed to send document {doc_id} to processor after {max_retries} attempts")
    
    try:
        data_manager.update_document_status(doc_id, {
            'processing_status': 'Failed',
            'error_message': f"Không thể gửi đến bộ xử lý sau {max_retries} lần thử"
        })
    except Exception as update_error:
        logger.error(f"Error when updating status failed: {str(update_error)}")
    
    return False


@app.route('/batch_analyze_conflicts', methods=['POST'])
def batch_analyze_conflicts():
    """
    API to scan and analyze conflicts for all documents that haven't been analyzed in the system.
    Supports batch processing with a limit on the number of documents processed at once.
    """
    try:
        data = request.json or {}
        batch_size = data.get('batch_size', 50)  
        chunk_status = data.get('chunk_status', 'Chunked')  
        
        with data_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, is_duplicate, duplicate_group_id, original_chunked_doc
                    FROM documents 
                    WHERE (conflict_analysis_status IS NULL 
                           OR conflict_analysis_status = 'NotAnalyzed' 
                           OR conflict_analysis_status = 'AnalysisFailed')
                    AND chunk_status = %s
                    AND is_valid = true
                    LIMIT %s
                """, (chunk_status, batch_size))
                
                doc_results = cursor.fetchall()
                doc_ids = []
                duplicate_docs = {}
                original_docs = []
                
                for row in doc_results:
                    doc_id, is_duplicate, duplicate_group_id, original_chunked_doc = row
                    doc_ids.append(doc_id)
                    
                    if is_duplicate and original_chunked_doc:
                        if original_chunked_doc not in duplicate_docs:
                            duplicate_docs[original_chunked_doc] = []
                        duplicate_docs[original_chunked_doc].append(doc_id)
                    else:
                        original_docs.append(doc_id)

        if not doc_ids:
            return jsonify({
                'status': 'success',
                'message': 'No documents found that need conflict analysis',
                'count': 0
            }), 200

        analyzed_count = 0
        failed_docs = []
        skipped_docs = []
        duplicate_count = 0
        
        for doc_id in original_docs:
            try:
                data_manager.update_document_status(doc_id, {
                    'conflict_analysis_status': 'Analyzing',
                    'modified_date': datetime.now().isoformat()
                })
                
                logger.info(f"Analyzing conflicts for original document {doc_id}")
                conflict_result = conflict_manager.analyze_document(doc_id)
                
                has_conflicts = conflict_result.get('has_conflicts', False)
                conflict_status = "Pending Review" if has_conflicts else "No Conflict"
                
                data_manager.update_document_status(doc_id, {
                    'has_conflicts': has_conflicts,
                    'conflict_info': json.dumps(conflict_result),
                    'conflict_status': conflict_status,
                    'last_conflict_check': datetime.now().isoformat(),
                    'conflict_analysis_status': 'Analyzed'
                })
                
                analyzed_count += 1
                
                if doc_id in duplicate_docs:
                    for dup_id in duplicate_docs[doc_id]:
                        try:
                            data_manager.update_document_status(dup_id, {
                                'has_conflicts': has_conflicts,
                                'conflict_info': json.dumps(conflict_result),
                                'conflict_status': conflict_status,
                                'last_conflict_check': datetime.now().isoformat(),
                                'conflict_analysis_status': 'Analyzed'
                            })
                            duplicate_count += 1
                        except Exception as dup_error:
                            logger.error(f"Error syncing conflicts for duplicate document {dup_id}: {str(dup_error)}")
                            failed_docs.append({
                                'doc_id': dup_id,
                                'error': f"Sync error: {str(dup_error)}"
                            })
                
            except Exception as e:
                error_msg = str(e).replace('%', '%%')
                logger.error(f"Error analyzing conflicts for document {doc_id}: {error_msg}")
                logger.error(traceback.format_exc())
                
                failed_docs.append({
                    'doc_id': doc_id,
                    'error': str(e)
                })
                
                data_manager.update_document_status(doc_id, {
                    'conflict_analysis_status': 'AnalysisFailed',
                    'conflict_analysis_error': str(e)
                })
        
        remaining_duplicates = set(doc_ids) - set(original_docs) - set(sum(duplicate_docs.values(), []))
        
        for doc_id in remaining_duplicates:
            try:
                document = data_manager.get_document_by_id(doc_id)
                original_doc_id = document.get('original_chunked_doc')
                
                if original_doc_id:
                    original_doc = data_manager.get_document_by_id(original_doc_id)
                    
                    if original_doc and original_doc.get('conflict_analysis_status') == 'Analyzed':
                        conflict_info = original_doc.get('conflict_info')
                        has_conflicts = original_doc.get('has_conflicts', False)
                        conflict_status = original_doc.get('conflict_status', 'No Conflict')
                        
                        data_manager.update_document_status(doc_id, {
                            'has_conflicts': has_conflicts,
                            'conflict_info': conflict_info,
                            'conflict_status': conflict_status,
                            'last_conflict_check': datetime.now().isoformat(),
                            'conflict_analysis_status': 'Analyzed'
                        })
                        
                        duplicate_count += 1
                    else:
                        skipped_docs.append({
                            'doc_id': doc_id,
                            'reason': f"Original document {original_doc_id} not yet analyzed"
                        })
                else:
                    data_manager.update_document_status(doc_id, {
                        'conflict_analysis_status': 'Analyzing'
                    })
                    
                    conflict_result = conflict_manager.analyze_document(doc_id)
                    
                    has_conflicts = conflict_result.get('has_conflicts', False)
                    conflict_status = "Pending Review" if has_conflicts else "No Conflict"
                    
                    data_manager.update_document_status(doc_id, {
                        'has_conflicts': has_conflicts,
                        'conflict_info': json.dumps(conflict_result),
                        'conflict_status': conflict_status,
                        'last_conflict_check': datetime.now().isoformat(),
                        'conflict_analysis_status': 'Analyzed'
                    })
                    
                    analyzed_count += 1
            except Exception as e:
                logger.error(f"Error processing duplicate document {doc_id}: {str(e)}")
                failed_docs.append({
                    'doc_id': doc_id,
                    'error': str(e)
                })
                
                data_manager.update_document_status(doc_id, {
                    'conflict_analysis_status': 'AnalysisFailed',
                    'conflict_analysis_error': str(e)
                })
                
        duplicate_groups = set()
        for doc_id in doc_ids:
            try:
                document = data_manager.get_document_by_id(doc_id)
                if document and document.get('duplicate_group_id'):
                    duplicate_groups.add(document.get('duplicate_group_id'))
            except Exception:
                pass
                
        for group_id in duplicate_groups:
            try:
                conflict_manager.sync_group_conflicts_by_group(group_id)
            except Exception as sync_error:
                logger.error(f"Error syncing conflicts for group {group_id}: {str(sync_error)}")
        
        with data_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT COUNT(*) FROM documents 
                    WHERE (conflict_analysis_status IS NULL 
                           OR conflict_analysis_status = 'NotAnalyzed'
                           OR conflict_analysis_status = 'AnalysisFailed')
                    AND chunk_status = %s
                    AND is_valid = true
                """, (chunk_status,))
                remaining_count = cursor.fetchone()[0]
        
        return jsonify({
            'status': 'success',
            'analyzed_count': analyzed_count,
            'duplicate_count': duplicate_count,
            'failed_count': len(failed_docs),
            'skipped_count': len(skipped_docs),
            'failed_documents': failed_docs,
            'skipped_documents': skipped_docs,
            'remaining_count': remaining_count,
            'message': f'Analyzed {analyzed_count} original documents and synchronized {duplicate_count} duplicate documents'
        }), 200
        
    except Exception as e:
        error_msg = f"Error in batch conflict analysis: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        return jsonify({
            'status': 'error',
            'message': error_msg
        }), 500


@app.route('/rescan_null_status', methods=['POST'])
def rescan_null_status():
    try:
        documents = data_manager.get_documents_need_rescan()
        
        if documents.empty:
            return jsonify({
                'status': 'success',
                'message': 'Không có documents nào cần rescan',
                'count': 0
            }), 200

        queued_count = 0
        failed_docs = []
        
        for _, doc in documents.iterrows():
            try:
                doc_id = doc['id']
                
                scan_queue.put(doc_id)
                
                try:
                    with data_manager.get_connection() as conn:
                        with conn.cursor() as cursor:
                            cursor.execute("""
                                CREATE OR REPLACE FUNCTION prepare_for_rescan(p_doc_id VARCHAR) 
                                RETURNS BOOLEAN AS $$
                                DECLARE
                                    v_current_scan_status VARCHAR;
                                    v_current_proc_status VARCHAR;
                                BEGIN
                                    SELECT scan_status, processing_status INTO v_current_scan_status, v_current_proc_status
                                    FROM documents 
                                    WHERE id = p_doc_id;
                                    
                                    IF v_current_scan_status = 'ScanFailed' THEN
                                        -- Từ ScanFailed -> Processing -> Queued
                                        UPDATE documents 
                                        SET processing_status = 'Processing',
                                            scan_status = 'Processing', 
                                            modified_date = CURRENT_TIMESTAMP 
                                        WHERE id = p_doc_id;
                                        
                                        UPDATE documents 
                                        SET processing_status = 'Queued', 
                                            scan_status = 'Queued',
                                            modified_date = CURRENT_TIMESTAMP 
                                        WHERE id = p_doc_id;
                                    ELSIF v_current_scan_status IS NULL OR v_current_scan_status = '' OR v_current_scan_status = 'Pending' THEN 
                                        
                                        UPDATE documents 
                                        SET processing_status = 'Processing', 
                                            scan_status = 'Processing',
                                            modified_date = CURRENT_TIMESTAMP 
                                        WHERE id = p_doc_id;
                                        
                                        UPDATE documents 
                                        SET processing_status = 'Queued', 
                                            scan_status = 'Queued',
                                            modified_date = CURRENT_TIMESTAMP 
                                        WHERE id = p_doc_id;
                                    ELSE
                                        
                                        EXECUTE 'UPDATE documents SET processing_status = ''Queued'', scan_status = ''Queued'', modified_date = CURRENT_TIMESTAMP WHERE id = $1'
                                        USING p_doc_id;
                                    END IF;
                                    
                                    RETURN TRUE;
                                END;
                                $$ LANGUAGE plpgsql;
                            """)
                            conn.commit()
                            
                            cursor.execute("SELECT prepare_for_rescan(%s)", (doc_id,))
                            result = cursor.fetchone()[0]
                            conn.commit()
                    
                    queued_count += 1
                    logger.info(f"Đã queue document {doc_id} để scan lại")
                except Exception as update_error:
                    logger.error(f"Lỗi cập nhật trạng thái: {str(update_error)}")
                    queued_count += 1  
                    failed_docs.append({
                        'doc_id': doc_id,
                        'error': 'Document đã thêm vào queue nhưng không cập nhật được trạng thái'
                    })
                
            except Exception as e:
                logger.error(f"Lỗi khi queue document {doc_id}: {str(e)}")
                failed_docs.append({
                    'doc_id': doc_id,
                    'error': str(e)
                })

        response = {
            'status': 'success',
            'total_documents': len(documents),
            'queued_count': queued_count,
            'failed_count': len(failed_docs),
            'failed_documents': failed_docs,
            'message': f'Đã queue {queued_count} documents để scan lại'
        }

        if failed_docs:
            return jsonify(response), 207 
        return jsonify(response), 200

    except Exception as e:
        error_msg = f"Lỗi khi rescan documents: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        return jsonify({
            'status': 'error',
            'message': error_msg
        }), 500

def check_and_rescan_documents():
    """
    Check and queue documents to be rescanned on startup.
    """
    try:
        
        documents = data_manager.get_documents_need_rescan()
        
        if documents is None or documents.empty:
            logger.info("No documents need to be rescanned on startup")
            return
            
        logger.info(f"Found {len(documents)} documents that need to be rescanned")
        count = 0
        
        for _, doc in documents.iterrows():
            try:
                doc_id = doc['id']
                current_scan_status = doc.get('scan_status')
                
                logger.info(f"Processing document to be rescanned: {doc_id} (status: {current_scan_status})")
                scan_queue.put(doc_id)
                
                try:
                    with data_manager.get_connection() as conn:
                        with conn.cursor() as cursor:
                            cursor.execute("""
                                BEGIN;
                                SET session_replication_role = 'replica';
                                
                                UPDATE documents 
                                SET processing_status = 'Queued',
                                    scan_status = 'Queued',
                                    modified_date = CURRENT_TIMESTAMP,
                                    chunk_failure_count = 0
                                WHERE id = %s;
                                
                                SET session_replication_role = 'origin';
                                COMMIT;
                            """, (doc_id,))
                            conn.commit()
                            logger.info(f"Đã cập nhật trạng thái tài liệu {doc_id} thành Queued")
                            
                except Exception as update_error:
                    logger.warning(f"Error updating status for document {doc_id}: {str(update_error)}")
                    logger.warning("Continue processing documents in queue despite status update error")
                count += 1
                logger.info(f"Document {doc_id} placed in queue for rescan (#{count})")
                
            except Exception as e:
                logger.error(f"Error while enqueuing document {doc.get('id', 'unknown')}: {str(e)}")
                logger.error(traceback.format_exc())
                
        logger.info(f"Successfully put {count}/{len(documents)} documents into the queue for rescan on startup")
    except Exception as e:
        logger.error(f"Overall error while checking and rescanning document: {str(e)}")
        logger.error(traceback.format_exc())
        
        
@app.route('/check_document_status/<doc_id>', methods=['GET'])
def check_document_status(doc_id):
    try:
        document = data_manager.get_document_by_id(doc_id)
        if not document:
            return jsonify({
                'status': 'error',
                'message': f'Không tìm thấy document với ID {doc_id}'
            }), 404
        
        document_info = {
            'id': document['id'],
            'processing_status': document.get('processing_status'),
            'scan_status': document.get('scan_status'),
            'chunk_status': document.get('chunk_status'),
            'is_valid': document.get('is_valid'),
            'error_message': document.get('error_message'),
            'modified_date': document.get('modified_date'),
            'chunk_failure_count': document.get('chunk_failure_count')
        }
        
        needs_rescan = False
        rescan_reason = []
        
        if document_info['chunk_status'] == 'ChunkingFailed':
            needs_rescan = True
            rescan_reason.append('Document has status ChunkingFailed')
            
        if not document_info['scan_status']:
            needs_rescan = True
            rescan_reason.append('scan_status is empty')
            
        if document_info['scan_status'] == 'ScanFailed':
            needs_rescan = True
            rescan_reason.append('Document has ScanFailed status')
            
        document_info['needs_rescan'] = needs_rescan
        document_info['rescan_reason'] = rescan_reason
        
        return jsonify({
            'status': 'success',
            'data': document_info
        }), 200
        
    except Exception as e:
        error_msg = f"Error checking document status: {str(e)}"
        logger.error(error_msg)
        return jsonify({
            'status': 'error',
            'message': error_msg
        }), 500

@app.route('/list_all_documents', methods=['GET'])
def list_all_documents():
    try:
        documents = data_manager.get_all_documents()
        
        document_list = []
        for _, row in documents.iterrows():
            doc = {
                'id': row['id'],
                'processing_status': row.get('processing_status'),
                'scan_status': row.get('scan_status'),
                'chunk_status': row.get('chunk_status'),
                'is_valid': row.get('is_valid'),
                'error_message': row.get('error_message'),
                'modified_date': row.get('modified_date'),
                'chunk_failure_count': row.get('chunk_failure_count')
            }
            document_list.append(doc)
            
        return jsonify({
            'status': 'success',
            'total_documents': len(document_list),
            'documents': document_list
        }), 200
        
    except Exception as e:
        error_msg = f"Lỗi khi lấy danh sách documents: {str(e)}"
        logger.error(error_msg)
        return jsonify({
            'status': 'error',
            'message': error_msg
        }), 500

@app.route('/rescan_failed_chunks', methods=['POST'])
def rescan_failed_chunks():
    try:
        failed_docs = []
        
        with data_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id FROM documents 
                    WHERE chunk_status = 'ChunkingFailed'
                    AND is_valid = true
                    AND approval_status = 'Pending'
                """)
                doc_ids = [row[0] for row in cursor.fetchall()]
        
        if not doc_ids:
            return jsonify({
                'status': 'success',
                'message': 'No documents with chunking failures found',
                'count': 0
            }), 200

        queued_count = 0
        
        for doc_id in doc_ids:
            try:
                scan_queue.put(doc_id)
                
                try:
                    with data_manager.get_connection() as conn:
                        with conn.cursor() as cursor:
                            cursor.execute("""
                                CREATE OR REPLACE FUNCTION prepare_failed_chunk_for_rescan(p_doc_id VARCHAR) 
                                RETURNS BOOLEAN AS $
                                DECLARE
                                    v_current_status VARCHAR;
                                BEGIN
                                    SELECT chunk_status INTO v_current_status 
                                    FROM documents 
                                    WHERE id = p_doc_id;
                                    
                                    IF v_current_status = 'ChunkingFailed' THEN
                                        UPDATE documents 
                                        SET processing_status = 'Processing',
                                            scan_status = 'Processing',
                                            chunk_status = 'Pending',
                                            modified_date = CURRENT_TIMESTAMP 
                                        WHERE id = p_doc_id;
                                        
                                        UPDATE documents 
                                        SET processing_status = 'Queued',
                                            scan_status = 'Queued',
                                            modified_date = CURRENT_TIMESTAMP 
                                        WHERE id = p_doc_id;
                                    ELSE
                                        EXECUTE 'UPDATE documents SET processing_status = ''Queued'', scan_status = ''Queued'', chunk_status = ''Pending'', modified_date = CURRENT_TIMESTAMP WHERE id = $1'
                                        USING p_doc_id;
                                    END IF;
                                    
                                    RETURN TRUE;
                                END;
                                $ LANGUAGE plpgsql;
                            """)
                            conn.commit()
                            
                            cursor.execute("SELECT prepare_failed_chunk_for_rescan(%s)", (doc_id,))
                            result = cursor.fetchone()[0]
                            conn.commit()
                    
                    queued_count += 1
                    logger.info(f"Queued document {doc_id} for rescan")
                except Exception as update_error:
                    logger.error(f"Error updating status: {str(update_error)}")
                    queued_count += 1  
                    failed_docs.append({
                        'doc_id': doc_id,
                        'error': 'Document đã thêm vào queue nhưng không cập nhật được trạng thái'
                    })
                
            except Exception as e:
                logger.error(f"Error queuing document {doc_id}: {str(e)}")
                failed_docs.append({
                    'doc_id': doc_id,
                    'error': str(e)
                })

        response = {
            'status': 'success',
            'queued_count': queued_count,
            'failed_count': len(failed_docs),
            'failed_documents': failed_docs,
            'message': f'Queued {queued_count} documents for rescan'
        }

        if failed_docs:
            return jsonify(response), 207 
        return jsonify(response), 200

    except Exception as e:
        error_msg = f"Error initiating rescan: {str(e)}"
        logger.error(error_msg)
        return jsonify({
            'status': 'error',
            'message': error_msg
        }), 500

@app.route('/chunk_callback', methods=['POST'])
def chunk_callback():
    """
    Handle callback when chunk creation is complete.
    Update document status and analyze conflicts if necessary.
    """
    try:
        data = request.json
        if not data or 'doc_id' not in data:
            return jsonify({'error': 'Thiếu doc_id'}), 400
            
        doc_id = data['doc_id']
        chunk_status = data.get('chunk_status')
        error_message = data.get('error_message')

        document = data_manager.get_document_by_id(doc_id)
        if not document:
            return jsonify({'error': 'Không tìm thấy tài liệu'}), 404

        if chunk_status == 'success':
            status_update = {
                'chunk_status': 'Chunked',
                'processing_status': 'Processed'
            }
            
            duplicate_group_id = document.get('duplicate_group_id')
            if duplicate_group_id:
                group_docs = data_manager.get_documents_in_group(duplicate_group_id)
                original_doc = min(group_docs, key=lambda x: x.get('created_date', ''))
                
                if document['id'] == original_doc['id']:
                    logger.info(f"Document {doc_id} is original in duplicate group {duplicate_group_id}")
                    data_manager.update_document_status(doc_id, status_update)
                    
                    for group_doc in group_docs:
                        if group_doc['id'] != original_doc['id']:
                            data_manager.update_document_status(group_doc['id'], {
                                'chunk_status': 'NotRequired',
                                'processing_status': 'Duplicate',
                                'original_chunked_doc': original_doc['id']
                            })
                else:
                    logger.info(f"Document {doc_id} is duplicate in group {duplicate_group_id}")
                    data_manager.update_document_status(doc_id, {
                        'chunk_status': 'NotRequired',
                        'processing_status': 'Duplicate',
                        'original_chunked_doc': original_doc['id']
                    })
            else:
                data_manager.update_document_status(doc_id, status_update)
            
            data_manager.update_document_status(doc_id, {
                'conflict_analysis_status': 'Analyzing'
            })
            
            def analyze_conflicts_async(doc_id, duplicate_group_id):
                try:
                    logger.info(f"Starting conflict analysis for document {doc_id}")
                    conflict_result = conflict_manager.analyze_document(doc_id)
                    
                    has_conflicts = conflict_result.get('has_conflicts', False)
                    conflict_status = "Pending Review" if has_conflicts else "No Conflict"
                    
                    data_manager.update_document_status(doc_id, {
                        'has_conflicts': has_conflicts,
                        'conflict_info': json.dumps(conflict_result),
                        'conflict_status': conflict_status,
                        'last_conflict_check': datetime.now().isoformat(),
                        'conflict_analysis_status': 'Analyzed'
                    })
                    
                    logger.info(f"Completing conflict analysis for document {doc_id}")
                    
                    if duplicate_group_id:
                        try:
                            logger.info(f"Sync conflicts for duplicate group {duplicate_group_id}")
                            conflict_manager.sync_group_conflicts(doc_id)
                            logger.info(f"Complete sync conflicts for group {duplicate_group_id}")
                        except Exception as sync_error:
                            logger.error(f"Error while syncing conflicts for group {duplicate_group_id}: {str(sync_error)}")
                            logger.error(traceback.format_exc())
                    
                except Exception as conflict_error:
                    logger.error(traceback.format_exc())
                    
                    data_manager.update_document_status(doc_id, {
                        'conflict_analysis_status': 'Failed',
                        'conflict_analysis_error': str(conflict_error)
                    })
            
            conflict_thread = threading.Thread(
                target=analyze_conflicts_async, 
                args=(doc_id, duplicate_group_id)
            )
            conflict_thread.daemon = True
            conflict_thread.start()
                
        else:
            status_update = {
                'chunk_status': 'ChunkingFailed',
                'processing_status': 'Failed', 
                'error_message': error_message,
                'conflict_analysis_status': 'NotRequired'
            }
            data_manager.update_document_status(doc_id, status_update)

        return jsonify({'status': 'success'}), 200

    except Exception as e:
        logger.error(f"Error processing chunk callback: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({'error': str(e)}), 500

@app.route('/check_conflict_status/<doc_id>', methods=['GET'])
def check_conflict_status(doc_id):
    """
    Check the conflict analysis status of a document
    """
    try:
        document = data_manager.get_document_by_id(doc_id)
        if not document:
            return jsonify({
                'status': 'error',
                'message': f'Không tìm thấy tài liệu {doc_id}'
            }), 404
            
        conflict_status = {
            'doc_id': doc_id,
            'conflict_analysis_status': document.get('conflict_analysis_status', 'NotStarted'),
            'conflict_status': document.get('conflict_status', 'No Conflict'),
            'has_conflicts': document.get('has_conflicts', False),
            'conflict_info': document.get('conflict_info'),
            'conflict_analysis_error': document.get('conflict_analysis_error'),
            'duplicate_group_id': document.get('duplicate_group_id'),
            'last_conflict_check': document.get('last_conflict_check')
        }
        
        conflict_info = document.get('conflict_info')
        if conflict_info:
            if isinstance(conflict_info, str):
                try:
                    conflict_info = json.loads(conflict_info)
                except json.JSONDecodeError:
                    pass
                    
            if isinstance(conflict_info, dict):
                content_conflicts = conflict_info.get('content_conflicts', [])
                internal_conflicts = conflict_info.get('internal_conflicts', [])
                external_conflicts = conflict_info.get('external_conflicts', [])
                
                conflict_status['conflict_counts'] = {
                    'content': len(content_conflicts),
                    'internal': len(internal_conflicts),
                    'external': len(external_conflicts),
                    'total': len(content_conflicts) + len(internal_conflicts) + len(external_conflicts)
                }
        
        return jsonify({
            'status': 'success',
            'data': conflict_status
        })
        
    except Exception as e:
        logger.error(f"Error checking for conflicting state: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/sync_group_conflicts/<doc_id>', methods=['POST'])
def sync_group_conflicts(doc_id):
    """
    Synchronize conflicting information for all documents in the same duplicate group
    """
    try:
        document = data_manager.get_document_by_id(doc_id)
        if not document:
            return jsonify({
                'status': 'error',
                'message': f'Không tìm thấy tài liệu {doc_id}'
            }), 404
            
        duplicate_group_id = document.get('duplicate_group_id')
        if not duplicate_group_id:
            return jsonify({
                'status': 'error',
                'message': f'Tài liệu {doc_id} không thuộc nhóm trùng lặp nào'
            }), 400
            
        result = conflict_manager.sync_group_conflicts(doc_id)
        
        return jsonify({
            'status': 'success',
            'message': f'Đã đồng bộ hóa mâu thuẫn cho nhóm {duplicate_group_id}',
            'result': result
        })
        
    except Exception as e:
        logger.error(f"Error while synchronizing conflict: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500
        
def add_conflict_columns():
    try:
        data_manager = DatabaseManager()
        with data_manager.get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'documents' AND column_name = 'conflict_analysis_status'
                    ) THEN
                        ALTER TABLE documents ADD COLUMN conflict_analysis_status VARCHAR(50) DEFAULT 'NotAnalyzed';
                    END IF;
                    
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'documents' AND column_name = 'conflict_analysis_error'
                    ) THEN
                        ALTER TABLE documents ADD COLUMN conflict_analysis_error TEXT;
                    END IF;
                    
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint WHERE conname = 'valid_analysis_status'
                    ) THEN
                        ALTER TABLE documents ADD CONSTRAINT valid_analysis_status 
                            CHECK (conflict_analysis_status IN (
                                'NotAnalyzed', 'Analyzing', 'Analyzed',
                                'AnalysisFailed', 'AnalysisInvalidated'
                            ));
                    END IF;
                END $$;
                """)
        
        return True
    except Exception as e:
        return False

def scan_worker():
    worker_id = threading.get_ident()
    logger.info(f"Start scanning worker with ID: {worker_id}")
    
    while True:
        doc_id = None
        try:
            doc_id = scan_queue.get(timeout=1)
            logger.info(f"Worker {worker_id} received document {doc_id} from queue")
            
            doc = data_manager.get_document_by_id(doc_id)
            if not doc:
                logger.error(f"Worker {worker_id}: Document {doc_id} not found in database")
                scan_queue.task_done()
                continue

            valid_states = ['Processed', 'Queued', 'Edited', 'Pending', 'Failed', 'Processing']
            current_status = doc.get('processing_status')
            current_scan_status = doc.get('scan_status')
            
            logger.info(f"Worker {worker_id}: Document {doc_id} has current status: processing={current_status}, scan={current_scan_status}")
            if current_status not in valid_states:
                logger.warning(f"Worker {worker_id}: Document {doc_id} has invalid status {current_status}, skipping")
                scan_queue.task_done()
                continue

            try:
                with data_manager.get_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("""
                            BEGIN;
                            SET session_replication_role = 'replica';
                            
                            UPDATE documents
                            SET processing_status = 'Scanning',
                                scan_status = 'Scanning',
                                modified_date = CURRENT_TIMESTAMP
                            WHERE id = %s;
                            
                            SET session_replication_role = 'origin';
                            COMMIT;
                        """, (doc_id,))
                        conn.commit()
                        logger.info(f"Worker {worker_id}: Updated document {doc_id} status to Scanning")
            except Exception as e:
                logger.error(f"Worker {worker_id}: Error updating Scanning status for document {doc_id}: {str(e)}")

            try:
                scan_document(doc_id)
                logger.info(f"Worker {worker_id}: Finished scanning document {doc_id}")
            except Exception as scan_error:
                logger.error(f"Worker {worker_id}: Error scanning document {doc_id}: {str(scan_error)}")
                logger.error(traceback.format_exc())
                try:
                    check_and_handle_document_failure(doc_id, f"Scan error: {str(scan_error)}")
                except Exception as update_error:
                    logger.error(f"Worker {worker_id}: Error while updating status failed for document {doc_id}: {str(update_error)}")
                    
            scan_queue.task_done()
            
        except queue.Empty:
            time.sleep(1)
            continue
        
        except Exception as e:
            logger.error(f"Worker {worker_id}: Unknown error while processing document {doc_id if doc_id else 'unknown'}: {str(e)}")
            logger.error(traceback.format_exc())
            
            if doc_id:
                try:
                    check_and_handle_document_failure(doc_id, f"Unknown error: {str(e)}")
                except Exception as update_error:
                    logger.error(f"Worker {worker_id}: Error when updating status failed: {str(update_error)}")
                scan_queue.task_done()
            
            time.sleep(2)


@app.route('/scan_doc', methods=['POST'])
def scan_doc():
    """
    API endpoint to receive document scanning requests and queue them
    """
    try:
        data = request.json
        if not data or 'doc_id' not in data:
            logger.error("Thiếu doc_id trong request")
            return jsonify({'error': 'Thiếu doc_id'}), 400
            
        doc_id = data['doc_id']
        logger.info(f"Receive request to scan document with ID: {doc_id}")
        
        doc = data_manager.get_document_by_id(doc_id)
        if not doc:
            logger.error(f"Document {doc_id} not found in database")
            return jsonify({'error': 'Tài liệu không tồn tại', 'doc_id': doc_id}), 404

        logger.info(f"Document {doc_id} currently has status: processing={doc.get('processing_status')}, scan={doc.get('scan_status')}")
        queue_size = scan_queue.qsize()
        logger.info(f"Current queue size: {queue_size}")
        scan_queue.put(doc_id)
        logger.info(f"Added document {doc_id} to scan queue. New queue size: {scan_queue.qsize()}")
        
        try:
            with data_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        BEGIN;
                        SET session_replication_role = 'replica';
                        
                        UPDATE documents 
                        SET processing_status = 'Queued',
                            scan_status = 'Queued',
                            modified_date = CURRENT_TIMESTAMP 
                        WHERE id = %s;
                        
                        SET session_replication_role = 'origin';
                        COMMIT;
                    """, (doc_id,))
                    conn.commit()
                    logger.info(f"Updated document status {doc_id} to Queued")
        except Exception as update_error:
            logger.error(f"Error updating document status {doc_id}: {str(update_error)}")
            logger.error(traceback.format_exc())
        
        return jsonify({
            'status': 'success',
            'message': f'Tài liệu {doc_id} đã được đưa vào hàng đợi để quét',
            'queue_size': scan_queue.qsize()
        }), 200
        
    except Exception as e:
        logger.error(f"Error processing document scan request: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({'error': str(e)}), 500

def check_and_handle_document_failure(doc_id, error_message):
    """
    Update document status to Failed, ignoring all constraints

    Args:
    doc_id (str): ID of the document
    error_message (str): Error message
    """
    try:
        with data_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    BEGIN;
                    SET session_replication_role = 'replica';
                    
                    UPDATE documents 
                    SET processing_status = 'Failed', 
                        scan_status = 'ScanFailed',
                        error_message = %s,
                        modified_date = CURRENT_TIMESTAMP 
                    WHERE id = %s;
                    
                    SET session_replication_role = 'origin';
                    COMMIT;
                """, (error_message, doc_id))
                conn.commit()
                logger.info(f"Updated failure status for document {doc_id}")
    except Exception as update_error:
        logger.error(f"Error handling failure for document {doc_id}: {str(update_error)}")

def update_document_status_direct(doc_id, status_data):
    """
    Update document status directly, bypassing constraints

    Args:
    doc_id (str): ID of the document
    status_data (dict): Status data to update
    """
    try:
        for key, value in status_data.items():
            if key in ['conflict_info', 'similar_documents'] and isinstance(value, (dict, list)):
                status_data[key] = json.dumps(value)
        
        set_clauses = []
        values = []
        for key, value in status_data.items():
            set_clauses.append(f"{key} = %s")
            values.append(value)
        
        set_sql = ", ".join(set_clauses)
        if set_sql:
            set_sql += ", "
        set_sql += "modified_date = CURRENT_TIMESTAMP"
        
        values.append(doc_id) 
        
        with data_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    BEGIN;
                    SET session_replication_role = 'replica';
                    
                    UPDATE documents 
                    SET {set_sql}
                    WHERE id = %s;
                    
                    SET session_replication_role = 'origin';
                    COMMIT;
                """, values)
                conn.commit()
                logger.info(f"Updated status for document {doc_id}")
                return True
    except Exception as e:
        logger.error(f"Error updating document status: {str(e)}")
        return False

def scan_document(doc_id, retry_count=0):
    """
    Scan the document and send it to the processor with improved error handling and logging

    Args:
    doc_id (str): ID of the document to scan
    retry_count (int): Number of retries (for recursion)

    Returns:
    bool: True on success, False on failure
    """
    try:
        logger.info(f"Starting scanning document {doc_id} (retry counts: {retry_count})")
        document = data_manager.get_document_by_id(doc_id)
        if not document:
            logger.error(f"Document not found: {doc_id}")
            return False

        current_processing_status = document.get('processing_status')
        current_scan_status = document.get('scan_status')
        
        logger.info(f"Current status of {doc_id}: processing={current_processing_status}, scan={current_scan_status}")

        try:
            with data_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        BEGIN;
                        SET session_replication_role = 'replica';
                        
                        UPDATE documents
                        SET processing_status = 'Scanning',
                            scan_status = 'Scanning',
                            modified_date = CURRENT_TIMESTAMP
                        WHERE id = %s;
                        
                        SET session_replication_role = 'origin';
                        COMMIT;
                    """, (doc_id,))
                    conn.commit()
                    logger.info(f"Updated the status of document {doc_id} to Scanning")
        except Exception as status_error:
            logger.error(f"Error updating Scanning status: {str(status_error)}")
     
        
        content = document.get('content', '')
        if not content or not content.strip():
            error_msg = "Document has no content"
            logger.error(f"Error while scanning document {doc_id}: {error_msg}")
            check_and_handle_document_failure(doc_id, error_msg)
            return False

        logger.info(f"Analyzing similarity for document {doc_id}")
        similar_docs = analyze_document_similarity(doc_id, content)
        is_duplicate = False
        duplicate_info = None

        if similar_docs:
            most_similar = similar_docs[0]
            similarity = float(most_similar['similarity'])
            logger.info(f"Most similar document: {most_similar['id']} with similarity {similarity}")
            
            if similarity >= 0.98:
                related_docs = [str(doc_id)] + [str(d['id']) for d in similar_docs]
                duplicate_group_id = f"dup_group_{int(time.time())}"
                
                try:
                    documents = data_manager.get_all_documents()
                    group_docs = documents[documents['id'].isin(related_docs)]
                    
                    if not group_docs.empty:
                        original_doc = group_docs.sort_values('created_date').iloc[0]
                        original_doc_id = original_doc['id']
                        original_chunks = None

                        if original_doc['chunk_status'] == 'Chunked':
                            original_chunks = chroma_manager.get_chunks_by_document_id(original_doc_id)
                            logger.info(f"Tài liệu gốc {original_doc_id} đã có {len(original_chunks) if original_chunks else 0} chunks")
                        
                        for curr_doc_id in related_docs:
                            is_original = (curr_doc_id == original_doc_id)
                            has_original_chunks = (original_chunks is not None and len(original_chunks) > 0)
                            
                            logger.info(f"Cập nhật tài liệu {curr_doc_id} - IsOriginal: {is_original}, HasOriginalChunks: {has_original_chunks}")
                            
                            status_data = {
                                'is_duplicate': True,
                                'similarity_score': float(similarity),
                                'similarity_level': 'Duplicate',
                                'duplicate_group_id': duplicate_group_id,
                                'duplicate_count': len(related_docs),
                            }
                            
                            if is_original:
                                if has_original_chunks:
                                    status_data.update({
                                        'processing_status': 'Processed',
                                        'scan_status': 'Completed',
                                        'chunk_status': 'Chunked',
                                        'detailed_analysis': 'Original document with existing chunks'
                                    })
                                else:
                                    status_data.update({
                                        'processing_status': 'Processing',
                                        'scan_status': 'Completed',
                                        'chunk_status': 'Pending',
                                        'detailed_analysis': f'Original document in group {duplicate_group_id}'
                                    })
                            else:
                                status_data.update({
                                    'processing_status': 'Duplicate',
                                    'scan_status': 'Completed',
                                    'chunk_status': 'NotRequired',
                                    'detailed_analysis': f'Using chunks from original document: {original_doc_id}',
                                    'original_chunked_doc': original_doc_id
                                })
                            
                            update_document_status_direct(curr_doc_id, status_data)
                                
                            if curr_doc_id == original_doc_id and not has_original_chunks:
                                logger.info(f"Gửi tài liệu gốc {curr_doc_id} đến processor")
                                success = send_to_processor(curr_doc_id, False)
                                if not success:
                                    logger.error(f"Could not send original document {curr_doc_id} to processor")
                                    update_document_status_direct(curr_doc_id, {
                                        'processing_status': 'Failed',
                                        'error_message': 'Failed to send to processor'
                                    })
                        
                        is_duplicate = True
                        duplicate_info = {
                            "duplicate_group_id": duplicate_group_id,
                            "document_ids": related_docs,
                            "original_doc_id": original_doc_id,
                            "has_chunks": bool(original_chunks and len(original_chunks) > 0)
                        }
                except Exception as dup_error:
                    logger.error(f"Error processing duplicate document: {str(dup_error)}")
                    logger.error(traceback.format_exc())

        if not is_duplicate:
            try:
                logger.info(f"Document {doc_id} is not a duplicate, preparing for processing")
                
                status_data = {
                    'processing_status': 'Processing',
                    'scan_status': 'Completed',
                    'is_duplicate': False,
                    'similarity_score': float(similarity) if similar_docs else 0.0,
                    'similarity_level': 'Unique',
                    'chunk_status': 'Pending',
                    'duplicate_group_id': None,
                    'original_chunked_doc': None
                }
                
                update_document_status_direct(doc_id, status_data)
                
                logger.info(f"Send document {doc_id} to processor")
                success = send_to_processor(doc_id, False, duplicate_info)
                if not success:
                    logger.error(f"Could not send document {doc_id} to processor")
                    update_document_status_direct(doc_id, {
                        'processing_status': 'Failed',
                        'error_message': 'Failed to send to processor'
                    })
                    return False
                    
                return True
                
            except Exception as update_error:
                logger.error(f"Lỗi khi cập nhật trạng thái xử lý: {str(update_error)}")
                logger.error(traceback.format_exc())
                check_and_handle_document_failure(doc_id, str(update_error))
                return False

        return True

    except Exception as e:
        logger.error(f"Error scanning document {doc_id}: {str(e)}")
        logger.error(traceback.format_exc())
        
        if retry_count < 3:
            logger.info(f"Retry {retry_count + 1} for document {doc_id}")
            time.sleep(5)
            return scan_document(doc_id, retry_count + 1)
        else:
            check_and_handle_document_failure(doc_id, str(e))
            return False

@app.route('/analyze_conflicts/<doc_id>', methods=['POST'])
def analyze_conflicts(doc_id):
    """
    Conflict analysis for a document
    """
    try:
        document = data_manager.get_document_by_id(doc_id)
        if not document:
            return jsonify({
                'status': 'error',
                'message': f'Document {doc_id} not found'
            }), 404
            
        data_manager.update_document_status(doc_id, {
            'conflict_analysis_status': 'Analyzing'
        })
        
        conflict_result = conflict_manager.analyze_document(doc_id)
        
        return jsonify({
            'status': 'success',
            'message': 'Analysis completed',
            'result': conflict_result
        })
        
    except Exception as e:
        logger.error(f"Error analyzing conflicts: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


def initialize_scan_queue():
    """
    Initializes the scan queue with documents that need to be scanned.
    """
    try:
        documents = data_manager.get_documents_to_scan()
        
        if documents.empty:
            return
            
        for _, doc in documents.iterrows():
            try:
                doc_id = doc['id']
                current_scan_status = doc['scan_status']
                
                scan_queue.put(doc_id)
                
                if current_scan_status != 'Queued':
                    try:
                        with data_manager.get_connection() as conn:
                            with conn.cursor() as cursor:
                                cursor.execute("""
                                    CREATE OR REPLACE FUNCTION prepare_for_scan(p_doc_id VARCHAR) 
                                    RETURNS BOOLEAN AS $
                                    DECLARE
                                        v_current_status VARCHAR;
                                    BEGIN
                                        SELECT scan_status INTO v_current_status 
                                        FROM documents 
                                        WHERE id = p_doc_id;
                                        
                                        EXECUTE 'UPDATE documents SET processing_status = ''Queued'', scan_status = ''Queued'', modified_date = CURRENT_TIMESTAMP WHERE id = $1'
                                        USING p_doc_id;
                                        
                                        RETURN TRUE;
                                    END;
                                    $ LANGUAGE plpgsql;
                                """)
                                conn.commit()
                                
                                cursor.execute("SELECT prepare_for_scan(%s)", (doc_id,))
                                conn.commit()
                    except Exception as update_error:
                        logger.warning(f"Failed to update status for {doc_id}: {str(update_error)}")
                
                logger.info(f"Document {doc_id} queued for scanning")
            except Exception as e:
                logger.error(f"Error adding document {doc_id} to queue: {str(e)}")
            
    except Exception as e:
        logger.error(f"Error initializing scan queue: {str(e)}")


if __name__ == '__main__':
    add_conflict_columns()
    initialize_scan_queue()
    check_and_rescan_documents()

    for _ in range(worker_pool._max_workers):
        worker = threading.Thread(target=scan_worker, daemon=True)
        worker.start()
        logger.info("Started scan worker thread")

    port = int(os.getenv('KMS_SCANNER_PORT'))
    app.run(host='0.0.0.0', debug=True, port=port)
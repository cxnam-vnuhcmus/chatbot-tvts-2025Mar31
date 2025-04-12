from flask import Flask, request, jsonify
from flask_cors import CORS
import threading
import queue
import time
import logging
import sys
import os
import requests
import json 
import traceback
from datetime import datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from common.data_manager import DatabaseManager
from common.chroma_manager import ChromaManager
from common.gpt_processor import GPTProcessor
from common.conflict_manager import ConflictManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

data_manager = DatabaseManager()
chroma_manager = ChromaManager()
gpt_processor = GPTProcessor()

logger.info("Using OpenAI for conflict analysis")
conflict_manager = ConflictManager(data_manager, chroma_manager)

process_queue = queue.Queue()
retry_queue = queue.Queue()

MAX_RETRIES = 3
RETRY_DELAY = 5
KMS_SCANNER_API = os.getenv('KMS_SCANNER_API')


def notify_scanner(doc_id, status, error_message=None):
    """
    Send document processing result back to the scanner with improved error handling.
    """
    if not KMS_SCANNER_API:
        logger.warning("KMS_SCANNER_API environment variable not set - skipping notification")
        return

    try:
        response = requests.post(
            f"{KMS_SCANNER_API}/chunk_callback",
            json={
                'doc_id': doc_id,
                'chunk_status': status,
                'error_message': error_message
            },
            timeout=10
        )
        response.raise_for_status()
        logger.info(f"Successfully notified scanner for {doc_id} with status: {status}")
    except requests.exceptions.RequestException as e:
        logger.warning(f"Failed to notify scanner for {doc_id}: {str(e)} - continuing processing")
    except Exception as e:
        logger.error(f"Unexpected error notifying scanner for {doc_id}: {str(e)}")

def handle_processing_failure(doc_id, error):
    """
    Handle the failure of document processing and update the document's status accordingly.

    Args:
        doc_id (str): The ID of the document that failed processing.
        error (Exception): The error encountered during document processing.

    Returns:
        None
    """
    try:
        failure_count = data_manager.update_chunk_failure_count(doc_id, increment=True)

        if failure_count >= MAX_RETRIES:
            data_manager.update_document_status(doc_id, {
                'processing_status': 'Failed',
                'chunk_status': 'ChunkingFailed',
                'error_message': str(error)
            })
            notify_scanner(doc_id, 'failed', str(error))
        else:
            retry_queue.put(doc_id)

    except Exception as e:
        logger.error(f"Error handling failure for document {doc_id}: {str(e)}")

@app.route('/reprocess_failed', methods=['POST'])
def reprocess_failed_documents():
    try:
        doc_id = request.json.get('doc_id')
        
        if doc_id:
            document = data_manager.get_document_by_id(doc_id)
            if document and document['chunk_status'] in ['ChunkingFailed', 'Failed']:
                process_queue.put({'doc_id': doc_id})
                
                with data_manager.get_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("""
                            BEGIN;
                            SET session_replication_role = 'replica';
                            
                            UPDATE documents 
                            SET chunk_status = 'Chunked',
                                processing_status = 'Processed',
                                modified_date = CURRENT_TIMESTAMP 
                            WHERE id = %s;
                            
                            SET session_replication_role = 'origin';
                            COMMIT;
                        """, (doc_id,)) 
                        conn.commit()
                        
                return jsonify({'message': f'Tài liệu {doc_id} đã được đưa vào hàng đợi để xử lý lại'}), 200
                      
        failed_docs = data_manager.get_documents_by_status(['ChunkingFailed', 'Failed'])
        
        requeued_count = 0
        for doc in failed_docs:
            try:
                process_queue.put({'doc_id': doc['id']})
                
                with data_manager.get_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("""
                            BEGIN;
                            SET session_replication_role = 'replica';
                            
                            UPDATE documents 
                            SET processing_status = 'Queued',
                                chunk_status = 'Pending',
                                chunk_failure_count = 0,
                                modified_date = CURRENT_TIMESTAMP 
                            WHERE id = %s;
                            
                            SET session_replication_role = 'origin';
                            COMMIT;
                        """, (doc['id'],))
                        conn.commit()
                        
                requeued_count += 1
            
            except Exception as e:
                logger.error(f"Error reprocessing document {doc['id']}: {str(e)}")
            
        return jsonify({
            'message': f'Đã đưa {requeued_count}/{len(failed_docs)} tài liệu vào hàng đợi để xử lý lại'
        }), 200
        
    except Exception as e:
        logger.error(f"Error reprocessing document: {str(e)}")
        return jsonify({'error': str(e)}), 500
    

@app.route('/process_doc', methods=['POST'])
def handle_document():
    """
    Handle incoming document requests, determine duplicate status, and queue for processing if necessary.

    Returns:
        Response: A JSON response indicating the success or failure of the operation.

    Raises:
        Exception: If an error occurs during processing.

    Request Data:
        - doc_id (str): The unique identifier for the document.
       
        - duplicate_info (optional, dict): Information about duplication, if available.

    Possible Responses:
        - 200 OK: Indicates successful handling of the document.
        - 400 Bad Request: Indicates a missing or invalid `doc_id`.
        - 404 Not Found: Indicates that the specified document could not be found.
        - 500 Internal Server Error: Indicates an unhandled error during processing.
    """
    try:
        data = request.json
        if not data or 'doc_id' not in data:
            return jsonify({'error': 'Missing doc_id'}), 400

        doc_id = data['doc_id']
        document = data_manager.get_document_by_id(doc_id)
        if not document:
            return jsonify({'error': 'Document not found'}), 404

        duplicate_info = data.get('duplicate_info')
        
        
        is_duplicate = document.get('is_duplicate', False)
        
        if is_duplicate:
            data_manager.update_document_status(doc_id, {
                'processing_status': 'Duplicate',
                'chunk_status': 'NotRequired',
                'detailed_analysis': f"Using chunks from original document: {document.get('original_doc_id')}"
            })
            return jsonify({
                'status': 'success',
                'message': 'Document marked as duplicate - chunking not required'
            }), 200

        # Queue unique documents for processing
        process_queue.put({
            'doc_id': doc_id,
            'duplicate_info': duplicate_info
        })

        return jsonify({
            'status': 'success',
            'message': f'Document {doc_id} queued'
        }), 200

    except Exception as e:
        logger.error(f"Error handling request: {str(e)}")
        return jsonify({'error': str(e)}), 500
    

def process_document(doc_id, duplicate_info=None):
    """
    Process a document by breaking it into chunks, analyzing for conflicts, and storing the result.
    Uses transaction management to ensure data consistency.

    Args:
        doc_id (str): The ID of the document to process.
        duplicate_info (dict, optional): Information about duplicates, if any, to be included in the status.

    Returns:
        bool: True if the document was processed successfully, False otherwise.
    """
    try:
        document = data_manager.get_document_by_id(doc_id)
        if not document:
            logger.error(f"Document {doc_id} not found")
            return False

        current_status = document.get('processing_status')
        current_chunk_status = document.get('chunk_status')
        
        update_result = data_manager.update_document_status(doc_id, {
            'processing_status': 'Processing',
            'chunk_status': 'Processing',
            'modified_date': datetime.now().isoformat()
        })
        
        if not update_result:
            logger.error(f"Failed to update document status for {doc_id}")
            return False
        
        document = data_manager.get_document_by_id(doc_id)
        if not document:
            logger.error(f"Document {doc_id} not found after status update")
            return False

        content = document.get('content')
        if not content:
            logger.error(f"No content to process for document {doc_id}")
            return False

        processor = GPTProcessor()

        update_result = data_manager.update_document_status(doc_id, {
            'chunk_status': 'Chunking',
            'modified_date': datetime.now().isoformat()
        })
        
        if not update_result:
            logger.error(f"Failed to update chunking status for {doc_id}")
            return False

        try:
            chunks_data = processor.process_content(content, doc_id)
        except Exception as proc_error:
            logger.error(f"Error processing content for document {doc_id}: {str(proc_error)}")
            logger.error(traceback.format_exc())
            
            data_manager.update_document_status(doc_id, {
                'processing_status': 'Failed',
                'chunk_status': 'ChunkingFailed',
                'error_message': f"Content processing error: {str(proc_error)}"
            })
            
            notify_scanner(doc_id, 'failed', str(proc_error))
            return False

        try:
            success = chroma_manager.add_chunks(
                doc_id,
                chunks_data,
                document.get('unit', ''),
                duplicate_info=duplicate_info
            )
        except Exception as chunks_error:
            logger.error(f"Error adding chunks for document {doc_id}: {str(chunks_error)}")
            logger.error(traceback.format_exc())
            
            data_manager.update_document_status(doc_id, {
                'processing_status': 'Failed',
                'chunk_status': 'ChunkingFailed',
                'error_message': f"Chunk storage error: {str(chunks_error)}"
            })
            
            notify_scanner(doc_id, 'failed', str(chunks_error))
            return False

        if success:
            update_success = data_manager.update_document_status(doc_id, {
                'chunk_status': 'Chunked',
                'processing_status': 'Processed',
                'modified_date': datetime.now().isoformat()
            })
            
            if not update_success:
                logger.error(f"Failed to update document to Chunked status: {doc_id}")
                return False

            document = data_manager.get_document_by_id(doc_id)
            if not document:
                logger.error(f"Document {doc_id} not found after successful chunk processing")
                return False

            try:
                chunks = chroma_manager.get_chunks_by_document_id(doc_id)
                if not chunks:
                    logger.warning(f"No chunks found for document {doc_id} after successful processing")
                    notify_scanner(doc_id, 'success')
                    return True
                
                conflict_result = conflict_manager.analyze_document(doc_id)

                status_update = {
                    'has_conflicts': conflict_result['has_conflicts'],
                    'conflict_info': json.dumps(conflict_result) if conflict_result['has_conflicts'] else None,
                    'modified_date': datetime.now().isoformat()
                }

                if duplicate_info:
                    status_update.update({
                        'is_duplicate': True,
                        'duplicate_group_id': duplicate_info.get('duplicate_group_id')
                    })

                data_manager.update_document_status(doc_id, status_update)
                
            except Exception as analysis_error:
                logger.error(f"Error during conflict analysis for document {doc_id}: {str(analysis_error)}")
                logger.error(traceback.format_exc())
            
            notify_scanner(doc_id, 'success')
            return True
        else:
            data_manager.update_document_status(doc_id, {
                'processing_status': 'Failed',
                'chunk_status': 'ChunkingFailed',
                'error_message': "Failed to store chunks in ChromaDB"
            })
            
            notify_scanner(doc_id, 'failed', "Failed to store chunks in ChromaDB")
            return False

    except Exception as e:
        logger.error(f"Error processing document {doc_id}: {str(e)}")
        logger.error(traceback.format_exc())
        
        try:
            data_manager.update_document_status(doc_id, {
                'processing_status': 'Failed',
                'chunk_status': 'ChunkingFailed',
                'error_message': str(e)
            })
        except Exception as update_error:
            logger.error(f"Failed to update error status for document {doc_id}: {str(update_error)}")

        notify_scanner(doc_id, 'failed', str(e))
        return False

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


def processing_worker():
    """
    Worker to process documents from the queue.
    """
    while True:
        try:
            data = process_queue.get(timeout=1)
            doc_id = data['doc_id']
            duplicate_info = data.get('duplicate_info')
            
            success = process_document(doc_id, duplicate_info)
            if not success:
                handle_processing_failure(doc_id, "Processing failed")
                
            process_queue.task_done()
            
        except queue.Empty:
            time.sleep(1)
        except Exception as e:
            logger.error(f"Worker error: {str(e)}")
            process_queue.task_done()
            
def retry_worker():
    """
    Worker to handle document retries from the retry queue.
    """
    while True:
        doc_id = None
        try:
            doc_id = retry_queue.get(timeout=1)
            time.sleep(RETRY_DELAY)
            success = process_document(doc_id)
            if not success:
                handle_processing_failure(doc_id, "Retry failed")
            retry_queue.task_done()
        except queue.Empty:
            time.sleep(1)
        except Exception as e:
            logger.error(f"Retry error: {str(e)}")
            if doc_id:
                handle_processing_failure(doc_id, str(e))
            retry_queue.task_done()

def start_workers():
    """
    Start processing and retry workers in separate threads.
    """
    worker_count = 5  
    for i in range(worker_count):
        worker = threading.Thread(target=processing_worker, daemon=True)
        worker.start()
        logger.info(f"Started processing worker #{i+1}")
 
    for i in range(worker_count):
        worker = threading.Thread(target=retry_worker, daemon=True)
        worker.start()
        logger.info(f"Started retry worker #{i+1}")

def check_failed_documents():
    """
    Kiểm tra và xử lý lại các tài liệu thất bại
    """
    try:
        failed_docs = data_manager.get_documents_by_status(['ChunkingFailed', 'Processing'])
        
        if failed_docs is None or failed_docs.empty:
            logger.info("No documents to reprocess")
            return
            
        logger.info(f"Found {len(failed_docs)} documents to reprocess")
        
        for _, doc in failed_docs.iterrows():
            try:
                doc_id = doc['id']
                process_queue.put({'doc_id': doc_id})
                data_manager.update_document_status(doc_id, {
                    'processing_status': 'Queued',
                    'chunk_status': 'Pending',
                    'chunk_failure_count': 0
                })
                logger.info(f"Re-queued document {doc_id} for processing")
            except Exception as doc_error:
                logger.error(f"Error processing document {doc.get('id', 'Unknown')}: {str(doc_error)}")
                logger.error(traceback.format_exc())
                continue
    except Exception as e:
        logger.error(f"Error when checking documents failed: {str(e)}")
        logger.error(traceback.format_exc())

@app.route('/chunk_callback', methods=['POST'])
def chunk_callback():
    try:
        data = request.json
        if not data or 'doc_id' not in data:
            return jsonify({'error': 'Missing doc_id'}), 400
            
        doc_id = data['doc_id']
        chunk_status = data.get('chunk_status')
        error_message = data.get('error_message')

        document = data_manager.get_document_by_id(doc_id)
        if not document:
            return jsonify({'error': 'Document not found'}), 404

        if chunk_status == 'success':
            try:
                conflict_manager = ConflictManager(data_manager, chroma_manager)
                conflict_result = conflict_manager.analyze_document(doc_id)

                status_update = {
                    'chunk_status': 'Chunked',
                    'processing_status': 'Processed',
                    'has_conflicts': conflict_result["has_conflicts"],
                    'conflict_info': json.dumps(conflict_result),
                    'last_conflict_check': datetime.now().isoformat(),
                    'conflict_status': "Pending Review" if conflict_result["has_conflicts"] else "No Conflict"
                }
                
            except Exception as analysis_error:
                logger.error(f"Conflict analysis error: {str(analysis_error)}")
                status_update = {
                    'chunk_status': 'ChunkingFailed',
                    'processing_status': 'Failed',
                    'error_message': str(analysis_error)
                }

        else:
            status_update = {
                'chunk_status': 'ChunkingFailed', 
                'processing_status': 'Failed',
                'error_message': error_message
            }

        data_manager.update_document_status(doc_id, status_update)
        return jsonify({'status': 'success'}), 200

    except Exception as e:
        logger.error(f"Error processing chunk callback: {str(e)}")
        return jsonify({'error': str(e)}), 500




if __name__ == '__main__':
    try:
        start_workers()
        check_failed_documents()
        port = int(os.getenv('KMS_PROCESSOR_PORT'))
        app.run(host='0.0.0.0', debug=True, port=port)
        
    except Exception as e:
        logger.error(traceback.format_exc())
        sys.exit(1)
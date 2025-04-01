from flask import Flask, request, jsonify
import logging
import os
import json
import time
import sys
from datetime import datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from common.data_manager import DatabaseManager
from common.chroma_manager import ChromaManager
from common.async_conflict_processor import AsyncConflictProcessor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

data_manager = DatabaseManager()
chroma_manager = ChromaManager()

try:
    conflict_processor = AsyncConflictProcessor(data_manager, chroma_manager, max_workers=3)
    logger.info("AsyncConflictProcessor initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize AsyncConflictProcessor: {str(e)}")
    conflict_processor = None
    

@app.route('/health', methods=['GET'])
def health_check():
    status = {
        "status": "healthy" if conflict_processor else "degraded",
        "use_openai": True,  
        "timestamp": datetime.now().isoformat()
    }
    
    if conflict_processor:
        status["queue_stats"] = conflict_processor.get_queue_stats()
        
    return jsonify(status)


@app.route('/analyze/document', methods=['POST'])
def analyze_document():
    """Analyzing conflicts in a document"""
    if not conflict_processor:
        return jsonify({
            "status": "error",
            "message": "Conflict processor is not available"
        }), 503
        
    try:
        data = request.json
        if not data or 'doc_id' not in data:
            return jsonify({
                "status": "error",
                "message": "Missing required parameter: doc_id"
            }), 400
            
        doc_id = data['doc_id']
        priority = int(data.get('priority', 5))
        
        document = data_manager.get_document_by_id(doc_id)
        if not document:
            return jsonify({
                "status": "error", 
                "message": f"Document {doc_id} not found"
            }), 404
            
        task_id = conflict_processor.queue_document(doc_id, priority)
        
        return jsonify({
            "status": "success",
            "message": f"Document {doc_id} queued for analysis",
            "task_id": task_id
        })
        
    except Exception as e:
        logger.error(f"Error queueing document for analysis: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/analyze/chunks', methods=['POST'])
def analyze_chunks():
    """Analyze the contradiction between two paragraphs"""
    if not conflict_processor:
        return jsonify({
            "status": "error",
            "message": "Conflict processor is not available"
        }), 503
        
    try:
        data = request.json
        if not data or 'content1' not in data or 'content2' not in data:
            return jsonify({
                "status": "error",
                "message": "Missing required parameters: content1, content2"
            }), 400
            
        content1 = data['content1']
        content2 = data['content2']
        conflict_type = data.get('conflict_type', 'internal')
        priority = int(data.get('priority', 3))
        
        chunk1 = {'id': f'temp1_{int(time.time())}', 'original_text': content1}
        chunk2 = {'id': f'temp2_{int(time.time())}', 'original_text': content2}
        
        task_id = conflict_processor.queue_chunk_pair(chunk1, chunk2, conflict_type, priority)
        
        return jsonify({
            "status": "success",
            "message": "Chunks queued for analysis",
            "task_id": task_id
        })
        
    except Exception as e:
        logger.error(f"Error queueing chunks for analysis: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/analyze/content', methods=['POST'])
def analyze_content():
    """Analyzing conflicts in a text"""
    if not conflict_processor:
        return jsonify({
            "status": "error",
            "message": "Conflict processor is not available"
        }), 503
        
    try:
        data = request.json
        if not data or 'content' not in data:
            return jsonify({
                "status": "error",
                "message": "Missing required parameter: content" 
            }), 400
            
        content = data['content']
        priority = int(data.get('priority', 3))
        
        task_id = conflict_processor.queue_content(content, priority)
        
        return jsonify({
            "status": "success",
            "message": "Content queued for analysis",
            "task_id": task_id
        })
        
    except Exception as e:
        logger.error(f"Error queueing content for analysis: {str(e)}")
        return jsonify({
            "status": "error", 
            "message": str(e)
        }), 500

@app.route('/tasks/<task_id>', methods=['GET'])
def get_task_status(task_id):
    """Get the status of the task"""
    if not conflict_processor:
        return jsonify({
            "status": "error",
            "message": "Conflict processor is not available"
        }), 503
        
    try:
        task_status = conflict_processor.get_task_status(task_id)
        return jsonify(task_status)
        
    except Exception as e:
        logger.error(f"Error getting task status: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/queue/stats', methods=['GET'])
def get_queue_stats():
    """Get queue statistics"""
    if not conflict_processor:
        return jsonify({
            "status": "error",
            "message": "Conflict processor is not available"
        }), 503
        
    try:
        stats = conflict_processor.get_queue_stats()
        return jsonify({
            "status": "success",
            "stats": stats
        })
        
    except Exception as e:
        logger.error(f"Error getting queue stats: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/reprocess/<doc_id>', methods=['POST'])
def reprocess_document(doc_id):
    """Reanalyzing conflicts in a document"""
    if not conflict_processor:
        return jsonify({
            "status": "error",
            "message": "Conflict processor is not available"
        }), 503
        
    try:
        document = data_manager.get_document_by_id(doc_id)
        if not document:
            return jsonify({
                "status": "error",
                "message": f"Document {doc_id} not found"
            }), 404
            
        data_manager.update_document_status(doc_id, {
            'conflict_analysis_status': 'NotAnalyzed',
            'last_conflict_check': None
        })
        
        task_id = conflict_processor.queue_document(doc_id, priority=1)
        
        return jsonify({
            "status": "success",
            "message": f"Document {doc_id} requeued for analysis",
            "task_id": task_id
        })
        
    except Exception as e:
        logger.error(f"Error reprocessing document: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

if __name__ == '__main__':
    port = int(os.getenv('CONFLICT_API_PORT', 5002))
    app.run(host='0.0.0.0', debug=True, port=port)
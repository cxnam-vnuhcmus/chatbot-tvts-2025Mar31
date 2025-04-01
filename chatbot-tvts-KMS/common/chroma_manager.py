import chromadb
from chromadb.utils import embedding_functions
from typing import List, Dict, Any
import os
import time
import traceback
import json 
import logging
from datetime import datetime
from common.data_manager import DatabaseManager 
from dotenv import load_dotenv
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ChromaManager:
    def __init__(self):
        
        self.deployment_env = os.getenv('DEPLOYMENT_ENV')
        self.embedding_model = os.getenv('EMBEDDING_MODEL_NAME') 
        self.collection_name = os.getenv('CHROMA_DB')
        
        self.data_manager = DatabaseManager()
        
        self.client = chromadb.HttpClient(
            host=os.getenv('CHROMA_HOST'),
            port=int(os.getenv('CHROMA_PORT'))
        )
        
        self.max_retries = 3
        self.retry_delay = 5

        self.openai_api_key = os.getenv('OPENAI_API_KEY')

        openai_ef = embedding_functions.OpenAIEmbeddingFunction(
            api_key=self.openai_api_key,
            model_name=self.embedding_model
        )
        try:
            self.collection = self.client.get_collection(
                name=self.collection_name,
                embedding_function=openai_ef
            )
            logger.info(f"Connected to existing collection: {self.collection_name}")
            
        except Exception as e:
            logger.info(f"Collection not found, creating new one: {str(e)}")
            self.collection = self.client.create_collection(
                name=self.collection_name,
                embedding_function=openai_ef,
                metadata={"chunking_method": "gpt"}
            )
            logger.info(f"Created new collection: {self.collection_name}")

    def add_chunks(self, doc_id: str, chunks_data: Dict, unit: str = '', duplicate_info: Dict = None) -> bool:
        """
        Add chunks to ChromaDB and update document state.

        Args:
            doc_id (str): ID of document
            chunks_data (Dict): Chunk data from GPT
            unit (str, optional): Unit of document
            duplicate_info (Dict, optional): Information about duplicate documents

        Returns:
            bool: True on success, False on error
        """
        try:
            if not doc_id or not isinstance(doc_id, str):
                return False
                
            if not chunks_data or not isinstance(chunks_data, dict):
                return False
            
            required_fields = ['CHUNKS', 'TOPIC', 'CHUNK_NUMBER']
            if not all(field in chunks_data for field in required_fields):
                return False
                
            if not chunks_data['CHUNKS']:
                return True
                
            expected_chunks = int(chunks_data['CHUNK_NUMBER'])
            actual_chunks = len(chunks_data['CHUNKS'])
            if expected_chunks != actual_chunks:
                logger.warning(f"Chunk count mismatch for document {doc_id}. Expected: {expected_chunks}, Got: {actual_chunks}")
            
            chunk_objects = []
            doc_topic = chunks_data.get('TOPIC', '').strip()
            
            for i, chunk in enumerate(chunks_data['CHUNKS'], 1):
                required_chunk_fields = ['chunk_topic', 'original_chunk', 'revised_chunk', 'index']
                if not all(field in chunk for field in required_chunk_fields):
                    continue
                
                expected_index = f"Paragraph {i}"
                if chunk['index'] != expected_index:
                    chunk['index'] = expected_index

                
                if isinstance(chunk['revised_chunk'], list):
                    revised_text = '\n'.join([str(item) for item in chunk['revised_chunk']])
                else:
                    revised_text = chunk['revised_chunk'].strip()
                    
                if isinstance(chunk['original_chunk'], list):
                    original_text = '\n'.join([str(item) for item in chunk['original_chunk']])
                else:
                    original_text = chunk['original_chunk'].strip()
                
                if not ('Q:' in revised_text or 'Hỏi:' in revised_text) or not ('A:' in revised_text or 'Đáp:' in revised_text):
                    logger.warning(f"Chunk {i} in document {doc_id} may not be in proper Q&A format")
                
                paragraph_number = str(i)  
                chunk_id = f"{doc_id}_paragraph_{paragraph_number}"
                
                metadata = {
                    'document_topic': doc_topic,
                    'chunk_topic': chunk['chunk_topic'].strip(),
                    'paragraph': chunk['index'],
                    'original_text': original_text,
                    'revised_chunk': revised_text,
                    'original_id': doc_id,
                    'unit': unit.strip() if unit else ''
                }
                
                if duplicate_info and isinstance(duplicate_info, dict):
                    if 'duplicate_group_id' in duplicate_info and 'document_ids' in duplicate_info:
                        metadata.update({
                            'duplicate_group_id': duplicate_info['duplicate_group_id'],
                            'is_original': True,
                            'duplicate_count': len(duplicate_info['document_ids'])
                        })
                
                formatted_chunk = f"""
                DOCUMENT TOPIC: {doc_topic}
                CHUNK TOPIC: {chunk['chunk_topic']}

                FAQs: 
                {revised_text}

                ORIGINAL TEXT: 
                {original_text}
                """.strip()
                
                chunk_objects.append({
                    'id': chunk_id,
                    'metadata': metadata,
                    'content': formatted_chunk
                })
            
            if not chunk_objects:
                logger.warning(f"No valid chunks to add for document {doc_id}")
                return False
            
            retry_count = 0
            while retry_count < self.max_retries:
                try:
                    self.collection.add(
                        ids=[c['id'] for c in chunk_objects],
                        documents=[c['content'] for c in chunk_objects],
                        metadatas=[c['metadata'] for c in chunk_objects]
                    )
                    logger.info(f"Successfully added {len(chunk_objects)} chunks for document {doc_id}")
                    return True
                    
                except Exception as e:
                    retry_count += 1
                    if retry_count < self.max_retries:
                        wait_time = self.retry_delay * (2 ** (retry_count - 1))
                        logger.warning(f"Retry {retry_count}/{self.max_retries} after {wait_time}s for document {doc_id}")
                        time.sleep(wait_time)
                    else:
                        logger.error(f"Failed to add chunks after {self.max_retries} retries for document {doc_id}: {str(e)}")
                        return False
                        
        except Exception as e:
            logger.error(f"Error adding chunks for document {doc_id}: {str(e)}")
            logger.error(traceback.format_exc())
            return False
    
    def delete_document_chunks(self, doc_id: str) -> bool:
        """
        Delete all chunks associated with a document from ChromaDB

        Args:
        doc_id (str): ID of the document whose chunks to delete

        Returns:
        bool: True on success, False on failure
        """
        try:
            
            where_condition = {"original_id": doc_id}
            
            results = self.collection.get(where=where_condition)
            if results and results['ids']:
                chunk_ids = results['ids']
                
                self.collection.delete(where=where_condition)
                
                logger.info(f"Successfully deleted {len(chunk_ids)} chunks for document {doc_id}")
                return True
            else:
                logger.info(f"No chunks found for document {doc_id}, no need to delete")
                return True
                
        except Exception as e:
            logger.error(traceback.format_exc())
            return False
    
    def get_chunks_by_document_id(self, doc_id: str, limit: int = None):
        """
        Get chunks of document by ID, handle duplicate documents, with pagination
        
        Args:
            doc_id (str): Document ID
            limit (int, optional): Limit number of chunks to return
            
        Returns:
            list: List of chunks
        """ 
        try:
            document = self.data_manager.get_document_by_id(doc_id)
            if not document:
                logger.warning(f"Document {doc_id} not found")
                return None

            logger.info(f"""[DEBUG] Looking for chunks with doc_id={doc_id}:
                - chunk_status: {document.get('chunk_status')}
                - is_duplicate: {document.get('is_duplicate')}
                - duplicate_group_id: {document.get('duplicate_group_id')}
                - original_chunked_doc: {document.get('original_chunked_doc')}
            """)
            
            original_doc = document.get('original_chunked_doc')
            duplicate_group_id = document.get('duplicate_group_id')
            is_duplicate = document.get('is_duplicate')
            chunk_status = document.get('chunk_status')

            logger.info(f"""[DEBUG] Document info:
                - ID: {doc_id}
                - original_chunked_doc: {original_doc}
                - duplicate_group_id: {duplicate_group_id}
                - is_duplicate: {is_duplicate}
                - chunk_status: {chunk_status}
            """)

            chunks_source = None
            source_reason = ""
            attempted_sources = []

            if original_doc:
                where_clause = {"original_id": original_doc}
                chunks_source = original_doc
                source_reason = "original_chunked_doc"
                attempted_sources.append(f"original_chunked_doc: {original_doc}")
            
            #2. If the document is in a duplicate group and there is no original_chunked_doc
            elif duplicate_group_id and not chunks_source:
                group_docs = self.data_manager.get_documents_in_group(duplicate_group_id)
                if group_docs:
                    chunked_docs = [d for d in group_docs if d.get('chunk_status') == 'Chunked']
                    if chunked_docs:
                        chunks_source = chunked_docs[0]['id']
                        where_clause = {"original_id": chunks_source}
                        source_reason = "group_chunked_doc"
                        attempted_sources.append(f"group_chunked_doc: {chunks_source}")
            
            #3. Finally, if there are no sources, use the current document itself
            if not chunks_source:
                where_clause = {"original_id": doc_id}
                chunks_source = doc_id
                source_reason = "self"
                attempted_sources.append(f"self: {doc_id}")
                
            logger.info(f"Using chunks source: {chunks_source} (reason: {source_reason})")
            logger.info(f"Attempted sources: {attempted_sources}")

            query_params = {"where": where_clause}
            if limit and isinstance(limit, int) and limit > 0:
                query_params["limit"] = limit
                
            logger.info(f"Final ChromaDB query: {query_params}")
            results = self.collection.get(**query_params)

            if not results or not results['ids']:
                logger.warning(f"No chunks found for source {chunks_source}")
                return None

            logger.info(f"Found {len(results['ids'])} chunks for source {chunks_source}")

            formatted_chunks = []
            for chunk_id, content, metadata in zip(
                results['ids'],
                results['documents'],
                results['metadatas']
            ):
                chunk = {
                    'id': chunk_id,
                    'document_topic': metadata.get('document_topic'),
                    'chunk_topic': metadata.get('chunk_topic'),
                    'paragraph': metadata.get('paragraph'),
                    'original_text': metadata.get('original_text'), 
                    'qa_content': content,
                    'metadata': metadata,
                    'unit': metadata.get('unit')
                }
                formatted_chunks.append(chunk)

            return formatted_chunks

        except Exception as e:
            logger.error(f"Error getting chunks: {str(e)}")
            logger.error(traceback.format_exc())
            return None
        
    def update_chunk(self, chunk_id: str, new_content: str, metadata: dict = None) -> bool:
        """
        Update a chunk's content and metadata in the ChromaDB collection.
        
        Args:
            chunk_id (str): The ID of the chunk to update
            new_content (str): The new content for the chunk
            metadata (dict, optional): New metadata to update or add
            
        Returns:
            bool: True if update successful, False otherwise
        """
        try:
            logger.info(f"Starting update for chunk {chunk_id}")
            
            results = self.collection.get(
                ids=[chunk_id],
                include=['metadatas', 'documents']  
            )
            
            if not results or not results['ids']:
                logger.error(f"Chunk {chunk_id} not found")
                return False
                
            current_metadata = results['metadatas'][0] if results['metadatas'] else {}
            if metadata:
                current_metadata.update(metadata)
            current_metadata['updated_at'] = datetime.now().isoformat()

            embedding_function = self.collection._embedding_function
            if not embedding_function:
                logger.error("No embedding function found")
                return False
                
            embeddings = embedding_function([new_content])
            if not embeddings or len(embeddings) == 0:
                logger.error("Failed to create new embedding")
                return False

            if not isinstance(embeddings, list) or len(embeddings) != 1:
                logger.error("Invalid embedding format")
                return False

            try:
                self.collection.update(
                    ids=[chunk_id],
                    embeddings=embeddings,  
                    documents=[new_content],
                    metadatas=[current_metadata]
                )
                logger.info(f"Successfully updated chunk {chunk_id}")
                return True
                
            except Exception as e:
                logger.error(f"Failed to update chunk: {str(e)}")
                return False
                
        except Exception as e:
            logger.error(f"Error updating chunk {chunk_id}: {str(e)}")
            logger.error(traceback.format_exc())
            return False
    

    def update_chunk_metadata(self, chunk_id: str, metadata: dict) -> bool:
        """
        Update chunk metadata in ChromaDB and trigger conflict re-analysis when necessary.
        
        Args:
            chunk_id (str): ID of chunk to update
            metadata (dict): New metadata to update
                
        Returns:
            bool: True if successful, False if failed
        """
        try:
            doc_id = None
            if '_paragraph_' in chunk_id:
                doc_id = chunk_id.split('_paragraph_')[0]
            
            current_metadata = None
            previous_enabled_state = None
            
            try:
                results = self.collection.get(
                    ids=[chunk_id],
                    include=['metadatas', 'documents']
                )
                
                if not results or not results['ids']:
                    logger.error(f"Chunk {chunk_id} not found")
                    return False
                    
                current_metadata = results['metadatas'][0] if results['metadatas'] else {}
                current_document = results['documents'][0] if results['documents'] else None
                
                if 'is_enabled' in metadata and 'is_enabled' in current_metadata:
                    previous_enabled_state = current_metadata.get('is_enabled')
                    if isinstance(previous_enabled_state, str):
                        previous_enabled_state = previous_enabled_state.lower() == 'true'
            except Exception as fetch_error:
                logger.error(f"Error fetching current metadata: {str(fetch_error)}")
                return False
            
            is_updating_enabled = False
            new_enabled_state = None
            
            if 'is_enabled' in metadata:
                is_updating_enabled = True
                if isinstance(metadata['is_enabled'], str):
                    metadata['is_enabled'] = metadata['is_enabled'].lower() == 'true'
                new_enabled_state = bool(metadata['is_enabled'])
                metadata['is_enabled'] = new_enabled_state
            
            merged_metadata = {
                **current_metadata,
                **metadata,
                'last_updated': datetime.now().isoformat()
            }
            
            modified_document = current_document
            if is_updating_enabled and not new_enabled_state and current_document:
                if '[DISABLED]' not in current_document:
                    content_parts = current_document.split('\n\n')
                    if len(content_parts) > 0:
                        content_parts[0] = f"{content_parts[0]} [DISABLED]"
                        modified_document = '\n\n'.join(content_parts)
                    else:
                        modified_document = f"{current_document} [DISABLED]"
         
            elif is_updating_enabled and new_enabled_state and current_document and '[DISABLED]' in current_document:
                modified_document = current_document.replace('[DISABLED]', '').strip()
            
            try:
                self.collection.update(
                    ids=[chunk_id],
                    metadatas=[merged_metadata],
                    documents=[modified_document]
                )
                
                if is_updating_enabled and doc_id and previous_enabled_state != new_enabled_state:
                    try:
                        from common.conflict_manager import ConflictManager
                        from common.data_manager import DatabaseManager
                        
                        db_manager = DatabaseManager()
                        conflict_manager = ConflictManager(db_manager, self)
                        
                        handle_conflicts_result = conflict_manager.handle_conflicts(chunk_id, db_manager)
                        
                        if handle_conflicts_result and handle_conflicts_result["status"] == "success":
                            logger.info(f"Successfully processed conflicts for chunk {chunk_id}")
                            conflicts = handle_conflicts_result.get("conflicts", [])
                            if conflicts:
                                logger.info(f"Found {len(conflicts)} conflicts for chunk {chunk_id}")
                            else:
                                logger.info(f"No conflicts found for chunk {chunk_id}")
                        else:
                            logger.warning(f"Error handling conflicts: {handle_conflicts_result.get('message', 'unknown error')}")
                    except Exception as conflict_error:
                        logger.error(f"Error in conflict handling: {str(conflict_error)}")
                        logger.error(traceback.format_exc())
                
                logger.info(f"Successfully updated chunk {chunk_id} metadata with is_enabled={metadata.get('is_enabled', 'unchanged')}")
                return True
                    
            except Exception as update_error:
                logger.error(f"Failed to update chunk: {str(update_error)}")
                return False

        except Exception as e:
            logger.error(f"Error updating chunk {chunk_id}: {str(e)}")
            logger.error(traceback.format_exc())
            return False
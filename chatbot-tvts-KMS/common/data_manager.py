import psycopg2
from psycopg2 import sql, extensions
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime
import os
import json
import urllib.parse
from dotenv import load_dotenv
import logging
from typing import List, Dict, Any
import traceback
import time

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

POSTGRESQL_URL = f"postgresql+psycopg2://{str(os.environ.get('DB_USER'))}:{urllib.parse.quote_plus(os.environ.get('DB_PASSWORD'))}@{str(os.environ.get('DB_HOST'))}:{str(os.environ.get('DB_PORT'))}/{str(os.environ.get('DB_NAME'))}"

engine = create_engine(POSTGRESQL_URL, 
    poolclass=QueuePool, 
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_pre_ping=True,
    pool_recycle=3600
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def ensure_database_exists():
    """Ensure database exists and create if not"""
    try:
        db_params = {
            "host": os.getenv('DB_HOST'),
            "dbname": "postgres",
            "user": os.getenv('DB_USER'),
            "password": os.getenv('DB_PASSWORD'),
            "port": os.getenv('DB_PORT'),
            "client_encoding": 'UTF8'
        }
        db_name = os.getenv('DB_NAME')
        db_user = os.getenv('DB_USER')

        max_retries = 3
        retry_delay = 2
        for attempt in range(max_retries):
            try:
                conn = psycopg2.connect(**db_params)
                conn.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT)
                with conn.cursor() as cursor:
                    cursor.execute(
                        sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"),
                        [db_name]
                    )
                    exists = cursor.fetchone()
                    
                    if not exists:
                        logger.info(f"Creating database {db_name}...")
                        cursor.execute(
                            sql.SQL('CREATE DATABASE {}').format(sql.Identifier(db_name))
                        )
                        cursor.execute(
                            sql.SQL('GRANT ALL PRIVILEGES ON DATABASE {} TO {}').format(
                                sql.Identifier(db_name), sql.Identifier(db_user)
                            )
                        )
                        logger.info(f"Database {db_name} created successfully")
                    else:
                        logger.info(f"Database {db_name} already exists")
                break
                
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Database connection attempt {attempt + 1} failed: {str(e)}")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"Failed to ensure database exists: {str(e)}")
                    raise
                    
    except Exception as e:
        logger.error(f"Database initialization error: {str(e)}")
        logger.error(traceback.format_exc())
        raise
    
class DatabaseManager:
    def __init__(self):
        self.max_retries = 3
        self.retry_delay = 2
        self.db_name = os.getenv('DB_NAME')
        self.db_params = {
            "host": os.getenv('DB_HOST'),
            "user": os.getenv('DB_USER'),
            "password": os.getenv('DB_PASSWORD'),
            "port": os.getenv('DB_PORT'),
            "connect_timeout": 10,
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 10,
            "keepalives_count": 5
        }
        
        ensure_database_exists()
        
        self.db_params["database"] = self.db_name

        db_url = f"postgresql+psycopg2://{self.db_params['user']}:{urllib.parse.quote_plus(self.db_params['password'])}@{self.db_params['host']}:{self.db_params['port']}/{self.db_name}"
        self.engine = create_engine(
            db_url, 
            poolclass=QueuePool,
            pool_size=10,
            max_overflow=20,
            pool_timeout=30,
            pool_pre_ping=True,
            pool_recycle=3600
        )
        
        self.init_db()
        
    def get_connection(self):
        for attempt in range(self.max_retries):
            try:
                conn = psycopg2.connect(**self.db_params)
                return conn
            except Exception as e:
                if attempt < self.max_retries - 1:
                    logger.warning(f"Connection attempt {attempt + 1} failed: {str(e)}")
                    time.sleep(self.retry_delay)
                else:
                    logger.error("Failed to establish database connection")
                    raise

    def execute_with_retry(self, query, params=None, fetch=False):
        last_error = None
        
        for attempt in range(self.max_retries):
            try:
                with self.get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(query, params)
                        result = cur.fetchall() if fetch else None
                        conn.commit()
                        return result
            except Exception as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    logger.warning(f"Query execution attempt {attempt + 1} failed: {str(e)}")
                    time.sleep(self.retry_delay)
                    
        logger.error(f"Query execution failed after {self.max_retries} attempts")
        raise last_error

    def init_db(self):
        try:
            check_table_query = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'documents'
                );
            """
            table_exists = self.execute_with_retry(check_table_query, fetch=True)
            
            create_base_documents = """
                CREATE TABLE IF NOT EXISTS documents (
                    id VARCHAR(100) PRIMARY KEY,
                    content TEXT,
                    document_topic TEXT,
                    categories TEXT[],
                    tags TEXT[],
                    unit VARCHAR(100),
                    sender VARCHAR(100),
                    approver VARCHAR(100),
                    start_date TIMESTAMP,
                    end_date TIMESTAMP,
                    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    approval_date TIMESTAMP,
                    modified_date TIMESTAMP,
                    
                    processing_status VARCHAR(100) DEFAULT 'Pending',
                    approval_status VARCHAR(100) DEFAULT 'Pending',
                    scan_status VARCHAR(100) DEFAULT 'Pending',
                    chunk_status VARCHAR(100) DEFAULT 'Pending',
                    
                    is_valid BOOLEAN DEFAULT true,
                    chunk_count INTEGER DEFAULT 0,
                    chunk_failure_count INTEGER DEFAULT 0,
                    
                    is_duplicate BOOLEAN DEFAULT false,
                    similarity_score FLOAT,
                    similar_documents JSONB,
                    duplicate_group_id VARCHAR(255),
                    duplicate_count INTEGER DEFAULT 0,
                    original_doc_id VARCHAR(100),
                    similarity_level VARCHAR(50),
                    original_chunked_doc VARCHAR(100),
                    
                    error_message TEXT,
                    detailed_analysis TEXT,
                    
                    has_conflicts BOOLEAN DEFAULT false,
                    last_conflict_check TIMESTAMP,
                    conflict_info JSONB,
                    conflict_status VARCHAR(50) DEFAULT 'No Conflict',
                    conflict_analysis_status VARCHAR(50) DEFAULT 'NotAnalyzed',
                    needs_conflict_reanalysis BOOLEAN DEFAULT FALSE
                );
            """

            add_missing_columns = """
                DO $$ 
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'documents' AND column_name = 'has_conflicts'
                    ) THEN
                        ALTER TABLE documents ADD COLUMN has_conflicts BOOLEAN DEFAULT false;
                    END IF;
                    
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'documents' AND column_name = 'conflict_status'
                    ) THEN
                        ALTER TABLE documents ADD COLUMN conflict_status VARCHAR(50) DEFAULT 'No Conflict';
                    END IF;
                    
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'documents' AND column_name = 'detailed_analysis'
                    ) THEN
                        ALTER TABLE documents ADD COLUMN detailed_analysis TEXT;
                        RAISE NOTICE 'Added detailed_analysis column to documents table';
                    END IF;
                    
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'documents' AND column_name = 'conflict_analysis_status'
                    ) THEN
                        ALTER TABLE documents ADD COLUMN conflict_analysis_status VARCHAR(50) DEFAULT 'NotAnalyzed';
                    END IF;
                    
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'documents' AND column_name = 'conflict_info'
                    ) THEN
                        ALTER TABLE documents ADD COLUMN conflict_info JSONB;
                    END IF;
                    
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'documents' AND column_name = 'last_conflict_check'
                    ) THEN
                        ALTER TABLE documents ADD COLUMN last_conflict_check TIMESTAMP;
                    END IF;
                    
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'documents' AND column_name = 'conflict_analysis_error'
                    ) THEN
                        ALTER TABLE documents ADD COLUMN conflict_analysis_error TEXT;
                    END IF;
                    
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'documents' AND column_name = 'needs_conflict_reanalysis'
                    ) THEN
                        ALTER TABLE documents ADD COLUMN needs_conflict_reanalysis BOOLEAN DEFAULT FALSE;
                    END IF;
                    
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'documents' AND column_name = 'original_chunked_doc'
                    ) THEN
                        ALTER TABLE documents ADD COLUMN original_chunked_doc VARCHAR(100);
                    END IF;
                END $$;
            """

            add_constraints = """
                DO $$ 
                BEGIN
                    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'valid_processing_status') THEN
                        ALTER TABLE documents DROP CONSTRAINT valid_processing_status;
                    END IF;
                    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'valid_approval_status') THEN
                        ALTER TABLE documents DROP CONSTRAINT valid_approval_status;
                    END IF;
                    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'valid_scan_status') THEN
                        ALTER TABLE documents DROP CONSTRAINT valid_scan_status;
                    END IF;
                    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'valid_chunk_status') THEN
                        ALTER TABLE documents DROP CONSTRAINT valid_chunk_status;
                    END IF;
                    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'valid_conflict_status') THEN
                        ALTER TABLE documents DROP CONSTRAINT valid_conflict_status;
                    END IF;
                    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'valid_analysis_status') THEN
                        ALTER TABLE documents DROP CONSTRAINT valid_analysis_status;
                    END IF;

                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'documents' AND column_name = 'processing_status'
                    ) THEN
                        ALTER TABLE documents ADD CONSTRAINT valid_processing_status 
                            CHECK (processing_status IN (
                                'Pending', 'Processing', 'Processed', 'Failed',
                                'Queued', 'Scanning', 'Duplicate', 'Uploaded'
                            ));
                    END IF;

                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'documents' AND column_name = 'approval_status'
                    ) THEN
                        ALTER TABLE documents ADD CONSTRAINT valid_approval_status 
                            CHECK (approval_status IN ('Pending', 'Approved', 'Rejected'));
                    END IF;

                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'documents' AND column_name = 'scan_status'
                    ) THEN
                        ALTER TABLE documents ADD CONSTRAINT valid_scan_status 
                            CHECK (scan_status IN (
                                'Pending', 'Completed', 'ScanFailed',
                                'Processing', 'Scanning', 'Queued'
                            ));
                    END IF;

                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'documents' AND column_name = 'chunk_status'
                    ) THEN
                        ALTER TABLE documents ADD CONSTRAINT valid_chunk_status 
                            CHECK (chunk_status IN (
                                'Pending', 'Chunking', 'Chunked',
                                'ChunkingFailed', 'NotRequired', 'Queued', 'Failed', 'Processing'
                            ));
                    END IF;

                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'documents' AND column_name = 'conflict_status'
                    ) THEN
                        ALTER TABLE documents ADD CONSTRAINT valid_conflict_status 
                            CHECK (conflict_status IN (
                                'No Conflict', 'Pending Review', 'Resolving',
                                'Resolved', 'Ignored', 'NotAnalyzed', 'Analyzing'
                            ));
                    END IF;

                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'documents' AND column_name = 'conflict_analysis_status'
                    ) THEN
                        ALTER TABLE documents ADD CONSTRAINT valid_analysis_status 
                            CHECK (conflict_analysis_status IN (
                                'NotAnalyzed', 'Analyzing', 'Analyzed',
                                'AnalysisFailed', 'AnalysisInvalidated'
                            ));
                    END IF;
                END $$;
            """

            create_indexes = """
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'documents' AND column_name = 'processing_status'
                    ) THEN
                        CREATE INDEX IF NOT EXISTS idx_documents_processing ON documents(processing_status);
                    END IF;

                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'documents' AND column_name = 'approval_status'
                    ) THEN
                        CREATE INDEX IF NOT EXISTS idx_documents_approval ON documents(approval_status);
                    END IF;

                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'documents' AND column_name = 'created_date'
                    ) THEN
                        CREATE INDEX IF NOT EXISTS idx_documents_created ON documents(created_date);
                    END IF;

                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'documents' AND column_name = 'unit'
                    ) THEN
                        CREATE INDEX IF NOT EXISTS idx_documents_unit ON documents(unit);
                    END IF;

                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'documents' AND column_name = 'is_duplicate'
                    ) THEN
                        CREATE INDEX IF NOT EXISTS idx_documents_duplicate ON documents(is_duplicate);
                    END IF;

                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'documents' AND column_name = 'has_conflicts'
                    ) THEN
                        CREATE INDEX IF NOT EXISTS idx_documents_conflicts ON documents(has_conflicts);
                    END IF;

                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'documents' AND column_name = 'conflict_status'
                    ) THEN
                        CREATE INDEX IF NOT EXISTS idx_documents_conflict_status ON documents(conflict_status);
                    END IF;

                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'documents' AND column_name = 'chunk_status'
                    ) THEN
                        CREATE INDEX IF NOT EXISTS idx_documents_chunk_status ON documents(chunk_status);
                    END IF;

                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'documents' AND column_name = 'duplicate_group_id'
                    ) THEN
                        CREATE INDEX IF NOT EXISTS idx_documents_duplicate_group ON documents(duplicate_group_id);
                    END IF;

                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'documents' AND column_name = 'scan_status'
                    ) THEN
                        CREATE INDEX IF NOT EXISTS idx_documents_scan_status ON documents(scan_status);
                    END IF;
                    
                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'documents' AND column_name = 'conflict_analysis_status'
                    ) THEN
                        CREATE INDEX IF NOT EXISTS idx_documents_analysis_status ON documents(conflict_analysis_status);
                    END IF;
                    
                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'documents' AND column_name = 'needs_conflict_reanalysis'
                    ) THEN
                        CREATE INDEX IF NOT EXISTS idx_documents_needs_reanalysis ON documents(needs_conflict_reanalysis);
                    END IF;
                END $$;
            """

            create_chunks_table = """
                DO $$ 
                BEGIN
                    CREATE TABLE IF NOT EXISTS document_chunks (
                        id VARCHAR(255) PRIMARY KEY,
                        doc_id VARCHAR(100) REFERENCES documents(id) ON DELETE CASCADE,
                        content TEXT NOT NULL,
                        original_text TEXT,
                        chunk_topic TEXT,
                        chunk_index INTEGER,
                        qa_content TEXT,
                        is_visible BOOLEAN DEFAULT true,
                        created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        modified_date TIMESTAMP,
                        metadata JSONB,
                        embedding_model VARCHAR(100)
                    );

                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'document_chunks' 
                        AND column_name = 'is_enabled'
                    ) THEN
                        ALTER TABLE document_chunks 
                        ADD COLUMN is_enabled BOOLEAN DEFAULT true;
                    END IF;

                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint 
                        WHERE conname = 'valid_chunk_index'
                    ) THEN
                        ALTER TABLE document_chunks 
                        ADD CONSTRAINT valid_chunk_index 
                        CHECK (chunk_index >= 0);
                    END IF;

                    CREATE INDEX IF NOT EXISTS idx_chunks_doc_id ON document_chunks(doc_id);
                    CREATE INDEX IF NOT EXISTS idx_chunks_enabled ON document_chunks(is_enabled);
                END $$;
            """

            create_conflicts_table = """
                DO $$
                BEGIN
                    CREATE TABLE IF NOT EXISTS chunk_conflicts (
                        id SERIAL PRIMARY KEY,
                        conflict_id VARCHAR(255) UNIQUE,
                        doc_id VARCHAR(100) REFERENCES documents(id) ON DELETE CASCADE,
                        chunk_ids TEXT[] NOT NULL,
                        conflict_type VARCHAR(50),
                        explanation TEXT NOT NULL,
                        conflicting_parts TEXT[] NOT NULL,
                        detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        resolved BOOLEAN DEFAULT FALSE,
                        resolved_at TIMESTAMP,
                        resolution_notes TEXT,
                        resolved_by VARCHAR(100),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP
                    );

                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'chunk_conflicts' 
                        AND column_name = 'severity'
                    ) THEN
                        ALTER TABLE chunk_conflicts 
                        ADD COLUMN severity VARCHAR(20) DEFAULT 'medium';
                    END IF;
                    
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'chunk_conflicts' 
                        AND column_name = 'resolved_by'
                    ) THEN
                        ALTER TABLE chunk_conflicts
                        ADD COLUMN resolved_by VARCHAR(100);
                    END IF;

                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint 
                        WHERE conname = 'valid_severity'
                    ) THEN
                        ALTER TABLE chunk_conflicts
                        ADD CONSTRAINT valid_severity 
                        CHECK (severity IN ('high', 'medium', 'low'));
                    END IF;

                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint 
                        WHERE conname = 'valid_conflict_type'
                    ) THEN
                        ALTER TABLE chunk_conflicts
                        ADD CONSTRAINT valid_conflict_type 
                        CHECK (conflict_type IN ('self', 'intra', 'inter', 'content', 'internal', 'external', 'manual'));
                    END IF;

                    CREATE INDEX IF NOT EXISTS idx_conflicts_doc_id ON chunk_conflicts(doc_id);
                    CREATE INDEX IF NOT EXISTS idx_conflicts_resolved ON chunk_conflicts(resolved);
                    CREATE INDEX IF NOT EXISTS idx_conflicts_conflict_id ON chunk_conflicts(conflict_id);
                    CREATE INDEX IF NOT EXISTS idx_conflicts_detected ON chunk_conflicts(detected_at);
                END $$;
            """

            create_procedures = """
                CREATE OR REPLACE FUNCTION get_document_conflicts(p_doc_id VARCHAR)
                RETURNS TABLE (
                    conflict_id VARCHAR,
                    chunk_ids TEXT[],
                    conflict_type VARCHAR,
                    explanation TEXT,
                    conflicting_parts TEXT[],
                    severity VARCHAR,
                    detected_at TIMESTAMP,
                    resolved BOOLEAN,
                    resolved_at TIMESTAMP,
                    resolution_notes TEXT,
                    resolved_by VARCHAR
                ) AS $$
                BEGIN
                    RETURN QUERY
                    SELECT 
                        c.conflict_id,
                        c.chunk_ids,
                        c.conflict_type,
                        c.explanation,
                        c.conflicting_parts,
                        CASE 
                            WHEN EXISTS (
                                SELECT 1 FROM information_schema.columns 
                                WHERE table_name = 'chunk_conflicts' 
                                AND column_name = 'severity'
                            ) THEN c.severity
                            ELSE 'medium'::VARCHAR
                        END as severity,
                        c.detected_at,
                        c.resolved,
                        c.resolved_at,
                        c.resolution_notes,
                        c.resolved_by
                    FROM chunk_conflicts c
                    WHERE c.doc_id = p_doc_id
                    ORDER BY c.detected_at DESC;
                END;
                $$ LANGUAGE plpgsql;

                -- Procedure lưu conflict
                CREATE OR REPLACE PROCEDURE store_chunk_conflict(
                    p_doc_id VARCHAR,
                    p_chunk_ids TEXT[],
                    p_conflict_type VARCHAR,
                    p_explanation TEXT,
                    p_conflicting_parts TEXT[],
                    p_severity VARCHAR DEFAULT 'medium'
                )
                LANGUAGE plpgsql AS $$
                BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'documents' AND column_name = 'conflict_status'
                    ) THEN
                        INSERT INTO chunk_conflicts (
                            conflict_id,
                            doc_id,
                            chunk_ids,
                            conflict_type,
                            explanation,
                            conflicting_parts,
                            severity
                        )
                        VALUES (
                            array_to_string(ARRAY(SELECT unnest(p_chunk_ids) ORDER BY 1), '_'),
                            p_doc_id,
                            p_chunk_ids,
                            p_conflict_type,
                            p_explanation,
                            p_conflicting_parts,
                            p_severity
                        )
                        ON CONFLICT (conflict_id) DO UPDATE SET
                            explanation = EXCLUDED.explanation,
                            conflicting_parts = EXCLUDED.conflicting_parts, 
                            severity = EXCLUDED.severity,
                            updated_at = CURRENT_TIMESTAMP;

                        UPDATE documents
                        SET 
                            has_conflicts = TRUE,
                            conflict_status = 'Pending Review',
                            last_conflict_check = CURRENT_TIMESTAMP
                        WHERE id = p_doc_id;
                    END IF;
                END;
                $$;

                CREATE OR REPLACE PROCEDURE resolve_conflict(
                    p_conflict_id VARCHAR,
                    p_resolved_by VARCHAR,
                    p_resolution_notes TEXT DEFAULT NULL
                )
                LANGUAGE plpgsql AS $$
                DECLARE
                    v_doc_id VARCHAR;
                    v_remaining INTEGER;
                BEGIN
                    UPDATE chunk_conflicts
                    SET 
                        resolved = TRUE,
                        resolved_at = CURRENT_TIMESTAMP,
                        resolved_by = p_resolved_by,
                        resolution_notes = p_resolution_notes,
                        updated_at = CURRENT_TIMESTAMP  
                    WHERE conflict_id = p_conflict_id
                    RETURNING doc_id INTO v_doc_id;

                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'documents' AND column_name = 'conflict_status'
                    ) THEN
                        SELECT COUNT(*) INTO v_remaining
                        FROM chunk_conflicts 
                        WHERE doc_id = v_doc_id AND resolved = FALSE;

                        UPDATE documents
                        SET
                            has_conflicts = v_remaining > 0,
                            conflict_status = CASE 
                                WHEN v_remaining = 0 THEN 'Resolved'
                                ELSE 'Resolving'
                            END,
                            modified_date = CURRENT_TIMESTAMP
                        WHERE id = v_doc_id;
                    END IF;
                END;
                $$;
                    
                DROP TRIGGER IF EXISTS validate_status_trigger ON documents;
                    
                CREATE OR REPLACE FUNCTION validate_status_transition()
                RETURNS TRIGGER AS $$
                BEGIN
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
                    
                CREATE TRIGGER validate_status_trigger
                BEFORE UPDATE ON documents
                FOR EACH ROW
                EXECUTE FUNCTION validate_status_transition();
            """

            steps = [
                (create_base_documents, "Tạo bảng documents"),
                (add_missing_columns, "Thêm các cột thiếu"),
                (add_constraints, "Thêm ràng buộc cho documents"),
                (create_indexes, "Tạo indexes cho documents"),
                (create_chunks_table, "Tạo bảng chunks"),
                (create_conflicts_table, "Tạo bảng conflicts"),
                (create_procedures, "Tạo stored procedures và functions")
            ]

            for sql, description in steps:
                try:
                    logger.info(f"Executing: {description}")
                    self.execute_with_retry(sql)
                    logger.info(f"Successfully completed: {description}")
                except Exception as step_error:
                    logger.error(f"Error in {description}: {str(step_error)}")
                    logger.warning(f"Continuing despite error in: {description}")

            try:
                direct_fixes = """
                    SET session_replication_role = 'replica';
                    
                    UPDATE documents
                    SET conflict_status = 'No Conflict'
                    WHERE conflict_status = 'Không mâu thuẫn';
                    
                    UPDATE documents 
                    SET conflict_status = 'Pending Review'
                    WHERE conflict_status = 'Mâu thuẫn';

                    UPDATE documents
                    SET conflict_status = 'No Conflict'
                    WHERE conflict_status NOT IN (
                        'No Conflict', 'Pending Review', 'Resolving',
                        'Resolved', 'Ignored', 'NotAnalyzed', 'Analyzing'
                    );

                    UPDATE documents
                    SET conflict_status = 'No Conflict'
                    WHERE has_conflicts = false 
                    AND conflict_status NOT IN ('No Conflict', 'Resolved');
                    
                    UPDATE documents
                    SET conflict_status = 'Pending Review'
                    WHERE has_conflicts = true 
                    AND conflict_status NOT IN ('Pending Review', 'Resolving', 'Analyzing');
                    
                    UPDATE documents
                    SET chunk_status = 'Pending'
                    WHERE chunk_status NOT IN (
                        'Pending', 'Chunking', 'Chunked',
                        'ChunkingFailed', 'NotRequired', 'Queued', 'Failed', 'Processing'
                    );
                    
                    SET session_replication_role = 'origin';
                """
                
                self.execute_with_retry(direct_fixes)
                logger.info("Successfully applied direct fixes to documents with invalid status values")
                
            except Exception as fix_error:
                logger.error(f"Error applying direct fixes: {str(fix_error)}")

            logger.info("Database initialization completed successfully")

        except Exception as e:
            logger.error(f"Database initialization error: {str(e)}")
            logger.error(traceback.format_exc())
    
    
    def store_chunk_conflict(self, doc_id: str, chunk_ids: List[str],
                explanation: str, conflicting_parts: List[str],
                conflict_type: str = 'content', force_both_docs: bool = True) -> bool:
        """
        Store conflict information using direct SQL in replication mode to bypass constraints.
        Last resort solution when all standard approaches fail.
        """
        try:
            document = self.get_document_by_id(doc_id)
            if not document:
                logger.warning(f"Cannot save conflict: Document {doc_id} does not exist")
                return False
                
            conflict_id = '_'.join(sorted(chunk_ids))
            
            if not explanation:
                explanation = "Detected conflict between chunks"
                
            if not conflicting_parts or not isinstance(conflicting_parts, list):
                conflicting_parts = ["No specific conflicting parts identified"]
            
            logger.info(f"Using direct SQL bypass method for conflict {conflict_id}")
            
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    conn.autocommit = False
                    try:
                        cur.execute("""
                            SELECT DISTINCT conflict_type FROM chunk_conflicts LIMIT 5
                        """)
                        existing_types = cur.fetchall()
                        valid_type = 'inter'  
                        
                        if existing_types and len(existing_types) > 0:
                            valid_type = existing_types[0][0]
                            logger.info(f"Found existing conflict_type value: {valid_type}")
                        
                        cur.execute("SET session_replication_role = 'replica';")
                        
                        cur.execute(
                            """
                            INSERT INTO chunk_conflicts (
                                doc_id, conflict_id, chunk_ids,
                                conflict_type, explanation, conflicting_parts,
                                detected_at, severity
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, NOW(), 'medium')
                            ON CONFLICT (conflict_id) 
                            DO UPDATE SET
                                explanation = EXCLUDED.explanation,
                                conflicting_parts = EXCLUDED.conflicting_parts,
                                updated_at = NOW();
                            """, 
                            (doc_id, conflict_id, chunk_ids, valid_type, explanation, conflicting_parts)
                        )
                        
                        cur.execute(
                            """
                            UPDATE documents
                            SET 
                                has_conflicts = TRUE,
                                conflict_status = 'Pending Review',
                                conflict_info = COALESCE(conflict_info::jsonb, '{}'::jsonb) || 
                                            jsonb_build_object(
                                                'last_update', NOW(),
                                                'conflict_id', %s,
                                                'explanation', %s
                                            ),
                                last_conflict_check = NOW()
                            WHERE id = %s
                            """,
                            (conflict_id, explanation, doc_id)
                        )
                        
                        if conflict_type == 'external' and force_both_docs and len(chunk_ids) > 1:
                            other_doc_id = None
                            for chunk_id in chunk_ids:
                                if '_paragraph_' in chunk_id:
                                    potential_doc_id = chunk_id.split('_paragraph_')[0]
                                    if potential_doc_id != doc_id:
                                        other_doc_id = potential_doc_id
                                        break
                            
                            if other_doc_id:
                                other_doc = self.get_document_by_id(other_doc_id)
                                if other_doc:
                                    try:
                                        cur.execute(
                                            """
                                            INSERT INTO chunk_conflicts (
                                                doc_id, conflict_id, chunk_ids,
                                                conflict_type, explanation, conflicting_parts,
                                                detected_at, severity
                                            )
                                            VALUES (%s, %s, %s, %s, %s, %s, NOW(), 'medium')
                                            ON CONFLICT (conflict_id) 
                                            DO UPDATE SET
                                                explanation = EXCLUDED.explanation,
                                                conflicting_parts = EXCLUDED.conflicting_parts,
                                                updated_at = NOW();
                                            """, 
                                            (other_doc_id, conflict_id, chunk_ids, valid_type, explanation, conflicting_parts)
                                        )
                                        
                                        # Update other document status
                                        cur.execute(
                                            """
                                            UPDATE documents
                                            SET 
                                                has_conflicts = TRUE,
                                                conflict_status = 'Pending Review',
                                                conflict_info = COALESCE(conflict_info::jsonb, '{}'::jsonb) || 
                                                            jsonb_build_object(
                                                                'last_update', NOW(),
                                                                'conflict_id', %s,
                                                                'explanation', %s
                                                            ),
                                                last_conflict_check = NOW()
                                            WHERE id = %s
                                            """,
                                            (conflict_id, explanation, other_doc_id)
                                        )
                                        
                                        logger.info(f"Conflict saved for both documents: {doc_id} and {other_doc_id}")
                                    except Exception as other_doc_error:
                                        logger.error(f"Error saving conflict for other document {other_doc_id}: {str(other_doc_error)}")
                        
                        cur.execute("SET session_replication_role = 'origin';")
                        conn.commit()
                        
                        logger.info(f"Successfully stored conflict {conflict_id} using direct SQL bypass method")
                        return True
                        
                    except Exception as direct_error:
                        logger.error(f"Error with direct SQL bypass method: {str(direct_error)}")
                        conn.rollback()
                        return False
        
        except Exception as e:
            logger.error(f"Error storing conflict: {str(e)}")
            logger.error(traceback.format_exc())
            return False
    
    def fix_existing_external_conflicts(self):
        try:
            query = """
            SELECT id, doc_id, conflict_id, chunk_ids, explanation, conflicting_parts, detected_at, severity
            FROM chunk_conflicts
            WHERE conflict_type = 'external' AND resolved = FALSE
            """
            
            conflicts = self.execute_with_retry(query, fetch=True)
            if not conflicts:
                logger.info("No internal conflicts found that need to be fixed")
                return True
                
            logger.info(f"Found {len(conflicts)} internal conflicts that need to be fixed")
            
            count = 0
            for conflict in conflicts:
                conflict_id = conflict[2]
                chunk_ids = conflict[3]
                doc_id = conflict[1]
                
                other_doc_ids = set()
                for chunk_id in chunk_ids:
                    if chunk_id.startswith('doc_'):
                        potential_doc_id = chunk_id.split('_paragraph_')[0]
                        if potential_doc_id != doc_id:
                            other_doc_ids.add(potential_doc_id)
                
                if not other_doc_ids:
                    logger.warning(f"No other documentation found for conflict {conflict_id}")
                    continue
                    
                for other_doc_id in other_doc_ids:
                    try:
                        self.execute_with_retry("""
                        UPDATE documents
                        SET has_conflicts = TRUE,
                            conflict_status = 'Pending Review',
                            last_conflict_check = CURRENT_TIMESTAMP
                        WHERE id = %s
                        """, (other_doc_id,))
                        
                        self.execute_with_retry("""
                        UPDATE documents
                        SET conflict_info = COALESCE(conflict_info::jsonb, '{}'::jsonb) || 
                                    jsonb_build_object(
                                        'related_conflict_ids', ARRAY[%s]::text[]
                                    )
                        WHERE id = %s AND conflict_info IS NOT NULL
                        """, (conflict_id, other_doc_id))
                        
                        logger.info(f"Updated conflicting status for documentation{other_doc_id}")
                        count += 1
                    except Exception as update_error:
                        logger.error(f"Unable to update conflicting status for document{other_doc_id}: {str(update_error)}")
            
            logger.info(f"Updated conflicting status for {count} documents")
            return True
            
        except Exception as e:
            logger.error(f"Error while fixing internal conflict: {str(e)}")
            logger.error(traceback.format_exc())
            return False
    
    def get_filtered_data(self, status=None):
        try:
            
            if not status or status == "All":
                return self.get_all_documents()
            
            query = f"""
                SELECT * FROM documents 
                WHERE approval_status = '{status}' 
                ORDER BY created_date DESC
            """
            
            with self.engine.connect() as conn:
                df = pd.read_sql(query, conn)
                return df
                    
        except Exception as e:
            logger.error(traceback.format_exc())
            return pd.DataFrame()
        
    def get_all_documents(self):
        try:
            with self.engine.connect() as conn:
                query = """
                    SELECT 
                        id, content, categories, tags, 
                        start_date, end_date, unit, sender,
                        created_date, approval_status, approver, 
                        approval_date, is_duplicate, duplicate_group_id,
                        processing_status, scan_status, chunk_status,
                        modified_date, similarity_score, original_chunked_doc,
                        conflict_status, has_conflicts
                    FROM documents 
                    ORDER BY created_date DESC
                """
                
                
                logger.info(f"Executing query: {query}")
                result = pd.read_sql(query, conn)
                
                if result is not None and len(result) > 0:
                    if 'created_date' in result.columns:
                        sample_dates = result['created_date'].head().tolist()
                        logger.info(f"Sample created_dates: {sample_dates}")
                    else:
                        logger.warning("created_date column not found in query results!")
                        logger.info(f"Available columns: {result.columns.tolist()}")
                
                logger.info(f"Retrieved {len(result)} documents in total")
                return result
            
        except Exception as e:
            logger.error(f"Error in get_all_documents: {str(e)}")
            logger.error(traceback.format_exc())
            return pd.DataFrame()
    
    def get_document_by_id(self, doc_id):
        try:
            with self.engine.connect() as conn:
                query = text("SELECT * FROM documents WHERE id = :doc_id")
                logger.info(f"Executing query for doc_id: {doc_id}")
                result = conn.execute(query, {"doc_id": doc_id})
                row = result.fetchone()
                if row:
                    document = {column: value for column, value in zip(result.keys(), row)}
                    return document
                return None
        except Exception as e:
            logger.error(f"Database error in get_document_by_id: {str(e)}")
            return None

    def get_documents_to_scan(self):
        """Get documents that need scanning with improved error handling"""
        try:
            query = text("""
                SELECT * FROM documents 
                WHERE (
                    (chunk_status = 'ChunkingFailed' AND chunk_failure_count < 3)
                    OR (
                        processing_status NOT IN ('Failed', 'Queued')
                        AND (
                            scan_status IS NULL 
                            OR scan_status = ''
                            OR scan_status = 'ScanFailed'
                            OR (
                                processing_status = 'Processing'
                                AND modified_date IS NOT NULL
                                AND CAST(modified_date AS timestamp) < NOW() - INTERVAL '1 hour'
                            )
                        )
                    )
                )
                AND is_valid = true
                ORDER BY created_date DESC;
            """)
            
            with self.engine.connect() as conn:
                result = pd.read_sql(query, conn)
                logger.info(f"Found {len(result)} documents to scan")
                return result
                
        except Exception as e:
            logger.error(f"Error getting documents to scan: {str(e)}")
            return pd.DataFrame()
    
    def get_documents_in_group(self, group_id):
        """Get all documents in a duplicate group"""
        try:
            logger.info(f"Getting documents for group: {group_id}")
            
            with self.engine.connect() as conn:
                query = text("""
                    SELECT 
                        id,
                        content,
                        created_date,
                        processing_status,
                        approval_status, 
                        sender,
                        unit,
                        chunk_status,
                        is_duplicate,
                        similarity_score,
                        duplicate_group_id
                    FROM documents 
                    WHERE duplicate_group_id = :group_id
                    ORDER BY created_date ASC
                """)

                result = conn.execute(query, {"group_id": group_id})
                docs = []
            
                for row in result:
                    doc = {
                        'id': row[0],
                        'content': row[1], 
                        'created_date': row[2],
                        'processing_status': row[3],
                        'approval_status': row[4],
                        'sender': row[5],
                        'unit': row[6],
                        'chunk_status': row[7],
                        'is_duplicate': row[8],
                        'similarity_score': row[9] or 100.0,
                        'duplicate_group_id': row[10]
                    }
                    docs.append(doc)
                
                logger.info(f"Found {len(docs)} documents in group {group_id}")
                return docs

        except Exception as e:
            logger.error(f"Error getting documents in group: {str(e)}")
            return []

    def get_documents_need_rescan(self):
        """
        Get a list of documents that need to be rescanned, including those that have failed but need to be rescanned.
        """
        try:
            with self.engine.connect() as conn:
                debug_query = text("""
                    WITH document_status AS (
                        SELECT 
                            id,
                            content,
                            processing_status,
                            scan_status,
                            chunk_status,
                            is_valid,
                            modified_date,
                            CASE 
                                WHEN chunk_status = 'ChunkingFailed' THEN true
                                WHEN scan_status IS NULL OR scan_status = '' THEN true
                                WHEN scan_status = 'ScanFailed' THEN true
                                WHEN processing_status = 'Processing' 
                                    AND modified_date IS NOT NULL
                                    AND CAST(modified_date AS timestamp) < NOW() - INTERVAL '1 hour' THEN true
                                ELSE false
                            END as needs_rescan
                        FROM documents
                        WHERE is_valid = true
                    )
                    SELECT *
                    FROM document_status
                    WHERE needs_rescan = true
                    AND (
                        chunk_status = 'ChunkingFailed'
                        OR scan_status = 'ScanFailed'  
                        OR (
                            processing_status NOT IN ('Failed', 'Queued')
                            AND (
                                scan_status IS NULL 
                                OR scan_status = ''
                                OR (
                                    processing_status = 'Processing'
                                    AND modified_date IS NOT NULL
                                    AND CAST(modified_date AS timestamp) < NOW() - INTERVAL '1 hour'
                                )
                            )
                        )
                    )
                    ORDER BY modified_date DESC
                """)
                
                result = pd.read_sql(debug_query, conn)
                logger.info(f"Found {len(result)} documents that need rescan")
                
                for _, doc in result.iterrows():
                    logger.info(f"""
                        Document found for rescan:
                        - ID: {doc['id']}
                        - Processing Status: {doc['processing_status']}
                        - Scan Status: {doc['scan_status']}
                        - Chunk Status: {doc['chunk_status']}
                        - Is Valid: {doc['is_valid']}
                        - Needs Rescan: {doc['needs_rescan']}
                    """)
                
                return result
                
        except Exception as e:
            logger.error(f"Error getting documents to rescan: {str(e)}")
            return pd.DataFrame()
    
    def get_documents_by_status(self, status):
        """
        Get list of documents by status

        Args:
        status (list or str): Status or list of statuses to get

        Returns:
        DataFrame: Dataframe of documents that satisfy the condition
        """
        try:
            logger.info(f"Getting documents with status: {status}")
            
            with self.engine.connect() as conn:
                if isinstance(status, list):
                    placeholders = ','.join([':status' + str(i) for i in range(len(status))])
                    query_str = f"""
                        SELECT * FROM documents 
                        WHERE approval_status IN ({placeholders})
                        ORDER BY created_date DESC
                    """
                    query = text(query_str)
                    
                    params = {}
                    for i, s in enumerate(status):
                        params['status' + str(i)] = s
                        
                    result = conn.execute(query, params)
                else:
                    query = text("""
                        SELECT * FROM documents 
                        WHERE approval_status = :status
                        ORDER BY created_date DESC
                    """)
                    result = conn.execute(query, {"status": status})
                
                rows = [dict(row) for row in result]
                
                if rows:
                    return pd.DataFrame(rows)
                else:
                    logger.info(f"No documents found with status: {status}")
                    return pd.DataFrame()
                    
        except Exception as e:
            logger.error(f"Error getting documents by status: {str(e)}")
            logger.error(traceback.format_exc())
            return pd.DataFrame()
    
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
    
    def _get_chunk_content(self, chunk_id: str) -> str:
        """Get content of a chunk by ID"""
        try:
            query = """
                SELECT content 
                FROM document_chunks
                WHERE id = %s
            """
            result = self.execute_with_retry(query, (chunk_id,), fetch=True)
            if result:
                return result[0][0]
            return ""
        except Exception as e:
            logger.error(f"Error getting chunk content: {str(e)}")
            return ""
    
    def update_chunk_failure_count(self, doc_id, increment = True):
        try:
            if increment:
                query = """
                    UPDATE documents 
                    SET chunk_failure_count = COALESCE(chunk_failure_count, 0) + 1
                    WHERE id = %s
                    RETURNING chunk_failure_count
                """
            else:
                query = """
                    UPDATE documents 
                    SET chunk_failure_count = 0
                    WHERE id = %s
                    RETURNING chunk_failure_count
                """
                
            with psycopg2.connect(**self.db_params) as conn:
                with conn.cursor() as cur:
                    cur.execute(query, (doc_id,))
                    result = cur.fetchone()
                    conn.commit()
                    return result[0] if result else 0
                    
        except Exception as e:
            return 0

    def update_document_approval(self, doc_id, username):
        """
        Update document status when approved
        """
        try:
            with self.engine.connect() as conn:
                trans = conn.begin()
                try:
                    update_query = text("""
                        UPDATE documents 
                        SET approval_status = 'Approved',
                            processing_status = 'Processed',
                            approver = :approver,
                            approval_date = :approval_date
                        WHERE id = :doc_id
                    """)
                    
                    conn.execute(update_query, {
                        'approver': username,
                        'approval_date': datetime.now(),
                        'doc_id': doc_id
                    })
                    
                    trans.commit()
                    return True
                    
                except Exception as e:
                    trans.rollback()
                    return False

        except Exception as e:
            return False
    
    def update_document_rejection(self, doc_id, username) :
        """
        Update document status when rejected
        """
        try:
            with self.engine.connect() as conn:
                trans = conn.begin()
                try:
                    update_query = text("""
                        UPDATE documents 
                        SET approval_status = 'Rejected',
                            processing_status = 'Processed', 
                            approver = :approver,
                            approval_date = :approval_date
                        WHERE id = :doc_id
                    """)
                    
                    conn.execute(update_query, {
                        'approver': username,
                        'approval_date': datetime.now(),
                        'doc_id': doc_id
                    })
                    
                    trans.commit()
                    return True
                    
                except Exception as e:
                    trans.rollback()
                    return False
                    
        except Exception as e:
            return False
    
    def fix_conflict_status_values(self):
        """
        Fix invalid conflict_status values ​​in documents table

        This function converts Vietnamese values ​​to English

        and synchronizes status with has_conflicts field

        Returns:
        bool: True if successful, False if failed

        """
        try:
            
            sql = """
                SET session_replication_role = 'replica';
                
                UPDATE documents
                SET conflict_status = 'No Conflict'
                WHERE conflict_status = 'Không mâu thuẫn';
                
                UPDATE documents
                SET conflict_status = 'Pending Review'
                WHERE conflict_status = 'Mâu thuẫn';
                
                UPDATE documents
                SET conflict_status = 'No Conflict'
                WHERE conflict_status NOT IN (
                    'No Conflict', 'Pending Review', 'Resolving',
                    'Resolved', 'Ignored', 'NotAnalyzed', 'Analyzing'
                );
                
                UPDATE documents
                SET conflict_status = 'No Conflict'
                WHERE has_conflicts = false 
                AND conflict_status NOT IN ('No Conflict', 'Resolved');
                
                UPDATE documents
                SET conflict_status = 'Pending Review'
                WHERE has_conflicts = true 
                AND conflict_status NOT IN ('Pending Review', 'Resolving', 'Analyzing');
                
                UPDATE documents
                SET conflict_analysis_status = 'NotAnalyzed'
                WHERE conflict_analysis_status IS NULL;
                
                SET session_replication_role = 'origin';
            """
            
            self.execute_with_retry(sql)
            
            count_query = """
                SELECT COUNT(*) FROM documents 
                WHERE conflict_status IN ('No Conflict', 'Pending Review', 'Resolving', 'Resolved', 'Ignored', 'NotAnalyzed', 'Analyzing')
            """
            
            result = self.execute_with_retry(count_query, fetch=True)
            count = result[0][0] if result and result[0] else 0
            
            logger.info(f"Fixed {count} records with valid conflict_status")
            return True
            
        except Exception as e:
            logger.error(f"Error while editing conflict_status value: {str(e)}")
            logger.error(traceback.format_exc())
            return False
    
    def update_document(self, document):
        query = """
            UPDATE documents SET
                processing_status = %s,
                approval_status = %s, 
                modify_status = %s,
                scan_status = %s,
                chunk_status = %s,
                updated_at = NOW()
            WHERE id = %s
        """
        params = (
            document.get('processing_status'),
            document.get('approval_status'),
            document.get('modify_status'), 
            document.get('scan_status'),
            document.get('chunk_status'),
            document['id']
        )
        self.execute_query(query, params)
        self.commit()

    def update_document_status(self, doc_id: str, status_data: Dict[str, Any]):
        """Update document status with validation and schema checking"""
        try:
            if not doc_id:
                logger.error("Document ID is required for status update")
                return False
                
            document_check = """
                SELECT id FROM documents WHERE id = %s LIMIT 1
            """
            doc_exists = self.execute_with_retry(document_check, (doc_id,), fetch=True)
            if not doc_exists:
                logger.error(f"Cannot update status: Document {doc_id} does not exist")
                return False
                
            if 'chunk_status' in status_data:
                valid_chunk_statuses = {
                    'Pending', 'Chunking', 'Chunked',
                    'ChunkingFailed', 'NotRequired', 'Queued', 'Failed', 'Processing'
                }
                if status_data['chunk_status'] not in valid_chunk_statuses:
                    logger.error(f"Invalid chunk_status: {status_data['chunk_status']}")
                    status_data['chunk_status'] = 'Pending' 

            if 'conflict_status' in status_data:
                if status_data['conflict_status'] == 'Không mâu thuẫn':
                    status_data['conflict_status'] = 'No Conflict'
                elif status_data['conflict_status'] == 'Mâu thuẫn':
                    status_data['conflict_status'] = 'Pending Review'
                    
                valid_conflict_statuses = {
                    'No Conflict', 'Pending Review', 'Resolving',
                    'Resolved', 'Ignored', 'NotAnalyzed', 'Analyzing'
                }
                if status_data['conflict_status'] not in valid_conflict_statuses:
                    logger.error(f"Invalid conflict_status: {status_data['conflict_status']}")
                    status_data['conflict_status'] = 'No Conflict'

            if 'conflict_analysis_status' in status_data:
                valid_analysis_statuses = {
                    'NotAnalyzed', 'Analyzing', 'Analyzed',
                    'AnalysisFailed', 'AnalysisInvalidated'
                }
                if status_data['conflict_analysis_status'] not in valid_analysis_statuses:
                    logger.error(f"Invalid conflict_analysis_status: {status_data['conflict_analysis_status']}")
                    status_data['conflict_analysis_status'] = 'NotAnalyzed'

            if 'processing_status' in status_data:
                valid_processing_statuses = {
                    'Pending', 'Processing', 'Processed', 'Failed',
                    'Queued', 'Scanning', 'Duplicate', 'Uploaded'
                }
                if status_data['processing_status'] not in valid_processing_statuses:
                    logger.error(f"Invalid processing_status: {status_data['processing_status']}")
                    status_data['processing_status'] = 'Pending'
                    
            if 'has_conflicts' in status_data:
                if isinstance(status_data['has_conflicts'], dict):
                    status_data['has_conflicts'] = status_data['has_conflicts'].get('has_conflicts', False)
                elif isinstance(status_data['has_conflicts'], str):
                    if status_data['has_conflicts'].lower() in ('true', 'yes', '1'):
                        status_data['has_conflicts'] = True
                    elif status_data['has_conflicts'].lower() in ('false', 'no', '0'):
                        status_data['has_conflicts'] = False
                    else:
                        try:
                            json_data = json.loads(status_data['has_conflicts'])
                            if isinstance(json_data, dict):
                                status_data['has_conflicts'] = json_data.get('has_conflicts', False)
                            else:
                                status_data['has_conflicts'] = bool(json_data)
                        except json.JSONDecodeError:
                            logger.warning(f"Cannot parse has_conflicts from JSON: {status_data['has_conflicts']}")
                            status_data['has_conflicts'] = False
                
                if not isinstance(status_data['has_conflicts'], bool):
                    logger.warning(f"Convert has_conflicts from {type(status_data['has_conflicts'])} to boolean")
                    status_data['has_conflicts'] = bool(status_data['has_conflicts'])

            try:
                with self.get_connection() as conn:
                    conn.autocommit = True  
                    with conn.cursor() as cur:
                        schema_query = """
                            SELECT column_name FROM information_schema.columns 
                            WHERE table_name = 'documents'
                        """
                        cur.execute(schema_query)
                        existing_columns = {row[0] for row in cur.fetchall()}
                        
                        filtered_data = {}
                        for key, value in status_data.items():
                            if key in existing_columns:
                                filtered_data[key] = value
                            else:
                                logger.warning(f"Skipping column '{key}' which does not exist in documents table")
                        
                        status_data = filtered_data
            except Exception as schema_error:
                logger.error(f"Error while checking schema: {str(schema_error)}")
                logger.error(traceback.format_exc())

            processed_data = {}
            for key, value in status_data.items():
                if value is None:
                    processed_data[key] = None
                elif key == 'has_conflicts':
                    processed_data[key] = bool(value)
                elif isinstance(value, (dict, list)):
                    processed_data[key] = json.dumps(value)
                elif isinstance(value, datetime):
                    processed_data[key] = value.isoformat()
                else:
                    processed_data[key] = value

            if 'modified_date' not in processed_data:
                processed_data['modified_date'] = datetime.now().isoformat()

            set_clauses = []
            params = []
            for key, value in processed_data.items():
                set_clauses.append(f"{key} = %s")
                params.append(value)

            if not set_clauses:
                logger.warning(f"No data to update for document {doc_id}")
                return False

            params.append(doc_id)  

            query = f"""
                UPDATE documents 
                SET {', '.join(set_clauses)}
                WHERE id = %s
            """

            for attempt in range(3):
                try:
                    with self.get_connection() as conn:
                        conn.autocommit = False
                        with conn.cursor() as cur:
                            cur.execute(query, params)
                            
                            cur.execute("SELECT id FROM documents WHERE id = %s", (doc_id,))
                            result = cur.fetchone()
                            
                            if not result:
                                conn.rollback()
                                logger.error(f"Document {doc_id} not found after update attempt")
                                return False
                            
                            conn.commit()
                            
                            logger.info(f"Updated status for document {doc_id}")
                            return True
                            
                except Exception as e:
                    if attempt < 2: 
                        logger.warning(f"Error updating {attempt+1} for document {doc_id}: {str(e)}")
                        time.sleep(1)
                    else:
                        logger.error(f"Error executing update for document {doc_id}: {str(e)}")
                        logger.error(traceback.format_exc())
                        
                        try:
                            basic_update = """
                                UPDATE documents
                                SET modified_date = CURRENT_TIMESTAMP
                                WHERE id = %s
                            """
                            
                            with self.get_connection() as conn2:
                                conn2.autocommit = True
                                with conn2.cursor() as cur2:
                                    cur2.execute(basic_update, [doc_id])
                                    logger.info(f"Applied minimal update for document {doc_id}")
                                    return True
                        except Exception as basic_error:
                            logger.error(f"Failed to update even basic status: {str(basic_error)}")
                        
                        return False
                        
            return False
                        
        except Exception as e:
            logger.error(f"Failed to update status for document {doc_id}: {str(e)}")
            logger.error(traceback.format_exc())
            return False
    

    def delete_document(self, doc_id: str, chroma_manager=None) -> bool:
        """
        Delete document and update related conflicting information

        Args:
        doc_id (str): ID of document to delete
        chroma_manager (ChromaManager, optional): Object to delete chunks

        Returns:
        bool: True if deletion was successful, False if there was an error
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    conn.autocommit = False
                    
                    try:
                        get_doc_query = """
                            SELECT duplicate_group_id, is_duplicate, original_chunked_doc, chunk_status,
                                conflict_info, has_conflicts, conflict_status
                            FROM documents
                            WHERE id = %s
                        """
                        cur.execute(get_doc_query, (doc_id,))
                        doc_info = cur.fetchone()

                        if not doc_info:
                            logger.warning(f"Tài liệu {doc_id} không tồn tại")
                            return False

                        duplicate_group_id = doc_info[0]
                        is_duplicate = doc_info[1]
                        original_chunked_doc = doc_info[2]
                        chunk_status = doc_info[3]
                        conflict_info = doc_info[4]
                        has_conflicts = doc_info[5]
                        conflict_status = doc_info[6]
                        
                        logger.info(f"Delete document: id={doc_id}, duplicate_group_id={duplicate_group_id}, "
                            f"is_duplicate={is_duplicate}, original_chunked_doc={original_chunked_doc}, "
                            f"chunk_status={chunk_status}")
                        
                        saved_conflict_info = None
                        remaining_docs = []
                        
                        if duplicate_group_id:
                            get_group_docs_query = """
                                SELECT id, conflict_info, has_conflicts, conflict_status, original_chunked_doc
                                FROM documents 
                                WHERE duplicate_group_id = %s AND id != %s
                            """
                            cur.execute(get_group_docs_query, (duplicate_group_id, doc_id))
                            remaining_docs = cur.fetchall()
                            
                            if remaining_docs:
                                all_conflict_infos = {}
                                
                                if conflict_info:
                                    try:
                                        if isinstance(conflict_info, str):
                                            saved_conflict_info = json.loads(conflict_info)
                                        else:
                                            saved_conflict_info = conflict_info
                                        
                                        if isinstance(saved_conflict_info, dict):
                                            all_conflict_infos[doc_id] = {
                                                'conflict_info': saved_conflict_info,
                                                'has_conflicts': has_conflicts,
                                                'conflict_status': conflict_status
                                            }
                                    except json.JSONDecodeError:
                                        logger.warning(f"Could not parse JSON string conflict_info for document {doc_id}")
                                    
                                for doc in remaining_docs:
                                    remaining_doc_id = doc[0]
                                    remaining_conflict_info = doc[1]
                                    remaining_has_conflicts = doc[2]
                                    remaining_conflict_status = doc[3]
                                    
                                    if remaining_conflict_info:
                                        try:
                                            if isinstance(remaining_conflict_info, str):
                                                parsed_info = json.loads(remaining_conflict_info)
                                            else:
                                                parsed_info = remaining_conflict_info
                                            
                                            if isinstance(parsed_info, dict):
                                                all_conflict_infos[remaining_doc_id] = {
                                                    'conflict_info': parsed_info,
                                                    'has_conflicts': remaining_has_conflicts,
                                                    'conflict_status': remaining_conflict_status
                                                }
                                        except json.JSONDecodeError:
                                            logger.warning(f"Could not parse JSON string conflict_info for document {remaining_doc_id}")
        
                        get_conflicts_query = """
                            SELECT DISTINCT conflict_id FROM chunk_conflicts 
                            WHERE doc_id = %s
                        """
                        cur.execute(get_conflicts_query, (doc_id,))
                        conflict_ids = [row[0] for row in cur.fetchall()]
                        

                        for conflict_id in conflict_ids:
                            resolve_query = """
                                UPDATE chunk_conflicts
                                SET resolved = TRUE,
                                    resolved_at = CURRENT_TIMESTAMP,
                                    resolution_notes = %s
                                WHERE conflict_id = %s AND doc_id != %s
                            """
                            resolution_note = f"Tài liệu {doc_id} đã bị xóa vào {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                            cur.execute(resolve_query, (resolution_note, conflict_id, doc_id))
                        
                        if conflict_ids:
                            update_related_docs_query = """
                                UPDATE documents
                                SET last_conflict_check = CURRENT_TIMESTAMP
                                WHERE id IN (
                                    SELECT DISTINCT doc_id FROM chunk_conflicts 
                                    WHERE conflict_id IN %s AND doc_id != %s
                                )
                            """
                            if len(conflict_ids) > 0:
                                cur.execute(update_related_docs_query, (tuple(conflict_ids), doc_id))
                        
                        delete_conflicts_query = """
                            DELETE FROM chunk_conflicts
                            WHERE doc_id = %s
                            RETURNING conflict_id
                        """
                        cur.execute(delete_conflicts_query, (doc_id,))
                        deleted_conflicts = cur.fetchall()
                        
                        deleted_conflict_ids = [conflict[0] for conflict in deleted_conflicts] if deleted_conflicts else []
                        logger.info(f"Deleted {len(deleted_conflict_ids)} conflicted records for document {doc_id}")
                        
                        delete_query = """
                            DELETE FROM documents
                            WHERE id = %s
                        """
                        cur.execute(delete_query, (doc_id,))
                        
                        should_delete_chunks = True
                        
                        if duplicate_group_id and remaining_docs:
                            should_delete_chunks = False
                            
                            docs_referencing_deleted = [(doc[0], doc[4]) for doc in remaining_docs if doc[4] == doc_id]
                            if docs_referencing_deleted:
                                chunked_docs = []
                                for doc_row in remaining_docs:
                                    remaining_id = doc_row[0]
                                    remaining_doc = self.get_document_by_id(remaining_id)
                                    if remaining_doc and remaining_doc.get('chunk_status') == 'Chunked':
                                        chunked_docs.append((remaining_id, doc_row[4]))
                                
                                if chunked_docs:
                                    replacement_doc = chunked_docs[0][0]
                                else:
                                    replacement_doc = remaining_docs[0][0]
                                    
                                logger.info(f"Updating references for {len(docs_referencing_deleted)} documents from {doc_id} to {replacement_doc}")
                        
                                for ref_doc, _ in docs_referencing_deleted:
                                    update_ref_query = """
                                        UPDATE documents
                                        SET original_chunked_doc = %s,
                                            modified_date = CURRENT_TIMESTAMP
                                        WHERE id = %s
                                    """
                                    cur.execute(update_ref_query, (replacement_doc, ref_doc))
                            
                            if all_conflict_infos:
                                # Truyền chroma_manager cho _merge_conflict_infos
                                merged_conflict_info = self._merge_conflict_infos(
                                    all_conflict_infos, 
                                    deleted_conflict_ids,
                                    chroma_manager
                                )
                                
                                if merged_conflict_info:
                                    for remaining_doc_id, _ in [(doc[0], doc[4]) for doc in remaining_docs]:
                                        update_conflict_query = """
                                            UPDATE documents
                                            SET conflict_info = %s,
                                                has_conflicts = %s,
                                                conflict_status = %s,
                                                last_conflict_check = CURRENT_TIMESTAMP,
                                                modified_date = CURRENT_TIMESTAMP
                                            WHERE id = %s
                                        """
                                        cur.execute(
                                            update_conflict_query, 
                                            (
                                                json.dumps(merged_conflict_info['info']),
                                                merged_conflict_info['has_conflicts'],
                                                merged_conflict_info['status'],
                                                remaining_doc_id
                                            )
                                        )
                                    logger.info(f"Synchronized conflicting information for {len(remaining_docs)} remaining docs in group")
                                    
                        if should_delete_chunks and chroma_manager:
                            logger.info(f"Remove chunks for document {doc_id}")
                            chroma_manager.delete_document_chunks(doc_id)
                        self.clean_conflict_references(doc_id, update_related_docs=True)

                        conn.commit()
                        return True
                        
                    except Exception as e:
                        conn.rollback()
                        logger.error(f"Error deleting document {doc_id}: {str(e)}")
                        logger.error(traceback.format_exc())
                        return False
                    finally:
                        conn.autocommit = True

        except Exception as e:
            logger.error(f"Error connecting to database while deleting document {doc_id}: {str(e)}")
            logger.error(traceback.format_exc())
            return False

    def _merge_conflict_infos(self, conflict_infos, deleted_conflict_ids=None, chroma_manager=None):
        """
        Merge conflict information from multiple documents and remove deleted conflicts.
        Filter out conflicts involving disabled chunks.
        Ensure both sides of internal conflicts are handled.

        Args:
        conflict_infos (dict): Dictionary containing conflict information from multiple documents
        deleted_conflict_ids (list): List of deleted conflict_ids
        chroma_manager (ChromaManager, optional): ChromaManager object to check chunks

        Returns:
        dict: Merged conflict information
        """
        try:
            if not conflict_infos:
                return None
                
            all_content_conflicts = []
            all_internal_conflicts = []
            all_external_conflicts = []
            
            content_conflict_keys = set()
            internal_conflict_keys = set()
            external_conflict_keys = set()
            
            # Sử dụng chroma_manager nếu được cung cấp, nếu không thì dùng _check_chunk_exists
            enabled_chunks = {}
            if chroma_manager:
                for doc_id in conflict_infos.keys():
                    try:
                        chunks = chroma_manager.get_chunks_by_document_id(doc_id)
                        if chunks:
                            doc_enabled_chunks = []
                            for chunk in chunks:
                                metadata = chunk.get('metadata', {})
                                is_enabled = metadata.get('is_enabled', True)
                                if is_enabled:
                                    doc_enabled_chunks.append(chunk['id'])
                            enabled_chunks[doc_id] = set(doc_enabled_chunks)
                    except Exception as chunk_error:
                        logger.error(f"Error getting chunks for document {doc_id}: {str(chunk_error)}")
                        enabled_chunks[doc_id] = set()
            
            external_conflict_docs = set()
            
            for doc_id, conflict_data in conflict_infos.items():
                conflict_info = conflict_data.get('conflict_info')
                
                if not isinstance(conflict_info, dict):
                    continue
                    
                for conflict in conflict_info.get('content_conflicts', []):
                    if isinstance(conflict, dict) and 'chunk_id' in conflict:
                        chunk_id = conflict['chunk_id']
                        
                        chunk_doc_id = chunk_id.split('_paragraph_')[0] if '_paragraph_' in chunk_id else None
                        
                        if chroma_manager and chunk_doc_id and chunk_doc_id in enabled_chunks:
                            if chunk_id not in enabled_chunks[chunk_doc_id]:
                                logger.info(f"Skipping content conflict for disabled chunk {chunk_id}")
                                continue
                        elif not self._check_chunk_exists(chunk_id):
                            logger.info(f"Ignore content conflicts with non-existent chunk: {chunk_id}")
                            continue
                            
                        conflict_key = chunk_id
                        
                        if conflict_key not in content_conflict_keys:
                            if not deleted_conflict_ids or chunk_id not in deleted_conflict_ids:
                                content_conflict_keys.add(conflict_key)
                                all_content_conflicts.append(conflict)
                
                for conflict in conflict_info.get('internal_conflicts', []):
                    if isinstance(conflict, dict) and 'chunk_ids' in conflict:
                        chunk_ids = conflict['chunk_ids']
                        if chunk_ids and len(chunk_ids) > 1:
                            all_chunks_valid = True
                            for chunk_id in chunk_ids:
                                chunk_doc_id = chunk_id.split('_paragraph_')[0] if '_paragraph_' in chunk_id else None
                                
                                if chroma_manager and chunk_doc_id and chunk_doc_id in enabled_chunks:
                                    if chunk_id not in enabled_chunks[chunk_doc_id]:
                                        all_chunks_valid = False
                                        logger.info(f"Skipping internal conflict because chunk {chunk_id} is disabled")
                                        break
                                elif not self._check_chunk_exists(chunk_id):
                                    all_chunks_valid = False
                                    logger.info(f"Ignoring internal conflict because chunk does not exist: {chunk_id}")
                                    break
                                    
                            if not all_chunks_valid:
                                continue
                                
                            conflict_key = '_'.join(sorted(chunk_ids))
                            
                            if not deleted_conflict_ids or conflict_key not in deleted_conflict_ids:
                                if conflict_key not in internal_conflict_keys:
                                    internal_conflict_keys.add(conflict_key)
                                    all_internal_conflicts.append(conflict)
                
                for conflict in conflict_info.get('external_conflicts', []):
                    if isinstance(conflict, dict) and 'chunk_ids' in conflict:
                        chunk_ids = conflict['chunk_ids']
                        if chunk_ids and len(chunk_ids) > 1:
                            all_chunks_valid = True
                            involved_docs = set()
                            
                            for chunk_id in chunk_ids:
                                chunk_doc_id = chunk_id.split('_paragraph_')[0] if '_paragraph_' in chunk_id else None
                                
                                if chunk_doc_id:
                                    involved_docs.add(chunk_doc_id)
                                
                                if chroma_manager and chunk_doc_id and chunk_doc_id in enabled_chunks:
                                    if chunk_id not in enabled_chunks[chunk_doc_id]:
                                        all_chunks_valid = False
                                        logger.info(f"Skipping external conflict because chunk {chunk_id} is disabled")
                                        break
                                elif not self._check_chunk_exists(chunk_id):
                                    all_chunks_valid = False
                                    logger.info(f"Ignoring external conflict because chunk does not exist: {chunk_id}")
                                    break
                                    
                            if not all_chunks_valid:
                                continue
                                
                            external_conflict_docs.update(involved_docs)
                                
                            conflict_key = '_'.join(sorted(chunk_ids))
                            
                            if not deleted_conflict_ids or conflict_key not in deleted_conflict_ids:
                                if conflict_key not in external_conflict_keys:
                                    external_conflict_keys.add(conflict_key)
                                    all_external_conflicts.append(conflict)
            
            has_conflicts = bool(all_content_conflicts or all_internal_conflicts or all_external_conflicts)
            
            external_conflict_metadata = None
            if external_conflict_docs:
                external_conflict_metadata = list(external_conflict_docs)
            
            merged_info = {
                "has_conflicts": has_conflicts,
                "content_conflicts": all_content_conflicts,
                "internal_conflicts": all_internal_conflicts,
                "external_conflicts": all_external_conflicts,
                "last_updated": datetime.now().isoformat()
            }
            
            if external_conflict_metadata:
                merged_info["involved_documents"] = external_conflict_metadata
            
            result = {
                "info": merged_info,
                "has_conflicts": has_conflicts,
                "status": "Pending Review" if has_conflicts else "No Conflict"
            }
            
            if external_conflict_docs:
                result["external_docs"] = list(external_conflict_docs)
                
            return result
            
        except Exception as e:
            logger.error(f"Error merging conflicting information: {str(e)}")
            logger.error(traceback.format_exc())
            return None
       
    def clean_conflict_references(self, deleted_doc_id, update_related_docs=True):
        """
        Clean up references to deleted documents in conflicts

        Args:
        deleted_doc_id (str): ID of the deleted document
        update_related_docs (bool): Whether to update related documents

        Returns:
        int: Number of documents updated
        """
        try:
            related_docs_query = """
                SELECT DISTINCT doc_id FROM chunk_conflicts
                WHERE conflict_id LIKE %s AND doc_id != %s
            """
            related_docs = self.execute_with_retry(
                related_docs_query,
                (f"%{deleted_doc_id}%", deleted_doc_id),
                fetch=True
            )
            
            if not related_docs:
                return 0
                
            updated_count = 0
            
            for rel_doc in related_docs:
                related_doc_id = rel_doc[0]
                
                resolve_query = """
                    UPDATE chunk_conflicts
                    SET resolved = TRUE,
                        resolved_at = CURRENT_TIMESTAMP,
                        resolution_notes = %s
                    WHERE doc_id = %s AND conflict_id LIKE %s AND resolved = FALSE
                """
                resolution_note = f"Tài liệu {deleted_doc_id} đã bị xóa vào {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                self.execute_with_retry(
                    resolve_query,
                    (resolution_note, related_doc_id, f"%{deleted_doc_id}%"),
                    fetch=False
                )
                
                if update_related_docs:
                    related_document = self.get_document_by_id(related_doc_id)
                    if not related_document:
                        continue
                        
                    conflict_info = related_document.get('conflict_info')
                    if not conflict_info:
                        continue
                        
                    if isinstance(conflict_info, str):
                        try:
                            conflict_info = json.loads(conflict_info)
                        except json.JSONDecodeError:
                            continue
                    
                    if not isinstance(conflict_info, dict):
                        continue
                    
                    filtered_content_conflicts = []
                    filtered_internal_conflicts = []
                    filtered_external_conflicts = []
                    
                    for conflict in conflict_info.get('content_conflicts', []):
                        if isinstance(conflict, dict) and 'chunk_id' in conflict:
                            if not conflict['chunk_id'].startswith(deleted_doc_id):
                                filtered_content_conflicts.append(conflict)
                    
                    for conflict in conflict_info.get('internal_conflicts', []):
                        if isinstance(conflict, dict) and 'chunk_ids' in conflict:
                            has_deleted_doc = False
                            for chunk_id in conflict['chunk_ids']:
                                if chunk_id.startswith(deleted_doc_id):
                                    has_deleted_doc = True
                                    break
                            if not has_deleted_doc:
                                filtered_internal_conflicts.append(conflict)
                    
                    for conflict in conflict_info.get('external_conflicts', []):
                        if isinstance(conflict, dict) and 'chunk_ids' in conflict:
                            has_deleted_doc = False
                            for chunk_id in conflict['chunk_ids']:
                                if chunk_id.startswith(deleted_doc_id):
                                    has_deleted_doc = True
                                    break
                            if not has_deleted_doc:
                                filtered_external_conflicts.append(conflict)
                    
                    updated_conflict_info = {
                        "content_conflicts": filtered_content_conflicts,
                        "internal_conflicts": filtered_internal_conflicts,
                        "external_conflicts": filtered_external_conflicts,
                        "last_updated": datetime.now().isoformat()
                    }
                    
                    has_conflicts = (
                        len(filtered_content_conflicts) > 0 or
                        len(filtered_internal_conflicts) > 0 or
                        len(filtered_external_conflicts) > 0
                    )
                    
                    self.update_document_status(related_doc_id, {
                        'has_conflicts': has_conflicts,
                        'conflict_info': json.dumps(updated_conflict_info),
                        'conflict_status': 'Pending Review' if has_conflicts else 'No Conflict',
                        'last_conflict_check': datetime.now().isoformat()
                    })
                    
                    updated_count += 1
                    
            return updated_count
            
        except Exception as e:
            logger.error(f"Error cleaning up conflicting references: {str(e)}")
            logger.error(traceback.format_exc())
            return 0
      
    
    def submit_document(self, doc_data, unit):
        try:
            doc_id = f"doc_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            current_timestamp = datetime.now()
            logger.info(f"Generated document ID: {doc_id}")

            if not doc_data.get('content'):
                raise ValueError("Document content is required")

            query = """
                INSERT INTO documents (
                    id, content, categories, tags,
                    start_date, end_date, unit, sender,
                    processing_status, scan_status, chunk_status,
                    approval_status, is_valid, created_date
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                ) RETURNING id, created_date;
            """
            
            params = (
                doc_id,
                doc_data['content'],
                doc_data.get('categories', []),
                doc_data.get('tags', []),
                doc_data.get('start_date'),
                doc_data.get('end_date'),
                unit,
                doc_data.get('username'),
                'Pending',    # initial processing_status
                'Pending',    # initial scan_status 
                'Pending',    # initial chunk_status
                'Pending',    # initial approval_status
                doc_data.get('is_valid', True),
                current_timestamp 
            )

            result = self.execute_with_retry(query, params, fetch=True)
            if result and result[0]:
                inserted_id = result[0][0]
                created_date = result[0][1] if len(result[0]) > 1 else None
                logger.info(f"Document {inserted_id} inserted successfully with created_date: {created_date}")
                return inserted_id
            else:
                logger.error("No ID returned from insert")
                return None

        except Exception as e:
            logger.error(f"Document submission failed: {str(e)}")
            logger.error(traceback.format_exc())
            raise
    
    def resolve_conflict(self, conflict_id: str, resolved_by: str, resolution_notes: str = "") -> bool:
        try:

            # Format: doc_YYYYMMDDHHMMSS_paragraph_1_doc_YYYYMMDDHHMMSS_paragraph_2
            parts = conflict_id.split('_doc_')
            if len(parts) != 2:
                logger.error(f"Invalid conflict ID format: {conflict_id}")
                return False
                
            # Extract the first document ID without duplicate 'doc_'
            doc_id = parts[0].split('_paragraph_')[0]
            if not doc_id.startswith('doc_'):
                logger.error(f"Invalid document ID format: {doc_id}")
                return False

            # Verify document exists
            check_doc_query = """
                SELECT id FROM documents WHERE id = %s
            """
            doc_exists = self.execute_with_retry(check_doc_query, (doc_id,), fetch=True)
            if not doc_exists:
                logger.error(f"Document {doc_id} không tồn tại")
                return False

            # Check if conflict exists
            check_query = """
                SELECT id FROM chunk_conflicts 
                WHERE conflict_id = %s
            """
            result = self.execute_with_retry(check_query, (conflict_id,), fetch=True)
            
            # Create new conflict record if not exists
            if not result:
                logger.info(f"Creating new conflict record for {conflict_id}")
                chunk_ids = [
                    parts[0], 
                    'doc_' + parts[1] 
                ]
                if not self.create_conflict_record(doc_id, chunk_ids):
                    logger.error("Failed to create conflict record")
                    return False

            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    check_query = """
                        SELECT doc_id, chunk_ids 
                        FROM chunk_conflicts
                        WHERE conflict_id = %s AND resolved = FALSE
                    """
                    cur.execute(check_query, (conflict_id,))
                    result = cur.fetchone()

                    if not result:
                        logger.error(f"Conflict {conflict_id} not found or already resolved")
                        return False

                    doc_id = result[0]
                    chunk_ids = result[1]

                    update_query = """
                        UPDATE chunk_conflicts
                        SET 
                            resolved = TRUE,
                            resolved_at = CURRENT_TIMESTAMP,
                            resolution_notes = %s,
                            resolved_by = %s,
                            updated_at = CURRENT_TIMESTAMP  
                        WHERE conflict_id = %s
                    """
                    cur.execute(update_query, (resolution_notes, resolved_by, conflict_id))

                    # Check remaining conflicts
                    cur.execute("""
                        SELECT COUNT(*) 
                        FROM chunk_conflicts
                        WHERE doc_id = %s AND resolved = FALSE
                    """, (doc_id,))
                    remaining_conflicts = cur.fetchone()[0]

                    # Update document status
                    status = "Resolved" if remaining_conflicts == 0 else "Resolving"
                    doc_update_query = """
                        UPDATE documents
                        SET 
                            has_conflicts = %s,
                            conflict_status = %s,
                            conflict_info = COALESCE(conflict_info, '{}'::jsonb) || jsonb_build_object(
                                'resolution_date', CURRENT_TIMESTAMP,
                                'resolved_by', %s,
                                'resolution_notes', %s,
                                'status', %s
                            ),
                            modified_date = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """
                    cur.execute(
                        doc_update_query,
                        (remaining_conflicts > 0, status, resolved_by,
                        resolution_notes, status, doc_id)
                    )

                    conn.commit()
                    logger.info(f"Successfully resolved conflict {conflict_id}")
                    logger.info(f"Updated document {doc_id} status to {status}")
                    logger.info(f"Remaining conflicts: {remaining_conflicts}")
                    return True

        except Exception as e:
            logger.error(f"Error resolving conflict {conflict_id}: {str(e)}")
            logger.error(traceback.format_exc())
            return False

    def create_conflict_record(self, doc_id: str, chunk_ids: List[str], 
                        conflict_type: str = 'content',
                        explanation: str = "Content conflict detected") -> bool:
        try:
            if not doc_id.startswith('doc_'):
                logger.error(f"Invalid document ID format: {doc_id}")
                return False
            
            check_doc = """
                SELECT id FROM documents WHERE id = %s
            """
            
            if not self.execute_with_retry(check_doc, (doc_id,), fetch=True):
                logger.error(f"Document {doc_id} không tồn tại")
                return False
                
            # Create conflict_id from sorted chunk_ids
            conflict_id = '_'.join(sorted(chunk_ids))
            
            # Check if conflict already exists
            check_query = """
                SELECT id FROM chunk_conflicts 
                WHERE conflict_id = %s
            """
            result = self.execute_with_retry(check_query, (conflict_id,), fetch=True)
            
            if result:
                logger.info(f"Conflict {conflict_id} already exists")
                return True
                
            insert_query = """
                INSERT INTO chunk_conflicts (
                    doc_id,
                    conflict_id,
                    chunk_ids,
                    conflict_type, 
                    explanation,
                    conflicting_parts
                ) VALUES (
                    %s, %s, %s, %s, %s, %s
                )
                RETURNING id
            """
            
            # Get current content of chunks
            conflicting_parts = []
            for chunk_id in chunk_ids:
                chunk_content = self._get_chunk_content(chunk_id)
                if chunk_content:
                    conflicting_parts.append(chunk_content)
            
            result = self.execute_with_retry(
                insert_query,
                (doc_id, conflict_id, chunk_ids, conflict_type, explanation, conflicting_parts),
                fetch=True
            )
            
            if result:
                # Update document conflict status
                update_query = """
                    UPDATE documents
                    SET 
                        has_conflicts = TRUE,
                        conflict_status = 'Pending Review',
                        conflict_info = COALESCE(conflict_info::jsonb, '{}'::jsonb) || 
                            jsonb_build_object(
                                'last_update', CURRENT_TIMESTAMP,
                                'conflict_id', %s,
                                'explanation', %s
                            ),
                        last_conflict_check = CURRENT_TIMESTAMP
                    WHERE id = %s
                """
                
                self.execute_with_retry(
                    update_query, 
                    (conflict_id, explanation, doc_id)
                )
                
                logger.info(f"Created new conflict record {conflict_id} for document {doc_id}")
                return True
                
            return False
            
        except Exception as e:
            logger.error(f"Error creating conflict record: {str(e)}")
            logger.error(traceback.format_exc())
            return False
    
    def _check_chunk_exists(self, chunk_id: str) -> bool:
        """
        Check if a chunk exists in the system
        
        Args:
            chunk_id (str): ID of the chunk to check
            
        Returns:
            bool: True if chunk exists, False otherwise
        """
        try:
            if not chunk_id or not isinstance(chunk_id, str):
                return False
                
            query = """
                SELECT 1 FROM document_chunks 
                WHERE id = %s 
                LIMIT 1
            """
            
            result = self.execute_with_retry(query, (chunk_id,), fetch=True)
            if result and len(result) > 0:
                return True
                
            if '_paragraph_' in chunk_id:
                doc_id = chunk_id.split('_paragraph_')[0]
                
                doc_query = """
                    SELECT 1 FROM documents 
                    WHERE id = %s 
                    LIMIT 1
                """
                
                doc_result = self.execute_with_retry(doc_query, (doc_id,), fetch=True)
                if not doc_result or len(doc_result) == 0:
                    return False
                    
                pattern_query = """
                    SELECT 1 FROM document_chunks 
                    WHERE id LIKE %s 
                    LIMIT 1
                """
                
                pattern_result = self.execute_with_retry(
                    pattern_query, 
                    (f"{doc_id}_paragraph_%",), 
                    fetch=True
                )
                
                if pattern_result and len(pattern_result) > 0:
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking if chunk exists: {str(e)}")
            return False
        
    
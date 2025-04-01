from datetime import datetime, date
import json
import logging
from typing import Dict, Any, Optional
from dotenv import load_dotenv
from common.data_manager import DatabaseManager
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class UsageLogger:
    def __init__(self, chroma_manager=None):
        self.data_manager = DatabaseManager()
        self._init_usage_tables()

    def _init_usage_tables(self):
        try:
            create_api_usage_table = """
                CREATE TABLE IF NOT EXISTS api_usage (
                    id SERIAL PRIMARY KEY,
                    doc_id VARCHAR(100),
                    model VARCHAR(100) NOT NULL,
                    prompt_tokens INTEGER NOT NULL,
                    completion_tokens INTEGER NOT NULL,
                    total_tokens INTEGER NOT NULL,
                    input_cost NUMERIC(10, 6) NOT NULL,
                    output_cost NUMERIC(10, 6) NOT NULL,
                    total_cost NUMERIC(10, 6) NOT NULL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    operation VARCHAR(100),
                    metadata JSONB
                );
                
                CREATE INDEX IF NOT EXISTS idx_api_usage_doc_id ON api_usage(doc_id);
                CREATE INDEX IF NOT EXISTS idx_api_usage_model ON api_usage(model);
                CREATE INDEX IF NOT EXISTS idx_api_usage_timestamp ON api_usage(timestamp);
            """
            
            self.data_manager.execute_with_retry(create_api_usage_table)
            
        except Exception as e:
            raise
            
    def log_usage(self, doc_id: str, usage_data: Dict, model: str, operation: str = "processing") -> bool:
        try:
            if not usage_data:
                raise ValueError("Dados de uso ausentes")
                
            prompt_tokens = usage_data.get("prompt_tokens", 0)
            completion_tokens = usage_data.get("completion_tokens", 0)
            total_tokens = usage_data.get("total_tokens", 0)
            
            costs = usage_data.get("costs", {})
            input_cost = costs.get("input_cost", 0)
            output_cost = costs.get("output_cost", 0)
            total_cost = costs.get("total_cost", 0)
            
            metadata = usage_data.copy()
            if "costs" in metadata:
                del metadata["costs"]
            
            try:
                check_table_query = """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = 'api_usage'
                    );
                """
                result = self.data_manager.execute_with_retry(check_table_query, fetch=True)
                
                if not result or not result[0][0]:
                    self._init_usage_tables()
            except Exception as table_check_error:
                self._init_usage_tables()
            
            insert_query = """
                INSERT INTO api_usage (
                    doc_id, model, prompt_tokens, completion_tokens, total_tokens,
                    input_cost, output_cost, total_cost, operation, metadata
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                RETURNING id;
            """
            
            try:
                result = self.data_manager.execute_with_retry(
                    insert_query,
                    (
                        doc_id, model, prompt_tokens, completion_tokens, total_tokens,
                        input_cost, output_cost, total_cost, operation, json.dumps(metadata)
                    ),
                    fetch=True
                )
                
                if result:
                    return True
                else:
                    return False
            except Exception as insert_error:
                logger.error(f"Error {str(insert_error)}")
                
                
                log_entry = {
                    "timestamp": datetime.now().isoformat(),
                    "doc_id": doc_id,
                    "model": model,
                    "tokens": {
                        "prompt_tokens": prompt_tokens,
                        "completion_tokens": completion_tokens,
                        "total_tokens": total_tokens
                    },
                    "costs": {
                        "input_cost": input_cost,
                        "output_cost": output_cost,
                        "total_cost": total_cost
                    },
                    "operation": operation
                }
                logger.info(f"API Usage Fallback Log: {json.dumps(log_entry)}")
                return False
                
        except Exception as e:
            return False
        
    def _update_daily_summary(self, doc_id: str, date_str: str, model: str, tokens: Dict, costs: Dict) -> bool:
        """
        Update daily usage statistics

        Args:
        doc_id (str): ID of the document
        date_str (str): ISO format date (YYYY-MM-DD)
        model (str): AI model name
        tokens (dict): Tokens information
        costs (dict): Cost information

        Returns:
        bool: Success status
        """
        try:
            summary_id = f"summary_{date_str}"
            
            check_summary_query = """
                SELECT models_data, total_tokens, total_cost
                FROM api_usage_summary
                WHERE date = %s
            """
            
            result = self.data_manager.execute_with_retry(check_summary_query, (date_str,), fetch=True)
            
            total_tokens = tokens.get("total_tokens", 0)
            total_cost = costs.get("total_cost", 0.0)
            
            if result:
                existing_data = result[0]
                existing_models_data = existing_data[0] if existing_data[0] else {}
                existing_total_tokens = existing_data[1] or 0
                existing_total_cost = existing_data[2] or 0.0
                
                if not isinstance(existing_models_data, dict):
                    try:
                        existing_models_data = json.loads(existing_models_data)
                    except:
                        existing_models_data = {}
                
                if model not in existing_models_data:
                    existing_models_data[model] = {
                        "total_tokens": 0,
                        "requests": 0,
                        "total_cost": 0.0
                    }
                
                existing_models_data[model]["total_tokens"] = existing_models_data[model].get("total_tokens", 0) + total_tokens
                existing_models_data[model]["requests"] = existing_models_data[model].get("requests", 0) + 1
                existing_models_data[model]["total_cost"] = existing_models_data[model].get("total_cost", 0.0) + total_cost
                
                # Update total tokens and cost
                new_total_tokens = existing_total_tokens + total_tokens
                new_total_cost = existing_total_cost + total_cost
                
                update_query = """
                    UPDATE api_usage_summary
                    SET 
                        total_tokens = %s,
                        total_cost = %s,
                        models_data = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE date = %s
                """
                
                update_params = (
                    new_total_tokens,
                    new_total_cost,
                    json.dumps(existing_models_data),
                    date_str
                )
                
                self.data_manager.execute_with_retry(update_query, update_params)
                
            else:
                models_data = {
                    model: {
                        "total_tokens": total_tokens,
                        "requests": 1,
                        "total_cost": total_cost
                    }
                }
                
                insert_query = """
                    INSERT INTO api_usage_summary (
                        summary_id, date, total_tokens, total_cost, models_data
                    ) VALUES (
                        %s, %s, %s, %s, %s
                    )
                """
                
                insert_params = (
                    summary_id,
                    date_str,
                    total_tokens,
                    total_cost,
                    json.dumps(models_data)
                )
                
                self.data_manager.execute_with_retry(insert_query, insert_params)
            
            logger.info(f"Updated daily usage statistics for date {date_str}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating daily statistics: {str(e)}")
            return False

    def get_daily_summary(self, target_date: Optional[date] = None) -> Dict[str, Any]:
        """
        Get usage statistics for a specific day

        Args:
        target_date (date, optional): Date to get statistics. Default is today.

        Returns:
        dict: Usage statistics information
        """
        try:
            if target_date is None:
                target_date = date.today()
                
            target_date_str = target_date.isoformat()
            
            query = """
                SELECT
                    date,
                    total_tokens,
                    total_cost,
                    models_data,
                    created_at,
                    updated_at
                FROM api_usage_summary
                WHERE date = %s
            """
            
            result = self.data_manager.execute_with_retry(query, (target_date_str,), fetch=True)
            
            if result and result[0]:
                row = result[0]
                summary = {
                    "date": row[0].isoformat() if row[0] else target_date_str,
                    "total_tokens": row[1] or 0,
                    "total_cost": row[2] or 0.0,
                    "models": json.loads(row[3]) if row[3] else {},
                    "created_at": row[4].isoformat() if row[4] else None,
                    "updated_at": row[5].isoformat() if row[5] else None
                }
                return summary
                
            return {
                "date": target_date_str,
                "total_tokens": 0,
                "total_cost": 0.0,
                "models": {}
            }
            
        except Exception as e:
            return {
                "date": target_date.isoformat() if target_date else date.today().isoformat(),
                "total_tokens": 0,
                "total_cost": 0.0,
                "models": {},
                "error": str(e)
            }

    def get_usage_logs(self, doc_id: Optional[str] = None, start_date: Optional[date] = None, end_date: Optional[date] = None) -> list:
        """
        Get API usage history with filtering options

        Args:
        doc_id (str, optional): Filter by document ID
        start_date (date, optional): Start date
        end_date (date, optional): End date

        Returns:
        list: List of API usage records
        """
        try:
            conditions = []
            params = []
            
            if doc_id:
                conditions.append("doc_id = %s")
                params.append(doc_id)
                
            if start_date:
                conditions.append("date >= %s")
                params.append(start_date.isoformat())
                
            if end_date:
                conditions.append("date <= %s")
                params.append(end_date.isoformat())
            
            where_clause = ""
            if conditions:
                where_clause = "WHERE " + " AND ".join(conditions)
                
            query = f"""
                SELECT
                    log_id,
                    doc_id,
                    model,
                    prompt_tokens,
                    completion_tokens,
                    total_tokens,
                    input_cost,
                    output_cost,
                    total_cost,
                    timestamp,
                    date
                FROM api_usage_logs
                {where_clause}
                ORDER BY timestamp DESC
            """
            
            results = self.data_manager.execute_with_retry(query, params, fetch=True)
            
            logs = []
            if results:
                for row in results:
                    log = {
                        "log_id": row[0],
                        "doc_id": row[1],
                        "model": row[2],
                        "tokens": {
                            "prompt_tokens": row[3],
                            "completion_tokens": row[4],
                            "total_tokens": row[5]
                        },
                        "costs": {
                            "input_cost": row[6],
                            "output_cost": row[7],
                            "total_cost": row[8]
                        },
                        "timestamp": row[9].isoformat() if row[9] else None,
                        "date": row[10].isoformat() if row[10] else None
                    }
                    logs.append(log)
                    
            return logs
            
        except Exception as e:
            logger.error(f"Error when getting logs using API: {str(e)}")
            return []
    
    def get_usage_summary_range(self, start_date: Optional[date] = None, end_date: Optional[date] = None) -> Dict[str, Any]:
        """
        Get summary API usage statistics for a period

        Args:
        start_date (date, optional): Start date of the period
        end_date (date, optional): End date of the period

        Returns:
        dict: Summary information
        """
        try:
            conditions = []
            params = []
            
            if start_date:
                conditions.append("date >= %s")
                params.append(start_date.isoformat())
                
            if end_date:
                conditions.append("date <= %s")
                params.append(end_date.isoformat())
                
            where_clause = ""
            if conditions:
                where_clause = "WHERE " + " AND ".join(conditions)
                
            query = f"""
                SELECT
                    SUM(total_tokens) as total_tokens,
                    SUM(total_cost) as total_cost,
                    JSON_AGG(DISTINCT model) as models,
                    COUNT(DISTINCT doc_id) as doc_count,
                    MIN(date) as min_date,
                    MAX(date) as max_date
                FROM api_usage_logs
                {where_clause}
            """
            
            result = self.data_manager.execute_with_retry(query, params, fetch=True)
            
            if result and result[0]:
                row = result[0]
                
                model_query = f"""
                    SELECT
                        model,
                        SUM(total_tokens) as tokens,
                        COUNT(*) as requests,
                        SUM(total_cost) as cost
                    FROM api_usage_logs
                    {where_clause}
                    GROUP BY model
                    ORDER BY tokens DESC
                """
                
                model_results = self.data_manager.execute_with_retry(model_query, params, fetch=True)
                models_data = {}
                
                if model_results:
                    for m_row in model_results:
                        models_data[m_row[0]] = {
                            "total_tokens": m_row[1] or 0,
                            "requests": m_row[2] or 0,
                            "total_cost": m_row[3] or 0.0
                        }
                
                return {
                    "total_tokens": row[0] or 0,
                    "total_cost": row[1] or 0.0,
                    "models": models_data,
                    "document_count": row[3] or 0,
                    "start_date": row[4].isoformat() if row[4] else None,
                    "end_date": row[5].isoformat() if row[5] else None
                }
                
            return {
                "total_tokens": 0,
                "total_cost": 0.0,
                "models": {},
                "document_count": 0,
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None
            }
            
        except Exception as e:
            logger.error(f"Error while retrieving statistics using API: {str(e)}")
            return {
                "total_tokens": 0,
                "total_cost": 0.0,
                "models": {},
                "document_count": 0,
                "error": str(e)
            }
from typing import List, Dict, Optional, Union
from datetime import datetime
import json
import asyncio
import logging
from openai import OpenAI
import os
from dotenv import load_dotenv
import time
import hashlib
from common.models import ConflictResult

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class OpenAIConflictAnalyzer:
    """
    Contradiction analysis layer using OpenAI API within and between documents
    """
    def __init__(self, api_key: str = None, model: str = None, use_cache: bool = True, max_workers: int = 2):
        self.api_key = api_key or os.getenv('OPENAI_API_KEY')
        if not self.api_key:
            raise ValueError("OpenAI API key not found")

            
        self.client = OpenAI(api_key=self.api_key)
        self.model = model or os.getenv('MODEL')
        
        self.use_cache = use_cache
        self.cache = {}
        self.cache_hits = 0
        self.cache_misses = 0
        
        self.timeout = 30  
        self.max_retries = 3  
        self.retry_delay = 5 
        
        self.analyzed_pairs = set()
        
        logger.info(f"Initializing OpenAI Conflict Analyzer with model {self.model}")
        
    def _generate_cache_key(self, text1: str, text2: str = None, conflict_type: str = "content") -> str:
        """
        Generate cache key from text content

        Args:
        text1: First text content
        text2: Second text content (if any)
        conflict_type: Type of conflict to analyze

        Returns:
        str: Cache key
        """
        if text2:
            combined = f"{text1[:500]}|{text2[:500]}|{conflict_type}"
        else:
            combined = f"{text1[:1000]}|{conflict_type}"
        return hashlib.md5(combined.encode()).hexdigest()
    
    def _create_content_conflict_prompt(self, content: str) -> List[Dict]:
        """
        Create a prompt to analyze conflicts in a document

        Args:
        content: The text content to analyze

        Returns:
        List[Dict]: List of messages for the request
        """
        return [
            {
                "role": "system",
                "content": """Bạn là chuyên gia phân tích mâu thuẫn trong văn bản, giỏi nhận diện các logic không nhất quán.
                
                Nhiệm vụ của bạn là phân tích kỹ lưỡng và xác định TẤT CẢ các mâu thuẫn trong văn bản được cung cấp. 
                    
                Tập trung vào các loại mâu thuẫn sau:
                1. Các yêu cầu, điều kiện hoặc tiêu chí trái ngược
                2. Các số liệu, ngưỡng không nhất quán
                3. Các phát biểu mà không thể đồng thời đúng
                4. Thông tin xung đột về cùng một đối tượng hoặc chủ đề
                5. Các quy định hoặc chính sách mâu thuẫn nhau
                
                Hãy LIỆT KÊ và GIẢI THÍCH CHI TIẾT từng mâu thuẫn riêng biệt tìm thấy, không bỏ sót bất kỳ mâu thuẫn nào.
                Đánh số từng mâu thuẫn và giải thích cụ thể TẠI SAO đây là mâu thuẫn.
                
                Phân tích văn bản một cách CẨN THẬN, TOÀN DIỆN, không chủ quan.
                
                Trả lời theo cấu trúc JSON tiếng Việt sau:
                ```json
                {
                    "has_contradiction": "yes/no",
                    "contradiction_count": <số lượng mâu thuẫn tìm thấy>,
                    "contradictions": [
                        {
                            "id": 1,
                            "description": "Mô tả ngắn gọn về mâu thuẫn",
                            "explanation": "Giải thích chi tiết tại sao đây là mâu thuẫn",
                            "conflicting_parts": ["Phần 1 mâu thuẫn", "Phần 2 mâu thuẫn"]
                        },
                        // thêm các mâu thuẫn khác nếu có
                    ],
                    "explanation": "Tóm tắt tổng quan về các mâu thuẫn",
                    "conflicting_parts": ["Trích dẫn các phần mâu thuẫn chính"],
                    "conflict_type": "content"
                }
                ```
                
                Nếu không tìm thấy mâu thuẫn, trả về:
                ```json
                {
                    "has_contradiction": "no",
                    "contradiction_count": 0,
                    "contradictions": [],
                    "explanation": "Không tìm thấy mâu thuẫn trong văn bản",
                    "conflicting_parts": [],
                    "conflict_type": "content"
                }
                ```
                
                Chỉ trả về JSON hợp lệ, KHÔNG thêm bất kỳ giải thích nào bên ngoài cấu trúc JSON."""
            },
            {
                "role": "user",
                "content": f"VĂN BẢN CẦN PHÂN TÍCH:\n\n{content}"
            }
        ]

    async def analyze_conflict_async(self, content1: str, content2: str = None, conflict_type: str = "content") -> ConflictResult:
        """
        Asynchronous conflict analysis (using asyncio)

        Args:
        content1: First content to analyze
        content2: Second content (optional, if analyzing conflict between 2 paragraphs)
        conflict_type: Conflict type ("content", "internal", "external")

        Returns:
        ConflictResult: Conflict analysis result
        """
        try:
            if self.use_cache:
                cache_key = self._generate_cache_key(content1, content2, conflict_type)
                cached_result = self.cache.get(cache_key)
                if cached_result:
                    self.cache_hits += 1
                    logger.info(f"[Async] Cache hit ({self.cache_hits}/{self.cache_hits + self.cache_misses})")
                    return cached_result

                self.cache_misses += 1

            if conflict_type == "content":
                messages = self._create_content_conflict_prompt(content1)
            else:
                if not content2:
                    raise ValueError("Need second content to analyze conflict between paragraphs")
                messages = self._create_comparison_conflict_prompt(content1, content2, conflict_type)

            response = await asyncio.to_thread(
                self.client.chat.completions.create,
                model=self.model,
                messages=messages,
                temperature=0.1,
                response_format={"type": "json_object"},
                timeout=self.timeout
            )

            try:
                result_json = json.loads(response.choices[0].message.content)
            except json.JSONDecodeError as e:
                logger.error(f"[Async] Invalid JSON response: {str(e)}")
                logger.error(f"[Async] Raw response: {response.choices[0].message.content}")
                raise ValueError("Response is not valid JSON")

            result = self._process_result(result_json, conflict_type)

            if self.use_cache:
                self.cache[cache_key] = result

            return result

        except Exception as e:
            logger.error(f"[Async] Error analyzing conflict: {str(e)}")
            return ConflictResult(
                has_conflict=False,
                explanation=f"Lỗi khi phân tích bất đồng bộ: {str(e)}",
                conflicting_parts=[],
                analyzed_at=datetime.now(),
                chunk_ids=[],
                conflict_type=conflict_type,
                severity="medium"
            )

    def _create_comparison_conflict_prompt(self, content1: str, content2: str, conflict_type: str = "internal") -> List[Dict]:
        """
        Create a prompt to analyze conflicts between two text paragraphs

        Args:
        content1: First content
        content2: Second content
        conflict_type: Conflict type ("internal" or "external")

        Returns:
        List[Dict]: List of messages for the request
        """
        conflict_type_text = "trong cùng một tài liệu" if conflict_type == "internal" else "giữa các tài liệu khác nhau"
        
        return [
            {
                "role": "system",
                "content": f"""Bạn là chuyên gia phân tích mâu thuẫn {conflict_type_text}, giỏi nhận diện các mâu thuẫn logic và thông tin xung đột.
                
                Nhiệm vụ của bạn là phân tích kỹ lưỡng và xác định TẤT CẢ các mâu thuẫn giữa hai đoạn văn bản được cung cấp.
                
                Tập trung vào các loại mâu thuẫn sau:
                1. Thông tin trái ngược về cùng một chủ đề, sự kiện, hoặc đối tượng
                2. Các yêu cầu, điều kiện hoặc tiêu chí không nhất quán
                3. Số liệu, dữ liệu hoặc thống kê mâu thuẫn
                4. Khẳng định mà không thể đồng thời đúng
                5. Quy định, hướng dẫn hoặc chính sách xung đột
                
                Hãy PHÂN TÍCH TỪ TỪNG MẪU THUẪN riêng biệt và giải thích chi tiết tại sao chúng mâu thuẫn.
                Xếp hạng mức độ nghiêm trọng của mâu thuẫn (low/medium/high).
                
                Trả lời theo cấu trúc JSON tiếng Việt sau:
                ```json
                {{
                    "has_contradiction": "yes/no",
                    "contradiction_count": <số lượng mâu thuẫn tìm thấy>,
                    "contradictions": [
                        {{
                            "id": 1,
                            "description": "Mô tả ngắn gọn về mâu thuẫn",
                            "explanation": "Giải thích chi tiết tại sao đây là mâu thuẫn",
                            "conflicting_parts": ["Phần 1 mâu thuẫn", "Phần 2 mâu thuẫn"],
                            "severity": "low/medium/high"
                        }},
                        // thêm các mâu thuẫn khác nếu có
                    ],
                    "explanation": "Tóm tắt tổng quan về các mâu thuẫn",
                    "conflicting_parts": ["Trích dẫn phần mâu thuẫn chính từ nội dung 1", "Trích dẫn phần mâu thuẫn chính từ nội dung 2"],
                    "conflict_type": "{conflict_type}"
                }}
                ```
                
                Nếu không tìm thấy mâu thuẫn, trả về:
                ```json
                {{
                    "has_contradiction": "no",
                    "contradiction_count": 0,
                    "contradictions": [],
                    "explanation": "Không tìm thấy mâu thuẫn giữa hai đoạn văn bản",
                    "conflicting_parts": [],
                    "conflict_type": "{conflict_type}"
                }}
                ```
                
                Chỉ trả về JSON hợp lệ, KHÔNG thêm bất kỳ giải thích nào bên ngoài cấu trúc JSON."""
            },
            {
                "role": "user",
                "content": f"""NỘI DUNG 1:
                
                {content1}
                
                NỘI DUNG 2:
                
                {content2}"""
            }
        ]
        
    def analyze_conflict(self, content1: str, content2: str = None, conflict_type: str = "content") -> ConflictResult:
        """
        Conflict analysis with multiple conflict detection

        Args:
        content1: First content to analyze
        content2: Second content (optional, if analyzing conflict between 2 paragraphs)
        conflict_type: Conflict type ("content", "internal", "external")

        Returns:
        ConflictResult: Conflict analysis result
        """
        try:
            if self.use_cache:
                cache_key = self._generate_cache_key(content1, content2, conflict_type)
                cached_result = self.cache.get(cache_key)
                if cached_result:
                    self.cache_hits += 1
                    logger.info(f"Cache hit ({self.cache_hits}/{self.cache_hits + self.cache_misses})")
                    return cached_result
                    
                self.cache_misses += 1
            
            if conflict_type == "content":
                messages = self._create_content_conflict_prompt(content1)
            else:
                if not content2:
                    raise ValueError("Need second content to analyze conflict between paragraphs")
                messages = self._create_comparison_conflict_prompt(content1, content2, conflict_type)

            start_time = time.time()
            
            result_json = None
            for attempt in range(self.max_retries):
                try:
                    response = self.client.chat.completions.create(
                        model=self.model,
                        messages=messages,
                        temperature=0.1,
                        response_format={"type": "json_object"},
                        timeout=self.timeout
                    )
                    
                    try:
                        result_json = json.loads(response.choices[0].message.content)
                        execution_time = time.time() - start_time
                        logger.info(f"OpenAI analysis took {execution_time:.2f} seconds")
                        break
                    except json.JSONDecodeError as e:
                        logger.error(f"Invalid JSON response: {str(e)}")
                        logger.error(f"Raw response: {response.choices[0].message.content}")
                        if attempt == self.max_retries - 1:
                            raise
                        time.sleep(self.retry_delay)
                        continue
                        
                except Exception as e:
                    logger.error(f"API error on attempt {attempt+1}: {str(e)}")
                    if attempt == self.max_retries - 1:
                        raise
                    time.sleep(self.retry_delay)
            
            if not result_json:
                raise ValueError("No results received from conflict analysis")
                
            result = self._process_result(result_json, conflict_type)
            
            if self.use_cache:
                self.cache[cache_key] = result
            
            return result
            
        except Exception as e:
            logger.error(f"Error analyzing conflict: {str(e)}")
            return ConflictResult(
                has_conflict=False,
                explanation=f"Lỗi khi phân tích: {str(e)}",
                conflicting_parts=[],
                analyzed_at=datetime.now(),
                chunk_ids=[],
                conflict_type=conflict_type,
                severity="medium"
            )

    def _process_result(self, result_json: Dict, conflict_type: str) -> ConflictResult:
        """
        Process JSON result from API to convert into ConflictResult
        
        Args:
            result_json: JSON result from API
            conflict_type: Type of conflict
            
        Returns:
            ConflictResult: Normalized conflict analysis result
        """
        has_contradiction = result_json.get("has_contradiction", "no") == "yes"
        
        explanation = result_json.get("explanation", "No contradictions found")
        conflicting_parts = result_json.get("conflicting_parts", [])
        
        contradictions = result_json.get("contradictions", [])
        contradiction_count = result_json.get("contradiction_count", len(contradictions))
        
        actual_conflict_type = result_json.get("conflict_type", conflict_type)
        
        if has_contradiction and not contradictions:
            contradictions = [{
                "id": 1,
                "description": f"{actual_conflict_type} contradiction detected",
                "explanation": explanation,
                "conflicting_parts": conflicting_parts
            }]
        
        return ConflictResult(
            has_conflict=has_contradiction,
            explanation=explanation,
            conflicting_parts=conflicting_parts,
            analyzed_at=datetime.now(),
            chunk_ids=[],
            conflict_type=actual_conflict_type,
            severity="medium",
            contradictions=contradictions
        )
    
    def clear_cache(self):
        cache_size = len(self.cache)
        self.cache = {}
        self.cache_hits = 0
        self.cache_misses = 0
        logger.info(f"Cleared cache ({cache_size} entries)")
        
    def get_cache_stats(self):
        return {
            "cache_size": len(self.cache),
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "hit_ratio": self.cache_hits / (self.cache_hits + self.cache_misses) if (self.cache_hits + self.cache_misses) > 0 else 0
        }

    def shutdown(self):
        self.clear_cache()
        logger.info("OpenAI analyzer shutdown completed")
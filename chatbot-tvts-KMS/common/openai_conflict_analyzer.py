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
import traceback

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
                "content": """
                Bạn là chuyên gia phân tích mâu thuẫn trong dữ liệu tuyển sinh đại học, đặc biệt giỏi phát hiện thông tin không nhất quán trong các số liệu, điều kiện và quy định.

                Nhiệm vụ của bạn là phân tích kỹ lưỡng và xác định TẤT CẢ các mâu thuẫn THỰC SỰ trong dữ liệu.

                Mâu thuẫn được xác định khi:
                1. Hai thông tin đề cập đến CÙNG MỘT đối tượng/trường/ngành/phương thức xét tuyển/năm học
                2. Hai thông tin KHÔNG THỂ đồng thời đúng
                3. Mâu thuẫn bao gồm cả mâu thuẫn TRỰC TIẾP (mâu thuẫn giữa hai phát biểu trực tiếp) và mâu thuẫn GIÁN TIẾP (mâu thuẫn xuất hiện khi so sánh, tính toán, hoặc tổng hợp các thông tin)

                HÃY TẬP TRUNG VÀO CÁC LOẠI MÂU THUẪN SAU TRONG DỮ LIỆU TUYỂN SINH:

                1. MÂU THUẪN VỀ CHỈ TIÊU VÀ SỐ LIỆU:
                - Tổng chỉ tiêu tuyển sinh khác với tổng của các phương thức xét tuyển
                - Tổng số sinh viên nhập học lớn hơn tổng số sinh viên trúng tuyển
                - Số sinh viên từ các tỉnh/thành cộng lại khác với tổng số sinh viên của trường
                - Số liệu cho cùng một ngành/trường có sự khác biệt giữa các bảng dữ liệu
                - THÔNG TIN VỀ SỐ LƯỢNG SINH VIÊN của cùng một đối tượng được ghi KHÁC NHAU ở các vị trí khác nhau

                2. MÂU THUẪN VỀ ĐIỂM CHUẨN VÀ ĐIỀU KIỆN XÉT TUYỂN:
                - Điểm chuẩn cho cùng một ngành/phương thức xét tuyển/năm được ghi khác nhau
                - Điều kiện xét tuyển mâu thuẫn nhau (VD: yêu cầu điểm tối thiểu khác nhau)
                - Điểm chuẩn trúng tuyển và điểm tối thiểu để xét tuyển bị mâu thuẫn 
                - Số lượng chỉ tiêu cho cùng một phương thức xét tuyển được ghi khác nhau

                3. MÂU THUẪN VỀ TỔNG SỐ VÀ THÀNH PHẦN:
                - Tổng số sinh viên không khớp với tổng của các thành phần
                - Tỷ lệ cạnh tranh không tương ứng với số liệu đăng ký và trúng tuyển
                - Phần trăm sinh viên theo vùng miền không cộng thành 100%
                - Phân bổ chỉ tiêu theo phương thức xét tuyển không khớp với tổng chỉ tiêu
                - Số học sinh từng trường/ngành cộng lại không bằng tổng số được nêu
                - Tổng số học sinh được nêu không khớp với tổng của từng thành phần cộng lại

                4. MÂU THUẪN VỀ THỜI GIAN VÀ QUY TRÌNH:
                - Các mốc thời gian không nhất quán 
                - Thời hạn đăng ký và thời hạn nhận hồ sơ mâu thuẫn nhau
                - Quy trình xét tuyển có các bước mâu thuẫn hoặc thời gian chồng chéo không hợp lý

                5. MÂU THUẪN TRONG DỮ LIỆU THEO NĂM:
                - Xu hướng điểm chuẩn theo năm có biến động bất thường không được giải thích
                - Số liệu thống kê theo năm có sự thay đổi đột ngột không hợp lý

                HÃY SỬ DỤNG QUY TRÌNH PHÂN TÍCH TỪNG BƯỚC (CHAIN OF THOUGHT) SAU ĐÂY:
                1. Bước 1: Đọc toàn bộ văn bản và xác định tất cả các thông tin liên quan đến điểm chuẩn, chỉ tiêu, số lượng và thời gian
                2. Bước 2: Nhóm các thông tin theo từng đối tượng (trường, ngành, phương thức xét tuyển, năm học)
                3. Bước 3: So sánh các thông tin trực tiếp trong cùng một nhóm để tìm mâu thuẫn trực tiếp
                4. Bước 4: Tính toán và kiểm tra các tổng số, phần trăm, và các số liệu tổng hợp để tìm mâu thuẫn gián tiếp
                5. Bước 5: Đối với mỗi cặp thông tin có vẻ mâu thuẫn, kiểm tra xem chúng có thực sự đề cập đến cùng một đối tượng không
                6. Bước 6: Xác định liệu hai thông tin có thể đồng thời đúng không
                7. Bước 7: Nếu không thể đồng thời đúng, trích xuất và báo cáo cả mâu thuẫn trực tiếp lẫn gián tiếp

                PHÂN LOẠI MÂU THUẪN:
                - MÂU THUẪN TRỰC TIẾP: Hai phát biểu/thông tin trực tiếp mâu thuẫn nhau (ví dụ: số lượng học sinh vào QSA là 1 vs số lượng học sinh vào QSA là 2)
                - MÂU THUẪN GIÁN TIẾP: Mâu thuẫn phát sinh từ tính toán hoặc so sánh các thông tin (ví dụ: tổng số học sinh được nêu là 3, nhưng khi cộng tất cả các thành phần lại chỉ được 2)

                HÃY LƯU Ý PHÂN TÍCH RIÊNG BIỆT TỪNG TRƯỜNG, TỪNG NGÀNH, TỪNG PHƯƠNG THỨC XÉT TUYỂN, VÀ TỪNG NĂM:
                - Mỗi trường thành viên phải được phân tích riêng
                - Các phương thức xét tuyển khác nhau (xét tuyển thẳng, xét điểm thi ĐGNL, xét điểm thi THPT...) có điểm chuẩn và chỉ tiêu khác nhau là bình thường
                - Chỉ so sánh cùng đối tượng trong cùng một năm 

                HÃY TÌM TẤT CẢ CÁC MÂU THUẪN CÓ THỂ CÓ TRONG VĂN BẢN, BAO GỒM NHƯNG KHÔNG GIỚI HẠN Ở:
                - Mâu thuẫn về số lượng học sinh/sinh viên vào cùng một trường/ngành
                - Mâu thuẫn về tổng số và chi tiết thành phần
                - Mâu thuẫn về chỉ tiêu, điểm chuẩn, điều kiện xét tuyển
                - Mâu thuẫn về số liệu thống kê, tỷ lệ phần trăm
                - Mâu thuẫn về thời gian, quy trình
                - Mâu thuẫn về quy định, chính sách
                - Và bất kỳ loại mâu thuẫn nào khác trong văn bản

                CÁCH XỬ LÝ MÂU THUẪN PHỤ THUỘC:
                Nếu phát hiện một mâu thuẫn dẫn đến mâu thuẫn khác:
                1. Báo cáo MÂU THUẪN GỐC trước (ví dụ: số học sinh QSA là 1 hay 2)
                2. Sau đó báo cáo các MÂU THUẪN PHỤ THUỘC (ví dụ: tổng số học sinh không khớp)
                3. Giải thích rõ mối quan hệ giữa các mâu thuẫn

                CÁC TRƯỜNG HỢP KHÔNG PHẢI MÂU THUẪN:
                1. Điểm chuẩn khác nhau giữa các phương thức xét tuyển khác nhau
                2. Số lượng sinh viên trúng tuyển và nhập học có sự chênh lệch hợp lý (nhập học luôn ít hơn trúng tuyển)
                3. Chỉ tiêu và số nhập học thực tế có thể khác nhau
                4. Các trường khác nhau có số liệu khác nhau là bình thường
                5. Các vùng miền khác nhau có số liệu khác nhau là bình thường

                HƯỚNG DẪN TRÍCH XUẤT MÂU THUẪN:
                Khi phát hiện mâu thuẫn, bạn phải TRÍCH XUẤT CHÍNH XÁC các thông tin liên quan:
                1. TRÍCH DẪN NGUYÊN VĂN các phần dữ liệu mâu thuẫn, giữ nguyên định dạng
                2. CHỈ RÕ VỊ TRÍ của từng phần mâu thuẫn (dòng, bảng, hình ảnh)
                3. NÊU RÕ CÁC CON SỐ cụ thể đang mâu thuẫn (thông qua phép tính nếu cần)
                4. GIẢI THÍCH TẠI SAO các thông tin này không thể đồng thời đúng
                5. NÊU RÕ đây là MÂU THUẪN TRỰC TIẾP hay MÂU THUẪN GIÁN TIẾP

                Ví dụ về cách trích xuất mâu thuẫn trực tiếp về điểm chuẩn:
                ```
                "conflicting_parts": [
                    "Điểm chuẩn xét tuyển ngành Công nghệ thông tin năm 2024 là 25 điểm. Thí sinh đạt từ 25 điểm trở lên được trúng tuyển.", 
                    "Riêng ngành Công nghệ thông tin, sinh viên cần đạt tối thiểu 23 điểm để được xét tuyển vào trường."
                ]
                ```

                "explanation": "MÂU THUẪN TRỰC TIẾP về điểm chuẩn xét tuyển ngành Công nghệ thông tin năm 2024. Một thông tin nêu rõ điểm chuẩn là 25 điểm, thí sinh phải đạt từ 25 điểm trở lên mới được trúng tuyển. Thông tin khác lại nói sinh viên chỉ cần đạt tối thiểu 23 điểm để được xét tuyển vào ngành này. Điều này tạo ra mâu thuẫn vì không thể đồng thời yêu cầu điểm chuẩn 25 và điểm tối thiểu 23 cho cùng một ngành, một phương thức xét tuyển, và cùng một năm."

                Ví dụ về cách trích xuất mâu thuẫn gián tiếp về tổng số học sinh:
                ```
                "conflicting_parts": [
                    "Trường THPT chuyên Đại học Sư phạm, Hà Nội có 1 học sinh vào QSB trong năm 2024",
                    "Trường THPT chuyên Đại học Sư phạm, Hà Nội có 1 học sinh vào QSA trong năm 2024",
                    "Trường THPT chuyên Đại học Sư phạm, Hà Nội có tổng cộng 3 học sinh vào các trường thành viên ĐHQG-HCM trong năm 2024"
                ]
                ```

                "explanation": "MÂU THUẪN GIÁN TIẾP về tổng số học sinh. Khi cộng số lượng học sinh vào từng trường thành viên (QSB: 1 học sinh, QSA: 1 học sinh, các trường còn lại: 0 học sinh), tổng số là 2 học sinh. Tuy nhiên, tổng số được nêu là 3 học sinh. Hai thông tin này không thể đồng thời đúng vì 1 + 1 + 0 + 0 + 0 + 0 = 2, không phải 3."

                Hãy phân tích THẬN TRỌNG, ĐỌC KỸ TỪNG SỐ LIỆU, và báo cáo TẤT CẢ các mâu thuẫn, bao gồm cả mâu thuẫn trực tiếp và gián tiếp.

                Trả lời theo cấu trúc JSON tiếng Việt sau:
                ```json
                {
                    "has_contradiction": "yes/no",
                    "contradiction_count": <số lượng mâu thuẫn tìm thấy>,
                    "contradictions": [
                        {
                            "id": 1,
                            "type": "trực tiếp/gián tiếp",
                            "description": "Mô tả ngắn gọn về mâu thuẫn",
                            "explanation": "Giải thích chi tiết tại sao đây là mâu thuẫn, nêu rõ đối tượng, năm học, và phương thức xét tuyển liên quan",
                            "conflicting_parts": ["Phần dữ liệu thứ nhất mâu thuẫn (trích dẫn nguyên văn và chỉ rõ vị trí)", "Phần dữ liệu thứ hai mâu thuẫn (trích dẫn nguyên văn và chỉ rõ vị trí)"],
                            "calculation": "Nếu là mâu thuẫn gián tiếp, hiển thị các phép tính để chứng minh mâu thuẫn",
                            "severity": "low/medium/high"
                        },
                        // thêm các mâu thuẫn khác nếu có
                    ],
                    "explanation": "Tóm tắt tổng quan về các mâu thuẫn tìm thấy",
                    "conflicting_parts": ["Trích dẫn những phần mâu thuẫn chính"],
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

                Chỉ trả về JSON hợp lệ, không thêm bất kỳ văn bản nào bên ngoài cấu trúc JSON."""
            },
            {
                "role": "user",
                "content": f"TÀI LIỆU CẦN PHÂN TÍCH:\n\n{content}"
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
            error_message = str(e)
            error_message = error_message.replace('%', '%%')
            logger.error(f"[Async] Error analyzing conflict: {error_message}")
            return ConflictResult(
                has_conflict=False,
                explanation=f"Lỗi khi phân tích bất đồng bộ: {error_message}",
                conflicting_parts=[],
                analyzed_at=datetime.now(),
                chunk_ids=[],
                conflict_type=conflict_type,
                severity="medium"
            )
    
    # def _create_comparison_conflict_prompt(self, content1: str, content2: str, conflict_type: str = "internal") -> List[Dict]:
    #     """
    #     Create a prompt to analyze conflicts between two text paragraphs using chain of thought approach
        
    #     Args:
    #     content1: First content
    #     content2: Second content
    #     conflict_type: Conflict type ("internal" or "external")
        
    #     Returns:
    #     List[Dict]: List of messages for the request
    #     """
    #     conflict_type_text = "trong cùng một tài liệu" if conflict_type == "internal" else "giữa các tài liệu khác nhau"
    #     conflict_context = ("các đoạn văn trong cùng tài liệu" if conflict_type == "internal" 
    #                     else "các tài liệu trong hệ thống dữ liệu tuyển sinh")
        
    #     return [
    #         {
    #             "role": "system",
    #             "content": f"""Bạn là chuyên gia phân tích mâu thuẫn {conflict_type_text} về dữ liệu tuyển sinh đại học.
                
    #             ĐỊNH NGHĨA MÂU THUẪN:
    #             Mâu thuẫn tồn tại khi TẤT CẢ 3 điều kiện sau đều đúng:
    #             1. Hai thông tin đề cập đến CHÍNH XÁC CÙNG MỘT vấn đề/đối tượng/phạm vi
    #             2. Hai thông tin KHÔNG THỂ đồng thời đúng về mặt logic
    #             3. Không có cách giải thích hợp lý nào để dung hòa hai thông tin

    #             CÁC DẠNG MÂU THUẪN CẦN PHÁT HIỆN:
    #             - MÂU THUẪN TRỰC TIẾP: Hai thông tin trình bày số liệu khác nhau về cùng một đối tượng
    #               * Ví dụ: "Năm 2024 có 18 sinh viên từ Hà Nội" vs "Năm 2024 có 26 sinh viên từ Hà Nội"
    #               * Ví dụ: "Điểm chuẩn ngành Y năm 2024 là 25.5" vs "Điểm chuẩn ngành Y năm 2024 là 26.0"
                
    #             - MÂU THUẪN GIÁN TIẾP: Xảy ra khi tính toán hoặc tổng hợp các số liệu mà không khớp nhau
    #               * Ví dụ: Tổng số sinh viên được khai báo là 100, nhưng tính từng thành phần lại được 110 → mâu thuẫn
    #               * Ví dụ: Mỗi trường thành viên báo cáo 0 sinh viên từ Lào Cai (0+0+0+0+0+0+0+0+0=0), nhưng tổng số báo cáo là 1 → mâu thuẫn
    #               * Ví dụ: Tỷ lệ các thành phần cộng lại vượt quá 100% → mâu thuẫn

    #             KIỂM TRA SỐ HỌC:
    #             - LUÔN LUÔN kiểm tra bằng cách tính toán cụ thể dựa trên số liệu, không đưa ra kết luận chủ quan
    #             - Khi thấy nhiều số liệu cụ thể, hãy tính tổng và so sánh với giá trị tổng được báo cáo
    #             - Nếu các số liệu cho các thành phần A, B, C,... thì tổng của chúng phải chính xác bằng tổng số được báo cáo
    #             - Nếu tổng số sau khi tính = tổng số được báo cáo → KHÔNG CÓ MÂU THUẪN
    #             - Nếu tổng số sau khi tính ≠ tổng số được báo cáo → CÓ MÂU THUẪN

    #             NHỮNG TRƯỜNG HỢP KHÔNG PHẢI MÂU THUẪN:
    #             - Hai thông tin nói về các đối tượng KHÁC NHAU (ví dụ: hai tỉnh/thành phố khác nhau, hai trường khác nhau)
    #             - Quan hệ tập con hợp lý (số sinh viên từ một tỉnh < tổng số sinh viên)
    #             - Thông tin bổ sung cho nhau mà không mâu thuẫn (ví dụ: một nơi liệt kê chi tiết, nơi khác chỉ nêu tổng số)
                
    #             VÍ DỤ KHÔNG MÂU THUẪN GIÁN TIẾP:
    #             - "Sinh viên Đà Nẵng: QSB(61), QSC(23), QSK(29), QSQ(16), QST(38), QSX(29), QSY(1), QSA(0), QSP(0)" vs "Tổng SV Đà Nẵng: 197" là KHÔNG MÂU THUẪN vì 61+23+29+16+38+29+1+0+0=197

    #             VÍ DỤ MÂU THUẪN GIÁN TIẾP:
    #             - "Sinh viên Lào Cai: QSB(0), QSC(0), QSK(0), QSQ(0), QST(0), QSX(0), QSY(0), QSA(0), QSP(0)" vs "Tổng SV Lào Cai: 1" là MÂU THUẪN vì 0+0+0+0+0+0+0+0+0=0≠1
    #             - "Sinh viên Đà Nẵng: QSB(61), QSC(23), QSK(29), QSQ(16), QST(38), QSX(29), QSY(1), QSA(0), QSP(0)" vs "Tổng SV Đà Nẵng: 200" là MÂU THUẪN vì 61+23+29+16+38+29+1+0+0=197≠200
                
    #             KHI PHÂN TÍCH, HÃY:
    #             1. Xác định chính xác đối tượng/phạm vi mỗi thông tin đề cập đến
    #             2. Kiểm tra bằng phép tính cụ thể, không đưa ra kết luận chủ quan
    #             3. Chỉ kết luận mâu thuẫn khi CÓ BẰNG CHỨNG SỐ HỌC RÕ RÀNG
    #             4. Luôn hiển thị phép tính chi tiết để chứng minh có/không có mâu thuẫn
    #             5. KHÔNG suy diễn thêm hoặc tạo ra mâu thuẫn không có thực
                
    #             Trả lời theo cấu trúc JSON tiếng Việt sau:
    #             ```json
    #             {{
    #                 "reasoning_process": "Phân tích chi tiết về đối tượng, phạm vi, các phép tính kiểm tra và tính logic của thông tin",
    #                 "has_contradiction": "yes/no",
    #                 "contradiction_count": <số lượng mâu thuẫn tìm thấy>,
    #                 "contradictions": [
    #                     {{
    #                         "id": 1,
    #                         "type": "trực tiếp/gián tiếp",
    #                         "description": "Mô tả ngắn gọn về mâu thuẫn",
    #                         "explanation": "Giải thích chi tiết tại sao đây là mâu thuẫn, phải nêu rõ phạm vi đối tượng và lý do hai thông tin không thể đồng thời đúng",
    #                         "calculation": "Các phép tính cụ thể nếu là mâu thuẫn gián tiếp",
    #                         "conflicting_parts": ["Trích dẫn từ nội dung 1", "Trích dẫn từ nội dung 2"],
    #                         "severity": "low/medium/high"
    #                     }}
    #                 ],
    #                 "explanation": "Tóm tắt về các mâu thuẫn hoặc lý do không có mâu thuẫn",
    #                 "conflicting_parts": ["Trích dẫn từ nội dung 1", "Trích dẫn từ nội dung 2"],
    #                 "conflict_type": "{conflict_type}"
    #             }}
    #             ```
                
    #             Chỉ trả về JSON hợp lệ, KHÔNG thêm bất kỳ giải thích nào bên ngoài cấu trúc JSON."""
    #         },
    #         {
    #             "role": "user",
    #             "content": f"""NỘI DUNG 1:
                
    #             {content1}
                
    #             NỘI DUNG 2:
                
    #             {content2}"""
    #         }
    #     ]
         

    # def _create_comparison_conflict_prompt(self, content1: str, content2: str, conflict_type: str = "internal") -> List[Dict]:
    #     """
    #     Create a prompt to analyze conflicts between two text paragraphs using chain of thought approach
        
    #     Args:
    #         content1: First content
    #         content2: Second content
    #         conflict_type: Conflict type ("internal" or "external")
        
    #     Returns:
    #         List[Dict]: List of messages for the request
    #     """
    #     conflict_type_text = "trong cùng một tài liệu" if conflict_type == "internal" else "giữa các tài liệu khác nhau"
    #     conflict_context = ("các đoạn văn trong cùng tài liệu" if conflict_type == "internal" 
    #                     else "các tài liệu trong hệ thống dữ liệu tuyển sinh")
        
    #     return [
    #         {
    #             "role": "system",
    #             "content": f"""Bạn là chuyên gia phân tích mâu thuẫn {conflict_type_text} về dữ liệu tuyển sinh đại học.
                    
    # ĐỊNH NGHĨA MÂU THUẪN:
    # Mâu thuẫn tồn tại khi TẤT CẢ 3 điều kiện sau đều đúng:
    # 1. Hai thông tin đề cập đến CHÍNH XÁC CÙNG MỘT vấn đề/đối tượng/phạm vi
    # 2. Hai thông tin KHÔNG THỂ đồng thời đúng về mặt logic
    # 3. Không có cách giải thích hợp lý nào để dung hòa hai thông tin

    # CÁC DẠNG MÂU THUẪN CẦN PHÁT HIỆN:
    # - Hai câu văn có cấu trúc và nội dung rất giống nhau nhưng số liệu khác nhau
    # * Ví dụ: "Thí sinh thi đánh giá năng lực đạt 800 điểm trở lên sẽ được cộng 1 điểm" vs "Thí sinh thi đánh giá năng lực đạt 850 điểm trở lên sẽ được cộng 1 điểm"
    # * Ví dụ: "Số lượng chỉ tiêu: 500" vs "Số lượng chỉ tiêu: 560"

    # - Hai thông tin trình bày số liệu khác nhau về cùng một đối tượng
    # * Ví dụ: "Năm 2024 có 18 sinh viên từ Hà Nội" vs "Năm 2024 có 26 sinh viên từ Hà Nội"
    # * Ví dụ: "Điểm chuẩn ngành Y năm 2024 là 25.5" vs "Điểm chuẩn ngành Y năm 2024 là 26.0"

    # - Số liệu tổng hợp không khớp với các thành phần
    # * Ví dụ: Tổng số sinh viên được khai báo là 100, nhưng tính từng thành phần lại được 110
    # * Ví dụ: Mỗi trường thành viên báo cáo 0 sinh viên từ Lào Cai (0+0+0+0+0+0+0+0+0=0), nhưng tổng số báo cáo là 1
    # * Ví dụ: Tỷ lệ các thành phần cộng lại vượt quá 100%

    # KIỂM TRA SỐ HỌC:
    # - LUÔN LUÔN kiểm tra bằng cách tính toán cụ thể dựa trên số liệu, không đưa ra kết luận chủ quan
    # - Khi thấy nhiều số liệu cụ thể, hãy tính tổng và so sánh với giá trị tổng được báo cáo
    # - Nếu các số liệu cho các thành phần A, B, C,... thì tổng của chúng phải chính xác bằng tổng số được báo cáo
    # - Nếu tổng số sau khi tính = tổng số được báo cáo → KHÔNG CÓ MÂU THUẪN
    # - Nếu tổng số sau khi tính ≠ tổng số được báo cáo → CÓ MÂU THUẪN

    # KIỂM TRA CÂU CÓ CẤU TRÚC GIỐNG NHAU:
    # - So sánh cấu trúc ngữ pháp và từ vựng của hai câu
    # - Nếu hai câu có nội dung rất giống nhau nhưng số liệu khác nhau, đó có thể là mâu thuẫn
    # - Kiểm tra xem hai câu có đang nói về cùng một đối tượng, cùng một thời điểm không
    # - Nếu hai câu giống nhau 80% trở lên và chỉ khác số liệu → CÓ KHẢ NĂNG MÂU THUẪN

    # NHỮNG TRƯỜNG HỢP KHÔNG PHẢI MÂU THUẪN:
    # - Hai thông tin nói về các đối tượng KHÁC NHAU (ví dụ: hai tỉnh/thành phố khác nhau, hai trường khác nhau)
    # - Quan hệ tập con hợp lý (số sinh viên từ một tỉnh < tổng số sinh viên)
    # - Thông tin bổ sung cho nhau mà không mâu thuẫn (ví dụ: một nơi liệt kê chi tiết, nơi khác chỉ nêu tổng số)
    # - Hai thông tin nói về các năm học khác nhau hoặc các đợt tuyển sinh khác nhau

    # VÍ DỤ KHÔNG MÂU THUẪN:
    # - "Sinh viên Đà Nẵng: QSB(61), QSC(23), QSK(29), QSQ(16), QST(38), QSX(29), QSY(1), QSA(0), QSP(0)" vs "Tổng SV Đà Nẵng: 197" là KHÔNG MÂU THUẪN vì 61+23+29+16+38+29+1+0+0=197

    # VÍ DỤ MÂU THUẪN:
    # - "Sinh viên Lào Cai: QSB(0), QSC(0), QSK(0), QSQ(0), QST(0), QSX(0), QSY(0), QSA(0), QSP(0)" vs "Tổng SV Lào Cai: 1" là MÂU THUẪN vì 0+0+0+0+0+0+0+0+0=0≠1
    # - "Sinh viên Đà Nẵng: QSB(61), QSC(23), QSK(29), QSQ(16), QST(38), QSX(29), QSY(1), QSA(0), QSP(0)" vs "Tổng SV Đà Nẵng: 200" là MÂU THUẪN vì 61+23+29+16+38+29+1+0+0=197≠200
    # - "Thí sinh thi đánh giá năng lực đạt 800 điểm trở lên sẽ được cộng 1 điểm" vs "Thí sinh thi đánh giá năng lực đạt 850 điểm trở lên sẽ được cộng 1 điểm" là MÂU THUẪN vì đang nói về cùng một chính sách nhưng điều kiện điểm khác nhau
                    
    # KHI PHÂN TÍCH, HÃY:
    # 1. Xác định chính xác đối tượng/phạm vi mỗi thông tin đề cập đến
    # 2. Kiểm tra bằng phép tính cụ thể, không đưa ra kết luận chủ quan
    # 3. Chỉ kết luận mâu thuẫn khi CÓ BẰNG CHỨNG SỐ HỌC RÕ RÀNG
    # 4. Luôn hiển thị phép tính chi tiết để chứng minh có/không có mâu thuẫn
    # 5. KHÔNG suy diễn thêm hoặc tạo ra mâu thuẫn không có thực
    # 6. So sánh cấu trúc câu để phát hiện các câu giống nhau nhưng khác số liệu
                    
    # Trả lời theo cấu trúc JSON tiếng Việt sau:
    # ```json
    # {{
    #     "reasoning_process": "Phân tích chi tiết về đối tượng, phạm vi, các phép tính kiểm tra và tính logic của thông tin",
    #     "has_contradiction": "yes/no",
    #     "contradiction_count": <số lượng mâu thuẫn tìm thấy>,
    #     "contradictions": [
    #         {{
    #             "id": 1,
    #             "description": "Mô tả ngắn gọn về mâu thuẫn",
    #             "explanation": "Giải thích chi tiết tại sao đây là mâu thuẫn, phải nêu rõ phạm vi đối tượng và lý do hai thông tin không thể đồng thời đúng",
    #             "calculation": "Các phép tính cụ thể chứng minh mâu thuẫn",
    #             "conflicting_parts": ["Trích dẫn từ nội dung 1", "Trích dẫn từ nội dung 2"],
    #             "severity": "low/medium/high"
    #         }}
    #     ],
    #     "explanation": "Tóm tắt về các mâu thuẫn hoặc lý do không có mâu thuẫn",
    #     "conflicting_parts": ["Trích dẫn từ nội dung 1", "Trích dẫn từ nội dung 2"],
    #     "conflict_type": "{conflict_type}"
    # }}
    # ```
                    
    # Chỉ trả về JSON hợp lệ, KHÔNG thêm bất kỳ giải thích nào bên ngoài cấu trúc JSON."""
    #         },
    #         {
    #             "role": "user",
    #             "content": f"""NỘI DUNG 1:
                    
    #             {content1}
                                
    #             NỘI DUNG 2:
                                
    #             {content2}"""
    #         }
    #     ]
    
    def _create_comparison_conflict_prompt(self, content1: str, content2: str, conflict_type: str = "internal") -> List[Dict]:
        """
        Create a prompt to analyze conflicts between two text paragraphs using chain of thought approach
        
        Args:
            content1: First content
            content2: Second content
            conflict_type: Conflict type ("internal" or "external")
        
        Returns:
            List[Dict]: List of messages for the request
        """
        conflict_type_text = "trong cùng một tài liệu" if conflict_type == "internal" else "giữa các tài liệu khác nhau"
        conflict_context = ("các đoạn văn trong cùng tài liệu" if conflict_type == "internal" 
                        else "các tài liệu trong hệ thống dữ liệu tuyển sinh")
        
        return [
            {
                "role": "system",
                "content": f"""Bạn là chuyên gia phân tích mâu thuẫn {conflict_type_text} về dữ liệu tuyển sinh đại học.
                    
            ĐỊNH NGHĨA MÂU THUẪN:
            KIỂM TRA CÂU CÓ CẤU TRÚC GIỐNG NHAU:
            - So sánh cấu trúc ngữ pháp và từ vựng của hai câu
            - Nếu hai câu có nội dung rất giống nhau nhưng số liệu khác nhau, đó có thể là mâu thuẫn
            - Kiểm tra xem hai câu có đang nói về cùng một đối tượng, cùng một thời điểm không
            - Nếu hai câu giống nhau 80% trở lên và chỉ khác số liệu → CÓ KHẢ NĂNG MÂU THUẪN

            KHI PHÂN TÍCH, HÃY:
            1. Xác định chính xác đối tượng/phạm vi mỗi thông tin đề cập đến
            2. So sánh cấu trúc câu để phát hiện các câu giống nhau nhưng khác số liệu
            3. Chỉ kết luận mâu thuẫn khi hai câu có cấu trúc tương tự nhưng số liệu khác nhau

            Trả lời theo cấu trúc JSON tiếng Việt sau:
            ```json
            {{
                "reasoning_process": "Phân tích chi tiết về đối tượng, phạm vi và mức độ tương đồng về cấu trúc của hai câu",
                "has_contradiction": "yes/no",
                "contradiction_count": <số lượng mâu thuẫn tìm thấy>,
                "contradictions": [
                    {{
                        "id": 1,
                        "description": "Mô tả ngắn gọn về mâu thuẫn",
                        "explanation": "Giải thích chi tiết tại sao đây là mâu thuẫn, phải nêu rõ phạm vi đối tượng và lý do hai thông tin không thể đồng thời đúng",
                        "calculation": "Các phép tính cụ thể chứng minh mâu thuẫn",
                        "conflicting_parts": ["Trích dẫn từ nội dung 1", "Trích dẫn từ nội dung 2"],
                        "severity": "low/medium/high"
                    }}
                ],
                "explanation": "Tóm tắt về các mâu thuẫn hoặc lý do không có mâu thuẫn",
                "conflicting_parts": ["Trích dẫn từ nội dung 1", "Trích dẫn từ nội dung 2"],
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
            error_message = str(e)
            error_message = error_message.replace('%', '%%')
            logger.error(f"Error analyzing conflict: {error_message}")
            return ConflictResult(
                has_conflict=False,
                explanation=f"Lỗi khi phân tích: {error_message}",
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
        try:
            has_contradiction = result_json.get("has_contradiction", "no") == "yes"
            
            explanation = result_json.get("explanation", "No contradictions found")
            conflicting_parts = result_json.get("conflicting_parts", [])
            
            contradictions = result_json.get("contradictions", [])
            contradiction_count = result_json.get("contradiction_count", len(contradictions))
            
            processed_contradictions = []
            
            if contradictions:
                for contradiction in contradictions:
                    processed_contradiction = {
                        "id": contradiction.get("id", len(processed_contradictions) + 1),
                        "description": contradiction.get("description", "Detected contradiction"),
                        "explanation": contradiction.get("explanation", ""),
                        "conflicting_parts": contradiction.get("conflicting_parts", []),
                        "severity": contradiction.get("severity", "medium")
                    }
                    
                    if "type" in contradiction:
                        processed_contradiction["type"] = contradiction["type"]
                    
                    if "calculation" in contradiction:
                        processed_contradiction["calculation"] = contradiction["calculation"]
                    
                    processed_contradictions.append(processed_contradiction)
            
            actual_conflict_type = result_json.get("conflict_type", conflict_type)
            
            if has_contradiction and not processed_contradictions:
                processed_contradictions = [{
                    "id": 1,
                    "type": "trực tiếp",  
                    "description": f"{actual_conflict_type} contradiction detected",
                    "explanation": explanation,
                    "conflicting_parts": conflicting_parts,
                    "severity": "medium"
                }]
            
            return ConflictResult(
                has_conflict=has_contradiction,
                explanation=explanation,
                conflicting_parts=conflicting_parts,
                analyzed_at=datetime.now(),
                chunk_ids=[],
                conflict_type=actual_conflict_type,
                severity="medium",
                contradictions=processed_contradictions
            )
        
        except Exception as e:
            error_message = str(e)
            logger.error(f"Error processing result: {error_message}")
            logger.error(traceback.format_exc())
            return ConflictResult(
                has_conflict=False,
                explanation=f"Lỗi khi xử lý kết quả phân tích: {error_message}",
                conflicting_parts=[],
                analyzed_at=datetime.now(),
                chunk_ids=[],
                conflict_type=conflict_type,
                severity="medium",
                contradictions=[]
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
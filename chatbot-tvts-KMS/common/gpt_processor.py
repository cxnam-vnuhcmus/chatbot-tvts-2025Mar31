import openai
from openai import OpenAI
import json
from datetime import datetime
import os
import traceback
import logging
from typing import Dict, Optional
from dotenv import load_dotenv
import asyncio
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class GPTProcessor:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        self.openai_api_key = os.getenv('OPENAI_API_KEY')
        if not self.openai_api_key:
            raise ValueError("OpenAI API key not found in environment variables")
            
        openai.api_key = self.openai_api_key
        self.client = OpenAI()
       
        self.has_tiktoken = False
        self.tiktoken = None
        try:
            import tiktoken
            self.has_tiktoken = True
            self.tiktoken = tiktoken
        except ImportError:
            self.logger.warning("tiktoken not installed. Token counting will be estimated.")

        self.models_config = {
            "gpt-4": {
                "input_cost": 0.03,    
                "output_cost": 0.06,
                "token_limit": 8192
            },
            "gpt-4o-mini": {         
                "input_cost": 0.015,   
                "output_cost": 0.03,
                "token_limit": 4096    
            },
            "gpt-3.5-turbo": {
                "input_cost": 0.0015,  
                "output_cost": 0.002,
                "token_limit": 4096
            }
        }
        
        self.model = os.getenv('MODEL', 'gpt-4o-mini')
        if self.model not in self.models_config:
            logger.warning(f"Invalid model {self.model}, using default gpt-4o-mini")
            self.model = "gpt-4o-mini"
        
        self.SYSTEM_PROMPT = """
            Objective: Split a document enclosed in triple quotes into multiple, standalone chunks that maintain clarity and context. Each chunk will address a single paragraph or session from the original document, restructured in a Q&A (FAQs) format to ensure independent understanding without losing the original meaning.

            Instructions:

            1. **Chunk Creation:**
            - Read through the document and identify logical sections or paragraphs suitable for standalone presentation.
            - Separate the document into these identified chunks.

            2. **Chunk Revision:**
            - For each chunk, revise the content into a Q&A format (FAQs style) that preserves the original meaning.
            - Ensure the revised content is clear, concise, and can stand alone without requiring reference to other chunks.

            3. **Formatting:**
            - Organize each chunk in JSON format using the following structure:

            {  
                "CHUNKS": [{
                    "chunk_topic": "<Brief description of the topic covered in this chunk>",
                    "original_chunk": "<Original text of the paragraph or session>",
                    "revised_chunk": "<Revised text in FAQs style>",
                    "index": "Paragraph/Session <Section or paragraph number>"
                }],
                "TOPIC": "<Brief description of the topic covered in all chunks>",
                "CHUNK_NUMBER": "<Total number of chunks>"
            }

            4. **Quality Check:**
            - Review each chunk for clarity and independence, ensuring it is comprehensible when read on its own.

            5. **Language Consistency:**
            - Use Vietnamese, maintaining the same tone as the original document in the revisions to ensure consistency throughout.
        """
        
        # self.SYSTEM_PROMPT = """
        #     Objective: Create a Q&A (FAQs) format from the entire document enclosed in triple quotes that ensures understanding without losing the original meaning.

        #     Instructions:

        #     1. **Document Analysis:**
        #     - Read through the entire document and identify key information and topics.
        #     - Process the document as a whole, without splitting it into chunks.

        #     2. **Q&A Creation:**
        #     - Create a comprehensive set of Q&A pairs that covers the entire document.
        #     - Ensure the Q&A content preserves the original meaning and important details.

        #     3. **Formatting:**
        #     - Organize the document in JSON format using the following structure:

        #     {  
        #         "CHUNKS": [{
        #             "chunk_topic": "<Brief description of the topic covered in the document>",
        #             "original_chunk": "<Original text of the document>",
        #             "revised_chunk": "<Entire document revised in FAQs style>",
        #             "index": "Paragraph/Session 1"
        #         }],
        #         "TOPIC": "<Brief description of the topic covered in the document>",
        #         "CHUNK_NUMBER": "1"
        #     }

        #     4. **Quality Check:**
        #     - Review the Q&A for clarity and comprehensiveness, ensuring it captures the essential information from the document.

        #     5. **Language Consistency:**
        #     - Use Vietnamese, maintaining the same tone as the original document in the revisions to ensure consistency throughout.
        # """
    
    async def convert_async(self, chunk_id: str, content: str) -> Dict:
        """Convert content to standard format asynchronously"""
        try:
            if not content:
                self.logger.warning(f"Empty content for chunk {chunk_id}")
                return None

            messages = [
                {
                    "role": "system",
                    "content": """Convert the given content into Q&A format and return a JSON object following these rules:
                        - Extract key information into question-answer pairs
                        - Keep important details in answers
                        - Use natural, conversational language
                        - Format in Vietnamese
                        - Return JSON in following format:
                        {
                            "TOPIC": "Main topic",
                            "CHUNKS": [{
                                "chunk_topic": "Subtopic",
                                "original_chunk": "Original text",
                                "revised_chunk": "Q&A formatted text", 
                                "index": "Paragraph number"
                            }],
                            "CHUNK_NUMBER": "Total chunks"
                        }"""
                },
                {
                    "role": "user", 
                    "content": content
                }
            ]
            
            try:
                response = await asyncio.to_thread(
                    self.client.chat.completions.create,
                    model=self.model,
                    messages=messages,
                    temperature=0,
                    response_format={"type": "json_object"}
                )

                self._log_token_usage_to_application_logs(
                    chunk_id,
                    response.usage.prompt_tokens,
                    response.usage.completion_tokens,
                    response.usage.total_tokens,
                    self.model
                )

                result = json.loads(response.choices[0].message.content)
                self.logger.info(f"Successfully converted chunk {chunk_id}")
                return result
                
            except Exception as api_error:
                self.logger.error(f"API error for chunk {chunk_id}: {str(api_error)}")
                return None

        except Exception as e:
            self.logger.error(f"Error converting chunk {chunk_id}: {str(e)}")
            return None
        
    def calculate_tokens(self, text: str, model: Optional[str] = None) -> int:
        """Calculate number of tokens in text"""
        try:
            if not text:
                return 0
                
            model = model or self.model
            
            if self.has_tiktoken:
                encoder = self.tiktoken.encoding_for_model(model)
                token_count = len(encoder.encode(text))
            else:
                words = text.split()
                token_count = int(len(words) * 1.5)  
            
            if token_count < 0:
                logger.error("Negative token count detected")
                return 0
                
            model_limit = self.models_config[model]['token_limit']
            if token_count > model_limit:
                logger.warning(f"Text exceeds model token limit: {token_count} > {model_limit}")
                return model_limit
                
            return token_count
            
        except Exception as e:
            logger.error(f"Token calculation error: {str(e)}")
            return len(text.split())  
        
    def one_chunk(self, content: str, doc_id: Optional[str] = None) -> Dict:
        return {
                "CHUNKS": [{
                    "chunk_topic": "",
                    "original_chunk": content,
                    "revised_chunk": "",
                    "index": "Paragraph/Session <Section or paragraph number>"
                }],
                "TOPIC": "",
                "CHUNK_NUMBER": "1"
            }


    def process_content(self, content, doc_id=None):
        return self.one_chunk(content, doc_id)

    def process_content_bak(self, content, doc_id=None):
        """Process document content with GPT and log usage"""
        try:
            if not content or not content.strip():
                raise ValueError("Input content is empty")
                
            logger.info(f"Processing document: {doc_id if doc_id else 'Unknown'}")

            if not doc_id:
                doc_id = f"doc_{int(datetime.now().timestamp())}"

            completion = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": self.SYSTEM_PROMPT},
                    {"role": "user", "content": f"DOCUMENT: '''{content}'''\nDOC_ID: {doc_id}"}
                ],
                response_format={"type": "json_object"}
            )

            self._log_token_usage_to_application_logs(
                doc_id,
                completion.usage.prompt_tokens,
                completion.usage.completion_tokens,
                completion.usage.total_tokens,
                self.model
            )

            try:
                result = json.loads(completion.choices[0].message.content)
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON response: {str(e)}")
                logger.error(f"Raw response: {completion.choices[0].message.content}")
                raise ValueError("Response is not in valid JSON format")

            if not isinstance(result, dict):
                raise ValueError("Response is not a dictionary")
                
            if 'CHUNKS' not in result:
                raise ValueError("Missing CHUNKS field in response")
                
            if not isinstance(result['CHUNKS'], list):
                raise ValueError("CHUNKS is not an array")

            for i, chunk in enumerate(result['CHUNKS']):
                if not isinstance(chunk, dict):
                    raise ValueError(f"Chunk {i} is not a dictionary")
                    
                required_fields = ['chunk_topic', 'original_chunk', 'revised_chunk', 'index']
                missing_fields = [field for field in required_fields if field not in chunk]
                
                if missing_fields:
                    raise ValueError(f"Chunk {i} is missing fields: {', '.join(missing_fields)}")

                expected_index = f"Paragraph {i+1}"
                if chunk['index'] != expected_index:
                    chunk['index'] = expected_index

            if not result.get('TOPIC'):
                logger.warning("Missing TOPIC field in response")
                result['TOPIC'] = "Unknown Topic"

            if not result.get('CHUNK_NUMBER'):
                logger.warning("Missing CHUNK_NUMBER field in response")
                result['CHUNK_NUMBER'] = str(len(result['CHUNKS']))
            
            logger.info(f"Successfully processed {len(result['CHUNKS'])} chunks for document {doc_id}")
            return result

        except Exception as e:
            logger.error(f"Error processing document {doc_id}: {str(e)}")
            logger.error(traceback.format_exc())
            raise


    def calculate_cost(self, prompt_tokens: int, completion_tokens: int, model: Optional[str] = None) -> Dict[str, float]:
        """
        Calculate cost based on token usage
        
        Args:
            prompt_tokens (int): Number of tokens in the prompt
            completion_tokens (int): Number of tokens in the completion
            model (Optional[str]): Model name to use for calculation

        Returns:
            Dict[str, float]: Calculated costs
        """
        try:
            if prompt_tokens < 0 or completion_tokens < 0:
                logger.error("Invalid token count")
                return {"input_cost": 0, "output_cost": 0, "total_cost": 0}
            
            model = model or self.model
            if model not in self.models_config:
                logger.warning(f"Invalid model {model}, using default gpt-4o-mini")
                model = "gpt-4o-mini"
            
            rates = self.models_config[model]
            input_cost = (prompt_tokens / 1000) * rates["input_cost"]
            output_cost = (completion_tokens / 1000) * rates["output_cost"]
            
            return {
                "input_cost": round(input_cost, 6),
                "output_cost": round(output_cost, 6),
                "total_cost": round(input_cost + output_cost, 6)
            }
            
        except Exception as e:
            logger.error(f"Cost calculation error: {str(e)}")
            return {"input_cost": 0, "output_cost": 0, "total_cost": 0}

    def _log_token_usage_to_application_logs(self, doc_id: str, prompt_tokens: int, 
                                          completion_tokens: int, total_tokens: int, 
                                          model: str) -> None:
        """
        Log token usage directly to application logs
        
        Args:
            doc_id (str): Document ID
            prompt_tokens (int): Number of tokens in the prompt
            completion_tokens (int): Number of tokens in the completion
            total_tokens (int): Total number of tokens
            model (str): Model name used
        """
        try:
            costs = self.calculate_cost(prompt_tokens, completion_tokens, model)
            
            usage_info = {
                "timestamp": datetime.now().isoformat(),
                "doc_id": doc_id,
                "model": model,
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": total_tokens,
                "input_cost": costs["input_cost"],
                "output_cost": costs["output_cost"],
                "total_cost": costs["total_cost"]
            }
            
            logger.info(f"API Usage: {json.dumps(usage_info)}")
        except Exception as e:
            logger.error(f"Error logging token usage: {str(e)}")
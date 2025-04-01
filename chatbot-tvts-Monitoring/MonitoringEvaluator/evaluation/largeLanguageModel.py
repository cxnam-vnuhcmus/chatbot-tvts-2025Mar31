import os
from openai import OpenAI
from openai.types.chat import ChatCompletion
from dotenv import load_dotenv
from evaluation import generated

load_dotenv()

OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
MODEL = os.getenv('MODEL')

class LargeLanguageModel():
    def __init__(self):
        self.client = OpenAI(api_key=OPENAI_API_KEY)

    def generate_score(self, system_prompt: str, user_prompt: str, normalize: float = 5.0) -> float:
        messages = self.create_messages(system_prompt, user_prompt)
        response = self.client.chat.completions.create(
            model=MODEL,
            messages=messages
        )
        content = self.get_content(response)
        usage = self.get_usage(response)
        return generated.re_1_5_rating(content)
    
    def create_messages(self, system_prompt: str, user_prompt: str): 
        return [
            { "role": "system", "content": system_prompt, },
            { "role": "user", "content": user_prompt, },
        ]
    
    def get_content(self, c: ChatCompletion) -> str:
        return c.choices[0].message.content 
    
    def get_usage(self, c: ChatCompletion) -> object:
        return c.usage 
    

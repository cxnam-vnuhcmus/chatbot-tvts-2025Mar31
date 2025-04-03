import requests
import os
from dotenv import load_dotenv
from model.conversationDto import ConversationDto

load_dotenv()
url = os.getenv('MONIROTING_EVALUATOR_SERVICE')

def get_all_conversations(page_index: int = 0, page_size: int = 100) -> list[ConversationDto]:
    response = requests.get(f"{url}/conversations?pageIndex={page_index}&pageSize={page_size}")
    if response.status_code == 200:
        return [ConversationDto(**item) for item in response.json()] 
    else:
        raise Exception(f"Error: {response.status_code}, {response.text}")

def get_total_count_conversation() -> int:
    response = requests.get(f"{url}/conversations/count")
    if response.status_code == 200:
        return response.json()["total"]
    else:
        raise Exception(f"Error: {response.status_code}, {response.text}")

def get_conversation_by_id(conversation_id) -> ConversationDto:
    response = requests.get(f"{url}/conversations/{conversation_id}")
    if response.status_code == 200:
        return ConversationDto(**response.json()) 
    else: 
        raise Exception(f"Error: {response.status_code}, {response.text}")

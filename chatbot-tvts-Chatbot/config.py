import os
import urllib.parse

OPENAI_API_KEY = str(os.environ.get("OPENAI_API_KEY"))

MODEL = str(os.environ.get("MODEL"))

USE_CHATBOT_V1 = str(os.environ.get("SETTING_CHATBOT_VERSION")) == '1'

POSTGRESQL_URL= f"postgresql+psycopg2://{str(os.environ.get('DB_USER'))}:{urllib.parse.quote_plus(str(os.environ.get('DB_PASSWORD')))}@{str(os.environ.get('DB_HOST'))}:{str(os.environ.get('DB_PORT'))}/{str(os.environ.get('DB_CHATNAME'))}"

CHROMA_HOST = str(os.environ.get('CHROMA_HOST'))

CHROMA_PORT = str(os.environ.get('CHROMA_PORT'))

EMBEDDING_MODEL_NAME= str(os.environ.get('EMBEDDING_MODEL_NAME'))

CHROMA_DB=str(os.environ.get("CHROMA_DB"))

CHATBOT_AGENT_PORT=str(os.getenv('CHATBOT_AGENT_PORT', 6811))

API_URL = str(os.environ.get("API_URL", "http://127.0.0.1:6811")) 

N_RESULTS = int(os.environ.get("N_RESULTS", 5))
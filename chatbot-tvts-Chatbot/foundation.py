import asyncio
from datetime import datetime, timezone
from enum import Enum
import json
import os
import random
import uuid
from openai import OpenAI, Stream
from openai.types.chat import ChatCompletion
from textwrap import dedent
import chromadb
from chromadb.config import Settings
from abc import ABC, abstractmethod
import sys
import requests
import sqlalchemy as db
import chromadb.utils.embedding_functions as embedding_functions
from typing import TypeVar, Generic, TypedDict, Any
from json import JSONEncoder
import itertools

from models import RoleEnum, get_session, Session, Dialogue
from config import API_URL, OPENAI_API_KEY, MODEL, POSTGRESQL_URL, CHROMA_HOST, CHROMA_PORT, EMBEDDING_MODEL_NAME, CHROMA_DB, N_RESULTS
from prompts import *
from utils import _extract_tag_content, _get_content

chroma_client = chromadb.HttpClient(host=CHROMA_HOST, port=int(
    CHROMA_PORT), settings=Settings(allow_reset=True, anonymized_telemetry=False))

# Generation


class Logger:
    def __init__(self):
        self.engine = get_session()

    async def create_async(self, docs: dict):
        insert = db.insert(Dialogue).values(
            **docs
        )
        with self.engine.connect() as conn:
            conn.execute(insert)
            conn.commit()
        return insert


class History:

    def __init__(self, role: str, content: str):
        self.role = role
        self.content = content

    def to_str(self) -> str:
        return dedent(
            """
          {role}: {content}
          """
        ).format(role=self.role, content=self.content)

    def __str__(self):
        return self.to_str()

    def __repr__(self):
        return self.to_str()

    def __dict__(self):
        return self.to_str()


class QuestionResponse:

    def __init__(self, id: str, question: str):
        self.id = id
        self.question = question

    def to_str(self):
        return dedent(
            """
        Id: {id}
        Question: {question}
      """
        ).format(id=self.id, question=self.question)

    def __repr__(self) -> str:
        return self.to_str()


class KnowledgeBase:

    def __init__(self):
        self.client = OpenAI(api_key=OPENAI_API_KEY)
        self.collection = chroma_client.get_collection(
            name=CHROMA_DB, embedding_function=self.get_ef())

    def get_ef(self):
        if EMBEDDING_MODEL_NAME:
            ef = embedding_functions.OpenAIEmbeddingFunction(
                api_key=OPENAI_API_KEY,
                model_name=EMBEDDING_MODEL_NAME
            )
        else:
            ef = embedding_functions.ONNXMiniLM_L6_V2(
                preferred_providers=['CPUExecutionProvider'])
        return ef

    def gen(
            self,
            user: str,
            system: str, **kwargs) -> ChatCompletion:
        stream = kwargs.get("stream", False)
        json_object = kwargs.get("json_object", False)
        completion = self.client.chat.completions.create(
            model=MODEL,
            messages=[
                {
                    "role": "system",
                    "content": system,
                },
                {
                    "role": "user",
                    "content": user,
                },
            ],
            stream=stream,
            stream_options={
                "include_usage": True
            } if stream else None,
            response_format={
                "type": "json_object",
            } if json_object else None
        )
        return completion

    def search_docs(self, intent: str, terms: list[str], **kwargs) -> chromadb.QueryResult:
        collection = self.collection
        DB = kwargs.get("DB")
        if DB:
            collection = chroma_client.get_collection(
                name=DB, embedding_function=self.get_ef())
        res = collection.query(
            query_texts=terms,
            n_results=N_RESULTS,
        )
        return res

    def search_ques(self, intent: str, term: str) -> list[QuestionResponse]:
        return [
            QuestionResponse(id="1", question="abc"),
            QuestionResponse(id="2", question="xyz"),
            QuestionResponse(id="3", question="xyz"),
        ]


class Generation:

    def __init__(self):
        self.knowledge_base = KnowledgeBase()
        self.logger = Logger()

    def intent(self, question: str, histories: list[History]) -> tuple[str, ChatCompletion, str]:
        his = "\n".join(map(lambda e: e.to_str(), histories))
        prompt = INTENT_PROMPT_TEMPLATE.replace("[HISTORIES]", his)
        completion = self.knowledge_base.gen(
            system=prompt,
            user=question,
            json_object=True
        )
        return _get_content(completion), completion, prompt

    def search_query(self, question: str, histories: list[History]) -> tuple[str, ChatCompletion, str]:
        his = "\n".join(map(lambda e: e.to_str(), histories))
        prompt = SEARCH_QUERY_PROMPT_TEMPLATE.replace("[HISTORIES]", his)
        completion = self.knowledge_base.gen(
            system=prompt,
            user=question,
        )
        return _get_content(completion), completion, prompt

    def search_query_using_breakdown_template(self, question: str, histories: list[History], **kwargs) -> tuple[list[str], ChatCompletion, str]:
        his = "\n".join(map(lambda e: e.to_str(), histories))
        prompt = SEARCH_QUERY_BREAKDOWN_PROMPT_TEMPLATE.replace(
            "[HISTORIES]", his)
        completion = self.knowledge_base.gen(
            system=prompt,
            user=question,
        )
        contents = _get_content(completion)
        query_1 = _extract_tag_content(contents, "QUERY_1")
        query_2 = _extract_tag_content(contents, "QUERY_2")
        query_3 = _extract_tag_content(contents, "QUERY_3")
        result = []
        if query_1:
            result.append(query_1)
        if query_2:
            result.append(query_2)
        if query_3:
            result.append(query_3)
        return result, completion, prompt

    def search_docs(self, intent: str, search_terms: list[str], **kwargs) -> tuple[list[str], chromadb.QueryResult]:
        node = self.knowledge_base.search_docs(
            intent=intent, terms=search_terms, kwargs=kwargs)
        if node['documents'] is not None:
            docs = list(itertools.chain.from_iterable(node['documents']))
            result = list(set(docs))
        else:
            result = []
        if not len(result):
            return [f"Không tìm thấy được thông tin liên quan đến câu hỏi", *search_terms], node
        return result, node

    def search_docs_by_chunk_id(self, chunk_id: str) -> tuple[str, chromadb.GetResult]:
        node = self.knowledge_base.collection.get(ids=[chunk_id])
        if node['documents'] is not None:
            result = node['documents'][0]
        else:
            result = [""]
        return result, node

    def ranking_docs(self, question, histories, docs) -> tuple[float, ChatCompletion, str, str]:
        his = "\n".join(map(lambda e: e.to_str(), histories))
        system_prompt = RANKING_DOCS_SYSTEM_PROMPT_TEMPLATE

        documents2 = []
        for i in range(len(docs)):
            id = f"id_{i}"
            doc = docs[i]
            documents2.append(
                {
                    "chunk_id": id,
                    "text": doc
                }
            )

        user_prompt = RANKING_DOCS_USER_PROMPT_TEMPLATE.replace(
            "[HISTORIES]", his
        ).replace(
            "[DOCS]", json.dumps(documents2)
        ).replace(
            "[QUERY]", question
        )
        completion = self.knowledge_base.gen(
            system=system_prompt,
            user=user_prompt,
            json_object=True
        )

        print(_get_content(completion))
        chunks = json.loads(_get_content(completion))
        if 'chunks' not in chunks:
            return []
        chunks = chunks['chunks']
        chunks.sort(key=lambda x: x['score'], reverse=True)
        print("=" * 5, "Sorted chunks", "=" * 5)
        print(chunks)

        ranked_documents = []

        for chunk in chunks:
            if chunk['score'] == 1:  # 1: Not relevant
                break
            id = chunk['chunk_id']
            id_i = int(id.split("_")[1])
            ranked_documents.append({
                "rank": chunk['score'],
                "document": docs[id_i],
            })

        return ranked_documents, completion

    def answers(self, question: str, docs: list[str], **kwargs) -> tuple[str, ChatCompletion, str]:
        documents = "\n".join(docs)
        prompt = ANSWER_PROMPT_TEMPLATE.replace("[DOCS]", documents)
        completion = self.knowledge_base.gen(
            system=prompt,
            user=question,
        )
        return _get_content(completion), completion, prompt

    def answers_using_stream(self, question: str, docs: list[str], histories: list[History]) -> tuple[Stream, str]:
        his = "\n".join(map(lambda e: e.to_str(), histories))
        documents = "\n".join(docs)
        prompt = ANSWER_PROMPT_TEMPLATE.replace(
            "[DOCS]", documents).replace("[HISTORIES]", his)
        completion = self.knowledge_base.gen(
            system=prompt,
            user=question,
            stream=True
        )
        return completion, prompt

    def followup_questions(
        self,
        search_term: str,
        answer: str,
        histories: list[History],
        intent: str,
        n=3,
    ) -> tuple[ChatCompletion, str, list[str]]:
        his = "\n".join(map(lambda e: e.to_str(), histories))
        prompt = FOLLOWUP_QUESTIONS_PROMPT_TEMPLATE.replace(
            "[SEARCH_TERM]", search_term
        ).replace("[ANSWER]", answer).replace("[HISTORIES]", his)

        chat_completion = self.knowledge_base.gen(
            system=prompt,
            user=search_term,
        )
        contents = _get_content(chat_completion)
        ques_1 = _extract_tag_content(contents, "QUESTION_1")
        ques_2 = _extract_tag_content(contents, "QUESTION_2")
        ques_3 = _extract_tag_content(contents, "QUESTION_3")

        results = []
        if ques_1:
            results.append(ques_1)
        if ques_2:
            results.append(ques_2)
        if ques_3:
            results.append(ques_3)
        return chat_completion, prompt, results

    def generate_questions(self, content: str, num=1) -> tuple[list[str], ChatCompletion, str]:
        prompt = GENERATE_QUESTIONS_PROMPT_TEMPLATE.replace(
            "[NUMBER_QUESTIONS]", str(num)).replace("[CONTENT]", content)
        completion = self.knowledge_base.gen(
            system=prompt,
            user=content
        )
        contents = _get_content(completion)
        questions = _extract_tag_content(contents, "QUESTIONS").split("\n")
        return questions, completion, prompt

    def check_if_answer_related_to_content(self, question: str, answer: str, content: str) -> tuple[bool, ChatCompletion, str]:
        prompt = CHECKING_ANSWER_PROMPT_TEMPLATE.replace(
            "[CONTENT]", content
        ).replace("[QUESTION]", question)

        completion = self.knowledge_base.gen(
            system=prompt,
            user=answer,
        )
        contents = _get_content(completion)
        is_related = _extract_tag_content(contents, "RELATED") == "YES"
        return is_related, completion, prompt


# Command

T = TypeVar('T')
R = TypeVar('R')

generation_instance = Generation()
logger = Logger()


class IntentTypeDict(TypedDict):
    intent: str
    completion: dict
    prompt: str
    action: dict


class SearchTermTypeDict(TypedDict):
    search_terms: list[str]
    completion: dict
    prompt: str


class SearchDocsTypeDict(TypedDict):
    documents: list[str]
    nodes: chromadb.QueryResult


class RankdingDocsTypeDict(TypedDict):
    rank: float
    completion: dict
    prompt: str
    document: str


class AnswerUsingStreamTypeDict(TypedDict):
    answer: str
    completion: dict
    prompt: str
    docs: list[str]


class AnswerUsingTemplatesTypeDict(TypedDict):
    question: str
    answer: str
    templates: list[str]


class LogActivitiesTypeDict(TypedDict):
    conversationId: str
    question: str
    intent: IntentTypeDict
    search_terms: SearchTermTypeDict
    search_docs: SearchDocsTypeDict
    ranked_documents: list[RankdingDocsTypeDict]
    answer: AnswerUsingStreamTypeDict
    followup_questions: list[QuestionResponse]


class CheckingAnswerRelatedToContentTypeDict(TypedDict):
    question: str
    answer: str
    content: str
    is_related: bool
    completion: dict
    prompt: str


class GenerateQuestionsTypeDict(TypedDict):
    questions: list[str]
    completion: dict
    prompt: str
    content: str


class SearchDocsByChunkIdTypeDict(TypedDict):
    chunk_id: str
    document: str
    node: chromadb.GetResult


class SessionTypeDict(TypedDict):
    session_id: str
    role: RoleEnum
    content: str


class GetHistoriesBySessionIdTypeDict(TypedDict):
    role: str
    content: str


class FollowupQuestionsTypeDict(TypedDict):
    followup_questions: list[str]
    completion: dict
    prompt: str


class Command(ABC, Generic[T]):
    input: dict[str, Any]
    result: T
    start_time: datetime
    end_time: datetime

    def __init__(self, **kwargs):
        self.input = kwargs
        self.start_time = None
        self.end_time = None
        self.include_execution_time = kwargs.get(
            "include_execution_time", True)

    @abstractmethod
    def execute(self) -> Any:
        pass

    def set_execution_time(self, start_time: datetime, end_time: datetime):
        if not self.include_execution_time:
            raise Exception("Cannot set execution time")
        self.start_time = start_time
        self.end_time = end_time


class IntentCommand(Command[IntentTypeDict]):
    def __init__(self, question: str, histories: list[History], **kwargs) -> None:
        his = "\n".join(map(lambda e: e.to_str(), histories))
        super().__init__(question=question, histories=his, **kwargs)
        self.question = question
        self.histories = histories
        self.default_action = {
            "DESCRIPTION": "Default description",
            "ID": "defaut_query",
            "ACTION": {
                "CMD": "ANSWER_TEMPLATE",
                "TEMPLATES": ["Cảm ơn bạn đã liên hệ, thông tin của bạn đã được ghi nhận."]
            }
        }

    def execute(self):
        intent, intent_completion, intent_prompt = generation_instance.intent(
            question=self.question, histories=self.histories)
        try:
            intent_object = json.loads(intent)
        except:
            intent_object = None
        rephased_intent = None

        print("intent_object:", intent_object)

        if intent_object:
            # for k in intent_object:
            #     intent_payload = self._get_payload_intent(k)
            #     rephased_intent = intent_object[k]
            #     break
            intent_id = intent_object["INTENT_NAME"]
            intent_payload = self._get_payload_intent(intent_id)
            rephased_intent = intent_object.get("REPHRASED_INTENT")
        else:
            intent_payload = self.default_action

        self.result = {
            "intent": intent,
            "rephased_intent": rephased_intent,
            "completion": intent_completion.to_dict(),
            "prompt": intent_prompt,
            "intent_payload": intent_payload
        }

        return self.result

    def _get_payload_intent(self, intent: str) -> dict:
        with open("intents.json", "r") as file:
            f = json.load(file)
            intent_dict = f.get(intent)
            return intent_dict


class SearchQueryCommand(Command[SearchTermTypeDict]):
    def __init__(self, question: str, histories: list[History], **kwargs) -> None:
        his = "\n".join(map(lambda e: e.to_str(), histories))
        super().__init__(question=question, histories=his, **kwargs)
        self.question = question
        self.histories = histories

    def execute(self):
        search_terms, search_term_completion, search_term_prompt = generation_instance.search_query_using_breakdown_template(
            question=self.question, histories=self.histories)
        self.result = {
            "search_terms": search_terms,
            "completion": search_term_completion.to_dict(),
            "prompt": search_term_prompt
        }
        return self.result


class SearchDocsCommand(Command[SearchDocsTypeDict]):
    def __init__(self, intent: str, search_terms: list[str], **kwargs) -> None:
        super().__init__(intent=intent, search_terms=search_terms, **kwargs)
        self.intent = intent
        self.search_terms = search_terms
        self.DB = kwargs.get("DB")

    def execute(self):
        docs, docs_list_nodes = generation_instance.search_docs(
            intent=self.intent, search_terms=self.search_terms, DB=self.DB)
        self.result = {
            "documents": docs,
            "nodes": docs_list_nodes
        }
        return self.result


class SearchDocsByChunkIdCommand(Command[SearchDocsByChunkIdTypeDict]):
    def __init__(self, chunk_id: str, **kwargs) -> None:
        super().__init__(chunk_id=chunk_id, **kwargs)
        self.chunk_id = chunk_id

    def execute(self):
        doc, docs_list_nodes = generation_instance.search_docs_by_chunk_id(
            chunk_id=self.chunk_id)
        self.result = {
            "document": doc,
            "node": docs_list_nodes,
            "chunk_id": self.chunk_id
        }

        return self.result


class RankingDocsCommand(Command[list[RankdingDocsTypeDict]]):
    def __init__(self, question: str, histories: list[History], docs: list[str], **kwargs) -> None:
        his = "\n".join(map(lambda e: e.to_str(), histories))
        super().__init__(question=question, histories=his, docs=docs, **kwargs)
        self.question = question
        self.histories = histories
        self.docs = docs

    def execute(self):
        ranked_documents: list[RankdingDocsTypeDict] = []

        ranked_documents, completion = generation_instance.ranking_docs(
            question=self.question, histories=self.histories, docs=self.docs)

        self.result = {
            "completion": completion.to_dict(),
            "docs": ranked_documents
        }
        return self.result


class ChatAction(Enum):
    SEARCH_DOCS = "SEARCH_DOCS"
    ANSWER_TEMPLATE = "ANSWER_TEMPLATE"
    ANSWER = "ANSWER"


class AnswerUsingTemplatesCommand(Command[AnswerUsingTemplatesTypeDict]):
    def __init__(self, question: str, templates: list[str], **kwargs) -> None:
        super().__init__(question=question, templates=templates, **kwargs)
        self.question = question
        self.templates = templates

    def execute(self):
        answer = random.choice(self.templates)
        self.result = {
            "question": self.question,
            "templates": self.templates,
            "answer": answer
        }
        return self.result


class AnswerUsingStreamCommand(Command[AnswerUsingStreamTypeDict]):
    def __init__(self, question: str, docs: list[str], histories: list[History], **kwargs) -> None:
        his = "\n".join(map(lambda e: e.to_str(), histories))
        super().__init__(question=question, docs=docs, histories=his, **kwargs)
        self.question = question
        self.docs = docs
        self.histories = histories

    def execute(self):
        full_answer = ""
        actual_ans_completion = None
        docs = self.docs
        ans_completion, ans_prompt = generation_instance.answers_using_stream(
            question=self.question, docs=docs, histories=self.histories)
        for chunk in ans_completion:
            if chunk is not None:
                if len(chunk.choices) == 0:
                    actual_ans_completion = chunk  # Usage on the end of chunk
                    break
                else:
                    content = str(chunk.choices[0].delta.content)
                    if content != "None":
                        yield content
                        full_answer += content
        self.result = {
            "answer": full_answer,
            "completion": actual_ans_completion.to_dict() if actual_ans_completion is not None else {},
            "prompt": ans_prompt,
            "docs": docs
        }
        return self.result


class FollowupQuestionsCommand(Command[list[FollowupQuestionsTypeDict]]):
    def __init__(self, search_term: str, intent: str, answer: str, histories: list[History], **kwargs) -> None:
        his = "\n".join(map(lambda e: e.to_str(), histories))
        super().__init__(search_term=search_term, answer=answer,
                         intent=intent, histories=his, **kwargs)
        self.search_term = search_term
        self.answer = answer
        self.intent = intent
        self.histories = histories

    def execute(self):
        completion, prompt, followup_questions = generation_instance.followup_questions(
            search_term=self.search_term, answer=self.answer, intent=self.intent, histories=self.histories)

        self.result = {
            "completion": completion.to_dict(),
            "prompt": prompt,
            "followup_questions": followup_questions,
        }

        return self.result


class LogActivitiesCommand(Command[LogActivitiesTypeDict]):

    def __init__(self, session_id: str, question: str, histories: list[History], commandHistories: list[Command[T]], answer: str, start_time: datetime, end_time: datetime, **kwargs) -> None:
        super().__init__(session_id=session_id, question=question, histories=histories,
                         commandHistorires=commandHistories, answer=answer, **kwargs)
        self.session_id = session_id
        self.question = question
        self.answer = answer
        self.histories = histories
        self.commandHistories = commandHistories
        self.start_time = start_time
        self.end_time = end_time

    def execute(self):
        calls = []
        for cmd in self.commandHistories:
            call = self._build_log(cmd)
            calls.append(call)
        log = {
            "record_id": str(uuid.uuid4()),
            "conversation_id": self.session_id,
            "app_id": "chatbot",
            "main_input": self.question,
            "main_output": self.answer,
            "main_error": "",
            "perf": {
                "start_time": self.start_time.isoformat(),
                "end_time": self.end_time.isoformat()
            },
            "calls": calls,
        }
        asyncio.run(logger.create_async(log))
        self.result = log
        return self.result

    def _build_log(self, cmd: Command):
        return {
            "call_id": str(uuid.uuid4()),
            "call_name": cmd.__class__.__name__,
            "args": cmd.input,
            "rets": cmd.result,
            "error": "",
            "perf": {
                "start_time": cmd.start_time.isoformat(),
                "end_time": cmd.end_time.isoformat()
            },
        }


class GenerateQuestionCommand(Command[GenerateQuestionsTypeDict]):

    def __init__(self, content: str, **kwargs) -> None:
        super().__init__(content=content, **kwargs)
        self.content = content

    def execute(self):
        questions, completion, prompt = generation_instance.generate_questions(
            content=self.content)
        self.result = {
            'questions': questions,
            'completion': completion.to_dict(),
            'prompt': prompt,
            'content': self.content
        }
        return self.result


class AskChatbotV1Command(Command[str]):
    def __init__(self, session_id: str, question: str, **kwargs) -> None:
        super().__init__(session_id=session_id, question=question, **kwargs)
        self.session_id = session_id
        self.question = question

    def execute(self) -> Any:
        url = f"{API_URL}/completion"
        payload = json.dumps({
            "session_id": self.session_id,
            "msg": self.question
        })
        headers = {
            'Content-Type': 'application/json'
        }
        response = requests.post(
            url,
            headers=headers,
            data=payload
        )
        if response.status_code != 200:
            print(f"Error: {response.status_code}")
            return
        data = response.json()
        self.result = data.get("answer")

        return self.result


class CheckingAnswerRelatedToContentCommand(Command[CheckingAnswerRelatedToContentTypeDict]):
    def __init__(self, question: str, answer: str, content: str, **kwargs) -> None:
        super().__init__(question=question, answer=answer, content=content, **kwargs)
        self.answer = answer
        self.content = content
        self.question = question

    def execute(self) -> Any:
        is_related, completion, prompt = generation_instance.check_if_answer_related_to_content(
            question=self.question,
            answer=self.answer,
            content=self.content
        )
        self.result = {
            "is_related": is_related,
            "completion": completion.to_dict(),
            "prompt": prompt,
            "question": self.question,
            "answer": self.answer,
            "content": self.content
        }
        return self.result


class GetHistoriesBySessionIdCommand(Command[list[GetHistoriesBySessionIdTypeDict]]):
    def __init__(self, session_id: str, num=5, **kwargs) -> None:
        super().__init__(session_id=session_id, num=num, **kwargs)
        self.session_id = session_id
        self.num = num

    def execute(self) -> Any:
        histories = []
        with get_session().connect() as conn:
            for row in conn.execute(db.select(Session).where(Session.c.session_id == self.session_id).order_by(Session.c.created_at.desc())).fetchmany(self.num):
                histories.insert(0, {
                    "role": row.role.value,
                    "content": row.content

                })
        self.result = histories
        return self.result


class SaveSessionCommand(Command[SessionTypeDict]):
    def __init__(self, session_id: str, role: RoleEnum, content: str, **kwargs) -> None:
        super().__init__(session_id=session_id, role=role, content=content, **kwargs)
        self.session_id = session_id
        self.role = role
        self.content = content

    def execute(self) -> Any:
        with get_session().connect() as conn:
            conn.execute(Session.insert().values(
                session_id=self.session_id, role=self.role, content=self.content))
            conn.commit()
        self.result = {"session_id": self.session_id}
        return self.result


class ChatbotController:
    commandHistories: list[Command]

    def executeCommand(self, command: Command[T], **kwargs) -> T:
        include_execution_time = kwargs.pop("include_execution_time", True)
        exclude_save_history = kwargs.pop("exclude_save_history", False)
        if include_execution_time:
            start_time = datetime.now(tz=timezone.utc)
            cmd = command.execute(**kwargs)
            end_time = datetime.now(tz=timezone.utc)
            command.set_execution_time(
                start_time=start_time, end_time=end_time
            )
        else:
            cmd = command.execute(**kwargs)
        if not exclude_save_history:
            self.commandHistories.append(command)
        return cmd

    def __init__(self) -> None:
        self.commandHistories = []

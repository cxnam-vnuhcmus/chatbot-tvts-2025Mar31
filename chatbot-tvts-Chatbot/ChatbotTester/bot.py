from enum import Enum
import os
import sys
from textwrap import dedent
from typing import Generator

sys.path.append(os.path.abspath("."))

from foundation import AskChatbotV1Command, ChatbotController, CheckingAnswerRelatedToContentCommand, GenerateQuestionCommand, SearchDocsByChunkIdCommand


class ChatCommand(Enum):
    FOUND_DOCUMENT = 1
    GENERATE_QUESTIONS = 2
    CHECKING_RELATED_TO_CONTENT = 3


class ChatEvaluationResponse:
    def __init__(self, chunk_id: str, question: str, ans: str, document: str):
        self.chunk_id = chunk_id
        self.question = question
        self.ans = ans
        self.document = document

    def to_str(self) -> str:
        return dedent(
            """
        Document: {document}
        Question: {question}
        Answer: {answer}
        """
        ).format(
            question=self.question,
            answer=self.ans,
            document=self.document
        )


class ChatEvaluation:
    def ask(self, session_id: str, chunk_id: str, **kwargs) -> Generator[tuple[ChatCommand, str], None, ChatEvaluationResponse]:

        controller = ChatbotController()

        resutls_chunk = controller.executeCommand(
            SearchDocsByChunkIdCommand(chunk_id=chunk_id))

        document = resutls_chunk.get("document")  # this is content of chunk
        yield ChatCommand.FOUND_DOCUMENT.name, str(document)

        results = controller.executeCommand(
            GenerateQuestionCommand(content=document))
        yield ChatCommand.GENERATE_QUESTIONS.name, str(results.get('questions'))

        question = results.get('questions')[0]
        results_call_chatbot = controller.executeCommand(
            AskChatbotV1Command(session_id=session_id, question=question))

        res_checking_related = controller.executeCommand(CheckingAnswerRelatedToContentCommand(
            question=question,
            answer=results_call_chatbot,
            content=document
        ))
        ans = dedent(f"""
        Chunk Id: {chunk_id}
        Question: {res_checking_related.get('question')}
        Answer: {res_checking_related.get('answer')}
        Is related: {"YES" if res_checking_related.get('is_related') else "No" }
        """)

        yield ChatCommand.CHECKING_RELATED_TO_CONTENT.name, ans

        return ChatEvaluationResponse(chunk_id=chunk_id, question=results.get('questions'), ans=ans, document=document)


def get_chatbot_evaluation_instance() -> ChatEvaluation:
    return ChatEvaluation()

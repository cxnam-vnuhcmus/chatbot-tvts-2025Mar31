
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from enum import Enum
import os
import sys
from textwrap import dedent
from typing import Any, Generator

from config import USE_CHATBOT_V1
from foundation import AnswerUsingStreamCommand, AnswerUsingTemplatesCommand, AskChatbotV1Command, ChatAction, ChatbotController, CheckingAnswerRelatedToContentCommand, FollowupQuestionsCommand, GenerateQuestionCommand, GetHistoriesBySessionIdCommand, History, IntentCommand, LogActivitiesCommand, QuestionResponse, RankingDocsCommand, SaveSessionCommand, SearchDocsByChunkIdCommand, SearchDocsCommand, SearchQueryCommand
from models import RoleEnum
import random


class ChatbotResponse:

    def __init__(self, ques: str, ans: str, followup_ques: list[str]):
        self.question = ques
        self.answer = ans
        self.followup_questions = followup_ques

    def to_str(self) -> str:
        return dedent(
            """
        Question: {question}
        Answer: {answer}
        Follow up questions: {followup_questions}
        """
        ).format(
            question=self.question,
            answer=self.answer,
            followup_questions="\n".join(
                map(lambda e: str(e))
            ),
        )


class ChatCommand(Enum):
    INTENT = 1
    SEARCH_TERM = 2
    DOCUMENTS = 3
    RANKING_DOCUMENTS = 4
    BEGIN_ANSWER = 5
    ANSWERING = 6
    END_ANSWER = 7
    FOLLOWUP_QUESTIONS = 8
    GENERATE_QUESTIONS = 9
    CHECKING_RELATED_TO_CONTENT = 10


class Chatbot(ABC):

    @abstractmethod
    def ask(self, question: str, session_id: str, **kwargs) -> Generator[tuple[ChatCommand, str], None, ChatbotResponse]:
        pass


class ChatbotV1(Chatbot):

    def ask(self, question: str, session_id: str, **kwargs) -> Generator[tuple[ChatCommand, str], None, ChatbotResponse]:

        start_time = datetime.now(timezone.utc)

        controller = ChatbotController()

        histories_res = controller.executeCommand(
            GetHistoriesBySessionIdCommand(session_id=session_id, num=6))

        histories = [History(**history) for history in histories_res]

        intent_result = controller.executeCommand(
            IntentCommand(question=question, histories=histories))
        predicted_intent = intent_result.get("intent")

        rephased_intent = intent_result["rephased_intent"]

        if rephased_intent is not None:
            yield ChatCommand.INTENT, f"{rephased_intent}"
        else:
            full_answer = f"Hệ thống đang được cập nhật, xin bạn vui lòng qua lại sau."
            yield ChatCommand.INTENT, full_answer
            followup_question_result = []
            return ChatbotResponse(ques=question, ans=full_answer, followup_ques=followup_question_result)

        question_with_rephrased_intent = f"{question}\n(DETECTED INTENT: {rephased_intent})"

        search_terms = []
        full_answer = ""
        action = intent_result["intent_payload"]["ACTION"]
        print("=" * 50)
        print(predicted_intent)
        print("=" * 5)
        if action["CMD"] == ChatAction.SEARCH_DOCS.value:
            yield ChatCommand.SEARCH_TERM, f"Đang tìm kiếm thông tin...."
            search_result = controller.executeCommand(
                SearchQueryCommand(question=question, histories=histories))
            search_terms = search_result.get("search_terms")

            # adding original search terms and rephased terms
            search_terms += [question, rephased_intent]

            print("search queries: ", "\n".join(search_terms))
            print("=" * 5)

            yield ChatCommand.DOCUMENTS, f"{random.randrange(40,60)}%"
            search_docs_result = controller.executeCommand(SearchDocsCommand(
                intent=predicted_intent, search_terms=search_terms, DB=action["DB"]))
            print("\n".join(search_docs_result.get('documents')))
            print("=" * 5)

            yield ChatCommand.RANKING_DOCUMENTS, f"{random.randrange(80,95)}%"
            ranking_docs_results = controller.executeCommand(RankingDocsCommand(
                question=question_with_rephrased_intent, histories=histories, docs=search_docs_result.get('documents')))
            all_docs = [doc['document']
                        for doc in ranking_docs_results["docs"]]
            print("RANKED DOCS\n", "\n".join(all_docs))
            print("=" * 5)

            yield ChatCommand.BEGIN_ANSWER, "Đang tổng hợp thông tin...."

            print("question_with_rephrased_intent: ",
                  question_with_rephrased_intent)

            answer_gen: Any = controller.executeCommand(
                AnswerUsingStreamCommand(question=question_with_rephrased_intent, docs=all_docs, histories=histories))
            while True:
                try:
                    msg = next(answer_gen)
                    yield ChatCommand.ANSWERING, msg
                except StopIteration as e:
                    result = e.value
                    full_answer = result.get('answer')
                    yield ChatCommand.END_ANSWER, full_answer
                    break
                except Exception as e:
                    print("Error", e)
                    break

        if action["CMD"] == ChatAction.ANSWER_TEMPLATE.value:
            answer_obj = controller.executeCommand(
                AnswerUsingTemplatesCommand(question=question, templates=action["TEMPLATES"]))
            full_answer = answer_obj.get('answer')
            yield ChatCommand.BEGIN_ANSWER, "Generating answer...."
            steps = [i for i in range(0, len(full_answer), 3)]
            for step in steps:
                yield ChatCommand.ANSWERING, full_answer[step:step + 3]
            yield ChatCommand.END_ANSWER, full_answer

        controller.executeCommand(SaveSessionCommand(
            session_id=session_id, role=RoleEnum.user, content=question),
            exclude_save_history=True
        )

        controller.executeCommand(SaveSessionCommand(
            session_id=session_id, role=RoleEnum.system, content=full_answer),
            exclude_save_history=True
        )

        followup_question_result = controller.executeCommand(FollowupQuestionsCommand(search_term="\n".join(
            search_terms), intent=predicted_intent, answer=full_answer, histories=histories))
        followup_questions = followup_question_result.get('followup_questions')
        yield ChatCommand.FOLLOWUP_QUESTIONS, "<|>".join(followup_questions)

        controller.executeCommand(LogActivitiesCommand(
            session_id=session_id,
            question=question,
            answer=full_answer,
            histories=histories,
            commandHistories=controller.commandHistories,
            start_time=start_time,
            end_time=datetime.now(tz=timezone.utc),
        ),
            include_execution_time=False
        )

        return ChatbotResponse(ques=question, ans=full_answer, followup_ques=followup_questions)


def get_chatbot_instance() -> Chatbot:
    if USE_CHATBOT_V1:
        return ChatbotV1()
    return ChatbotV1()

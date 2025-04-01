from prompt.promptDB import PromptDB
from evaluation.largeLanguageModel import LargeLanguageModel

class RateService():
    def __init__(self):
        self.llm = LargeLanguageModel()
        self.prompt_DB = PromptDB()

    def get_context_relevance(self, question: str, context: str) -> float: 
        system_prompt = self.get_system_promt(self.prompt_DB.context_relevance.system_prompt)
        user_prompt = str.format(self.prompt_DB.context_relevance.user_prompt, question=question, context=context)
        score = self.llm.generate_score(system_prompt, user_prompt)
        return score

    def get_answer_relevance(self, question: str, answer: str) -> float: 
        system_prompt = self.get_system_promt(self.prompt_DB.answer_relevance.system_prompt)
        user_prompt = str.format(self.prompt_DB.answer_relevance.user_prompt, question=question, answer=answer)
        score = self.llm.generate_score(system_prompt, user_prompt)
        return score

    def get_groundedness(self, answer: str, context: str) -> float: 
        system_prompt = self.get_system_promt(self.prompt_DB.groundedness.system_prompt)
        user_prompt = str.format(self.prompt_DB.groundedness.user_prompt, answer=answer, context=context)
        score = self.llm.generate_score(system_prompt, user_prompt)
        return score

    def get_sentiment(self, histories: str, question: str, answer: str) -> float: 
        system_prompt = self.get_system_promt(self.prompt_DB.sentiment.system_prompt)
        user_prompt = str.format(self.prompt_DB.sentiment.user_prompt, histories=histories, question=question, answer=answer)
        score = self.llm.generate_score(system_prompt, user_prompt)
        return score


    def get_system_promt(self, system_prompt) -> str:
        if (isinstance(system_prompt, tuple)): 
            return "".join(map(str, system_prompt))
        return system_prompt
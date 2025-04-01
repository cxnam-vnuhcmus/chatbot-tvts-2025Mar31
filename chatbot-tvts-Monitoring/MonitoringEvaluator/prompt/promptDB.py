from prompt.contextRelevance import ContextRelevance
from prompt.answerRelevance import AnswerRelevance
from prompt.groundedness import Groundedness
from prompt.sentiment import Sentiment

class PromptDB():
    def __init__(self):
        self.context_relevance = ContextRelevance()
        self.answer_relevance = AnswerRelevance()
        self.groundedness = Groundedness()
        self.sentiment = Sentiment()

    def get_list_prompts(self):
        return [
            self.context_relevance.__class__.__name__,
            self.answer_relevance.__class__.__name__,
            self.groundedness.__class__.__name__,
            self.sentiment.__class__.__name__,
        ]
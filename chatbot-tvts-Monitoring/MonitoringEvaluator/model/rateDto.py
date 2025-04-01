class RateDto:
    def __init__(self, id=None, record_id=None, conversation_id=None, csat=None, groundedness=None, answer_relevance=None, context_relevance=None, sentiment=None, created_date=None):
        self.id = id
        self.record_id = record_id
        self.conversation_id = conversation_id
        self.csat = csat
        self.groundedness = groundedness
        self.answer_relevance = answer_relevance
        self.context_relevance = context_relevance
        self.sentiment = sentiment
        self.created_date = created_date

    def __dict__(self):
        return dict(
            id = self.id,
            record_id = self.record_id,
            conversation_id = self.conversation_id,
            csat = self.csat,
            groundedness = self.groundedness,
            answer_relevance = self.answer_relevance,
            context_relevance = self.context_relevance,
            sentiment = self.sentiment,
            created_date = self.created_date,
        )
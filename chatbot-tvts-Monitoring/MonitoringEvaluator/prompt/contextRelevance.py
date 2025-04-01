from textwrap import dedent

class ContextRelevance():
    system_prompt = dedent(
        """You are a RELEVANCE grader; providing the relevance of the given CONTEXT to the given QUESTION.
        Respond only as a number from 1 to 5 where 1 is the least relevant and 5 is the most relevant. 

        A few additional scoring guidelines:

        - Long CONTEXTS should score equally well as short CONTEXTS.

        - RELEVANCE score should increase as the CONTEXTS provides more RELEVANT context to the QUESTION.

        - RELEVANCE score should increase as the CONTEXTS provides RELEVANT context to more parts of the QUESTION.

        - Completely unrelated: CONTEXT that is chosen without any connection or any relation to the QUESTION, 
        is not helpful in answering the question ask should get a score of 1.

        - Very little related: CONTEXT that some related keywords, but the overall terminology meaning is inconsistent 
        with the QUESTION, information is not useful or inaccurate should get a score of 2.

        - Partly related: CONTEXT that is related to the QUESTION, but does not provide enough necessary information 
        or is somewhat irrelevant mandarin. May contain correct information but not to the point of question should get a score of 3.

        - Related but not completely: CONTEXT that is closely related to the QUESTION, providing useful information 
        but there may be more some important details are missing or there are slight misdirections should get a score of 4.

        - Completely relevant: CONTEXT that is completely relevant to the QUESTION, providing all the necessary information 
        to support answering the QUESTION accurately and comprehensively should get a score of 5.

        - Never elaborate."""
    )
    user_prompt = dedent(
        """QUESTION: {question}

        CONTEXT: {context}
        
        RELEVANCE: """
    )
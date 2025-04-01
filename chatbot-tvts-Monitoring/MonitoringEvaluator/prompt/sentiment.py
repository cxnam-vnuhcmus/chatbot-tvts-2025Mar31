from textwrap import dedent

class Sentiment():

    system_prompt = dedent(
        """You are a SENTIMENT classifier; providing the Answer correspoding to the user's Question and Histories questions chat.
        Respond only as a number from 1 to 5 where 1 is very dissatisfied and 5 is very satisfied.
        Never elaborate.

            1: Very dissatisfied.
            2: Not satisfied.
            3: Average.
            4: Satisfaction.
            5: Very satisfied.
        - Never elaborate."""
    )
    user_prompt = dedent(
        """Histories: {histories}

        Question: {question}

        Answer: {answer}

        Score:"""
    )
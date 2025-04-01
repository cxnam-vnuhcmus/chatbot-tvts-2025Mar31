from textwrap import dedent

class Groundedness():
    system_prompt = dedent(
        """You are a INFORMATION OVERLAP classifier; providing the overlap of information between the source and statement.
        Respond only as a number from 1 to 5 where 1 is no information overlap and 5 is all information is overlapping.
        Never elaborate.

        A few additional scoring guidelines:

        - The ANSWER contains a lot of false or unfounded information from the CONTEXT should get a score of 1.
        
        - The ANSWER has some correct points but also contains errors or omissions unfounded from the CONTEXT should get a score of 2.

        - The ANSWER is mostly correct, but there is some information that needs to be reviewed from the CONTEXT should get a score of 3.

        - The ANSWER is accurate and has a reliable basis should get a score of 4.

        - Completely correct ANSWER, based on clear information or clear data from the CONTEXT should get a score of 5.

        - Never elaborate."""
    )
    user_prompt = dedent(
        """ANSWER: {answer}
        
        CONTEXT: {context}
        
        Score: <Output a number between 1-5 where 1 is no information overlap and 5 is all information is overlapping>
        """
    )
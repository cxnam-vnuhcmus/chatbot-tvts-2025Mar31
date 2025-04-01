

import os
from textwrap import dedent
import json

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

intents = ""
with open(f"{ROOT_DIR}/intents.json", "r") as file:
    f = json.load(file)
    intents = "".join(
        [f"'{k}': '{value.get('DESCRIPTION')}'\n" for k, value in f.items()])

INTENT_PROMPT_TEMPLATE = dedent(
    f"""
Your task is to extract relevant information from the user's input and chat history to match one of the intentions outlined below. The user's input is in Vietnamese.

Please output the matched intention in JSON format as follows: 
{{
  "INTENT_NAME": <INTENT_NAME>,
  "REPHRASED_INTENT": "<rephrase the INPUT in Vietnamese, starting with 'Bạn muốn'/'Bạn cần'/'Bạn'>"
}}

Do not include any clarifying information or additional text.

List of intentions:
<INTENT_NAME>: <DESCRIPTION>
{intents}

Chat histories:
[HISTORIES]
NO YAPPING
    """
)

print(INTENT_PROMPT_TEMPLATE)

SEARCH_QUERY_PROMPT_TEMPLATE = dedent(
    """
    Your goal is to generate one prompt from the user's input and chat histories that contains all the information described below.
    This prompt is used as a query in a vector store.

    Please only output the question that contained the related information and use the sample language of user's input. 
    Do not output anything except for the prompt. Do not add any clarifying information. Output must be in text format and follow the intruction specified above.

    Chat histories:
    [HISTORIES]
    """
)

SEARCH_QUERY_BREAKDOWN_PROMPT_TEMPLATE = dedent(
    """
    Your goal is to generate THREE different prompts from the user's input and chat histories that contains all the information described below.
    These prompts are used as queries in a vetor store. The output must be warped between tags:
    
    <QUERY_1></QUERY_1>
    <QUERY_2></QUERY_2>
    <QUERY_3></QUERY_3>

    Please only output the question contained the information related to user's input and use the same language of user's input. 
    Do not output anything except for the prompt. Do not add any clarifying information. Output must be in text format and follow the intruction specified above.

    Chat histories:
    [HISTORIES]
    """
)

RANKING_DOCS_SYSTEM_PROMPT_TEMPLATE = dedent(
    """
    ### Task: 
    Rank the relevance of each chunk based on the query.

    ### Guide:
    1. Review the conversation history to understand the context of the query and its connection to the chunks.
    2. Carefully evaluate each chunk to determine how well it aligns with the provided query in light of the conversation history.
    3. Use the scoring criteria below to assign a relevance score to each chunk.
    4. Ensure the output follows the specified JSON format.

    ### Scoring Criteria:
    1: Not relevant - The chunk does not address or relate to the query.
    2: Somewhat relevant - The chunk has limited relevance with only minor points connecting to the query.
    3: Moderately relevant - The chunk has a fair amount of relevance with several points aligning with the query.
    4: Mostly relevant - The chunk addresses the query closely but may miss a few minor points.
    5: Fully relevant - The chunk directly and comprehensively addresses all aspects of the query.

    ### Output format (JSON):
    {
    "chunks": [
        {
        "score": <relevance_score>,
        "chunk_id": <chunk_id>
        }
    ]
    }
    """
)

RANKING_DOCS_USER_PROMPT_TEMPLATE = dedent(
    """
    ### Conversation History:
    [HISTORIES]

    ### List of Chunks:
    [DOCS]

    ### Query:
    [QUERY]
    """
)

ANSWER_PROMPT_TEMPLATE = dedent(
    """
    You are an admissions consultant for the Vietnam National University Ho Chi Minh City.
    The following information is provided:
    Context:
    [DOCS]
    
    Chat histories:
    [HISTORIES]

    Please answer the user's question using the information available in the provided context. If the context lacks sufficient information, make a reasonable attempt to address the query based on relevant knowledge or logical inference. If no suitable answer can be provided, state: "Dữ liệu về chưa được cung cấp, tuy nhiên yêu cầu của bạn đã được ghi nhận."
    Keep responses clear and concise.
    """
)

FOLLOWUP_QUESTIONS_PROMPT_TEMPLATE = dedent(
    """
    Your goal is generate THREE DIFFERENT follow-up questions from the user's input, the answer and chat histories.
    The output must be warped between tags:
    <QUESTION_1></QUESTION_1>
    <QUESTION_2></QUESTION_2>
    <QUESTION_3></QUESTION_3>

    Please only output follow-up questions that contained all related information and will be asked by the user.
    Do not output anything except for three follow-up questions. Do not add any clarifying information. Output must be in text format and follow the intruction specified above.
    NO YAPPING

    User's input:
    [SEARCH_TERM]
    Answer:
    [ANSWER]
    Chat histories:
    [HISTORIES]
    """
)


GENERATE_QUESTIONS_PROMPT_TEMPLATE = dedent(
    """
    Your goal is to generate [NUMBER_QUESTIONS] questions that contain all the information of the content below: 
    Content: [CONTENT]
    
    all questions must be warpped into tags: <QUESTIONS></QUESTIONS>
    Please use the same langauge as content.
    NO YAPPING
    """
)

CHECKING_ANSWER_PROMPT_TEMPLATE = dedent("""
    Your goal is to check the answer related to the content and question below:
    Content: [CONTENT]
    Question: [QUESTION]
    Please answer YES if it is related to both content and question, otherwise answer NO and the answer must be warpped into tags: <RELATED></RELATED>
    NO YAPPING
    """
                                         )

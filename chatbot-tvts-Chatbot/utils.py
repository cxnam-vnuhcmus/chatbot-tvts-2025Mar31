from openai.types.chat import ChatCompletion
import re

# Generate between <JSON> and </JSON> tag
def _extract_tag_content(s: str, tag: str) -> str:
    m = re.search(rf"<{tag}>(.*)</{tag}>", s, re.MULTILINE | re.DOTALL)
    if m:
        return m.group(1)
    else:
        m = re.search(rf"<{tag}>(.*)<{tag}>", s, re.MULTILINE | re.DOTALL)
        if m:
            return m.group(1)
    return ""

def _get_content(c : ChatCompletion) -> str:
    return str(c.choices[0].message.content)


import logging
from bs4 import BeautifulSoup
import re
import traceback
import Levenshtein
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def format_date(date):
    """
    Format a datetime object into a string.

    Args:
        date (datetime): The datetime object to be formatted.

    Returns:
        str: The formatted date as a string in 'YYYY-MM-DD HH:MM:SS' format,
            or an empty string if the date is None.
    """
    return date.strftime("%Y-%m-%d %H:%M:%S") if date else ""

def format_content_markdown(content: str) -> str:
    """
    Format content for main document view while preserving original structure.
    Used in update_detail_view.
        
    Args:
        content (str): Raw content string to format
                
        Returns:
            str: Formatted markdown string preserving structure
        """
    if not content:
        return ""
        
    def format_links(text):
        if 'http' in text or 'www.' in text:
            urls = re.findall(r'(https?://[^\s]+|www\.[^\s]+)', text)
            for url in urls:
                if not url.startswith(('http://', 'https://')):
                    full_url = 'http://' + url
                else:
                    full_url = url
                text = text.replace(url, f'[{url}]({full_url})')
        return text
        
    result = []
    lines = content.split('\n')
        
    for line in lines:
        line = line.strip()
        if not line:
            result.append('')  
            continue
                
        # Format links in the line    
        line = format_links(line)
            
        # Preserve the original bullet points
        if line.startswith('•'):
            result.append(line)  
        elif line.startswith('-'):
            result.append(line.replace('-', '•'))  
        else:
            result.append(line)
        
    # Clean up multiple consecutive empty lines
    formatted = '\n'.join(result)
    formatted = re.sub(r'\n{3,}', '\n\n', formatted)
        
    return formatted

def remove_html(html_content):
    """
    Removes HTML tags from the given content and returns plain text.
    
    Args:
        html_content (str): The input HTML content as a string.
        
    Returns:
        str: The plain text content without HTML tags.
    """
    try:
        if not html_content:
            return ""
            
        html_content = str(html_content)
            
        if '<' in html_content and '>' in html_content:
            soup = BeautifulSoup(html_content, 'html.parser')
            text = soup.get_text(separator=' ', strip=True)
        else:
            text = ' '.join(html_content.split())
            
        return text
        
    except Exception as e:
        logger.error(f"Error removing HTML: {str(e)}")
        logger.error(traceback.format_exc())
        return str(html_content)

def preprocessing(doc):
    """
    Enhanced Vietnamese text preprocessing.
    
    Args:
        doc (str): Input text document.
        
    Returns:
        str: Preprocessed text.
    """
    try:
        if not isinstance(doc, str):
            logger.warning(f"Input is not string: {type(doc)}")
            return ""

        # Define valid Vietnamese characters
        valid_chars = set('aàáảãạâầấẩẫậăằắẳẵặbcdđeèéẻẽẹêềếểễệfghiìíỉĩịjklmnoòóỏõọôồốổỗộơờớởỡợpqrstuùúủũụưừứửữựvwxyỳýỷỹỵz0123456789 ,.!?()[]{}"\'-+=%$@#&*:;')
            
        # Remove HTML
        doc = remove_html(doc)

        # Convert to lowercase
        doc = doc.lower()
        
        # Process text in chunks for large documents
        words = []
        for word in doc.split():
            cleaned_word = ''.join(char for char in word if char in valid_chars)
            if cleaned_word:
                words.append(cleaned_word)
        
        # Join words and normalize whitespace
        processed_text = ' '.join(words)
        
        # Remove redundant punctuation
        processed_text = re.sub(r'([,.!?])\1+', r'\1', processed_text)
        
        # Normalize spaces around punctuation
        processed_text = re.sub(r'\s*([,.!?])\s*', r'\1 ', processed_text)
        
        processed_text = ' '.join(processed_text.split())
            
        return processed_text.strip()
        
    except Exception as e:
        logger.error(f"Error in preprocessing: {str(e)}")
        logger.error(traceback.format_exc())
        return ""

def ratio(text1, text2):
    """
    Calculate similarity ratio between two texts.
    
    Args:ßß
        text1 (str): First text string
        text2 (str): Second text string
        
    Returns:
        float: Similarity ratio (0-1)
    """
    try:
        if not text1 or not text2:
            return 0.0
            
        text1 = ' '.join(text1.lower().split())
        text2 = ' '.join(text2.lower().split())
        
        similarity = Levenshtein.ratio(text1, text2)
        
        if not (0 <= similarity <= 1):
            logger.warning(f"Invalid similarity score: {similarity}")
            return 0.0
            
        return similarity
        
    except Exception as e:
        logger.error(f"Error calculating ratio: {str(e)}")
        logger.error(traceback.format_exc())
        return 0.0
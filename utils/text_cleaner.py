# chuẩn hóa description
# utils/text_cleaner.py
from bs4 import BeautifulSoup
import re
import html
import unicodedata

def clean_description(raw_html: str | None, max_len: int = 2000) -> str:
    """
    Convert HTML description to clean text, normalize whitespace, unescape entities,
    remove excessive newlines and truncate to max_len characters.
    """
    if not raw_html:
        return ""

    # Unescape HTML entities first
    try:
        raw = html.unescape(raw_html)
    except Exception:
        raw = raw_html

    # Parse HTML and extract visible text
    soup = BeautifulSoup(raw, "lxml")  # fast and robust
    text = soup.get_text(separator=" ", strip=True)

    # Normalize unicode (NFKC/NFKD depending needs)
    text = unicodedata.normalize("NFKC", text)

    # Replace multiple whitespace/newlines with single space
    text = re.sub(r"\s+", " ", text).strip()

    # Optionally shorten - attempt to cut at sentence boundary if possible
    if len(text) > max_len:
        # try to cut at last period before max_len
        cut = text.rfind(".", 0, max_len)
        if cut == -1 or cut < max_len * 0.5:
            text = text[:max_len]
        else:
            text = text[:cut+1]

    return text

from pathlib import Path

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR  = DATA_DIR / "raw"
META_DIR = DATA_DIR / "metadata"
EXTR_DIR = DATA_DIR / "extracted"

DB_PATH  = META_DIR / "publications.db"

# Polite crawl settings
REQUEST_DELAY = 1.5   # seconds between requests
REQUEST_TIMEOUT = 30  # seconds
USER_AGENT = (
    "Meridian17-ResearchBot/1.0 "
    "(academic research; contact: info@meridian17.org)"
)

SOURCES = {
    "ecdpm": {
        "name": "European Centre for Development Policy Management",
        "base_url": "https://ecdpm.org",
        "publications_url": "https://ecdpm.org/work/publications",
        "type": "wordpress",
    },
    "odi": {
        "name": "Overseas Development Institute",
        "base_url": "https://odi.org",
        "publications_url": "https://odi.org/en/publications/",
        "type": "custom",
    },
    "saiia": {
        "name": "South African Institute of International Affairs",
        "base_url": "https://saiia.org.za",
        "publications_url": "https://saiia.org.za/research/",
        "type": "wordpress",
    },
    "tips": {
        "name": "Trade & Industrial Policy Strategies",
        "base_url": "https://tips.org.za",
        "publications_url": "https://tips.org.za/research-archive",
        "type": "wordpress",
    },
    "policy_center": {
        "name": "Policy Center for the New South",
        "base_url": "https://www.policycenter.ma",
        "publications_url": "https://www.policycenter.ma/publications",
        "type": "custom",
    },
    "acet": {
        "name": "African Center for Economic Transformation",
        "base_url": "https://acetforafrica.org",
        "publications_url": "https://acetforafrica.org/publications/",
        "type": "wordpress",
    },
}

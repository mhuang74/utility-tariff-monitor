import os
import logging
import sqlite3
import hashlib
import requests
from datetime import datetime
from urllib.parse import urlparse, urlunparse
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain

# Load environment variables
load_dotenv()
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
UTILITY_NAME = "Austin Energy"
TARGET_URL = "https://austinenergy.com/rates/approved-rates-schedules"
DB_PATH = "./resources/tariff_monitor.db"

def setup_database():
    """Initialize the SQLite database and create table if not exists."""
    logger.info("Setting up database...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tariff_documents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            utility_name TEXT NOT NULL,
            url TEXT NOT NULL,
            document_name TEXT,
            hash TEXT,
            last_checked DATETIME,
            last_updated DATETIME,
            status TEXT
        )
    ''')
    conn.commit()
    conn.close()
    logger.info("Database setup complete.")

def scrape_links(url):
    """Scrape all PDF links from the given URL."""
    logger.info(f"Scraping links from {url}")
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        links = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            is_pdf = '.pdf' in href.lower()
            if is_pdf:
                full_url = href if href.startswith('http') else f"https://austinenergy.com{href}"
                parsed = urlparse(full_url)
                clean_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, '', '', ''))
                logger.info(f"Cleaned Potential Tariff PDF URL: {clean_url}")
                links.append({
                    'text': a.get_text(strip=True),
                    'url': clean_url
                })
        logger.info(f"Found {len(links)} PDF links")
        return links
    except requests.RequestException as e:
        logger.error(f"Error scraping links: {e}")
        return []

def select_best_url_with_llm(links):
    """Use LLM to select the most likely URL for commercial tariff rates."""
    logger.info("Using LLM to select best URL...")
    if not GOOGLE_API_KEY:
        logger.error("GOOGLE_API_KEY not found in environment")
        return None

    try:
        llm = ChatGoogleGenerativeAI(model="gemini-2.0-flash", google_api_key=GOOGLE_API_KEY)
        prompt = PromptTemplate(
            input_variables=["links"],
            template="""
            Analyze the following list of PDF links and their text descriptions.
            Identify the most likely URL that contains the Electric Utility Commercial Tariff Rates document.
            Look for keywords like "commercial", "tariff", "rates", "schedule", etc.
            Return only the URL of the best match, nothing else.

            Links:
            {links}
            """
        )
        chain = LLMChain(llm=llm, prompt=prompt)
        links_text = "\n".join([f"Text: {link['text']}\nURL: {link['url']}" for link in links])
        result = chain.run(links=links_text)
        selected_url = result.strip()
        logger.info(f"LLM selected URL: {selected_url}")
        return selected_url
    except Exception as e:
        logger.error(f"Error with LLM: {e}")
        return None

def download_and_hash_pdf(url):
    """Download PDF and compute hash."""
    logger.info(f"Downloading PDF from {url}")
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/pdf,*/*',
        }
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        if 'application/pdf' not in response.headers.get('content-type', ''):
            logger.error("Downloaded content is not a PDF")
            return None, None

        content = response.content
        pdf_hash = hashlib.sha256(content).hexdigest()
        document_name = url.split('/')[-1] or "unknown.pdf"
        logger.info(f"PDF downloaded, hash: {pdf_hash}")
        return pdf_hash, document_name
    except requests.RequestException as e:
        logger.error(f"Error downloading PDF: {e}")
        return None, None

def update_database(utility_name, url, document_name, pdf_hash):
    """Update or insert record in database."""
    logger.info("Updating database...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    now = datetime.now()

    # Check if URL exists
    cursor.execute("SELECT id, hash FROM tariff_documents WHERE utility_name = ? AND url = ?", (utility_name, url))
    existing = cursor.fetchone()

    if existing:
        # Update existing
        if existing[1] != pdf_hash:
            cursor.execute("""
                UPDATE tariff_documents
                SET hash = ?, last_checked = ?, last_updated = ?
                WHERE id = ?
            """, (pdf_hash, now, now, existing[0]))
            logger.info("Updated existing record")
        else:
            cursor.execute("""
                UPDATE tariff_documents
                SET last_checked = ?
                WHERE id = ?
            """, (now, existing[0]))
            logger.info("No changes detected")
    else:
        # Mark existing as obsolete
        cursor.execute("""
            UPDATE tariff_documents
            SET status = 'OBSOLETE'
            WHERE utility_name = ? AND status = 'ACTIVE'
        """, (utility_name,))

        # Insert new
        cursor.execute("""
            INSERT INTO tariff_documents (utility_name, url, document_name, hash, last_checked, last_updated, status)
            VALUES (?, ?, ?, ?, ?, ?, 'ACTIVE')
        """, (utility_name, url, document_name, pdf_hash, now, now))
        logger.info("Inserted new record")

    conn.commit()
    conn.close()

def main():
    """Main execution function."""
    logger.info("Starting utility tariff monitor")
    setup_database()

    links = scrape_links(TARGET_URL)
    if not links:
        logger.error("No links found")
        return

    best_url = select_best_url_with_llm(links)
    if not best_url:
        logger.error("No URL selected by LLM")
        return

    pdf_hash, document_name = download_and_hash_pdf(best_url)
    if not pdf_hash:
        logger.error("Failed to download or hash PDF")
        return

    update_database(UTILITY_NAME, best_url, document_name, pdf_hash)
    logger.info("Process complete")

if __name__ == "__main__":
    main()

import os
import logging
import sqlite3
import hashlib
import requests
import argparse
from datetime import datetime
from urllib.parse import urlparse, urlunparse
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from email.utils import parsedate_to_datetime
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
            tariff_last_updated DATETIME,
            status TEXT
        )
    ''')
    conn.commit()
    conn.close()
    logger.info("Database setup complete.")

def read_seed_urls(input_file):
    """Read seed URLs from input file, one URL per line."""
    urls = []
    try:
        with open(input_file, 'r') as f:
            for line in f:
                url = line.strip()
                if url and not url.startswith('#'):  # Skip empty lines and comments
                    urls.append(url)
        logger.info(f"Read {len(urls)} seed URLs from {input_file}")
        return urls
    except FileNotFoundError:
        logger.error(f"Input file not found: {input_file}")
        return []
    except Exception as e:
        logger.error(f"Error reading input file: {e}")
        return []

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

        # Get base URL for relative links
        parsed_base = urlparse(url)
        base_url = f"{parsed_base.scheme}://{parsed_base.netloc}"

        for a in soup.find_all('a', href=True):
            href = a['href']
            is_pdf = '.pdf' in href.lower()
            if is_pdf:
                if href.startswith('http'):
                    full_url = href
                elif href.startswith('//'):
                    full_url = f"https:{href}"
                elif href.startswith('/'):
                    full_url = f"{base_url}{href}"
                else:
                    full_url = f"{base_url}/{href}"

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
            return None, None, None

        content = response.content
        pdf_hash = hashlib.sha256(content).hexdigest()
        document_name = url.split('/')[-1] or "unknown.pdf"

        # Parse Last-Modified header
        last_modified = None
        if 'Last-Modified' in response.headers:
            try:
                last_modified = parsedate_to_datetime(response.headers['Last-Modified'])
                logger.info(f"Last-Modified header found: {last_modified}")
            except Exception as e:
                logger.warning(f"Failed to parse Last-Modified header: {e}")

        logger.info(f"PDF downloaded, hash: {pdf_hash}")
        return pdf_hash, document_name, last_modified
    except requests.RequestException as e:
        logger.error(f"Error downloading PDF: {e}")
        return None, None, None

def update_database(utility_name, url, document_name, pdf_hash, last_modified):
    """Update or insert record in database."""
    logger.info("Updating database...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    now = datetime.now()

    # Determine tariff_last_updated value
    tariff_last_updated = last_modified if last_modified else now

    # Check if URL exists
    cursor.execute("SELECT id, hash FROM tariff_documents WHERE utility_name = ? AND url = ?", (utility_name, url))
    existing = cursor.fetchone()

    if existing:
        # Update existing
        if existing[1] != pdf_hash:
            cursor.execute("""
                UPDATE tariff_documents
                SET hash = ?, last_checked = ?, tariff_last_updated = ?
                WHERE id = ?
            """, (pdf_hash, now, tariff_last_updated, existing[0]))
            logger.info("Updated existing record with new hash")
        else:
            cursor.execute("""
                UPDATE tariff_documents
                SET last_checked = ?
                WHERE id = ?
            """, (now, existing[0]))
            logger.info("No changes detected, only updated last_checked")
    else:
        # Mark existing as obsolete
        cursor.execute("""
            UPDATE tariff_documents
            SET status = 'OBSOLETE'
            WHERE utility_name = ? AND status = 'ACTIVE'
        """, (utility_name,))

        # Insert new
        cursor.execute("""
            INSERT INTO tariff_documents (utility_name, url, document_name, hash, last_checked, tariff_last_updated, status)
            VALUES (?, ?, ?, ?, ?, ?, 'ACTIVE')
        """, (utility_name, url, document_name, pdf_hash, now, tariff_last_updated))
        logger.info("Inserted new record")

    conn.commit()
    conn.close()

def get_utility_name_from_url(url):
    """Derive utility name from URL domain."""
    parsed = urlparse(url)
    domain = parsed.netloc
    # Remove www. prefix if present
    if domain.startswith('www.'):
        domain = domain[4:]
    # Capitalize words
    utility_name = ' '.join(word.capitalize() for word in domain.split('.'))
    return utility_name

def process_seed_url(seed_url):
    """Process a single seed URL through the full pipeline."""
    logger.info(f"{'='*60}")
    logger.info(f"PROCESSING SEED URL: {seed_url}")
    logger.info(f"{'='*60}")

    utility_name = get_utility_name_from_url(seed_url)
    logger.info(f"Derived utility name: {utility_name}")

    links = scrape_links(seed_url)
    if not links:
        logger.error(f"No links found for {seed_url}")
        return

    best_url = select_best_url_with_llm(links)
    if not best_url:
        logger.error(f"No URL selected by LLM for {seed_url}")
        return

    pdf_hash, document_name, last_modified = download_and_hash_pdf(best_url)
    if not pdf_hash:
        logger.error(f"Failed to download or hash PDF for {seed_url}")
        return

    update_database(utility_name, best_url, document_name, pdf_hash, last_modified)
    logger.info(f"Completed processing for {seed_url}")

def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description='Monitor utility tariff documents')
    parser.add_argument('--tariff-webpage-urls', required=True, help='Path to file containing tariff webpage URLs (one per line)')
    parser.add_argument('--initialize', action='store_true', help='Initialize the database')

    args = parser.parse_args()

    logger.info("Starting utility tariff monitor")

    if args.initialize:
        setup_database()

    seed_urls = read_seed_urls(args.tariff_webpage_urls)
    if not seed_urls:
        logger.error("No seed URLs found in input file")
        return

    logger.info(f"Processing {len(seed_urls)} seed URLs")

    for seed_url in seed_urls:
        try:
            process_seed_url(seed_url)
        except Exception as e:
            logger.error(f"Error processing {seed_url}: {e}")
            continue

    logger.info("All seed URLs processed")

if __name__ == "__main__":
    main()

import os
import logging
import sqlite3
import hashlib
import requests
import argparse
from datetime import datetime
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
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

# Centralized headers to mimic browser requests
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Cache-Control': 'max-age=0',
}

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
            status TEXT,
            link_text TEXT
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

def extract_link_context(a_tag):
    """Extract contextual text for a link by traversing the DOM tree."""
    context_parts = []
    link_text = a_tag.get_text(strip=True)
    context_parts.append(link_text)

    current = a_tag.parent
    max_levels = 3
    for level in range(max_levels):
        if not current:
            break

        # Check preceding siblings for headings or paragraphs
        prev_sib = current.previous_sibling
        while prev_sib:
            if prev_sib.name in ['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                sib_text = prev_sib.get_text(strip=True)
                if sib_text and len(sib_text) > 5:  # Avoid very short texts
                    context_parts.insert(0, sib_text)
                    break  # Take the first relevant sibling
            prev_sib = prev_sib.previous_sibling

        # Move to parent
        current = current.parent

    # Combine and truncate to reasonable length
    full_context = ' '.join(context_parts)
    if len(full_context) > 500:
        full_context = full_context[:500] + '...'
    elif len(full_context) < 30:
        # If too short, try to get more from parent's text
        if a_tag.parent:
            parent_text = a_tag.parent.get_text(strip=True)
            if len(parent_text) > len(full_context):
                full_context = parent_text[:500] if len(parent_text) > 500 else parent_text

    return full_context

def scrape_links(url):
    """Scrape all PDF links from the given URL."""
    logger.info(f"Scraping links from {url}")
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
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
                # Selectively strip query parameters that cause cache misses
                query_params = parse_qs(parsed.query)
                filtered_params = {k: v for k, v in query_params.items() if k.lower() not in ['rev', 'hash']}
                new_query = urlencode(filtered_params, doseq=True)
                clean_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))
                link_text = a.get_text(strip=True)
                context = extract_link_context(a)
                logger.info(f"PDF LINK: {link_text} | Context: {context} | URL: {clean_url}")
                links.append({
                    'text': link_text,
                    'url': clean_url,
                    'context': context
                })
        logger.info(f"Found {len(links)} PDF links")
        return links
    except requests.RequestException as e:
        logger.error(f"Error scraping links: {e}")
        return []

def select_best_url_with_llm(links):
    """Use LLM to select URLs for commercial tariff rates and return with rationales and response."""
    logger.info("Using LLM to select best URLs...")
    if not GOOGLE_API_KEY:
        logger.error("GOOGLE_API_KEY not found in environment")
        raise ValueError("GOOGLE_API_KEY not found in environment")

    try:
        llm = ChatGoogleGenerativeAI(model="gemini-2.0-flash", google_api_key=GOOGLE_API_KEY)
        prompt = PromptTemplate(
            input_variables=["links"],
            template="""
            Analyze the following list of PDF links, their text descriptions, and contextual information from the webpage.
            Identify all URLs that contain Electric Utility Commercial Tariff Rates documents.
            Look for keywords like "commercial", "general service", "standard rates", "electrical service", "electric service", "tariff", "rates", "fees", "charges", "fees & charges", "schedule" in the text, context, and URL.
            Similarly, avoid keywords like "residential", "industrial", "wholesale", "transmission", "school", "church", "municipal", "large power".
            If multiple tariffs are available, select one approved tariff from the current year.
            If multiple Utility Companies are listed, return one tariff for each Utility.
            Use the context to understand the hierarchical structure and relevance of each link.

            IMPORTANT: Your response must be ONLY a valid JSON object. Do not include any explanations, comments, or additional text outside the JSON.

            Return a JSON object with two keys:
            - "urls": an array where each element is an object with "url" and "rationale"
            - "response": a string explaining the selection process or issues encountered

            If no suitable URLs are found, set "urls" to an empty array and provide an explanation in "response" about why no URLs were selected.

            Example response format (return ONLY the JSON, nothing else):
            {{
                "urls": [
                    {{
                        "url": "https://example.com/abc_tariff.pdf",
                        "rationale": "Contains commercial electrical service rates for Utility ABC"
                    }},
                    {{
                        "url": "https://example.com/xyz_tariff.pdf",
                        "rationale": "General service tariff document with commercial rates for Utility XYZ"
                    }}
                ],
                "response": "Selected two commercial tariff documents from different utilities based on keyword matching and context analysis."
            }}

            Links:
            {links}
            """
        )
        chain = LLMChain(llm=llm, prompt=prompt)
        links_text = "\n".join([f"Text: {link['text']}\nContext: {link['context']}\nURL: {link['url']}" for link in links])
        result = chain.run(links=links_text)
        result = result.strip()

        # Parse JSON response - handle markdown code blocks
        import json
        import re

        # Extract JSON from markdown code blocks if present
        json_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', result, re.DOTALL)
        if json_match:
            json_content = json_match.group(1)
        else:
            # Try to find JSON object directly
            json_match = re.search(r'(\{.*\})', result, re.DOTALL)
            if json_match:
                json_content = json_match.group(1)
            else:
                json_content = result

        try:
            response_data = json.loads(json_content)
            if not isinstance(response_data, dict) or 'urls' not in response_data or 'response' not in response_data:
                raise ValueError("LLM response is not a valid JSON object with required keys")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM response as JSON. Raw response: {result}")
            logger.error(f"Extracted JSON content: {json_content}")
            raise ValueError(f"LLM returned invalid JSON: {e}")

        selected_urls = response_data['urls']
        llm_response = response_data['response']

        logger.info(f"LLM selected {len(selected_urls)} URLs")
        logger.info(f"LLM Response: {llm_response}")

        # Validate URLs
        valid_urls = []
        for item in selected_urls:
            if isinstance(item, dict) and 'url' in item and 'rationale' in item:
                url = item['url'].strip()
                if url.startswith("http"):
                    valid_urls.append({
                        'url': url,
                        'rationale': item['rationale']
                    })
                else:
                    logger.warning(f"Invalid URL format: {url}")
            else:
                logger.warning(f"Invalid item format: {item}")

        if not valid_urls:
            logger.warning("No valid URLs found in LLM response")

        # Log selected URLs with rationales
        for item in valid_urls:
            logger.info(f"Selected URL: {item['url']} | Rationale: {item['rationale']}")

        return valid_urls, llm_response
    except Exception as e:
        logger.error(f"Error with LLM: {e}")
        raise

def download_and_hash_pdf(url):
    """Download PDF and compute hash."""
    logger.info(f"Downloading PDF from {url}")
    try:
        # Use centralized headers but modify Accept for PDF downloads
        pdf_headers = HEADERS.copy()
        pdf_headers['Accept'] = 'application/pdf,*/*'
        response = requests.get(url, headers=pdf_headers, timeout=120)
        response.raise_for_status()
        if 'application/pdf' not in response.headers.get('content-type', ''):
            error_msg = "Downloaded content is not a PDF"
            logger.error(error_msg)
            return None, None, None, error_msg

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
        return pdf_hash, document_name, last_modified, None
    except requests.RequestException as e:
        error_msg = f"HTTP {getattr(e.response, 'status_code', 'Unknown')} - {str(e)}"
        logger.error(f"Error downloading PDF: {error_msg}")
        return None, None, None, error_msg

def update_database(utility_name, url, document_name, pdf_hash, last_modified, link_text):
    """Update or insert record in database."""
    logger.info("Updating database...")
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        now = datetime.now()

        # Determine tariff_last_updated value
        tariff_last_updated = last_modified if last_modified else now

        # Fuzzy match: check if record exists based on hash, url, or link_text
        cursor.execute("""
            SELECT id, hash FROM tariff_documents
            WHERE utility_name = ? AND (hash = ? OR url = ? OR link_text = ?)
        """, (utility_name, pdf_hash, url, link_text))
        existing = cursor.fetchone()

        if existing:
            # Update existing
            if existing[1] != pdf_hash:
                cursor.execute("""
                    UPDATE tariff_documents
                    SET hash = ?, last_checked = ?, tariff_last_updated = ?, url = ?, link_text = ?
                    WHERE id = ?
                """, (pdf_hash, now, tariff_last_updated, url, link_text, existing[0]))
                logger.info("Updated existing record with new hash")
                status = "UPDATED"
            else:
                cursor.execute("""
                    UPDATE tariff_documents
                    SET last_checked = ?
                    WHERE id = ?
                """, (now, existing[0]))
                logger.info("No changes detected, only updated last_checked")
                status = "NO CHANGE"
        else:
            # Mark existing as obsolete
            cursor.execute("""
                UPDATE tariff_documents
                SET status = 'OBSOLETE'
                WHERE utility_name = ? AND status = 'ACTIVE'
            """, (utility_name,))

            # Insert new
            cursor.execute("""
                INSERT INTO tariff_documents (utility_name, url, document_name, hash, last_checked, tariff_last_updated, status, link_text)
                VALUES (?, ?, ?, ?, ?, ?, 'ACTIVE', ?)
            """, (utility_name, url, document_name, pdf_hash, now, tariff_last_updated, link_text))
            logger.info("Inserted new record")
            status = "ADDED"

        conn.commit()
        return status, tariff_last_updated
    except sqlite3.Error as e:
        logger.error(f"Database error in update_database: {e}")
        raise
    finally:
        if conn:
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

def process_seed_url(seed_url, quick_mode=False):
    """Process a single seed URL through the full pipeline and return aggregated report data."""
    logger.info(f"{'='*60}")
    logger.info(f"PROCESSING SEED URL: {seed_url}")
    if quick_mode:
        logger.info("QUICK MODE ENABLED")
    logger.info(f"{'='*60}")

    utility_name = get_utility_name_from_url(seed_url)
    logger.info(f"Derived utility name: {utility_name}")

    links = scrape_links(seed_url)
    potential_urls_found = len(links)
    errors_encountered = 0

    if not links:
        logger.error(f"No links found for {seed_url}")
        return {
            'utility_name': utility_name,
            'seed_url': seed_url,
            'potential_urls_found': 0,
            'llm_selections': 0,
            'llm_selection_response': 'No PDF links found on the page',
            'records_added': 0,
            'records_updated': 0,
            'errors_encountered': 1,
            'selected_urls_details': []
        }

    # Only use LLM to select URLs when there are more than a single link
    if len(links) == 1:
        selected_urls = [{'url': links[0]['url'], 'rationale': 'Only one PDF link found on page'}]
        llm_response = "Only one PDF link found, no LLM selection needed"
    elif len(links) > 1:
        try:
            selected_urls, llm_response = select_best_url_with_llm(links)
        except Exception as e:
            logger.error(f"LLM selection failed for {seed_url}: {e}")
            selected_urls = []
            llm_response = f"LLM selection failed: {str(e)}"
            errors_encountered += 1

    llm_selections = len(selected_urls) if selected_urls else 0

    if not selected_urls:
        logger.warning(f"No URLs selected by LLM for {seed_url}")
        return {
            'utility_name': utility_name,
            'seed_url': seed_url,
            'potential_urls_found': potential_urls_found,
            'llm_selections': 0,
            'llm_selection_response': llm_response,
            'records_added': 0,
            'records_updated': 0,
            'errors_encountered': errors_encountered,
            'selected_urls_details': []
        }

    logger.info(f"Processing {len(selected_urls)} selected URLs for {seed_url}")

    selected_urls_details = []
    records_added = 0
    records_updated = 0

    # Process each selected URL
    for i, url_info in enumerate(selected_urls, 1):
        current_url = url_info['url']
        rationale = url_info['rationale']

        logger.info(f"{'-'*40}")
        logger.info(f"PROCESSING URL {i}/{len(selected_urls)}: {current_url}")
        logger.info(f"Rationale: {rationale}")
        logger.info(f"{'-'*40}")

        # Find the link text for the current URL
        link_text = None
        for link in links:
            if link['url'] == current_url:
                link_text = link['text']
                break
        if not link_text:
            logger.warning(f"Link text not found for selected URL: {current_url}")
            link_text = ""

        document_changed = False
        db_status = "N/A"
        last_modified = "N/A"
        error_detail = None

        # Quick mode logic for each URL
        skip_download = False
        if quick_mode:
            logger.info("Quick mode: Checking for existing document...")
            try:
                existing_last_modified = find_existing_document(utility_name, current_url, link_text)
                if existing_last_modified:
                    # Fetch current Last-Modified header
                    current_last_modified = get_pdf_last_modified(current_url)
                    if current_last_modified:
                        # Compare timestamps (considering them equal if they are on the same date)
                        if current_last_modified.date() == existing_last_modified.date():
                            logger.info("PDF has not changed (Last-Modified matches). Skipping download.")
                            # Update last_checked timestamp
                            conn = None
                            try:
                                conn = sqlite3.connect(DB_PATH)
                                cursor = conn.cursor()
                                cursor.execute("""
                                    UPDATE tariff_documents
                                    SET last_checked = ?
                                    WHERE utility_name = ? AND (url = ? OR link_text = ?) AND status = 'ACTIVE'
                                """, (datetime.now(), utility_name, current_url, link_text))
                                conn.commit()
                                logger.info(f"Completed processing URL {i} (quick mode - no changes)")
                                skip_download = True
                                document_changed = False
                                db_status = "NO CHANGE"
                                last_modified = existing_last_modified.strftime('%Y-%m-%d %H:%M:%S') if existing_last_modified else "N/A"
                            except sqlite3.Error as e:
                                logger.error(f"Database error updating last_checked for {current_url}: {e}")
                                errors_encountered += 1
                                error_detail = f"Database error: {str(e)}"
                                skip_download = True  # Skip download but mark as error
                                db_status = "DB ERROR"
                                last_modified = "N/A"
                            finally:
                                if conn:
                                    conn.close()
                        else:
                            logger.info("PDF has been modified. Proceeding with download.")
                            document_changed = True
                    else:
                        logger.warning("Could not fetch Last-Modified header. Proceeding with download.")
                        document_changed = True
                else:
                    logger.info("No existing document found. Proceeding with download.")
                    document_changed = True
            except Exception as e:
                logger.error(f"Error in quick mode processing for {current_url}: {e}")
                errors_encountered += 1
                error_detail = f"Quick mode error: {str(e)}"
                skip_download = True  # Skip download but mark as error
                db_status = "ERROR"
                last_modified = "N/A"

        if not skip_download:
            error_detail = None
            pdf_hash, document_name, last_modified_raw, error_detail = download_and_hash_pdf(current_url)
            if not pdf_hash:
                logger.error(f"Failed to download or hash PDF for URL {i}: {current_url}")
                errors_encountered += 1
                selected_urls_details.append({
                    'url': current_url,
                    'rationale': rationale,
                    'document_changed': False,
                    'db_status': 'DOWNLOAD FAILED',
                    'last_modified': 'N/A',
                    'error_detail': error_detail
                })
                continue

            try:
                db_status, last_modified_datetime = update_database(utility_name, current_url, document_name, pdf_hash, last_modified_raw, link_text)
                last_modified = last_modified_datetime.strftime('%Y-%m-%d %H:%M:%S') if last_modified_datetime else "N/A"
                document_changed = db_status == "UPDATED"

                if db_status == "ADDED":
                    records_added += 1
                elif db_status == "UPDATED":
                    records_updated += 1
            except Exception as e:
                logger.error(f"Database update failed for {current_url}: {e}")
                errors_encountered += 1
                db_status = "DB ERROR"
                last_modified = "N/A"
                error_detail = f"Database error: {str(e)}"

        selected_urls_details.append({
            'url': current_url,
            'rationale': rationale,
            'document_changed': document_changed,
            'db_status': db_status,
            'last_modified': last_modified,
            'error_detail': error_detail
        })

        logger.info(f"Completed processing URL {i}/{len(selected_urls)}: {current_url}")

    logger.info(f"Completed processing all {len(selected_urls)} URLs for {seed_url}")

    return {
        'utility_name': utility_name,
        'seed_url': seed_url,
        'potential_urls_found': potential_urls_found,
        'llm_selections': llm_selections,
        'llm_selection_response': llm_response,
        'records_added': records_added,
        'records_updated': records_updated,
        'errors_encountered': errors_encountered,
        'selected_urls_details': selected_urls_details
    }

def get_pdf_last_modified(url):
    """Fetch Last-Modified header from PDF URL using HEAD request."""
    logger.info(f"Fetching Last-Modified header from {url}")
    try:
        response = requests.head(url, headers=HEADERS, timeout=10)
        response.raise_for_status()

        if 'Last-Modified' in response.headers:
            try:
                last_modified = parsedate_to_datetime(response.headers['Last-Modified'])
                logger.info(f"Last-Modified header: {last_modified}")
                return last_modified
            except Exception as e:
                logger.warning(f"Failed to parse Last-Modified header: {e}")
                return None
        else:
            logger.warning("No Last-Modified header found")
            return None
    except requests.RequestException as e:
        logger.error(f"Error fetching Last-Modified header: {e}")
        return None

def find_existing_document(utility_name, url, link_text):
    """Find existing document in database using fuzzy match criteria."""
    logger.info("Checking for existing document in database...")
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Fuzzy match: check if record exists based on url or link_text
        cursor.execute("""
            SELECT id, tariff_last_updated FROM tariff_documents
            WHERE utility_name = ? AND (url = ? OR link_text = ?) AND status = 'ACTIVE'
        """, (utility_name, url, link_text))
        existing = cursor.fetchone()

        if existing:
            logger.info(f"Found existing document with tariff_last_updated: {existing[1]}")
            # Parse the datetime string from database back to datetime object
            if existing[1]:
                try:
                    return datetime.fromisoformat(existing[1])
                except (ValueError, TypeError) as e:
                    logger.warning(f"Failed to parse tariff_last_updated from database: {e}")
                    return None
            else:
                return None
        else:
            logger.info("No existing document found")
            return None
    except sqlite3.Error as e:
        logger.error(f"Database error in find_existing_document: {e}")
        return None
    finally:
        if conn:
            conn.close()

def generate_report(all_report_data, input_file_path):
    """Generate a Markdown report file with summary table and detailed sections."""
    if not all_report_data:
        logger.warning("No report data to generate")
        return

    # Create report filename
    input_filename = os.path.basename(input_file_path)
    report_filename = input_filename.replace('.txt', '_run_report.md')
    report_path = os.path.join(os.path.dirname(input_file_path), report_filename)

    logger.info(f"Generating report: {report_path}")

    with open(report_path, 'w') as f:
        f.write("# Utility Tariff Monitor Run Report\n\n")
        f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write(f"Input file: {input_filename}\n\n")

        # Part 1: Summary Table
        f.write("## Summary Table\n\n")
        f.write("| # | Utility Name | PDFs Found | LLM Selections | LLM Response | Records Added | Records Updated | Errors |\n")
        f.write("|---|--------------|------------|----------------|--------------|---------------|-----------------|--------|\n")

        for i, seed_data in enumerate(all_report_data, 1):
            utility_name = seed_data['utility_name']
            pdfs_found = seed_data['potential_urls_found']
            llm_selections = seed_data['llm_selections']
            llm_response = seed_data['llm_selection_response'][:50] + "..." if len(seed_data['llm_selection_response']) > 50 else seed_data['llm_selection_response']
            records_added = seed_data['records_added']
            records_updated = seed_data['records_updated']
            errors = seed_data['errors_encountered']

            f.write(f"| {i} | {utility_name} | {pdfs_found} | {llm_selections} | {llm_response} | {records_added} | {records_updated} | {errors} |\n")

        # Part 2: Detailed Information
        f.write("\n## Detailed Information\n\n")

        for i, seed_data in enumerate(all_report_data, 1):
            f.write(f"### Seed URL {i}: {seed_data['utility_name']} - {seed_data['seed_url']}\n\n")
            f.write(f"**Potential PDF URLs Found:** {seed_data['potential_urls_found']}\n\n")
            f.write(f"**LLM Selections:** {seed_data['llm_selections']}\n\n")
            f.write(f"**LLM Selection Response:** {seed_data['llm_selection_response']}\n\n")
            f.write(f"**Records Added:** {seed_data['records_added']}\n\n")
            f.write(f"**Records Updated:** {seed_data['records_updated']}\n\n")
            f.write(f"**Errors Encountered:** {seed_data['errors_encountered']}\n\n")

            if seed_data['selected_urls_details']:
                f.write("**Selected URLs Details:**\n\n")
                for j, url_detail in enumerate(seed_data['selected_urls_details'], 1):
                    f.write(f"#### URL {j}\n")
                    f.write(f"- **URL:** {url_detail['url']}\n")
                    f.write(f"- **LLM Rationale:** {url_detail['rationale']}\n")
                    f.write(f"- **PDF Has Changed:** {'Yes' if url_detail['document_changed'] else 'No'}\n")
                    f.write(f"- **Database Status:** {url_detail['db_status']}\n")
                    f.write(f"- **PDF Last Modified:** {url_detail['last_modified']}\n")
                    if url_detail.get('error_detail'):
                        f.write(f"- **Error Detail:** {url_detail['error_detail']}\n")
                    f.write("\n")
            else:
                f.write("**No URLs were selected for processing.**\n\n")

    logger.info(f"Report generated successfully: {report_path}")

def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description='Monitor utility tariff documents')
    parser.add_argument('--tariff-webpage-urls', required=True, help='Path to file containing tariff webpage URLs (one per line)')
    parser.add_argument('--initialize', action='store_true', help='Initialize the database')
    parser.add_argument('--quick', action='store_true', help='Quick mode: skip download if Last-Modified matches database')

    args = parser.parse_args()

    logger.info("Starting utility tariff monitor")

    if args.initialize:
        setup_database()

    seed_urls = read_seed_urls(args.tariff_webpage_urls)
    if not seed_urls:
        logger.error("No seed URLs found in input file")
        return

    logger.info(f"Processing {len(seed_urls)} seed URLs")

    all_report_data = []
    for seed_url in seed_urls:
        try:
            report_data = process_seed_url(seed_url, args.quick)
            if report_data:
                all_report_data.append(report_data)
        except Exception as e:
            logger.error(f"Error processing {seed_url}: {e}")
            continue

    logger.info("All seed URLs processed")

    # Generate the report
    generate_report(all_report_data, args.tariff_webpage_urls)

if __name__ == "__main__":
    main()

# Utility Tariff Monitor

A Python tool for monitoring updates to Electric Utility Tariff PDF documents. This script is part of a larger project to maintain a database with updated US Utility Tariff Rates. It serves as a proof of concept to detect tariff document changes.

## Overview

The Utility Tariff Monitor script automates the process of:

1. Scraping utility company websites for PDF links
2. Using AI (Google Gemini) to identify commercial tariff documents
3. Downloading and hashing PDFs to detect changes
4. Maintaining a database of tariff documents with change history
5. Generating detailed reports of the monitoring process

This tool is designed to be the first component in a larger system that will eventually include:
- Archiving of PDF documents
- Parsing PDFs for specific tariff rates
- A UI for viewing and querying the database

## Database Schema

The script uses an SQLite database (`resources/tariff_monitor.db`) with the following schema:

```sql
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
```

### Field Descriptions:

- **id**: Unique identifier for each record
- **utility_name**: Name of the utility company (derived from the domain)
- **url**: URL of the PDF document
- **document_name**: Filename of the PDF
- **hash**: SHA-256 hash of the PDF content (used to detect changes)
- **last_checked**: Timestamp when the document was last checked
- **tariff_last_updated**: Timestamp when the tariff was last updated (from PDF metadata)
- **status**: Status of the document (ACTIVE, OBSOLETE)
- **link_text**: Text of the link that pointed to the PDF

## How to Run

### Prerequisites

1. Python 3.6+
2. Dependencies listed in `environment.txt`
3. Google API key for Gemini (set in `.env` file)

### Setup

1. Clone the repository
2. Install dependencies:
   ```
   pip install -r environment.txt
   ```
3. Create a `.env` file in the project root with your Google API key:
   ```
   GOOGLE_API_KEY=your_api_key_here
   ```

### Running the Script

Basic usage:

```bash
python src/utility_tariff_monitor.py --tariff-webpage-urls resources/utility_rate_seed_urls.txt
```

Initialize the database (first run):

```bash
python src/utility_tariff_monitor.py --tariff-webpage-urls resources/utility_rate_seed_urls.txt --initialize
```

Quick mode (skip download if PDF hasn't changed):

```bash
python src/utility_tariff_monitor.py --tariff-webpage-urls resources/utility_rate_seed_urls.txt --quick
```

## Output Report

The script generates a detailed Markdown report in the same directory as the input file. For example, if the input file is `resources/utility_rate_seed_urls.txt`, the report will be `resources/utility_rate_seed_urls_run_report.md`.

The report includes:

1. **Summary Table**: A high-level overview of the results for each utility
   - Utility name
   - Number of PDFs found
   - Number of PDFs selected by the LLM
   - Records added/updated
   - Errors encountered

2. **Detailed Information**: For each utility, detailed information about:
   - The seed URL
   - Potential PDF URLs found
   - LLM selection response
   - Records added/updated
   - Errors encountered
   - Details for each selected URL (URL, rationale, change status, etc.)

## Example Run

Here's an example of running the script with a sample seed URL:

```bash
python src/utility_tariff_monitor.py --tariff-webpage-urls resources/utility_rate_seed_urls.txt --quick
```

This will:
1. Read the seed URLs from the file
2. For each URL, scrape the page for PDF links
3. Use the LLM to select commercial tariff documents
4. Download and hash the PDFs (or skip if unchanged in quick mode)
5. Update the database with any changes
6. Generate a report

### Sample Seed URL File

```
https://tnmp.com/customers/rates-0
https://www.prairielandelectric.com/rate-schedule-tariffs
https://www.cleco.com/residential-commercial/rates-billing-payment/rates-fees
https://fkec.com/access-your-account/billing-information-fees-2/
https://www.fpl.com/rates.html
```

### Sample Report Excerpt

```markdown
# Utility Tariff Monitor Run Report

Generated on: 2025-09-09 11:09:00

Input file: utility_rate_seed_urls.txt

## Summary Table

| # | Utility Name | PDFs Found | LLM Selections | LLM Response | Records Added | Records Updated | Errors |
|---|--------------|------------|----------------|--------------|---------------|-----------------|--------|
| 1 | [Tnmp Com](#tnmp-com) | 2 | 1 | Selected the retail tariff document as it is likel... | 0 | 0 | 0 |
| 2 | [Prairielandelectric Com](#prairielandelectric-com) | 22 | 3 | Selected General Service tariffs from Prairieland ... | 0 | 0 | 0 |

## Detailed Information

### <a id="tnmp-com"></a>Seed URL 1: Tnmp Com - https://tnmp.com/customers/rates-0

**Potential PDF URLs Found:** 2

**LLM Selections:** 1

**LLM Selection Response:** Selected the retail tariff document as it is likely to contain commercial rates and is effective in the current year. The wholesale tariff was excluded.

**Records Added:** 0

**Records Updated:** 0

**Errors Encountered:** 0

**Selected URLs Details:**

#### URL 1
- **URL:** https://tnmp.com/sites/default/files/inline-images/TNMP%20Retail%20Tariff%20%2820250901%29.pdf
- **LLM Rationale:** Contains retail delivery service tariff information, which can include commercial rates. The document is effective in the current year and avoids keywords like 'wholesale'.
- **PDF Has Changed:** No
- **Database Status:** NO CHANGE
- **PDF Last Modified:** 2025-07-03 17:53:33
```

## How It Works

The script follows these steps:

1. **Database Setup**: Creates an SQLite database if it doesn't exist
2. **Seed URL Processing**: Reads URLs from the input file
3. **Web Scraping**: For each seed URL:
   - Scrapes the page for PDF links
   - Extracts context for each link
4. **AI Selection**: Uses Google Gemini to select commercial tariff documents
   - Analyzes link text, context, and URL patterns
   - Selects documents based on keywords like "commercial", "general service", etc.
   - Avoids documents for residential, industrial, or other non-commercial categories
5. **Document Processing**: For each selected document:
   - Downloads the PDF
   - Computes a SHA-256 hash
   - Extracts metadata (Last-Modified)
   - Compares with existing records in the database
6. **Database Update**: Updates the database with new or changed documents
7. **Report Generation**: Creates a detailed Markdown report

The script includes a "quick mode" that uses HTTP headers to check if a document has been modified before downloading it, which can significantly speed up subsequent runs.

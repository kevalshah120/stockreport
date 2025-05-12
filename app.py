from flask import Flask, jsonify, request
from flask_cors import CORS
import requests
from bs4 import BeautifulSoup
import os
import json
from datetime import datetime
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import google.generativeai as genai

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Windows; Windows x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.114 Safari/537.36'
}

PDF_FOLDER = "QuarterlyResultPdf"

# Load Gemini API key from environment variable
GEMINI_API_KEY = "AIzaSyARFQyj9urbmslrRDu7xCXr92M07ZjZqZw"  # Set this as an environment variable in production
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-1.5-flash')


# Initialize PDF folder
try:
    if not os.path.exists(PDF_FOLDER):
        os.makedirs(PDF_FOLDER, exist_ok=True)
        logger.info("Created PDF folder: %s", PDF_FOLDER)
except Exception as e:
    logger.error("Failed to create PDF folder: %s", str(e))
    raise

class ScreenerScraper:
    def __init__(self, stock_name):
        self.stock_name = stock_name
        self.base_url = f"https://www.screener.in/company/{stock_name}/consolidated/"
        self.documents = {
            "concalls": {"transcripts": [], "ppt": []},
            "quarterly_results": {}
        }

    def fetch_page(self, url):
        logger.info("Fetching URL: %s", url)
        session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        session.mount('http://', HTTPAdapter(max_retries=retries))
        session.mount('https://', HTTPAdapter(max_retries=retries))
        
        try:
            time.sleep(1)  # Avoid rate-limiting
            response = session.get(url, headers=HEADERS, timeout=30)
            response.raise_for_status()
            logger.info("Successfully fetched URL: %s", url)
            return response.text
        except requests.exceptions.RequestException as e:
            logger.error("Error fetching %s: %s", url, str(e))
            return None

    def format_link(self, href):
        if href.startswith("http"):
            return href
        elif href.startswith("/"):
            return f"https://www.screener.in{href}"
        return None
    
    def extract_concalls(self, soup):
        for link in soup.find_all("a", href=True, class_="concall-link"):
            href = link["href"]
            text = link.get_text(strip=True).lower()
            formatted_link = self.format_link(href)
            if formatted_link and "amazonaws" not in href:
                if "transcript" in text:
                    self.documents["concalls"]["transcripts"].append(formatted_link)
                elif "ppt" in text:
                    self.documents["concalls"]["ppt"].append(formatted_link)

    def extract_quarterly_results(self, soup):
        quarters_section = soup.find('section', id='quarters')
        if quarters_section:
            results_table = quarters_section.find('table', class_='data-table')
            if results_table:
                headers = [th.get_text(strip=True) for th in results_table.find('thead').find_all('th')[1:]]
                for row in results_table.find_all('tr'):
                    first_cell = row.find('td')
                    if first_cell and 'Raw PDF' in first_cell.get_text(strip=True):
                        cells = row.find_all('td')[1:]
                        for date, cell in zip(headers, cells):
                            link = cell.find('a')
                            if link and link.get('href'):
                                formatted_link = self.format_link(link['href'])
                                if formatted_link:
                                    self.documents["quarterly_results"][date] = formatted_link
            else:
                logger.warning("No results table found in quarters section")
        else:
            logger.warning("No quarters section found")

    def scrape_documents(self):
        logger.info("Scraping documents for %s", self.stock_name)
        html_content = self.fetch_page(self.base_url)
        if not html_content:
            logger.error("Failed to fetch webpage for %s", self.stock_name)
            return {"error": "Failed to fetch webpage"}
        
        soup = BeautifulSoup(html_content, "html.parser")
        logger.info("Extracting concalls for %s", self.stock_name)
        self.extract_concalls(soup)
        logger.info("Extracting quarterly results for %s", self.stock_name)
        self.extract_quarterly_results(soup)
        logger.info("Saving links to file for %s", self.stock_name)
        self.save_links_to_file()
        logger.info("Downloading latest PDF for %s", self.stock_name)
        self.download_latest_pdf()
        logger.info("Scraped documents: %s", self.documents)
        return self.documents

    def save_links_to_file(self):
        filename = f"{self.stock_name}_documents.json"
        try:
            with open(filename, 'w', encoding='utf-8') as file:
                json.dump(self.documents, file, indent=4)
            logger.info("Saved results to %s", filename)
        except Exception as e:
            logger.error("Failed to save links to %s: %s", filename, str(e))

    def download_pdf(self, url, filename):
        pdf_path = os.path.join(PDF_FOLDER, filename)
        if os.path.exists(pdf_path):
            logger.info("Skipping already downloaded file: %s", filename)
            return
        
        session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        session.mount('http://', HTTPAdapter(max_retries=retries))
        session.mount('https://', HTTPAdapter(max_retries=retries))
        
        try:
            response = session.get(url, headers=HEADERS, stream=True, timeout=30)
            response.raise_for_status()
            with open(pdf_path, 'wb') as pdf_file:
                for chunk in response.iter_content(1024):
                    pdf_file.write(chunk)
            logger.info("Downloaded: %s", filename)
        except requests.exceptions.RequestException as e:
            logger.error("Failed to download %s: %s", filename, str(e))
            raise
        except IOError as e:
            logger.error("File system error for %s: %s", filename, str(e))
            raise

    def download_latest_pdf(self):
        if not self.documents["quarterly_results"]:
            logger.warning("No quarterly results found to download")
            return
        latest_date = list(self.documents["quarterly_results"].keys())[-1]
        url = self.documents["quarterly_results"][latest_date]
        filename = f"{self.stock_name}_{latest_date.replace(' ', '_')}.pdf"
        self.download_pdf(url, filename)

    def get_latest_quarterly_result(self):
        json_file = f"{self.stock_name}_documents.json"
        try:
            if not os.path.exists(json_file):
                logger.error("Documents JSON not found: %s", json_file)
                return None, None
            with open(json_file, 'r') as f:
                data = json.load(f)
            
            if not data["quarterly_results"]:
                logger.error("No quarterly results found in JSON")
                return None, None
            
            latest_date = list(data["quarterly_results"].keys())[-1]
            filename = f"{self.stock_name}_{latest_date.replace(' ', '_')}.pdf"
            pdf_path = os.path.join(PDF_FOLDER, filename)
            
            return pdf_path, latest_date
        except Exception as e:
            logger.error("Error getting latest quarterly result: %s", str(e))
            return None, None

    def analyze_quarterly_result(self):
        logger.info("Analyzing quarterly result for %s", self.stock_name)
        pdf_path, quarter_date = self.get_latest_quarterly_result()
        if not pdf_path or not os.path.exists(pdf_path):
            logger.warning("PDF not found at %s, attempting to re-download", pdf_path)
            self.download_latest_pdf()
            pdf_path, quarter_date = self.get_latest_quarterly_result()
            if not pdf_path or not os.path.exists(pdf_path):
                logger.error("Latest quarterly result PDF not found at %s", pdf_path)
                return {"error": "Latest quarterly result PDF not found"}

        try:
            prompt = """FIRST, analyze the entire document to determine if it represents a company's financial report. Check for:
            - Sections titled "Financial Results", "Income Statement", "Balance Sheet" or similar
            - Presence of financial tables with metrics like Revenue, Profit, Expenses
            - Common financial terms (EBITDA, EPS, Assets, Liabilities, etc.)
            - Numerical data with currency units

            If the document DOES NOT contain financial results or appears unrelated to company financials, respond ONLY with: "The uploaded PDF does not appear to be a financial report."

            ONLY IF financial content is confirmed, proceed with:
            Analyze the quarterly result PDF and extract the consolidated financial metrics from the section titled "STATEMENT OF CONSOLIDATED UNAUDITED FINANCIAL RESULTS" for Core Financial Performance
                Focus specifically on these financial metrics from the CONSOLIDATED table (not standalone):
                1. Core Financial Performance:
                   - Total Revenue/Income from Operations
                   - Total Income (including other income)
                   - Total Expenses / Total Expenditure / Total Cost
                   - Total Tax Expense
                   - Profit/ (Loss) Before Tax (PBT)
                   - Net Profit/ (Loss) or (Profit After Tax)
                   - Basic EPS
                   - Diluted EPS
                extract other disclosure related data or metrics from the section titled "Other Disclosures - Consolidated" 
                2. Other Disclosures
                   - Operating Profit Margin (%)
                   - Net Profit Margin (%)
                   - Debt equity ratio (in times)
                   - Total debts to total assets ratio (in %)
                   - Net Worth
                extract balance related data or metrics from the section titled "Consolidated Statement of Assets and Liabilities"   
                3. Balance Sheet Highlights:
                   - Total Assets
                   - Total Liabilities
                   - Total Equity
               
                Important Instructions:
                1. ONLY extract data from the CONSOLIDATED financial results table
                2. Look for the section titled "STATEMENT OF CONSOLIDATED UNAUDITED FINANCIAL RESULTS" or similar
                3. Focus on current quarter figures ONLY (not year-to-date/annual)
                4. Include units (e.g., INR in Crores)
                5. Set null for unavailable metrics
                
                Return in this JSON format:
                {
                    "quarter": "Q# FY##",
                    "date": "YYYY-MM-DD",
                    "metrics": {
                        "core_financials": {
                            "revenue": {"value": number, "unit": "string"},
                            "total_income": {"value": number, "unit": "string"},
                            "total_expenses": {"value": number, "unit": "string"},
                            "total_tax_expense": {"value": number, "unit": "string"},
                            "profit_before_tax": {"value": number, "unit": "string"},
                            "net_profit": {"value": number, "unit": "string"},
                            "basic_eps": {"value": number, "unit": "INR"},
                            "diluted_eps": {"value": number, "unit": "INR"}
                        },
                        "other_disclosures": {
                            "operating_margin": {"value": number, "unit": "%"},
                            "net_margin": {"value": number, "unit": "%"},
                            "total_debt_to_asset_ratio": {"value": number, "unit": "%"},
                            "debt_equity_ratio": {"value": number, "unit": "times"},
                            "net_worth": {"value": number, "unit": "string"}
                        },
                        "balance_sheet": {
                            "total_assets": {"value": number, "unit": "string"},
                            "total_liabilities": {"value": number, "unit": "string"},
                            "total_equity": {"value": number, "unit": "string"}
                        }
                    }
                }"""

            logger.info("Reading PDF file: %s", pdf_path)
            with open(pdf_path, 'rb') as pdf_file:
                pdf_content = pdf_file.read()
            
            logger.info("Calling Gemini API for PDF analysis")
            generation_config = {
                "temperature": 0.1,
                "top_p": 0.1,
                "top_k": 16,
                "max_output_tokens": 2048,
            }
            response = model.generate_content(
                contents=[prompt, {"mime_type": "application/pdf", "data": pdf_content}],
                generation_config=generation_config,
                stream=False
            )
            logger.info("Gemini API response received")
            
            response_text = response.text
            logger.info("Gemini API response text: %s", response_text[:1000])
            
            if "does not appear to be a financial report" in response_text:
                logger.error("PDF does not contain financial data")
                return {"error": "The uploaded PDF does not contain financial data"}

            try:
                json_start = response_text.find('{')
                json_end = response_text.rfind('}') + 1
                if json_start == -1 or json_end == -1:
                    logger.error("No valid JSON found in response: %s", response_text)
                    return {"error": "No valid JSON found in Gemini API response"}
                
                json_str = response_text[json_start:json_end]
                json_str = json_str.replace('```json', '').replace('```', '').strip()
                result = json.loads(json_str)
                logger.info("Parsed JSON result: %s", result)
            except json.JSONDecodeError as e:
                logger.error("JSON parsing error: %s, response: %s", str(e), response_text)
                return {"error": f"JSON parsing error: {str(e)}"}

            result["stock_name"] = self.stock_name
            result["pdf_source"] = pdf_path
            result["extraction_date"] = datetime.now().isoformat()

            output_file = f"{self.stock_name}_quarterly_data.json"
            logger.info("Saving result to %s", output_file)
            try:
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(result, f, indent=4)
                logger.info("Successfully saved quarterly data to %s", output_file)
            except Exception as e:
                logger.error("Failed to save quarterly data to %s: %s", output_file, str(e))

            return result

        except Exception as e:
            logger.exception("Error analyzing quarterly result: %s", str(e))
            return {"error": f"Error analyzing quarterly result: {str(e)}"}

@app.route('/api/analyze-stock', methods=['POST'])
def analyze_stock():
    try:
        data = request.get_json()
        logger.info("Received request with data: %s", data)
        stock_name = data.get('stockName')
        
        if not stock_name:
            logger.error("Stock name is missing")
            return jsonify({'error': 'Stock name is required'}), 400

        logger.info("Initializing scraper for stock: %s", stock_name)
        scraper = ScreenerScraper(stock_name)
        
        logger.info("Scraping documents for %s", stock_name)
        documents = scraper.scrape_documents()
        logger.info("Scraped documents: %s", documents)
        
        logger.info("Analyzing quarterly result for %s", stock_name)
        result = scraper.analyze_quarterly_result()
        
        if result:
            logger.info("Analysis successful for %s", stock_name)
            return jsonify(result)
        else:
            logger.error("Analysis failed for %s", stock_name)
            return jsonify({'error': 'Analysis failed'}), 500

    except Exception as e:
        logger.exception("Unexpected error in analyze_stock: %s", str(e))
        return jsonify({'error': str(e)}), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    logger.info("Health check requested")
    return jsonify({'status': 'healthy'}), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    logger.info("Starting Flask app on port %d", port)
    app.run(host='0.0.0.0', port=port)

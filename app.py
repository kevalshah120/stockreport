from flask import Flask, jsonify, request
from flask_cors import CORS
import requests
from bs4 import BeautifulSoup
import os
import json
from concurrent.futures import ThreadPoolExecutor
import google.generativeai as genai
from datetime import datetime

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Windows; Windows x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.114 Safari/537.36'
}

PDF_FOLDER = "QuarterlyResultPdf"
GEMINI_API_KEY = "AIzaSyC9KkbgmUDIB8BbiaKDmjrxTVI1omRh-TQ"  # Set this as an environment variable in production
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-1.5-flash')

# Initialize PDF folder
if not os.path.exists(PDF_FOLDER):
    os.makedirs(PDF_FOLDER, exist_ok=True)

class ScreenerScraper:
    def __init__(self, stock_name):
        self.stock_name = stock_name
        self.base_url = f"https://www.screener.in/company/{stock_name}/consolidated/"
        self.documents = {
            "concalls": {"transcripts": [], "ppt": []},
            "quarterly_results": {}
        }
        if not os.path.exists(PDF_FOLDER):
            os.makedirs(PDF_FOLDER, exist_ok=True)

    def fetch_page(self, url):
        try:
            response = requests.get(url, headers=HEADERS)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")
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

    def scrape_documents(self):
        html_content = self.fetch_page(self.base_url)
        if not html_content:
            return {"error": "Failed to fetch the webpage"}
        
        soup = BeautifulSoup(html_content, "html.parser")
        self.extract_concalls(soup)
        self.extract_quarterly_results(soup)
        self.save_links_to_file()
        self.download_latest_pdf()
        return self.documents

    def save_links_to_file(self):
        filename = f"{self.stock_name}_documents.json"
        with open(filename, 'w', encoding='utf-8') as file:
            json.dump(self.documents, file, indent=4)
        print(f"Saved results to {filename}")

    def download_pdf(self, url, filename):
        pdf_path = os.path.join(PDF_FOLDER, filename)
        # Check if file already exists
        if os.path.exists(pdf_path):
            print(f"Skipping already downloaded file: {filename}")
            return
            
        try:
            response = requests.get(url, headers=HEADERS, stream=True)
            response.raise_for_status()
            with open(pdf_path, 'wb') as pdf_file:
                for chunk in response.iter_content(1024):
                    pdf_file.write(chunk)
            print(f"Downloaded: {filename}")
        except requests.exceptions.RequestException as e:
            print(f"Failed to download {filename}: {e}")

    def download_latest_pdf(self):
        latest_date = list(self.documents["quarterly_results"].keys())[-1]
        url = self.documents["quarterly_results"][latest_date]
        filename = f"{self.stock_name}_{latest_date.replace(' ', '_')}.pdf"
        self.download_pdf(url, filename)

    def get_latest_quarterly_result(self):
        """Get the path to the latest quarterly result PDF."""
        json_file = f"{self.stock_name}_documents.json"
        try:
            with open(json_file, 'r') as f:
                data = json.load(f)
                
            # Get the last entry from quarterly_results
            latest_date = list(data["quarterly_results"].keys())[-1]
            filename = f"{self.stock_name}_{latest_date.replace(' ', '_')}.pdf"
            pdf_path = os.path.join(PDF_FOLDER, filename)
            
            return pdf_path, latest_date
        except Exception as e:
            print(f"Error getting latest quarterly result: {e}")
            return None, None

    def analyze_quarterly_result(self):
        """Analyze the latest quarterly result focusing on consolidated financial data."""
        pdf_path, quarter_date = self.get_latest_quarterly_result()
        if not pdf_path or not os.path.exists(pdf_path):
            print("Latest quarterly result PDF not found")
            return

        try:
            prompt = """FIRST, analyze the entire document to determine if it represents a company's financial report. Check for:
        - Sections titled "Financial Results", "Income Statement", "Balance Sheet" or similar
        - Presence of financial tables with metrics like Revenue, Profit, Expenses
        - Common financial terms (EBITDA, EPS, Assets, Liabilities, etc.)
        - Numerical data with currency units

        If the document DOES NOT contain financial results or appears unrelated to company financials, respond ONLY with: "The uploaded PDF does not appear to be a financial report."

        ONLY IF financial content is confirmed, proceed with:
        Analyze the quarterly result PDF and extract the consolidated financial metrics from the section titled "STATEMENT OF CONSOLIDATED UNAUDITED FINANCIAL RESULTS" for  Core Financial Performance
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
            extract other diclourse releated data or metrics from the section titled "Other Disclosures - Consolidated" 
            2. Other Disclosures
               - Operating Profit Margin (%)
               - Net Profit Margin (%)
               - Debt equity ratio (in times)
               - Total debts to total assets ratio (in %)
               - Net Worth
            extract balance releated data or metrics from the section titled "Consolidated Statement of Assets and Liabilities"   
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
                        "net_profit": {"value": number, "unit": "string"}
                        "basic_eps": {"value": number, "unit": "INR"},
                        "diluted_eps": {"value": number, "unit": "INR"},
                    },
                    "other_discloures": {
                        "operating_margin": {"value": number, "unit": "%"},
                        "net_margin": {"value": number, "unit": "%"}
                        "total_debt_to_asset_ratio": {"value": number, "unit": "%"},
                        "debt_equity_ratio": {"value": number, "unit": "times"},
                    },
                    "balance_sheet": {
                        "total_assets": {"value": number, "unit": "string"},
                        "total_liabilities": {"value": number, "unit": "string"},
                        "net_worth": {"value": number, "unit": "string"},
                        "debt": {"value": number, "unit": "string"}
                    }
                }
            }"""

            # Read the PDF file and generate content
            with open(pdf_path, 'rb') as pdf_file:
                pdf_content = pdf_file.read()
                
            # Create generation config for structured output
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
            
            # Extract JSON from response
            response_text = response.text
            json_start = response_text.find('{')
            json_end = response_text.rfind('}') + 1
            
            if json_start != -1 and json_end != -1:
                json_str = response_text[json_start:json_end]
                json_str = json_str.replace('```json', '').replace('```', '').strip()
                result = json.loads(json_str)
            else:
                raise ValueError("No valid JSON found in response")

            # Add metadata
            result["stock_name"] = self.stock_name
            result["pdf_source"] = pdf_path
            result["extraction_date"] = datetime.now().isoformat()

            # Save to file
            output_file = f"{self.stock_name}_quarterly_data.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=4)
            
            print(f"Successfully extracted and saved quarterly data to {output_file}")
            return result

        except Exception as e:
            print(f"Error analyzing quarterly result: {e}")
            print(f"Full error details: {str(e)}")
            if 'response_text' in locals():
                print(f"Raw response: {response_text}")
            return None

@app.route('/api/analyze-stock', methods=['POST'])
def analyze_stock():
    try:
        data = request.get_json()
        stock_name = data.get('stockName')
        
        if not stock_name:
            return jsonify({'error': 'Stock name is required'}), 400

        scraper = ScreenerScraper(stock_name)
        # First scrape and download the latest document
        documents = scraper.scrape_documents()
        
        # Then analyze the latest quarterly result
        result = scraper.analyze_quarterly_result()
        
        if result:
            return jsonify(result)
        else:
            return jsonify({'error': 'Analysis failed'}), 500

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy'}), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)

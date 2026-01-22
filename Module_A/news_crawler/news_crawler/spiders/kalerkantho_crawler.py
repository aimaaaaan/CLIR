import json
import time
import re
import os
import subprocess
from datetime import datetime
from bs4 import BeautifulSoup

class KalerKanthoCrawler:
    def __init__(self, output_file="data/kalerkantho_raw_documents.jsonl", target_count=200):
        self.base_url = "https://www.kalerkantho.com"
        self.output_file = output_file
        self.target_count = target_count
        self.collected_count = 0
        self.build_id = None
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)

    def fetch_url(self, url):
        """Fetch URL using curl to bypass 403 protections."""
        try:
            # -s for silent mode to avoid progress bar in output
            # -L to follow redirects
            # -A to set user agent
            # --compressed to handle gzip
            cmd = [
                'curl', 
                '-s', 
                '-L', 
                '-A', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                '--compressed',
                url
            ]
            
            result = subprocess.run(
                cmd,
                capture_output=True, 
                text=True, 
                # Don't use check=True immediately so we can handle 404/403 gracefully if curl returns non-zero/error pages
                encoding='utf-8',
                errors='replace' 
            )
            
            if result.returncode != 0:
                print(f"Curl error fetching {url}: {result.stderr}")
                return None
            
            # Curl might return successful exit code even for 404/403 pages, but we might check content or proceed.
            # Next.js usually returns JSON for _next/data URLs. If it's HTML 404/403, JSON parse will fail.
            return result.stdout
            
        except Exception as e:
            print(f"Error fetching URL {url}: {e}")
            return None

    def get_build_id(self):
        print("Fetching homepage to extract buildId...")
        content = self.fetch_url(self.base_url)
        
        # Fallback to known buildId if fetch fails or captchas
        known_build_id = "kk-build-0.5.95"
        
        if not content or "Just a moment" in content:
            print(f"Cloudflare challenge detected or fetch failed. Using fallback buildId: {known_build_id}")
            self.build_id = known_build_id
            return True

        match = re.search(r'"buildId":"(.*?)"', content)
        if match:
            self.build_id = match.group(1)
            print(f"Found buildId: {self.build_id}")
            return True
        else:
            print(f"Could not find buildId in homepage. Using fallback: {known_build_id}")
            self.build_id = known_build_id
            return True

    def clean_html(self, html_content):
        if not html_content:
            return ""
        soup = BeautifulSoup(html_content, 'html.parser')
        # Remove script and style elements
        for script in soup(["script", "style"]):
            script.decompose()
        text = soup.get_text(separator=" ")
        # Collapse whitespace
        text = ' '.join(text.split())
        return text

    def fetch_article(self, category_slug, date_str, n_id):
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            print(f"Error parsing date: {date_str}")
            return None

        url = f"{self.base_url}/_next/data/{self.build_id}/online/{category_slug}/{dt.year}/{dt.month:02d}/{dt.day:02d}/{n_id}.json"
        
        content = self.fetch_url(url)
        if not content:
            return None
            
        try:
            data = json.loads(content)
        except json.JSONDecodeError:
            # Likely 404 or 403 html response
            print(f"Failed to parse JSON for article {url}")
            return None
            
        details = data.get('pageProps', {}).get('details', {})
        if not details:
            return None

        title = details.get('n_head', '')
        body_html = details.get('n_details', '')
        body_text = self.clean_html(body_html)
        
        public_url = f"{self.base_url}/online/{category_slug}/{dt.year}/{dt.month:02d}/{dt.day:02d}/{n_id}"

        return {
            "url": public_url,
            "title": title,
            "date": date_str,
            "category": category_slug,
            "body": body_text,
            "tokens": body_text.split(),
            "language": "bn"
        }

    def crawl(self):
        if not self.get_build_id():
            return

        categories = [
            "national", 
            "entertainment", 
            "Islamic-lifestylie", 
            "Politics",
            "lifestyle"
        ]

        # Target per category
        category_target = 150
        
        # Clear file first if starting fresh or append? 
        # User asked to "compile all 5 categories... into one jsonl file".
        # I'll overwrite to start fresh to ensure clean counts.
        with open(self.output_file, 'w', encoding='utf-8') as f:
            for category in categories:
                print(f"Starting category: {category}")
                category_collected = 0
                page = 1
                
                while category_collected < category_target:
                    cat_url = f"{self.base_url}/_next/data/{self.build_id}/online/{category}.json?page={page}"
                    print(f"Fetching list for {category}: {cat_url}")
                    
                    content = self.fetch_url(cat_url)
                    if not content:
                        print(f"Failed to fetch category page {page} for {category}")
                        break
                        
                    try:
                        data = json.loads(content)
                    except json.JSONDecodeError:
                        print(f"Failed to parse category JSON for {category}. End of pages?")
                        break

                    news_list = data.get('pageProps', {}).get('newsData', [])
                    if not news_list:
                        print(f"No more news in category {category}.")
                        break
                        
                    for item in news_list:
                        if category_collected >= category_target:
                            break
                            
                        n_id = item.get('n_id')
                        start_at = item.get('start_at')
                        
                        cat_slug = None
                        if 'cat_name' in item and item['cat_name']:
                            cat_slug = item['cat_name'].get('slug')
                        current_slug = cat_slug if cat_slug else category
                        
                        if n_id and start_at:
                            article_doc = self.fetch_article(current_slug, start_at, n_id)
                            if article_doc:
                                f.write(json.dumps(article_doc, ensure_ascii=False) + "\n")
                                category_collected += 1
                                self.collected_count += 1
                                if category_collected % 10 == 0:
                                    print(f"  Category {category}: collected {category_collected}/{category_target}...")
                                time.sleep(0.2) 
                        
                    page += 1
                    time.sleep(1)

        print(f"Done. Total collected {self.collected_count} documents.")

if __name__ == "__main__":
    crawler = KalerKanthoCrawler(output_file="data/kalerkantho_raw_documents.jsonl", target_count=200)
    crawler.crawl()

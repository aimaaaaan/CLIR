import json
import time
import re
import os
import subprocess
from datetime import datetime
from bs4 import BeautifulSoup
import urllib.parse

class DailySunCrawler:
    def __init__(self, output_file="data/daily_sun_raw_documents.jsonl", target_count=150):
        self.base_url = "https://www.daily-sun.com"
        self.output_file = output_file
        self.target_count = target_count
        self.collected_count = 0
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        
        self.categories = {
            "law-&-Order": 276,
            "sports": 26, # Need to verify, but usually sequential or distinct. Will fetch to confirm if needed.
            "diplomacy": 270, # Placeholder, will need to be dynamic or verified
            "business": 23,
            "sci-tech": 33,
            "arts-culture": 35
        }
        # Reset categories to just slugs, we will resolve IDs dynamically if possible or assume/hardcode if found
        self.category_urls = [
            "https://www.daily-sun.com/law-&-Order",
            "https://www.daily-sun.com/sports",
            "https://www.daily-sun.com/diplomacy",
            "https://www.daily-sun.com/business",
            "https://www.daily-sun.com/sci-tech",
            "https://www.daily-sun.com/arts-culture"
        ]

    def fetch_url(self, url):
        """Fetch URL using curl to bypass 403 protections."""
        try:
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
                encoding='utf-8',
                errors='replace' 
            )
            
            if result.returncode != 0:
                print(f"Curl error fetching {url}: {result.stderr}")
                return None
            
            return result.stdout
            
        except Exception as e:
            print(f"Error fetching URL {url}: {e}")
            return None

    def get_category_metadata(self, category_url):
        content = self.fetch_url(category_url)
        if not content:
            return None, None

        # Look for the AJAX load more pattern: 
        # https://www.daily-sun.com/ajax/load/categorynews/276/20/'+paginate+'/20'+'?lastID=853260
        # Regex to capture ID and lastID
        # Pattern: categorynews/(\d+)/
        cat_id_match = re.search(r'categorynews/(\d+)/', content)
        last_id_match = re.search(r'lastID=(\d+)', content)
        
        cat_id = cat_id_match.group(1) if cat_id_match else None
        last_id = last_id_match.group(1) if last_id_match else None
        
        return cat_id, last_id

    def extract_id_from_url(self, url):
        # Url format: https://www.daily-sun.com/law-&-Order/853260/dhaka-college-students-clash-with-cops
        match = re.search(r'/(\d+)/', url)
        return match.group(1) if match else None

    def fetch_article(self, url, category_slug):
        content = self.fetch_url(url)
        if not content:
            return None
            
        soup = BeautifulSoup(content, 'html.parser')
        
        # Title
        title_tag = soup.select_one('h1.detailHeadline') or soup.find('h1')
        title = title_tag.get_text(strip=True) if title_tag else ""
        
        # Date
        # Format: "Published: 22 Jan 2026, 01:32 PM"
        date_str = ""
        date_tag = soup.select_one('.publishedTime')
        if date_tag:
            date_text = date_tag.get_text(strip=True).replace("Published:", "").strip()
            # Try to parse date
            try:
                dt = datetime.strptime(date_text, "%d %b %Y, %I:%M %p")
                date_str = dt.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                date_str = date_text
                
        # Body
        body_div = soup.select_one('.desktopDetailBody') or soup.select_one('.detailBody')
        body_text = ""
        if body_div:
            # Remove scripts
            for script in body_div(["script", "style"]):
                script.decompose()
            body_text = body_div.get_text(separator=" ").strip()
            body_text = ' '.join(body_text.split())

        return {
            "url": url,
            "title": title,
            "date": date_str,
            "category": category_slug,
            "body": body_text,
            "tokens": body_text.split(),
            "language": "en" 
        }

    def crawl(self):
        # Overwrite file to start fresh
        with open(self.output_file, 'w', encoding='utf-8') as f:
            for cat_url in self.category_urls:
                category_slug = cat_url.split('/')[-1]
                print(f"Starting category: {category_slug}")
                
                cat_id, last_id = self.get_category_metadata(cat_url)
                if not cat_id:
                    print(f"Could not find category ID for {category_slug}, skipping.")
                    continue
                
                print(f"  Category ID: {cat_id}, Initial Last ID: {last_id}")
                
                # Fetch initial page articles (from the HTML itself, or just use the AJAX API immediately?)
                # The HTML contains the first 20 or so articles.
                # To be simple and robust, let's parse the HTML list first, then go to AJAX.
                
                # Logic: Parsed HTML -> List of links.
                # Then Page 1 AJAX, Page 2...
                # Note: The AJAX URL uses `paginate` param. 
                # https://www.daily-sun.com/ajax/load/categorynews/{cat_id}/20/{page}/20?lastID={last_id}
                
                category_collected = 0
                page = 1
                
                # Fetch first batch from HTML (optional, but good practice). 
                # Actually, the AJAX API behaves like "load more". 
                # If we just hit the AJAX API page=1, it might give us the *next* batch after the HTML ones?
                # Let's extract links from the main category page first.
                
                content = self.fetch_url(cat_url)
                soup = BeautifulSoup(content, 'html.parser')
                # Links in category page. 
                # Selector seems to be `.linkOverlay` or just `a` tags in news items.
                # Valid articles usually have `/category_slug/id/slug`
                
                seen_urls = set()
                
                # Parse main page links
                for a in soup.find_all('a', href=True):
                    href = a['href']
                    if f"/{category_slug}/" in href and re.search(r'/\d+/', href):
                        full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                        if full_url not in seen_urls:
                            if category_collected >= self.target_count:
                                break
                            
                            # Fetch Article
                            print(f"    Fetching: {full_url}")
                            article = self.fetch_article(full_url, category_slug)
                            if article and article['body']:
                                f.write(json.dumps(article, ensure_ascii=False) + "\n")
                                f.flush()
                                category_collected += 1
                                self.collected_count += 1
                                seen_urls.add(full_url)
                                last_id = self.extract_id_from_url(full_url) # Update last_id to the most recent one we found? 
                                # Actually last_id in the URL likely refers to the "oldest" article on the page to paginate *after* it.
                                # But let's trust the one we found in the script for the AJAX call initially.
                                time.sleep(0.5)
                
                print(f"  Collected {category_collected} from initial page.")
                
                # AJAX Pagination
                # Note: The AJAX script uses `lastID` from the *javascript* variable which usually matches the last item on the rendered HTML.
                # We should probably use that `last_id` extracted from `get_category_metadata` for the first AJAX call.
                
                while category_collected < self.target_count:
                    # Construct AJAX URL
                    # paginate starts at 1 usually for "Load More"
                    ajax_url = f"{self.base_url}/ajax/load/categorynews/{cat_id}/20/{page}/20?lastID={last_id}"
                    print(f"  Fetching AJAX Page {page}: {ajax_url}")
                    
                    try:
                        resp = self.fetch_url(ajax_url)
                        if not resp:
                            print("  Empty response from AJAX.")
                            break
                            
                        # Response is JSON
                        try:
                            data = json.loads(resp)
                        except json.JSONDecodeError:
                            print("  Failed to parse JSON response.")
                            break
                            
                        if not data:
                            print("  No more data.")
                            break
                            
                        # data is a list of objects
                        # Each obj has 'url' field usually relative or absolute
                        
                        has_new = False
                        for item in data:
                            if category_collected >= self.target_count:
                                break
                                
                            item_url = item.get('url')
                            if not item_url:
                                continue
                                
                            full_url = item_url if item_url.startswith('http') else f"{self.base_url}{item_url}"
                            
                            if full_url in seen_urls:
                                continue
                                
                            has_new = True
                            print(f"    Fetching (AJAX): {full_url}")
                            article = self.fetch_article(full_url, category_slug)
                            if article and article['body']:
                                f.write(json.dumps(article, ensure_ascii=False) + "\n")
                                f.flush()
                                category_collected += 1
                                self.collected_count += 1
                                seen_urls.add(full_url)
                                
                                # Update last_id from the latest item? 
                                # The AJAX loop usually increments page, but sometimes relies on lastID shifting.
                                # In the site script: `var url = ... +paginate+'/20'+'?lastID=853260';`
                                # It seems lastID might differ per request if it's truly cursor based? 
                                # But the example script increments `paginate` (1, 2, 3...) keeping lastID constant?
                                # Let's keep lastID constant as per the script analysis (it was hardcoded in the script snippet viewed).
                                
                                time.sleep(0.5)
                        
                        if not has_new:
                            print("  No new articles found in this batch.")
                            break
                            
                    except Exception as e:
                        print(f"  Error in AJAX loop: {e}")
                        break
                        
                    page += 1
                    time.sleep(1)

        print(f"Done. Total collected {self.collected_count} documents.")

if __name__ == "__main__":
    crawler = DailySunCrawler()
    crawler.crawl()

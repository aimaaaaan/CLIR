import aiohttp
import asyncio
from bs4 import BeautifulSoup
import time
import json
import os
import random
import re
import nest_asyncio
from tqdm.asyncio import tqdm

# Allow nested event loops (useful for notebooks or complex environments)
nest_asyncio.apply()

class NewAgeBDCrawler:
    def __init__(self, output_file="data/newage_raw_documents.jsonl", target_count=200, target_categories=None):
        # We target specific categories found in the URL structure
        if target_categories:
            self.target_categories = target_categories
        else:
            self.target_categories = ["banking", "politics", "asia", "cricket"]
        
        self.target_count = target_count
        self.output_file = output_file
        
        self.sitemaps = [
            "https://www.newagebd.net/category-sitemap.xml",
            "https://www.newagebd.net/page-sitemap.xml",
            "https://www.newagebd.net/tag-sitemap.xml"
        ]
        # Dynamically add news sitemaps
        for i in range(1, 21):
            self.sitemaps.append(f"https://www.newagebd.net/news-sitemap/{i}.xml")
        
        self.setup_output()
        self.collected_counts = {cat: 0 for cat in self.target_categories}
        self.visited_urls = set()
        self.lock = asyncio.Lock() # For writing to file/updating counts safely

    def setup_output(self):
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        print(f"[SETUP] Output file set to: {self.output_file}")

    async def fetch_url(self, session, url):
        retries = 3
        backoff = 1
        for i in range(retries):
            try:
                async with session.get(url, timeout=30) as response:
                    # Handle 429 specifically
                    if response.status == 429:
                        wait = int(response.headers.get("Retry-After", 60))
                        print(f"[WARN] 429 Too Many Requests for {url}. Waiting {wait}s...")
                        await asyncio.sleep(wait)
                        continue
                        
                    response.raise_for_status()
                    return await response.text()
            except Exception as e:
                if i == retries - 1:
                    print(f"[ERROR] Failed to fetch {url} after {retries} attempts: {e}")
                    return None
                await asyncio.sleep(backoff * (2 ** i) + random.uniform(0, 1))

    async def parse_sitemap(self, session, sitemap_url):
        print(f"[SITEMAP] Fetching {sitemap_url}...")
        xml_content = await self.fetch_url(session, sitemap_url)
        if not xml_content:
            return {}

        soup = BeautifulSoup(xml_content, 'xml')
        urls = soup.find_all('loc')
        
        # Organize URLs by category
        category_urls = {cat: [] for cat in self.target_categories}
        
        print(f"[SITEMAP] Parsing {len(urls)} URLs from sitemap...")
        
        for url_tag in urls:
            url = url_tag.get_text(strip=True)
            # Check if URL belongs to one of our target categories
            for cat in self.target_categories:
                if f"/post/{cat}/" in url:
                    category_urls[cat].append(url)
                    break
        
        for cat, links in category_urls.items():
            print(f"  > Found {len(links)} links for category '{cat}'")
            
        return category_urls

    def parse_article(self, url, html, category):
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Extract Title
            title_tag = soup.find('h1')
            title = title_tag.get_text(strip=True) if title_tag else "No Title"
            
            if "Most Popular Outspoken English Daily" in title:
                # Redirect or home page
                return None
            
            # Extract Date
            date_str = "Unknown"
            # Try specific class for article date first
            time_tag = soup.find('time', class_='ms-0 ms-sm-2 ms-md-3')
            if not time_tag and soup.find('div', class_='post-atribute'):
                 time_tag = soup.find('div', class_='post-atribute').find('time')
            
            if time_tag and time_tag.has_attr('datetime'):
                date_str = time_tag['datetime']
            else:
                meta_date = soup.find('meta', {'property': 'article:published_time'})
                if meta_date:
                    date_str = meta_date.get('content')
            
            # Extract Body
            content_div = soup.find('div', class_='post-content') or \
                          soup.find('div', id='content') or \
                          soup.find('main', id='content') or \
                          soup.find('article')
                          
            if content_div:
                paragraphs = content_div.find_all('p')
                body = "\n".join([p.get_text(strip=True) for p in paragraphs])
            else:
                paragraphs = soup.find_all('p')
                body = "\n".join([p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 50])

            if not body or len(body) < 100:
                # print(f"[DEBUG] Skipped {url} - Body too short ({len(body) if body else 0})")
                return None

            # Calculate Tokens
            tokens_count = len(re.findall(r'\w+', body))

            return {
                "url": url,
                "title": title,
                "date": date_str,
                "category": category,
                "body": body,
                "data": body,
                "html": html,
                "tokens": tokens_count,
                "language": "en"
            }

        except Exception as e:
            print(f"[ERROR] Parsing article {url}: {e}")
            return None

    async def process_article(self, session, url, category):
        # Early exit if target met
        if self.collected_counts[category] >= self.target_count:
            return

        async with self.lock:
            if url in self.visited_urls:
                return
            self.visited_urls.add(url)

        html = await self.fetch_url(session, url)
        if html:
            # CPU-bound parsing
            doc = self.parse_article(url, html, category)
            if doc:
                async with self.lock:
                    if self.collected_counts[category] < self.target_count:
                        self.save_document(doc)
                        self.collected_counts[category] += 1
                        if self.collected_counts[category] % 10 == 0:
                            print(f"[{category.upper()}] Progress: {self.collected_counts[category]}")
            else:
                 # print(f"[DEBUG] Failed to extract valid doc from {url}")
                 pass
        else:
             pass 

    def save_document(self, doc):
        with open(self.output_file, 'a', encoding='utf-8') as f:
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")

    async def crawl(self):
        # Clean previous run
        if os.path.exists(self.output_file):
            os.remove(self.output_file)

        connector = aiohttp.TCPConnector(limit=50) # Concurrency limit high but with backoff
        async with aiohttp.ClientSession(connector=connector, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }) as session:
            
            # 1. Parse Sitemaps to get candidate URLs
            all_candidates = {cat: [] for cat in self.target_categories}
            
            for sitemap_url in self.sitemaps:
                cat_urls = await self.parse_sitemap(session, sitemap_url)
                for cat, urls in cat_urls.items():
                    all_candidates[cat].extend(urls)
                    
            # 2. Process URLs concurrently
            tasks = []
            print("[CRAWL] Starting async crawl...")
            
            for cat in self.target_categories:
                urls = all_candidates[cat]
                # Shuffle to avoid hitting same date/pattern sequentially if beneficial, or just take first N
                # random.shuffle(urls) 
                
                # We do NOT slice candidates. Process ALL until done.
                candidates_to_process = urls
                
                print(f"[CRAWL] Queueing {len(candidates_to_process)} tasks for category '{cat}'")
                
                for url in candidates_to_process:
                    tasks.append(self.process_article(session, url, cat))
            
            # Run with progress bar
            _ = [await f for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Processing Articles")]
            
            print("[DONE] Crawl finished.")
            print("Final Counts:", self.collected_counts)

if __name__ == "__main__":
    output_path = os.path.join(os.getcwd(), "data", "newage_raw_documents.jsonl")
    crawler = NewAgeBDCrawler(output_file=output_path, target_count=200)
    asyncio.run(crawler.crawl())

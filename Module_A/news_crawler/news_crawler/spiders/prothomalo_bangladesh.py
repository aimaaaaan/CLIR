import scrapy
import json
from urllib.parse import urlencode


class ProthomAloBangladeshSpider(scrapy.Spider):
    name = "prothomalo_bangladesh"
    allowed_domains = ["prothomalo.com"]

    custom_settings = {
        "DOWNLOAD_DELAY": 1,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "LOG_LEVEL": "INFO",
        "FEED_EXPORT_ENCODING": "utf-8"
    }

    start_urls = [
        "https://www.prothomalo.com/bangladesh"
    ]

    API_BASE = "https://www.prothomalo.com/api/v1/collections/bangladesh-all"

    offset = 0
    limit = 10
    max_items = 500   # you can increase later

    items_seen = 0

    # ----------------------------
    # START: HTML PAGE
    # ----------------------------
    def parse(self, response):
        self.logger.info("Parsing Bangladesh section page")

        # 1️⃣ HEADLINE-ONLY ITEMS (NO LINK)
        for title in response.css("span.tilte-no-link-parent::text").getall():
            title = title.strip()
            if not title:
                continue

            self.items_seen += 1

            yield {
                "title": title,
                "body": None,
                "url": None,
                "date": None,
                "language": "bn",
                "author": None,
                "tokens": len(title.split()),
                "section": "bangladesh",
                "subcategory": "bangladesh",
                "source": "Prothom Alo",
                "content_type": "headline-only"
            }

        # 2️⃣ LINKED ARTICLES
        for a in response.css("a.excerpt"):
            title = a.css("::text").get()
            href = a.css("::attr(href)").get()

            if not title or not href:
                continue

            yield response.follow(
                href,
                callback=self.parse_article,
                meta={
                    "title": title.strip(),
                    "subcategory": "bangladesh"
                }
            )

        # 3️⃣ START API PAGINATION (আরও)
        yield self.next_api_request()

    # ----------------------------
    # API PAGINATION
    # ----------------------------
    def next_api_request(self):
        params = {
            "item-type": "story",
            "offset": self.offset,
            "limit": self.limit
        }
        url = f"{self.API_BASE}?{urlencode(params)}"

        self.offset += self.limit

        return scrapy.Request(
            url=url,
            callback=self.parse_api,
            headers={
                "Accept": "application/json",
                "Referer": "https://www.prothomalo.com/bangladesh"
            }
        )

    def parse_api(self, response):
        data = json.loads(response.text)
        items = data.get("items", [])

        if not items:
            self.logger.info("No more Bangladesh API items")
            return

        for entry in items:
            if self.items_seen >= self.max_items:
                return

            story = entry.get("story", {})
            item = entry.get("item", {})

            # headline
            headline = None
            if item.get("headline"):
                headline = item["headline"][0].strip()

            # story url (may not exist)
            story_path = story.get("url")

            # author
            author = story.get("author-name")

            self.items_seen += 1

            # ❌ NO LINK → headline-only
            if not story_path:
                yield {
                    "title": headline,
                    "body": None,
                    "url": None,
                    "date": None,
                    "language": "bn",
                    "author": author,
                    "tokens": len(headline.split()) if headline else 0,
                    "section": "bangladesh",
                    "subcategory": "bangladesh",
                    "source": "Prothom Alo",
                    "content_type": "headline-only"
                }
                continue

            # ✅ LINKED ARTICLE
            article_url = response.urljoin(story_path)

            yield scrapy.Request(
                url=article_url,
                callback=self.parse_article,
                meta={
                    "title": headline,
                    "author": author,
                    "subcategory": "bangladesh"
                }
            )

        # next page
        yield self.next_api_request()

    # ----------------------------
    # ARTICLE PAGE
    # ----------------------------
    def parse_article(self, response):
        paragraphs = response.css("div.story-element-text p::text").getall()
        body = " ".join(p.strip() for p in paragraphs if p.strip())

        yield {
            "title": response.meta.get("title"),
            "body": body,
            "url": response.url,
            "date": response.css("time::attr(datetime)").get(),
            "language": "bn",
            "author": response.meta.get("author"),
            "tokens": len(body.split()),
            "section": "bangladesh",
            "subcategory": response.meta.get("subcategory"),
            "source": "Prothom Alo",
            "content_type": "full-article"
        }

import os, csv, io, time, json, asyncio, datetime, re
from typing import List, Tuple
import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from crawl4ai.content_filter_strategy import PruningContentFilter

WEBHOOK = os.getenv("N8N_WEBHOOK_URL")
SHEET_URL = os.getenv("SHEET_URL", "").strip()
URLS_FILE = os.getenv("URLS_FILE", "urls.txt")
CONCURRENCY = int(os.getenv("CONCURRENCY", "4"))
WAIT_MS = int(os.getenv("WAIT_MS", "6000"))   # vänta igenom ev. Cloudflare-sida
TIMEOUT = int(os.getenv("TIMEOUT", "45"))

assert WEBHOOK, "N8N_WEBHOOK_URL not set"

def looks_like_url(s: str) -> bool:
    s = (s or "").strip()
    return s.startswith("http://") or s.startswith("https://")

def read_urls_from_txt(path: str) -> List[str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return [ln.strip() for ln in f if looks_like_url(ln)]
    except FileNotFoundError:
        return []

def read_urls_from_sheet(url: str) -> List[str]:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.content.decode("utf-8", errors="ignore")
    buf = io.StringIO(data)
    reader = csv.reader(buf)
    rows = list(reader)
    if not rows:
        return []
    header = [h.strip().lower() for h in rows[0]]
    urls = []
    if "url" in header:
        idx = header.index("url")
        for row in rows[1:]:
            if idx < len(row) and looks_like_url(row[idx]):
                urls.append(row[idx].strip())
    else:
        # fallback: första kolumnen
        for row in rows[1:]:
            if row and looks_like_url(row[0]):
                urls.append(row[0].strip())
    return urls

def extract_meta(html: str, url: str) -> Tuple[str, str, str, str, List[str]]:
    """
    Returnerar (title, canonical, published_time_iso, lang, links)
    Försök att läsa från og:taggar, schema.org och vanliga meta-taggar.
    """
    title = ""
    canonical = ""
    published_iso = ""
    lang = ""
    links: List[str] = []
    try:
        soup = BeautifulSoup(html or "", "html.parser")

        # html-lang
        if soup.html and soup.html.get("lang"):
            lang = soup.html["lang"].strip()

        # title
        mt = soup.find("meta", property="og:title") or soup.find("meta", attrs={"name": "title"})
        if mt and mt.get("content"): title = mt["content"].strip()
        if not title and soup.title and soup.title.string:
            title = soup.title.string.strip()

        # canonical
        can = soup.find("link", rel=lambda v: v and "canonical" in v)
        if can and can.get("href"): canonical = can["href"].strip()

        # published time (pröva flera varianter)
        date_candidates = []
        for sel in [
            {"property": "article:published_time"},
            {"name": "article:published_time"},
            {"property": "og:updated_time"},
            {"name": "pubdate"},
            {"itemprop": "datePublished"},
            {"name": "date"},
        ]:
            m = soup.find("meta", attrs=sel)
            if m and m.get("content"):
                date_candidates.append(m["content"].strip())

        # schema.org JSON-LD
        for script in soup.find_all("script", type=lambda t: t and "ld+json" in t):
            try:
                data = json.loads(script.string or "{}")
                if isinstance(data, list):
                    for obj in data:
                        if isinstance(obj, dict) and obj.get("datePublished"):
                            date_candidates.append(str(obj["datePublished"]))
                elif isinstance(data, dict) and data.get("datePublished"):
                    date_candidates.append(str(data["datePublished"]))
            except Exception:
                pass

        # fallback: datum i text (YYYY-MM-DD etc)
        if not date_candidates:
            m = re.search(r"\b(20\d{2}-\d{2}-\d{2})\b", html)
            if m: date_candidates.append(m.group(1))

        # parse första rimliga kandidat
        for d in date_candidates:
            try:
                dt = dateparser.parse(d)
                if dt: 
                    published_iso = dt.astimezone(datetime.timezone.utc).isoformat()
                    break
            except Exception:
                continue

        # länkar
        seen = set()
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if looks_like_url(href) and href not in seen:
                seen.add(href)
                links.append(href)

    except Exception:
        pass

    return (title, canonical or url, published_iso, lang, links)

async def crawl_one(crawler, url: str, run_cfg: CrawlerRunConfig, sem: asyncio.Semaphore, results: list):
    t0 = time.time()
    status, err = "ok", None
    try:
        async with sem:
            res = await crawler.arun(url=url, config=run_cfg)
            # Vänta igenom ev. Cloudflare/JS
            await asyncio.sleep(WAIT_MS / 1000)

            # Clean/fit markdown direkt från Crawl4AI:
            md_raw = res.markdown.raw_markdown if getattr(res, "markdown", None) else ""
            md_fit = res.markdown.fit_markdown if getattr(res, "markdown", None) else ""
            html = res.html or ""

            title, canonical, published_iso, lang, links = extract_meta(html, url)

            payload = {
                "url": url,
                "canonical_url": canonical,
                "title": title,
                "published_time": published_iso,     # ISO 8601 (UTC) om hittad
                "lang": lang or "",
                "markdown_raw": md_raw,              # full markdown
                "markdown_fit": md_fit,              # rensad/”news-vänlig”
                "html": html,                        # rå html om du vill debugga
                "links": links[:100],                # klipp listan lite
                "word_count": len((md_fit or md_raw).split()),
                "fetched_at": datetime.datetime.utcnow().isoformat() + "Z",
                "source": "github-actions"
            }

            r = requests.post(WEBHOOK, json=payload, timeout=60)
            r.raise_for_status()
    except Exception as e:
        status, err = "error", str(e)
    finally:
        results.append({"url": url, "status": status, "sec": round(time.time() - t0, 2), "error": err})

async def main():
    # Markdown-generator: bra default + lätt pruning av brus
    run_cfg = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        markdown_generator=DefaultMarkdownGenerator(
            content_filter=PruningContentFilter(
                threshold=0.48,       # 0.35–0.55 brukar vara bra; höj = mer rens
                threshold_type="fixed",
                min_word_threshold=0
            )
        ),
        word_count_threshold=1,
    )

    if SHEET_URL:
        urls = read_urls_from_sheet(SHEET_URL)
    else:
        urls = read_urls_from_txt(URLS_FILE)

    if not urls:
        print("Found 0 URLs"); return

    print(f"Found {len(urls)} URLs")

    results = []
    sem = asyncio.Semaphore(CONCURRENCY)
    browser = BrowserConfig(headless=True, verbose=False)

    async with AsyncWebCrawler(config=browser) as crawler:
        tasks = [crawl_one(crawler, u, run_cfg, sem, results) for u in urls]
        await asyncio.gather(*tasks)

    total = sum(r["sec"] for r in results)
    ok = sum(1 for r in results if r["status"] == "ok")
    err = [r for r in results if r["status"] != "ok"]
    print(json.dumps({"total_sec": round(total, 2), "ok": ok, "err_count": len(err), "errors": err}, ensure_ascii=False))

if __name__ == "__main__":
    asyncio.run(main())

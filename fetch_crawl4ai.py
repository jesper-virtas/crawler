import os, csv, io, time, json, asyncio, datetime
from typing import List
import requests

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from crawl4ai.content_filter_strategy import PruningContentFilter

# --- Konfiguration via env ---
WEBHOOK = os.getenv("N8N_WEBHOOK_URL")
SHEET_URL = os.getenv("SHEET_URL", "").strip()
URLS_FILE = os.getenv("URLS_FILE", "urls.txt")
CONCURRENCY = int(os.getenv("CONCURRENCY", "4"))
WAIT_MS = int(os.getenv("WAIT_MS", "6000"))     # vänta igenom ev. "Just a moment…"
TIMEOUT = int(os.getenv("TIMEOUT", "45"))       # (ej direkt använt här, behåll för ev. framtida timeout-hantering)

assert WEBHOOK, "N8N_WEBHOOK_URL not set"

# --- Hjälpare ---
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
    """
    Läser en publik CSV (Google Sheet export) och letar efter kolumnen 'URL'.
    Fallback: första kolumnen.
    """
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
        for row in rows[1:]:
            if row and looks_like_url(row[0]):
                urls.append(row[0].strip())
    return urls

def get_markdowns(res):
    """
    Robust extraktion av markdown ur Crawl4AI-resultat.
    Returnerar (md_raw, md_fit) oavsett om res.markdown är:
    - str
    - dict med nycklar 'raw_markdown'/'fit_markdown'
    - objekt med attribut .raw_markdown / .fit_markdown
    """
    m = getattr(res, "markdown", None)
    md_raw = ""
    md_fit = ""

    if m is None:
        return md_raw, md_fit

    # 1) ren sträng
    if isinstance(m, str):
        md_raw = m
        md_fit = m
        return md_raw, md_fit

    # 2) dict-lik
    if isinstance(m, dict):
        md_raw = m.get("raw_markdown") or m.get("raw") or ""
        md_fit = m.get("fit_markdown") or m.get("fit") or "" or md_raw
        return md_raw, md_fit

    # 3) objekt med attribut
    md_raw = getattr(m, "raw_markdown", None) or getattr(m, "raw", "") or ""
    md_fit = getattr(m, "fit_markdown", None) or getattr(m, "fit", "") or md_raw
    return md_raw, md_fit

# --- Crawl ---
async def crawl_one(crawler, url: str, run_cfg: CrawlerRunConfig, sem: asyncio.Semaphore, results: list):
    t0 = time.time()
    status, err = "ok", None
    try:
        async with sem:
            res = await crawler.arun(url=url, config=run_cfg)
            # Vänta igenom JS/Cloudflare om det behövs
            await asyncio.sleep(WAIT_MS / 1000)

            md_raw, md_fit = get_markdowns(res)
            html = getattr(res, "html", "") or ""

            payload = {
                "url": url,
                "markdown_fit": md_fit,
                "markdown_raw": md_raw,
                # Behåll html för felsökning – ta bort om du vill minimera payload:
                "html": html,
                "fetched_at": datetime.datetime.utcnow().isoformat() + "Z",
                "source": "github-actions"
            }

            r = requests.post(WEBHOOK, json=payload, timeout=60)
            r.raise_for_status()
    except Exception as e:
        status, err = "error", str(e)
    finally:
        results.append({
            "url": url,
            "status": status,
            "sec": round(time.time() - t0, 2),
            "error": err
        })

async def main():
    # Crawl4AI: clean/fit markdown med lätt pruning av brus
    run_cfg = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        markdown_generator=DefaultMarkdownGenerator(
            content_filter=PruningContentFilter(
                threshold=0.48,          # höj till 0.55 om du vill rensa hårdare
                threshold_type="fixed",
                min_word_threshold=0
            )
        ),
        word_count_threshold=1,
    )

    # Läs URLer
    if SHEET_URL:
        urls = read_urls_from_sheet(SHEET_URL)
    else:
        urls = read_urls_from_txt(URLS_FILE)

    if not urls:
        print("Found 0 URLs")
        return

    print(f"Found {len(urls)} URLs")

    # Kör med semafor (parallellism)
    results = []
    sem = asyncio.Semaphore(CONCURRENCY)
    browser = BrowserConfig(headless=True, verbose=False)

    async with AsyncWebCrawler(config=browser) as crawler:
        tasks = [crawl_one(crawler, u, run_cfg, sem, results) for u in urls]
        await asyncio.gather(*tasks)

    # Summering i loggen
    total = sum(r["sec"] for r in results)
    ok = sum(1 for r in results if r["status"] == "ok")
    err = [r for r in results if r["status"] != "ok"]
    print(json.dumps({
        "total_sec": round(total, 2),
        "ok": ok,
        "err_count": len(err),
        "errors": err[:10]  # visa max 10 i loggen
    }, ensure_ascii=False))

if __name__ == "__main__":
    asyncio.run(main())

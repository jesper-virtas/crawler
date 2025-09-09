import os, csv, io, time, json, asyncio, datetime
import requests
from typing import List
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode

N8N_WEBHOOK = os.getenv("N8N_WEBHOOK_URL")
SHEET_URL = os.getenv("SHEET_URL", "").strip()   # valfritt: Google Sheet public CSV
URLS_FILE = os.getenv("URLS_FILE", "urls.txt")   # fallback: fil i repo
CONCURRENCY = int(os.getenv("CONCURRENCY", "4")) # parallella hämtningar
WAIT_MS = int(os.getenv("WAIT_MS", "6000"))      # vänta igenom ev. Cloudflare
TIMEOUT = int(os.getenv("TIMEOUT", "45"))        # s per sida

assert N8N_WEBHOOK, "N8N_WEBHOOK_URL not set"

def read_urls_from_txt(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        urls = [ln.strip() for ln in f if ln.strip() and not ln.strip().startswith("#")]
    return urls

def read_urls_from_sheet(url: str) -> List[str]:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.content
    # funkar både för CSV och enkel radlista
    try:
        buf = io.StringIO(data.decode("utf-8"))
        reader = csv.reader(buf)
        urls = []
        for row in reader:
            if not row: 
                continue
            candidate = row[0].strip()
            if candidate and candidate.lower().startswith("http"):
                urls.append(candidate)
        return urls
    except Exception:
        txt = data.decode("utf-8")
        return [ln.strip() for ln in txt.splitlines() if ln.strip().startswith("http")]

async def crawl_one(crawler, url: str, run_cfg: CrawlerRunConfig, sem: asyncio.Semaphore, results: list):
    t0 = time.time()
    status = "ok"
    html = md = ""
    err = None
    try:
        async with sem:
            res = await crawler.arun(url=url, config=run_cfg)
            # Vänta ut “Just a moment …” om den dyker upp
            await asyncio.sleep(WAIT_MS / 1000)
            html = res.html or ""
            md = getattr(res, "markdown", "") or ""
            # Posta direkt till n8n
            payload = {
                "url": url,
                "html": html,
                "markdown": md,
                "fetched_at": datetime.datetime.utcnow().isoformat() + "Z",
                "source": "github-actions"
            }
            pr = requests.post(N8N_WEBHOOK, json=payload, timeout=60)
            pr.raise_for_status()
    except Exception as e:
        status = "error"
        err = str(e)
    finally:
        dt = time.time() - t0
        results.append({"url": url, "status": status, "sec": round(dt, 2), "error": err})

async def main():
    # Läs in URL:er
    if SHEET_URL:
        urls = read_urls_from_sheet(SHEET_URL)
    else:
        urls = read_urls_from_txt(URLS_FILE)

    if not urls:
        raise RuntimeError("No URLs provided")

    print(f"Found {len(urls)} URLs")

    browser = BrowserConfig(headless=True, verbose=False)
    run_cfg = CrawlerRunConfig(cache_mode=CacheMode.BYPASS, word_count_threshold=1)

    results = []
    sem = asyncio.Semaphore(CONCURRENCY)
    async with AsyncWebCrawler(config=browser) as crawler:
        tasks = [crawl_one(crawler, u, run_cfg, sem, results) for u in urls]
        await asyncio.gather(*tasks)

    # Summering (skrivs i Actions loggen)
    total = sum(r["sec"] for r in results)
    ok = sum(1 for r in results if r["status"] == "ok")
    err = [r for r in results if r["status"] != "ok"]
    print(json.dumps({"total_sec": round(total, 2), "ok": ok, "err_count": len(err), "errors": err}, ensure_ascii=False))

if __name__ == "__main__":
    asyncio.run(main())

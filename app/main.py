import asyncio
import ipaddress
import os
import socket
from contextlib import asynccontextmanager
from urllib.parse import urlparse

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, HttpUrl
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

ANALYSIS_TIMEOUT_SECONDS = 15
INITIAL_OBSERVE_SECONDS = 3

CF_WORKERS = 3
STEEL_WORKERS = 2
RENDER_WORKERS = 1

CF_ACCOUNT_ID = os.getenv("CLOUDFLARE_ACCOUNT_ID")
CF_API_TOKEN = os.getenv("CLOUDFLARE_API_TOKEN")
STEEL_API_KEY = os.getenv("STEEL_API_KEY")

# Shared in-memory scheduler. A task's attempt is 0=CF, 1=Steel, 2=Render.
class Job:
    def __init__(self, url: str):
        self.url = url
        self.attempt = 0
        self.future = asyncio.get_running_loop().create_future()

jobs = []
jobs_condition = asyncio.Condition()
workers = []

playwright = None
render_browser = None


class AnalyzeRequest(BaseModel):
    url: HttpUrl


def is_public_hostname(hostname: str) -> bool:
    if not hostname:
        return False
    host = hostname.strip().lower().rstrip(".")
    if host == "localhost" or host.endswith(".localhost"):
        return False
    try:
        infos = socket.getaddrinfo(host, None, proto=socket.IPPROTO_TCP)
    except socket.gaierror:
        return False
    for info in infos:
        ip = ipaddress.ip_address(info[4][0])
        if (ip.is_private or ip.is_loopback or ip.is_link_local or
                ip.is_multicast or ip.is_reserved or ip.is_unspecified or
                str(ip) == "169.254.169.254"):
            return False
    return True


def validate_target(raw_url: str) -> None:
    parsed = urlparse(raw_url)
    if parsed.scheme not in {"http", "https"}:
        raise HTTPException(400, "Only http:// and https:// URLs are allowed.")
    if parsed.port not in (None, 80, 443):
        raise HTTPException(400, "Only ports 80 and 443 are allowed.")
    if not is_public_hostname(parsed.hostname or ""):
        raise HTTPException(400, "This host is not allowed.")


async def request_is_public(request) -> bool:
    try:
        parsed = urlparse(request.url)
        if parsed.scheme not in {"http", "https"}:
            return True
        return parsed.port in (None, 80, 443) and is_public_hostname(parsed.hostname or "")
    except Exception:
        return False


async def analyze_with_browser(browser, target_url: str):
    """Run the existing Playwright detector against one browser. Timeout is the ONLY fallback signal."""
    validate_target(target_url)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + ANALYSIS_TIMEOUT_SECONDS

    def remaining_seconds():
        return max(0.1, deadline - loop.time())

    # Steel returns a context/page already; local/CF browsers need a new context/page.
    contexts = browser.contexts
    if contexts:
        context = contexts[0]
        page = await context.new_page()
        own_context = False
    else:
        context = await browser.new_context()
        page = await context.new_page()
        own_context = True

    found = asyncio.Event()
    result = {"url": None}
    clicked = False

    async def watch_request(request):
        if not await request_is_public(request):
            return
        parsed = urlparse(request.url)
        filename = parsed.path.rsplit("/", 1)[-1]
        if filename == "index.m3u8":
            result["url"] = request.url
            found.set()

    async def route_handler(route, request):
        if await request_is_public(request):
            await route.continue_()
        else:
            await route.abort()

    page.on("request", watch_request)
    await page.route("**/*", route_handler)

    try:
        try:
            response = await page.goto(
                target_url,
                wait_until="domcontentloaded",
                timeout=int(remaining_seconds() * 1000),
            )
        except PlaywrightTimeoutError as e:
            # THIS is the only condition that causes provider fallback.
            raise e

        if response is not None and response.status >= 400:
            return {"found": False, "url": None, "clicked_play": False,
                    "error": f"Site returned HTTP {response.status}."}

        if not found.is_set():
            try:
                await asyncio.wait_for(found.wait(), timeout=min(INITIAL_OBSERVE_SECONDS, remaining_seconds()))
            except asyncio.TimeoutError:
                pass

        if not found.is_set() and remaining_seconds() > 0.1:
            selectors = [
                'button[aria-label*="play" i]',
                '[role="button"][aria-label*="play" i]',
                '.vjs-big-play-button', '.jw-icon-playback',
                '.plyr__control--overlaid', 'video',
            ]
            for selector in selectors:
                try:
                    locator = page.locator(selector).first
                    if await locator.count() and await locator.is_visible():
                        await locator.click(timeout=min(1500, int(remaining_seconds() * 1000)), force=True)
                        clicked = True
                        break
                except Exception:
                    continue

        if not found.is_set() and remaining_seconds() > 0.1:
            try:
                await asyncio.wait_for(found.wait(), timeout=remaining_seconds())
            except asyncio.TimeoutError:
                # Detection deadline expired. This is a fallback condition.
                raise asyncio.TimeoutError("analysis timed out")

        return {
            "found": bool(result["url"]),
            "url": result["url"],
            "clicked_play": clicked,
            "error": None if result["url"] else "index.m3u8 was not found.",
        }
    finally:
        try:
            await page.close()
        except Exception:
            pass
        if own_context:
            try:
                await context.close()
            except Exception:
                pass


async def claim_job(provider: int):
    async with jobs_condition:
        while True:
            for i, job in enumerate(jobs):
                if job.attempt == provider:
                    return jobs.pop(i)
            await jobs_condition.wait()


async def put_job(job: Job):
    async with jobs_condition:
        jobs.append(job)
        jobs_condition.notify_all()


async def provider_worker(provider: int):
    while True:
        job = await claim_job(provider)
        try:
            if provider == 0:
                result = await run_cloudflare(job.url)
            elif provider == 1:
                result = await run_steel(job.url)
            else:
                result = await run_render(job.url)

            if not job.future.done():
                job.future.set_result(result)
        except (asyncio.TimeoutError, PlaywrightTimeoutError):
            # ONLY TIMEOUT FALLS THROUGH TO THE NEXT PROVIDER.
            if provider < 2:
                job.attempt += 1
                await put_job(job)
            else:
                if not job.future.done():
                    job.future.set_result({
                        "ok": False,
                        "message": "Timed out in all browser providers.",
                        "clicked_play": False,
                    })
        except Exception as e:
            # Non-timeout errors NEVER fall back.
            if not job.future.done():
                job.future.set_result({
                    "ok": False,
                    "message": f"Provider error: {type(e).__name__}",
                    "clicked_play": False,
                })


async def run_cloudflare(target_url: str):
    if not CF_ACCOUNT_ID or not CF_API_TOKEN:
        raise RuntimeError("Cloudflare credentials are not configured")
    endpoint = (
        f"wss://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}"
        "/browser-rendering/devtools/browser?keep_alive=600000"
    )
    browser = await playwright.chromium.connect_over_cdp(
        endpoint,
        headers={"Authorization": f"Bearer {CF_API_TOKEN}"},
    )
    try:
        return await analyze_with_browser(browser, target_url)
    finally:
        try:
            await browser.close()
        except Exception:
            pass


async def run_steel(target_url: str):
    if not STEEL_API_KEY:
        raise RuntimeError("Steel API key is not configured")

    # Steel's REST session API; avoids adding an SDK dependency.
    import urllib.request
    import json

    req = urllib.request.Request(
        "https://api.steel.dev/v1/sessions",
        data=json.dumps({"timeout": ANALYSIS_TIMEOUT_SECONDS * 1000}).encode(),
        headers={"steel-api-key": STEEL_API_KEY, "Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5) as r:
        session = json.loads(r.read().decode())

    session_id = session["id"]
    ws = session.get("websocketUrl") or session.get("websocket_url")
    if not ws:
        raise RuntimeError("Steel did not return a websocket URL")
    if "apiKey=" not in ws:
        ws += ("&" if "?" in ws else "?") + "apiKey=" + STEEL_API_KEY

    browser = None
    try:
        browser = await playwright.chromium.connect_over_cdp(ws)
        return await analyze_with_browser(browser, target_url)
    finally:
        if browser:
            try:
                await browser.close()
            except Exception:
                pass
        # Explicit release so sessions don't sit around billing until their timeout.
        release = urllib.request.Request(
            f"https://api.steel.dev/v1/sessions/{session_id}",
            headers={"steel-api-key": STEEL_API_KEY},
            method="DELETE",
        )
        try:
            urllib.request.urlopen(release, timeout=5).close()
        except Exception:
            pass


async def run_render(target_url: str):
    if render_browser is None:
        raise RuntimeError("Render browser is not ready")
    return await analyze_with_browser(render_browser, target_url)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global playwright, render_browser, workers
    playwright = await async_playwright().start()
    render_browser = await playwright.chromium.launch(headless=True, args=["--disable-dev-shm-usage"])

    for provider, count in ((0, CF_WORKERS), (1, STEEL_WORKERS), (2, RENDER_WORKERS)):
        for _ in range(count):
            workers.append(asyncio.create_task(provider_worker(provider)))
    try:
        yield
    finally:
        for task in workers:
            task.cancel()
        if workers:
            await asyncio.gather(*workers, return_exceptions=True)
        if render_browser:
            await render_browser.close()
        if playwright:
            await playwright.stop()


app = FastAPI(title="M3U8 Detector", lifespan=lifespan)


@app.get("/")
async def home():
    return FileResponse("app/static/index.html")


@app.get("/health")
async def health():
    return {"ok": True}


@app.post("/api/analyze")
async def analyze(payload: AnalyzeRequest):
    validate_target(str(payload.url))
    job = Job(str(payload.url))
    await put_job(job)
    return await job.future

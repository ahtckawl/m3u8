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

# One analysis at a time. Extra requests wait here in memory.
analysis_lock = asyncio.Lock()

# Whole analysis, including initial detection and optional click.
ANALYSIS_TIMEOUT_SECONDS = 15

# Small initial window to catch an automatically requested index.m3u8
INITIAL_OBSERVE_SECONDS = 3

playwright = None
browser = None


class AnalyzeRequest(BaseModel):
    url: HttpUrl


def is_public_hostname(hostname: str) -> bool:
    """Block localhost and obvious private/reserved IP targets."""
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
        if (
            ip.is_private
            or ip.is_loopback
            or ip.is_link_local
            or ip.is_multicast
            or ip.is_reserved
            or ip.is_unspecified
        ):
            return False

        # Explicitly reject common cloud metadata address.
        if str(ip) == "169.254.169.254":
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
    """Re-check each browser request, including redirects."""
    try:
        parsed = urlparse(request.url)
        if parsed.scheme not in {"http", "https"}:
            return True
        return parsed.port in (None, 80, 443) and is_public_hostname(parsed.hostname or "")
    except Exception:
        return False


async def find_index_m3u8(target_url: str):
    validate_target(target_url)

    async with analysis_lock:
        if browser is None:
            raise HTTPException(503, "Browser is not ready yet.")

        # One strict deadline for the ENTIRE analysis.
        loop = asyncio.get_running_loop()
        deadline = loop.time() + ANALYSIS_TIMEOUT_SECONDS

        def remaining_seconds():
            return max(0.1, deadline - loop.time())

        context = await browser.new_context()
        page = await context.new_page()
        found = asyncio.Event()
        result = {"url": None}
        clicked = False

        async def watch_request(request):
            if not await request_is_public(request):
                return

            parsed = urlparse(request.url)
            filename = parsed.path.rsplit("/", 1)[-1]

            # Query strings do not matter: /index.m3u8?token=...
            if filename == "index.m3u8":
                result["url"] = request.url
                found.set()

        page.on("request", watch_request)

        async def route_handler(route, request):
            if await request_is_public(request):
                await route.continue_()
            else:
                await route.abort()

        await page.route("**/*", route_handler)

        try:
            try:
                response = await page.goto(
                    target_url,
                    wait_until="domcontentloaded",
                    timeout=int(remaining_seconds() * 1000),
                )
            except PlaywrightTimeoutError:
                return {
                    "found": False,
                    "url": None,
                    "clicked_play": False,
                    "error": "Timed out while loading the page.",
                }
            except Exception as e:
                # DNS failure, connection refused/reset, invalid navigation, etc.
                return {
                    "found": False,
                    "url": None,
                    "clicked_play": False,
                    "error": f"Could not open the site: {type(e).__name__}",
                }

            # HTTP errors can fail immediately instead of occupying the queue.
            if response is not None and response.status >= 400:
                return {
                    "found": False,
                    "url": None,
                    "clicked_play": False,
                    "error": f"Site returned HTTP {response.status}.",
                }

            # First observation window: no clicking yet.
            if not found.is_set():
                try:
                    await asyncio.wait_for(
                        found.wait(),
                        timeout=min(INITIAL_OBSERVE_SECONDS, remaining_seconds()),
                    )
                except asyncio.TimeoutError:
                    pass

            # Only click if index.m3u8 was not already requested.
            if not found.is_set() and remaining_seconds() > 0.1:
                selectors = [
                    'button[aria-label*="play" i]',
                    '[role="button"][aria-label*="play" i]',
                    '.vjs-big-play-button',
                    '.jw-icon-playback',
                    '.plyr__control--overlaid',
                    'video',
                ]

                for selector in selectors:
                    try:
                        locator = page.locator(selector).first
                        if await locator.count() and await locator.is_visible():
                            await locator.click(
                                timeout=min(1500, int(remaining_seconds() * 1000)),
                                force=True,
                            )
                            clicked = True
                            break
                    except Exception:
                        continue

            # Use ONLY the time left from the original 15-second deadline.
            if not found.is_set() and remaining_seconds() > 0.1:
                try:
                    await asyncio.wait_for(found.wait(), timeout=remaining_seconds())
                except asyncio.TimeoutError:
                    pass

            return {
                "found": bool(result["url"]),
                "url": result["url"],
                "clicked_play": clicked,
                "error": None if result["url"] else "index.m3u8 was not found within 15 seconds.",
            }

        finally:
            await context.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global playwright, browser
    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch(
        headless=True,
        args=["--disable-dev-shm-usage"],
    )
    try:
        yield
    finally:
        if browser:
            await browser.close()
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
    result = await find_index_m3u8(str(payload.url))
    if not result["found"]:
        return {
            "ok": False,
            "message": result.get("error") or "index.m3u8 was not found within the time limit.",
            "clicked_play": result["clicked_play"],
        }

    return {
        "ok": True,
        "url": result["url"],
        "clicked_play": result["clicked_play"],
    }

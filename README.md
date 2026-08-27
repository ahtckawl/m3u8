# M3U8 Detector

Render + FastAPI + Playwright project.

## Behavior

- One analysis runs at a time.
- Extra API requests wait on an in-memory queue via an async lock.
- The page is opened and network requests are observed first.
- If `index.m3u8` is not observed, likely play controls are clicked.
- Total analysis budget is 15 seconds.
- Requests to localhost, private/reserved IPs, non-standard ports, and cloud metadata targets are blocked.
- No DRM or access-control bypass is implemented.

## Render

Deploy as a Docker Web Service.

Health endpoint:

`/health`

## Optional keep-alive workflow

The GitHub Actions workflow calls:

`RENDER_SERVICE_URL/health`

Add the full public Render service URL as a GitHub Actions repository secret named:

`RENDER_SERVICE_URL`

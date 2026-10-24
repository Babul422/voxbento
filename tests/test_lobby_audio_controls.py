"""Regression tests for My booths audio controls layout and CSS (#995, #997)."""

from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
import subprocess
import tempfile
import urllib.request
from pathlib import Path

import pytest
import websockets
from httpx import ASGITransport, AsyncClient

from fastapi_app import app
from portal.auth import create_user_token
from portal.database import (
    configure,
    create_booth,
    create_event,
    create_room,
    create_user,
    dispose,
    get_session,
    init_db,
    set_booth_membership,
    set_event_membership,
)
from portal.roles import EVENT_OWNER, INTERPRETER

ROOT = Path(__file__).resolve().parent.parent / "portal"
LOBBY_CSS = ROOT / "static" / "css" / "lobby.css"
STATIC_CSS_DIR = ROOT / "static" / "css"


@pytest.fixture
async def setup_db():
    configure("sqlite+aiosqlite://")
    await init_db()
    yield
    await dispose()


def client(cookies: dict[str, str] | None = None) -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test", cookies=cookies)


def find_chrome() -> str | None:
    env_chrome = os.environ.get("CHROME_BIN") or os.environ.get("CHROME_PATH")
    if env_chrome and os.path.exists(env_chrome):
        return env_chrome
    for name in (
        "google-chrome",
        "google-chrome-stable",
        "chromium-browser",
        "chromium",
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/Applications/Chromium.app/Contents/MacOS/Chromium",
    ):
        if name.startswith("/"):
            if os.path.exists(name):
                return name
        else:
            p = shutil.which(name)
            if p:
                return p
    return None


def test_lobby_css_has_microphone_sizing_rules():
    """Verify lobby.css maintains the sizing rules that prevent mic controls overflow."""
    content = LOBBY_CSS.read_text(encoding="utf-8")

    # .lobby-inline must allow shrinking within the grid
    assert re.search(
        r"\.lobby-inline\s*\{[^}]*min-width:\s*0;", content
    ), ".lobby-inline must declare min-width: 0;"

    # .lobby-select must have shrinking constraints (min-width: 0 and width: 0)
    assert re.search(
        r"\.lobby-select\s*\{[^}]*min-width:\s*0;[^}]*width:\s*0;", content
    ), ".lobby-select must declare min-width: 0 and width: 0;"

    # .lobby-inline .aa-btn must not shrink
    assert re.search(
        r"\.lobby-inline\s+\.aa-btn\s*\{[^}]*flex-shrink:\s*0;", content
    ), ".lobby-inline .aa-btn must declare flex-shrink: 0;"


@pytest.mark.anyio
@pytest.mark.parametrize("viewport_width", [320, 360])
async def test_my_booths_audio_controls_do_not_overflow_card(setup_db, viewport_width: int):
    """Render the actual My booths page and verify audio controls stay inside the card."""
    chrome = find_chrome()
    if not chrome:
        pytest.skip("Chrome/Chromium executable not found in environment")

    async with get_session() as s:
        event = await create_event(s, slug="summit", display_name="Summit 2026")
        room = await create_room(s, event_id=event.id, display_name="Main Hall")
        booth = await create_booth(s, event_id=event.id, room_id=room.id, language_code="de", language_name="Deutsch")
        user = await create_user(s, email="interp@example.org", display_name="Interp")
        await set_event_membership(s, user_id=user.id, event_id=event.id, role=EVENT_OWNER)
        await set_booth_membership(s, user_id=user.id, booth_id=booth.id, role=INTERPRETER)
        token = create_user_token(user_id=user.id, email=user.email)

    async with client(cookies={"user_token": token}) as http:
        res = await http.get("/interpreter")
    assert res.status_code == 200
    html = res.text

    # Rewrite static CSS links to absolute file:// URLs so headless browser resolves them
    static_css_path = str(STATIC_CSS_DIR.resolve())
    prepared_html = re.sub(
        r'href=["\'][^"\']*/static/css/([^"\'?]+)(?:\?[^"\']*)?["\']',
        rf'href="file://{static_css_path}/\1"',
        html,
    )

    # Inject a long microphone device label to test overflow resistance
    long_label = "Extremely Long External Studio Condenser USB Microphone Audio Interface (046d:0825)"
    prepared_html = prepared_html.replace(
        '<option value="">Loading devices…</option>',
        f'<option value="long-device">{long_label}</option>',
    )

    viewport_height = 568
    with tempfile.TemporaryDirectory() as td:
        html_file = os.path.join(td, "interpreter.html")
        with open(html_file, "w", encoding="utf-8") as f:
            f.write(prepared_html)

        profile_dir = os.path.join(td, "chrome_profile")
        os.makedirs(profile_dir)

        proc = subprocess.Popen(
            [
                chrome,
                "--headless=new",
                "--disable-gpu",
                "--no-sandbox",
                "--allow-file-access-from-files",
                f"--user-data-dir={profile_dir}",
                "--remote-debugging-port=0",
                f"--window-size={viewport_width},{viewport_height}",
                "about:blank",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        try:
            port_file = os.path.join(profile_dir, "DevToolsActivePort")
            for _ in range(50):
                if os.path.exists(port_file) or proc.poll() is not None:
                    break
                await asyncio.sleep(0.1)

            if proc.poll() is not None:
                stdout, stderr = proc.communicate()
                pytest.fail(f"Chrome failed to start (exit {proc.returncode}): {stderr.decode(errors='replace')}")

            assert os.path.exists(port_file), "Failed to read DevToolsActivePort from Chrome within timeout"

            with open(port_file, encoding="utf-8") as f:
                port = int(f.read().splitlines()[0])

            with urllib.request.urlopen(f"http://127.0.0.1:{port}/json", timeout=3) as r:
                targets = json.load(r)
            ws_url = [t for t in targets if t.get("type") == "page"][0]["webSocketDebuggerUrl"]

            async with asyncio.timeout(10.0):
                async with websockets.connect(ws_url, open_timeout=3.0) as ws:
                    cmd_id = 0

                    async def send(method, params=None):
                        nonlocal cmd_id
                        cmd_id += 1
                        cur = cmd_id
                        await ws.send(json.dumps({"id": cur, "method": method, "params": params or {}}))
                        while True:
                            resp = json.loads(await ws.recv())
                            if resp.get("id") == cur:
                                return resp.get("result")

                    await send(
                        "Emulation.setDeviceMetricsOverride",
                        {
                            "width": viewport_width,
                            "height": viewport_height,
                            "deviceScaleFactor": 1,
                            "mobile": False,
                        },
                    )
                    await send("Page.navigate", {"url": f"file://{html_file}"})
                    await asyncio.sleep(0.4)

                    res = await send(
                        "Runtime.evaluate",
                        {
                            "expression": f"""(() => {{
                                const card = document.querySelector('.lobby-check');
                                const select = document.querySelector('#mic-device-select');
                                const btn = document.querySelector('#mic-test-btn');
                                if (!card || !select || !btn) {{
                                    return {{ error: 'elements not found' }};
                                }}
                                const cr = card.getBoundingClientRect();
                                const sr = select.getBoundingClientRect();
                                const br = btn.getBoundingClientRect();
                                const vw = {viewport_width};
                                const selectedText = select.options[select.selectedIndex]?.text || '';
                                return {{
                                    cardRight: cr.right,
                                    cardWidth: cr.width,
                                    selectLeft: sr.left,
                                    selectRight: sr.right,
                                    selectWidth: sr.width,
                                    btnLeft: br.left,
                                    btnRight: br.right,
                                    btnWidth: br.width,
                                    selectedText: selectedText,
                                    hasLongLabel: selectedText.includes("Extremely Long"),
                                    cardInsideViewport: cr.right <= vw + 0.5,
                                    selectInsideCard: sr.left >= cr.left - 0.5 && sr.right <= cr.right + 0.5,
                                    btnInsideCard: br.left >= cr.left - 0.5 && br.right <= cr.right + 0.5,
                                    btnInsideViewport: br.right <= vw + 0.5,
                                    btnToRightOfSelect: sr.right <= br.left + 0.5,
                                    btnVisibleAndReadable: br.width >= 40
                                }};
                            }})()""",
                            "returnByValue": True,
                        },
                    )
                    metrics = res["result"]["value"]
                    assert "error" not in metrics, metrics.get("error")
                    assert metrics["hasLongLabel"], f"Long label not found in select: {metrics}"
                    assert metrics["cardInsideViewport"], f"Card expanded past viewport: {metrics}"
                    assert metrics["selectInsideCard"], f"Select overflows card: {metrics}"
                    assert metrics["btnInsideCard"], f"Test button overflows card: {metrics}"
                    assert metrics["btnInsideViewport"], f"Test button overflows viewport: {metrics}"
                    assert metrics["btnToRightOfSelect"], f"Test button overlaps select: {metrics}"
                    assert metrics["btnVisibleAndReadable"], f"Test button shrunk too small: {metrics}"
        finally:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=2)

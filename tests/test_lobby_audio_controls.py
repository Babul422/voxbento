"""Regression tests for My booths audio controls layout and CSS (#995, #997)."""

from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
import socket
import subprocess
import tempfile
import urllib.request
from pathlib import Path, PurePath, PureWindowsPath
from typing import Any

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

REQUIRED_LOBBY_STYLESHEETS = [
    "tokens.css",
    "fonts.css",
    "interpreter.css",
    "account-area.css",
    "app-header.css",
    "lobby.css",
]

VERIFY_LOBBY_STYLES_SCRIPT = """(() => {
    const inline = document.querySelector('.lobby-inline');
    const select = document.querySelector('.lobby-select');
    const btn = document.querySelector('.lobby-inline .aa-btn');
    if (!inline || !select || !btn) {
        return { ok: false, error: 'Required audio control elements not found' };
    }
    const csi = window.getComputedStyle(inline);
    const css = window.getComputedStyle(select);
    const csb = window.getComputedStyle(btn);

    const issues = [];
    if (csi.display !== 'flex') {
        issues.push(`.lobby-inline display is '${csi.display}', expected 'flex'`);
    }
    if (css.minWidth !== '0px') {
        issues.push(`.lobby-select min-width is '${css.minWidth}', expected '0px'`);
    }
    if (css.flexGrow !== '1') {
        issues.push(`.lobby-select flex-grow is '${css.flexGrow}', expected '1'`);
    }
    if (css.flexBasis !== '0%' && css.flexBasis !== '0px') {
        issues.push(`.lobby-select flex-basis is '${css.flexBasis}', expected '0%' or '0px'`);
    }
    if (csb.flexShrink !== '0') {
        issues.push(`.lobby-inline .aa-btn flex-shrink is '${csb.flexShrink}', expected '0'`);
    }
    if (issues.length > 0) {
        return { ok: false, error: issues.join('; ') };
    }
    return { ok: true };
})()"""


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


async def find_webkit() -> str | None:
    """Find WebKit runner: returns 'playwright', or path to WebKitWebDriver, or None."""
    env_webkit = os.environ.get("WEBKIT_BIN") or os.environ.get("WEBKIT_PATH")
    if env_webkit and (os.path.exists(env_webkit) or shutil.which(env_webkit)):
        return env_webkit

    try:
        from playwright.async_api import async_playwright

        async with async_playwright() as p:
            try:
                path = p.webkit.executable_path
                if path and os.path.exists(path):
                    return "playwright"
            except Exception:
                pass
    except (ImportError, Exception):
        pass

    for name in ("WebKitWebDriver", "/usr/bin/WebKitWebDriver"):
        if name.startswith("/"):
            if os.path.exists(name):
                return name
        else:
            p = shutil.which(name)
            if p:
                return p
    return None


def parse_devtools_active_port(content: str) -> tuple[int, str] | None:
    """Parse DevToolsActivePort content into (port, ws_path), or None if incomplete/invalid."""
    lines = [line.strip() for line in content.splitlines() if line.strip()]
    if len(lines) < 2:
        return None
    port_str, ws_path = lines[0], lines[1]
    if not port_str.isdigit():
        return None
    port = int(port_str)
    if not (1 <= port <= 65535):
        return None
    if not ws_path:
        return None
    return port, ws_path


async def wait_for_devtools_active_port(
    port_file: str | Path,
    proc: subprocess.Popen,
    timeout: float = 15.0,
    poll_interval: float = 0.1,
    stderr_file: str | Path | None = None,
) -> tuple[int, str]:
    """Poll until DevToolsActivePort can be read with a valid port and WebSocket path."""
    port_path = Path(port_file)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout

    while loop.time() < deadline:
        if proc.poll() is not None:
            stderr_msg = ""
            if stderr_file and os.path.exists(stderr_file):
                try:
                    stderr_msg = Path(stderr_file).read_text(encoding="utf-8", errors="replace").strip()
                except OSError:
                    pass
            elif hasattr(proc, "communicate"):
                _, stderr = proc.communicate()
                stderr_msg = stderr.decode(errors="replace").strip()
            raise RuntimeError(
                f"Chrome exited prematurely (exit code {proc.returncode}): {stderr_msg}"
            )
        if port_path.exists():
            try:
                content = port_path.read_text(encoding="utf-8")
                parsed = parse_devtools_active_port(content)
                if parsed is not None:
                    return parsed
            except OSError:
                pass
        await asyncio.sleep(poll_interval)

    # Bounded timeout reached
    stderr_msg = ""
    if hasattr(proc, "terminate") and proc.poll() is None:
        try:
            proc.terminate()
            if hasattr(proc, "wait"):
                proc.wait(timeout=2)
        except Exception:
            if hasattr(proc, "kill"):
                try:
                    proc.kill()
                    if hasattr(proc, "wait"):
                        proc.wait(timeout=1)
                except Exception:
                    pass
    if stderr_file and os.path.exists(stderr_file):
        try:
            stderr_msg = Path(stderr_file).read_text(encoding="utf-8", errors="replace").strip()
        except OSError:
            pass
    elif hasattr(proc, "communicate"):
        _, stderr = proc.communicate()
        stderr_msg = stderr.decode(errors="replace").strip()

    err_suffix = f": {stderr_msg}" if stderr_msg else ""
    raise TimeoutError(f"Failed to read valid DevToolsActivePort from Chrome within {timeout}s timeout{err_suffix}")


def rewrite_css_links(html: str, static_dir: Path | PurePath | str = STATIC_CSS_DIR) -> str:
    """Rewrite static CSS links to absolute file:// URLs using normalized POSIX paths."""
    if hasattr(static_dir, "resolve"):
        try:
            p = static_dir.resolve()
        except OSError:
            p = static_dir
    else:
        p = Path(static_dir)

    posix_path = p.as_posix().lstrip("/")
    base_file_url = f"file:///{posix_path}"
    return re.sub(
        r'href=["\'][^"\']*/static/css/([^"\'?]+)(?:\?[^"\']*)?["\']',
        lambda m: f'href="{base_file_url}/{m.group(1)}"',
        html,
    )


def build_geometry_eval_script(viewport_width: int) -> str:
    """Generate JavaScript snippet to evaluate bounding-box layout metrics."""
    return f"""(() => {{
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
    }})()"""


def assert_audio_controls_geometry(metrics: dict[str, Any]) -> None:
    """Assert microphone controls remain within the card, within viewport, and do not overlap."""
    assert "error" not in metrics, metrics.get("error")
    assert metrics["hasLongLabel"], f"Long label not found in select: {metrics}"
    assert metrics["cardInsideViewport"], f"Card expanded past viewport: {metrics}"
    assert metrics["selectInsideCard"], f"Select overflows card: {metrics}"
    assert metrics["btnInsideCard"], f"Test button overflows card: {metrics}"
    assert metrics["btnInsideViewport"], f"Test button overflows viewport: {metrics}"
    assert metrics["btnToRightOfSelect"], f"Test button overlaps select: {metrics}"
    assert metrics["btnVisibleAndReadable"], f"Test button shrunk too small: {metrics}"


async def render_and_evaluate_webkit(
    webkit_runner: str,
    html_file: str,
    viewport_width: int,
    viewport_height: int,
) -> dict[str, Any]:
    """Render fixture in WebKit and return geometry metrics."""
    if webkit_runner == "playwright":
        from playwright.async_api import async_playwright

        async with async_playwright() as p:
            browser = await p.webkit.launch(headless=True)
            try:
                page = await browser.new_page(viewport={"width": viewport_width, "height": viewport_height})
                await page.goto(f"file://{html_file}", wait_until="load")

                # WebKit on Linux enforces strict cross-directory file:// security boundaries,
                # blocking external <link rel="stylesheet"> files stored outside the HTML file's directory.
                # Explicitly verify whether stylesheets were loaded; if blocked by the local file sandbox,
                # inject the exact project stylesheets in order and re-verify required computed styles.
                style_check = await page.evaluate(VERIFY_LOBBY_STYLES_SCRIPT)
                if not style_check.get("ok"):
                    for css_name in REQUIRED_LOBBY_STYLESHEETS:
                        css_file = STATIC_CSS_DIR / css_name
                        if not css_file.is_file():
                            raise FileNotFoundError(f"Required project stylesheet not found: {css_file}")
                        try:
                            await page.add_style_tag(path=str(css_file))
                        except Exception as e:
                            raise RuntimeError(
                                f"Failed to inject project stylesheet '{css_name}' into WebKit: {e}"
                            ) from e

                    style_check = await page.evaluate(VERIFY_LOBBY_STYLES_SCRIPT)
                    if not style_check.get("ok"):
                        raise RuntimeError(
                            f"WebKit stylesheet verification failed after injection: {style_check.get('error')}"
                        )

                return await page.evaluate(build_geometry_eval_script(viewport_width))
            finally:
                await browser.close()

    # Otherwise WebKitWebDriver (alternative runner; note: unverified in environments without WebKitWebDriver installed)
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()

    proc = subprocess.Popen(
        [webkit_runner, "-p", str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        base_url = f"http://127.0.0.1:{port}"
        async with AsyncClient(base_url=base_url, timeout=10.0) as http:
            # Poll status
            for _ in range(50):
                if proc.poll() is not None:
                    raise RuntimeError(f"WebKit driver exited prematurely with code {proc.returncode}")
                try:
                    res = await http.get("/status")
                    if res.status_code == 200:
                        break
                except Exception:
                    pass
                await asyncio.sleep(0.1)

            session_res = await http.post(
                "/session",
                json={"capabilities": {"alwaysMatch": {}}},
            )
            session_res.raise_for_status()
            session_id = session_res.json()["value"]["sessionId"]
            try:
                await http.post(
                    f"/session/{session_id}/window/rect",
                    json={"width": viewport_width, "height": viewport_height},
                )
                await http.post(
                    f"/session/{session_id}/url",
                    json={"url": f"file://{html_file}"},
                )
                style_res = await http.post(
                    f"/session/{session_id}/execute/sync",
                    json={"script": f"return {VERIFY_LOBBY_STYLES_SCRIPT};", "args": []},
                )
                style_res.raise_for_status()
                style_check = style_res.json().get("value", {})
                if not style_check.get("ok"):
                    for css_name in REQUIRED_LOBBY_STYLESHEETS:
                        css_file = STATIC_CSS_DIR / css_name
                        if not css_file.is_file():
                            raise FileNotFoundError(f"Required project stylesheet not found: {css_file}")
                        css_content = css_file.read_text(encoding="utf-8")
                        inject_res = await http.post(
                            f"/session/{session_id}/execute/sync",
                            json={
                                "script": "const s = document.createElement('style'); s.textContent = arguments[0]; document.head.appendChild(s);",
                                "args": [css_content],
                            },
                        )
                        inject_res.raise_for_status()
                    style_res = await http.post(
                        f"/session/{session_id}/execute/sync",
                        json={"script": f"return {VERIFY_LOBBY_STYLES_SCRIPT};", "args": []},
                    )
                    style_res.raise_for_status()
                    style_check = style_res.json().get("value", {})
                    if not style_check.get("ok"):
                        raise RuntimeError(
                            f"WebKitWebDriver stylesheet verification failed after injection: {style_check.get('error')}"
                        )

                script = f"return {build_geometry_eval_script(viewport_width)};"
                eval_res = await http.post(
                    f"/session/{session_id}/execute/sync",
                    json={"script": script, "args": []},
                )
                eval_res.raise_for_status()
                return eval_res.json()["value"]
            finally:
                await http.delete(f"/session/{session_id}")
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=2)


class CDPClient:
    """Minimal CDP client over WebSocket with safe reader lifecycle and command dispatch."""

    def __init__(self, ws: Any) -> None:
        self.ws = ws
        self._cmd_id = 0
        self._pending_commands: dict[int, asyncio.Future[Any]] = {}
        self._lifecycle_load_events: dict[str, asyncio.Event] = {}
        self._closed = False
        self._shutdown_error: Exception | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()

    def get_loader_event(self, loader_id: str) -> asyncio.Event:
        if loader_id not in self._lifecycle_load_events:
            self._lifecycle_load_events[loader_id] = asyncio.Event()
        return self._lifecycle_load_events[loader_id]

    async def start(self) -> None:
        if self._reader_task is None:
            self._reader_task = asyncio.create_task(self._reader())

    async def _reader(self) -> None:
        shutdown_cause: Exception | None = None
        try:
            async for raw in self.ws:
                msg = json.loads(raw)
                if "id" in msg:
                    async with self._lock:
                        fut = self._pending_commands.pop(msg["id"], None)
                    if fut and not fut.done():
                        if "error" in msg:
                            fut.set_exception(
                                RuntimeError(f"CDP command {msg['id']} failed: {msg['error']}")
                            )
                        else:
                            fut.set_result(msg.get("result"))
                elif msg.get("method") == "Page.lifecycleEvent":
                    params = msg.get("params", {})
                    if params.get("name") == "load" and params.get("loaderId"):
                        self.get_loader_event(params["loaderId"]).set()
        except asyncio.CancelledError:
            shutdown_cause = ConnectionError("CDP reader was cancelled")
            raise
        except Exception as exc:
            shutdown_cause = exc
        finally:
            if shutdown_cause is None:
                shutdown_cause = ConnectionError("CDP connection closed")
            await self._shutdown(shutdown_cause)

    async def _shutdown(self, cause: Exception) -> None:
        async with self._lock:
            self._closed = True
            if self._shutdown_error is None:
                self._shutdown_error = cause
            pending = list(self._pending_commands.values())
            self._pending_commands.clear()

        for fut in pending:
            if not fut.done():
                fut.set_exception(cause)

    async def send(self, method: str, params: dict[str, Any] | None = None) -> Any:
        async with self._lock:
            if self._closed:
                raise ConnectionError(
                    f"Cannot send CDP command '{method}'; connection is closed"
                ) from self._shutdown_error
            self._cmd_id += 1
            cur = self._cmd_id
            loop = asyncio.get_running_loop()
            fut: asyncio.Future[Any] = loop.create_future()
            self._pending_commands[cur] = fut

        try:
            await self.ws.send(json.dumps({"id": cur, "method": method, "params": params or {}}))
        except BaseException:
            async with self._lock:
                self._pending_commands.pop(cur, None)
            if fut.done() and not fut.cancelled():
                fut.exception()
            raise

        try:
            return await fut
        except asyncio.CancelledError:
            async with self._lock:
                self._pending_commands.pop(cur, None)
            if fut.done() and not fut.cancelled():
                fut.exception()
            raise

    async def close(self) -> None:
        async with self._lock:
            if not self._closed:
                self._closed = True
                self._shutdown_error = ConnectionError("CDP client closed")
        if self._reader_task and not self._reader_task.done():
            self._reader_task.cancel()
            await asyncio.gather(self._reader_task, return_exceptions=True)
        await self._shutdown(self._shutdown_error or ConnectionError("CDP client closed"))

    async def __aenter__(self) -> CDPClient:
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()


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


def test_parse_devtools_active_port():
    assert parse_devtools_active_port("12345\n/devtools/browser/abc-123\n") == (12345, "/devtools/browser/abc-123")
    assert parse_devtools_active_port("  9222 \n  /devtools/browser/uuid  \n") == (9222, "/devtools/browser/uuid")
    assert parse_devtools_active_port("") is None
    assert parse_devtools_active_port("12345") is None
    assert parse_devtools_active_port("12345\n") is None
    assert parse_devtools_active_port("not_a_port\n/devtools/browser/abc") is None
    assert parse_devtools_active_port("0\n/devtools/browser/abc") is None
    assert parse_devtools_active_port("70000\n/devtools/browser/abc") is None
    assert parse_devtools_active_port("12345\n   \n") is None


class _MockProc:
    def __init__(self, returncode: int | None = None, stderr: bytes = b"") -> None:
        self.returncode = returncode
        self._stderr = stderr

    def poll(self) -> int | None:
        return self.returncode

    def communicate(self) -> tuple[bytes, bytes]:
        return b"", self._stderr

    def terminate(self) -> None:
        pass

    def kill(self) -> None:
        pass

    def wait(self, timeout: float | None = None) -> int | None:
        return self.returncode


@pytest.mark.anyio
async def test_wait_for_devtools_active_port_premature_exit():
    mock_proc = _MockProc(returncode=1, stderr=b"Chrome failed immediately")
    with pytest.raises(RuntimeError, match="Chrome exited prematurely"):
        await wait_for_devtools_active_port("/nonexistent/port", mock_proc, timeout=0.5, poll_interval=0.01)


@pytest.mark.anyio
async def test_wait_for_devtools_active_port_timeout(tmp_path: Path):
    mock_proc = _MockProc(returncode=None)
    port_file = tmp_path / "DevToolsActivePort"
    port_file.write_text("partial_only\n", encoding="utf-8")
    with pytest.raises(TimeoutError, match="Failed to read valid DevToolsActivePort"):
        await wait_for_devtools_active_port(port_file, mock_proc, timeout=0.05, poll_interval=0.01)


@pytest.mark.anyio
async def test_wait_for_devtools_active_port_success(tmp_path: Path):
    mock_proc = _MockProc(returncode=None)
    port_file = tmp_path / "DevToolsActivePort"

    async def write_delayed():
        await asyncio.sleep(0.02)
        port_file.write_text("9876\n/devtools/browser/test-path\n", encoding="utf-8")

    asyncio.create_task(write_delayed())
    port, ws_path = await wait_for_devtools_active_port(port_file, mock_proc, timeout=1.0, poll_interval=0.01)
    assert port == 9876
    assert ws_path == "/devtools/browser/test-path"


def test_rewrite_css_links_windows_and_posix():
    sample_html = (
        '<link rel="stylesheet" href="/static/css/lobby.css?v=20261025">'
        '<link rel="stylesheet" href=\'/static/css/interpreter.css\'>'
    )
    # Default STATIC_CSS_DIR
    rewritten_default = rewrite_css_links(sample_html)
    expected_default = f"file:///{STATIC_CSS_DIR.resolve().as_posix().lstrip('/')}"
    assert f'href="{expected_default}/lobby.css"' in rewritten_default
    assert f'href="{expected_default}/interpreter.css"' in rewritten_default
    assert "?v=" not in rewritten_default

    # Windows path with backslashes
    rewritten_win = rewrite_css_links(
        sample_html,
        static_dir=PureWindowsPath(r"C:\Users\runneradmin\app\portal\static\css"),
    )
    assert 'href="file:///C:/Users/runneradmin/app/portal/static/css/lobby.css"' in rewritten_win
    assert 'href="file:///C:/Users/runneradmin/app/portal/static/css/interpreter.css"' in rewritten_win


def test_assert_audio_controls_geometry_valid():
    valid_metrics = {
        "hasLongLabel": True,
        "cardInsideViewport": True,
        "selectInsideCard": True,
        "btnInsideCard": True,
        "btnInsideViewport": True,
        "btnToRightOfSelect": True,
        "btnVisibleAndReadable": True,
    }
    assert_audio_controls_geometry(valid_metrics)


def test_assert_audio_controls_geometry_failures():
    base = {
        "hasLongLabel": True,
        "cardInsideViewport": True,
        "selectInsideCard": True,
        "btnInsideCard": True,
        "btnInsideViewport": True,
        "btnToRightOfSelect": True,
        "btnVisibleAndReadable": True,
    }
    # Long label missing
    with pytest.raises(AssertionError, match="Long label not found"):
        assert_audio_controls_geometry({**base, "hasLongLabel": False})

    # Button overflows card
    with pytest.raises(AssertionError, match="Test button overflows card"):
        assert_audio_controls_geometry({**base, "btnInsideCard": False})


@pytest.mark.anyio
async def test_find_webkit(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    fake_driver = tmp_path / "fake_webkit_driver"
    fake_driver.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    monkeypatch.setenv("WEBKIT_BIN", str(fake_driver))
    assert await find_webkit() == str(fake_driver)

    monkeypatch.delenv("WEBKIT_BIN", raising=False)
    monkeypatch.delenv("WEBKIT_PATH", raising=False)


@pytest.mark.anyio
async def test_verify_lobby_styles_script_validation():
    """Verify VERIFY_LOBBY_STYLES_SCRIPT catches unstyled and malformed CSS and passes valid rules."""
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        try:
            browser = await p.webkit.launch(headless=True)
        except Exception as e:
            if os.environ.get("CI"):
                pytest.fail(f"Playwright WebKit required in CI but failed to launch: {e}")
            pytest.skip("Playwright WebKit not available for style script unit test")
        try:
            page = await browser.new_page()
            # 1. Elements missing
            await page.set_content("<html><body></body></html>")
            r_missing = await page.evaluate(VERIFY_LOBBY_STYLES_SCRIPT)
            assert not r_missing["ok"]
            assert "elements not found" in r_missing["error"]

            # 2. Unstyled elements
            await page.set_content(
                '<html><body><div class="lobby-inline"><select class="lobby-select"></select><button class="aa-btn"></button></div></body></html>'
            )
            r_unstyled = await page.evaluate(VERIFY_LOBBY_STYLES_SCRIPT)
            assert not r_unstyled["ok"]
            assert "expected 'flex'" in r_unstyled["error"]

            # 3. Correctly styled elements
            await page.add_style_tag(
                content=(
                    ".lobby-inline { display: flex; gap: 8px; min-width: 0; } "
                    ".lobby-select { flex: 1; min-width: 0; width: 0; } "
                    ".lobby-inline .aa-btn { flex-shrink: 0; }"
                )
            )
            r_styled = await page.evaluate(VERIFY_LOBBY_STYLES_SCRIPT)
            assert r_styled["ok"], r_styled.get("error")
        finally:
            await browser.close()



class _MockWS:
    def __init__(
        self,
        messages: list[str] | None = None,
        fail_recv: Exception | None = None,
        fail_send: Exception | None = None,
    ) -> None:
        self.messages = messages or []
        self.fail_recv = fail_recv
        self.fail_send = fail_send
        self.sent: list[str] = []
        self._idx = 0

    def __aiter__(self) -> _MockWS:
        return self

    async def __anext__(self) -> str:
        if self.fail_recv:
            raise self.fail_recv
        if self._idx < len(self.messages):
            msg = self.messages[self._idx]
            self._idx += 1
            return msg
        raise StopAsyncIteration

    async def send(self, data: str) -> None:
        if self.fail_send:
            raise self.fail_send
        self.sent.append(data)


@pytest.mark.anyio
async def test_cdp_client_success_and_lifecycle_event():
    ws = _MockWS(
        [
            json.dumps({"id": 1, "result": {"result": {"value": 42}}}),
            json.dumps({"method": "Page.lifecycleEvent", "params": {"name": "load", "loaderId": "L-123"}}),
        ]
    )
    async with CDPClient(ws) as cdp:
        res = await cdp.send("Runtime.evaluate", {"expression": "42"})
        assert res == {"result": {"value": 42}}
        loader_evt = cdp.get_loader_event("L-123")
        await asyncio.wait_for(loader_evt.wait(), timeout=1.0)
        assert loader_evt.is_set()


@pytest.mark.anyio
async def test_cdp_client_command_error():
    ws = _MockWS([json.dumps({"id": 1, "error": {"code": -32000, "message": "Evaluation failed"}})])
    async with CDPClient(ws) as cdp:
        with pytest.raises(RuntimeError, match="CDP command 1 failed"):
            await cdp.send("Runtime.evaluate")


@pytest.mark.anyio
async def test_cdp_client_shutdown_on_connection_close():
    class _HangingWS(_MockWS):
        def __init__(self) -> None:
            super().__init__()
            self.stop_event = asyncio.Event()

        async def __anext__(self) -> str:
            await self.stop_event.wait()
            raise StopAsyncIteration

    ws = _HangingWS()
    cdp = CDPClient(ws)
    await cdp.start()

    send_task = asyncio.create_task(cdp.send("Page.navigate"))
    await asyncio.sleep(0.02)
    # Reader exits normally when WebSocket stream terminates
    ws.stop_event.set()

    with pytest.raises(ConnectionError, match="CDP connection closed"):
        await send_task

    await cdp.close()


@pytest.mark.anyio
async def test_cdp_client_send_after_close():
    ws = _MockWS([])
    cdp = CDPClient(ws)
    await cdp.start()
    await cdp.close()

    with pytest.raises(ConnectionError, match="Cannot send CDP command 'Page.enable'; connection is closed"):
        await cdp.send("Page.enable")


@pytest.mark.anyio
async def test_cdp_client_ws_send_failure():
    ws = _MockWS(fail_send=ConnectionResetError("Socket broken"))
    async with CDPClient(ws) as cdp:
        with pytest.raises(ConnectionResetError, match="Socket broken"):
            await cdp.send("Page.enable")
        assert len(cdp._pending_commands) == 0


@pytest.mark.anyio
@pytest.mark.parametrize("browser_engine", ["chrome", "webkit"])
@pytest.mark.parametrize("viewport_width", [320, 360])
async def test_my_booths_audio_controls_do_not_overflow_card(
    setup_db, browser_engine: str, viewport_width: int
):
    """Render the actual My booths page and verify audio controls stay inside the card."""
    if browser_engine == "chrome":
        chrome = find_chrome()
        if not chrome:
            if os.environ.get("CI"):
                pytest.fail("Chrome/Chromium executable required in CI but not found")
            pytest.skip("Chrome/Chromium executable not found in environment")
    elif browser_engine == "webkit":
        webkit = await find_webkit()
        if not webkit:
            if os.environ.get("CI"):
                pytest.fail("WebKit browser/driver (WebKitWebDriver or Playwright WebKit) required in CI but not found")
            pytest.skip("WebKit browser/driver (WebKitWebDriver or Playwright WebKit) not found in environment")
    else:
        raise ValueError(f"Unknown browser engine: {browser_engine}")

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

    # Rewrite static CSS links to absolute file:// URLs using normalized paths and callable replacement
    prepared_html = rewrite_css_links(html)
    assert "file://" in prepared_html, "Static CSS links were not rewritten to file:// URLs"
    assert "lobby.css" in prepared_html, "lobby.css link was not found in rewritten HTML"

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

        if browser_engine == "webkit":
            metrics = await render_and_evaluate_webkit(webkit, html_file, viewport_width, viewport_height)
            assert_audio_controls_geometry(metrics)
            return

        # Chrome browser execution
        profile_dir = os.path.join(td, "chrome_profile")
        os.makedirs(profile_dir)
        stderr_log = os.path.join(td, "chrome_stderr.log")

        with open(stderr_log, "wb") as stderr_f:
            proc = subprocess.Popen(
                [
                    chrome,
                    "--headless=new",
                    "--disable-gpu",
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--allow-file-access-from-files",
                    f"--user-data-dir={profile_dir}",
                    "--remote-debugging-port=0",
                    f"--window-size={viewport_width},{viewport_height}",
                    "about:blank",
                ],
                stdout=subprocess.DEVNULL,
                stderr=stderr_f,
            )

        try:
            port_file = os.path.join(profile_dir, "DevToolsActivePort")
            try:
                port, _ = await wait_for_devtools_active_port(
                    port_file, proc, timeout=15.0, stderr_file=stderr_log
                )
            except (RuntimeError, TimeoutError) as e:
                pytest.fail(f"Chrome failed to start: {e}")

            with urllib.request.urlopen(f"http://127.0.0.1:{port}/json", timeout=3) as r:
                targets = json.load(r)
            ws_url = [t for t in targets if t.get("type") == "page"][0]["webSocketDebuggerUrl"]

            async with asyncio.timeout(10.0):
                async with websockets.connect(ws_url, open_timeout=3.0) as ws:
                    async with CDPClient(ws) as cdp:
                        send = cdp.send

                        await send("Page.enable")
                        await send("Page.setLifecycleEventsEnabled", {"enabled": True})
                        await send(
                            "Emulation.setDeviceMetricsOverride",
                            {
                                "width": viewport_width,
                                "height": viewport_height,
                                "deviceScaleFactor": 1,
                                "mobile": False,
                            },
                        )
                        nav = await send("Page.navigate", {"url": f"file://{html_file}"})
                        loader_id = nav.get("loaderId") if isinstance(nav, dict) else None
                        assert loader_id, f"Page.navigate did not return loaderId: {nav}"
                        await cdp.get_loader_event(loader_id).wait()

                        # Explicitly verify required CSS rules are active in Chrome
                        style_res = await send(
                            "Runtime.evaluate",
                            {
                                "expression": VERIFY_LOBBY_STYLES_SCRIPT,
                                "returnByValue": True,
                            },
                        )
                        style_check = style_res["result"]["value"]
                        assert style_check.get("ok"), (
                            f"Chrome stylesheet verification failed: {style_check.get('error')}"
                        )

                        res = await send(
                            "Runtime.evaluate",
                            {
                                "expression": build_geometry_eval_script(viewport_width),
                                "returnByValue": True,
                            },
                        )
                        metrics = res["result"]["value"]
                        assert_audio_controls_geometry(metrics)
        finally:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=2)

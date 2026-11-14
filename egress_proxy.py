"""Validating egress proxy: closes the DNS-rebinding gap between "checked the name" and "connected to the address".

The crawler's HTTP client and Playwright send every request through this local proxy. The proxy resolves the host ONCE, refuses the
request if ANY answer is not globally routable, and connects to the very address it validated (so a second, different DNS answer
can never be used). HTTPS goes through CONNECT, i.e. TLS stays end to end between client and site: SNI and certificate checks are
untouched. It listens on 127.0.0.1 only, in a background thread of each process.
shortcut: HTTP/1.1 with Connection: close per plain-HTTP request, no keep-alive/pipelining; fine for a crawler, upgrade if throughput needs it.
"""

import asyncio
import socket
import threading
import time
from urllib.parse import urlsplit

import crawler_core as core

_lock = threading.Lock()
_url: str | None = None


DNS_TTL = 300  # seconds; the cache only ever holds answers, _handle still validates every cached address on every request
_dns: dict[str, tuple[float, list[str]]] = {}  # touched only from the proxy's single event loop, so no lock needed
# shortcut: fixed TTL, ignores the DNS record's own TTL; entries are evicted only when read stale or when 10k are held


async def resolve(host: str) -> list[str]:  # one lookup per TTL; tests replace this to stage a rebinding attack
    if core.ALLOW_PRIVATE and host.endswith(".localhost"):  # tests only: Windows cannot resolve *.localhost
        return [core.test_ip(host)]
    hit = _dns.get(host)
    if hit and hit[0] > time.monotonic():
        return list(hit[1])
    infos = await asyncio.get_running_loop().getaddrinfo(host, None, type=socket.SOCK_STREAM)
    addrs = list(dict.fromkeys(i[4][0] for i in infos))
    if len(_dns) >= 10_000:
        _dns.clear()
    _dns[host] = (time.monotonic() + DNS_TTL, addrs)  # failures raise above and are never cached
    return addrs


async def _pipe(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
    try:
        while data := await r.read(65536):
            w.write(data)
            await w.drain()
    except (ConnectionError, OSError):
        pass
    finally:
        w.close()


async def _deny(w: asyncio.StreamWriter, code: int, why: str) -> None:
    core.log_blocked(why)
    w.write(f"HTTP/1.1 {code} {why}\r\nContent-Length: 0\r\nConnection: close\r\nX-Egress-Blocked: 1\r\n\r\n".encode("latin-1", "replace"))
    await w.drain()


async def _handle(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
    up = None
    try:
        head = await asyncio.wait_for(r.readuntil(b"\r\n\r\n"), 30)
        line, *headers = head[:-4].split(b"\r\n")
        method, target, _ = line.decode("latin-1").split(" ", 2)
        if method == "CONNECT":
            host, _, port = target.rpartition(":")
            port = int(port)
        else:
            u = urlsplit(target)
            if u.scheme != "http" or not u.hostname:
                return await _deny(w, 400, "only absolute http:// URLs or CONNECT")
            host, port = u.hostname, u.port or 80
        host = host.strip("[]")
        try:
            addrs = await resolve(host)
        except OSError as e:
            return await _deny(w, 502, f"DNS failure for {host}: {e}")
        bad = [a for a in addrs if not core.ip_allowed(a)]
        if bad or not addrs:  # one private answer among public ones is enough to refuse: the client could be given either
            return await _deny(w, 403, f"{host} resolves to non-public address {bad[0] if bad else '(none)'}")
        for a in addrs:  # connect to the validated literal address, never to the name again
            try:
                up_r, up = await asyncio.wait_for(asyncio.open_connection(a, port), 10)
                break
            except (OSError, asyncio.TimeoutError):
                continue
        else:
            return await _deny(w, 502, f"cannot connect to {host}")
        if method == "CONNECT":
            w.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
        else:
            drop = (b"proxy-", b"connection:", b"keep-alive:")
            keep = [h for h in headers if not h.lower().startswith(drop)]
            path = (u.path or "/") + (f"?{u.query}" if u.query else "")
            up.write(f"{method} {path} HTTP/1.1\r\n".encode() + b"\r\n".join(keep) + b"\r\nConnection: close\r\n\r\n")
        await w.drain()
        await asyncio.gather(_pipe(r, up), _pipe(up_r, w))
    except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, asyncio.TimeoutError, ValueError, ConnectionError, OSError):
        pass
    finally:
        w.close()
        if up:
            up.close()


def ensure() -> str:
    """Start the proxy in this process (once) and return its URL, e.g. http://127.0.0.1:53211."""
    global _url
    with _lock:
        if _url:
            return _url
        ready = threading.Event()
        box: dict = {}

        def run() -> None:
            loop = asyncio.new_event_loop()
            srv = loop.run_until_complete(asyncio.start_server(_handle, "127.0.0.1", 0, backlog=512))
            box["port"] = srv.sockets[0].getsockname()[1]
            ready.set()
            loop.run_forever()

        threading.Thread(target=run, name="egress-proxy", daemon=True).start()
        ready.wait(10)
        _url = f"http://127.0.0.1:{box['port']}"
        return _url

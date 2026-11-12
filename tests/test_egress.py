"""DNS rebinding: the egress proxy resolves once, validates every answer, and connects to the validated address only."""
import socket

import pytest

import crawler_core as core
import egress_proxy
from test_crawler import HITS, SITES, page, port  # noqa: F401 - local test server


@pytest.fixture()
def dns(monkeypatch, port):
    """Scripted DNS: calls[host] counts lookups; answers[host] is the sequence of answers an attacker's DNS would give."""
    monkeypatch.setattr(core, "ALLOW_PRIVATE", False)
    monkeypatch.setattr(core, "ip_allowed", lambda a: a == "127.0.0.1")  # stand-in: 127.0.0.1 plays "the public IP", everything else is private
    calls, answers = {}, {}

    async def fake(host):
        calls[host] = calls.get(host, 0) + 1
        seq = answers[host]
        return seq[min(calls[host], len(seq)) - 1]
    monkeypatch.setattr(egress_proxy, "resolve", fake)
    SITES.clear(); HITS.clear()
    SITES["rebind.test"] = {"/": page("hello")}
    return calls, answers, port


def ask(first_line: str, extra: bytes = b"", read_body: bool = True) -> bytes:
    with socket.create_connection(egress_proxy.ensure().removeprefix("http://").split(":")[0:1] + [int(egress_proxy.ensure().rsplit(":", 1)[1])], timeout=10) as s:
        s.sendall(first_line.encode() + b"\r\n" + extra + b"\r\n")
        out = b""
        while chunk := s.recv(65536):
            out += chunk
            if not read_body or b"hello" in out or b"X-Egress-Blocked" in out:
                break
        return out


def test_rebinding_second_answer_is_never_used(dns):
    calls, answers, port = dns
    answers["rebind.test"] = [["127.0.0.1"], ["169.254.169.254"]]  # first answer passes validation, the second would hit cloud metadata
    r = ask(f"GET http://rebind.test:{port}/ HTTP/1.1", f"Host: rebind.test:{port}\r\n".encode())
    assert b"200" in r.split(b"\r\n")[0] and b"hello" in r
    assert calls["rebind.test"] == 1  # pinned: the name was looked up once and never again for the connection
    assert HITS[("rebind.test", "/")] == 1


def test_any_private_answer_refuses_the_request(dns):
    calls, answers, port = dns
    answers["rebind.test"] = [["127.0.0.1", "10.0.0.5"]]  # mixed answer: the client could be given either
    r = ask(f"GET http://rebind.test:{port}/ HTTP/1.1", f"Host: rebind.test:{port}\r\n".encode())
    assert b" 403 " in r.split(b"\r\n")[0] and b"X-Egress-Blocked" in r
    assert not HITS


def test_private_only_answer_and_connect_tunnel_refused(dns):
    calls, answers, port = dns
    answers["rebind.test"] = [["169.254.169.254"]]
    assert b" 403 " in ask(f"GET http://rebind.test:{port}/ HTTP/1.1", b"Host: x\r\n").split(b"\r\n")[0]
    assert b" 403 " in ask(f"CONNECT rebind.test:{port} HTTP/1.1", b"Host: x\r\n").split(b"\r\n")[0]
    assert not HITS


def test_connect_tunnel_to_validated_address_works(dns):
    calls, answers, port = dns
    answers["rebind.test"] = [["127.0.0.1"], ["10.0.0.5"]]
    with socket.create_connection(("127.0.0.1", int(egress_proxy.ensure().rsplit(":", 1)[1])), timeout=10) as s:
        s.sendall(f"CONNECT rebind.test:{port} HTTP/1.1\r\n\r\n".encode())
        assert b" 200 " in s.recv(1024)
        s.sendall(f"GET / HTTP/1.1\r\nHost: rebind.test\r\nConnection: close\r\n\r\n".encode())
        data = b""
        while chunk := s.recv(65536):
            data += chunk
    assert b"hello" in data and calls["rebind.test"] == 1


def test_real_policy_blocks_loopback_and_private(monkeypatch):
    monkeypatch.setattr(core, "ALLOW_PRIVATE", False)
    for bad in ("127.0.0.1", "10.1.2.3", "169.254.169.254", "::1", "::ffff:127.0.0.1", "100.64.0.1", "0.0.0.0", "224.0.0.1"):
        assert not core.ip_allowed(bad), bad
    assert core.ip_allowed("93.184.216.34")

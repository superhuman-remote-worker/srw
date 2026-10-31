"""The public-address rule for server-side fetches (audit finding N34).

``check_public_http_url`` gates every URL the agent pod fetches for a model:
http(s) only, and the host must be, or resolve only to, public addresses.
DNS is a fake resolver (or a patched ``getaddrinfo``); nothing here touches
the network.
"""

from __future__ import annotations

import ipaddress
import socket

import pytest

from shared.url_safety import (
    UnsafeURLError,
    check_public_http_url,
    literal_ip,
    public_address_refusal,
    system_resolver,
    url_host,
)

#: The fake DNS: never the system resolver.
DNS = {
    "public.example": ["93.184.216.34"],
    "dual.example": ["93.184.216.34", "2606:2800:220:1:248:1893:25c8:1946"],
    "internal.example": ["10.0.0.5"],
    "mixed.example": ["93.184.216.37", "10.0.0.7"],
    "metadata.example": ["169.254.169.254"],
    "cgnat.example": ["100.64.1.2"],
    "loopback6.example": ["::1"],
    "ula.example": ["fd12:3456::1"],
    "linklocal6.example": ["fe80::1%eth0"],
    "mapped.example": ["::ffff:10.0.0.1"],
}


def fake_resolver(host: str, port: int) -> list[str]:
    try:
        return DNS[host]
    except KeyError:
        raise socket.gaierror(socket.EAI_NONAME, "Name or service not known")


def check(url: str) -> str:
    return check_public_http_url(url, resolver=fake_resolver)


class TestPublicUrlsPass:
    @pytest.mark.parametrize(
        "url",
        [
            "https://public.example/page",
            "http://public.example:8080/page?q=1",
            "HTTPS://PUBLIC.EXAMPLE/",
            "https://dual.example/",
            "https://93.184.216.34/",
            "https://[2606:2800:220:1:248:1893:25c8:1946]/",
        ],
    )
    def test_public(self, url):
        assert check(url) == url

    def test_resolver_gets_the_url_port_or_the_scheme_default(self):
        seen = []

        def resolver(host, port):
            seen.append((host, port))
            return ["93.184.216.34"]

        check_public_http_url("https://public.example/", resolver=resolver)
        check_public_http_url("http://public.example/", resolver=resolver)
        check_public_http_url("http://public.example:8443/", resolver=resolver)
        assert seen == [
            ("public.example", 443),
            ("public.example", 80),
            ("public.example", 8443),
        ]


class TestLiteralAddressesRefused:
    @pytest.mark.parametrize(
        "url",
        [
            "http://127.0.0.1/",
            "http://127.0.0.1:5432/",
            "http://10.0.0.1/",
            "http://10.43.0.10:8085/api/jobs",
            "http://172.16.5.4/",
            "http://192.168.1.1/",
            "http://169.254.169.254/latest/meta-data/",
            "http://100.64.0.1/",
            "http://100.127.255.254/",
            "http://0.0.0.0:8080/",
            "http://224.0.0.1/",
            "http://240.0.0.1/",
            "http://[::1]/",
            "http://[::]/",
            "http://[fc00::1]/",
            "http://[fd00:ec2::254]/",
            "http://[fe80::1%25eth0]/",
            "http://[::ffff:10.0.0.1]/",
            "http://[::ffff:169.254.169.254]/",
            "http://[::ffff:127.0.0.1]/",
            "http://[::a00:1]/",  # IPv4-compatible
            "http://[64:ff9b::a00:1]/",  # NAT64 carrying 10.0.0.1
            "http://[2002:a9fe:a9fe::1]/",  # 6to4 carrying metadata
        ],
    )
    def test_refused(self, url):
        with pytest.raises(UnsafeURLError, match=r"^refused: "):
            check(url)

    @pytest.mark.parametrize(
        "url",
        [
            "http://2852039166/",  # 169.254.169.254, decimal
            "http://0xa9fea9fe/",  # hex
            "http://0251.0376.0251.0376/",  # octal
            "http://0xa9.0xfe.0xa9.0xfe/",
            "http://127.1/",
            "http://017700000001/",
            "http://0x7f.1/",
            "http://10.0.0.1./",  # trailing dot
            "http://%31%30.0.0.1/",  # percent-encoded
            "http://\uff11\uff16\uff19\uff0e\uff12\uff15\uff14\uff0e\uff11\uff16\uff19\uff0e\uff12\uff15\uff14/",  # full-width
            # A backslash ends the authority for requests and browsers.
            "http://10.0.0.1\\@public.example/",
            "http://169.254.169.254\\.public.example/",
        ],
    )
    def test_other_spellings_of_refused_addresses(self, url):
        with pytest.raises(UnsafeURLError, match=r"^refused: "):
            check(url)

    def test_a_literal_is_not_resolved(self):
        def resolver(host, port):
            raise AssertionError("a literal address must not reach DNS")

        with pytest.raises(UnsafeURLError):
            check_public_http_url("http://10.0.0.1/", resolver=resolver)
        assert (
            check_public_http_url("http://93.184.216.34/", resolver=resolver)
            == "http://93.184.216.34/"
        )

    def test_message_names_the_host_and_reason(self):
        with pytest.raises(UnsafeURLError) as exc:
            check("http://169.254.169.254/latest/")
        assert str(exc.value) == (
            "refused: 169.254.169.254 is link-local, metadata, multicast or unspecified"
        )

    def test_cgnat_is_named(self):
        with pytest.raises(UnsafeURLError, match="not a public address|CGNAT"):
            check("http://100.64.0.1/")


class TestResolvedAddressesRefused:
    @pytest.mark.parametrize(
        "host",
        [
            "internal.example",
            "metadata.example",
            "cgnat.example",
            "loopback6.example",
            "ula.example",
            "linklocal6.example",
            "mapped.example",
        ],
    )
    def test_refused(self, host):
        with pytest.raises(UnsafeURLError) as exc:
            check(f"https://{host}/page")
        assert str(exc.value).startswith(f"refused: {host} resolves to an address")

    def test_one_private_answer_among_public_ones_refuses(self):
        with pytest.raises(UnsafeURLError, match="mixed.example resolves"):
            check("https://mixed.example/")

    def test_the_resolved_address_is_not_disclosed(self):
        with pytest.raises(UnsafeURLError) as exc:
            check("https://internal.example/")
        assert "10.0.0.5" not in str(exc.value)

    def test_a_name_that_does_not_resolve_is_a_fetch_failure_not_a_refusal(self):
        with pytest.raises(OSError) as exc:
            check("https://nowhere.example/")
        assert not isinstance(exc.value, UnsafeURLError)

    def test_an_empty_answer_is_a_fetch_failure(self):
        with pytest.raises(OSError):
            check_public_http_url("https://x.example/", resolver=lambda h, p: [])


class TestSchemesAndShapes:
    @pytest.mark.parametrize(
        "url",
        [
            "file:///etc/passwd",
            "gopher://public.example:70/_GET",
            "ftp://public.example/",
            "dict://public.example:11211/",
            "javascript:alert(1)",
            "data:text/html,hi",
        ],
    )
    def test_non_http_schemes(self, url):
        with pytest.raises(UnsafeURLError, match="http and https only"):
            check(url)

    def test_no_scheme(self):
        with pytest.raises(UnsafeURLError, match="has no scheme"):
            check("public.example/page")

    @pytest.mark.parametrize("url", ["http:///path", "https://:443/"])
    def test_no_host(self, url):
        with pytest.raises(UnsafeURLError, match="no host"):
            check(url)

    def test_bad_port(self):
        with pytest.raises(UnsafeURLError, match="not a valid URL"):
            check("http://public.example:99999/")

    def test_whitespace_in_host(self):
        # inet_aton would read "127.0.0.1 x" as 127.0.0.1.
        with pytest.raises(UnsafeURLError):
            check("http://127.0.0.1 x/")

    def test_credentials_do_not_hide_the_host(self):
        with pytest.raises(UnsafeURLError, match="refused: 10.0.0.1"):
            check("http://public.example@10.0.0.1/")


class TestLiteralIp:
    @pytest.mark.parametrize(
        ("host", "expected"),
        [
            ("10.0.0.1", "10.0.0.1"),
            ("2852039166", "169.254.169.254"),
            ("0xa9fea9fe", "169.254.169.254"),
            ("127.1", "127.0.0.1"),
            ("10.0.0.1.", "10.0.0.1"),
            ("::1", "::1"),
            ("fe80::1%25eth0", "fe80::1"),
            ("::ffff:10.0.0.1", "::ffff:10.0.0.1"),
            ("%31%30.0.0.1", "10.0.0.1"),
            ("\uff11\uff10\uff0e\uff10\uff0e\uff10\uff0e\uff11", "10.0.0.1"),
            ("10\u30020\u30020\u30021", "10.0.0.1"),  # ideographic full stop
        ],
    )
    def test_addresses(self, host, expected):
        assert literal_ip(host) == ipaddress.ip_address(expected)

    @pytest.mark.parametrize(
        "host", ["example.com", "cafe.be", "beef", "127.0.0.1 x", "", "a:b:c:zz"]
    )
    def test_names(self, host):
        assert literal_ip(host) is None


class TestUrlHost:
    @pytest.mark.parametrize(
        ("url", "host"),
        [
            ("http://10.0.0.1\\@public.example/", "10.0.0.1"),
            ("http://public.example\\path", "public.example"),
            ("http://user:pw@public.example:8080/", "public.example"),
            ("http://public.example#@10.0.0.1/", "public.example"),
            ("http://public.example?@10.0.0.1/", "public.example"),
        ],
    )
    def test_host_as_clients_read_it(self, url, host):
        assert url_host(url).hostname == host

    def test_bad_port_raises(self):
        with pytest.raises(ValueError):
            url_host("http://public.example:99999/")


class TestPublicAddressRefusal:
    @pytest.mark.parametrize(
        "address",
        ["100.64.0.0", "100.100.100.100", "100.127.255.255", "::ffff:100.64.0.1"],
    )
    def test_cgnat_refused(self, address):
        assert public_address_refusal(ipaddress.ip_address(address)) is not None

    @pytest.mark.parametrize("address", ["100.63.255.255", "100.128.0.0"])
    def test_just_outside_cgnat_passes(self, address):
        assert public_address_refusal(ipaddress.ip_address(address)) is None


def test_system_resolver_returns_every_answer(monkeypatch):
    calls = []

    def fake_getaddrinfo(host, port, *args, **kwargs):
        calls.append((host, port, kwargs))
        return [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", port)),
            (socket.AF_INET6, socket.SOCK_STREAM, 6, "", ("::1", port, 0, 0)),
        ]

    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)

    assert system_resolver("rebind.example", 443) == ["93.184.216.34", "::1"]
    with pytest.raises(UnsafeURLError, match="rebind.example resolves"):
        check_public_http_url("https://rebind.example/")
    assert calls[0][:2] == ("rebind.example", 443)

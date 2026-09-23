"""Presentation-boundary sanitizer — audit finding OC-05.

Two properties matter and they pull against each other. It must catch the
credential shapes a worker could copy into evidence or a routed question; and it
must NOT erase the identifiers an officer navigates by, because evidence he
cannot correlate is evidence he cannot use. The false-positive cases here are
as load-bearing as the true-positive ones.
"""

import time

import pytest

from shared.content_redaction import (
    REDACTED,
    sanitize,
    sanitize_text,
    sanitize_tool_output,
)

# Synthetic, never a real credential. 40 hex chars: the shape of a Gitea
# access token, and exactly the shape of a commit SHA.
GITEA_TOKEN = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b"
GIT_REMOTE_V = (
    f"origin\thttps://oauth2:{GITEA_TOKEN}@gitea.local/org/repo.git (fetch)\n"
    f"origin\thttps://oauth2:{GITEA_TOKEN}@gitea.local/org/repo.git (push)\n"
)

BOTH_PROFILES = pytest.mark.parametrize(
    "redact", [sanitize, sanitize_tool_output], ids=["presentation", "tool"]
)


class TestSecretShapes:
    def test_bearer_tokens(self):
        r = sanitize("curl -H 'Authorization: Bearer abc123XYZ_-token'")
        assert "abc123XYZ_-token" not in r.text
        assert r.redacted

    def test_jwt(self):
        jwt = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NSJ9.dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1"
        assert jwt not in sanitize(f"token={jwt}").text

    def test_provider_key_prefixes(self):
        for secret in (
            "sk-abcdefghijklmnopqrstuvwxyz012345",
            "xoxb-1234567890-abcdefghij",
            "ghp_abcdefghijklmnopqrstuvwxyz0123",
            "AKIAIOSFODNN7EXAMPLE",
            "AIza" + "b" * 35,
        ):
            out = sanitize(f"the worker printed {secret} to stdout").text
            assert secret not in out, secret

    def test_key_value_forms(self):
        text = "password: hunter2\napi_key=deadbeefcafe\nclient_secret: 'shhh-9999'"
        out = sanitize(text).text
        assert "hunter2" not in out
        assert "deadbeefcafe" not in out
        assert "shhh-9999" not in out
        # The key NAME survives so the reader can see what was withheld.
        assert "password" in out and "api_key" in out

    def test_url_userinfo_keeps_the_host(self):
        # Which remote failed is diagnostic; the credential is not.
        out = sanitize(
            "fatal: could not read https://bob:s3cr3t@gitea.local/x.git"
        ).text
        assert "s3cr3t" not in out
        assert "gitea.local/x.git" in out

    def test_scp_style_remote(self):
        out = sanitize("git@host: deploy:pa55word@gitea.local:org/repo.git").text
        assert "pa55word" not in out

    def test_whole_pem_block_is_one_redaction(self):
        pem = (
            "-----BEGIN RSA PRIVATE KEY-----\n"
            "MIIEowIBAAKCAQEA1234567890abcdef\n"
            "ZZZZmoreBase64Here0000000000000\n"
            "-----END RSA PRIVATE KEY-----"
        )
        r = sanitize(f"leaked:\n{pem}\ndone")
        assert "MIIEowIBAAKCAQEA" not in r.text
        assert "ZZZZmoreBase64Here" not in r.text
        assert r.count == 1  # not header-plus-surviving-body
        assert "done" in r.text

    def test_a_lone_pem_header_still_redacts(self):
        # Diagnostics often echo one line of a key rather than the whole block.
        assert (
            "BEGIN OPENSSH PRIVATE KEY"
            not in sanitize(
                "error near -----BEGIN OPENSSH PRIVATE KEY----- while parsing"
            ).text
        )


class TestKnownRuntimeSecrets:
    def test_exact_value(self):
        token = "workspace-token-abcdef123456"
        out = sanitize(f"cloned with {token}", secrets=[token]).text
        assert token not in out

    def test_url_encoded_form(self):
        secret = "p@ss/word+value"
        text = "https://x/?t=p%40ss%2Fword%2Bvalue"
        assert "p%40ss" not in sanitize(text, secrets=[secret]).text

    def test_a_short_secret_is_ignored_rather_than_shredding_the_text(self):
        # An 8-char floor: erasing every occurrence of a 3-char "secret" would
        # destroy the surrounding prose and tell the reader nothing.
        out = sanitize("the cat sat on the mat", secrets=["cat"]).text
        assert out == "the cat sat on the mat"

    def test_longest_variant_wins(self):
        # Redacting a prefix first would strand the tail as an orphan fragment.
        out = sanitize(
            "value=abcdef123456789", secrets=["abcdef12", "abcdef123456789"]
        ).text
        assert "abcdef" not in out


class TestFalsePositives:
    """What must SURVIVE. An officer correlates evidence by these."""

    def test_ids_hashes_and_paths_survive(self):
        text = (
            "job 1ad5d2a0-8a67-418c-947d-b2112292f230 commit 9e4c8d63a1 "
            "sha256:3112549f0613c96a8012cea6a1f06f4b6249ec43c7b1c4a70161ba27b18af37e "
            "wrote projects/resavio/report.md (4051 bytes)"
        )
        r = sanitize(text)
        assert r.text == text
        assert not r.redacted

    def test_ordinary_prose_is_untouched(self):
        text = "The deploy failed because the health check timed out after 30s."
        assert sanitize(text).text == text

    def test_the_word_token_alone_is_not_a_secret(self):
        # Only key=value shapes trigger; a sentence mentioning tokens does not.
        text = "The token budget was exceeded twice this week."
        assert sanitize(text).text == text


class TestReporting:
    def test_count_lets_a_surface_say_something_was_withheld(self):
        r = sanitize("a=1 password: one api_key=two")
        assert r.count == 2 and r.redacted

    def test_clean_text_reports_zero(self):
        r = sanitize("nothing to see")
        assert r.count == 0 and not r.redacted

    def test_empty_and_none_are_safe(self):
        assert sanitize(None).text == "" and not sanitize(None).redacted
        assert sanitize("").count == 0
        assert sanitize_text(None) == ""

    def test_sanitize_text_is_the_no_report_convenience(self):
        assert REDACTED in sanitize_text("password: hunter2")


class TestCredentialBearingRemotes:
    """workspace_git_credentials_in_tool_and_audit_output: `git remote -v`
    against a token-authenticated clone printed both URLs into tool output."""

    @BOTH_PROFILES
    def test_git_remote_v_keeps_the_remote_and_loses_the_token(self, redact):
        r = redact(GIT_REMOTE_V)
        assert GITEA_TOKEN not in r.text
        assert "oauth2" not in r.text  # the userinfo goes whole
        assert r.text.count("gitea.local/org/repo.git") == 2
        assert "(fetch)" in r.text and "(push)" in r.text
        assert r.count == 2

    @BOTH_PROFILES
    def test_push_failure_diagnostic_stays_readable(self, redact):
        text = (
            "remote: Invalid username or password.\n"
            "fatal: Authentication failed for "
            "'https://x-access-token:ghs_16CharsOfSyntheticToken@github.com/o/r.git/'"
        )
        out = redact(text).text
        assert "ghs_16CharsOfSyntheticToken" not in out
        assert "Authentication failed for" in out
        assert "github.com/o/r.git/" in out

    @BOTH_PROFILES
    def test_password_only_userinfo(self, redact):
        out = redact("REDIS_URL=redis://:s3cretPass9xQ7vL2mN@cache.local:6379/0").text
        assert "s3cretPass9xQ7vL2mN" not in out
        assert "cache.local:6379/0" in out

    def test_a_dev_default_password_is_a_credential_only_for_a_reader(self):
        # A compose file or .env.example the agent edits is full of these. The
        # tool profile asks for a generated-looking value (letters AND digits,
        # 16+); a reader-facing surface withholds any password.
        text = (
            "DATABASE_URL=postgres://postgres:postgres@localhost:5432/app\n"
            "REDIS_URL=redis://:devpassword@localhost:6379/0\n"
            "ftp://anonymous:guest@ftp.example.com/pub\n"
            "k3d cluster create -p 127.0.0.1:8443:30443@server:0\n"
        )
        assert sanitize_tool_output(text).text == text
        presented = sanitize(text).text
        assert "postgres:postgres@" not in presented
        assert "devpassword" not in presented

    @BOTH_PROFILES
    def test_percent_encoded_userinfo(self, redact):
        out = redact("https://bot:p%40ss%2Fw0rd%3Aend@gitea.local/x.git").text
        assert "p%40ss" not in out and "w0rd" not in out
        assert "gitea.local/x.git" in out

    @BOTH_PROFILES
    def test_url_query_secrets(self, redact):
        for secret_url, value in (
            ("https://h/api?access_token=abc123DEFghi456JKL&page=2", "abc123DEF"),
            (
                "https://s3.local/b/k?X-Amz-Signature=deadbeef0042deadbeef0042"
                "&X-Amz-Date=1",
                "deadbeef0042",
            ),
            (
                "https://idp/cb#id_token=opaqueValue77opaqueValue&state=xyz",
                "opaqueValue77",
            ),
            (
                "https://gitlab.local/api/v4?private_token=glSynthetic99glSynthetic",
                "glSynthetic99",
            ),
            (
                "https://acct.blob.core.windows.net/c?sv=2020&sig=abcdEFGH%2Bxyz%3D0k",
                "abcdEFGH",
            ),
        ):
            out = redact(secret_url).text
            assert value not in out, secret_url
        out = redact("https://h/api?access_token=abc123DEFghi456JKL&page=2").text
        assert out == f"https://h/api?access_token={REDACTED}&page=2"

    def test_a_doc_example_query_value_survives_the_tool_profile(self):
        text = "https://maps.local/js?key=MYKEY&callback=init\nhttps://x/?sig=1\n"
        assert sanitize_tool_output(text).text == text

    @BOTH_PROFILES
    def test_a_markdown_link_keeps_its_parenthesis(self, redact):
        out = redact("see [the run](https://h/r?token=abc123def456ghi789) now").text
        assert out == f"see [the run](https://h/r?token={REDACTED}) now"

    @BOTH_PROFILES
    def test_known_bare_token_is_matched_literally(self, redact):
        # A 40-hex token outside a URL is indistinguishable from a SHA; only
        # the known value can catch it.
        text = f"GITEA_TOKEN={GITEA_TOKEN}"
        assert GITEA_TOKEN in redact(text).text
        assert GITEA_TOKEN not in redact(text, secrets=[GITEA_TOKEN]).text

    @BOTH_PROFILES
    def test_a_known_pair_catches_its_basic_auth_header(self, redact):
        # `curl -v -u oauth2:$TOK` / GIT_CURL_VERBOSE print the pair base64'd.
        import base64

        header = base64.b64encode(f"oauth2:{GITEA_TOKEN}".encode()).decode()
        text = f"> Authorization: Basic {header}\n"
        out = redact(text, secrets=[GITEA_TOKEN, f"oauth2:{GITEA_TOKEN}"]).text
        assert header not in out and header.rstrip("=") not in out
        assert out.startswith("> Authorization: ")

    @BOTH_PROFILES
    def test_an_unknown_auth_header_credential_goes(self, redact):
        for header in (
            f"Authorization: token {GITEA_TOKEN}",
            "Authorization: Basic b2F1dGgyOmQzNGRiMzNmY2FmZTAxMjM0NTY3ODlh",
        ):
            out = redact(f"-H '{header}'").text
            assert header.split()[-1] not in out, header


class TestSplitOutput:
    """A read can end mid-line (a pane still printing), cutting the URL
    before the `@` the full pattern anchors on."""

    @BOTH_PROFILES
    def test_url_cut_off_before_its_at_sign(self, redact):
        partial = f"origin\thttps://oauth2:{GITEA_TOKEN[:23]}"
        out = redact(partial).text
        assert GITEA_TOKEN[:23] not in out
        assert out.startswith("origin\thttps://")

    @BOTH_PROFILES
    def test_a_trailing_port_is_not_a_cut_credential(self, redact):
        text = "  Local:   http://localhost:4200\n"
        assert redact(text).text == text

    def test_no_head_of_the_remote_line_leaks_a_token_prefix(self):
        # However a still-printing line is cut, the read must not carry a
        # prefix of the token that a later read would complete. The tool
        # profile needs 8 characters to tell a token's head from a port or a
        # word, so a shorter head is the accepted residue.
        line = GIT_REMOTE_V.splitlines()[0]
        token_at = line.index(GITEA_TOKEN)
        for cut in range(token_at + 8, token_at + len(GITEA_TOKEN)):
            prefix = line[token_at:cut]
            head = sanitize_tool_output(line[:cut]).text
            assert prefix not in head, cut

    @BOTH_PROFILES
    def test_a_host_and_port_at_the_end_is_not_a_cut_credential(self, redact):
        for text in (
            'url: "nats://nats:4222"',
            "curl http://[::1]:8080",
            "export OTEL=http://collector:4317\n",
        ):
            assert redact(text).text == text, text


class TestToolOutputProfile:
    """What the model's transcript must keep. The agent edits files from what
    it read, so a false positive writes the marker into the file."""

    def test_source_code_that_names_secrets_survives(self):
        code = (
            "def check(token: str, password: str) -> bool:\n"
            "    api_key = os.environ['API_KEY']\n"
            '    headers = {"Authorization": f"Bearer {token}"}\n'
            "    secret = settings.SECRET\n"
        )
        r = sanitize_tool_output(code)
        assert r.text == code and not r.redacted
        # The presentation profile would have shredded it — the reason there
        # are two profiles.
        assert sanitize(code).redacted

    def test_prose_about_bearer_tokens_survives(self):
        doc = "Send a Bearer token in the Authorization header."
        assert sanitize_tool_output(doc).text == doc

    def test_a_credential_after_bearer_still_goes(self):
        out = sanitize_tool_output(
            "> Authorization: Bearer 0123456789abcdefABCDEF0123456789\n"
        ).text
        assert "0123456789abcdefABCDEF" not in out

    def test_a_pem_literal_in_code_survives_but_a_key_does_not(self):
        literal = 'if text.startswith("-----BEGIN RSA PRIVATE KEY-----"):'
        assert sanitize_tool_output(literal).text == literal
        key = (
            "-----BEGIN OPENSSH PRIVATE KEY-----\n"
            "b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQ\n"
            "-----END OPENSSH PRIVATE KEY-----"
        )
        assert "b3BlbnNzaC1rZXkt" not in sanitize_tool_output(key).text

    def test_prefixed_provider_tokens_still_go(self):
        for secret in (
            "sk-abcdefghijklmnopqrstuvwxyz012345",
            "ghp_abcdefghijklmnopqrstuvwxyz0123",
            "github_pat_11ABCDEFG0123456789_abcdefghijklmnop",
            "glpat-abcdefghij0123456789",
            "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NSJ9.dBjftJeZ4CVP-mB92K27uhbUJU1p",
        ):
            assert secret not in sanitize_tool_output(f"x {secret} y").text, secret

    @BOTH_PROFILES
    def test_repository_coordinates_survive(self, redact):
        text = (
            "https://gitea.local/org/repo.git\n"
            "git@github.com:org/repo.git\n"
            "ssh://git@gitea.local:2222/org/repo.git\n"
            "ssh://srw-repo-main/org/repo.git\n"
            "https://github.com/o/r/pull/12?tab=files#diff-9e4c8d63a1\n"
            "https://pypi.org/simple/?tokenizer=bert&sort=key\n"
        )
        r = redact(text)
        assert r.text == text and not r.redacted

    @BOTH_PROFILES
    def test_hashes_uuids_and_base64_survive(self, redact):
        text = (
            f"commit {GITEA_TOKEN}\n"
            "job 1ad5d2a0-8a67-418c-947d-b2112292f230\n"
            "sha256:3112549f0613c96a8012cea6a1f06f4b6249ec43c7b1c4a70161ba27b18af37e\n"
            "data: SGVsbG8sIFdvcmxkIQ==/+QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVo=\n"
        )
        r = redact(text)
        assert r.text == text and not r.redacted

    @BOTH_PROFILES
    def test_template_placeholders_are_not_secrets(self, redact):
        text = (
            "DATABASE_URL=postgresql://${PGUSER}:${PGPASSWORD}@db:5432/app\n"
            "clone https://oauth2:$GITEA_TOKEN@gitea.local/org/repo.git\n"
            "url: https://srw:{{.Values.pw}}@srw-postgres:5432/srw\n"
            "masked: https://bob:***@gitea.local/x.git\n"
            "docs: https://api.local/v1?token=YOUR_TOKEN&key={key}\n"
            'dsn = f"postgresql+asyncpg://{s.user}:{s.password}@{s.host}/{s.db}"\n'
            '"https://{}:{}@{}".format(user, tok, host)\n'
            'dsn = "postgresql://%s:%s@%s/%s" % (u, p, h, d)\n'
            'dsn = "postgresql://%(user)s:%(password)s@db/app" % cfg\n'
            "curl https://bot:$(cat /run/secrets/tok)@gitea.local/x.git\n"
            "git clone https://oauth2:$pass@gitea.local/x.git\n"
            "url=https://app:${DB_PASS:-changeme}@db/app\n"
        )
        r = redact(text)
        assert r.text == text and not r.redacted

    @BOTH_PROFILES
    def test_an_image_digest_is_not_a_password(self, redact):
        text = (
            "FROM python:3.12-slim@sha256:0123456789abcdef0123456789abcdef"
            "0123456789abcdef0123456789abcdef\n"
            "docker pull redis:7@sha256:abc\n"
        )
        assert redact(text).text == text

    def test_code_between_pem_marker_literals_is_not_a_key(self):
        # The shape of a key-parsing module (shared/runtime/utils/ssh_key.py):
        # BEGIN and END literals with code, not base64, between them.
        code = (
            "_BEGIN_MARKERS = (\n"
            '    "-----BEGIN OPENSSH PRIVATE KEY-----",\n'
            '    "-----BEGIN RSA PRIVATE KEY-----",\n'
            ")\n"
            "_END_MARKERS = (\n"
            '    "-----END OPENSSH PRIVATE KEY-----",\n'
            ")\n"
        )
        assert sanitize_tool_output(code).text == code

    @BOTH_PROFILES
    def test_an_encrypted_key_with_pem_headers_still_goes(self, redact):
        key = (
            "-----BEGIN RSA PRIVATE KEY-----\n"
            "Proc-Type: 4,ENCRYPTED\n"
            "DEK-Info: AES-128-CBC,0123456789ABCDEF0123456789ABCDEF\n"
            "\n"
            "MIIEowIBAAKCAQEA1234567890abcdefZZZZmoreBase64Here000000000\n"
            "-----END RSA PRIVATE KEY-----"
        )
        r = redact(f"key:\n{key}\nafter")
        assert "MIIEowIBAAKCAQEA" not in r.text and "DEK-Info" not in r.text
        assert r.text.endswith("after")

    def test_a_json_embedded_key_still_goes(self):
        text = (
            '{"key": "-----BEGIN PRIVATE KEY-----\\nMIIEvQIBADANBgkqhkiG9w0BAQEF'
            'AASC\\nBKcwggSjAgEAAoIBAQC7\\n-----END PRIVATE KEY-----\\n"}'
        )
        assert "MIIEvQIBADANBgkq" not in sanitize_tool_output(text).text

    def test_ssh_key_types_and_model_names_are_not_api_keys(self):
        text = (
            "sk-ecdsa-sha2-nistp256@openssh.com AAAAInNrLWVjZHNh user@host\n"
            'MODEL = "sk-learn-regression-model-v1"\n'
        )
        assert sanitize_tool_output(text).text == text

    def test_short_scp_shaped_text_survives(self):
        text = (
            "foo.bar:baz@qux.com:22\nuser.name:secret@host.com/path\n10:30:00@host:\n"
        )
        assert sanitize_tool_output(text).text == text

    def test_a_token_shaped_scp_password_still_goes(self):
        out = sanitize_tool_output(f"oauth2:{GITEA_TOKEN}@gitea.local:org/repo.git")
        assert GITEA_TOKEN not in out.text


class TestIdempotence:
    """Layered surfaces sanitize twice (a tool result later archived, an
    escalation body later recorded as a notification). The second pass must
    change nothing and count nothing."""

    @BOTH_PROFILES
    def test_a_second_pass_counts_nothing(self, redact):
        text = (
            GIT_REMOTE_V
            + "password: hunter2\nAuthorization: Bearer abc123XYZ_-token\n"
            + "https://h/?access_token=abc123\n"
            + "curl -H 'Authorization: Bearer 0123456789abcdefABCDEF0123456789'\n"
        )
        once = redact(text)
        twice = redact(once.text)
        assert once.redacted
        assert twice.text == once.text
        assert twice.count == 0


class TestLinearOnHostileInput:
    """Tool output can carry a fetched web page. Each of these was quadratic
    (seconds per 50 KB) before the patterns were bounded."""

    @pytest.mark.parametrize(
        "payload",
        [
            "a+" * 25_000,
            "eyJ-" * 12_500,
            "a." * 25_000,
            "x://" + ":" * 50_000,
            "x://" + "a:" * 25_000,
            "-----BEGIN RSA PRIVATE KEY-----" * 1_600,
            "?token" * 8_000,
        ],
        ids=["scheme", "jwt", "scp", "colons", "user-colons", "pem", "query"],
    )
    @BOTH_PROFILES
    def test_bounded_time(self, redact, payload):
        started = time.perf_counter()
        redact(payload)
        assert time.perf_counter() - started < 0.5


def _best_of_three(fn, *args) -> float:
    """The fastest of three runs: a shared CI box adds noise, not asymptotics."""
    best = float("inf")
    for _ in range(3):
        started = time.perf_counter()
        fn(*args)
        best = min(best, time.perf_counter() - started)
    return best


class TestPlaceholderLookaheadsAreBounded:
    """The review's hostile inputs. An unbounded scan inside a lookahead that
    runs at every candidate position made each candidate cost the rest of the
    text: 4.9 s for 100 KB of `x://u:${`, 64 s for `_truncate_output` over a
    420 KB line of `a:<`. Each must now finish in under 100 ms."""

    @pytest.mark.parametrize(
        "unit", ["x://u:${", "x://u:<", "a:${", "a:<", "a:{{", "token=${", "?token=${"]
    )
    @BOTH_PROFILES
    def test_100kb_of_open_placeholders(self, redact, unit):
        payload = (unit * (100_000 // len(unit) + 1))[:100_000]
        assert _best_of_three(redact, payload) < 0.1

    @pytest.mark.parametrize(
        "payload",
        [
            # perf7: a header line of `A: ` runs, 20 blocks (4.6 KB) — was 50 s
            ("-----BEGIN RSA PRIVATE KEY-----\n" + "A: " * 67) * 20,
            # perf7: one 302-byte block — was 2.4 s
            "-----BEGIN RSA PRIVATE KEY-----\n" + "A: " * 90,
            # perf6: three such lines in one block
            "-----BEGIN RSA PRIVATE KEY-----" + ("\n" + "A: " * 67) * 3,
            # a user/password split at every colon, inside the tail window
            "x://" + "a:" * 1_200,
        ],
        ids=["pem-20-blocks", "pem-one-block", "pem-3-lines", "tail-colons"],
    )
    @BOTH_PROFILES
    def test_optional_separator_shapes(self, redact, payload):
        assert _best_of_three(redact, payload) < 0.1

    def test_truncating_a_huge_single_line(self):
        from agent.tools.shell.coding_tools import _truncate_output

        assert _best_of_three(_truncate_output, "a:<" * 140_000, 50_000) < 0.1

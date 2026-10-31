"""The MCP OAuth consent page escapes every caller-derived value (N3).

``client_name`` comes from unauthenticated Dynamic Client Registration, so
anyone could register a client whose name is markup and send a victim the
authorize link: the consent page rendered it raw on the MCP origin.
"""

from __future__ import annotations

from mcp_server.oauth_bridge import _render_consent

PAYLOAD = "<img src=x onerror=alert(1)>"


def _render(**overrides) -> str:
    values = {
        "session_id": "sess-1",
        "csrf_token": "csrf-1",
        "client_name": "Claude",
        "user_email": "user@example.com",
        "user_roles": [],
        "is_admin": False,
    }
    values.update(overrides)
    return _render_consent(**values)


class TestConsentEscaping:
    def test_client_name_markup_is_escaped(self):
        page = _render(client_name=PAYLOAD)

        assert "<img" not in page
        assert "&lt;img src=x onerror=alert(1)&gt;" in page

    def test_user_email_markup_is_escaped(self):
        page = _render(user_email=PAYLOAD)

        assert "<img" not in page
        assert page.count("&lt;img src=x onerror=alert(1)&gt;") == 2

    def test_attribute_values_cannot_break_out(self):
        page = _render(session_id='x" autofocus onfocus="alert(1)', csrf_token="'><b>")

        assert 'value="x&quot; autofocus onfocus=&quot;alert(1)"' in page
        assert 'value="&#x27;&gt;&lt;b&gt;"' in page
        assert "<b>" not in page

    def test_placeholder_inside_a_value_is_not_expanded(self):
        page = _render(client_name="{{admin_option}}", is_admin=True)

        assert "<h1>{{admin_option}}</h1>" in page
        assert page.count('<option value="all">Full Access (Admin)</option>') == 1

    def test_admin_option_stays_markup(self):
        assert '<option value="all">Full Access (Admin)</option>' in _render(
            is_admin=True
        )
        assert 'value="all"' not in _render(is_admin=False)

    def test_every_placeholder_is_filled(self):
        assert "{{" not in _render()

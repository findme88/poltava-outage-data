import asyncio
import json
import unittest
from datetime import datetime
from unittest.mock import AsyncMock, patch
from urllib.parse import parse_qs

import httpx

from poe_client import PoescheduleClient, QUEUE_NAMES


class ScheduleClientTests(unittest.IsolatedAsyncioTestCase):
    def new_schedule_block(self, day, changed=False):
        rows = []
        for queue in QUEUE_NAMES:
            cells = []
            for slot in range(48):
                start, end = slot * 30, (slot + 1) * 30
                state = "gpv-s2" if queue == "5.1" and slot == 24 else "gpv-s3" if slot == 25 else "gpv-s1"
                if changed and queue == "5.1" and slot == 26:
                    state = "gpv-s2"
                title = f"{queue}: {start // 60:02d}:{start % 60:02d}–{end // 60:02d}:{end % 60:02d}"
                cells.append(f'<td title="{title}" class="gpv-c {state} gpv-h"></td>')
            rows.append('<tr>' + ''.join(cells) + '</tr>')
        return (f'<div class="gpvinfodetail">На <b>{day} жовтня 2026 року</b>'
                f'<table class="gpv-table"><tbody>{"".join(rows)}</tbody></table></div>')

    async def test_new_format_keeps_separate_dates_and_yellow_slots(self):
        client = PoescheduleClient()
        client._fetch_html = AsyncMock(return_value=self.new_schedule_block(27) + self.new_schedule_block(28, changed=True))
        schedules, visuals, cancellations = await client.fetch_available_schedule_bundle(
            datetime(2026, 10, 27), {"2026-10-27", "2026-10-28"})
        self.assertEqual(set(schedules), {"2026-10-27", "2026-10-28"})
        self.assertEqual(schedules["2026-10-27"]["5.1"][24:27], [False, True, True])
        self.assertEqual(schedules["2026-10-28"]["5.1"][24:27], [False, True, False])
        self.assertEqual(visuals["2026-10-27"]["5.1"][25], "light_3")
        self.assertTrue(all(len(slots) == 48 for day in schedules.values() for slots in day.values()))
        self.assertFalse(cancellations)

    async def test_direct_failure_uses_complete_relay_and_cancels_others(self):
        client = PoescheduleClient()
        body = b"seldate=test"
        complete = self.new_schedule_block(27)
        cancelled = asyncio.Event()
        client._discover_proxies = AsyncMock(return_value=["http://1.1.1.1:8080", "http://8.8.8.8:8080"])

        async def relay(url, request_body, headers, proxy):
            self.assertEqual(request_body, body)
            if proxy.endswith("1.1.1.1:8080"):
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelled.set()
            return proxy, complete

        client._fetch_via_proxy = AsyncMock(side_effect=relay)
        transport = httpx.MockTransport(lambda request: (_ for _ in ()).throw(httpx.ConnectTimeout("offline")))
        actual_client = httpx.AsyncClient
        with patch("poe_client.httpx.AsyncClient", side_effect=lambda **kw: actual_client(transport=transport, **kw)):
            html = await client._fetch_html(client.NEW_URL, body)
        self.assertEqual(html, complete)
        self.assertEqual(client._last_working_proxy, "http://8.8.8.8:8080")
        self.assertTrue(cancelled.is_set())

    async def test_proxy_directory_rejects_private_hosts_and_invalid_ports(self):
        client = PoescheduleClient()
        entries = [
            {"ip": "127.0.0.1", "port": 8080, "country_code": "UA", "protocols": ["https"]},
            {"ip": "10.0.0.1", "port": 8080, "country_code": "UA", "protocols": ["https"]},
            {"ip": "1.1.1.1", "port": 8080, "country_code": "UA", "protocols": ["https"]},
            {"ip": "8.8.8.8", "port": 0, "country_code": "UA", "protocols": ["https"]},
            {"ip": "8.8.8.8", "port": 8080, "country_code": "ZZ", "protocols": ["https"]},
        ]
        transport = httpx.MockTransport(lambda request: httpx.Response(200, json=entries))
        actual_client = httpx.AsyncClient
        with patch("poe_client.httpx.AsyncClient", side_effect=lambda **kw: actual_client(transport=transport, **kw)):
            self.assertEqual(await client._discover_proxies(), ["http://1.1.1.1:8080"])

    async def test_relay_requires_complete_dated_schedule_with_tls_verification(self):
        client = PoescheduleClient()
        actual_client = httpx.AsyncClient
        options = []
        response_html = self.new_schedule_block(27)

        def make_client(**kwargs):
            options.append(kwargs)
            kwargs.pop("proxy", None)
            return actual_client(transport=httpx.MockTransport(lambda request: httpx.Response(200, text=response_html)), **kwargs)

        with patch("poe_client.httpx.AsyncClient", side_effect=make_client):
            _, html = await client._fetch_via_proxy(client.NEW_URL, b"data", {}, "http://1.1.1.1:8080")
            self.assertEqual(html, response_html)
            self.assertTrue(options[0]["verify"])
            self.assertFalse(options[0]["follow_redirects"])
            response_html = response_html.replace('gpv-c gpv-s1', 'gpv-c gpv-s9', 1)
            with self.assertRaises(ValueError):
                await client._fetch_via_proxy(client.NEW_URL, b"data", {}, "http://1.1.1.1:8080")

    def test_new_format_rejects_unknown_or_missing_slots(self):
        client = PoescheduleClient()
        good = self.new_schedule_block(27)
        for bad in (good.replace('gpv-c gpv-s1', 'gpv-c gpv-s9', 1),
                    good.replace('1.1: 00:00–00:30', '1.1: 00:30–01:00', 1),
                    good.replace('gpv-c gpv-s1', 'other gpv-s1', 1)):
            with self.subTest():
                with self.assertRaises(ValueError):
                    client._parse_schedule_html_bundle(bad)

    def schedule_block(self, day):
        slots = ''.join(f'<td class="{cls}"></td>' for cls in ["light_2", "light_3"] + ["light_1"] * 46)
        rows = ''.join(
            f'<tr><td class="turnoff-scheduleui-table-queue">{queue[0]}</td>'
            f'<td class="turnoff-scheduleui-table-subqueue">{queue[2]}</td>{slots}</tr>'
            for queue in QUEUE_NAMES)
        return (f'<div class="gpvinfodetail">На <b>{day} жовтня 2026 року</b>'
                f'<table class="turnoff-scheduleui-table">{rows}</table></div>')

    async def test_primary_resolves_both_dates_and_preserves_yellow_as_on(self):
        client = PoescheduleClient()
        client._fetch_html = AsyncMock(return_value=self.schedule_block(8) + self.schedule_block(9))
        schedules, visuals, cancellations = await client.fetch_available_schedule_bundle(
            datetime(2026, 10, 8), {"2026-10-08", "2026-10-09"})
        self.assertEqual(set(schedules), {"2026-10-08", "2026-10-09"})
        self.assertEqual(schedules["2026-10-08"]["5.1"][:2], [False, True])
        self.assertEqual(visuals["2026-10-08"]["5.1"][:2], ["light_2", "light_3"])
        self.assertFalse(cancellations)
        client._fetch_html.assert_awaited_once()

    async def test_entire_request_has_deadline_and_cancels_stalled_fetch(self):
        client = PoescheduleClient()
        client.REQUEST_TIMEOUT_SECONDS = 0.02
        cancelled = []

        async def stalled(*args):
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.append(True)

        client._fetch_html = stalled
        with self.assertRaises(TimeoutError):
            await asyncio.wait_for(client.fetch_available_schedule_bundle(datetime(2026, 10, 8)), 0.5)
        self.assertEqual(len(cancelled), 2)

    async def test_post_form_and_content_type_match_official_endpoint(self):
        captured = []

        def respond(req):
            captured.append(req)
            return httpx.Response(200, content="test response".encode())

        transport = httpx.MockTransport(respond)
        actual_client = httpx.AsyncClient
        with patch("poe_client.httpx.AsyncClient", side_effect=lambda **kwargs: actual_client(transport=transport, **kwargs)):
            result = await PoescheduleClient()._fetch_html(
                PoescheduleClient.NEW_URL, b"seldate=%7B%22date_in%22%3A%2208-10-2026%22%7D")
        self.assertEqual(result, "test response")
        self.assertEqual(captured[0].method, "POST")
        self.assertEqual(json.loads(parse_qs(captured[0].content.decode())["seldate"][0]), {"date_in": "08-10-2026"})
        self.assertIn("application/x-www-form-urlencoded", captured[0].headers["Content-Type"])


if __name__ == "__main__":
    unittest.main()

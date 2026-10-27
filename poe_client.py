"""Public, credential-free client for official Poltava outage schedules."""

import asyncio
import ipaddress
import json
import logging
import re
from datetime import date, datetime
from html.parser import HTMLParser
from typing import Dict, List, Optional, Set, Tuple
from urllib import parse

import httpx

logger = logging.getLogger(__name__)

QUEUE_NAMES = [
    "1.1", "1.2", "2.1", "2.2", "3.1", "3.2",
    "4.1", "4.2", "5.1", "5.2", "6.1", "6.2",
]


class _NewScheduleTableParser(HTMLParser):
    """Read the labeled half-hour cells in the official horizontal table."""

    STATE_CLASSES = {"gpv-s1": "light_1", "gpv-s2": "light_2", "gpv-s3": "light_3"}

    def __init__(self) -> None:
        super().__init__()
        self.in_table = False
        self.seen_table = False
        self.slots: Dict[str, Dict[int, str]] = {queue: {} for queue in QUEUE_NAMES}

    def handle_starttag(self, tag: str, attrs: list) -> None:
        attributes = dict(attrs)
        classes = set(attributes.get("class", "").split())
        if tag == "table" and "gpv-table" in classes and not self.seen_table:
            self.in_table = True
            self.seen_table = True
        if not self.in_table or tag != "td" or "gpv-c" not in classes:
            return
        states = classes & self.STATE_CLASSES.keys()
        match = re.match(r"^([1-6]\.[12]): (\d{2}):(00|30)[\u2013-](\d{2}):(00|30)", attributes.get("title", ""))
        if len(states) != 1 or not match:
            raise ValueError("Invalid new schedule cell")
        queue, start_h, start_m, end_h, end_m = match.groups()
        start = int(start_h) * 60 + int(start_m)
        end = int(end_h) * 60 + int(end_m)
        if end == 0:
            end = 1440
        if start >= 1440 or end - start != 30 or start // 30 in self.slots[queue]:
            raise ValueError("Invalid or duplicate schedule time")
        self.slots[queue][start // 30] = self.STATE_CLASSES[next(iter(states))]

    def handle_endtag(self, tag: str) -> None:
        if tag == "table" and self.in_table:
            self.in_table = False

    def result(self) -> Dict[str, List[str]]:
        if not self.seen_table or any(set(slots) != set(range(48)) for slots in self.slots.values()):
            raise ValueError("New schedule must contain all queues and half-hour slots")
        return {queue: [slots[index] for index in range(48)] for queue, slots in self.slots.items()}

class PoescheduleClient:
    BASE = "https://www.poe.pl.ua"
    NEW_URL = BASE + "/customs/newgpv-info.php"
    OLD_URL = BASE + "/customs/oldgpv-info.php"
    REQUEST_TIMEOUT_SECONDS = 35
    PROXY_DIRECTORY_URL = "https://hproxy.com/api/proxy-list?format=json&country=ua&protocol=https&limit=12"

    def __init__(self) -> None:
        self._proxy_cache: List[str] = []
        self._proxy_cache_until = 0.0
        self._last_working_proxy: Optional[str] = None

    @staticmethod
    def _has_schedule_table(html: str) -> bool:
        return "turnoff-scheduleui-table" in html or bool(
            re.search(r'<table\b[^>]*class=["\'][^"\']*\bgpv-table\b', html, re.IGNORECASE)
        )

    MONTHS_UA = {
        "\u0441\u0456\u0447\u043d\u044f": 1,
        "\u043b\u044e\u0442\u043e\u0433\u043e": 2,
        "\u0431\u0435\u0440\u0435\u0437\u043d\u044f": 3,
        "\u043a\u0432\u0456\u0442\u043d\u044f": 4,
        "\u0442\u0440\u0430\u0432\u043d\u044f": 5,
        "\u0447\u0435\u0440\u0432\u043d\u044f": 6,
        "\u043b\u0438\u043f\u043d\u044f": 7,
        "\u0441\u0435\u0440\u043f\u043d\u044f": 8,
        "\u0432\u0435\u0440\u0435\u0441\u043d\u044f": 9,
        "\u0436\u043e\u0432\u0442\u043d\u044f": 10,
        "\u043b\u0438\u0441\u0442\u043e\u043f\u0430\u0434\u0430": 11,
        "\u0433\u0440\u0443\u0434\u043d\u044f": 12,
    }

    @staticmethod
    def _strip_tags(text: str) -> str:
        text = re.sub(r"<[^>]+>", "", text)
        text = text.replace("&nbsp;", " ").strip()
        return text

    @staticmethod
    def _half_hour_to_str(idx: int) -> str:
        hh = (idx // 2) % 24
        mm = "30" if idx % 2 else "00"
        return f"{hh:02d}:{mm}"

    def _endpoint_for_date(self, dt: datetime) -> str:
        # Перехід ендпоінта: після 15.12.2024 працює newgpv-info.php
        pivot = datetime(2024, 12, 15).date()
        return self.NEW_URL if dt.date() > pivot else self.OLD_URL

    def _extract_effective_date(self, html: str) -> Optional[date]:
        match = re.search(
            r"(\d{1,2})\s+([\u0400-\u04FF]+)\s+(\d{4})\s+\u0440\u043e\u043a\u0443",
            html,
            flags=re.IGNORECASE,
        )
        if not match:
            return None

        day = int(match.group(1))
        month_word = match.group(2).lower()
        month = self.MONTHS_UA.get(month_word)
        year = int(match.group(3))
        if month is None:
            return None

        try:
            return date(year, month, day)
        except ValueError:
            return None

    @staticmethod
    def _extract_detail_blocks(html: str) -> List[str]:
        marker = '<div class="gpvinfodetail">'
        parts = html.split(marker)
        if len(parts) <= 1:
            return [html]
        return [marker + part for part in parts[1:]]

    def _extract_no_schedule_dates(self, html: str, tracked_dates: Optional[Set[str]] = None) -> Set[str]:
        result: Set[str] = set()
        for block_html in self._extract_detail_blocks(html):
            normalized = self._strip_tags(block_html).lower()
            if "не прогнозується" not in normalized:
                continue

            effective_date = self._extract_effective_date(block_html)
            if effective_date is None:
                continue

            day_iso = effective_date.isoformat()
            if tracked_dates is not None and day_iso not in tracked_dates:
                continue
            result.add(day_iso)

        return result

    async def _fetch_html(self, url: str, body: bytes) -> str:
        headers = {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8", "User-Agent": "Mozilla/5.0"}
        try:
            async with httpx.AsyncClient(timeout=8, follow_redirects=True) as client:
                response = await client.post(url, content=body, headers=headers)
                response.raise_for_status()
                return response.content.decode("utf-8", errors="ignore")
        except httpx.HTTPError as direct_error:
            candidates = await self._discover_proxies()
            if self._last_working_proxy in candidates:
                candidates.remove(self._last_working_proxy)
                candidates.insert(0, self._last_working_proxy)
            for offset in range(0, min(len(candidates), 6), 3):
                tasks = [asyncio.create_task(self._fetch_via_proxy(url, body, headers, proxy))
                         for proxy in candidates[offset:offset + 3]]
                try:
                    for task in asyncio.as_completed(tasks):
                        try:
                            proxy, html = await task
                        except (httpx.HTTPError, ValueError, asyncio.TimeoutError):
                            continue
                        self._last_working_proxy = proxy
                        logger.info("Official schedule fetched through a verified TLS relay")
                        return html
                finally:
                    for task in tasks:
                        if not task.done():
                            task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
            raise direct_error

    async def _discover_proxies(self) -> List[str]:
        now = asyncio.get_running_loop().time()
        if now < self._proxy_cache_until:
            return self._proxy_cache[:]
        try:
            async with httpx.AsyncClient(timeout=5, follow_redirects=True, trust_env=False) as client:
                response = await client.get(self.PROXY_DIRECTORY_URL)
                response.raise_for_status()
                entries = response.json()
            if not isinstance(entries, list):
                raise ValueError("Invalid proxy directory response")
            candidates = []
            for entry in entries[:20]:
                try:
                    if not isinstance(entry, dict) or entry.get("country_code") != "UA":
                        continue
                    ip = ipaddress.ip_address(entry.get("ip", ""))
                    port = entry.get("port")
                    if not ip.is_global or type(port) is not int or not 1 <= port <= 65535:
                        continue
                    if "https" not in entry.get("protocols", []):
                        continue
                    host = f"[{ip}]" if ip.version == 6 else str(ip)
                    candidate = f"http://{host}:{port}"
                    if candidate not in candidates:
                        candidates.append(candidate)
                except (ValueError, TypeError):
                    continue
            if candidates:
                self._proxy_cache = candidates
        except (httpx.HTTPError, ValueError):
            logger.warning("Could not refresh the public relay directory")
        self._proxy_cache_until = now + (600 if self._proxy_cache else 60)
        return self._proxy_cache[:]

    async def _fetch_via_proxy(self, url: str, body: bytes, headers: Dict[str, str], proxy: str) -> Tuple[str, str]:
        async with httpx.AsyncClient(proxy=proxy, timeout=10, follow_redirects=False, trust_env=False, verify=True) as client:
            async with client.stream("POST", url, content=body, headers=headers) as response:
                response.raise_for_status()
                data = bytearray()
                async for chunk in response.aiter_bytes():
                    data.extend(chunk)
                    if len(data) > 1_000_000:
                        raise ValueError("Official response is too large")
        html = data.decode("utf-8", errors="strict")
        for block in self._extract_detail_blocks(html):
            if self._extract_effective_date(block) is None:
                continue
            if self._has_schedule_table(block):
                try:
                    self._parse_schedule_html_bundle(block)
                except RuntimeError as error:
                    raise ValueError("Relay returned an incomplete schedule") from error
                return proxy, html
            if self._extract_no_schedule_dates(block):
                return proxy, html
        raise ValueError("Relay did not return a complete official schedule")

    async def fetch_available_schedule_bundle(
        self,
        day: datetime,
        tracked_dates: Optional[Set[str]] = None,
    ) -> Tuple[Dict[str, Dict[str, List[bool]]], Dict[str, Dict[str, List[str]]], Set[str]]:
        date_in = day.strftime("%d-%m-%Y")
        payload = {"seldate": json.dumps({"date_in": date_in}, ensure_ascii=False)}
        body = parse.urlencode(payload).encode("utf-8")

        primary = self._endpoint_for_date(day)
        secondary = self.OLD_URL if primary == self.NEW_URL else self.NEW_URL
        endpoints = [primary, secondary]

        last_error: Optional[Exception] = None
        by_date: Dict[str, Dict[str, List[bool]]] = {}
        visual_by_date: Dict[str, Dict[str, List[str]]] = {}
        no_schedule_dates: Set[str] = set()
        resolved_dates: Set[str] = set()

        for url in endpoints:
            endpoint_by_date: Dict[str, Dict[str, List[bool]]] = {}
            endpoint_visual_by_date: Dict[str, Dict[str, List[str]]] = {}
            endpoint_no_schedule_dates: Set[str] = set()
            try:
                # Bound the entire request, including DNS and a slow response body.
                html = await asyncio.wait_for(self._fetch_html(url, body), timeout=self.REQUEST_TIMEOUT_SECONDS)

                endpoint_no_schedule_dates.update(self._extract_no_schedule_dates(html, tracked_dates))

                if not self._has_schedule_table(html):
                    if not endpoint_no_schedule_dates:
                        first = html[:250].replace("\n", " ")
                        raise RuntimeError(f"Сторінка графіка не знайдена у відповіді: {first}")
                else:
                    parsed_any = False
                    for block_html in self._extract_detail_blocks(html):
                        if not self._has_schedule_table(block_html):
                            continue
                        try:
                            schedule, visual_schedule = self._parse_schedule_html_bundle(block_html)
                        except Exception as block_exc:
                            last_error = block_exc
                            continue

                        effective_date = self._extract_effective_date(block_html)
                        if effective_date is None:
                            continue

                        day_key = effective_date.isoformat()
                        if tracked_dates is not None and day_key not in tracked_dates:
                            continue
                        if day_key in endpoint_no_schedule_dates:
                            continue
                        if day_key not in endpoint_by_date:
                            endpoint_by_date[day_key] = schedule
                            endpoint_visual_by_date[day_key] = visual_schedule
                        parsed_any = True

                    if not parsed_any:
                        schedule, visual_schedule = self._parse_schedule_html_bundle(html)
                        effective_date = self._extract_effective_date(html)
                        if effective_date is not None:
                            day_key = effective_date.isoformat()
                            if tracked_dates is not None and day_key not in tracked_dates:
                                continue
                            if day_key not in endpoint_no_schedule_dates and day_key not in endpoint_by_date:
                                endpoint_by_date[day_key] = schedule
                                endpoint_visual_by_date[day_key] = visual_schedule
            except Exception as exc:
                last_error = exc
                continue

            if tracked_dates is None:
                candidate_dates = set(endpoint_by_date.keys()) | endpoint_no_schedule_dates
            else:
                candidate_dates = set(tracked_dates)

            for day_key in sorted(candidate_dates):
                if day_key in resolved_dates:
                    continue
                if day_key in endpoint_by_date:
                    by_date[day_key] = endpoint_by_date[day_key]
                    visual_by_date[day_key] = endpoint_visual_by_date[day_key]
                    resolved_dates.add(day_key)
                    continue
                if day_key in endpoint_no_schedule_dates:
                    no_schedule_dates.add(day_key)
                    resolved_dates.add(day_key)

            if tracked_dates is not None and tracked_dates <= resolved_dates:
                break

        if by_date or no_schedule_dates:
            return by_date, visual_by_date, no_schedule_dates
        if last_error is not None:
            raise last_error
        raise RuntimeError("Не вдалося отримати графік")

    async def fetch_available_schedules(self, day: datetime) -> Dict[str, Dict[str, List[bool]]]:
        by_date, _, _ = await self.fetch_available_schedule_bundle(day)
        return by_date

    async def fetch_day_schedule(self, day: datetime) -> Tuple[Dict[str, List[bool]], Optional[date]]:
        by_date = await self.fetch_available_schedules(day)
        requested_iso = day.date().isoformat()
        if requested_iso in by_date:
            return by_date[requested_iso], day.date()

        first_key, first_schedule = next(iter(by_date.items()))
        return first_schedule, date.fromisoformat(first_key)

    @staticmethod
    def _normalize_visual_class(css_class: str) -> str:
        cls = css_class.lower()
        return cls if cls in {"light_1", "light_2", "light_3"} else "light_1"

    def _parse_schedule_html_bundle(self, html: str) -> Tuple[Dict[str, List[bool]], Dict[str, List[str]]]:
        if re.search(r'<table\b[^>]*class=["\'][^"\']*\bgpv-table\b', html, re.IGNORECASE):
            parser = _NewScheduleTableParser()
            parser.feed(html)
            visuals = parser.result()
            return {queue: [slot != "light_2" for slot in slots] for queue, slots in visuals.items()}, visuals

        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, flags=re.IGNORECASE | re.DOTALL)

        by_queue: Dict[str, List[bool]] = {}
        visual_by_queue: Dict[str, List[str]] = {}
        current_queue_num: Optional[str] = None

        for row in rows:
            queue_match = re.search(
                r'<td[^>]*class="[^"]*turnoff-scheduleui-table-queue[^"]*"[^>]*>(.*?)</td>',
                row,
                flags=re.IGNORECASE | re.DOTALL,
            )
            if queue_match:
                qtxt = self._strip_tags(queue_match.group(1))
                num_match = re.search(r"(\d+)", qtxt)
                if num_match:
                    current_queue_num = num_match.group(1)

            sub_match = re.search(
                r'<td[^>]*class="[^"]*turnoff-scheduleui-table-subqueue[^"]*"[^>]*>(.*?)</td>',
                row,
                flags=re.IGNORECASE | re.DOTALL,
            )
            if not sub_match or current_queue_num is None:
                continue

            sub = self._strip_tags(sub_match.group(1))
            if sub not in {"1", "2"}:
                continue

            classes = re.findall(r'<td[^>]*class="(light_\d+)"[^>]*>', row, flags=re.IGNORECASE)
            if len(classes) < 48:
                continue

            visual_48 = [self._normalize_visual_class(cls) for cls in classes[:48]]
            state_48 = [cls != "light_2" for cls in visual_48]
            key = f"{current_queue_num}.{sub}"
            if key in QUEUE_NAMES:
                by_queue[key] = state_48
                visual_by_queue[key] = visual_48

        missing = [q for q in QUEUE_NAMES if q not in by_queue]
        if missing:
            raise RuntimeError(f"Не вдалося розпарсити черги: {', '.join(missing)}")

        return by_queue, visual_by_queue

    def _parse_schedule_html(self, html: str) -> Dict[str, List[bool]]:
        by_queue, _ = self._parse_schedule_html_bundle(html)
        return by_queue

    @classmethod
    def build_interval_lines(cls, state_48: List[bool], is_on: bool) -> List[str]:
        lines: List[str] = []
        i = 0
        while i < 48:
            if state_48[i] != is_on:
                i += 1
                continue

            start = i
            while i + 1 < 48 and state_48[i + 1] == is_on:
                i += 1
            end = i + 1

            lines.append(f"{cls._half_hour_to_str(start)} - {cls._half_hour_to_str(end)}")
            i += 1

        return lines

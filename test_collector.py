import asyncio
from copy import deepcopy
from datetime import datetime, timedelta
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import AsyncMock, patch

from collector import (KYIV, SLOT_PAIRS, build_payload, collect_days, decode_queues,
                       encode_queues, load_cached_days, midnight_stamp, save_payload,
                       should_publish, timestamp, run)
from poe_client import QUEUE_NAMES


class ExportTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 10, 27, 20, tzinfo=KYIV)
        self.day = self.now.date().isoformat()
        self.tomorrow = (self.now + timedelta(days=1)).date().isoformat()
        self.visuals = {queue: ["light_1"] * 48 for queue in QUEUE_NAMES}

    def entry(self, visuals=None, status="scheduled", fresh=True):
        return {"visuals": deepcopy(visuals or self.visuals), "status": status,
                "verifiedAt": timestamp(self.now), "fresh": fresh}

    def test_all_six_legacy_codes_round_trip_without_losing_yellow(self):
        self.visuals["5.1"][:12] = [slot for pair in SLOT_PAIRS.values() for slot in pair]
        groups = encode_queues(self.visuals)
        self.assertEqual(list(groups["GPV5.1"].values())[:6], list(SLOT_PAIRS))
        self.assertEqual(decode_queues(groups), self.visuals)
        self.assertEqual(groups["GPV5.1"]["6"], "msecond")

    def test_exact_legacy_shape_and_today_do_not_depend_on_available_dates(self):
        payload = build_payload({self.tomorrow: self.entry()}, self.now)
        self.assertEqual(payload["regionId"], "Poltava")
        self.assertEqual(payload["fact"]["today"], int(midnight_stamp(self.day)))
        self.assertEqual(set(payload["fact"]["data"]), {midnight_stamp(self.tomorrow)})
        groups = payload["fact"]["data"][midnight_stamp(self.tomorrow)]
        self.assertEqual(len(groups), 12)
        self.assertTrue(all(len(hours) == 24 for hours in groups.values()))
        self.assertEqual(payload["preset"]["time_zone"]["24"], ["23-24", "23:00", "24:00"])
        self.assertEqual(set(payload["preset"]["time_type"]), set(SLOT_PAIRS))
        self.assertEqual(payload["metadata"]["days"][self.day]["status"], "unknown")

    def test_missing_invalid_or_unrepresentable_cells_are_not_replaced_with_green(self):
        cases = []
        missing_queue = deepcopy(self.visuals)
        missing_queue.pop("6.2")
        cases.append(missing_queue)
        for change in (lambda slots: slots.pop(), lambda slots: slots.__setitem__(0, "unknown"),
                       lambda slots: slots.__setitem__(1, "light_3")):
            visuals = deepcopy(self.visuals)
            change(visuals["5.1"])
            cases.append(visuals)
        for case in cases:
            with self.subTest(case=case):
                with self.assertRaises(ValueError):
                    encode_queues(case)

    def test_cancelled_day_is_explicitly_marked_and_legacy_consumers_see_no_outages(self):
        payload = build_payload({self.day: self.entry(status="no_outages")}, self.now)
        self.assertEqual(payload["metadata"]["days"][self.day]["status"], "no_outages")
        self.assertTrue(all(code == "yes" for hours in payload["fact"]["data"][midnight_stamp(self.day)].values()
                            for code in hours.values()))

    def test_corrupted_cache_is_ignored_not_relabelled_verified(self):
        payload = build_payload({self.day: self.entry()}, self.now)
        payload["fact"]["data"][midnight_stamp(self.day)]["GPV5.1"].pop("24")
        self.assertEqual(load_cached_days(payload, {self.day}, self.now), {})

    def test_cached_cancellation_cannot_contain_red_slots(self):
        self.visuals["5.1"][0:2] = ["light_2", "light_2"]
        with self.assertRaises(ValueError):
            build_payload({self.day: self.entry(status="no_outages")}, self.now)
        payload = build_payload({self.day: self.entry()}, self.now)
        payload["metadata"]["days"][self.day]["status"] = "no_outages"
        self.assertEqual(load_cached_days(payload, {self.day}, self.now), {})

    def test_unchanged_checks_publish_hourly_not_every_five_minutes(self):
        previous = build_payload({self.day: self.entry()}, self.now)
        entry = self.entry()
        entry["verifiedAt"] = timestamp(self.now + timedelta(minutes=5))
        payload = build_payload({self.day: entry}, self.now)
        self.assertFalse(should_publish(previous, payload, self.now + timedelta(minutes=5)))
        self.assertTrue(should_publish(previous, payload, self.now + timedelta(hours=1)))

    def test_schedule_changes_and_freshness_changes_publish_immediately(self):
        previous = build_payload({self.day: self.entry()}, self.now)
        self.visuals["5.1"][0:2] = ["light_2", "light_3"]
        self.assertTrue(should_publish(previous, build_payload({self.day: self.entry()}, self.now), self.now))
        old = deepcopy(previous)
        old["metadata"]["days"][self.day]["fresh"] = False
        self.assertTrue(should_publish(old, previous, self.now))

    def test_kyiv_midnight_rollover_drops_yesterday_and_keeps_new_today(self):
        tomorrow = self.now + timedelta(days=1)
        payload = build_payload({self.day: self.entry(), self.tomorrow: self.entry()}, tomorrow)
        self.assertEqual(payload["fact"]["today"], int(midnight_stamp(self.tomorrow)))
        self.assertNotIn(midnight_stamp(self.day), payload["fact"]["data"])

    def test_dst_day_keys_use_local_midnight_not_86400_second_offsets(self):
        first = int(midnight_stamp("2026-10-25"))
        second = int(midnight_stamp("2026-10-26"))
        self.assertEqual(second - first, 25 * 3600)

    def test_only_public_allowlisted_fields_are_serialized(self):
        entry = self.entry()
        entry.update(subscribers={"secret-id": "name"}, token="DO_NOT_PUBLISH")
        payload = build_payload({self.day: entry}, self.now)
        text = json.dumps(payload)
        self.assertNotIn("DO_NOT_PUBLISH", text)
        self.assertNotIn("subscribers", text)
        with TemporaryDirectory() as root:
            path = Path(root) / "data" / "Poltavaoblenergo.json"
            save_payload(path, payload)
            self.assertEqual(json.loads(path.read_text(encoding="utf-8")), payload)
            self.assertFalse(path.with_suffix(".json.tmp").exists())


class CollectionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.now = datetime.now(KYIV)
        self.today = self.now.date().isoformat()
        self.tomorrow = (self.now + timedelta(days=1)).date().isoformat()
        self.visuals = {queue: ["light_1"] * 48 for queue in QUEUE_NAMES}
        self.client = AsyncMock()

    async def test_one_response_can_resolve_both_dates(self):
        self.client.fetch_available_schedule_bundle.return_value = (
            {}, {self.today: self.visuals, self.tomorrow: self.visuals}, set())
        days = await collect_days(self.client, self.now)
        self.assertEqual(set(days), {self.today, self.tomorrow})
        self.client.fetch_available_schedule_bundle.assert_awaited_once()

    async def test_date_is_never_assumed_from_the_request(self):
        self.client.fetch_available_schedule_bundle.return_value = ({}, {self.tomorrow: self.visuals}, set())
        days = await collect_days(self.client, self.now)
        self.assertEqual(set(days), {self.tomorrow})
        self.assertNotIn(self.today, days)

    async def test_failure_preserves_cache_and_does_not_advance_verification(self):
        old_check = timestamp(self.now - timedelta(hours=2))
        previous = build_payload({self.today: {"visuals": self.visuals, "status": "scheduled",
                                 "verifiedAt": old_check, "fresh": True}}, self.now)
        self.client.fetch_available_schedule_bundle.side_effect = [TimeoutError(), ({}, {self.tomorrow: self.visuals}, set())]
        days = await collect_days(self.client, self.now, previous)
        self.assertFalse(days[self.today]["fresh"])
        self.assertEqual(days[self.today]["verifiedAt"], old_check)
        self.assertTrue(days[self.tomorrow]["fresh"])

    async def test_complete_failure_raises_even_with_old_cache(self):
        self.client.fetch_available_schedule_bundle.side_effect = TimeoutError()
        with self.assertRaisesRegex(RuntimeError, "not modified"):
            await collect_days(self.client, self.now)

    async def test_explicit_cancellation_overrides_old_schedule(self):
        self.client.fetch_available_schedule_bundle.return_value = ({}, {}, {self.today, self.tomorrow})
        days = await collect_days(self.client, self.now)
        self.assertTrue(all(day["status"] == "no_outages" for day in days.values()))

    async def test_force_publication_still_requires_a_successful_collection(self):
        with TemporaryDirectory() as root:
            path = Path(root) / "Poltavaoblenergo.json"
            path.write_text('{"regionId": "Poltava"}', encoding="utf-8")
            original = path.read_bytes()
            with patch("collector.collect_days", new_callable=AsyncMock, side_effect=RuntimeError("offline")):
                with self.assertRaises(RuntimeError):
                    await run(path, force=True)
            self.assertEqual(path.read_bytes(), original)

    async def test_force_publication_bypasses_only_heartbeat_not_validation(self):
        days = {self.today: {"visuals": self.visuals, "status": "scheduled",
                            "verifiedAt": timestamp(self.now), "fresh": True}}
        with TemporaryDirectory() as root:
            path = Path(root) / "Poltavaoblenergo.json"
            with patch("collector.collect_days", new_callable=AsyncMock, return_value=days), \
                    patch("collector.should_publish", return_value=False):
                await run(path)
                self.assertFalse(path.exists())
                await run(path, force=True)
                self.assertEqual(json.loads(path.read_text(encoding="utf-8"))["regionId"], "Poltava")


if __name__ == "__main__":
    unittest.main()

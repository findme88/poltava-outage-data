"""Publish validated official schedules using the OE_OUTAGE_DATA JSON shape."""

import argparse
import asyncio
from copy import deepcopy
from datetime import datetime, time, timedelta, timezone
import json
import logging
from pathlib import Path
from zoneinfo import ZoneInfo

from poe_client import PoescheduleClient, QUEUE_NAMES


KYIV = ZoneInfo("Europe/Kyiv")
OUTPUT = Path("data/Poltavaoblenergo.json")
HEARTBEAT = timedelta(hours=1)
SLOT_PAIRS = {
    "yes": ("light_1", "light_1"),
    "no": ("light_2", "light_2"),
    "first": ("light_2", "light_1"),
    "second": ("light_1", "light_2"),
    "mfirst": ("light_3", "light_1"),
    "msecond": ("light_2", "light_3"),
}
PAIR_CODES = {pair: code for code, pair in SLOT_PAIRS.items()}
TIME_TYPES = {
    "yes": "\u0421\u0432\u0456\u0442\u043b\u043e \u0454",
    "no": "\u0421\u0432\u0456\u0442\u043b\u0430 \u043d\u0435\u043c\u0430\u0454",
    "first": "\u0411\u0435\u0437 \u0441\u0432\u0456\u0442\u043b\u0430 \u043f\u0435\u0440\u0448\u0456 30 \u0445\u0432.",
    "second": "\u0411\u0435\u0437 \u0441\u0432\u0456\u0442\u043b\u0430 \u0434\u0440\u0443\u0433\u0456 30 \u0445\u0432.",
    "mfirst": "\u041f\u0435\u0440\u0448\u0456 30 \u0445\u0432. \u043f\u0435\u0440\u0435\u043c\u0438\u043a\u0430\u043d\u043d\u044f, \u0434\u0440\u0443\u0433\u0456 30 \u0445\u0432. \u0441\u0432\u0456\u0442\u043b\u043e \u0454",
    "msecond": "\u041f\u0435\u0440\u0448\u0456 30 \u0445\u0432. \u0431\u0435\u0437 \u0441\u0432\u0456\u0442\u043b\u0430, \u0434\u0440\u0443\u0433\u0456 30 \u0445\u0432. \u043f\u0435\u0440\u0435\u043c\u0438\u043a\u0430\u043d\u043d\u044f",
}


def timestamp(value):
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("A timezone-aware timestamp is required")
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_timestamp(value):
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("Missing timestamp timezone")
    return result


def midnight_stamp(day):
    return str(int(datetime.combine(datetime.fromisoformat(day).date(), time(), KYIV).timestamp()))


def encode_queues(visuals):
    if not isinstance(visuals, dict) or set(visuals) != set(QUEUE_NAMES):
        raise ValueError("A day must contain exactly all 12 queues")
    groups = {}
    for queue in QUEUE_NAMES:
        slots = visuals[queue]
        if not isinstance(slots, list) or len(slots) != 48:
            raise ValueError("A queue must contain exactly 48 half-hour slots")
        hours = {}
        for index in range(0, 48, 2):
            pair = tuple(slots[index:index + 2])
            if pair not in PAIR_CODES:
                raise ValueError("Official state pair cannot be represented losslessly in the legacy format")
            hours[str(index // 2 + 1)] = PAIR_CODES[pair]
        groups["GPV" + queue] = hours
    return groups


def decode_queues(groups):
    if not isinstance(groups, dict) or set(groups) != {"GPV" + q for q in QUEUE_NAMES}:
        raise ValueError("Incomplete cached groups")
    visuals = {}
    for queue in QUEUE_NAMES:
        hours = groups["GPV" + queue]
        if not isinstance(hours, dict) or set(hours) != {str(h) for h in range(1, 25)}:
            raise ValueError("Incomplete cached hours")
        visuals[queue] = [slot for hour in range(1, 25) for slot in SLOT_PAIRS[hours[str(hour)]]]
    return visuals


def load_cached_days(previous, tracked, now):
    days = {}
    if not isinstance(previous, dict) or previous.get("regionId") != "Poltava":
        return days
    for day in tracked:
        try:
            metadata = previous["metadata"]["days"][day]
            status = metadata["status"]
            verified_at = parse_timestamp(metadata["verifiedAt"])
            if status not in {"scheduled", "no_outages"} or verified_at > now + timedelta(minutes=5):
                continue
            visuals = decode_queues(previous["fact"]["data"][midnight_stamp(day)])
            if status == "no_outages" and any(slot != "light_1" for slots in visuals.values() for slot in slots):
                continue
            days[day] = {"visuals": visuals, "status": status,
                         "verifiedAt": metadata["verifiedAt"], "fresh": False}
        except (KeyError, TypeError, ValueError, AttributeError):
            continue
    return days


async def collect_days(client, now, previous=None):
    now = now.astimezone(KYIV)
    requested = [now, now + timedelta(days=1)]
    tracked = {day.date().isoformat() for day in requested}
    days = load_cached_days(previous, tracked, now)
    confirmed = set()
    for request_day in requested:
        if tracked <= confirmed:
            break
        try:
            _, visuals, cancelled = await client.fetch_available_schedule_bundle(request_day, tracked)
            # Validate the entire returned batch before publishing any part of it.
            staged = {}
            verified = timestamp(datetime.now(KYIV))
            for day in tracked - confirmed:
                if day in cancelled:
                    slots = {queue: ["light_1"] * 48 for queue in QUEUE_NAMES}
                    status = "no_outages"
                elif day in visuals:
                    slots = visuals[day]
                    status = "scheduled"
                else:
                    continue
                encode_queues(slots)
                staged[day] = {"visuals": deepcopy(slots), "status": status,
                               "verifiedAt": verified, "fresh": True}
            days.update(staged)
            confirmed.update(staged)
        except Exception as exc:
            logging.warning("Official request failed (%s); keeping confirmed cached dates", type(exc).__name__)
    if not confirmed:
        raise RuntimeError("No current official dates could be verified; existing JSON was not modified")
    return days


def build_payload(days, now):
    now = now.astimezone(KYIV)
    today = now.date().isoformat()
    tracked = [today, (now + timedelta(days=1)).date().isoformat()]
    data, metadata = {}, {}
    checked = []
    for day in tracked:
        entry = days.get(day)
        if entry is None:
            metadata[day] = {"status": "unknown", "verifiedAt": None, "fresh": False}
            continue
        if entry["status"] not in {"scheduled", "no_outages"}:
            raise ValueError("Unknown schedule status")
        if entry["status"] == "no_outages" and any(
            slot != "light_1" for slots in entry["visuals"].values() for slot in slots
        ):
            raise ValueError("An explicit cancellation cannot contain outage or switching slots")
        data[midnight_stamp(day)] = encode_queues(entry["visuals"])
        checked.append(parse_timestamp(entry["verifiedAt"]))
        metadata[day] = {key: entry[key] for key in ("status", "verifiedAt", "fresh")}
    if not checked:
        raise ValueError("Cannot publish an empty unverified snapshot")
    last_checked = max(checked)
    return {
        "regionId": "Poltava",
        "lastUpdated": timestamp(last_checked),
        "fact": {"data": data, "update": last_checked.astimezone(KYIV).strftime("%d.%m.%Y %H:%M"),
                 "today": int(midnight_stamp(today))},
        "preset": {
            "time_zone": {str(hour + 1): [f"{hour:02d}-{hour + 1:02d}", f"{hour:02d}:00", f"{hour + 1:02d}:00"]
                          for hour in range(24)},
            "time_type": TIME_TYPES,
        },
        "metadata": {"schemaVersion": 1, "timezone": "Europe/Kyiv", "slotMinutes": 30,
                     "source": PoescheduleClient.NEW_URL, "unofficial": True, "days": metadata},
    }


def content_key(payload):
    return {"data": payload["fact"]["data"], "today": payload["fact"]["today"],
            "days": {day: {key: entry[key] for key in ("status", "fresh")}
                     for day, entry in payload["metadata"]["days"].items()}}


def should_publish(previous, payload, now):
    try:
        if content_key(previous) != content_key(payload):
            return True
        last = parse_timestamp(previous["lastUpdated"])
        return last > now or now - last >= HEARTBEAT
    except (KeyError, TypeError, ValueError, AttributeError):
        return True


def save_payload(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


async def run(output):
    previous = json.loads(output.read_text(encoding="utf-8")) if output.exists() else None
    now = datetime.now(KYIV)
    days = await asyncio.wait_for(collect_days(PoescheduleClient(), now, previous), timeout=180)
    # Do not label a pre-midnight collection as the next day's verification.
    if datetime.now(KYIV).date() != now.date():
        raise RuntimeError("Kyiv date changed during collection; retry on the next run")
    payload = build_payload(days, now)
    if should_publish(previous, payload, now):
        save_payload(output, payload)
        print("Published verified snapshot: " + ", ".join(
            f"{day}={entry['status']}" for day, entry in payload["metadata"]["days"].items()))
    else:
        print("Official schedules verified; no changes (hourly heartbeat not due)")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    asyncio.run(run(args.output))


if __name__ == "__main__":
    main()

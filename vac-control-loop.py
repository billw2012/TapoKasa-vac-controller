"""
Monitor the power draw of one device and toggle another device accordingly,
configured via a JSON file.

JSON example:
{
    "monitor_alias": "Server Rack",
    "control_alias": "AC Outlet",
    "threshold": 50,
    "off_delay": 10,
    "interval": 2,
    "username": "user@example.com",
    "password": "your_password"
}

Usage:
    python monitor-control-loop.py --config config.json

Errors are automatically published to the ntfy topic 'vac-control-loop-errors'.

"""

import argparse
import asyncio
import json
import logging
import sys
import traceback
import urllib.error
import urllib.request
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Union

from kasa import Discover
from kasa.iot import IotDevice

logger = logging.getLogger("vac_control")
NTFY_ERROR_TOPIC = "vac-control-loop-errors"
NTFY_STATUS_TOPIC = "vac-control-loop-status"
NTFY_RATE_LIMIT_SECONDS = 5 * 60
NTFY_MAX_LINES = 30

@dataclass
class _NtfyMessage:
    subject: str
    body: str
    topic: str


_NTFY_STOP = object()
_ntfy_queue: Optional["asyncio.Queue[Union[_NtfyMessage, object]]"] = None
_ntfy_consumer_task: Optional["asyncio.Task[None]"] = None


class ControllerError(Exception):
    """Raised when the controller encounters an expected error."""


def send_ntfy_notification(subject: str, body: str, topic: str) -> None:
    """Send a notification to the given ntfy topic."""
    url = f"https://ntfy.sh/{topic}"
    req = urllib.request.Request(
        url,
        data=body.encode("utf-8"),
        headers={
            "Title": subject,
            "Content-Type": "text/plain; charset=utf-8",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
        logger.info("Sent ntfy notification to topic '%s'.", topic)
    except:
        logger.exception("Failed to send ntfy notification to topic '%s'.", topic)


def notify_error(
    message: str, exc: Optional[BaseException] = None
) -> None:
    """Log the error and send an ntfy notification."""
    if exc:
        logger.exception("%s", message)
    else:
        logger.error("%s", message)

    body_lines = [message]
    if exc:
        body_lines.append("")
        body_lines.append("Traceback:")
        body_lines.append(
            "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        )
    _queue_ntfy_notification(
        "Vac Control Loop Error", "\n".join(body_lines), NTFY_ERROR_TOPIC
    )


def notify_status(message: str) -> None:
    """Send a status notification about the control loop lifecycle."""
    _queue_ntfy_notification("Vac Control Loop Status", message, NTFY_STATUS_TOPIC)


def _queue_ntfy_notification(subject: str, body: str, topic: str) -> None:
    """Enqueue notifications for processing by the ntfy consumer task."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        logger.warning(
            "Dropping ntfy notification for topic '%s'; no running event loop.", topic
        )
        return

    message = _NtfyMessage(subject=subject, body=body, topic=topic)
    try:
        if _ntfy_queue is None:
            logger.warning(
                "Dropping ntfy notification for topic '%s'; notification queue is not ready.",
                topic,
            )
            return
        _ntfy_queue.put_nowait(message)
    except asyncio.QueueFull:
        logger.warning(
            "Dropping ntfy notification for topic '%s'; notification queue is full.",
            topic,
        )


def _format_ntfy_lines(subject: str, body: str) -> List[str]:
    """Return the lines that represent a single notification entry."""
    lines = [f"[{subject}]"]
    body_lines = body.splitlines() or [""]
    lines.extend(body_lines)
    lines.append("")
    return lines


def _truncate_lines(lines: List[str]) -> List[str]:
    """Ensure the payload respects the line limit, truncating if needed."""
    trimmed = list(lines)
    while trimmed and trimmed[-1] == "":
        trimmed.pop()
    if len(trimmed) <= NTFY_MAX_LINES:
        return trimmed or [""]

    keep_count = max(1, NTFY_MAX_LINES - 1)
    truncated_count = len(trimmed) - keep_count
    truncated = trimmed[:keep_count]
    truncated.append(f"... ({truncated_count} more lines)")
    return truncated


async def _start_ntfy_consumer() -> None:
    """Ensure the ntfy consumer task is running."""
    global _ntfy_consumer_task
    if _ntfy_consumer_task is None:
        global _ntfy_queue
        if _ntfy_queue is None:
            _ntfy_queue = asyncio.Queue()
        _ntfy_consumer_task = asyncio.create_task(_ntfy_consumer())


async def _stop_ntfy_consumer() -> None:
    """Stop the ntfy consumer task, flushing what we can."""
    global _ntfy_consumer_task
    if _ntfy_consumer_task is None:
        return

    if _ntfy_queue is not None:
        await _ntfy_queue.put(_NTFY_STOP)
    await _ntfy_consumer_task
    _ntfy_consumer_task = None


async def _ntfy_consumer() -> None:
    """Background task that batches and rate-limits ntfy messages."""
    if _ntfy_queue is None:
        logger.warning("ntfy consumer exiting because queue is not initialized.")
        return

    queue = _ntfy_queue
    buffers: Dict[str, Dict[str, Any]] = {}
    try:
        while True:
            delay = _next_flush_delay(buffers)
            if delay is None:
                item = await queue.get()
            elif delay <= 0:
                _flush_ready_topics(buffers)
                continue
            else:
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=delay)
                except asyncio.TimeoutError:
                    _flush_ready_topics(buffers)
                    continue

            if item is _NTFY_STOP:
                break

            if not isinstance(item, _NtfyMessage):
                logger.warning("Ignoring unexpected ntfy queue item: %r", item)
                continue

            _accumulate_ntfy_message(buffers, item)
            _flush_ready_topics(buffers)
    finally:
        _flush_ready_topics(buffers)
        _log_unflushed_topics(buffers)


def _accumulate_ntfy_message(
    buffers: Dict[str, Dict[str, Any]], message: _NtfyMessage
) -> None:
    state = buffers.setdefault(
        message.topic,
        {"lines": [], "subject": message.subject, "next_allowed": 0.0},
    )
    state["subject"] = message.subject
    state["lines"].extend(_format_ntfy_lines(message.subject, message.body))


def _next_flush_delay(buffers: Dict[str, Dict[str, Any]]) -> Optional[float]:
    now = time.monotonic()
    delays: List[float] = []
    for state in buffers.values():
        if state["lines"]:
            delays.append(state["next_allowed"] - now)
    if not delays:
        return None
    min_delay = min(delays)
    return max(0.0, min_delay)


def _flush_ready_topics(
    buffers: Dict[str, Dict[str, Any]],
) -> None:
    now = time.monotonic()
    for topic, state in buffers.items():
        lines: List[str] = state.get("lines", [])
        if not lines:
            continue
        if state["next_allowed"] > now:
            continue
        payload_lines = _truncate_lines(lines)
        lines.clear()
        subject = state.get("subject") or "Vac Control Loop Notification"
        send_ntfy_notification(subject, "\n".join(payload_lines), topic)
        state["next_allowed"] = time.monotonic() + NTFY_RATE_LIMIT_SECONDS


def _log_unflushed_topics(buffers: Dict[str, Dict[str, Any]]) -> None:
    for topic, state in buffers.items():
        lines: List[str] = state.get("lines", [])
        if lines:
            logger.warning(
                "Leaving %d queued ntfy line(s) for topic '%s' unsent (rate limit active).",
                len(lines),
                topic,
            )


async def discover_required_devices(
    aliases: Iterable[str], username: str, password: str
) -> Dict[str, IotDevice]:
    """Discover devices and return a mapping of alias -> device for the required ones."""
    alias_list = list(aliases)
    alias_set = set(alias_list)
    logger.info(
        "Discovering %d device(s) for aliases: %s",
        len(alias_list),
        ", ".join(alias_list),
    )
    found_devices = await Discover.discover(username=username, password=password)
    await asyncio.gather(*(dev.update() for dev in found_devices.values()))
    alias_map = {dev.alias: dev for dev in found_devices.values() if dev.alias}
    logger.info(
        "Discovery returned %d device(s); available aliases: %s",
        len(found_devices),
        ", ".join(alias_map.keys()) or "none",
    )

    missing = [alias for alias in alias_list if alias not in alias_map]
    if missing:
        await asyncio.gather(*(dev.disconnect() for dev in found_devices.values()))
        raise ControllerError(
            f"Could not find device(s) with alias: {', '.join(missing)}"
        )

    unused_devices = [
        dev for dev in found_devices.values() if not dev.alias or dev.alias not in alias_set
    ]
    if unused_devices:
        logger.debug(
            "Disconnecting %d unused discovered device(s).", len(unused_devices)
        )
        await asyncio.gather(*(dev.disconnect() for dev in unused_devices))

    logger.info("All required devices located.")
    return {alias: alias_map[alias] for alias in alias_list}


async def monitor_and_control(
    monitor_device: IotDevice,
    control_device: IotDevice,
    threshold: float,
    off_delay: float,
    interval: float,
) -> None:
    """Continuously watch monitor_device consumption and toggle control_device."""
    consumption_feature = monitor_device.features.get("current_consumption")
    if consumption_feature is None:
        await asyncio.gather(monitor_device.disconnect(), control_device.disconnect())
        raise ControllerError(
            f"Device '{monitor_device.alias}' has no 'current_consumption' feature."
        )

    below_since = None
    control_on = False

    try:
        while True:
            await monitor_device.update()
            current = consumption_feature.value
            logger.debug(
                "[%s] current consumption=%.2f (threshold=%.2f)",
                monitor_device.alias,
                current,
                threshold,
            )

            if current > threshold:
                below_since = None
                if not control_on:
                    logger.info("Current exceeded threshold; turning ON '%s'", control_device.alias)
                    await control_device.turn_on()
                    control_on = True
            else:
                if control_on and below_since is None:
                    below_since = asyncio.get_event_loop().time()
                if control_on and below_since is not None:
                    elapsed = asyncio.get_event_loop().time() - below_since
                    if elapsed >= off_delay:
                        logger.info(
                            "Current below threshold for %.1fs; turning OFF '%s'",
                            elapsed,
                            control_device.alias,
                        )
                        await control_device.turn_off()
                        control_on = False
                        below_since = None

            await asyncio.sleep(interval)
    except KeyboardInterrupt:
        logger.info("Stopping monitor loop.")
    finally:
        logger.info("Disconnecting devices.")
        await asyncio.gather(
            monitor_device.disconnect(),
            control_device.disconnect(),
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Toggle one device based on another device's power draw."
    )
    parser.add_argument(
        "--config",
        required=False,
        default="config.json",
        help="Path to JSON config containing device aliases, thresholds, and credentials.",
    )
    return parser.parse_args()


def load_config(config_path: str) -> Dict[str, Any]:
    path = Path(config_path)
    if not path.is_file():
        raise ControllerError(f"Config file not found at '{config_path}'")

    try:
        config = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ControllerError(f"Invalid JSON in config file: {exc}") from exc

    required_keys = [
        "monitor_alias",
        "control_alias",
        "threshold",
        "off_delay",
        "interval",
        "username",
        "password",
    ]
    missing = [key for key in required_keys if key not in config]
    if missing:
        raise ControllerError(f"Missing required config keys: {', '.join(missing)}")

    logger.info(
        "Loaded config for monitor='%s', control='%s', threshold=%s, off_delay=%ss, interval=%ss",
        config["monitor_alias"],
        config["control_alias"],
        config["threshold"],
        config["off_delay"],
        config["interval"],
    )
    return config


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    args = parse_args()
    logger.info("Starting control loop with config path: %s", args.config)
    config: Optional[Dict[str, Any]] = None
    start_notification_sent = False
    await _start_ntfy_consumer()

    try:
        config = load_config(args.config)

        devices = await discover_required_devices(
            [config["monitor_alias"], config["control_alias"]],
            username=config["username"],
            password=config["password"],
        )

        logger.info(
            "Beginning monitor loop for monitor '%s' controlling '%s'",
            config["monitor_alias"],
            config["control_alias"],
        )
        notify_status(
            (
                f"Started monitoring '{config['monitor_alias']}' to control "
                f"'{config['control_alias']}' using config '{args.config}'."
            )
        )
        start_notification_sent = True
        await monitor_and_control(
            monitor_device=devices[config["monitor_alias"]],
            control_device=devices[config["control_alias"]],
            threshold=float(config["threshold"]),
            off_delay=float(config["off_delay"]),
            interval=float(config["interval"]),
        )
    except ControllerError as exc:
        notify_error(str(exc), exc)
        raise SystemExit(1)
    except Exception as exc:
        notify_error("Unexpected error occurred.", exc)
        raise SystemExit(1)
    finally:
        if start_notification_sent:
            notify_status("Vac control loop terminated.")
        await _stop_ntfy_consumer()


if __name__ == "__main__":
    asyncio.run(main())

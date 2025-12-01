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
"""

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, Iterable

from kasa import Discover
from kasa.iot import IotDevice

logger = logging.getLogger("vac_control")


async def discover_required_devices(
    aliases: Iterable[str], username: str, password: str
) -> Dict[str, IotDevice]:
    """Discover devices and return a mapping of alias -> device for the required ones."""
    alias_list = list(aliases)
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
        logger.error("Missing required device(s) with alias: %s", ", ".join(missing))
        await asyncio.gather(*(dev.disconnect() for dev in found_devices.values()))
        sys.exit(1)

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
        logger.error(
            "Device '%s' has no 'current_consumption' feature.", monitor_device.alias
        )
        await asyncio.gather(monitor_device.disconnect(), control_device.disconnect())
        sys.exit(1)

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
        logger.error("Config file not found at '%s'", config_path)
        sys.exit(1)

    try:
        config = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        logger.error("Invalid JSON in config file: %s", exc)
        sys.exit(1)

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
        logger.error("Missing required config keys: %s", ", ".join(missing))
        sys.exit(1)

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
    await monitor_and_control(
        monitor_device=devices[config["monitor_alias"]],
        control_device=devices[config["control_alias"]],
        threshold=float(config["threshold"]),
        off_delay=float(config["off_delay"]),
        interval=float(config["interval"]),
    )


if __name__ == "__main__":
    asyncio.run(main())

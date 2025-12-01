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
import sys
from pathlib import Path
from typing import Any, Dict, Iterable

from kasa import Discover, SmartDevice


async def discover_required_devices(
    aliases: Iterable[str], username: str, password: str
) -> Dict[str, SmartDevice]:
    """Discover devices and return a mapping of alias -> device for the required ones."""
    found_devices = await Discover.discover(username=username, password=password)
    await asyncio.gather(*(dev.update() for dev in found_devices.values()))
    alias_map = {dev.alias: dev for dev in found_devices.values() if dev.alias}

    missing = [alias for alias in aliases if alias not in alias_map]
    if missing:
        print(f"Error: could not find device(s) with alias: {', '.join(missing)}")
        await asyncio.gather(*(dev.disconnect() for dev in found_devices.values()))
        sys.exit(1)

    return {alias: alias_map[alias] for alias in aliases}


async def monitor_and_control(
    monitor_device: SmartDevice,
    control_device: SmartDevice,
    threshold: float,
    off_delay: float,
    interval: float,
) -> None:
    """Continuously watch monitor_device consumption and toggle control_device."""
    consumption_feature = monitor_device.features.get("current_consumption")
    if consumption_feature is None:
        print(
            f"Error: device '{monitor_device.alias}' has no 'current_consumption' feature."
        )
        await asyncio.gather(monitor_device.disconnect(), control_device.disconnect())
        sys.exit(1)

    below_since = None
    control_on = False

    try:
        while True:
            await monitor_device.update()
            current = consumption_feature.value
            print(
                f"[{monitor_device.alias}] current_consumption={current:.2f} "
                f"(threshold={threshold})"
            )

            if current > threshold:
                below_since = None
                if not control_on:
                    print(f"Turning ON '{control_device.alias}'")
                    await control_device.turn_on()
                    control_on = True
            else:
                if control_on and below_since is None:
                    below_since = asyncio.get_event_loop().time()
                if control_on and below_since is not None:
                    elapsed = asyncio.get_event_loop().time() - below_since
                    if elapsed >= off_delay:
                        print(
                            f"Current below threshold for {elapsed:.1f}s; "
                            f"turning OFF '{control_device.alias}'"
                        )
                        await control_device.turn_off()
                        control_on = False
                        below_since = None

            await asyncio.sleep(interval)
    except KeyboardInterrupt:
        print("Stopping monitor loop.")
    finally:
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
        required=True,
        help="Path to JSON config containing device aliases, thresholds, and credentials.",
    )
    return parser.parse_args()


def load_config(config_path: str) -> Dict[str, Any]:
    path = Path(config_path)
    if not path.is_file():
        print(f"Error: config file not found at '{config_path}'")
        sys.exit(1)

    try:
        config = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        print(f"Error: invalid JSON in config file: {exc}")
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
        print(f"Error: missing required config keys: {', '.join(missing)}")
        sys.exit(1)

    return config


async def main() -> None:
    args = parse_args()
    config = load_config(args.config)

    devices = await discover_required_devices(
        [config["monitor_alias"], config["control_alias"]],
        username=config["username"],
        password=config["password"],
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

"""Run the gateway; knowledge-base/knowledge/features/local_voice_gateway.md §4.4."""

import argparse
import logging

from aiohttp import web

from voice_gateway.config import ConfigError, load_config
from voice_gateway.server import Gateway


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    try:
        config = load_config(args.config)
    except ConfigError as exc:
        parser.exit(2, f"voice gateway: {exc}\n")
    web.run_app(
        Gateway(config).app, host=config.host, port=config.port, access_log=None
    )


if __name__ == "__main__":
    main()

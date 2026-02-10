"""Application entry point."""

import logging
from config_loader import read_config
from subscriber import Subscriber


def main():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    cfg = read_config("../config.yaml")
    Subscriber(cfg).start()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.exception("Unhandled error during startup")
        raise

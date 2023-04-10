import logging
from uuid import uuid4

import pendulum


def generate_unique_filename(base: str, filetype: str = None) -> str:
    if "/" in base:
        raise ValueError(
            "Forward slash in filename could result in unintended subdirctories"
        )
    if filetype:
        if not filetype.startswith("."):
            logging.warning("filetype argument should start with a period")
            filetype = f".{filetype}"

    ts = pendulum.now().int_timestamp
    uuid = uuid4().hex

    return f"{base}_ts-{ts}_uuid-{uuid}{filetype or ''}"

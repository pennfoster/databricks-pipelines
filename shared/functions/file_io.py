import logging
from uuid import uuid4

import pendulum


def generate_unique_filename(base: str, filetype: str = None) -> str:
    if "/" in base:
        raise ValueError(
            "Forward slash in filename could result in unintended subdirctories"
        )
    if filetype and not filetype.startswith("."):
        logging.warning("filetype argument should start with a period")
        filetype = f".{filetype}"

    ts = pendulum.now(tz="utc").format("YMMDDTHHmmss-zz")
    uuid = str(uuid4().hex)

    return f"{base}_{ts}_{uuid}{filetype or ''}"

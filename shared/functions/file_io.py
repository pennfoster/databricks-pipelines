from datetime import datetime
from uuid import uuid4
import pendulum


def generate_unique_filename(base: str) -> str:
    if "/" in base:
        raise ValueError(
            "Forward slash in filename could result in unintended subdirctories"
        )

    ts = pendulum.now().int_timestamp
    uuid = uuid4().hex

    return f"{base}_ts-{ts}_uuid-{uuid}"

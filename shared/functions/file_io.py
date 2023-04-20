from datetime import datetime
from uuid import uuid4
import json
import pendulum


def generate_unique_filename(base: str) -> str:
    if "/" in base:
        raise ValueError(
            "Forward slash in filename could result in unintended subdirctories"
        )

    ts = pendulum.now().int_timestamp
    uuid = uuid4().hex

    return f"{base}_ts-{ts}_uuid-{uuid}"


def save_json(
    dest_dir: str, file_name: str, data: json, suffix: str = None, parent: bool = False
) -> str:
    Path(dest_dir).mkdir(parents=parents, exist_ok=True)
    file_name = file_name.replace(".json", "")
    if suffix == "timestamp":
        file_name += f'_{datetime.now().strftime("%Y%m%d-%H%M%S")}'

    file_path = f"{dest_dir}/{file_name}.json"
    with open(f"{dest_dir}/{file_name}.json", "w") as file:
        file.write(json.dumps(data))

    return file_path


def load_json(file_path -> string):
    with open(file_path, "r") as file:
        data = json.load(file)

    return data

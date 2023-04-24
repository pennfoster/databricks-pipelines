import requests
from typing import List, Tuple, Literal


# ! This code is no longer inuse and can be deprecated. Left here in case for now. (2023-04-24 Noam Blanks)
def get_pr_files(
    repo: str, pr_number: int, directory_path: str = None
) -> List[
    Tuple[
        str,
        Literal[
            "added",
            "removed",
            "modified",
            "renamed",
            "copied",
            "changed",
            "unchanged",
        ],
    ]
]:
    changed_files = []
    url = f"https://api.github.com/repos/pennfoster/{repo}/pulls/{pr_number}/files"
    with requests.Session() as s:
        s.headers = {
            **s.headers,
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        params = {"per_page": 100, "page": 1}
        while True:
            response = s.get(url, params=params)
            print(response.request.url)
            print(response.request.body)
            response.raise_for_status()
            if response.json() == []:
                break
            changed_files.extend(
                [(file["filename"], file["status"]) for file in response.json()]
            )
            params["page"] += 1

    if directory_path:
        changed_files = [t for t in changed_files if t[0].startswith(directory_path)]
    return changed_files

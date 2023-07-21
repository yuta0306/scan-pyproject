import requests


class GitHub(object):
    def __init__(self, token: str) -> None:
        self.session = requests.Session()
        self.__token = token

    def fetch_clone(self, owner: str, repo: str, per: str | None = None):
        params = {}
        if per is not None:
            params["per"] = per
        res = self.session.get(
            f"https://api.github.com/repos/{owner}/{repo}/traffic/clones",
            headers={"Authorization": f"token {self.__token}"},
            params=params,
        )

        return res.json()


if __name__ == "__main__":
    import os

    client = GitHub(os.environ.get("GITHUB_API_KEY", ""))
    print(client.fetch_clone("yuta0306", "dslclib", per="week"))

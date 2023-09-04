import os

from core import BigQuery, GitHub
from fastapi import FastAPI

app = FastAPI()
bigquery = BigQuery()
github = GitHub(token=os.environ.get("GITHUB_API_KEY", ""))


@app.get("/pip/{project}")
async def pip(project: str, num: int | None, unit: str | None):
    if num is None:
        num = 30
    if unit is None:
        unit = "DAY"

    res = bigquery.pip_count(
        project=project,
        truncation=True,
        num=num,
        unit=unit,
    )

    return {
        "project": project,
        "num": num,
        "unit": unit,
        "results": {
            "count": sum([value for value in res.values()]),
            "uniques": None,
            "details": [
                {"timestamp": k, "count": v, "uniques": None} for k, v in res.items()
            ],
        },
    }


@app.get("/github/{project}")
async def clones(project: str, owner: str, per: str | None = None):
    res = github.fetch_clone(owner=owner, repo=project, per=per)
    return {
        "project": project,
        "owner": owner,
        "per": per,
        "results": {
            "count": res["count"],
            "uniques": res["uniques"],
            "details": [
                {
                    k: v.replace("T00:00:00Z", "") if k == "timestamp" else v
                    for k, v in item.items()
                }
                for item in res["clones"]
            ],
        },
    }

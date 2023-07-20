import re

from google.cloud import bigquery
from google.cloud.bigquery.table import Row

SIMPLE_COUNT = """SELECT COUNT(*) AS num_downloads
FROM `bigquery-public-data.pypi.file_downloads`
WHERE file.project = '{project}'
  AND DATE(timestamp)
    BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL {num} {unit})
    AND CURRENT_DATE()
"""

SIMPLE_QUERY = """SELECT *
FROM `bigquery-public-data.pypi.file_downloads`
WHERE file.project = '{project}'
  AND DATE(timestamp)
    BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL {num} {unit})
    AND CURRENT_DATE()
"""

PIP_COUNT = """SELECT COUNT(*) as num_downloads
FROM `bigquery-public-data.pypi.file_downloads`
WHERE file.project = '{project}'
  AND details.installer.name = 'pip'
  AND DATE(timestamp)
    BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL {num} {unit})
    AND CURRENT_DATE()
"""

PIP_QUERY = """SELECT *
FROM `bigquery-public-data.pypi.file_downloads`
WHERE file.project = '{project}'
  AND details.installer.name = 'pip'
  AND DATE(timestamp)
    BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL {num} {unit})
    AND CURRENT_DATE()
"""


class BigQuery(object):
    def __init__(self) -> None:
        self.client = bigquery.Client(project="scan-pyproject")

    def simple_count(
        self,
        project: str,
        truncation: bool = False,
        num: int = 30,
        unit: str = "DAY",
    ) -> int | dict[str, int]:
        query = SIMPLE_COUNT.format_map({"project": project, "num": num, "unit": unit})
        if truncation:
            query = re.sub(
                "\nFROM `bigquery-public-data.pypi.file_downloads`",
                f""",\nDATE_TRUNC(DATE(timestamp), {unit}) AS `{unit.lower()}`
FROM `bigquery-public-data.pypi.file_downloads`""",
                query,
            )
            query = re.sub(
                r"DATE_SUB\(CURRENT_DATE\(\), INTERVAL \d+ (DAY|WEEK|YEAR)\)",
                f"DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL {num} {unit}), {unit})",
                query,
            )
            query += f"""GROUP BY `{unit.lower()}`
ORDER BY `{unit.lower()}` DESC
"""

        query_job = self.client.query(query)

        results = query_job.result()
        if truncation:
            return {
                str(getattr(row, unit.lower())): int(row.num_downloads)
                for row in sorted(results, key=lambda x: getattr(x, unit.lower()))
            }

        for row in results:
            pass
        return int(row.num_downloads)

    def fetch_all(self, project: str, num: int = 30, unit: str = "DAY") -> list[Row]:
        query_job = self.client.query(
            SIMPLE_QUERY.format_map({"project": project, "num": num, "unit": unit})
        )

        results = query_job.result()
        return sorted(list(results), key=lambda x: x.timestamp)

    def pip_count(
        self,
        project: str,
        truncation: bool = True,
        num: int = 30,
        unit: str = "DAY",
    ) -> int | dict[str, int]:
        query = PIP_COUNT.format_map({"project": project, "num": num, "unit": unit})
        if truncation:
            query = re.sub(
                "\nFROM `bigquery-public-data.pypi.file_downloads`",
                f""",\nDATE_TRUNC(DATE(timestamp), {unit}) AS `{unit.lower()}`
FROM `bigquery-public-data.pypi.file_downloads`""",
                query,
            )
            query = re.sub(
                r"DATE_SUB\(CURRENT_DATE\(\), INTERVAL \d+ (DAY|WEEK|YEAR)\)",
                f"DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL {num} {unit}), {unit})",
                query,
            )
            query += f"""GROUP BY `{unit.lower()}`
ORDER BY `{unit.lower()}` DESC
"""
        query_job = self.client.query(query)

        results = query_job.result()  # Waits for job to complete.
        if truncation:
            return {
                str(getattr(row, unit.lower())): int(row.num_downloads)
                for row in sorted(results, key=lambda x: getattr(x, unit.lower()))
            }

        for row in results:
            pass
        return int(row.num_downloads)

    def fetch_pip(self, project: str, num: int = 30, unit: str = "DAY") -> list[Row]:
        query_job = self.client.query(
            PIP_QUERY.format_map({"project": project, "num": num, "unit": unit})
        )

        results = query_job.result()
        return sorted(list(results), key=lambda x: x.timestamp)


if __name__ == "__main__":
    client = BigQuery()
    # print(client.simple_count(project="dslclib"))
    print(client.pip_count(project="dslclib", truncation=True, num=6, unit="WEEK"))
    # print(client.pip_count(project="dslclib"))
    # res = client.fetch_all(project="dslclib", num=10)

from google.cloud import bigquery
from google.cloud.bigquery.table import Row

SIMPLE_COUNT = """
SELECT COUNT(*) AS num_downloads
FROM `bigquery-public-data.pypi.file_downloads`
WHERE file.project = '{project}'
  AND DATE(timestamp)
    BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL {num} {unit})
    AND CURRENT_DATE()
"""

SIMPLE_QUERY = """
SELECT *
FROM `bigquery-public-data.pypi.file_downloads`
WHERE file.project = '{project}'
  AND DATE(timestamp)
    BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL {num} {unit})
    AND CURRENT_DATE()
"""

PIP_COUNT = """
SELECT COUNT(*) as num_downloads
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

    def simple_count(self, project: str, num: int = 30, unit: str = "DAY") -> int:
        query_job = self.client.query(
            SIMPLE_COUNT.format_map({"project": project, "num": num, "unit": unit})
        )

        results = query_job.result()
        for row in results:
            pass
        return int(row.num_downloads)

    def fetch_all(self, project: str, num: int = 30, unit: str = "DAY") -> list[Row]:
        query_job = self.client.query(
            SIMPLE_QUERY.format_map({"project": project, "num": num, "unit": unit})
        )

        results = query_job.result()
        return list(results)

    def pip_count(self, project: str, num: int = 30, unit: str = "DAY") -> int:
        query_job = self.client.query(
            PIP_COUNT.format_map({"project": project, "num": num, "unit": unit})
        )

        results = query_job.result()  # Waits for job to complete.
        for row in results:
            pass
        return int(row.num_downloads)


if __name__ == "__main__":
    client = BigQuery()
    # print(client.simple_count(project="dslclib"))
    # print(client.pip_count(project="dslclib"))
    print(client.fetch_all(project="dslclib", num=5))

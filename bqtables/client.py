import logging
import os
from datetime import datetime, date
from typing import List, Sequence, Mapping

from google.cloud import bigquery

MODULE_NAME = __name__


class BQProject(object):
    def __init__(self):
        self.project_name = os.environ.get('GCP_PROJECT')
        self.client = bigquery.Client()


class BQTable(object):
    def __init__(
            self, project: BQProject,
            dataset_name: str,
            table_name: str,
            date_column_name: str,
            logger_name: str = MODULE_NAME,
    ):
        self.project = project
        self.dataset_name = dataset_name
        self.table_name = table_name
        self.date_column_name = date_column_name

        self.dataset_ref = self.project.client.dataset(self.dataset_name)
        self.table_ref = self.dataset_ref.table(self.table_name)

        self.fqtable_name = '.'.join(
            [self.project.project_name, self.dataset_name, self.table_name])

        self.logger = logging.getLogger(logger_name)

    def fetch(
            self,
            start_date: [str, datetime],
            end_date: [str, datetime]
    ) -> bigquery.table.RowIterator:
        try:
            if type(start_date) != str:
                start_date = start_date.isoformat()
            if type(end_date) != str:
                end_date = end_date.isoformat()

            query = f"""
                SELECT * from `{self.fqtable_name}`
                WHERE `{self.date_column_name}` BETWEEN
                    TIMESTAMP('{start_date}') AND TIMESTAMP('{end_date}')
                ORDER BY `{self.date_column_name}`"""
            self.logger.info(f'BQTable.fetch.query: {query}')
            job: bigquery.job.QueryJob = self.project.client.query(query)
            results = job.result()

            return results

        except Exception as ex:
            self.logger.error(f'BQTable.fetch.failed: {str(ex)}')
            raise

    def stream(self, data: List[dict]) -> Sequence[Mapping]:
        try:
            table = self.project.client.get_table(self.table_ref)
            self.logger.info(f'BQTable.stream.data: {data}')
            return self.project.client.insert_rows(table, data)
        except Exception as ex:
            self.logger.error(f'BQTable.stream.failed: {str(ex)}')
            raise

    def sync(
            self,
            existing_rows: bigquery.table.RowIterator,
            new_rows: List[dict],
            on_column: str,
            checksum_column: str = None,
    ):
        to_ignore = []
        to_update = []

        for existing_row in existing_rows:
            id = existing_row[on_column]
            new_row = next(
                (row for row in new_rows if row[on_column] == id), None)

            if not new_row:
                continue
            if checksum_column:
                if existing_row[checksum_column] == new_row[checksum_column]:
                    to_ignore.append(new_row)
                    continue

            to_update.append(new_row)

        to_insert = [row for row in new_rows if
                     (row not in to_ignore and row not in to_update)]

        self.update(to_update)
        self.stream(to_insert)

    def update(self, data: List[dict]):
        for item in data:
            try:
                on_value = self._as_query_value(item[self.on_column])
                set_operations = self._as_set_operations(data)
                query = f"""
                    UPDATE `{self.fqtable_name}
                    SET {set_operations}
                    WHERE `{self.on_column}` = {on_value}"""
                self.logger.info(f'BQTable.update.query: {query}')
                job: bigquery.job.QueryJob = self.project.client.query(query)
                results = job.result()
            except Exception as ex:
                self.logger.error(f'BQTable.update.failed: {str(ex)}')
                raise

    def _as_query_value(self, value) -> str:
        vtype = type(value)
        if vtype == str:
            return f'`{value}`'
        if vtype == datetime or vtype == date:
            return f'TIMESTAMP("{value.isoformat()}")'

        return str(value)

    def _as_set_operations(self, d: dict) -> str:
        sets = [f'`k` = {self._as_query_value(v)}' for k, v in d.items()]
        return f'SET {",".join(sets)}'

#!/bin/env python
# note: 3 source and target types required: csv <--> json <--> postgresql <--> csv
#   entries can be stored locally, s3, postgresql
#   lets implement generic transfer to move entries in any direction


import json
import logging
from enum import Enum
from . import helpers


logger = logging.getLogger('logging table_transfer')
logger.setLevel(logging.INFO)


class TransformType(Enum):
    INSERT = 'insert'
    UPSERT = 'upsert'
    TRUNCATE_INSERT = 'truncate_insert'


class TableTransfer:
    """Represents data as table and helps to transfer data from one system to another"""
    def __init__(
            self,
            source_s3_bucket=None,
            source_file_name=None,
            target_s3_bucket=None,
            target_file_name=None,
            source_pg_schema=None,
            source_pg_table=None,
            target_pg_schema=None,
            target_pg_table=None,
    ):
        self.list_of_dicts_entries = None
        self.source_s3_bucket = source_s3_bucket
        self.source_file_name = source_file_name
        self.target_s3_bucket = target_s3_bucket
        self.target_file_name = target_file_name
        self.source_pg_schema = source_pg_schema
        self.source_pg_table = source_pg_table
        self.target_pg_schema = target_pg_schema
        self.target_pg_table = target_pg_table
        self.s3_client = helpers.get_s3_client()

    def _check_entries(self, filter_empty: bool = True):
        """all upload methods should work only if entries has already been prepared"""

        if not self.list_of_dicts_entries:
            raise Exception('Get entries at first, cant proceed')

        if filter_empty:
            self.list_of_dicts_entries = [entry for entry in self.list_of_dicts_entries if entry]

    @property
    def list_of_dicts_entries_b(self):
        """list_of_dicts_entries as bytes"""

        self._check_entries()
        return json.dumps(self.list_of_dicts_entries, indent=2).encode('utf-8')

    @property
    def header(self):
        return self.list_of_dicts_entries[0].keys()

    def _get_entries_from_csv_or_json(self, bucket=None, file_name=None, source_type=None):
        """gets entries from s3 or from local file system if no bucket provided"""

        if bucket:
            self.source_s3_bucket = bucket

        if file_name:
            self.source_file_name = file_name
        if not self.source_file_name:
            raise Exception(f'No source_file_name provided, cant proceed')

        if self.source_s3_bucket:
            temp_file = helpers.download_object_from_s3(self.source_s3_bucket, self.source_file_name)
            content = helpers.read_temp_file(temp_file)
        else:
            content = helpers.read_local_file(self.source_file_name)

        self.list_of_dicts_entries = helpers.csv_file_content_to_dict(content)
        logger.info(f'Got entries as dicts from CSV: {self.list_of_dicts_entries=}')

        if source_type == 'CSV':
            self.list_of_dicts_entries = helpers.csv_file_content_to_dict(content)
            logger.info(f'Got entries as dicts from CSV: {self.list_of_dicts_entries=}')
        elif source_type == 'JSON':
            self.list_of_dicts_entries = json.loads(content)
            logger.info(f'Got entries as dicts from JSON: {self.list_of_dicts_entries=}')
        else:
            raise ValueError(f'Unexpected {source_type=}, CSV/JSON to be used, cant proceed')

    def get_entries_from_csv(self, bucket=None, file_name=None):
        self._get_entries_from_csv_or_json(bucket=bucket, file_name=file_name, source_type='CSV')

    def get_entries_from_json(self, bucket=None, file_name=None):
        self._get_entries_from_csv_or_json(bucket=bucket, file_name=file_name, source_type='JSON')

    def get_entries_from_pg(self):
        # TODO
        pass

    def upload_entries_to_csv(self, bucket=None, file_name=None):
        """warning:
           - TransformType.TRUNCATE_INSERT only
        """

        self._check_entries()

        if bucket:
            self.target_s3_bucket = bucket

        if file_name:
            self.target_file_name = file_name
        if not self.target_file_name:
            raise Exception(f'No target_file_name provided, cant proceed')

        if self.target_s3_bucket:
            content = helpers.list_of_dicts_to_csv_bytes(csv_dict=self.list_of_dicts_entries)
            helpers.upload_object_to_s3(self.target_s3_bucket, self.target_file_name, content)
            logger.info(f'Uploaded entries as csv to s3: {self.target_s3_bucket=}, {self.target_file_name=}')
        else:
            helpers.write_list_of_dicts_to_local_csv_file(self.target_file_name, self.list_of_dicts_entries)
            logger.info(f'Saved entries as csv locally: {self.target_file_name=}')

    def upload_entries_to_json(self, bucket=None, file_name=None):
        """warning: TransformType.TRUNCATE_INSERT only"""

        self._check_entries()

        if bucket:
            self.target_s3_bucket = bucket

        if file_name:
            self.target_file_name = file_name
        if not self.target_file_name:
            raise Exception(f'No target_file_name provided, cant proceed')

        if self.target_s3_bucket:
            helpers.upload_object_to_s3(self.target_s3_bucket, self.target_file_name, self.list_of_dicts_entries_b)
            logger.info(f'Uploaded entries as json to s3: {self.target_s3_bucket=}, {self.target_file_name=}')
        else:
            helpers.write_object_to_local_file(self.target_file_name, self.list_of_dicts_entries_b)
            logger.info(f'Saved entries as json locally: {self.target_file_name=}')

    def upload_entries_to_pg(self, transform_type: TransformType = TransformType.INSERT):
        """insert/upsert/truncate+insert"""

        self._check_entries()

        if transform_type == TransformType.UPSERT:
            raise NotImplemented('TBD: different way of copy to be implemented')
        elif transform_type == TransformType.TRUNCATE_INSERT:
            helpers.execute_postgresql_truncate_table(
                schema=self.target_pg_schema,
                table=self.target_pg_table,
            )
        helpers.execute_postgresql_copy_from(
            schema=self.target_pg_schema,
            table=self.target_pg_table,
            file_content=helpers.list_of_dicts_to_csv_bytes(self.list_of_dicts_entries),
        )

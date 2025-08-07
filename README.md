### what is it?

Lightweight library, intended to help transfer CSV/JSON/Postgresql table/dict in any direction.
Table/dict supposed be stored locally, in s3-compatible storage or Postgresql database.


### disclaimer
This library is under active development, any interface may change without notice.
It is part of another private project, provided "as is" and is unlikely to be changed upon request.
The only reason it is published is to host it in a highly available environment.

> [!WARNING]
> Overall: there is no point in using it for anyone, please use [pandas](https://github.com/pandas-dev/pandas) instead


### installation

```shell
pip install git+https://github.com/smirnovkirilll/table_transfer.git
```


### usage examples

```python
import os
from table_transfer import TableTransfer


# adjust log level for more verbosity (optional)
import logging
logger = logging.getLogger('logging table_transfer')
logger.setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO)

# 1. local-csv-to-local-json
table1 = TableTransfer(
    source_file_name=os.environ['LOCAL_SOURCE_FILE_NAME_CSV'],
    target_file_name=os.environ['LOCAL_TARGET_FILE_NAME_JSON'],
)
table1.get_entries_from_csv()
table1.upload_entries_to_json()

# 2. local-csv-to-s3-json
# actually, s3 credentials should present in environmental variables too
table2 = TableTransfer(
    source_file_name=os.environ['LOCAL_SOURCE_FILE_NAME_CSV'],
    target_s3_bucket=os.environ['S3_BUCKET'],
    target_file_name=os.environ['S3_TARGET_FILE_NAME_JSON'],
)
table2.get_entries_from_csv()
table2.upload_entries_to_json()

# 3. s3-json-to-local-csv
table3 = TableTransfer(
    source_s3_bucket=os.environ['S3_BUCKET'],
    source_file_name=os.environ['S3_SOURCE_FILE_NAME_JSON'],
    target_file_name=os.environ['LOCAL_TARGET_FILE_NAME_CSV'],
)
table3.get_entries_from_json()
table3.upload_entries_to_csv()

# 4. s3-csv-to-postgresql
# NOT IMPLEMENTED YET
```

Easy to see that main class `TableTransfer` designed to achieve its goal by two types of method calls:
- transfer external object to inner representation
- save inner object to external form

This design decision defines limitations of library:
- object should fit in memory
- snapshot ingestion model is in prior (no increments/chunks or whatever)

> [!NOTE]
> This library is designed for migrating small to medium sized tables and is certainly not intended for use in production data warehouses

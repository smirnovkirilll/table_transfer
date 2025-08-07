#!/bin/env python
# note: the implementation of yandex-cloud specific functions borrowed from
# official manuals and training courses https://yandex.cloud


import boto3
import csv
import logging
import os
import psycopg2
import requests
import typing as tp
import yandexcloud
from bs4 import BeautifulSoup
from datetime import datetime
from io import BytesIO
from requests import Session
from requests.adapters import HTTPAdapter
from urllib.parse import urlparse
from urllib3.util.retry import Retry
from yandex.cloud.lockbox.v1.payload_service_pb2 import GetPayloadRequest
from yandex.cloud.lockbox.v1.payload_service_pb2_grpc import PayloadServiceStub


logger = logging.getLogger('logging helpers')
logger.setLevel(logging.INFO)
boto_session = None
s3_client = None
docapi_table = None
ymq_queue = None
DTTM_FORMAT = '%Y-%m-%d %H:%M:%S'
HTTP = 'http://'
HTTPS = 'https://'
BLACKLISTED_DOMAIN = os.environ['BLACKLISTED_DOMAIN'].split(',')
BLACKLISTED_URL = os.environ['BLACKLISTED_URL'].split(',')
TEMP_FILE_NAME = os.environ['TEMP_FILE_NAME']


def _timestamp_to_dttm(timestamp: int, dttm_format: str = DTTM_FORMAT) -> str:

    return datetime.strftime(datetime.fromtimestamp(timestamp), dttm_format)


def _date_to_timestamp(date: str, date_format: str) -> int:

    return int(datetime.strptime(date, date_format).timestamp())


def _requests_session() -> Session:
    """
    need more flexible retry policy
    https://stackoverflow.com/questions/15431044/can-i-set-max-retries-for-requests-request
    """

    retry_strategy = Retry(
        total=1,
        backoff_factor=0.1,
        backoff_max=5,
        respect_retry_after_header=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session


def _is_cloud_execution() -> bool:
    """
    depending on local/cloud script execution, different strategies will be used
    (for keys retrieval, database handling)
    """

    if os.environ.get('CLOUD_EXECUTION_TRUE'):
        logger.info(f'Cloud execution')
        return True
    else:
        logger.info(f'Local execution')
        return False


def _get_lockbox_payload(secret_id: str):
    """initialize lockbox and read secret value"""

    yc_sdk = yandexcloud.SDK()
    channel = yc_sdk._channels.channel('lockbox-payload')
    lockbox = PayloadServiceStub(channel)

    return lockbox.Get(GetPayloadRequest(secret_id=secret_id))


def _lockbox_payload_to_dict(payload, expected) -> dict:
    """transform lockbox payload to dict of expected form"""

    available = expected.copy()
    for entry in payload.entries:
        if entry.key.lower() in available.keys():
            available[entry.key.lower()] = entry.text_value
    if None in available.values():
        raise Exception('Secrets required')

    return available


def _get_environ_aws_secrets() -> dict:
    """for development and local usage"""

    return {
        'secret_id': None,
        'aws_access_key_id': os.environ['AWS_ACCESS_KEY_ID'],
        'aws_secret_access_key': os.environ['AWS_SECRET_ACCESS_KEY'],
    }


def _get_lockbox_aws_secrets() -> dict:

    secrets = {
        'secret_id': os.environ['LOCKBOX_AWS_SECRET_ID'],
        'aws_access_key_id': None,
        'aws_secret_access_key': None,
    }
    payload = _get_lockbox_payload(secrets['secret_id'])

    return _lockbox_payload_to_dict(payload, secrets)


def get_aws_secrets() -> dict:

    if _is_cloud_execution():
        return _get_lockbox_aws_secrets()
    else:
        return _get_environ_aws_secrets()


def _get_environ_postgresql_secrets() -> dict:
    """for development and local usage"""

    return {
        'secret_id': None,
    }


def _get_lockbox_postgresql_secrets() -> dict:
    """due to python API for connection manager is not implemented yet, retrieve all connection data from lockbox"""

    secrets = {
        'secret_id': os.environ['LOCKBOX_PG_SECRET_ID'],
        'database': None,
        'user': None,
        'host': None,
        'port': None,
        'password': None,
    }
    payload = _get_lockbox_payload(secrets['secret_id'])

    return _lockbox_payload_to_dict(payload, secrets)


def get_postgresql_secrets() -> dict:

    if _is_cloud_execution():
        return _get_lockbox_postgresql_secrets()
    else:
        return _get_environ_postgresql_secrets()


def _get_postgresql_connection():
    """for production and development usage"""

    connection_data = get_postgresql_secrets()
    secret_id = connection_data.pop('secret_id')

    if secret_id:
        connection_data['sslmode'] = os.environ['POSTGRESQL_SSLMODE'],
        connection_data['target_session_attrs'] = os.environ['POSTGRESQL_TARGET_SESSION_ATTRS']
        connection = psycopg2.connect(**connection_data)
        logger.info(f'Built postgresql connection for {secret_id=}')
        return connection
    else:
        logger.info(f'Cant build postgresql connection, {secret_id=}')
        return None


def get_postgresql_connection():
    """for production usage only"""

    connection = _get_postgresql_connection()
    if not connection:
        raise Exception('No postgresql_connection provided, cant proceed')
    return connection


def get_boto_session():

    global boto_session
    if boto_session is not None:
        return boto_session

    aws_secrets = get_aws_secrets()
    secret_id = aws_secrets.pop('secret_id')
    boto_session = boto3.session.Session(**aws_secrets)
    logger.info(f'Built boto-session for {secret_id=}')

    return boto_session


def get_ymq_queue():

    global ymq_queue
    if ymq_queue is not None:
        return ymq_queue

    ymq_queue_url = os.environ['YMQ_QUEUE_URL']
    endpoint_url = os.environ['YMQ_ENDPOINT_URL']
    region_name = os.environ['YMQ_REGION_NAME']

    ymq_queue = get_boto_session().resource(
        service_name='sqs',
        endpoint_url=endpoint_url,
        region_name=region_name,
    ).queue(ymq_queue_url)
    logger.info(f'Got sqs={ymq_queue_url}')

    return ymq_queue


def get_docapi_table(table_name: str):

    global docapi_table
    if docapi_table is not None:
        return docapi_table

    endpoint_url = os.environ['DOCAPI_ENDPOINT_URL']
    region_name = os.environ['DOCAPI_REGION_NAME']

    docapi_table = get_boto_session().resource(
        service_name='dynamodb',
        endpoint_url=endpoint_url,
        region_name=region_name,
    ).Table(table_name)
    logger.info(f'Got YDB-{table_name=}')

    return docapi_table


def get_s3_client():

    global s3_client
    if s3_client is not None:
        return s3_client

    endpoint_url = os.environ['S3_ENDPOINT_URL']
    region_name = os.environ['S3_REGION_NAME']

    s3_client = get_boto_session().client(
        service_name='s3',
        endpoint_url=endpoint_url,
        region_name=region_name,
    )
    logger.info(f'Got S3-client')

    return s3_client


def upload_object_to_s3(bucket: str, file_name: str, data_obj: bytes) -> None:

    temp_file = BytesIO()
    temp_file.write(data_obj)
    temp_file.seek(0)

    s3_client = get_s3_client()
    logger.info(f'Starting upload to S3 {file_name=}')
    s3_client.upload_fileobj(temp_file, bucket, file_name)


def download_object_from_s3(bucket: str, file_name: str) -> BytesIO:

    temp_file = BytesIO()

    s3_client = get_s3_client()
    logger.info(f'Starting download from S3 {file_name=}')
    s3_client.download_fileobj(bucket, file_name, temp_file)

    return temp_file


def read_local_file(file_name: str, mode: str = 'r') -> tp.Union[str, bytes]:

    with open(file_name, mode) as f:
        return f.read()


def read_temp_file(temp_file: BytesIO) -> str:

    temp_file.seek(0)
    content = temp_file.read().decode()
    temp_file.close()

    return content


def csv_file_content_to_dict(file_content: str) -> list[dict[str: str]]:

    return [row for row in csv.DictReader(file_content.splitlines())]


def write_object_to_local_file(file_name: str, data_obj: bytes) -> None:

    with open(file_name, 'wb') as f:
        f.write(data_obj)


def write_list_of_dicts_to_local_csv_file(file_name: str, csv_dict: list[dict[str: str]]) -> None:
    """csv library works with string objs, so workflow to be slightly changed"""

    with open(file_name, 'w', newline='') as f:
        writer = csv.DictWriter(f, csv_dict[0].keys())
        writer.writeheader()
        writer.writerows(csv_dict)


def list_of_dicts_to_csv_bytes(
        csv_dict: list[dict[str: str]],
        temp_file_name: str = TEMP_FILE_NAME,
) -> bytes:
    """convert list of dicts to csv entries in byte format"""

    write_list_of_dicts_to_local_csv_file(temp_file_name, csv_dict)
    content = read_local_file(temp_file_name, mode='rb')
    os.remove(temp_file_name)

    return content


def make_clean_url(url: str) -> str:

    labels = (
        '?ssr=true',
        '?utm',
        '&utm',
        '?fbclid',
        '?source',
        '?fb_ref',
        '?fb_action_ids',
        '?fb_action_types',
    )

    clean_url = url
    for label in labels:
        clean_url = clean_url.split(label)[0]

    return clean_url


def get_domain_by_url(url: str) -> str:

    return urlparse(url).netloc


def is_correct_url(url: str) -> bool:

    return bool(get_domain_by_url(url))


def secure_url(url: str) -> str:

    if url.startswith(HTTPS):
        return url
    elif url.startswith(HTTP):
        return HTTPS + url.split(HTTP)[1]
    else:
        raise ValueError(f'Incorrect form of {url=}')


def get_unshorten_url(url: str, session: Session = None) -> tp.Optional[str]:
    """potentially time spending func"""

    if get_domain_by_url(url) in BLACKLISTED_DOMAIN or url in BLACKLISTED_URL:
        return None

    session_instance = session or requests
    headers = session_instance.head(url, timeout=5).headers

    if 'location' in headers and headers['location'].startswith('http'):
        return headers['location']
    else:
        return url


def get_title_by_url(url: str, session: Session = None) -> tp.Optional[str]:
    """potentially time spending func"""

    if get_domain_by_url(url) in BLACKLISTED_DOMAIN or url in BLACKLISTED_URL:
        return None

    session_instance = session or requests
    response = session_instance.get(url, timeout=5)
    soup = BeautifulSoup(response.text, 'html.parser')
    title = soup.find('title')

    if title:
        return (
            title.text
            .strip()
            .replace('\n', ' ')
            .replace(';', ' ')
            .replace('&quot', ' ')
            .replace('\u2028', ' ')
        )
    else:
        return None


def execute_postgresql_query(query: str):

    connection = get_postgresql_connection()
    if connection:
        cursor = connection.cursor()
        logger.info(f'Starting execute {query=}')
        cursor.execute(query)
        return cursor.fetchall()

    raise Exception('Cant execute any query, no connection provided')


def execute_postgresql_copy_expert(query: str, file: BytesIO) -> None:

    connection = get_postgresql_connection()
    if connection:
        cursor = connection.cursor()
        logger.info(f'Starting execute copy_expert {query=}')
        cursor.copy_expert(query, file)
        logger.info(f'Finished execute copy_expert {query=}')

    raise Exception('Cant execute any query, no connection provided')


def execute_postgresql_copy_from(schema: str, table: str, file_content: bytes) -> None:

    query = f"""COPY `{schema}`.`{table}` FROM STDIN WITH CSV HEADER;"""
    execute_postgresql_copy_expert(query, BytesIO(file_content))


def execute_postgresql_truncate_table(schema: str, table: str):

    query = f"""TRUNCATE `{schema}`.`{table}`;"""
    execute_postgresql_query(query)

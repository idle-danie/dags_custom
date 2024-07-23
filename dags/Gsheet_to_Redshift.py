from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.models import Variable

from datetime import datetime
from datetime import timedelta
from plugins import gsheet
from plugins import s3

import requests
import logging
import psycopg2
import json


def download_tab_in_gsheet(**context):
    url = context["params"]["url"]
    tab = context["params"]["tab"]
    table = context["params"]["table"]
    data_dir = Variable.get("DATA_DIR")

    gsheet.get_google_sheet_to_csv(url, tab, data_dir + "{}.csv".format(table))


def copy_to_s3(**context):
    table = context["params"]["table"]
    s3_key = context["params"]["s3_key"]

    s3_conn_id = "aws_conn_id"
    s3_bucket = "dags-custom-dev"
    data_dir = Variable.get("DATA_DIR")
    local_files_to_upload = [data_dir + "{}.csv".format(table)]
    replace = True

    s3.upload_to_s3(s3_conn_id, s3_bucket, s3_key, local_files_to_upload, replace)


dag = DAG(
    dag_id="Gsheet_to_Redshift",
    start_date=datetime(2021, 11, 27),  # 날짜가 미래인 경우 실행이 안됨
    schedule="0 9 * * *",  # 적당히 조절
    max_active_runs=1,
    max_active_tasks=2,
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
    },
)

sheets = [
    {
        "url": "https://docs.google.com/spreadsheets/d/1hW-_16OqgctX-_lXBa0VSmQAs98uUnmfOqvDYYjuE50/",
        "tab": "SheetToRedshift",
        "schema": "danie",
        "table": "spreadsheet_copy_testing",
    }
]

for sheet in sheets:
    download_tab_in_gsheet_task = PythonOperator(
        task_id="download_{}_in_gsheet".format(sheet["table"]),
        python_callable=download_tab_in_gsheet,
        params=sheet,
        dag=dag,
    )

    s3_key = sheet["schema"] + "_" + sheet["table"]

    copy_to_s3_task = PythonOperator(
        task_id="copy_{}_to_s3".format(sheet["table"]),
        python_callable=copy_to_s3,
        params={"table": sheet["table"], "s3_key": s3_key},
        dag=dag,
    )

    run_copy_sql = S3ToRedshiftOperator(
        task_id="run_copy_sql_{}".format(sheet["table"]),
        s3_bucket="dags-custom-dev",
        s3_key=s3_key,
        schema=sheet["schema"],
        table=sheet["table"],
        copy_options=["csv", "IGNOREHEADER 1"],
        method="REPLACE",
        redshift_conn_id="redshift_dev_db",
        aws_conn_id="aws_conn_id",
        dag=dag,
    )

    download_tab_in_gsheet_task >> copy_to_s3_task >> run_copy_sql

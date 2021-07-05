import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from utils.bq_data_extraction import get_all_data_for_date
from utils.data_transformation import transform
from utils.model_evaluation import predict
from utils.telegram import send_message


def combine_all():
    yesterday = str(datetime.date.today()-datetime.timedelta(days = 1))
    today = str(datetime.date.today())
    data_raw = get_all_data_for_date(yesterday, today)
    data_transformed = transform(data_raw, yesterday, today)
    prediction = predict(data_transformed)
    send_message(prediction)

dag_name = Path(__file__).stem

DAG = DAG(
    dag_name,
    description='BTC price prediction',
    start_date=datetime.datetime(2021, 7, 1),
    schedule_interval="0 6 * * *",
    catchup=False,)

do_magic = PythonOperator(
    task_id="crypto_data_prediction",
    python_callable=combine_all,
    dag=DAG
)

do_magic

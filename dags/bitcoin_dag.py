import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

from dags.utils.bq_data_extraction import get_data_for_date
from dags.utils.data_transformation import transform
from dags.utils.model_evaluation import predict
from dags.utils.telegram import send_message


def combine_all():
    yesterday = datetime.date.today()-datetime.timedelta(days = 1)
    data_raw = get_data_for_date(str(yesterday))
    data_transformed = transform(data_raw)
    prediction = predict(data_transformed)
    send_message(prediction)

dag_name = Path(__file__).stem

DAG = DAG(
    dag_name,
    description='BTC price prediction',
    start_date=datetime.datetime(2021,7,1),
    schedule_interval="0 8 * * *",
    catchup=False,)

do_magic = PythonOperator(
    task_id="crypto_data_prediction",
    python_callable=combine_all,
    dag=DAG
)

do_magic
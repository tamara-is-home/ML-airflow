import os
from google.cloud import bigquery
import pandas as pd

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "sound-potion-312013-6ddcdd6d2c36.json"

SQL = """WITH sent AS (
        SELECT 
            ARRAY_TO_STRING(i.addresses, '') as address,
            count(DISTINCT t.block_number) as sent_trx_number,
            sum(i.value) as sent_total,
            min(i.value) as sent_min,
            avg(i.value) as sent_avg,
            max(i.value) as sent_max,
            min(t.input_count) as min_inputs,
            avg(t.input_count) as avg_inputs,
            max(t.input_count) as max_inputs,
            min(t.output_count) as min_outputs,
            avg(t.output_count) as avg_outputs,
            max(t.output_count) as max_outputs,
            min(t.block_timestamp) as first_trx_date_out,
            max(t.block_timestamp) as last_trx_date_out
        FROM `bigquery-public-data.crypto_bitcoin.transactions` as t, 
        UNNEST(inputs) as i
        WHERE t.block_timestamp >'{0}'
        GROUP BY 1
    ),   
    received AS (
        SELECT 
            ARRAY_TO_STRING(o.addresses, '') as address,
            count(DISTINCT t.block_number) as received_trx_number,
            sum(o.value) as received_total,
            min(o.value) as received_min,
            avg(o.value) as received_avg,
            max(o.value) as received_max,
            min(t.block_timestamp) as first_trx_date_in,
            max(t.block_timestamp) as last_trx_date_in,
            max(t.is_coinbase) as has_coinbase
        FROM `bigquery-public-data.crypto_bitcoin.transactions` as t, 
        UNNEST(outputs)as o
        WHERE t.block_timestamp > '{1}'
        GROUP BY 1
    )
    
    SELECT
        s.address,
        s.sent_trx_number as sent_trx_number,
        r.received_trx_number as received_trx_number,
        s.sent_total as sent_total,
        s.sent_min as sent_min,
        s.sent_avg as sent_avg,
        s.sent_max as sent_max,
        r.received_total as received_total,
        r.received_min as received_min,
        r.received_avg as received_avg,
        r.received_max as received_max,
        s.min_inputs as min_inputs,
        s.avg_inputs as avg_inputs,
        s.max_inputs as max_inputs,
        s.min_outputs as min_outputs,
        s.avg_outputs as avg_outputs,
        s.max_outputs as max_outputs,
        s.first_trx_date_out as first_trx_date,
        s.last_trx_date_out as last_trx_date,
        r.first_trx_date_in as first_trx_date_in,
        r.last_trx_date_in as last_trx_date_in,
        r.has_coinbase as has_coinbase
    FROM sent as s 
    LEFT JOIN received as r
    ON s.address = r.address
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
"""


def get_data_for_date(date: str):
    bq_client = bigquery.Client()
    query = SQL.format(date, date)
    dataframe = bq_client.query(query).result().to_dataframe()
    return dataframe


df = get_data_for_date('2021-07-01')
print(df.head(1), df.shape[0])

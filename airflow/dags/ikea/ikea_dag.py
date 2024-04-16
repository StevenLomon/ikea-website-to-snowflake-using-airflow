import boto3
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from ikea_etl import extract_raw_data, transform_raw_ikea_data

s3_client = boto3.client('s3')
target_bucket_name = 'ikea-airflow-bucket'

url = "https://www.ikea.com/se/sv/new/new-products/"

def extract_data(**kwargs):
    url = kwargs['url']
    df = extract_raw_data(url)
    now = datetime.now()
    date_now_string = now.strftime('%Y-%m-%d')
    print(f"Date: {date_now_string}")
    file_str = f"ikea_data_{date_now_string}"
    df.to_csv(f"{file_str}.csv", index=False)
    output_file_path = f"/home/ubuntu/{file_str}.csv"
    return [output_file_path, file_str]

def transform_data(task_instance):
    data = task_instance.xcom_pull(task_ids='tsk_extract_ikea_data')[0]
    object_key = task_instance.xcom_pull(task_ids='tsk_extract_ikea_data')[1]
    df = pd.read_csv(data)

    clean_ikea_data = transform_raw_ikea_data(df)
    print('Num of rows:', len(df))
    print('Num of cols:', len(df.columns))

    # Convert DataFrame to CSV format
    csv_data = clean_ikea_data.to_csv(index=False)

    # Upload CSV to S3
    object_key = f"{object_key}.csv"
    s3_client.put_object(Bucket=target_bucket_name, Key=object_key, Body=csv_data)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 16),
    'email': ['steven.lennartsson@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=15)
}

with DAG('ikea_dag',
         default_args=default_args,
         # schedule_interval= '@monthly',
         cahchup=False) as dag:
    
    extract_ikea_data = PythonOperator(
        task_id= 'tsk_extract_ikea_data',
        python_callable=extract_data,
        op_kwargs={'url': url}
    )

    transform_ikea_data = PythonOperator(
        task_id= 'tsk_transform_ikea_data',
        python_callable=transform_data,
    )
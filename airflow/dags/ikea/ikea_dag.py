from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from ikea_etl import extract_raw_data

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
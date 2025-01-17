import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook

def upload_from_xcom_to_s3(**kwargs):
    # Pull the DataFrame from XCom
    ti = kwargs['ti']
    df_json = ti.xcom_pull(task_ids='extract_reddit_data', key='reddit_df')
    
    # Convert JSON back to DataFrame
    df = pd.read_json(df_json)

    # Define S3 details
    bucket_name = 'os3://bdus829/obinna/airflow/raw_data/'
    s3_key = f"reddit_posts_{kwargs['execution_date'].strftime('%Y%m%d')}.csv"

    # Convert the DataFrame to CSV and upload it to S3
    s3_hook = S3Hook(aws_conn_id="aws_default")  # Use your Airflow AWS connection ID
    s3_hook.load_string(df.to_csv(index=False), key=s3_key, bucket_name=bucket_name, replace=True)
    print(f"Data successfully uploaded to {bucket_name}/{s3_key}")

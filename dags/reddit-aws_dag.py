from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'obinna',
    'start_date': datetime(2025, 1, 16)
}

#file_postfix = datetime.now().strftime("%Y%m%d")

dag = DAG(
    dag_id='reddit-aws',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

# Define environment variables as a Bash command if needed
extract_reddit = """
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf
spark-submit --master yarn --jars /home/ec2-user/postgresql-42.5.3.jar /home/ec2-user/airflow/dags/AWS-Airflow/scripts/reddit2.py
"""

run_extract_reddit = BashOperator(
    task_id='extract_post_frm_reddit',
    bash_command=extract_reddit,
    dag=dag
)

upload_s3 = """
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf
spark-submit --master yarn --jars /home/ec2-user/postgresql-42.5.3.jar /home/ec2-user/airflow/dags/AWS-Airflow/scripts/reddit-etl.py
"""

run_upload_s3 = BashOperator(
    task_id='run_upload_s3',
    bash_command=upload_s3,
    dag=dag,
)

run_task = """
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf
spark-submit --master yarn --jars /home/ec2-user/postgresql-42.5.3.jar /home/ec2-user/airflow/dags/AWS-Airflow/scripts/reddit3.py
"""

run_task2 = BashOperator(
    task_id='run_upload_s3',
    bash_command=run_task,
    dag=dag,
)

# Task sequence: First set environment variables, then run SparkSubmitOperator
run_extract_reddit >> run_upload_s3 >> run_task2


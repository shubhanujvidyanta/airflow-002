from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from datetime import datetime
from files.airflow_002.scrape import scrape_task
from files.airflow_002.transform import transform_task




dag = DAG('create_ajio_product_data',
          description='dummy dag to copy files from GitHub to GCS bucket',
        #   default_args=default_args,
          schedule_interval=None,
          # depends_on_past= False,
          start_date=datetime(2024, 3, 4),
          catchup=False,
          )

data= {
    "robots" : "https://www.ajio.com/robots.txt",
    "bucket": "airflow-002",
    "gcs_raw_path": "raw_data/",
    "gcs_transformed_path":"transformed_data/",
    "temp_storage": "../data/temp/"
}

start_task = DummyOperator(task_id='start_task', dag=dag)
end_task = DummyOperator(task_id='end_task', dag=dag)

scrape_and_save_task = PythonOperator(task_id='scrape_and_save_task', python_callable=scrape_task, provide_context=True, op_kwargs=data, dag=dag)
transform_and_save_task = PythonOperator(task_id='transform_and_save_task', python_callable=transform_task, provide_context=True, op_kwargs=data, dag=dag)



start_task >> scrape_and_save_task >> transform_and_save_task >> end_task
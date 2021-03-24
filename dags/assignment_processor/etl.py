from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago
import pandas as pd
import gcsfs
from airflow.models import Variable

pg_hook = PostgresHook("heroku_psql")

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': days_ago(2),
    'email': ['huy@tactable.io'],
    'email_on_failure': False,
    'email_on_retry': False
}

dag = DAG(
    'assignment_etl',
    default_args=default_args,
    description='A demo on how you can utilize DAG to extract GCS',
    schedule_interval=None,
    max_active_runs=1
)


def pull_data_from_gcs(**context):
    print(context['dag_run'].conf.get('file_name'))
    fn = context['dag_run'].conf.get('file_name')
    print(context['dag_run'].conf.get('school_bucket'))
    sb = context['dag_run'].conf.get('school_bucket')
    gcs_token = Variable.get("gcs-access-token")
    fs = gcsfs.GCSFileSystem(project="tapd-demo",
                             token="/opt/airflow/dags/connections/google-cloud-storage/tapd-demo-11541f71287e.json")
    with fs.open(f'{sb}/processing/{fn}') as f:
        assignment_csv = pd.read_csv(f)
        return assignment_csv


def transform_data(**context):
    csv = context['task_instance'].xcom_pull(task_ids="extract_csv")
    student_data = csv[['student_id', 'student_first_name', 'student_last_name']]
    student_sql = """
        INSERT INTO student (id, given_name, family_name)
        VALUES
    """
    student_values = [
        "({0}, '{1}', '{2}')".format(student_id, student_first_name, student_last_name)
        for student_id, student_first_name, student_last_name
        in zip(student_data["student_id"], student_data["student_first_name"], student_data["student_last_name"])
    ]
    student_sql += ",".join(student_values)
    student_sql += "ON CONFLICT ON CONSTRAINT student_pkey DO NOTHING;"
    return {"student_sql": student_sql}


def load_data(**context):
    insert_statement = context['task_instance'].xcom_pull(task_ids="transform_csv")["student_sql"]
    print(insert_statement)
    pg_hook.run(insert_statement)


def move_to_archived(**context):
    print(context['dag_run'].conf.get('file_name'))
    fn = context['dag_run'].conf.get('file_name')
    print(context['dag_run'].conf.get('school_bucket'))
    sb = context['dag_run'].conf.get('school_bucket')
    fs = gcsfs.GCSFileSystem(project="tapd-demo",
                             token="/opt/airflow/dags/connections/google-cloud-storage/tapd-demo-11541f71287e.json")
    return fs.mv(f'{sb}/processing/{fn}', f'{sb}/archived/{fn}')


t1 = PythonOperator(
    task_id='extract_csv',
    python_callable=pull_data_from_gcs,
    dag=dag,
    provide_context=True
)

t2 = PythonOperator(
    task_id='transform_csv',
    python_callable=transform_data,
    dag=dag,
    provide_context=True
)

t3 = PythonOperator(
    task_id='load_csv',
    python_callable=load_data,
    dag=dag,
    provide_context=True
)

t4 = PythonOperator(
    task_id='move_csv',
    python_callable=move_to_archived,
    dag=dag,
    provide_context=True
)

t1 >> t2 >> t3 >> t4
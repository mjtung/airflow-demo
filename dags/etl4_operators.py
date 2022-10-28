import datetime
import pendulum
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag_id = 'etl4_ops'
@dag(
    dag_id=dag_id,
    schedule_interval="15 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=240),
    tags=['nba'],
)
def etl4_operators():

    def generate_number() -> int:
        return 5

    start = EmptyOperator(task_id='start')
    finish = EmptyOperator(task_id='finish')
    py_op = PythonOperator(task_id='py_op', python_callable=generate_number)
    bash_op = BashOperator(task_id='bash_op', bash_command=f'echo {py_op.output}')
    start >> py_op >> bash_op >> finish

globals()[dag_id] = etl4_operators()


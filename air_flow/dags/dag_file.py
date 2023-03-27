from datetime import timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
}
dag = DAG(
    dag_id='yad2Prod',
    default_args=args,
    schedule_interval= None #@daily
    tags=['yad2Prod']
)
Yad2ProjectProducer = BashOperator(
    task_id='yad2Prod',
    bash_command='python /home/naya/yad2_project/yad2Producer.py',
    dag=dag,
)
Yad2ProjectProducer

if _name_ == "_main_":
    dag.cli()
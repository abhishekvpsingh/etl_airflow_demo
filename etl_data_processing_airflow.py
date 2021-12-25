"""
Created on Dec-28-21 by Abhishek Singh
This is airflow file && only work under airflow dag directory.
Place it under the dag directory of airflow.
Change the path of the python and code as per your directory path.

"""
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

python_path = '/home/abhi/etl_airflow_demo/etl_airflow_demo_env/bin/python'
project_path = '/home/abhi/etl_airflow_demo'

args = {
    'owner': 'Abhishek Singh',
    'start_date': days_ago(2)
}

dag = DAG(
    dag_id='etl_data_processing',
    default_args=args,
    schedule_interval='0 0 * * *',
    catchup=False
)

create_orders_dir = BashOperator(
    task_id='create_orders_dir',
    bash_command='mkdir -p /tmp/orders && sleep 10',
    dag=dag
)

create_customers_dir = BashOperator(
    task_id='create_customers_dir',
    bash_command='mkdir -p /tmp/customers && sleep 10',
    dag=dag
)

get_orders_from_mysql = BashOperator(
    task_id='get_orders_from_mysql',
    bash_command=f'{python_path} {project_path}/app.py orders',
    dag=dag
)

get_customers_from_pg = BashOperator(
    task_id='get_customers_from_pg',
    bash_command=f'{python_path} {project_path}/app.py customers',
    dag=dag
)

join_orders_and_customers = BashOperator(
    task_id='join_orders_and_customers',
    bash_command=f'mkdir -p /tmp/join_orders_and_customers && {python_path} {project_path}/processing.py',
    dag=dag
)

drop_orders_dir = BashOperator(
    task_id='drop_orders_dir',
    bash_command='rm -rf /tmp/orders',
    dag=dag
)

drop_customers_dir = BashOperator(
    task_id='drop_customers_dir',
    bash_command='rm -rf /tmp/customers',
    dag=dag
)

create_orders_dir >> get_orders_from_mysql >> join_orders_and_customers
create_customers_dir >> get_customers_from_pg >> join_orders_and_customers
join_orders_and_customers >> drop_orders_dir
join_orders_and_customers >> drop_customers_dir

if __name__ == "__main__":
    dag.cli()

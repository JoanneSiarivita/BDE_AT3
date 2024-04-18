import datetime

import airflow
from airflow.operators.bash_operator import BashOperator

with airflow.DAG(
    "trigger_response_dag",
    start_date=datetime.now() - timedelta(days=1),
    schedule_interval=None,
) as dag:
    print_gcs_info = BashOperator(
        task_id="create_psql_table_task_id", bash_command="echo {{dag_run.conf}}"
    )
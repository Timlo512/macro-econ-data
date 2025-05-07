import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from airflow.sdk import Variable

repo_dir = Variable.get("macro_econ_data_repo_dir")

with DAG(
    "macro_econ_data_run_main",
    description = "Macro Econ Data Daily Job",
    schedule = "@daily",
    start_date = datetime.datetime(2025, 5, 7),
    tags = ["macro_econ_data"],
) as dag:
    
    t1 = BashOperator(
        task_id = "crawl_centane_site",
        bash_command = f"cd {repo_dir}\
            && ./.venv/bin/python3 main.py"
    )
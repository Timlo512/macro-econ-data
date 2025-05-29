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
            && ./.venv/bin/python3 main.py --tasks crawl_centaline_ccl"
    )

with DAG(
    "macro_econ_data_run_main_at_1116am_daily",
    description = "Macro Econ Data Daily Job at 11:16 AM (wait for 1 minute after HKAB refreshed)",
    schedule = "16 3 * * MON-FRI", # UTC
    start_date = datetime.datetime(2025, 5, 25),
    tags = ["macro_econ_data"],
):
    t2 = BashOperator(
        task_id = "crawl_hkab_hibor",
        bash_command = f"cd {repo_dir}\
            && ./.venv/bin/python3 main.py --tasks crawl_hkab_hibor"
    )

with DAG(
    "macro_econ_data_run_main_8am_8pm_hourly",
    description = "Macro Econ Data Hourly Job at 8 AM and 8 PM",
    schedule = "0 0-12 * * *", # UTC
):
    t3  = BashOperator(
        task_id = "crawl_centaline_transaction",
        bash_command = f"cd {repo_dir}\
            && ./.venv/bin/python3 main.py --tasks crawl_centaline_transaction"
    )
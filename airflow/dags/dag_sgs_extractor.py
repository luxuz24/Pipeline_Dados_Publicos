import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

default_args = {
    "owner": "Lck3k",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(minutes=20),
}


def alert_failure(context):
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id if context.get("dag") else "Unknown DAG"
    exception = context.get("exception")
    logger.error(
        "FALHA na DAG '%s', task '%s': %s",
        dag_id,
        task_instance.task_id if task_instance else "desconhecida",
        exception,
    )

default_args["on_failure_callback"] = alert_failure


def get_clickhouse_env():
    """
    Retrieves secure credentials from the Airflow Connection.
    """
    conn = BaseHook.get_connection("clickhouse_default")
    return {
        "CLICKHOUSE_HOST": conn.host,
        "CLICKHOUSE_PORT": str(conn.port),
        "CLICKHOUSE_USER": conn.login,
        "CLICKHOUSE_PASSWORD": conn.password,
        "CLICKHOUSE_DB": conn.schema,
    }


def _verify_clickhouse_connection(**_):
    """
    Perform a simple ping to ClickHouse before spending time extracting data.
    """
    import requests
    env = get_clickhouse_env()
    url = f"http://{env['CLICKHOUSE_HOST']}:{env['CLICKHOUSE_PORT']}/ping"
    try:
        response = requests.get(
            url,
            timeout=10,
            auth=(env["CLICKHOUSE_USER"], env["CLICKHOUSE_PASSWORD"]),
        )
        response.raise_for_status()
        logger.info("ClickHouse connection verified successfully.")
    except requests.exceptions.RequestException as exc:
        raise AirflowFailException(f"ClickHouse connection failed: {exc}") from exc


def _validate_data_load(**_):
    """
    Validates that data was loaded into ClickHouse by checking the row count.
    """
    import requests
    env = get_clickhouse_env()
    query = (
        f"SELECT count() FROM {env['CLICKHOUSE_DB']}.raw_indicadores "
        "WHERE toDate(extraido_em) = today() FORMAT TabSeparated"
    )
    url = f"http://{env['CLICKHOUSE_HOST']}:{env['CLICKHOUSE_PORT']}/"
    try:
        response = requests.post(
            url,
            params={"query": query},
            auth=(env["CLICKHOUSE_USER"], env["CLICKHOUSE_PASSWORD"]),
            timeout=15,
        )
        response.raise_for_status()
        total = int(response.text.strip() or 0)
    except (requests.exceptions.RequestException, ValueError) as exc:
        raise AirflowFailException(f"Failed to validate data load: {exc}") from exc

    if total == 0:
        raise AirflowFailException("No data loaded into ClickHouse for today.")
    logger.info(
        "Data load validation successful: %d records loaded into ClickHouse.",
        total,
    )


with DAG(
    dag_id="pipeline_extracao_sgs_bcb",
    default_args=default_args,
    description="Extraction of IPCA and Selic data from the BCB API (SGS) to ClickHouse.",
    schedule="0 9 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=45),
    tags=["bcb", "clickhouse", "ingestion"],
    doc_md=__doc__,
) as dag:
    
    verify_clickhouse_connection = PythonOperator(
        task_id="verify_clickhouse_connection",
        python_callable=_verify_clickhouse_connection,
    )

    run_pipeline_task = BashOperator(
        task_id="run_pipeline",
        bash_command="set -euo pipefail; python /opt/airflow/extractor/extract.py",
        env={**os.environ, **get_clickhouse_env()},
        execution_timeout=timedelta(minutes=15),
    )

    run_dbt_task = BashOperator(
        task_id="run_dbt",
        bash_command=(
            "set -euo pipefail; "
            "cd /opt/airflow/dbt_project; "
            "/home/airflow/.local/bin/dbt run; "
            "/home/airflow/.local/bin/dbt test"
        ),
        env={
            **get_clickhouse_env(),
            "DBT_PROFILES_DIR": "/opt/airflow/dbt_project",
            "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
        },
        execution_timeout=timedelta(minutes=15),
    )

    enrich_with_llm_task = BashOperator(
        task_id="llm_enrich",
        bash_command=(
            "set -euo pipefail; "
            "python /opt/airflow/extractor/enrich.py"
        ),
        env={**os.environ, **get_clickhouse_env()},
        execution_timeout=timedelta(minutes=15),
    )

    validate_data_load = PythonOperator(
        task_id="validate_data_load",
        python_callable=_validate_data_load,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    (
        verify_clickhouse_connection
        >> run_pipeline_task
        >> run_dbt_task
        >> validate_data_load
        >> enrich_with_llm_task
    )
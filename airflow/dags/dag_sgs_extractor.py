import logging
import os 
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator         # Ajustado o caminho
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule      # Ajustado o caminho
from airflow.hooks.base import BaseHook                 # Import adicionado

logger = logging.getLogger(__name__)

# Configurações Padrão
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
    # Retirada a vírgula que transformava a variável em uma tupla
    dag_id = context.get("dag").dag_id if context.get("dag") else "Unknown DAG"
    exception = context.get("exception")
    logger.error(
        "FALHA na DAG '%s', task '%s': %s",
        dag_id,
        task_instance.task_id if task_instance else "desconhecida",
        exception,
    )

# Movido para fora da função
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
        resp = requests.get(url, timeout=10, auth=(env['CLICKHOUSE_USER'], env['CLICKHOUSE_PASSWORD']))
        resp.raise_for_status()
        # Movido para dentro do try para só logar se der status 200
        logger.info("ClickHouse connection verified successfully.") 
    except requests.exceptions.RequestException as exc: # Ajustado para requests.exceptions
        raise AirflowFailException(f"ClickHouse connection failed: {exc}") from exc


def _validate_data_load(**_):
    """
    Validates that data was loaded into ClickHouse by checking the row count.
    """
    import requests
    env = get_clickhouse_env()
    query = (f"SELECT count() FROM {env['CLICKHOUSE_DB']}.raw_indicadores "
        "WHERE toDate(extraido_em) = today() FORMAT TabSeparated")
    url = f"http://{env['CLICKHOUSE_HOST']}:{env['CLICKHOUSE_PORT']}/"
    try:
        resp = requests.post(
            url,
            params={"query": query},
            auth=(env['CLICKHOUSE_USER'], env['CLICKHOUSE_PASSWORD']),
            timeout=15,
        )
        resp.raise_for_status()
        total = int(resp.text.strip() or 0)
    except (requests.exceptions.RequestException, ValueError) as exc:
        raise AirflowFailException(f"Failed to validate data load: {exc}") from exc

    if total == 0:
        raise AirflowFailException("No data loaded into ClickHouse for today.")
    logger.info(f"Data load validation successful: {total} records loaded into ClickHouse.")


with DAG(
    dag_id="pipeline_extracao_sgs_bcb",
    default_args=default_args,
    description="Extraction of IPCA and Selic data from the BCB API (SGS) to ClickHouse.",
    schedule="0 9 * * *",  # No Airflow 2.x o parâmetro é 'schedule' e não 'schedule_interval'
    start_date=datetime(2025, 1, 1), 
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=45), # Corrigido o typo 'degrun_timeout'
    tags=["bcb", "clickhouse","ingestion"],
    doc_md=__doc__,
) as dag:
    
    # Tarefa para verificar a conexão com ClickHouse
    verify_clickhouse_connection = PythonOperator(
        task_id="verify_clickhouse_connection",
        python_callable=_verify_clickhouse_connection,
    )

    # Tarefa para executar o pipeline de extração
    run_pipeline_task = BashOperator(
        task_id="run_pipeline",
        bash_command="set -euo pipefail; python /opt/airflow/extractor/extract.py",
        env=get_clickhouse_env(),
        execution_timeout=timedelta(minutes=15), # Adicionada a vírgula que faltava na linha de cima
    )

    # Tarefa para validar a carga de dados no ClickHouse
    validate_data_load = PythonOperator(
        task_id="validate_data_load",
        python_callable=_validate_data_load,
        trigger_rule=TriggerRule.ALL_SUCCESS,  
    )

    # Definindo a ordem das tarefas
    verify_clickhouse_connection >> run_pipeline_task >> validate_data_load
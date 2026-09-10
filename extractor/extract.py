# Importando as bibliotecas necessarias pra execução do script
import os 
import sys
import logging
from datetime import datetime
import requests
from typing import List, Any
import clickhouse_connect
from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Configurar o logger que sera utilizado para registrar e gerar mensagens de log estruturadas
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("BCB-Extractor")

# Variáveis de Conexão com Fallback
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 8123))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB")

# Dicionário de Séries do SGS/BCB
SERIES = {
    433: "IPCA - VARIAÇÃO MENSAL",
    432: "TAXA DE JUROS - SELIC META(% a.a.)"
}

# Função para obter o cliente ClickHouse
def get_clickhouse_client():
    logger.info(f"Conectando ao ClickHouse em {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}...")
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB
    )

# Função para configurar a tabela raw_indicadores
def setup_raw_table(client):
    logger.info("Verificando a existencia da tabela raw_indicadores...")
    query = """ 
    CREATE TABLE IF NOT EXISTS raw_indicadores (
        codigo_serie UInt32,
        nome_serie String,
        data_referencia Date,
        valor Float64,
        extraido_em DateTime DEFAULT now()
    ) ENGINE = ReplacingMergeTree(extraido_em)
    ORDER BY (codigo_serie, data_referencia);
    """
    client.command(query)
    logger.info("Tabela raw_indicadores pronta")

# Função para buscar dados da série do SGS/BCB
def fetch_sgs_series(codigo_serie: int, ultimos_meses: int = 20) -> list:
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo_serie}/dados/ultimos/{ultimos_meses}"
    
    params = {"formato": "json"}
    headers = {"Accept": "application/json"}
    
    logger.info(f"Extraindo dados da serie {codigo_serie} ({url})...")
    response = requests.get(url, params=params, headers=headers, timeout=15)
    
    if response.status_code != 200:
        logger.error(f"Erro {response.status_code} do BCB: {response.text}")
        response.raise_for_status()

    return response.json()

# Função para parsear o valor da série
def parse_valor(valor_str: Any) -> float:
    if valor_str is None or valor_str == "":
        return 0.0
    try:
        return float(str(valor_str).replace(",", "."))
    except ValueError:
        logger.warning(f"Formato numérico inválido retornado pela API: '{valor_str}'. Assumindo 0.0")
        return 0.0

# Função principal do pipeline de extração
def run_pipeline():
    start_time = datetime.now()
    logger.info("Iniciando pipeline de extração SGS/BCB...")

    try:
        client = get_clickhouse_client()
        setup_raw_table(client)

        total_registros = 0
        for codigo, nome in SERIES.items():
            raw_data = fetch_sgs_series(codigo)
            logger.info(f"Processando {len(raw_data)} registros da série {codigo} - {nome}...")
            
            rows_to_insert = []
            for item in raw_data:
                try:
                    dt = datetime.strptime(item["data"], "%d/%m/%Y").date()
                    valor = parse_valor(item["valor"])
                    rows_to_insert.append((codigo, nome, dt, valor))
                
                except (ValueError, KeyError) as e:
                    logger.warning(f"Ignorando registro invalido na serie {codigo}: {item}. Erro: {e}")
            
            if rows_to_insert:
                client.insert(
                    table="raw_indicadores",
                    data=rows_to_insert,
                    column_names=["codigo_serie", "nome_serie", "data_referencia", "valor"]
                )
                total_registros += len(rows_to_insert)
            
        logger.info(f"Extração finalizada com sucesso. Total de {total_registros} registros inseridos na tabela raw_indicadores.")
        logger.info(f"Tempo total de execução: {datetime.now() - start_time}")
        
    except Exception as e:
        logger.error(f"Erro durante a execução do pipeline: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    run_pipeline()